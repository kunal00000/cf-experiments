# Durable Scheduler

A scheduling primitive built on a SQLite-backed Cloudflare Durable Object. It multiplexes logical schedules onto the single native alarm available to each Durable Object.

```text
scheduleAfter / scheduleAt / scheduleCron
                    |
                    v
              SQLite rows
                    |
                    v
          setAlarm(MIN(run_at))
                    |
                    v
             alarm.handler()
                    |
          +---------+---------+
          |                   |
       success              failure
    delete/advance         retry/dead
```

## Building blocks

| Component | Role |
| --- | --- |
| Durable Object | Owns and coordinates the schedules for one tenant, workflow, user, or other domain boundary. Different object IDs provide independent partitions. |
| SQLite | Stores logical schedules and supports ordered due-work queries, filtering, and retry state. |
| Native alarm | Wakes the object for the earliest pending row. It is derived state, not the schedule store. |
| Named callbacks | Persist function identity across eviction and deployment while preserving callback-specific payload types at TypeScript call sites. |
| `cron-schedule` | Parses cron expressions and calculates their next occurrence. Execution remains with the native alarm. |
| Observability and source maps | Preserve useful failure context for callbacks executed outside the request that created them. |

The Worker harness uses the Durable Object named `test`. A larger system would choose an object ID from its coordination boundary. Cloudflare describes this partitioning model in [Rules of Durable Objects](https://developers.cloudflare.com/durable-objects/best-practices/rules-of-durable-objects/).

## Durable semantics

### SQLite is authoritative

The physical alarm is always projected from pending rows:

```sql
SELECT MIN(run_at)
FROM schedules
WHERE status = 'pending'
```

Inserts, cancellations, and alarm passes recompute that value. With no pending rows, the native alarm is deleted. If more than 50 due rows remain after a pass, the minimum stays in the past and causes another invocation as soon as possible.

This indirection is necessary because each Durable Object has one native alarm and a later `setAlarm()` replaces the earlier value. See [Durable Object Alarms](https://developers.cloudflare.com/durable-objects/api/alarms/).

### Logical time and retry time are separate

| Field | Meaning | Retry behavior |
| --- | --- | --- |
| `scheduledFor` | Identity of the logical occurrence | Stable |
| `runAt` | Time when the next physical attempt becomes eligible | Moves forward |

The idempotency key is `<schedule id>:<scheduledFor>`, so retries of one occurrence share a key. A successfully advanced cron occurrence receives a new `scheduledFor` and key.

Delivery is at least once. A callback's external effect and the following SQLite mutation cannot be committed atomically, so callbacks crossing a system boundary should propagate `context.idempotencyKey`.

### Execution and failures

One handler pass selects up to 50 rows that are due at query time and processes them sequentially in ascending `runAt` order. A slow callback delays later rows in that batch. Rows that become due during the pass are picked up after the alarm is rearmed.

| Outcome | Persisted result |
| --- | --- |
| One-shot succeeds | Row deleted |
| Cron succeeds | Next occurrence stored; attempts and error reset |
| Callback fails for the first or second time | Same occurrence retried after approximately 2 or 4 seconds |
| Callback fails for the third time | Row retained as `dead` |
| Callback key is missing | Row retained as `dead`; attempts unchanged |

Application callback errors are contained by the scheduler. Failures outside that boundary escape `handler()` and use Cloudflare's native alarm retry behavior.

Cron expressions use the Worker runtime's UTC clock. Five-field expressions have minute precision; six-field expressions include seconds. The next occurrence is calculated from callback completion time, so missed occurrences are skipped rather than replayed.

## API and use cases

```ts
const alarm = new DurableScheduler(ctx, callbacks);

await alarm.scheduleAfter(15 * 60_000, "expireCheckout", { checkoutId });
await alarm.scheduleAt(auction.closesAt, "closeAuction", { auctionId });
await alarm.scheduleCron("0 0 * * *", "rollUpDailyUsage", { tenantId });
```

| Method | Use it for |
| --- | --- |
| `scheduleAfter()` | Inactivity expiry, reservation grace periods, delayed status checks, or coalesced notifications. |
| `scheduleAt()` | Auction close times, embargoed publishing, credential expiry, or workflow deadlines owned by another record. |
| `scheduleCron()` | UTC usage rollups, retention work, compaction, or reporting cutoffs. |
| `get()` | Reading retry state or reconciling a stored schedule ID with domain state. |
| `list()` | Finding dead callbacks, auditing a callback before renaming it, or inspecting a time window. |
| `cancel()` | Revoking work after the domain intent disappears. Repeated cancellation returns `false`. |
| `handler()` | Bridging the Durable Object's native `alarm()` method to due-work processing. |

`scheduleAfter()` and `scheduleAt()` both create `type: "once"` rows. `scheduleCron()` returns an existing pending row when its callback key, payload JSON, and cron expression match.

Results from `list()` are ordered by `runAt` and are not paginated. A completed one-shot is absent from `get()` because successful one-shot rows are deleted rather than retained as history.

The Durable Object forwards its native handler directly:

```ts
alarm(): Promise<void> {
	return this.#alarm.handler();
}
```

## HTTP harness

The outer Worker routes every request to `MY_DURABLE_OBJECT.getByName("test")`; the Durable Object owns the request switch.

```sh
curl -sS http://localhost:8787 \
  -H 'content-type: application/json' \
  -d '{"action":"scheduleAfter","delayMs":1000,"payload":{"message":"hello"}}' | jq
```

Supported actions are `scheduleAfter`, `scheduleAt`, `scheduleCron`, `get`, `list`, and `cancel`. The HTTP body is type-asserted rather than runtime-validated.
