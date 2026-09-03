# Durable Scheduler Experiment

This project explores a small, standalone scheduling primitive built directly on a SQLite-backed Cloudflare Durable Object. It multiplexes many logical schedules onto the one physical alarm available to each Durable Object instance.

```text
 scheduleAfter / scheduleAt / scheduleCron
             |
             v
     SQLite schedule rows
             |
             v
   MIN(pending run_at)
             |
             v
  Durable Object setAlarm()
             |
             v
 alarm() executes due rows
             |
             +-- success --> delete or advance cron
             +-- failure --> retry or mark dead
```

The implementation is intentionally self-contained. It borrows API ideas from the [Cloudflare Agents scheduler](https://github.com/cloudflare/agents/blob/main/packages/agents/src/schedules/scheduler.ts), but does not adopt its Lifecycle capability, shared job queue, routing, or compatibility machinery.

## What earns a place

### A Durable Object per coordination boundary

A Durable Object supplies a globally addressable owner for both schedule state and execution. Calls for the same object ID are coordinated with the same private storage, while different IDs form independent scheduler partitions.

This fits schedules that naturally belong to an existing entity: a tenant, workflow, user, device, document, or checkout. It does not justify routing every schedule in an application through one global object. The experiment's Worker always uses the name `test`; that is a test harness, not a scaling model.

Cloudflare recommends alarms for per-entity scheduled work and warns that a single global Durable Object becomes a bottleneck. See [Rules of Durable Objects](https://developers.cloudflare.com/durable-objects/best-practices/rules-of-durable-objects/).

### SQLite as the logical schedule store

The platform provides only one alarm per Durable Object. SQLite holds the larger logical schedule set that the alarm represents.

SQLite earns its place because the scheduler needs more than durable key lookup:

- an ordered due-work query;
- a composite index over `status, run_at`;
- filters for inspection and operations;
- durable retry metadata;
- synchronous mutation before the physical alarm is rearmed.

The storage is private to the Durable Object instance and strongly consistent. The namespace uses `new_sqlite_classes` because `ctx.storage.sql` is unavailable to KV-backed Durable Objects. See [SQLite-backed Durable Object Storage](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/).

There is no schema migration framework yet. The table and index are created idempotently in the constructor because this experiment has not been deployed and has no historical schema to preserve.

### A single derived physical alarm

The physical alarm is derived state:

```sql
SELECT MIN(run_at)
FROM schedules
WHERE status = 'pending'
```

Every insert, cancellation, and alarm pass recomputes it. If no pending row remains, the scheduler deletes the physical alarm. This keeps SQLite authoritative and lets the next mutation recover a stale or overwritten alarm.

This indirection is required because `setAlarm()` replaces the one existing alarm for that object. Cloudflare explicitly recommends storing multiple logical events and rearming the alarm for the next one. See [Durable Object Alarms](https://developers.cloudflare.com/durable-objects/api/alarms/).

### Named, typed callbacks

Functions cannot be persisted across Durable Object eviction or deployment. A schedule stores a callback name and JSON payload, then resolves that name against the callback registry when it executes.

The generic callback map ties each name to its payload type:

```ts
const callbacks = {
	deliverWebhook: async (
		payload: { endpoint: string; body: unknown },
		context: ScheduleContext,
	) => {
		await fetch(payload.endpoint, {
			method: "POST",
			headers: { "idempotency-key": context.idempotencyKey },
			body: JSON.stringify(payload.body),
		});
	},
};

const scheduler = new DurableScheduler(ctx, callbacks);

await alarm.scheduleAfter(5_000, "deliverWebhook", {
	endpoint: "https://example.com/hooks/order",
	body: { orderId: "order_123" },
});
```

The compiler checks callback names and the payload supplied for each name. The persisted string remains a deployment compatibility contract: renaming or removing a callback while its rows exist makes those rows fail and eventually become dead.

The payload parameter is currently optional at the scheduler API boundary. Omitting it stores SQL `NULL` and delivers `undefined`, even when a callback's declared payload type is non-optional. That is a known gap in the current type surface.

### `cron-schedule` for calculation, not execution

`cron-schedule` is the scheduler's only runtime dependency. It parses five- or six-field cron expressions and calculates the next matching `Date`. It does not own timers in this project; Durable Object alarms remain the only execution mechanism.

That division matters. An in-memory timer would disappear on eviction and would compete with the physical alarm rather than contribute to durability. A small parser earns a place; a second scheduling runtime does not. The package's supported expression grammar is documented in [`cron-schedule`](https://github.com/P4sca1/cron-schedule#cron-expression-format).

The API has no timezone parameter. Cron evaluation follows the Worker's runtime clock, which is UTC. Supporting tenant-local calendar schedules would require an explicit timezone model, including daylight-saving transitions; silently accepting a timezone string would be incorrect.

### Explicit retry state

Callback failures are application failures, so the scheduler catches them and persists the next attempt instead of relying on the platform alarm retry budget.

The current row state provides:

- a stable logical occurrence time;
- a separate physical retry time;
- an attempt count;
- a terminal `dead` state;
- the latest error message.

This is deliberately visible through `get()` and `list()`. A retry policy that cannot be inspected is difficult to operate.

Unexpected failures outside the per-row callback boundary still escape `handler()`. Cloudflare then applies the platform alarm's at-least-once retry behavior. The platform currently retries thrown alarm executions with exponential backoff, while callback failures in this scheduler use the policy persisted in the row. These are separate retry planes.

### Observability and uploaded source maps

Wrangler observability and source-map upload are enabled because callback failures involve asynchronous execution detached from the request that created the schedule. The row retains only an error message; logs and source maps are where stack traces and execution context remain actionable.

TypeScript and Wrangler are development/deployment tooling rather than runtime scheduling components. `worker-configuration.d.ts` supplies the generated runtime and binding types. `@types/node` remains from the scaffold, but the scheduler itself does not use Node APIs.

## Core invariants

### `scheduledFor` and `runAt` are different clocks

| Field | Meaning | Changes on retry? |
| --- | --- | --- |
| `scheduledFor` | Identity of the logical occurrence | No |
| `runAt` | Time at which the next physical attempt becomes eligible | Yes |

The distinction keeps this idempotency key stable across retries:

```text
<schedule id>:<scheduledFor>
```

When a cron schedule advances successfully, `scheduledFor` changes and therefore produces a new key for the new logical occurrence.

### SQLite is authoritative; the alarm is a projection

The scheduler never treats the currently configured platform alarm as the schedule database. The invariant after each mutation is:

```text
physical alarm = MIN(run_at) across pending rows
```

If an alarm processes the batch limit while more due rows remain, the minimum remains in the past. Setting that value asks the platform to invoke the object again in the immediate future.

### Delivery is at least once

The callback's external side effect and the subsequent SQLite delete/update cannot be committed atomically. A reset after the side effect but before schedule completion can execute the same occurrence again. Callbacks that cross a system boundary should pass `context.idempotencyKey` to that system or enforce equivalent deduplication themselves.

### Cron advances only after callback success

A cron row does not advance while its callback is failing. It retries the same occurrence with the same idempotency key. Exhausting the retry budget marks the row dead and stops future occurrences until an operator changes or replaces it.

The next cron match is calculated from completion time. Occurrences missed during downtime or a long callback are skipped rather than replayed.

### Cron deduplication is structural and exact

`scheduleCron()` returns an existing pending row when these persisted values match:

- schedule type;
- callback name;
- JSON payload string;
- cron expression.

One-shot schedules never deduplicate. A dead cron row does not block creation of a replacement.

Payload comparison is byte-oriented after `JSON.stringify()`. Objects with the same keys inserted in different orders may not deduplicate; values changed by JSON serialization may collide. This is sufficient for the experiment but is not a canonical content identity scheme.

## Capabilities and strong use cases

The examples below assume each named callback is present in the registry passed to `DurableScheduler`.

### `scheduleAfter()` — relative follow-up from a domain event

Use `scheduleAfter()` when the delay itself is the policy and the absolute timestamp is incidental.

```ts
await alarm.scheduleAfter(15 * 60_000, "expireCheckout", {
	checkoutId,
});
```

Good fits include:

- expiring a checkout after inactivity;
- releasing a reservation after a grace period;
- checking whether an asynchronous operation completed after its expected latency;
- deferring a notification to allow related events to coalesce.

It is a poor fit for a business deadline already represented by an authoritative timestamp. Use `scheduleAt()` so retries in the request path do not recalculate and extend the deadline.

### `scheduleAt()` — execution eligibility at an authoritative instant

Use `scheduleAt()` when another domain record already owns the deadline.

```ts
await alarm.scheduleAt(auction.closesAt, "closeAuction", {
	auctionId: auction.id,
});
```

Good fits include:

- closing an auction or voting window;
- publishing embargoed content;
- expiring a credential, lease, or invitation;
- transitioning a workflow when its externally chosen deadline arrives.

The timestamp means "eligible no earlier than this time," not hard real-time execution. Failover and maintenance can delay a Durable Object alarm.

Both `scheduleAfter()` and `scheduleAt()` persist as `type: "once"`. The returned schedule does not retain which API produced it.

### `scheduleCron()` — UTC calendar recurrence

Use `scheduleCron()` when the rule is expressed in calendar fields rather than elapsed duration.

```ts
await alarm.scheduleCron("0 0 * * *", "rollUpDailyUsage", {
	tenantId,
});
```

Good fits include:

- daily UTC usage aggregation per tenant;
- weekly retention or compaction work for one entity;
- UTC reporting cutoffs;
- sparse calendar schedules that would otherwise require custom date arithmetic.

Five-field expressions have minute precision; six-field expressions add seconds. There is no timezone support, so a requirement such as "09:00 in each tenant's local timezone" is outside this experiment's contract.

### `get()` — inspect one durable intent

Use `get()` when another record stores the schedule ID or an operator needs the exact state of one job.

```ts
const schedule = alarm.get(scheduleId);

if (schedule?.status === "dead") {
	reportFailure(schedule.lastError);
}
```

Good fits include:

- showing a user the next planned action;
- reconciling a workflow record with its schedule;
- checking retry progress after a downstream incident;
- determining whether a stored schedule reference has become stale.

A completed one-shot returns `undefined` because successful one-shot rows are deleted rather than retained as history.

### `list()` — bounded-object inspection and reconciliation

Use `list()` to inspect the schedule set owned by one Durable Object.

```ts
const deadWebhooks = alarm.list({
	callback: "deliverWebhook",
	status: "dead",
});

const dueSoon = alarm.list({
	status: "pending",
	runAt: { to: Date.now() + 60_000 },
});
```

Good fits include:

- an operational view of dead schedules;
- auditing rows before renaming a callback;
- reconciling schedules after a domain-state repair;
- inspecting a time window during a test.

Results are ordered by `runAt`. There is no pagination or limit, so this interface assumes each Durable Object owns a bounded number of rows.

### `cancel()` — revoke future intent

Use `cancel()` when the domain action that justified a schedule is no longer valid.

```ts
const cancelled = await alarm.cancel(scheduleId);
```

Good fits include:

- cancelling an expiry after a checkout completes;
- stopping settlement polling after a terminal webhook arrives;
- revoking a scheduled publication;
- removing a cron schedule when an integration is disabled.

The boolean distinguishes a successful deletion from an already absent row, making repeated cancellation naturally idempotent.

Cancellation cannot retract a callback that has already started or undo an external side effect. Alarm execution may overlap with other requests to the same object at asynchronous boundaries.

### `handler()` — bridge from physical wake-up to logical work

Consumers do not call `handler()` as a scheduling API. The Durable Object forwards its platform alarm handler to the scheduler:

```ts
alarm(): Promise<void> {
	return this.#alarm.handler();
}
```

One pass selects at most 50 pending rows whose `runAt` is due, processes them sequentially, persists each outcome, then rearms the physical alarm.

Sequential processing avoids concurrent callback execution inside one pass, but a slow callback delays later due rows for the same Durable Object. Partitioning schedules across meaningful Durable Object IDs is the scaling mechanism.

### Callback retry context — idempotent external effects

The callback context is most valuable when work crosses a durability boundary:

```ts
async function chargeAccount(
	payload: { accountId: string; amount: number },
	context: ScheduleContext,
) {
	await billing.charge(payload, {
		idempotencyKey: context.idempotencyKey,
	});
}
```

`attempt` starts at one and increments for application retries. `scheduledFor` identifies the logical occurrence, while `scheduleId` identifies the durable cron or one-shot schedule.

## Execution and failure behavior

| Event | Persisted result |
| --- | --- |
| New schedule | `pending`, `attempts = 0`, `scheduledFor = runAt` |
| Callback succeeds for a one-shot | Row deleted |
| Callback succeeds for cron | Next occurrence stored; attempts and error reset |
| Callback fails below the limit | Same occurrence retained; `runAt` moved to retry time |
| Callback reaches the limit | Row retained as `dead`; excluded from alarm calculation |

The default maximum is three total callback attempts. With the current formula, failures after attempts one and two are retried after approximately two and four seconds. A third failure marks the row dead. Although the formula is capped at 60 seconds, the default attempt limit never reaches that cap.

Callbacks run sequentially in ascending `runAt` order. Ordering between rows with the same `runAt` is unspecified because the query has no secondary sort key.

An application callback error is contained and persisted, so it does not trigger Cloudflare's native alarm retry. A due-row query failure, retry-persistence failure, final rearm failure, or object reset can instead escape the scheduler and cause the platform to replay the alarm handler. Durable Object alarms are guaranteed at least once and currently receive a bounded platform retry sequence when the handler throws. See [Alarms: handler behavior](https://developers.cloudflare.com/durable-objects/api/alarms/#alarm).

## Payload persistence

Payloads cross eviction through JSON, not structured clone:

- `undefined` is represented by SQL `NULL`;
- explicit `null` is stored as the JSON string `"null"`;
- `Date` becomes a string;
- object properties with `undefined` are omitted;
- `BigInt`, circular structures, functions, and symbols are not reliable payload values.

Serialization happens before insertion. A payload for which `JSON.stringify()` returns `undefined`, or which causes it to throw, does not create a schedule.

## HTTP test harness

The default Worker only selects `MY_DURABLE_OBJECT.getByName("test")` and forwards the request. The Durable Object owns request dispatch so the harness does not need a public RPC passthrough for every scheduler operation. This makes the experiment easy to inspect while concentrating all test traffic in one Durable Object.

Example request:

```sh
curl -X POST http://localhost:8787 \
  -H 'content-type: application/json' \
  -d '{"action":"scheduleAfter","delayMs":1000,"payload":{"message":"hello"}}'
```

Supported actions:

| Action | Relevant input | Response |
| --- | --- | --- |
| `scheduleAfter` | `delayMs`, `payload` | Created `Schedule` |
| `scheduleAt` | `timestamp`, `payload` | Created `Schedule` |
| `scheduleCron` | `expression`, `payload` | Created or deduplicated `Schedule` |
| `get` | `id` | `Schedule`, or HTTP 404 |
| `list` | optional `criteria` | Ordered `Schedule[]` |
| `cancel` | `id` | `{ "cancelled": boolean }` |

The JSON body is asserted as `ScheduleRequest`; there is no runtime validation, authentication, or authorization. The typed callback guarantees apply to TypeScript callers of `DurableScheduler`, not to arbitrary HTTP input.

## Deliberate experiment boundaries

The current primitive does not provide:

- schema versioning or migrations;
- local-time or IANA-timezone cron rules;
- configurable retry policies;
- callback timeouts or hung-job detection;
- update, pause, resume, or dead-row replay operations;
- completed-schedule history;
- canonical payload hashing for deduplication;
- list pagination;
- per-callback concurrency policies;
- a production partitioning strategy;
- runtime validation at the HTTP boundary.

Those features should be added only when the experiment needs to answer a corresponding design question. They are not prerequisites for validating the central primitive: durable logical rows projected onto one self-rearming alarm.
