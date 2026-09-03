# What Happens When an Alarm Schedule Coincides with a Running Alarm?

I was curious what happens when a Durable Object alarm is still running and another alarm is scheduled to run before the first one finishes.

Would the new alarm be ignored? Would it wait until the running alarm finishes? Or would it interrupt the running alarm and take its place?

This is a small repro using only Cloudflare's native `setAlarm()` and `alarm()` APIs. There is no queue, scheduler, retry state, or concurrency control around them.

## Try it

```sh
pnpm install
pnpm dev
```

Start an alarm that runs for four seconds:

```sh
curl -sS localhost:8787 \
  -H 'content-type: application/json' \
  -d '{"delayMs":500,"durationMs":4000,"failure":"never"}' | jq
```

After `alarm.started` appears, schedule another alarm immediately:

```sh
curl -sS localhost:8787 \
  -H 'content-type: application/json' \
  -d '{"delayMs":0}' | jq
```

## What I observed

```text
alarm.started    # first alarm
alarm.set        # second alarm is scheduled while the first is running
alarm.completed  # first alarm
alarm.started    # second alarm
alarm.completed  # second alarm
```

- The request scheduling the second alarm was handled while the first handler was waiting.
- The running alarm was not interrupted.
- The second alarm waited for the first handler to finish; the two handlers never overlapped.
- `getAlarm()` returned `null` during the first handler because that alarm had already been consumed.
- Once the second alarm was set, `getAlarm()` returned its timestamp.
- In my local run, the second handler started about 4 ms after the first one completed.

There is still only one native alarm slot. Calling `setAlarm()` before the existing alarm starts replaces it. Calling it while an alarm is running schedules the next invocation.

## Native failure retry

I also tried throwing once from the raw alarm handler:

```sh
curl -sS localhost:8787 \
  -H 'content-type: application/json' \
  -d '{"delayMs":0,"durationMs":0,"failure":"once"}' | jq
```

The first invocation had `retryCount: 0` and `isRetry: false`. It threw, then ran again about 2.3 seconds later with `retryCount: 1` and `isRetry: true`.

These results match Cloudflare's documented behavior: only one `alarm()` invocation runs at a time per Durable Object, and uncaught failures receive native retries. See [Durable Object Alarms](https://developers.cloudflare.com/durable-objects/api/alarms/) and [Rules of Durable Objects](https://developers.cloudflare.com/durable-objects/best-practices/rules-of-durable-objects/#avoid-race-conditions-with-non-storage-io).

To inspect or clear the currently scheduled alarm:

```sh
curl -sS localhost:8787 | jq
curl -i -X DELETE localhost:8787
```
