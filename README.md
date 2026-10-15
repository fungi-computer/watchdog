# Watchdog (`@fungi.computer/watchdog`)

Watchdog is a small durable job engine. You hand it jobs, it stores them in your
SQLite database, and each `tick()` claims one ready job, runs your executor and
records exactly one terminal outcome. If the process dies mid-job, the next tick
asks your liveness probe what happened and recovers the work.

Watchdog never looks inside a job. The payload, queue name and lane are opaque
values you choose. Your code decides what a job means and how to run it.
Watchdog owns the lifecycle: queued, running, settled. Claims, claim tokens and
the SQL that moves rows between states stay private to the package.

Use it when you need work to survive a restart, run once per claim, and settle
with an honest outcome, without writing your own state machine. It runs on any
synchronous SQLite owner, such as a Cloudflare Durable Object's storage or
`node:sqlite`, and on shared D1 or PostgreSQL databases.

## Install or set up

```sh
npm install @fungi.computer/watchdog
```

Watchdog is ESM-only. The SQLite entrypoint runs anywhere you have a synchronous
SQLite handle, such as Cloudflare Durable Object storage or `node:sqlite` in
Node 24. It depends on `effect`, `pg` and `@cloudflare/workers-types`, which npm
installs for you.

Entrypoints:

- `@fungi.computer/watchdog`: the Promise API, `Watchdog.make`, and the public
  job types and schemas.
- `@fungi.computer/watchdog/effect`: the same runtime as Effect values, via
  `makeRuntime`.
- `@fungi.computer/watchdog/node-sqlite`: `fromNodeSqlite(database)` for a
  caller-owned Node SQLite database. It returns `NodeSqliteOwner`; its
  `NodeSqliteCursor` provides `toArray()` and `one()`, which returns exactly one
  decoded row and throws for zero or multiple rows.
- `@fungi.computer/watchdog/d1`: a runtime over a migrated Cloudflare D1
  database.
- `@fungi.computer/watchdog/postgres`: a runtime over a migrated PostgreSQL
  database.

## Quick start

This runs one job through an in-memory `node:sqlite` database. The `owner`
object is the whole host contract for the SQLite entrypoint: run a statement,
and run a synchronous transaction.

```ts
import { DatabaseSync } from "node:sqlite";
import { fromNodeSqlite } from "@fungi.computer/watchdog/node-sqlite";
import { Watchdog } from "@fungi.computer/watchdog";

const database = new DatabaseSync(":memory:");
const owner = fromNodeSqlite(database);

const watchdog = await Watchdog.make({
  owner,
  execute: async (job, signal) => {
    if (signal.aborted) return { kind: "cancelled" };
    console.log(`sending ${job.jobId}`, job.payload);
    return { kind: "completed" };
  },
  // This process owns every claim, so a running job is alive while it runs.
  isAlive: async () => true,
  // Arm your real timer or alarm here. Ticking by hand is enough for a demo.
  wake: { recompute: async () => {} },
});

await watchdog.ingest({
  jobId: "welcome-email-1",
  queue: "email",
  lane: "default",
  priority: 0,
  payload: { to: "ada@example.com" },
  state: "queued",
});

const tick = await watchdog.tick();
console.log(tick.status); // "settled"

const read = await watchdog.read("welcome-email-1");
if (read.status === "found" && read.job.state === "settled")
  console.log(read.job.outcome); // "completed"

database.close();
```

Run `tick()` again and it returns `{ status: "no_ready" }`. Ingest the same job
again and it returns `{ status: "duplicate" }`.

In a Durable Object, `ctx.storage` is already the SQLite owner.

## Core concepts

**Jobs.** A job has a `jobId`, a `queue`, a `lane`, an integer `priority`, an
opaque JSON `payload` and an optional `recovery: { maxRecoveries }`. IDs and
queue names are 1 to 256 characters; lanes are 1 to 512 characters. Owner-local
construction can reserve one lane for concurrent delivery; otherwise the lane is
stored without a scheduling policy. A job moves through three states:

- `queued`: accepted and waiting.
- `running`: claimed by one tick and handed to your executor.
- `settled`: finished, with an `outcome` of `completed`, `failed`, `cancelled`,
  `outcome_unknown` or `interrupted`, and an optional `failureReason`.

**Ingest is idempotent.** `ingest(job)` returns `queued` for a new ID and
`duplicate` for an identical replay. A replay with the same ID and different
queue, lane, priority, payload or recovery policy fails.

**One tick, one job.** `tick()` picks a ready queue head with the highest
priority, oldest first, across eligible queues. It claims that job, runs your
executor, and settles the exact claim it took. A second `tick()` on the same
runtime while one is in flight returns `{ status: "busy" }` instead of running
in parallel. Your executor returns one of `completed`, `failed` (with an
optional short `reason`), `cancelled` or `outcome_unknown`. If it throws or is
interrupted, Watchdog settles the job as `failed` with its own reason code, such
as `execution_failed` or `execution_interrupted`.

**Host ports.** You supply four things:

- `owner`: the SQLite capability shown in the quick start.
- `execute(job, signal)`: runs one claimed job. Honour the `AbortSignal`.
- `isAlive(job)`: answers whether a running job's execution still exists.
- `wake.recompute()`: arms your next timer or alarm after durable state changes.

**Disappearance recovery.** Each tick first checks the oldest running claim. If
`isAlive` returns `false`, the execution is gone. A job with recovery budget
left goes back to `queued`. Otherwise it settles as `interrupted`. If `isAlive`
throws, Watchdog does not guess. The claim stays as it is, and an otherwise idle
tick reports `{ status: "liveness_unknown", jobId, reason }`.

**Cancellation.** `cancel({ jobId, cancellationId })` records a durable
cancellation. A queued job settles as `cancelled` on its next tick. A job the
current runtime is executing is interrupted and settles as `cancelled`.
Replaying the same `cancellationId` returns `replayed`. A different one returns
`conflict`. Pass `onlyIfQueued: true` to refuse cancellation once a job has
started.

**Lane refusal.** `failLane({ lane, reason })` terminally settles queued and
running work in that lane as `failed`, retaining each job and its bounded owner
reason code. It handles at most 256 jobs per call, running jobs first, and
returns `{ settled, more }`. Repeat only while `more` is true; 256 is a
transition batch, not a cap on accepted work. Already settled history and other
lanes stay intact. Once exhausted, another call returns
`{ settled: 0, more: false }` and fires no projector. A later enqueue is new
work, not a permanent refusal marker.

On owner-local SQLite, every changed job invokes `project` in the same
transaction as its settlement. A projector throw rolls back the entire batch and
all owner projection writes. A locally running executor is interrupted after
durable refusal; its tick still awaits cleanup and reports the committed failure
without projecting it again. Replaced durable claims cannot overwrite that
failure. A refusal does not undo provider effects already accepted before it.

The shared D1/PostgreSQL adapters expose the same bounded settlement method and
fence leases in one atomic SQL mutation. They do not offer the synchronous
SQLite-only `project` callback. Hosts still own authority and must stop revoked
execution; Watchdog owns durable job settlement, not App admission.

**Owner transactions.** `runtime.transactional` is a synchronous projection for
code already inside your own SQLite transaction. `enqueueAcceptedJob` and
`recordCancellationIntent` use the same mutation as `ingest` and `cancel`, so
your domain write and the job commit or roll back together. `readQueueHead` and
`readLatestSettledJob` read queue state inside that transaction.

**Queue projection.** Pass `project` to `Watchdog.make` to receive a
`{ queue, head, settled? }` transition inside the same transaction as every
ingest, claim, cancellation, settlement, lane refusal, recovery and purge. Use
it to keep a per-queue status row in step with Watchdog. A throw from `project`
fails the transition.

**Purge.** `purge(queue)` deletes every queued job in one queue. Running and
settled jobs stay.

## Concurrent owner-local delivery

Owner-local construction may opt into one `concurrentLane`. Only the configured
runtime type adds `tickConcurrent()`: ordinary ticks exclude that lane and the
second tick selects only it. Both slots share one store and transactional
projection, with separate busy guards and active executions. Cancellation
signals the slot owning the job; recovery inspects only the idle slot's
partition. Both slots require the queue head across lanes, ordered by running
first, then priority descending and enqueue sequence ascending. Cancelled queued
jobs can settle past a busy queue without executing.

The Effect runtime's `readSettled(limit, afterSequence)` exposes the serial
partition in enqueue order. A prefix fence withholds later settlements until all
earlier serial jobs settle or are purged. Concurrent receipts never consume a
page slot; they remain readable by job ID. `hasPendingWork` still covers both
partitions. These owner-local operations require no stored schema change and do
not change the D1 or PostgreSQL adapters.

## Shared D1 and PostgreSQL storage

`@fungi.computer/watchdog/d1` exports `createD1Watchdog` and `watchdogD1Schema`.
`@fungi.computer/watchdog/postgres` exports `createPostgresWatchdog` and
`watchdogPostgresSchema`. Apply the package-owned schema through your migration
tool before you open a runtime. Opening a runtime never migrates a database.

These runtimes have the same `ingest`, `read`, `readQueueHead`, `tick`,
`cancel`, `failLane`, `purge` and `hasPendingWork` operations. They take
`execute`, `wake` and an optional `now` clock instead of an owner and liveness
probe. Watchdog answers liveness itself from the claim's lease.

D1 `enqueue(job)` returns a prepared statement for the producer's native batch.
`enqueueFromSelect({ text, values })` accepts a trusted native SQLite
`?NNN`-bound SELECT of `job_id`, `queue_name`, `lane`, `priority`,
`payload_json` and nullable `recovery_max`. Watchdog supplies its INSERT and
conflict checks. This lets a job depend on facts conditionally inserted earlier
in the same batch, without a JavaScript read in between. PostgreSQL
`enqueue(client, job)` uses the caller's checked-out transaction client. The
caller owns BEGIN, COMMIT and ROLLBACK. An identical replay does nothing. A
conflicting identity or cargo fails the whole native transaction.

Both adapters use the same lifecycle engine as the SQLite entrypoint. Claims
stay private, with two-minute leases, ten-second renewal and bounded renewal
I/O. Execution must honour its AbortSignal. Renewal and settlement reject
expired or replaced claims. A failed liveness probe never declares an execution
dead. Database time checks stop an in-flight renewal from resurrecting an
expired claim. The optional `now` clock replaces database time for deterministic
tests. Production hosts should use the default. A durable host wake still drives
ticks and disappearance recovery after a process dies. Provider effects remain
at-least-once.

`purgeD1Queues` and `purgePostgresQueues` return parameterized erasure
statements for queues selected by trusted producer SQL. The caller joins them to
its native batch or transaction, and Watchdog stays the only owner of job
deletion SQL. These helpers remove every attempt in the selected queues and
expose no claim or settlement controls. [Carrier](../carrier/README.md) composes
them with envelope erasure.

## API reference

The generated reference is in [docs/api](docs/api/README.md). It covers four
modules:

- `@fungi.computer/watchdog`: `Watchdog.make`, `WatchdogRuntime`,
  `WatchdogOptions`, `WatchdogError`, and the job, result and SQLite owner types
  with their Effect schemas.
- `@fungi.computer/watchdog/effect`: `makeRuntime`, `WatchdogRuntimeEffect` and
  `EffectWatchdogOptions`. The Effect runtime also has
  `readSettled(limit, afterSequence?)` for paging through settled jobs.
- `@fungi.computer/watchdog/node-sqlite`: `fromNodeSqlite(database)` for a
  caller-owned Node SQLite database. It returns `NodeSqliteOwner`; its
  `NodeSqliteCursor` provides `toArray()` and `one()`, which returns exactly one
  decoded row and throws for zero or multiple rows.
- `@fungi.computer/watchdog/d1`: `createD1Watchdog`, `prepareD1Admission`,
  `watchdogD1Schema` and `purgeD1Queues`.
- `@fungi.computer/watchdog/postgres`: `createPostgresWatchdog`,
  `watchdogPostgresSchema` and `purgePostgresQueues`.

## Examples and checks

The Effect entrypoint runs the same engine without the Promise wrapper:

```ts
import * as Effect from "effect/Effect";
import { makeRuntime } from "@fungi.computer/watchdog/effect";

const runtime = await Effect.runPromise(
  makeRuntime({
    owner, // the same owner as in the quick start
    executor: { execute: () => Effect.succeed({ kind: "completed" as const }) },
    isAlive: () => Effect.succeed(true),
    wake: { recompute: () => Effect.void },
  }),
);
const result = await Effect.runPromise(runtime.tick());
```

Inside your own transaction, enqueue work with the domain write it belongs to:

```ts
owner.transactionSync(() => {
  owner.sql.exec("INSERT INTO orders (id) VALUES (?)", "order-1");
  watchdog.transactional.enqueueAcceptedJob({
    jobId: "charge-order-1",
    queue: "order-1",
    lane: "default",
    priority: 0,
    payload: { orderId: "order-1" },
    state: "queued",
  });
});
```

The package tests run the engine on a real in-memory `node:sqlite` owner. They
cover the tick lifecycle, cancellation of queued and running jobs, disappearance
recovery, failure reasons, purge, queue projection, the transactional projection
and shared-storage leases. From the package directory:

```sh
pnpm test
```

## Limits and errors

Every failure is a `WatchdogError` with a closed `cause`:

- `store_unavailable`: the store threw, or an ingest replay conflicted with the
  stored job.
- `store_invalid`: the lane-refusal input failed its strict lane/reason decoder.
  Other store faults remain `store_unavailable`.
- `claim_unavailable`: a claim could not be taken or renewed.
- `execution_failed`: your Promise executor threw. `tick()` does not reject. The
  job settles as `failed` with this code as its `failureReason`.
- `settlement_rejected`: the claim was replaced or expired before settlement.
- `wake_unavailable`: `wake.recompute()` threw.
- `liveness_unavailable`: `isAlive` threw.

`underlying` keeps the original thrown value for server-side diagnosis. Do not
send it to clients.

Limits and guarantees:

- One tick runs at most one job. Call `tick()` again, or from your wake, to
  drain a queue.
- Concurrency is per runtime. Open one SQLite runtime per owner. The D1 and
  PostgreSQL runtimes use leases so more than one worker can share a database.
- Delivery is at-least-once. A job recovered after a crash runs again, so make
  executor side effects idempotent.
- `outcome_unknown` is a terminal outcome. Watchdog does not retry it.
- IDs and queue names are 1 to 256 characters. Lanes and `concurrentLane` share
  one 1-to-512-character bound across job admission, storage and construction. A
  canonical 256-character identity needs 263 characters with `member:` or 265
  with `team-key:`; 512 leaves room for composition without hashing or
  truncating the identity. Fresh shared database schemas enforce the same bound.
  `failureReason` is 1 to 200 characters and should be a short code, never
  provider text.

## Contribute and license

Watchdog is MIT licensed. See [LICENSE](./LICENSE).
