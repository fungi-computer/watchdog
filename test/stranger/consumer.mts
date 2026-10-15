import { DatabaseSync } from "node:sqlite";
import {
  fromNodeSqlite,
  type NodeSqliteOwner,
  type NodeSqliteCursor,
} from "@fungi.computer/watchdog/node-sqlite";
import { createD1Watchdog } from "@fungi.computer/watchdog/d1";
import { createPostgresWatchdog } from "@fungi.computer/watchdog/postgres";
import {
  Watchdog,
  type WatchdogQueuedJob,
  type WatchdogLaneFailureResult,
  type WatchdogLaneFailure,
  type WatchdogSqliteOwner,
} from "@fungi.computer/watchdog";
import {
  makeRuntime,
  type WatchdogRuntimeEffect,
} from "@fungi.computer/watchdog/effect";
import * as Effect from "effect/Effect";

const job = {
  jobId: "typed-stranger-job",
  queue: "typed-stranger-queue",
  lane: "typed-stranger-lane",
  priority: 0,
  payload: { opaque: true },
  state: "queued",
} as const satisfies WatchdogQueuedJob;

const database = new DatabaseSync(":memory:");
const nativeOwner: NodeSqliteOwner = fromNodeSqlite(database);
const cursor: NodeSqliteCursor = nativeOwner.sql.exec("SELECT 1 AS ready");
if (cursor.one().ready !== 1)
  throw new Error("Native cursor returned the wrong row");
const promiseOwner: WatchdogSqliteOwner = nativeOwner;
declare const effectOwner: WatchdogSqliteOwner;

const promiseRuntime = await Watchdog.make({
  owner: promiseOwner,
  execute: () => Promise.resolve({ kind: "completed" as const }),
  isAlive: () => Promise.resolve(true),
  wake: { recompute: () => Promise.resolve() },
});

const effectRuntime: WatchdogRuntimeEffect = await Effect.runPromise(
  makeRuntime({
    owner: effectOwner,
    executor: { execute: () => Effect.succeed({ kind: "completed" as const }) },
    isAlive: () => Effect.succeed(true),
    wake: { recompute: () => Effect.void },
  }),
);

promiseRuntime.transactional.enqueueAcceptedJob(job);
effectRuntime.transactional.enqueueAcceptedJob(job);
const promiseRead = await promiseRuntime.read(job.jobId);
const effectRead = await Effect.runPromise(effectRuntime.read(job.jobId));

if (
  promiseRead.status !== "found" ||
  promiseRead.job.jobId !== job.jobId ||
  effectRead.status !== "found" ||
  effectRead.job.jobId !== job.jobId
)
  throw new Error("Watchdog stranger types returned the wrong result");

const refusal = {
  lane: job.lane,
  reason: "access_changed",
} satisfies WatchdogLaneFailure;
const promiseFailure: WatchdogLaneFailureResult =
  await promiseRuntime.failLane(refusal);
const effectFailure: WatchdogLaneFailureResult = await Effect.runPromise(
  effectRuntime.failLane({ lane: job.lane, reason: "access_changed" }),
);
if (promiseFailure.more || effectFailure.more)
  throw new Error("Lane failure did not settle its batch");

export const nativeConstructors = { createD1Watchdog, createPostgresWatchdog };

database.close();
