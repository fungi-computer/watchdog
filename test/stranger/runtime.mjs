import { fromNodeSqlite } from "@fungi.computer/watchdog/node-sqlite";
import { createD1Watchdog } from "@fungi.computer/watchdog/d1";
import { createPostgresWatchdog } from "@fungi.computer/watchdog/postgres";
import { DatabaseSync } from "node:sqlite";
import assert from "node:assert/strict";

import { Watchdog } from "@fungi.computer/watchdog";
import { makeRuntime } from "@fungi.computer/watchdog/effect";
import * as Effect from "effect/Effect";

const database = new DatabaseSync(":memory:");
const owner = fromNodeSqlite(database);
const job = (suffix) => ({
  jobId: `stranger-job-${suffix}`,
  queue: "stranger-queue",
  lane: "stranger-lane",
  priority: 0,
  payload: { opaque: true },
  state: "queued",
});

try {
  if (owner.sql.exec("SELECT 1 AS ready").one().ready !== 1)
    throw new Error("Native cursor returned the wrong row");
  const promiseRuntime = await Watchdog.make({
    owner,
    execute: () => Promise.resolve({ kind: "completed" }),
    isAlive: () => Promise.resolve(true),
    wake: { recompute: () => Promise.resolve() },
  });
  const promiseJob = job("promise");
  await promiseRuntime.ingest(promiseJob);
  const promiseTick = await promiseRuntime.tick();

  const effectRuntime = await Effect.runPromise(
    makeRuntime({
      owner,
      executor: {
        execute: () => Effect.succeed({ kind: "completed" }),
      },
      isAlive: () => Effect.succeed(true),
      wake: { recompute: () => Effect.void },
    }),
  );
  const effectJob = job("effect");
  await Effect.runPromise(effectRuntime.ingest(effectJob));
  const effectTick = await Effect.runPromise(effectRuntime.tick());

  if (promiseTick.status !== "settled" || effectTick.status !== "settled")
    throw new Error("Watchdog stranger runtime returned the wrong result");

  const lane = `member:${"m".repeat(256)}`;
  const concurrent = await Watchdog.make({
    owner,
    concurrentLane: lane,
    execute: () => Promise.resolve({ kind: "completed" }),
    isAlive: () => Promise.resolve(true),
    wake: { recompute: () => Promise.resolve() },
  });
  await concurrent.ingest({ ...job("member-lane"), lane });
  assert.deepEqual(await concurrent.tick(), { status: "no_ready" });
  assert.equal((await concurrent.tickConcurrent()).job.lane, lane);
  await concurrent.ingest({ ...job("refused-lane"), lane });
  assert.deepEqual(
    await concurrent.failLane({ lane, reason: "access_changed" }),
    { settled: 1, more: false },
  );
  assert.deepEqual(
    await concurrent.failLane({ lane, reason: "access_changed" }),
    { settled: 0, more: false },
  );
  assert.equal(
    (await concurrent.read(job("refused-lane").jobId)).job.failureReason,
    "access_changed",
  );
} finally {
  database.close();
}

export const nativeConstructors = { createD1Watchdog, createPostgresWatchdog };
