import * as Effect from "effect/Effect";
import { expect, it } from "vitest";

import {
  makeRuntime,
  type WatchdogCancellationIntent,
  type WatchdogQueuedJob,
  type WatchdogSettledJob,
} from "../src/effect.js";
import { createSqliteOwnerFixture } from "./sqlite-owner.fixture.js";

const queuedJob = {
  jobId: "job-queued-cancel-1",
  queue: "work",
  lane: "maintenance",
  priority: 0,
  payload: { operation: "opaque" },
  state: "queued",
} as const satisfies WatchdogQueuedJob;

const settledJob = {
  ...queuedJob,
  state: "settled",
  outcome: "cancelled",
} as const satisfies WatchdogSettledJob;

const cancellation = {
  jobId: queuedJob.jobId,
  cancellationId: "cancellation-queued-1",
} as const satisfies WatchdogCancellationIntent;

it("recomputes wake after settling a queued cancellation", async () => {
  const fixture = createSqliteOwnerFixture();
  const trace: string[] = [];
  try {
    const runtime = await Effect.runPromise(
      makeRuntime({
        owner: fixture.owner,
        executor: {
          execute: () => {
            throw new Error("executor must not run for a cancelled job");
          },
        },
        wake: {
          recompute: () => Effect.sync(() => trace.push("wake")),
        },
        isAlive: () => Effect.succeed(true),
      }),
    );
    await Effect.runPromise(runtime.ingest(queuedJob));
    await Effect.runPromise(runtime.cancel(cancellation));

    const result = await Effect.runPromise(runtime.tick());

    expect(result).toEqual({
      status: "settled",
      job: settledJob,
    });
    expect(trace).toEqual(["wake", "wake"]);
  } finally {
    fixture.close();
  }
});
