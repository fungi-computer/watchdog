import { expect, it } from "vitest";

import {
  Watchdog,
  type WatchdogQueuedJob,
  type WatchdogRuntime,
} from "../src/index.js";
import { createSqliteOwnerFixture } from "./sqlite-owner.fixture.js";

const queuedJob = {
  jobId: "job-public-lifecycle-1",
  queue: "arbitrary-queue",
  lane: "opaque-lane",
  priority: 3,
  payload: { kind: "opaque", values: ["alpha", { nested: true }] },
  recovery: { maxRecoveries: 1 },
  state: "queued",
} as const satisfies WatchdogQueuedJob;

it("reconstructs and settles opaque work through the public Promise lifecycle", async () => {
  const fixture = createSqliteOwnerFixture();
  const trace: string[] = [];
  let executions = 0;
  try {
    const acceptingRuntime = await Watchdog.make({
      owner: fixture.owner,
      execute: () => Promise.resolve({ kind: "completed" as const }),
      isAlive: () => Promise.resolve(true),
      wake: { recompute: () => Promise.resolve() },
    });
    await expect(acceptingRuntime.hasPendingWork()).resolves.toBe(false);
    await expect(acceptingRuntime.ingest(queuedJob)).resolves.toEqual({
      status: "queued",
    });

    await expect(acceptingRuntime.hasPendingWork()).resolves.toBe(true);
    const reconstructedRuntime: WatchdogRuntime = await Watchdog.make({
      owner: fixture.owner,
      execute: (job) => {
        executions += 1;
        expect(job.payload).toEqual(queuedJob.payload);
        trace.push("execute");
        return reconstructedRuntime
          .hasPendingWork()
          .then((pending) => {
            expect(pending).toBe(true);
            return { kind: "completed" as const };
          })
          .finally(() => {
            trace.push("finalize");
          });
      },
      isAlive: () => Promise.resolve(true),
      wake: {
        recompute: () => {
          trace.push("wake");
          return Promise.resolve();
        },
      },
    });

    await expect(reconstructedRuntime.read(queuedJob.jobId)).resolves.toEqual({
      status: "found",
      job: queuedJob,
      cancellation: { status: "absent" },
    });
    const settled = await reconstructedRuntime.tick();
    expect(settled).toMatchObject({
      status: "settled",
      job: { ...queuedJob, state: "settled", outcome: "completed" },
    });
    if (settled.status !== "settled")
      throw new Error("Expected the opaque job to settle");
    expect(trace).toEqual(["execute", "finalize", "wake"]);
    expect(executions).toBe(1);
    await expect(reconstructedRuntime.hasPendingWork()).resolves.toBe(false);

    const observedByFreshRuntime = await Watchdog.make({
      owner: fixture.owner,
      execute: () => {
        throw new Error("settled work must not execute again");
      },
      isAlive: () => Promise.resolve(true),
      wake: { recompute: () => Promise.resolve() },
    });
    await expect(observedByFreshRuntime.read(queuedJob.jobId)).resolves.toEqual(
      {
        status: "found",
        job: settled.job,
        cancellation: { status: "absent" },
      },
    );
    await expect(observedByFreshRuntime.tick()).resolves.toEqual({
      status: "no_ready",
    });
  } finally {
    fixture.close();
  }
});
