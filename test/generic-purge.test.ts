import { expect, it } from "vitest";

import { Watchdog, type WatchdogQueuedJob } from "../src/index.js";
import { createSqliteOwnerFixture } from "./sqlite-owner.fixture.js";

const job = (
  jobId: string,
  queue: string,
  priority: number,
): WatchdogQueuedJob => ({
  jobId,
  queue,
  lane: "arbitrary-lane",
  priority,
  payload: { opaque: jobId },
  state: "queued",
});

it("purges only queued jobs in the requested queue", async () => {
  const fixture = createSqliteOwnerFixture();
  let startRunning!: () => void;
  let releaseRunning!: () => void;
  const runningStarted = new Promise<void>((resolve) => {
    startRunning = resolve;
  });
  const runningReleased = new Promise<void>((resolve) => {
    releaseRunning = resolve;
  });
  try {
    const settled = job("job-queue-a-settled", "queue-a", 1);
    const settlingRuntime = await Watchdog.make({
      owner: fixture.owner,
      execute: () => Promise.resolve({ kind: "completed" as const }),
      isAlive: () => Promise.resolve(true),
      wake: { recompute: () => Promise.resolve() },
    });
    await settlingRuntime.ingest(settled);
    await settlingRuntime.tick();

    const running = job("job-queue-a-running", "queue-a", 2);
    const runtime = await Watchdog.make({
      owner: fixture.owner,
      execute: async () => {
        startRunning();
        await runningReleased;
        return { kind: "completed" as const };
      },
      isAlive: () => Promise.resolve(true),
      wake: { recompute: () => Promise.resolve() },
    });
    await runtime.ingest(running);
    const runningTick = runtime.tick();
    await runningStarted;

    const queuedA = job("job-queue-a-queued", "queue-a", 3);
    const queuedB = job("job-queue-b-queued", "queue-b", 4);
    await runtime.ingest(queuedA);
    await runtime.ingest(queuedB);
    await runtime.purge("queue-a");

    await expect(runtime.read(queuedA.jobId)).resolves.toEqual({
      status: "missing",
    });
    await expect(runtime.read(queuedB.jobId)).resolves.toMatchObject({
      status: "found",
      job: queuedB,
    });
    await expect(runtime.read(running.jobId)).resolves.toMatchObject({
      status: "found",
      job: { ...running, state: "running" },
    });
    await expect(runtime.read(settled.jobId)).resolves.toMatchObject({
      status: "found",
      job: { ...settled, state: "settled", outcome: "completed" },
    });

    releaseRunning();
    await runningTick;
  } finally {
    releaseRunning();
    fixture.close();
  }
});
