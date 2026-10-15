import { expect, it } from "vitest";

import {
  Watchdog,
  WatchdogError,
  type WatchdogQueuedJob,
} from "../src/index.js";
import { createSqliteOwnerFixture } from "./sqlite-owner.fixture.js";

const job = (suffix: string): WatchdogQueuedJob => ({
  jobId: `job-correction-${suffix}`,
  queue: "generic-queue",
  lane: "generic-lane",
  priority: 0,
  payload: { opaque: suffix },
  state: "queued",
});

it.each([
  "m".repeat(256),
  `member:${"m".repeat(256)}`,
  `team-key:${"k".repeat(256)}`,
  "l".repeat(512),
])(
  "preserves lane %s through concurrent execution and durable settlement",
  async (lane) => {
    const fixture = createSqliteOwnerFixture();
    const queued = { ...job("lane-width"), lane };
    let executions = 0;
    try {
      const runtime = await Watchdog.make({
        owner: fixture.owner,
        concurrentLane: lane,
        execute: (running) => {
          expect(running.lane).toBe(lane);
          executions += 1;
          return Promise.resolve({ kind: "completed" as const });
        },
        isAlive: () => Promise.resolve(true),
        wake: { recompute: () => Promise.resolve() },
      });
      await expect(runtime.ingest(queued)).resolves.toEqual({
        status: "queued",
      });
      await expect(runtime.tick()).resolves.toEqual({ status: "no_ready" });
      await expect(runtime.tickConcurrent()).resolves.toMatchObject({
        status: "settled",
        job: { lane, state: "settled", outcome: "completed" },
      });
      await expect(runtime.read(queued.jobId)).resolves.toMatchObject({
        status: "found",
        job: { lane, state: "settled", outcome: "completed" },
      });
      expect(executions).toBe(1);
    } finally {
      fixture.close();
    }
  },
);

it("refuses a concurrent lane above 512 characters before creating a runtime", async () => {
  const fixture = createSqliteOwnerFixture();
  try {
    await expect(
      Watchdog.make({
        owner: fixture.owner,
        concurrentLane: "l".repeat(513),
        execute: () => Promise.resolve({ kind: "completed" as const }),
        isAlive: () => Promise.resolve(true),
        wake: { recompute: () => Promise.resolve() },
      }),
    ).rejects.toBeInstanceOf(WatchdogError);
  } finally {
    fixture.close();
  }
});

it("records queued cancellation before wake and later settles it after wake failure", async () => {
  const fixture = createSqliteOwnerFixture();
  const queued = job("queued-cancel");
  const cancellation = {
    jobId: queued.jobId,
    cancellationId: "cancel-correction-1",
  };
  let failWake = true;
  try {
    const runtime = await Watchdog.make({
      owner: fixture.owner,
      execute: () => {
        throw new Error("cancelled queued work must not execute");
      },
      isAlive: () => Promise.resolve(true),
      wake: {
        recompute: () => {
          if (failWake) return Promise.reject(new Error("wake unavailable"));
          return Promise.resolve();
        },
      },
    });
    await runtime.ingest(queued);
    await expect(runtime.cancel(cancellation)).rejects.toBeInstanceOf(
      WatchdogError,
    );
    await expect(runtime.read(queued.jobId)).resolves.toMatchObject({
      status: "found",
      job: queued,
      cancellation: { status: "present", cancellation },
    });

    failWake = false;
    await expect(runtime.tick()).resolves.toMatchObject({
      status: "settled",
      job: { state: "settled", outcome: "cancelled" },
    });
  } finally {
    fixture.close();
  }
});

it("returns busy for an overlapping public tick without another execution", async () => {
  const fixture = createSqliteOwnerFixture();
  const queued = job("busy");
  let started!: () => void;
  let release!: () => void;
  const executionStarted = new Promise<void>((resolve) => {
    started = resolve;
  });
  const executionReleased = new Promise<void>((resolve) => {
    release = resolve;
  });
  let executions = 0;
  try {
    const runtime = await Watchdog.make({
      owner: fixture.owner,
      execute: async () => {
        executions += 1;
        started();
        await executionReleased;
        return { kind: "completed" as const };
      },
      isAlive: () => Promise.resolve(true),
      wake: { recompute: () => Promise.resolve() },
    });
    await runtime.ingest(queued);
    const first = runtime.tick();
    await executionStarted;

    await expect(runtime.tick()).resolves.toEqual({ status: "busy" });
    expect(executions).toBe(1);
    release();
    await expect(first).resolves.toMatchObject({ status: "settled" });
  } finally {
    release();
    fixture.close();
  }
});

it("settles a rejected executor as failed without recovery or a rerun", async () => {
  const fixture = createSqliteOwnerFixture();
  const queued = job("failed-executor");
  let executions = 0;
  try {
    const runtime = await Watchdog.make({
      owner: fixture.owner,
      execute: () => {
        executions += 1;
        return Promise.reject(new Error("provider failure returned to host"));
      },
      isAlive: () => Promise.resolve(false),
      wake: { recompute: () => Promise.resolve() },
    });
    await runtime.ingest(queued);

    await expect(runtime.tick()).resolves.toMatchObject({
      status: "settled",
      job: { state: "settled", outcome: "failed" },
    });
    await expect(runtime.tick()).resolves.toEqual({ status: "no_ready" });
    expect(executions).toBe(1);
  } finally {
    fixture.close();
  }
});

it("projects one queue head from queued through running and settlement", async () => {
  const fixture = createSqliteOwnerFixture();
  const first = job("queue-head-running");
  const second = job("queue-head-cancelled");
  let resolveStarted: () => void = () => undefined;
  let resolveExecution: () => void = () => undefined;
  const started = new Promise<void>((resolve) => {
    resolveStarted = resolve;
  });
  const execution = new Promise<void>((resolve) => {
    resolveExecution = resolve;
  });
  try {
    const runtime = await Watchdog.make({
      owner: fixture.owner,
      execute: async () => {
        resolveStarted();
        await execution;
        return { kind: "completed" as const };
      },
      isAlive: () => Promise.resolve(true),
      wake: { recompute: () => Promise.resolve() },
    });

    await expect(runtime.readQueueHead(first.queue)).resolves.toEqual({
      state: "idle",
    });
    await runtime.ingest(first);
    await expect(runtime.readQueueHead(first.queue)).resolves.toEqual({
      state: "queued",
      jobId: first.jobId,
      cancellationRequested: false,
    });

    const ticking = runtime.tick();
    await started;
    await runtime.ingest(second);
    await expect(runtime.readQueueHead(first.queue)).resolves.toEqual({
      state: "running",
      jobId: first.jobId,
      cancellationRequested: false,
    });
    resolveExecution();
    await ticking;

    await runtime.cancel({
      jobId: second.jobId,
      cancellationId: "cancel-queue-head",
    });
    await expect(runtime.readQueueHead(second.queue)).resolves.toEqual({
      state: "queued",
      jobId: second.jobId,
      cancellationRequested: true,
    });
    await runtime.tick();
    await expect(runtime.readQueueHead(first.queue)).resolves.toEqual({
      state: "idle",
    });
  } finally {
    resolveExecution();
    fixture.close();
  }
});
