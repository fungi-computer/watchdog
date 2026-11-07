import * as Effect from "effect/Effect";
import { afterEach, expect, it, vi } from "vitest";
import { makeRuntime } from "../src/effect.js";
import { createSqliteOwnerFixture } from "./sqlite-owner.fixture.js";

afterEach(() => vi.restoreAllMocks());

const queued = (jobId: string, queue: string, notBefore = 0) => ({
  jobId,
  queue,
  notBefore,
  lane: "test",
  priority: 0,
  payload: null,
  state: "queued" as const,
});

it("retains a sleeping deadline and dispatches only once it is due", async () => {
  const fixture = createSqliteOwnerFixture();
  const now = vi.spyOn(Date, "now").mockReturnValue(1_000);
  const dispatched: string[] = [];
  try {
    const runtime = await Effect.runPromise(
      makeRuntime({
        owner: fixture.owner,
        isAlive: () => Effect.succeed(false),
        executor: {
          execute: (job) =>
            Effect.sync(() => {
              dispatched.push(job.jobId);
              return { kind: "completed" as const };
            }),
        },
        wake: { recompute: () => Effect.void },
      }),
    );
    await Effect.runPromise(runtime.ingest(queued("sleep", "queue", 2_000)));
    expect(await Effect.runPromise(runtime.wakeState(1_000))).toEqual({
      pending: false,
      nextWakeAt: 2_000,
    });
    expect(await Effect.runPromise(runtime.tick())).toEqual({
      status: "no_ready",
    });
    expect(dispatched).toEqual([]);
    now.mockReturnValue(2_000);
    expect(await Effect.runPromise(runtime.wakeState(2_000))).toEqual({
      pending: true,
      nextWakeAt: null,
    });
    expect(await Effect.runPromise(runtime.tick())).toMatchObject({
      status: "settled",
      job: { jobId: "sleep", outcome: "completed" },
    });
    expect(dispatched).toEqual(["sleep"]);
    expect(await Effect.runPromise(runtime.wakeState(2_000))).toEqual({
      pending: false,
      nextWakeAt: null,
    });
  } finally {
    fixture.close();
  }
});

it("keeps a sleeping queue head ahead of its tail while another queue proceeds", async () => {
  const fixture = createSqliteOwnerFixture();
  vi.spyOn(Date, "now").mockReturnValue(1_000);
  const dispatched: string[] = [];
  try {
    const runtime = await Effect.runPromise(
      makeRuntime({
        owner: fixture.owner,
        isAlive: () => Effect.succeed(false),
        executor: {
          execute: (job) =>
            Effect.sync(() => {
              dispatched.push(job.jobId);
              return { kind: "completed" as const };
            }),
        },
        wake: { recompute: () => Effect.void },
      }),
    );
    for (const job of [
      queued("head", "ordered", 2_000),
      queued("tail", "ordered"),
      queued("other", "independent"),
    ])
      await Effect.runPromise(runtime.ingest(job));
    expect(await Effect.runPromise(runtime.tick())).toMatchObject({
      job: { jobId: "other" },
    });
    expect(await Effect.runPromise(runtime.tick())).toEqual({
      status: "no_ready",
    });
    expect(await Effect.runPromise(runtime.wakeState(1_000))).toEqual({
      pending: false,
      nextWakeAt: 2_000,
    });
    expect(dispatched).toEqual(["other"]);
    await Effect.runPromise(
      runtime.cancel({ jobId: "head", cancellationId: "cancel-sleep" }),
    );
    expect(await Effect.runPromise(runtime.wakeState(1_000))).toEqual({
      pending: true,
      nextWakeAt: null,
    });
    expect(await Effect.runPromise(runtime.tick())).toMatchObject({
      job: { jobId: "head", outcome: "cancelled" },
    });
    expect(await Effect.runPromise(runtime.tick())).toMatchObject({
      job: { jobId: "tail", outcome: "completed" },
    });
    expect(dispatched).toEqual(["other", "tail"]);
  } finally {
    fixture.close();
  }
});
