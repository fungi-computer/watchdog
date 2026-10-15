import * as Effect from "effect/Effect";
import { expect, it } from "vitest";

import {
  makeRuntime,
  type WatchdogQueuedJob,
  type WatchdogRuntimeEffect,
} from "../src/effect.js";
import { WatchdogError } from "../src/index.js";
import { createSqliteOwnerFixture } from "./sqlite-owner.fixture.js";

const job = (suffix: string): WatchdogQueuedJob => ({
  jobId: `job-tick-${suffix}`,
  queue: "arbitrary-queue",
  lane: "arbitrary-lane",
  priority: 0,
  payload: { opaque: suffix },
  state: "queued",
});

it("settles only after executor finalization and recomputes the next wake", async () => {
  const fixture = createSqliteOwnerFixture();
  const queued = job("finalization");
  const trace: string[] = [];
  let runtime: WatchdogRuntimeEffect;
  try {
    runtime = await Effect.runPromise(
      makeRuntime({
        owner: fixture.owner,
        executor: {
          execute: () =>
            Effect.succeed({ kind: "completed" as const }).pipe(
              Effect.ensuring(Effect.sync(() => trace.push("finalize"))),
            ),
        },
        isAlive: () => Effect.succeed(true),
        wake: {
          recompute: () =>
            runtime.read(queued.jobId).pipe(
              Effect.tap((read) =>
                Effect.sync(() => {
                  expect(read).toMatchObject({
                    status: "found",
                    job: { state: "settled", outcome: "completed" },
                  });
                  trace.push("wake");
                }),
              ),
              Effect.asVoid,
            ),
        },
      }),
    );
    await Effect.runPromise(runtime.ingest(queued));

    await expect(Effect.runPromise(runtime.tick())).resolves.toMatchObject({
      status: "settled",
      job: { state: "settled", outcome: "completed" },
    });
    expect(trace).toEqual(["finalize", "wake"]);
  } finally {
    fixture.close();
  }
});

it("keeps durable settlement after a failed post-settlement wake", async () => {
  const fixture = createSqliteOwnerFixture();
  const queued = job("wake-failure");
  try {
    const runtime = await Effect.runPromise(
      makeRuntime({
        owner: fixture.owner,
        executor: {
          execute: () => Effect.succeed({ kind: "completed" as const }),
        },
        isAlive: () => Effect.succeed(true),
        wake: {
          recompute: () => Effect.fail(new WatchdogError("wake_unavailable")),
        },
      }),
    );
    await Effect.runPromise(runtime.ingest(queued));
    await expect(Effect.runPromise(runtime.tick())).rejects.toThrow(
      "Watchdog wake unavailable",
    );
    await expect(
      Effect.runPromise(runtime.read(queued.jobId)),
    ).resolves.toMatchObject({
      status: "found",
      job: { state: "settled", outcome: "completed" },
    });
  } finally {
    fixture.close();
  }
});
