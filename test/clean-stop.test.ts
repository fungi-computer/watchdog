import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import { expect, it } from "vitest";
import { makeRuntime } from "../src/effect.js";
import { createSqliteOwnerFixture } from "./sqlite-owner.fixture.js";

it.each([true, false])(
  "hands off only an execution with proven drain: %s",
  async (drained) => {
    const fixture = createSqliteOwnerFixture();
    let started: () => void = () => undefined;
    const executing = new Promise<void>((resolve) => {
      started = resolve;
    });
    const runtime = await Effect.runPromise(
      makeRuntime({
        owner: fixture.owner,
        executor: {
          execute: () =>
            Effect.sync(started).pipe(Effect.andThen(Effect.never)),
          isDrained: () => Effect.succeed(drained),
        },
        isAlive: () => Effect.succeed(false),
        wake: { recompute: () => Effect.void },
      }),
    );
    await Effect.runPromise(
      runtime.ingest({
        jobId: "stopped",
        queue: "q",
        lane: "test",
        priority: 0,
        payload: null,
        state: "queued",
        recovery: { maxRecoveries: 0 },
      }),
    );
    const running = Effect.runFork(runtime.tick());
    try {
      await executing;
      if (drained) {
        await Effect.runPromise(runtime.close);
        expect(await Effect.runPromise(Fiber.join(running))).toEqual({
          status: "stopped",
        });
      } else {
        await expect(Effect.runPromise(runtime.close)).rejects.toMatchObject({
          cause: "drain_failed",
        });
      }
      expect(await Effect.runPromise(runtime.read("stopped"))).toMatchObject({
        job: { state: drained ? "queued" : "running" },
      });
      expect(
        fixture.owner.sql
          .exec("SELECT recovery_count FROM watchdog_jobs")
          .toArray(),
      ).toEqual([{ recovery_count: 0 }]);
      expect(await Effect.runPromise(runtime.tick())).toEqual({
        status: "stopped",
      });
      const reopened = await Effect.runPromise(
        makeRuntime({
          owner: fixture.owner,
          executor: { execute: () => Effect.succeed({ kind: "completed" }) },
          isAlive: () => Effect.succeed(false),
          wake: { recompute: () => Effect.void },
        }),
      );
      expect(await Effect.runPromise(reopened.tick())).toMatchObject({
        status: "settled",
        job: { outcome: drained ? "completed" : "interrupted" },
      });
    } finally {
      await Effect.runPromise(Fiber.interrupt(running));
      fixture.close();
    }
  },
);

it("retains a claim when interrupted execution cleanup fails", async () => {
  const fixture = createSqliteOwnerFixture();
  let start: () => void = () => undefined;
  const started = new Promise<void>((resolve) => {
    start = resolve;
  });
  const runtime = await Effect.runPromise(
    makeRuntime({
      owner: fixture.owner,
      isAlive: () => Effect.succeed(false),
      wake: { recompute: () => Effect.void },
      executor: {
        execute: () =>
          Effect.sync(start).pipe(
            Effect.andThen(Effect.never),
            Effect.ensuring(Effect.die("cleanup failed")),
          ),
        isDrained: () => Effect.succeed(true),
      },
    }),
  );
  await Effect.runPromise(
    runtime.ingest({
      jobId: "failed-cleanup",
      queue: "q",
      lane: "test",
      priority: 0,
      payload: null,
      state: "queued",
    }),
  );
  const running = Effect.runFork(runtime.tick());
  try {
    await started;
    await expect(Effect.runPromise(runtime.close)).rejects.toMatchObject({
      cause: "drain_failed",
    });
    expect(
      await Effect.runPromise(runtime.read("failed-cleanup")),
    ).toMatchObject({ job: { state: "running" } });
  } finally {
    await Effect.runPromise(Fiber.interrupt(running));
    fixture.close();
  }
});

it("lets durable cancellation win while a stopped claim drains", async () => {
  const fixture = createSqliteOwnerFixture();
  let start: () => void = () => undefined;
  const started = new Promise<void>((resolve) => {
    start = resolve;
  });
  let finalizing: () => void = () => undefined;
  const cleanup = new Promise<void>((resolve) => {
    finalizing = resolve;
  });
  let release: () => void = () => undefined;
  const held = new Promise<void>((resolve) => {
    release = resolve;
  });
  const runtime = await Effect.runPromise(
    makeRuntime({
      owner: fixture.owner,
      isAlive: () => Effect.succeed(false),
      wake: { recompute: () => Effect.void },
      executor: {
        execute: () =>
          Effect.sync(start).pipe(
            Effect.andThen(Effect.never),
            Effect.ensuring(
              Effect.sync(finalizing).pipe(
                Effect.andThen(Effect.promise(() => held)),
              ),
            ),
          ),
        isDrained: () => Effect.succeed(true),
      },
    }),
  );
  await Effect.runPromise(
    runtime.ingest({
      jobId: "cancel-stop",
      queue: "q",
      lane: "test",
      priority: 0,
      payload: null,
      state: "queued",
    }),
  );
  const running = Effect.runFork(runtime.tick());
  try {
    await started;
    const closing = Effect.runFork(runtime.close);
    await cleanup;
    expect(
      await Effect.runPromise(
        runtime.cancel({ jobId: "cancel-stop", cancellationId: "user-cancel" }),
      ),
    ).toEqual({ status: "recorded" });
    release();
    await Effect.runPromise(Fiber.join(closing));
    expect(await Effect.runPromise(Fiber.join(running))).toMatchObject({
      status: "settled",
      job: { outcome: "cancelled" },
    });
  } finally {
    release();
    await Effect.runPromise(Fiber.interrupt(running));
    fixture.close();
  }
});
