import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import { expect, it, vi } from "vitest";

import { makeRuntime } from "../src/effect.js";
import { createSqliteOwnerFixture } from "./sqlite-owner.fixture.js";

it("interrupts the active Effect after durable cancellation", async () => {
  const fixture = createSqliteOwnerFixture();
  try {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const started = yield* Deferred.make<undefined>();
        let finalized = false;
        const runtime = yield* makeRuntime({
          owner: fixture.owner,
          executor: {
            execute: () =>
              Deferred.succeed(started, undefined).pipe(
                Effect.andThen(Effect.never),
                Effect.ensuring(
                  Effect.sync(() => {
                    finalized = true;
                  }),
                ),
              ),
          },
          wake: { recompute: () => Effect.void },
          isAlive: () => Effect.succeed(true),
        });
        yield* runtime.ingest({
          jobId: "job-active-cancel-1",
          queue: "work",
          lane: "maintenance",
          priority: 0,
          payload: { operation: "opaque" },
          state: "queued",
        });
        const tickFiber = yield* Effect.forkChild(runtime.tick());
        yield* Deferred.await(started);
        const cancellation = yield* runtime.cancel({
          jobId: "job-active-cancel-1",
          cancellationId: "cancellation-1",
        });
        const tick = yield* Fiber.join(tickFiber);
        const durable = yield* runtime.read("job-active-cancel-1");
        return { cancellation, durable, finalized, tick };
      }),
    );

    expect(result).toMatchObject({
      cancellation: { status: "recorded" },
      durable: {
        status: "found",
        cancellation: {
          status: "present",
          cancellation: {
            jobId: "job-active-cancel-1",
            cancellationId: "cancellation-1",
          },
        },
        job: {
          jobId: "job-active-cancel-1",
          state: "settled",
          outcome: "cancelled",
        },
      },
      finalized: true,
      tick: {
        status: "settled",
        job: {
          jobId: "job-active-cancel-1",
          state: "settled",
          outcome: "cancelled",
        },
      },
    });
  } finally {
    fixture.close();
  }
});

it("acknowledges durable cancellation before held execution cleanup, while tick owns settlement", async () => {
  const fixture = createSqliteOwnerFixture();
  const release = await Effect.runPromise(Deferred.make<undefined>());
  let acknowledged = false;
  let cleanupStarted = false;
  const result = Effect.runPromise(
    Effect.gen(function* () {
      const started = yield* Deferred.make<undefined>();
      const runtime = yield* makeRuntime({
        owner: fixture.owner,
        executor: {
          execute: () =>
            Deferred.succeed(started, undefined).pipe(
              Effect.andThen(Effect.never),
              Effect.ensuring(
                Effect.sync(() => {
                  cleanupStarted = true;
                }).pipe(Effect.andThen(Deferred.await(release))),
              ),
            ),
        },
        wake: { recompute: () => Effect.void },
        isAlive: () => Effect.succeed(true),
      });
      yield* runtime.ingest({
        jobId: "held-cleanup",
        queue: "work",
        lane: "maintenance",
        priority: 0,
        payload: { operation: "opaque" },
        state: "queued",
      });
      const tick = yield* Effect.forkChild(runtime.tick());
      yield* Deferred.await(started);
      const cancel = yield* Effect.forkChild(
        runtime
          .cancel({
            jobId: "held-cleanup",
            cancellationId: "cancel-held-cleanup",
          })
          .pipe(
            Effect.tap(() =>
              runtime.read("held-cleanup").pipe(
                Effect.tap((durable) =>
                  Effect.sync(() => {
                    expect(durable).toMatchObject({
                      status: "found",
                      job: { state: "running" },
                      cancellation: { status: "present" },
                    });
                    acknowledged = true;
                  }),
                ),
              ),
            ),
          ),
      );
      yield* Deferred.await(release);
      yield* Fiber.join(cancel);
      yield* Fiber.join(tick);
      return yield* runtime.read("held-cleanup");
    }),
  );
  try {
    await vi.waitFor(
      () => {
        expect(cleanupStarted).toBe(true);
        expect(acknowledged).toBe(true);
      },
      { timeout: 1_000, interval: 10 },
    );
  } finally {
    await Effect.runPromise(Deferred.succeed(release, undefined));
    try {
      expect(await result).toMatchObject({
        status: "found",
        job: { state: "settled", outcome: "cancelled" },
      });
    } finally {
      fixture.close();
    }
  }
});

it("fails a running lane durably before cleanup and does not project its claim twice", async () => {
  const fixture = createSqliteOwnerFixture();
  const lane = "member:refused";
  const started = await Effect.runPromise(Deferred.make<undefined>());
  const cleanup = await Effect.runPromise(Deferred.make<undefined>());
  const complete = await Effect.runPromise(Deferred.make<undefined>());
  const release = await Effect.runPromise(Deferred.make<undefined>());
  const projected: string[] = [];
  let finalized = false;
  const runtime = await Effect.runPromise(
    makeRuntime({
      owner: fixture.owner,
      concurrentLane: lane,
      executor: {
        execute: () =>
          Deferred.succeed(started, undefined).pipe(
            Effect.andThen(Deferred.await(complete)),
            Effect.as({ kind: "completed" as const }),
            Effect.ensuring(
              Deferred.succeed(cleanup, undefined).pipe(
                Effect.andThen(Deferred.await(release)),
                Effect.andThen(
                  Effect.sync(() => {
                    finalized = true;
                  }),
                ),
              ),
            ),
          ),
      },
      isAlive: () => Effect.succeed(true),
      wake: { recompute: () => Effect.void },
      project: ({ settled }) => {
        if (settled !== undefined) projected.push(settled.jobId);
      },
    }),
  );
  const running = {
    jobId: "refused-running",
    queue: "refused",
    lane,
    priority: 0,
    payload: null,
    state: "queued" as const,
  };
  // The queued row is older, but refusal must settle the live claim first.
  await Effect.runPromise(
    runtime.ingest({ ...running, jobId: "refused-queued", priority: -1 }),
  );
  await Effect.runPromise(runtime.ingest(running));
  const tick = Effect.runPromise(runtime.tickConcurrent());
  try {
    await Effect.runPromise(Deferred.await(started));
    await expect(
      Effect.runPromise(runtime.failLane({ lane, reason: "access_changed" })),
    ).resolves.toEqual({ settled: 2, more: false });
    await Effect.runPromise(Deferred.await(cleanup));
    expect(finalized).toBe(false);
    await expect(
      Effect.runPromise(runtime.read(running.jobId)),
    ).resolves.toMatchObject({
      status: "found",
      job: {
        state: "settled",
        outcome: "failed",
        failureReason: "access_changed",
      },
    });
    expect(projected).toEqual(["refused-running", "refused-queued"]);
    await Effect.runPromise(Deferred.succeed(release, undefined));
    await expect(tick).resolves.toMatchObject({
      status: "settled",
      job: { outcome: "failed", failureReason: "access_changed" },
    });
    expect(finalized).toBe(true);
    await expect(
      Effect.runPromise(runtime.failLane({ lane, reason: "access_changed" })),
    ).resolves.toEqual({ settled: 0, more: false });
    expect(projected).toEqual(["refused-running", "refused-queued"]);
  } finally {
    await Effect.runPromise(Deferred.succeed(complete, undefined));
    await Effect.runPromise(Deferred.succeed(release, undefined));
    await tick;
    fixture.close();
  }
});

it("routes refusal of another lane to the serial engine without interrupting the concurrent claim", async () => {
  const fixture = createSqliteOwnerFixture();
  const lane = "member:concurrent";
  const serialLane = "member:serial";
  const concurrentStarted = await Effect.runPromise(Deferred.make<undefined>());
  const serialStarted = await Effect.runPromise(Deferred.make<undefined>());
  const release = await Effect.runPromise(Deferred.make<undefined>());
  const finalized: string[] = [];
  const runtime = await Effect.runPromise(
    makeRuntime({
      owner: fixture.owner,
      concurrentLane: lane,
      executor: {
        execute: (job) =>
          Deferred.succeed(
            job.lane === lane ? concurrentStarted : serialStarted,
            undefined,
          ).pipe(
            Effect.andThen(Deferred.await(release)),
            Effect.as({ kind: "completed" as const }),
            Effect.ensuring(
              Effect.sync(() => {
                finalized.push(job.lane);
              }),
            ),
          ),
      },
      isAlive: () => Effect.succeed(true),
      wake: { recompute: () => Effect.void },
    }),
  );
  let concurrent: Promise<unknown> = Promise.resolve();
  let serial: Promise<unknown> = Promise.resolve();
  try {
    for (const actor of [lane, serialLane])
      await Effect.runPromise(
        runtime.ingest({
          jobId: actor,
          queue: actor,
          lane: actor,
          priority: 0,
          payload: null,
          state: "queued",
        }),
      );
    concurrent = Effect.runPromise(runtime.tickConcurrent());
    await Effect.runPromise(Deferred.await(concurrentStarted));
    serial = Effect.runPromise(runtime.tick());
    await Effect.runPromise(Deferred.await(serialStarted));
    expect(
      await Effect.runPromise(
        runtime.failLane({ lane: serialLane, reason: "access_changed" }),
      ),
    ).toEqual({ settled: 1, more: false });
    expect(await serial).toMatchObject({
      status: "settled",
      job: {
        lane: serialLane,
        outcome: "failed",
        failureReason: "access_changed",
      },
    });
    expect(finalized).toEqual([serialLane]);
    expect(await Effect.runPromise(runtime.read(lane))).toMatchObject({
      status: "found",
      job: { state: "running", lane },
    });
    await Effect.runPromise(Deferred.succeed(release, undefined));
    expect(await concurrent).toMatchObject({
      status: "settled",
      job: { lane, outcome: "completed" },
    });
    expect(finalized).toEqual([serialLane, lane]);
  } finally {
    await Effect.runPromise(Deferred.succeed(release, undefined));
    await Promise.allSettled([concurrent, serial]);
    fixture.close();
  }
});
