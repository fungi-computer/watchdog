import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import { expect, it } from "vitest";

import {
  makeRuntime,
  type WatchdogQueuedJob,
  type WatchdogQueueTransition,
} from "../src/effect.js";
import { Watchdog } from "../src/index.js";
import { createSqliteOwnerFixture } from "./sqlite-owner.fixture.js";

const job = (suffix: string, queue = "queue-a"): WatchdogQueuedJob => ({
  jobId: `job-projection-${suffix}`,
  queue,
  lane: "arbitrary-lane",
  priority: 0,
  payload: { opaque: suffix },
  state: "queued",
});

it("projects every queue transition inside its own transaction", async () => {
  const fixture = createSqliteOwnerFixture();
  const seen: WatchdogQueueTransition[] = [];
  try {
    const runtime = await Effect.runPromise(
      makeRuntime({
        owner: fixture.owner,
        executor: {
          execute: () => Effect.succeed({ kind: "failed" as const }),
        },
        isAlive: () => Effect.succeed(true),
        wake: { recompute: () => Effect.void },
        project: (transition) => {
          seen.push(transition);
        },
      }),
    );
    const first = job("first");
    const second = job("second");
    await Effect.runPromise(runtime.ingest(first));
    await Effect.runPromise(runtime.ingest(first));
    fixture.owner.transactionSync(() => {
      runtime.transactional.enqueueAcceptedJob(second);
    });
    await Effect.runPromise(runtime.tick());
    await Effect.runPromise(
      runtime.cancel({ jobId: second.jobId, cancellationId: "cancel-1" }),
    );
    await Effect.runPromise(runtime.tick());
    await Effect.runPromise(runtime.ingest(job("purged")));
    await Effect.runPromise(runtime.purge("queue-a"));

    const running = {
      state: "running",
      jobId: first.jobId,
      cancellationRequested: false,
    };
    const queuedSecond = {
      state: "queued",
      jobId: second.jobId,
      cancellationRequested: false,
    };
    expect(seen).toEqual([
      {
        queue: "queue-a",
        head: {
          state: "queued",
          jobId: first.jobId,
          cancellationRequested: false,
        },
      },
      {
        queue: "queue-a",
        head: {
          state: "queued",
          jobId: first.jobId,
          cancellationRequested: false,
        },
      },
      { queue: "queue-a", head: running },
      {
        queue: "queue-a",
        head: queuedSecond,
        settled: { ...first, state: "settled", outcome: "failed" },
      },
      {
        queue: "queue-a",
        head: { ...queuedSecond, cancellationRequested: true },
      },
      {
        queue: "queue-a",
        head: { state: "idle" },
        settled: { ...second, state: "settled", outcome: "cancelled" },
      },
      {
        queue: "queue-a",
        head: {
          state: "queued",
          jobId: "job-projection-purged",
          cancellationRequested: false,
        },
      },
      { queue: "queue-a", head: { state: "idle" } },
    ]);
    expect(
      fixture.owner.transactionSync(() =>
        runtime.transactional.readLatestSettledJob("queue-a"),
      ),
    ).toMatchObject({ jobId: second.jobId, outcome: "cancelled" });
    expect(runtime.transactional.readLatestSettledJob("queue-b")).toBe(
      undefined,
    );
  } finally {
    fixture.close();
  }
});

it("rolls the transition back when the owner projection fails", async () => {
  const fixture = createSqliteOwnerFixture();
  try {
    const runtime = await Effect.runPromise(
      makeRuntime({
        owner: fixture.owner,
        executor: {
          execute: () => Effect.succeed({ kind: "completed" as const }),
        },
        isAlive: () => Effect.succeed(true),
        wake: { recompute: () => Effect.void },
        project: () => {
          throw new Error("projection unavailable");
        },
      }),
    );
    await expect(
      Effect.runPromise(runtime.ingest(job("rejected"))),
    ).rejects.toThrow("Watchdog store unavailable");
    await expect(
      Effect.runPromise(runtime.read("job-projection-rejected")),
    ).resolves.toEqual({ status: "missing" });
  } finally {
    fixture.close();
  }
});

it("fails a refused lane once without purging jobs or touching another lane", async () => {
  const fixture = createSqliteOwnerFixture();
  const projected: WatchdogQueueTransition[] = [];
  const refused = "member:refused";
  try {
    const runtime = await Watchdog.make({
      owner: fixture.owner,
      execute: () => Promise.resolve({ kind: "completed" as const }),
      isAlive: () => Promise.resolve(true),
      wake: { recompute: () => Promise.resolve() },
      project: (transition) => projected.push(transition),
    });
    const history = { ...job("history"), lane: refused };
    await runtime.ingest(history);
    await runtime.tick();
    const first = { ...job("refused-first"), lane: refused };
    const second = { ...job("refused-second", "queue-b"), lane: refused };
    const other = { ...job("other"), lane: "member:other" };
    for (const queued of [first, second, other]) await runtime.ingest(queued);
    projected.length = 0;

    await expect(
      runtime.failLane({ lane: refused, reason: "access_changed" }),
    ).resolves.toEqual({ settled: 2, more: false });
    for (const queued of [first, second]) {
      await expect(runtime.read(queued.jobId)).resolves.toMatchObject({
        status: "found",
        job: {
          ...queued,
          state: "settled",
          outcome: "failed",
          failureReason: "access_changed",
        },
      });
    }
    await expect(runtime.read(history.jobId)).resolves.toMatchObject({
      status: "found",
      job: { state: "settled", outcome: "completed" },
    });
    await expect(runtime.read(other.jobId)).resolves.toMatchObject({
      status: "found",
      job: other,
    });
    expect(projected.map((transition) => transition.settled?.jobId)).toEqual([
      first.jobId,
      second.jobId,
    ]);
    expect(projected[0]?.head).toEqual({
      state: "queued",
      jobId: other.jobId,
      cancellationRequested: false,
    });
    expect(projected[1]?.head).toEqual({ state: "idle" });
    await expect(
      runtime.failLane({ lane: refused, reason: "access_changed" }),
    ).resolves.toEqual({ settled: 0, more: false });
    expect(projected).toHaveLength(2);
  } finally {
    fixture.close();
  }
});

it("rolls failed-lane settlement and every owner projection back together", async () => {
  const fixture = createSqliteOwnerFixture();
  const lane = "member:refused";
  let throwProjection = false;
  try {
    fixture.owner.sql.exec(
      "CREATE TABLE owner_projection (job_id TEXT PRIMARY KEY)",
    );
    const runtime = await Watchdog.make({
      owner: fixture.owner,
      execute: () => Promise.resolve({ kind: "completed" as const }),
      isAlive: () => Promise.resolve(true),
      wake: { recompute: () => Promise.resolve() },
      project: ({ settled }) => {
        if (settled === undefined) return;
        fixture.owner.sql.exec(
          "INSERT INTO owner_projection(job_id) VALUES (?)",
          settled.jobId,
        );
        if (throwProjection && settled.jobId.endsWith("second"))
          throw new Error("owner projection unavailable");
      },
    });
    const queued = ["first", "second"].map((suffix) => ({
      ...job(`rollback-${suffix}`),
      lane,
    }));
    for (const item of queued) await runtime.ingest(item);
    throwProjection = true;
    await expect(
      runtime.failLane({ lane, reason: "access_changed" }),
    ).rejects.toThrow("Watchdog store unavailable");
    for (const item of queued) {
      await expect(runtime.read(item.jobId)).resolves.toMatchObject({
        status: "found",
        job: item,
      });
    }
    expect(
      fixture.owner.sql.exec("SELECT job_id FROM owner_projection").toArray(),
    ).toEqual([]);
    throwProjection = false;
    await expect(
      runtime.failLane({ lane, reason: "access_changed" }),
    ).resolves.toEqual({ settled: 2, more: false });
    expect(
      fixture.owner.sql
        .exec("SELECT job_id FROM owner_projection ORDER BY job_id")
        .toArray(),
    ).toEqual(queued.map((item) => ({ job_id: item.jobId })));
  } finally {
    fixture.close();
  }
});

it("continues a large lane in bounded batches without treating 256 as a work cap", async () => {
  const fixture = createSqliteOwnerFixture();
  const lane = "member:large";
  const settled: string[] = [];
  try {
    const runtime = await Watchdog.make({
      owner: fixture.owner,
      execute: () => Promise.resolve({ kind: "completed" as const }),
      isAlive: () => Promise.resolve(true),
      wake: { recompute: () => Promise.resolve() },
      project: ({ settled: job }) => {
        if (job !== undefined) settled.push(job.jobId);
      },
    });
    fixture.owner.transactionSync(() => {
      for (let index = 0; index < 257; index++)
        runtime.transactional.enqueueAcceptedJob({
          ...job(`batch-${String(index)}`),
          lane,
        });
    });
    await expect(
      runtime.failLane({ lane, reason: "access_changed" }),
    ).resolves.toEqual({ settled: 256, more: true });
    expect(settled).toHaveLength(256);
    await expect(
      runtime.read("job-projection-batch-256"),
    ).resolves.toMatchObject({ status: "found", job: { state: "queued" } });
    await expect(
      runtime.failLane({ lane, reason: "access_changed" }),
    ).resolves.toEqual({ settled: 1, more: false });
    await expect(
      runtime.failLane({ lane, reason: "another_reason" }),
    ).resolves.toEqual({ settled: 0, more: false });
    expect(new Set(settled).size).toBe(257);
    await expect(
      runtime.read("job-projection-batch-256"),
    ).resolves.toMatchObject({
      status: "found",
      job: { outcome: "failed", failureReason: "access_changed" },
    });
  } finally {
    fixture.close();
  }
});

it("refuses invalid lane failure input before changing stored work", async () => {
  const fixture = createSqliteOwnerFixture();
  const lane = "member:refused";
  try {
    const runtime = await Watchdog.make({
      owner: fixture.owner,
      execute: () => Promise.resolve({ kind: "completed" as const }),
      isAlive: () => Promise.resolve(true),
      wake: { recompute: () => Promise.resolve() },
    });
    const queued = { ...job("invalid-failure"), lane };
    await runtime.ingest(queued);
    for (const input of [
      { lane: "l".repeat(513), reason: "access_changed" },
      { lane, reason: "" },
      { lane, reason: "r".repeat(201) },
      { lane, reason: "access_changed", extra: true },
    ])
      await expect(runtime.failLane(input)).rejects.toThrow(
        "Watchdog store invalid",
      );
    await expect(runtime.read(queued.jobId)).resolves.toMatchObject({
      status: "found",
      job: queued,
    });
  } finally {
    fixture.close();
  }
});

it.each(["completed", "failed"] as const)(
  "fences a late %s executor report after another runtime durably refuses its lane",
  async (kind) => {
    const fixture = createSqliteOwnerFixture();
    const started = await Effect.runPromise(Deferred.make<undefined>());
    const release = await Effect.runPromise(Deferred.make<undefined>());
    const projected: string[] = [];
    const lane = "member:refused";
    const project = ({ settled }: WatchdogQueueTransition) => {
      if (settled !== undefined) projected.push(settled.jobId);
    };
    const old = await Watchdog.make({
      owner: fixture.owner,
      execute: () =>
        Effect.runPromise(
          Deferred.succeed(started, undefined).pipe(
            Effect.andThen(Deferred.await(release)),
            Effect.as({ kind }),
          ),
        ),
      isAlive: () => Promise.resolve(true),
      wake: { recompute: () => Promise.resolve() },
      project,
    });
    const current = await Watchdog.make({
      owner: fixture.owner,
      execute: () => Promise.resolve({ kind: "completed" as const }),
      isAlive: () => Promise.resolve(true),
      wake: { recompute: () => Promise.resolve() },
      project,
    });
    const queued = { ...job("late-report"), lane };
    await old.ingest(queued);
    const ticking = old.tick();
    try {
      await Effect.runPromise(Deferred.await(started));
      await expect(
        current.failLane({ lane, reason: "access_changed" }),
      ).resolves.toEqual({ settled: 1, more: false });
      await Effect.runPromise(Deferred.succeed(release, undefined));
      await expect(ticking).rejects.toThrow("Watchdog settlement rejected");
      await expect(current.read(queued.jobId)).resolves.toMatchObject({
        status: "found",
        job: {
          state: "settled",
          outcome: "failed",
          failureReason: "access_changed",
        },
      });
      expect(projected).toEqual([queued.jobId]);
    } finally {
      await Effect.runPromise(Deferred.succeed(release, undefined));
      await Promise.allSettled([ticking]);
      fixture.close();
    }
  },
);

it("does not execute a claim refused by the owner before executor registration", async () => {
  const fixture = createSqliteOwnerFixture();
  const lane = "member:refused-before-start";
  let calls = 0;
  const projected: string[] = [];
  try {
    const refuser = await Effect.runPromise(
      makeRuntime({
        owner: fixture.owner,
        executor: { execute: () => Effect.succeed({ kind: "completed" }) },
        isAlive: () => Effect.succeed(true),
        wake: { recompute: () => Effect.void },
        project: ({ settled }) => {
          if (settled !== undefined) projected.push(settled.jobId);
        },
      }),
    );
    const runtime = await Watchdog.make({
      owner: fixture.owner,
      execute: () => {
        calls++;
        return Promise.resolve({ kind: "completed" });
      },
      isAlive: () => Promise.resolve(true),
      wake: { recompute: () => Promise.resolve() },
      // The real claim projection runs before tick receives its claimed row.
      // The owner refuses through the PUBLIC Effect API, inside the same SQLite owner.
      project: ({ head }) => {
        if (head.state === "running")
          expect(
            Effect.runSync(
              refuser.failLane({ lane, reason: "access_changed" }),
            ),
          ).toEqual({ settled: 1, more: false });
      },
    });
    const queued = { ...job("before-start"), lane };
    await runtime.ingest(queued);
    const result = await runtime.tick().then(
      (value) => value,
      (cause: unknown) => cause,
    );
    expect(calls).toBe(0);
    expect(result).toMatchObject({
      status: "settled",
      job: {
        state: "settled",
        outcome: "failed",
        failureReason: "access_changed",
      },
    });
    expect(projected).toEqual([queued.jobId]);
  } finally {
    fixture.close();
  }
});

it("refusal settles a queued cancellation intent as failed rather than cancelled", async () => {
  const fixture = createSqliteOwnerFixture();
  try {
    const runtime = await Watchdog.make({
      owner: fixture.owner,
      execute: () => Promise.resolve({ kind: "completed" }),
      isAlive: () => Promise.resolve(true),
      wake: { recompute: () => Promise.resolve() },
    });
    const queued = { ...job("refused-cancellation"), lane: "member:refused" };
    await runtime.ingest(queued);
    await runtime.cancel({
      jobId: queued.jobId,
      cancellationId: "cancel-before-refusal",
    });
    expect(
      await runtime.failLane({ lane: queued.lane, reason: "access_changed" }),
    ).toEqual({ settled: 1, more: false });
    expect(await runtime.read(queued.jobId)).toMatchObject({
      status: "found",
      job: {
        state: "settled",
        outcome: "failed",
        failureReason: "access_changed",
      },
      cancellation: { status: "present" },
    });
    expect(await runtime.tick()).toEqual({ status: "no_ready" });
  } finally {
    fixture.close();
  }
});
