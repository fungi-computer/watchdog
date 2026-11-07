import * as Effect from "effect/Effect";
import { afterEach, expect, it, vi } from "vitest";
import { makeRuntime, type WatchdogAttempt } from "../src/effect.js";
import { openSqliteState } from "../src/internal/sqlite-store.js";
import { createSqliteOwnerFixture } from "./sqlite-owner.fixture.js";

afterEach(() => vi.restoreAllMocks());
const job = {
  jobId: "delivery",
  queue: "recipient",
  lane: "post",
  priority: 0,
  payload: null,
  state: "queued" as const,
};
const policy = { baseMs: 100, maxMs: 1000, limit: { maxAttempts: 2 } };

it("retries the same identity and charges before I/O, including a lost final receiver attempt", async () => {
  const fixture = createSqliteOwnerFixture();
  const clock = vi.spyOn(Date, "now").mockReturnValue(1000);
  const observations: WatchdogAttempt[] = [];
  try {
    const open = () =>
      Effect.runPromise(
        makeRuntime({
          owner: fixture.owner,
          isAlive: () => Effect.succeed(false),
          wake: { recompute: () => Effect.void },
          executor: {
            retryPolicy: () => Effect.succeed(policy),
            execute: (_job, attempt) =>
              Effect.sync(() => {
                if (attempt === undefined)
                  throw new Error("delivery attempt missing");
                observations.push(attempt);
                expect(
                  fixture.owner.sql
                    .exec(
                      "SELECT attempt_count FROM watchdog_jobs WHERE job_id=?",
                      job.jobId,
                    )
                    .toArray()[0]?.attempt_count,
                ).toBe(attempt.count);
                return attempt.status === "exhausted"
                  ? { kind: "completed" as const }
                  : { kind: "retry" as const };
              }),
          },
        }),
      );
    const runtime = await open();
    await Effect.runPromise(runtime.ingest(job));
    expect(await Effect.runPromise(runtime.tick())).toEqual({
      status: "requeued",
      jobId: job.jobId,
    });
    expect(await Effect.runPromise(runtime.wakeState(1000))).toEqual({
      pending: false,
      nextWakeAt: 1100,
    });
    clock.mockReturnValue(1100);
    const store = openSqliteState(fixture.owner).store;
    expect(
      await Effect.runPromise(store.selectAndClaim("lost-claim")),
    ).toMatchObject({ status: "claimed" });
    expect(
      await Effect.runPromise(
        store.reserveAttempt({
          jobId: job.jobId,
          claimToken: "lost-claim",
          policy,
        }),
      ),
    ).toEqual({ status: "admitted", count: 2, final: true });
    // The receiver was entered, then this owner disappeared before returning a decision.
    const reopened = await open();
    expect(await Effect.runPromise(reopened.tick())).toEqual({
      status: "no_ready",
    });
    expect(await Effect.runPromise(reopened.wakeState(1100))).toEqual({
      pending: false,
      nextWakeAt: 1300,
    });
    clock.mockReturnValue(1300);
    expect(await Effect.runPromise(reopened.tick())).toMatchObject({
      status: "settled",
      job: { jobId: job.jobId, outcome: "completed" },
    });
    expect(observations).toEqual([
      { status: "admitted", count: 1, final: false },
      { status: "exhausted", count: 2, final: true },
    ]);
    expect(
      fixture.owner.sql
        .exec("SELECT count(*) AS count FROM watchdog_jobs")
        .toArray()[0]?.count,
    ).toBe(1);
  } finally {
    fixture.close();
  }
});

it("keeps the original delivery expiry and returns exhaustion without charging a new receiver call", async () => {
  const fixture = createSqliteOwnerFixture();
  const clock = vi.spyOn(Date, "now").mockReturnValue(1000);
  const attempts: WatchdogAttempt[] = [];
  try {
    const runtime = await Effect.runPromise(
      makeRuntime({
        owner: fixture.owner,
        isAlive: () => Effect.succeed(false),
        wake: { recompute: () => Effect.void },
        executor: {
          retryPolicy: () =>
            Effect.succeed({ ...policy, limit: { deadlineAt: 1050 } }),
          execute: (_job, attempt) =>
            Effect.sync(() => {
              if (attempt === undefined)
                throw new Error("delivery attempt missing");
              attempts.push(attempt);
              return attempt.status === "exhausted"
                ? { kind: "completed" as const }
                : { kind: "retry" as const };
            }),
        },
      }),
    );
    await Effect.runPromise(runtime.ingest(job));
    await Effect.runPromise(runtime.tick());
    expect(await Effect.runPromise(runtime.wakeState(1000))).toEqual({
      pending: false,
      nextWakeAt: 1050,
    });
    clock.mockReturnValue(1050);
    await Effect.runPromise(runtime.tick());
    expect(attempts).toEqual([
      { status: "admitted", count: 1, final: false },
      { status: "exhausted", count: 1, final: true },
    ]);
  } finally {
    fixture.close();
  }
});
