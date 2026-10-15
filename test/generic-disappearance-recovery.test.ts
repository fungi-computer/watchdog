import { expect, it } from "vitest";

import {
  Watchdog,
  WatchdogError,
  type WatchdogQueuedJob,
  type WatchdogExecutionDecision,
} from "../src/index.js";
import { createSqliteOwnerFixture } from "./sqlite-owner.fixture.js";

type Probe = "false" | "true" | "unanswered";

async function runningFixture(
  queued: WatchdogQueuedJob,
  decision: WatchdogExecutionDecision = { kind: "completed" },
): Promise<
  Readonly<{
    fixture: ReturnType<typeof createSqliteOwnerFixture>;
    oldTick: Promise<unknown>;
    release: () => void;
  }>
> {
  const fixture = createSqliteOwnerFixture();
  let started!: () => void;
  let release!: () => void;
  const executionStarted = new Promise<void>((resolve) => {
    started = resolve;
  });
  const executionReleased = new Promise<void>((resolve) => {
    release = resolve;
  });
  const runtime = await Watchdog.make({
    owner: fixture.owner,
    execute: async () => {
      started();
      await executionReleased;
      return decision;
    },
    isAlive: () => Promise.resolve(true),
    wake: { recompute: () => Promise.resolve() },
  });
  await runtime.ingest(queued);
  const oldTick = runtime.tick();
  await executionStarted;
  return { fixture, oldTick, release };
}

async function observeAnsweredProbe(probe: Probe) {
  const queued: WatchdogQueuedJob = {
    jobId: `job-disappeared-${probe}`,
    queue: "arbitrary-queue",
    lane: "opaque-lane",
    priority: 4,
    payload: { operation: "opaque", probe },
    recovery: { maxRecoveries: 1 },
    state: "queued",
  };
  const running = await runningFixture(queued);
  let recoveryExecutions = 0;
  try {
    const fresh = await Watchdog.make({
      owner: running.fixture.owner,
      execute: () => {
        recoveryExecutions += 1;
        return Promise.resolve({ kind: "completed" as const });
      },
      isAlive: () =>
        probe === "unanswered"
          ? Promise.reject(new WatchdogError("store_unavailable"))
          : Promise.resolve(probe === "true"),
      wake: { recompute: () => Promise.resolve() },
    });
    const result = await fresh.tick();
    const observed = await fresh.read(queued.jobId);
    running.release();
    if (probe === "false")
      await expect(running.oldTick).rejects.toMatchObject({
        cause: "settlement_rejected",
      });
    else await running.oldTick;
    return { result, observed, recoveryExecutions };
  } finally {
    running.release();
    running.fixture.close();
  }
}

it("recovers only after an answered false liveness check", async () => {
  await expect(observeAnsweredProbe("true")).resolves.toMatchObject({
    result: { status: "no_ready" },
    observed: { status: "found", job: { state: "running" } },
    recoveryExecutions: 0,
  });
  await expect(observeAnsweredProbe("unanswered")).resolves.toMatchObject({
    observed: { status: "found", job: { state: "running" } },
    recoveryExecutions: 0,
  });
  await expect(observeAnsweredProbe("false")).resolves.toMatchObject({
    result: {
      status: "settled",
      job: { state: "settled", outcome: "completed" },
    },
    observed: {
      status: "found",
      job: { state: "settled", outcome: "completed" },
    },
    recoveryExecutions: 1,
  });
});

it("reports a failed liveness check as unknown, never as alive or dead", async () => {
  const alive = await observeAnsweredProbe("true");
  const unanswered = await observeAnsweredProbe("unanswered");

  // An answered "alive" leaves nothing to report.
  expect(alive.result).toEqual({ status: "no_ready" });
  // An unanswered probe is its own outcome, and the job is left untouched.
  expect(unanswered.result).toEqual({
    status: "liveness_unknown",
    jobId: "job-disappeared-unanswered",
    reason: "liveness_unavailable",
  });
  expect(unanswered.observed).toMatchObject({
    status: "found",
    job: { state: "running" },
  });
  expect(unanswered.recoveryExecutions).toBe(0);
});

it("settles disappearance as interrupted when the recovery budget is absent", async () => {
  const queued: WatchdogQueuedJob = {
    jobId: "job-disappeared-no-budget",
    queue: "arbitrary-queue",
    lane: "opaque-lane",
    priority: 4,
    payload: { operation: "opaque" },
    state: "queued",
  };
  const running = await runningFixture(queued);
  try {
    const fresh = await Watchdog.make({
      owner: running.fixture.owner,
      execute: () => {
        throw new Error("interrupted work must not execute again");
      },
      isAlive: () => Promise.resolve(false),
      wake: { recompute: () => Promise.resolve() },
    });
    await expect(fresh.tick()).resolves.toMatchObject({
      status: "settled",
      job: { state: "settled", outcome: "interrupted" },
    });
    running.release();
    await expect(running.oldTick).rejects.toMatchObject({
      cause: "settlement_rejected",
    });
  } finally {
    running.release();
    running.fixture.close();
  }
});

it("keeps a completed claim when its owner settles during another worker's liveness check", async () => {
  const queued: WatchdogQueuedJob = {
    jobId: "job-completes-during-probe",
    queue: "q",
    lane: "test",
    priority: 0,
    payload: null,
    state: "queued",
  };
  const running = await runningFixture(queued);
  let recoveryExecutions = 0;
  try {
    const observer = await Watchdog.make({
      owner: running.fixture.owner,
      execute: () => {
        recoveryExecutions++;
        return Promise.resolve({ kind: "completed" as const });
      },
      isAlive: async () => {
        running.release();
        await running.oldTick;
        return false;
      },
      wake: { recompute: () => Promise.resolve() },
    });
    await expect(observer.tick()).resolves.toEqual({ status: "no_ready" });
    expect(await observer.read(queued.jobId)).toMatchObject({
      status: "found",
      job: { state: "settled", outcome: "completed" },
    });
    expect(recoveryExecutions).toBe(0);
  } finally {
    running.release();
    running.fixture.close();
  }
});

it.each(["failed", "cancelled", "outcome_unknown"] as const)(
  "keeps the raced terminal fact %s without executing it again",
  async (kind) => {
    const queued: WatchdogQueuedJob = {
      jobId: `job-raced-${kind}`,
      queue: "q",
      lane: "test",
      priority: 0,
      payload: null,
      state: "queued",
    };
    const running = await runningFixture(queued, { kind });
    try {
      const observer = await Watchdog.make({
        owner: running.fixture.owner,
        execute: () =>
          Promise.reject(new Error("a terminal fact must not execute again")),
        isAlive: async () => {
          running.release();
          await running.oldTick;
          return false;
        },
        wake: { recompute: () => Promise.resolve() },
      });
      await expect(observer.tick()).resolves.toEqual({ status: "no_ready" });
      expect(await observer.read(queued.jobId)).toMatchObject({
        status: "found",
        job: { state: "settled", outcome: kind },
      });
    } finally {
      running.release();
      running.fixture.close();
    }
  },
);

it("leaves a replacement running when an observer loses the old recovery claim", async () => {
  const queued: WatchdogQueuedJob = {
    jobId: "job-replaced-still-running",
    queue: "q",
    lane: "test",
    priority: 0,
    payload: null,
    state: "queued",
    recovery: { maxRecoveries: 1 },
  };
  const running = await runningFixture(queued);
  let notifyEntered: () => void = () => undefined;
  let releaseReplacement: () => void = () => undefined;
  const entered = new Promise<void>((resolve) => {
    notifyEntered = resolve;
  });
  const finish = new Promise<void>((resolve) => {
    releaseReplacement = resolve;
  });
  let replacement: Promise<unknown> | undefined;
  let executions = 0;
  try {
    const rescuer = await Watchdog.make({
      owner: running.fixture.owner,
      execute: async () => {
        executions++;
        notifyEntered();
        await finish;
        return { kind: "completed" as const };
      },
      isAlive: () => Promise.resolve(false),
      wake: { recompute: () => Promise.resolve() },
    });
    const observer = await Watchdog.make({
      owner: running.fixture.owner,
      execute: () =>
        Promise.reject(new Error("stale observer must not execute")),
      isAlive: async () => {
        replacement = rescuer.tick();
        await entered;
        return false;
      },
      wake: { recompute: () => Promise.resolve() },
    });
    await expect(observer.tick()).resolves.toEqual({ status: "no_ready" });
    expect(await observer.read(queued.jobId)).toMatchObject({
      status: "found",
      job: { state: "running" },
    });
    running.release();
    await expect(running.oldTick).rejects.toMatchObject({
      cause: "settlement_rejected",
    });
    releaseReplacement();
    await expect(replacement).resolves.toMatchObject({
      status: "settled",
      job: { outcome: "completed" },
    });
    expect(executions).toBe(1);
    expect(await observer.read(queued.jobId)).toMatchObject({
      status: "found",
      job: { state: "settled", outcome: "completed" },
    });
  } finally {
    running.release();
    releaseReplacement();
    await Promise.allSettled([
      running.oldTick,
      ...(replacement === undefined ? [] : [replacement]),
    ]);
    running.fixture.close();
  }
});

it("does not retry a running claim whose recovery generation changed during the probe", async () => {
  const fixture = createSqliteOwnerFixture();
  try {
    const observer = await Watchdog.make({
      owner: fixture.owner,
      execute: () =>
        Promise.reject(
          new Error("a running generation must not execute again"),
        ),
      isAlive: () => {
        // The owner persisted a later generation under this token. Compare both
        // identity fields rather than assuming the token alone is sufficient.
        fixture.owner.sql.exec(
          "UPDATE watchdog_jobs SET recovery_count=1 WHERE job_id=?",
          "generation",
        );
        return Promise.resolve(false);
      },
      wake: { recompute: () => Promise.resolve() },
    });
    await observer.ingest({
      jobId: "generation",
      queue: "q",
      lane: "test",
      priority: 0,
      payload: null,
      state: "queued",
    });
    fixture.owner.sql.exec(
      "UPDATE watchdog_jobs SET state='running',claim_token='stored-claim' WHERE job_id=?",
      "generation",
    );
    await expect(observer.tick()).resolves.toEqual({ status: "no_ready" });
    const current = await observer.read("generation");
    expect(current).toMatchObject({
      status: "found",
      job: { state: "running" },
    });
    expect(current).not.toHaveProperty("claimToken");
    expect(current).not.toHaveProperty("recoveryCount");
  } finally {
    fixture.close();
  }
});

it("rejects a refused recovery write when the same running token and generation remain", async () => {
  const fixture = createSqliteOwnerFixture();
  try {
    const observer = await Watchdog.make({
      owner: fixture.owner,
      execute: () =>
        Promise.reject(new Error("a refused recovery must not execute")),
      isAlive: () => Promise.resolve(false),
      wake: { recompute: () => Promise.resolve() },
    });
    await observer.ingest({
      jobId: "unchanged",
      queue: "q",
      lane: "test",
      priority: 0,
      payload: null,
      state: "queued",
    });
    fixture.owner.sql.exec(
      "UPDATE watchdog_jobs SET state='running',claim_token='stored-claim' WHERE job_id=?",
      "unchanged",
    );
    fixture.owner.sql
      .exec(`CREATE TRIGGER refuse_recovery BEFORE UPDATE OF state ON watchdog_jobs
      WHEN OLD.job_id='unchanged' AND NEW.state='settled'
      BEGIN SELECT RAISE(IGNORE); END`);
    await expect(observer.tick()).rejects.toMatchObject({
      cause: "settlement_rejected",
    });
    expect(await observer.read("unchanged")).toMatchObject({
      status: "found",
      job: { state: "running" },
    });
  } finally {
    fixture.close();
  }
});

it("rejects recovery when the stored row disappears during its liveness probe", async () => {
  const running = await runningFixture({
    jobId: "removed-during-probe",
    queue: "q",
    lane: "test",
    priority: 0,
    payload: null,
    state: "queued",
  });
  try {
    const observer = await Watchdog.make({
      owner: running.fixture.owner,
      execute: () => Promise.reject(new Error("missing work must not execute")),
      isAlive: () => {
        running.fixture.owner.sql.exec(
          "DELETE FROM watchdog_jobs WHERE job_id=?",
          "removed-during-probe",
        );
        return Promise.resolve(false);
      },
      wake: { recompute: () => Promise.resolve() },
    });
    await expect(observer.tick()).rejects.toMatchObject({
      cause: "settlement_rejected",
    });
    expect(await observer.read("removed-during-probe")).toEqual({
      status: "missing",
    });
    running.release();
    await expect(running.oldTick).rejects.toMatchObject({
      cause: "settlement_rejected",
    });
  } finally {
    running.release();
    await Promise.allSettled([running.oldTick]);
    running.fixture.close();
  }
});

it("leaves a replacement whose only changed identity field is the claim token", async () => {
  const running = await runningFixture({
    jobId: "token-only",
    queue: "q",
    lane: "test",
    priority: 0,
    payload: null,
    state: "queued",
  });
  try {
    const observer = await Watchdog.make({
      owner: running.fixture.owner,
      execute: () =>
        Promise.reject(new Error("the replacement retains its work")),
      isAlive: () => {
        running.fixture.owner.sql.exec(
          "UPDATE watchdog_jobs SET claim_token=? WHERE job_id=?",
          "replacement-token",
          "token-only",
        );
        return Promise.resolve(false);
      },
      wake: { recompute: () => Promise.resolve() },
    });
    await expect(observer.tick()).resolves.toEqual({ status: "no_ready" });
    expect(await observer.read("token-only")).toMatchObject({
      status: "found",
      job: { state: "running" },
    });
    running.release();
    await expect(running.oldTick).rejects.toMatchObject({
      cause: "settlement_rejected",
    });
  } finally {
    running.release();
    await Promise.allSettled([running.oldTick]);
    running.fixture.close();
  }
});
