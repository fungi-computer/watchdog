import { expect, it, vi } from "vitest";
import { DatabaseSync } from "node:sqlite";
import * as Schema from "effect/Schema";
import * as Effect from "effect/Effect";
import { openAsyncStore, heartbeatMs } from "../src/internal/async-store.js";
import { createAsyncRuntime } from "../src/internal/async-runtime.js";
import {
  schemaStatements,
  type SqlDatabase,
} from "../src/internal/async-sql.js";

it("preserves full actor lanes and the 512-character boundary in fresh shared SQL storage", async () => {
  const sqlite = new DatabaseSync(":memory:");
  const Rows = Schema.Array(
    Schema.Record(
      Schema.String,
      Schema.Union([Schema.String, Schema.Number, Schema.Null]),
    ),
  );
  try {
    for (const sql of schemaStatements(false)) sqlite.exec(sql);
    const database: SqlDatabase = {
      query: (statement) =>
        Promise.resolve(
          Schema.decodeUnknownSync(Rows)(
            sqlite
              .prepare(statement.text.replace(/\$(\d+)/gu, "?$1"))
              .all(...statement.values),
          ),
        ),
    };
    const runtime = createAsyncRuntime(database, {
      execute: () => Promise.resolve({ kind: "completed" }),
      wake: { recompute: () => Promise.resolve() },
    });
    for (const lane of [
      `member:${"m".repeat(256)}`,
      `team-key:${"k".repeat(256)}`,
      "l".repeat(512),
    ]) {
      const job = {
        jobId: `lane-${String(lane.length)}`,
        queue: "shared-lane",
        lane,
        priority: 0,
        payload: null,
        state: "queued" as const,
      };
      await expect(runtime.ingest(job)).resolves.toEqual({ status: "queued" });
      await expect(runtime.tick()).resolves.toMatchObject({
        status: "settled",
        job: { lane, state: "settled", outcome: "completed" },
      });
      await expect(runtime.read(job.jobId)).resolves.toMatchObject({
        status: "found",
        job: { lane, state: "settled" },
      });
    }
    await expect(
      runtime.ingest({
        jobId: "too-wide",
        queue: "shared-lane",
        lane: "l".repeat(513),
        priority: 0,
        payload: null,
        state: "queued",
      }),
    ).rejects.toThrow();
    await expect(runtime.read("too-wide")).resolves.toEqual({
      status: "missing",
    });
  } finally {
    sqlite.close();
  }
});

it("leaves a shared running claim intact when its native liveness query fails", async () => {
  const sqlite = new DatabaseSync(":memory:");
  const Rows = Schema.Array(
    Schema.Record(
      Schema.String,
      Schema.Union([Schema.String, Schema.Number, Schema.Null]),
    ),
  );
  let release: () => void = () => undefined;
  try {
    for (const sql of schemaStatements(false)) sqlite.exec(sql);
    let failProbe = false;
    const database: SqlDatabase = {
      query: (statement) => {
        if (failProbe && statement.text.includes("AS alive"))
          return Promise.reject(new Error("database disconnected"));
        return Promise.resolve(
          Schema.decodeUnknownSync(Rows)(
            sqlite
              .prepare(statement.text.replace(/\$(\d+)/gu, "?$1"))
              .all(...statement.values),
          ),
        );
      },
    };
    let notify: () => void = () => undefined;
    const started = new Promise<void>((resolve) => {
      notify = resolve;
    });
    const held = new Promise<void>((resolve) => {
      release = resolve;
    });
    const options = {
      now: () => 1000,
      execute: async () => {
        notify();
        await held;
        return { kind: "completed" } as const;
      },
      wake: { recompute: () => Promise.resolve() },
    };
    const first = createAsyncRuntime(database, options);
    await first.ingest({
      jobId: "held",
      queue: "q",
      lane: "test",
      priority: 0,
      payload: null,
      state: "queued",
    });
    const running = first.tick();
    await Promise.race([started, running]);
    failProbe = true;
    const other = createAsyncRuntime(database, options);
    expect(await other.tick()).toEqual({
      status: "liveness_unknown",
      jobId: "held",
      reason: "store_unavailable",
    });
    expect(await other.read("held")).toMatchObject({
      status: "found",
      job: { state: "running" },
    });
    failProbe = false;
    release();
    expect(await running).toMatchObject({
      status: "settled",
      job: { outcome: "completed" },
    });
  } finally {
    release();
    sqlite.close();
  }
});

it("rechecks the private lease before invoking an executor after a suspended claim", async () => {
  const sqlite = new DatabaseSync(":memory:");
  const Rows = Schema.Array(
    Schema.Record(
      Schema.String,
      Schema.Union([Schema.String, Schema.Number, Schema.Null]),
    ),
  );
  try {
    for (const sql of schemaStatements(false)) sqlite.exec(sql);
    let now = 1_000;
    let calls = 0;
    const database: SqlDatabase = {
      query: (statement) => {
        const rows = Schema.decodeUnknownSync(Rows)(
          sqlite
            .prepare(statement.text.replace(/\$(\d+)/gu, "?$1"))
            .all(...statement.values),
        );
        // Native claim committed; the owner resumes only after that claim expires.
        if (statement.text.includes("claim_token=CASE")) now += 120_001;
        return Promise.resolve(rows);
      },
    };
    const watchdog = createAsyncRuntime(database, {
      now: () => now,
      execute: () => {
        calls += 1;
        return Promise.resolve({ kind: "completed" });
      },
      wake: { recompute: () => Promise.resolve() },
    });
    await watchdog.ingest({
      jobId: "suspended",
      queue: "q",
      lane: "test",
      priority: 0,
      payload: null,
      state: "queued",
    });
    await expect(watchdog.tick()).rejects.toThrow("settlement rejected");
    expect(calls).toBe(0);
    expect(await watchdog.tick()).toMatchObject({
      status: "settled",
      job: { outcome: "interrupted" },
    });
  } finally {
    sqlite.close();
  }
});

it("rejects a refused recovery against an unchanged claim whose lease was renewed during the probe", async () => {
  const sqlite = new DatabaseSync(":memory:");
  const Rows = Schema.Array(
    Schema.Record(
      Schema.String,
      Schema.Union([Schema.String, Schema.Number, Schema.Null]),
    ),
  );
  try {
    for (const sql of schemaStatements(false)) sqlite.exec(sql);
    const database: SqlDatabase = {
      query: (statement) => {
        const rows = Schema.decodeUnknownSync(Rows)(
          sqlite
            .prepare(statement.text.replace(/\$(\d+)/gu, "?$1"))
            .all(...statement.values),
        );
        // The probe saw an expired lease, but the same owner renewed before the
        // recovery UPDATE. Its token and recovery generation did not change.
        if (statement.text.includes("AS alive"))
          sqlite
            .prepare(
              "UPDATE watchdog_jobs SET lease_until=121000 WHERE job_id=?",
            )
            .run("renewed");
        return Promise.resolve(rows);
      },
    };
    const watchdog = createAsyncRuntime(database, {
      now: () => 1000,
      execute: () =>
        Promise.reject(new Error("the live owner retains its execution")),
      wake: { recompute: () => Promise.resolve() },
    });
    await watchdog.ingest({
      jobId: "renewed",
      queue: "q",
      lane: "test",
      priority: 0,
      payload: null,
      state: "queued",
    });
    sqlite
      .prepare(
        "UPDATE watchdog_jobs SET state='running',claim_token='existing-owner',lease_until=0 WHERE job_id=?",
      )
      .run("renewed");
    await expect(watchdog.tick()).rejects.toMatchObject({
      cause: "settlement_rejected",
    });
    expect(await watchdog.read("renewed")).toMatchObject({
      status: "found",
      job: { state: "running" },
    });
  } finally {
    sqlite.close();
  }
});

it("atomically fails only the requested lane through the shared SQL runtime", async () => {
  const sqlite = new DatabaseSync(":memory:");
  const Rows = Schema.Array(
    Schema.Record(
      Schema.String,
      Schema.Union([Schema.String, Schema.Number, Schema.Null]),
    ),
  );
  try {
    for (const sql of schemaStatements(false)) sqlite.exec(sql);
    const database: SqlDatabase = {
      query: (statement) =>
        Promise.resolve(
          Schema.decodeUnknownSync(Rows)(
            sqlite
              .prepare(statement.text.replace(/\$(\d+)/gu, "?$1"))
              .all(...statement.values),
          ),
        ),
    };
    const runtime = createAsyncRuntime(database, {
      execute: () => Promise.resolve({ kind: "completed" }),
      wake: { recompute: () => Promise.resolve() },
    });
    const first = {
      jobId: "shared-refused",
      queue: "shared",
      lane: "member:refused",
      priority: 0,
      payload: null,
      state: "queued" as const,
    };
    const other = { ...first, jobId: "shared-other", lane: "member:other" };
    await runtime.ingest(first);
    await runtime.ingest(other);
    await expect(
      runtime.failLane({ lane: first.lane, reason: "access_changed" }),
    ).resolves.toEqual({ settled: 1, more: false });
    await expect(runtime.read(first.jobId)).resolves.toMatchObject({
      status: "found",
      job: {
        state: "settled",
        outcome: "failed",
        failureReason: "access_changed",
      },
    });
    await expect(runtime.read(other.jobId)).resolves.toMatchObject({
      status: "found",
      job: other,
    });
    await expect(
      runtime.failLane({ lane: first.lane, reason: "access_changed" }),
    ).resolves.toEqual({ settled: 0, more: false });
  } finally {
    sqlite.close();
  }
});

function sharedSqliteFixture() {
  const sqlite = new DatabaseSync(":memory:");
  for (const sql of schemaStatements(false)) sqlite.exec(sql);
  const rows = Schema.Array(
    Schema.Record(
      Schema.String,
      Schema.Union([Schema.String, Schema.Number, Schema.Null]),
    ),
  );
  const statements: string[] = [];
  const database: SqlDatabase = {
    query: (statement) => {
      statements.push(statement.text);
      return Promise.resolve(
        Schema.decodeUnknownSync(rows)(
          sqlite
            .prepare(statement.text.replace(/\$(\d+)/gu, "?$1"))
            .all(...statement.values),
        ),
      );
    },
  };
  return { sqlite, database, statements };
}

it("forgets an async claim when local lane refusal skips normal settlement", async () => {
  const fixture = sharedSqliteFixture();
  try {
    const state = openAsyncStore(fixture.database, () => 1000);
    await Effect.runPromise(
      state.store.ingest({
        jobId: "forgotten-claim",
        queue: "shared",
        lane: "member:refused",
        priority: 0,
        payload: null,
        state: "queued",
      }),
    );
    expect(
      await Effect.runPromise(state.store.selectAndClaim("claim-refused")),
    ).toMatchObject({ status: "claimed" });
    await Effect.runPromise(
      state.store.failLane({
        lane: "member:refused",
        reason: "access_changed",
      }),
    );
    const queries = fixture.statements.length;
    await expect(
      Effect.runPromise(state.renew("forgotten-claim")),
    ).rejects.toMatchObject({ cause: "claim_unavailable" });
    expect(fixture.statements.length).toBe(queries);
  } finally {
    fixture.sqlite.close();
  }
});

it("settles an async running claim before 300 older queued jobs and continues bounded batches", async () => {
  const fixture = sharedSqliteFixture();
  let release: () => void = () => undefined;
  let notify: () => void = () => undefined;
  const started = new Promise<void>((resolve) => {
    notify = resolve;
  });
  const held = new Promise<void>((resolve) => {
    release = resolve;
  });
  const runtime = createAsyncRuntime(fixture.database, {
    now: () => 1000,
    execute: async () => {
      notify();
      await held;
      return { kind: "completed" };
    },
    wake: { recompute: () => Promise.resolve() },
  });
  let ticking: Promise<unknown> = Promise.resolve();
  try {
    const job = {
      jobId: "queued0",
      queue: "queue0",
      lane: "member:refused",
      priority: 0,
      payload: null,
      state: "queued" as const,
    };
    for (let index = 0; index < 300; index++)
      await runtime.ingest({
        ...job,
        jobId: `queued${String(index)}`,
        queue: `queue${String(index)}`,
      });
    await runtime.ingest({
      ...job,
      jobId: "newer-running",
      queue: "running",
      priority: 1,
    });
    ticking = runtime.tick();
    await started;
    expect(
      await runtime.failLane({ lane: job.lane, reason: "access_changed" }),
    ).toEqual({ settled: 256, more: true });
    expect(await runtime.read("newer-running")).toMatchObject({
      status: "found",
      job: {
        state: "settled",
        outcome: "failed",
        failureReason: "access_changed",
      },
    });
    expect(await runtime.read("queued255")).toMatchObject({
      status: "found",
      job: { state: "queued" },
    });
    release();
    expect(await ticking).toMatchObject({
      status: "settled",
      job: { outcome: "failed", failureReason: "access_changed" },
    });
    expect(
      await runtime.failLane({ lane: job.lane, reason: "access_changed" }),
    ).toEqual({ settled: 45, more: false });
    expect(
      await runtime.failLane({ lane: job.lane, reason: "access_changed" }),
    ).toEqual({ settled: 0, more: false });
  } finally {
    release();
    await Promise.allSettled([ticking]);
    fixture.sqlite.close();
  }
});

it.each(["completed", "failed"] as const)(
  "fences a foreign async late %s report after lane refusal",
  async (kind) => {
    const fixture = sharedSqliteFixture();
    let release: () => void = () => undefined;
    let notify: () => void = () => undefined;
    const started = new Promise<void>((resolve) => {
      notify = resolve;
    });
    const held = new Promise<void>((resolve) => {
      release = resolve;
    });
    const old = createAsyncRuntime(fixture.database, {
      now: () => 1000,
      execute: async () => {
        notify();
        await held;
        return { kind };
      },
      wake: { recompute: () => Promise.resolve() },
    });
    const current = createAsyncRuntime(fixture.database, {
      now: () => 1000,
      execute: () => Promise.resolve({ kind: "completed" }),
      wake: { recompute: () => Promise.resolve() },
    });
    let ticking: Promise<unknown> = Promise.resolve();
    try {
      await old.ingest({
        jobId: "foreign-claim",
        queue: "shared",
        lane: "member:refused",
        priority: 0,
        payload: null,
        state: "queued",
      });
      ticking = old.tick();
      await started;
      expect(
        await current.failLane({
          lane: "member:refused",
          reason: "access_changed",
        }),
      ).toEqual({ settled: 1, more: false });
      release();
      await expect(ticking).rejects.toMatchObject({
        cause: "settlement_rejected",
      });
      expect(await current.read("foreign-claim")).toMatchObject({
        status: "found",
        job: {
          state: "settled",
          outcome: "failed",
          failureReason: "access_changed",
        },
      });
    } finally {
      release();
      await Promise.allSettled([ticking]);
      fixture.sqlite.close();
    }
  },
);

it("forgets a delayed claim reply after rereading the lane's committed refusal", async () => {
  const fixture = sharedSqliteFixture();
  let notify: () => void = () => undefined;
  let release: () => void = () => undefined;
  const claimed = new Promise<void>((resolve) => {
    notify = resolve;
  });
  const resume = new Promise<void>((resolve) => {
    release = resolve;
  });
  const database: SqlDatabase = {
    query: async (statement) => {
      const rows = await fixture.database.query(statement);
      if (statement.text.includes("claim_token=CASE")) {
        notify();
        await resume;
      }
      return rows;
    },
  };
  const state = openAsyncStore(database, () => 1000);
  let claiming: Promise<unknown> = Promise.resolve();
  try {
    await Effect.runPromise(
      state.store.ingest({
        jobId: "delayed-claim",
        queue: "shared",
        lane: "member:refused",
        priority: 0,
        payload: null,
        state: "queued",
      }),
    );
    claiming = Effect.runPromise(state.store.selectAndClaim("delayed-token"));
    await claimed;
    await Effect.runPromise(
      state.store.failLane({
        lane: "member:refused",
        reason: "access_changed",
      }),
    );
    release();
    expect(await claiming).toMatchObject({ status: "claimed" });
    expect(
      await Effect.runPromise(state.store.read("delayed-claim")),
    ).toMatchObject({
      status: "found",
      job: { state: "settled", outcome: "failed" },
    });
    const queries = fixture.statements.length;
    await expect(
      Effect.runPromise(state.renew("delayed-claim")),
    ).rejects.toMatchObject({ cause: "claim_unavailable" });
    expect(fixture.statements.length).toBe(queries);
  } finally {
    release();
    await Promise.allSettled([claiming]);
    fixture.sqlite.close();
  }
});

it.each(["queued", "missing"] as const)(
  "retains a live async claim when an earlier %s read snapshot arrives before heartbeat",
  async (snapshot) => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const fixture = sharedSqliteFixture();
    let readNotify: () => void = () => undefined;
    let readRelease: () => void = () => undefined;
    let jobNotify: () => void = () => undefined;
    let jobRelease: () => void = () => undefined;
    const readStarted = new Promise<void>((resolve) => {
      readNotify = resolve;
    });
    const resumeRead = new Promise<void>((resolve) => {
      readRelease = resolve;
    });
    const started = new Promise<void>((resolve) => {
      jobNotify = resolve;
    });
    const held = new Promise<void>((resolve) => {
      jobRelease = resolve;
    });
    let holdRead = false;
    let renewals = 0;
    const database: SqlDatabase = {
      query: async (statement) => {
        const rows = await fixture.database.query(statement);
        if (statement.text.includes("SET lease_until=")) renewals++;
        if (holdRead && statement.text.includes("WHERE job_id=$1")) {
          holdRead = false;
          readNotify();
          await resumeRead;
        }
        return rows;
      },
    };
    const runtime = createAsyncRuntime(database, {
      now: () => 1000,
      execute: async () => {
        jobNotify();
        await held;
        return { kind: "completed" };
      },
      wake: { recompute: () => Promise.resolve() },
    });
    let ticking: Promise<unknown> = Promise.resolve();
    let reading: Promise<unknown> = Promise.resolve();
    try {
      const job = {
        jobId: "queued-snapshot",
        queue: "shared",
        lane: "member:active",
        priority: 0,
        payload: null,
        state: "queued" as const,
      };
      if (snapshot === "queued") await runtime.ingest(job);
      holdRead = true;
      reading = runtime.read(job.jobId);
      await readStarted;
      if (snapshot === "missing") await runtime.ingest(job);
      ticking = runtime.tick();
      await started;
      readRelease();
      expect(await reading).toMatchObject(
        snapshot === "queued"
          ? { status: "found", job: { state: "queued" } }
          : { status: "missing" },
      );
      // Advance the maintained heartbeat timer, not a new test timeout or lease policy.
      await vi.advanceTimersByTimeAsync(heartbeatMs + 1);
      jobRelease();
      expect(await ticking).toMatchObject({
        status: "settled",
        job: { outcome: "completed" },
      });
      expect(renewals).toBe(2);
    } finally {
      readRelease();
      jobRelease();
      await Promise.allSettled([reading, ticking]);
      vi.useRealTimers();
      fixture.sqlite.close();
    }
  },
);
