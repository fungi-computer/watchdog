import { DatabaseSync } from "node:sqlite";
import * as Schema from "effect/Schema";
import { expect, it } from "vitest";
import { createAsyncRuntime } from "../src/internal/async-runtime.js";
import {
  schemaStatements,
  type SqlDatabase,
} from "../src/internal/async-sql.js";
import { WatchdogError } from "../src/index.js";

// A shared-database runtime (D1, Postgres) verifies the Watchdog schema before
// its first query. Billing opens a fresh runtime per handler on every tick, so
// the verification must be one per native database, not one per runtime.

const rows = Schema.decodeUnknownSync(
  Schema.Array(
    Schema.Record(
      Schema.String,
      Schema.Union([Schema.String, Schema.Number, Schema.Null]),
    ),
  ),
);

function sqlite(options: { schema?: boolean } = {}) {
  const native = new DatabaseSync(":memory:");
  const create = () => {
    for (const statement of schemaStatements(false)) native.exec(statement);
  };
  if (options.schema !== false) create();
  const log: string[] = [];
  const key = {};
  const open = (options: { shared?: boolean } = {}): SqlDatabase => ({
    dialect: "sqlite",
    shared: options.shared === false ? undefined : key,
    query: (statement) => {
      log.push(statement.text);
      const prepared = native.prepare(
        statement.text.replace(/\$(\d+)/gu, "?$1"),
      );
      return Promise.resolve(rows(prepared.all(...statement.values)));
    },
  });
  const runtime = (database: SqlDatabase, executed: string[] = []) =>
    createAsyncRuntime(database, {
      execute: (job) => {
        executed.push(job.jobId);
        return Promise.resolve({ kind: "completed" });
      },
      wake: { recompute: () => Promise.resolve() },
    });
  const verifications = () =>
    log.filter((text) => text.includes("FROM sqlite_schema")).length;
  return { native, create, log, open, runtime, verifications, key };
}

it("verifies the schema once for runtimes opened together on one database", async () => {
  const f = sqlite();
  const runtimes = Array.from({ length: 6 }, () => f.runtime(f.open()));
  await Promise.all(runtimes.map((runtime) => runtime.tick()));
  expect(f.verifications()).toBe(1);
  // Later ticks, and runtimes opened afterwards, add none.
  await Promise.all(runtimes.map((runtime) => runtime.tick()));
  await f.runtime(f.open()).tick();
  expect(f.verifications()).toBe(1);
  expect(f.log.filter((text) => text.includes("LIMIT 0"))).toHaveLength(1);
});

it("keeps one verification per runtime when no native database is shared", async () => {
  const f = sqlite();
  const runtimes = Array.from({ length: 4 }, () =>
    f.runtime(f.open({ shared: false })),
  );
  await Promise.all(runtimes.map((runtime) => runtime.tick()));
  expect(f.verifications()).toBe(4);
});

it("does not remember a failed verification: the next runtime verifies again", async () => {
  const f = sqlite({ schema: false });
  await expect(f.runtime(f.open()).tick()).rejects.toMatchObject({
    cause: "store_invalid",
  });
  await expect(f.runtime(f.open()).tick()).rejects.toBeInstanceOf(
    WatchdogError,
  );
  expect(f.verifications()).toBe(2);
  // The migration lands; the same database now verifies and is remembered.
  f.create();
  await expect(f.runtime(f.open()).tick()).resolves.toEqual({
    status: "no_ready",
  });
  await f.runtime(f.open()).tick();
  expect(f.verifications()).toBe(3);
});

it("claims each job exactly once when runtimes sharing a database tick together", async () => {
  const f = sqlite();
  const executed: string[] = [];
  const jobs = Array.from({ length: 7 }, (_, index) => `job-${String(index)}`);
  const ingest = f.runtime(f.open(), executed);
  for (const jobId of jobs)
    await ingest.ingest({
      jobId,
      queue: `queue-${jobId}`,
      lane: "lane",
      priority: 0,
      payload: null,
      state: "queued",
    });
  const drain = async () => {
    const runtime = f.runtime(f.open(), executed);
    for (let count = 0; count < 20; count++)
      if ((await runtime.tick()).status === "no_ready") return;
  };
  await Promise.all(Array.from({ length: 8 }, drain));
  expect([...executed].sort()).toEqual([...jobs].sort());
  for (const jobId of jobs)
    await expect(ingest.read(jobId)).resolves.toMatchObject({
      status: "found",
      job: { state: "settled", outcome: "completed" },
    });
});
