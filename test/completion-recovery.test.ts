import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it } from "vitest";

import { Watchdog, type WatchdogQueuedJob } from "../src/index.js";
import { createSqliteOwnerFixture } from "./sqlite-owner.fixture.js";

const job: WatchdogQueuedJob = {
  jobId: "completed-work",
  queue: "session",
  lane: "default",
  priority: 0,
  payload: { opaque: true },
  recovery: { maxRecoveries: 0 },
  state: "queued",
};

it("reopens a decided result after failed projection without executing or charging disappearance", async () => {
  const directory = await mkdtemp(join(tmpdir(), "watchdog-completion-"));
  const path = join(directory, "owner.sqlite");
  let fixture = createSqliteOwnerFixture(path);
  let executions = 0;
  let probes = 0;
  let refuseProjection = true;
  const open = () =>
    Watchdog.make({
      owner: fixture.owner,
      execute: () => {
        executions += 1;
        return Promise.resolve({ kind: "completed" });
      },
      isAlive: () => {
        probes += 1;
        return Promise.resolve(false);
      },
      wake: { recompute: () => Promise.resolve() },
      project: ({ settled }) => {
        if (settled === undefined) return;
        fixture.owner.sql.exec("INSERT INTO replies VALUES (?)", settled.jobId);
        if (refuseProjection) throw new Error("reply staging unavailable");
      },
    });
  try {
    fixture.owner.sql.exec("CREATE TABLE replies (job_id TEXT PRIMARY KEY)");
    const first = await open();
    await first.ingest(job);
    await first.ingest({ ...job, jobId: "later-work" });
    await expect(first.tick()).rejects.toMatchObject({
      cause: "store_unavailable",
    });
    expect(executions).toBe(1);
    await expect(first.read(job.jobId)).resolves.toMatchObject({
      job: { state: "completing", outcome: "completed" },
    });
    expect(fixture.owner.sql.exec("SELECT * FROM replies").toArray()).toEqual(
      [],
    );
    fixture.close();
    fixture = createSqliteOwnerFixture(path);
    const reopened = await open();
    for (let failure = 0; failure < 3; failure += 1)
      await expect(reopened.tick()).rejects.toMatchObject({
        cause: "store_unavailable",
      });
    expect(executions).toBe(1);
    expect(probes).toBe(0);
    await expect(reopened.read("later-work")).resolves.toMatchObject({
      job: { state: "queued" },
    });
    expect(fixture.owner.sql.exec("SELECT * FROM replies").toArray()).toEqual(
      [],
    );
    refuseProjection = false;
    await expect(reopened.tick()).resolves.toMatchObject({
      status: "settled",
      job: { jobId: job.jobId, outcome: "completed" },
    });
    expect(executions).toBe(1);
    expect(fixture.owner.sql.exec("SELECT * FROM replies").toArray()).toEqual([
      { job_id: job.jobId },
    ]);
    await expect(reopened.tick()).resolves.toMatchObject({
      status: "settled",
      job: { jobId: "later-work" },
    });
    expect(executions).toBe(2);
  } finally {
    fixture.close();
    await rm(directory, { recursive: true, force: true });
  }
});

it("reports the decided outcome instead of cancelling after execution completed", async () => {
  const fixture = createSqliteOwnerFixture();
  let refuseProjection = true;
  try {
    const runtime = await Watchdog.make({
      owner: fixture.owner,
      execute: () => Promise.resolve({ kind: "completed" }),
      isAlive: () => Promise.resolve(false),
      wake: { recompute: () => Promise.resolve() },
      project: ({ settled }) => {
        if (settled !== undefined && refuseProjection)
          throw new Error("projection unavailable");
      },
    });
    await runtime.ingest(job);
    await expect(runtime.tick()).rejects.toMatchObject({
      cause: "store_unavailable",
    });
    await expect(
      runtime.cancel({ jobId: job.jobId, cancellationId: "late-cancel" }),
    ).resolves.toEqual({
      status: "decided",
      outcome: "completed",
    });
    await expect(runtime.read(job.jobId)).resolves.toMatchObject({
      cancellation: { status: "absent" },
    });
    refuseProjection = false;
    await expect(runtime.tick()).resolves.toMatchObject({
      status: "settled",
      job: { outcome: "completed" },
    });
  } finally {
    fixture.close();
  }
});

it("refuses the obsolete lifecycle before execution or schema mutation", async () => {
  const fixture = createSqliteOwnerFixture();
  let executions = 0;
  try {
    fixture.owner.sql.exec(
      "CREATE TABLE watchdog_jobs (job_id TEXT, state TEXT CHECK(state IN ('queued', 'running', 'settled')))",
    );
    fixture.owner.sql.exec(
      "INSERT INTO watchdog_jobs VALUES ('retained', 'queued')",
    );
    await expect(
      Watchdog.make({
        owner: fixture.owner,
        execute: () => {
          executions += 1;
          return Promise.resolve({ kind: "completed" });
        },
        isAlive: () => Promise.resolve(false),
        wake: { recompute: () => Promise.resolve() },
      }),
    ).rejects.toMatchObject({ cause: "store_invalid" });
    expect(executions).toBe(0);
    expect(
      fixture.owner.sql.exec("SELECT * FROM watchdog_jobs").toArray(),
    ).toEqual([{ job_id: "retained", state: "queued" }]);
  } finally {
    fixture.close();
  }
});
