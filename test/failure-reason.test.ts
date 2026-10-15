import * as Effect from "effect/Effect";
import { expect, it } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  makeRuntime,
  type WatchdogExecutionDecision,
  type WatchdogQueuedJob,
} from "../src/effect.js";
import { WatchdogError } from "../src/index.js";
import { createSqliteOwnerFixture } from "./sqlite-owner.fixture.js";

const job = (suffix: string): WatchdogQueuedJob => ({
  jobId: `job-reason-${suffix}`,
  queue: "arbitrary-queue",
  lane: "arbitrary-lane",
  priority: 0,
  payload: { opaque: suffix },
  state: "queued",
});

/** Run one job through a fresh runtime and return its durable settled record. */
async function settle(
  suffix: string,
  execute: () => Effect.Effect<WatchdogExecutionDecision, WatchdogError>,
  prepare?: (fixture: ReturnType<typeof createSqliteOwnerFixture>) => void,
) {
  const fixture = createSqliteOwnerFixture();
  try {
    prepare?.(fixture);
    const queued = job(suffix);
    const runtime = await Effect.runPromise(
      makeRuntime({
        owner: fixture.owner,
        executor: { execute },
        isAlive: () => Effect.succeed(true),
        wake: { recompute: () => Effect.void },
      }),
    );
    await Effect.runPromise(runtime.ingest(queued));
    await Effect.runPromise(runtime.tick());
    const read = await Effect.runPromise(runtime.read(queued.jobId));
    const settled = await Effect.runPromise(runtime.readSettled(10));
    return { read, settled };
  } finally {
    fixture.close();
  }
}

it("records the executor's reason on a failed job", async () => {
  const { read, settled } = await settle("executor", () =>
    Effect.succeed({ kind: "failed" as const, reason: "store:rejected" }),
  );
  expect(read).toMatchObject({
    status: "found",
    job: {
      state: "settled",
      outcome: "failed",
      failureReason: "store:rejected",
    },
  });
  expect(settled[0]?.job).toMatchObject({ failureReason: "store:rejected" });
});

it("records why the executor itself failed instead of a bare failure", async () => {
  const typed = await settle("typed", () =>
    Effect.fail(new WatchdogError("execution_failed")),
  );
  expect(typed.read).toMatchObject({
    job: { outcome: "failed", failureReason: "execution_failed" },
  });

  const defect = await settle("defect", () =>
    Effect.die(new Error("provider text that must not be persisted")),
  );
  expect(defect.read).toMatchObject({
    job: { outcome: "failed", failureReason: "execution_defect" },
  });
  expect(JSON.stringify(defect.settled)).not.toContain("provider text");
});

it("leaves no failure reason on jobs that did not fail", async () => {
  const { read } = await settle("completed", () =>
    Effect.succeed({ kind: "completed" as const }),
  );
  expect(read).toMatchObject({ job: { outcome: "completed" } });
  expect(read).not.toHaveProperty("job.failureReason");
});

it("retains a failed job's reason when its current store is reopened", async () => {
  const root = mkdtempSync(join(tmpdir(), "watchdog-failure-reason-"));
  const filename = join(root, "watchdog.sqlite");
  try {
    const first = createSqliteOwnerFixture(filename);
    try {
      const queued = job("reopen");
      const runtime = await Effect.runPromise(
        makeRuntime({
          owner: first.owner,
          executor: {
            execute: () =>
              Effect.succeed({
                kind: "failed" as const,
                reason: "runtime:rejected",
              }),
          },
          isAlive: () => Effect.succeed(true),
          wake: { recompute: () => Effect.void },
        }),
      );
      await Effect.runPromise(runtime.ingest(queued));
      await Effect.runPromise(runtime.tick());
    } finally {
      first.close();
    }

    const reopened = createSqliteOwnerFixture(filename);
    try {
      const runtime = await Effect.runPromise(
        makeRuntime({
          owner: reopened.owner,
          executor: {
            execute: () => Effect.succeed({ kind: "completed" as const }),
          },
          isAlive: () => Effect.succeed(true),
          wake: { recompute: () => Effect.void },
        }),
      );
      expect(
        await Effect.runPromise(runtime.read("job-reason-reopen")),
      ).toMatchObject({
        status: "found",
        job: { outcome: "failed", failureReason: "runtime:rejected" },
      });
    } finally {
      reopened.close();
    }
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
