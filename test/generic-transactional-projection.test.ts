import * as Effect from "effect/Effect";
import { expect, it } from "vitest";

import { makeRuntime, type WatchdogQueuedJob } from "../src/effect.js";
import { createSqliteOwnerFixture } from "./sqlite-owner.fixture.js";

const job: WatchdogQueuedJob = {
  jobId: "job-projection-1",
  queue: "arbitrary-queue",
  lane: "arbitrary-lane",
  priority: 0,
  payload: { opaque: ["value", 7] },
  state: "queued",
};

it("keeps owner-atomic ingest and cancellation generic and idempotent", async () => {
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
      }),
    );
    const intent = { jobId: job.jobId, cancellationId: "cancel-1" };
    const result = fixture.owner.transactionSync(() => {
      runtime.transactional.enqueueAcceptedJob(job);
      runtime.transactional.enqueueAcceptedJob(job);
      const head = runtime.transactional.readQueueHead(job.queue);
      return {
        head,
        first: runtime.transactional.recordCancellationIntent(intent),
        replay: runtime.transactional.recordCancellationIntent(intent),
        conflict: runtime.transactional.recordCancellationIntent({
          jobId: job.jobId,
          cancellationId: "cancel-2",
        }),
        missing: runtime.transactional.recordCancellationIntent({
          jobId: "missing-job",
          cancellationId: "cancel-3",
        }),
      };
    });

    expect(result).toEqual({
      head: {
        state: "queued",
        jobId: job.jobId,
        cancellationRequested: false,
      },
      first: { status: "recorded", intent },
      replay: { status: "replayed", intent },
      conflict: { status: "conflict" },
      missing: { status: "missing" },
    });
    await expect(Effect.runPromise(runtime.read(job.jobId))).resolves.toEqual({
      status: "found",
      job,
      cancellation: { status: "present", cancellation: intent },
    });
  } finally {
    fixture.close();
  }
});
