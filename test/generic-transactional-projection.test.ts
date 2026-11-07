import * as Effect from "effect/Effect";
import { expect, it } from "vitest";

import {
  makeRuntime,
  type WatchdogQueuedJob,
  type WatchdogRuntimeEffect,
} from "../src/effect.js";
import { createSqliteOwnerFixture } from "./sqlite-owner.fixture.js";

const job: WatchdogQueuedJob = {
  jobId: "job-projection-1",
  queue: "arbitrary-queue",
  lane: "arbitrary-lane",
  priority: 0,
  payload: { opaque: ["value", 7] },
  state: "queued",
};

it.each([0, 1_000])(
  "reads the exact job lifecycle and deadline %i inside the owner transaction",
  async (notBefore) => {
    const fixture = createSqliteOwnerFixture();
    let refuseSettlement = true;
    let runningJob: ReturnType<
      WatchdogRuntimeEffect["transactional"]["readJob"]
    >;
    try {
      const runtime: WatchdogRuntimeEffect = await Effect.runPromise(
        makeRuntime({
          owner: fixture.owner,
          executor: {
            execute: () =>
              Effect.sync(() => {
                runningJob = runtime.transactional.readJob(job.jobId);
                return { kind: "completed" as const };
              }),
          },
          isAlive: () => Effect.succeed(true),
          wake: { recompute: () => Effect.void },
          project: ({ settled }) => {
            if (settled !== undefined && refuseSettlement)
              throw new Error("projection unavailable");
          },
        }),
      );
      expect(runtime.transactional.readJob(job.jobId)).toBeUndefined();
      fixture.owner.transactionSync(() => {
        runtime.transactional.enqueueAcceptedJob(
          notBefore === 0 ? job : { ...job, notBefore },
        );
        expect(runtime.transactional.readJob(job.jobId)).toEqual({
          state: "queued",
          notBefore,
        });
      });
      const first = await Effect.runPromiseExit(runtime.tick());
      expect(first._tag).toBe("Failure");
      expect(runningJob).toEqual({ state: "running", notBefore });
      expect(runtime.transactional.readJob(job.jobId)).toEqual({
        state: "completing",
        notBefore,
      });
      refuseSettlement = false;
      await Effect.runPromise(runtime.tick());
      expect(runtime.transactional.readJob(job.jobId)).toEqual({
        state: "settled",
        notBefore,
      });
    } finally {
      fixture.close();
    }
  },
);

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
