import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import type {
  EffectWatchdogOptions,
  WatchdogRuntimeEffect,
  WatchdogLaneFailure,
  WatchdogExecutionDecision,
  WatchdogQueuedJob,
  WatchdogRunningJob,
  WatchdogSettledJob,
  WatchdogCancellationIntent,
  WatchdogLivenessUnknown,
  WatchdogTickResult,
} from "../effect.js";
import type { WatchdogStore } from "./sqlite-store.js";
import { WatchdogError } from "./watchdog-error.js";
import { watchdogIds } from "./identities.js";

type ActiveExecution = {
  readonly jobId: string;
  readonly fiber: Fiber.Fiber<WatchdogExecutionDecision, WatchdogError>;
  // Durable terminal refusal won the race; tick owns cleanup, not a second settlement.
  failed?: WatchdogSettledJob;
};

/**
 * The engine's own reason for an executor exit that returned no decision: the
 * executor's typed failure code, an unexpected defect, or an interruption.
 */
function failureReasonOf(cause: Cause.Cause<WatchdogError>): string {
  if (Cause.hasInterruptsOnly(cause)) return "execution_interrupted";
  const failure = Cause.findErrorOption(cause);
  return failure._tag === "Some" ? failure.value.cause : "execution_defect";
}

type Selection =
  | Readonly<{ status: "no_ready" }>
  | WatchdogLivenessUnknown
  | Readonly<{ status: "claim_unavailable" }>
  | Readonly<{ status: "cancelled"; job: WatchdogSettledJob }>
  | Readonly<{ status: "recovered"; job: WatchdogSettledJob }>
  | Readonly<{
      status: "claimed";
      job: WatchdogRunningJob;
      claimToken: string;
    }>;

/** What inspecting the next running claim for disappearance concluded. */
type Inspection =
  | Readonly<{ status: "none" }>
  | Readonly<{ status: "recovered"; job: WatchdogSettledJob }>
  | WatchdogLivenessUnknown;

export const bindWatchdogEngine = (
  options: Pick<EffectWatchdogOptions, "isAlive" | "executor" | "wake">,
  store: WatchdogStore,
): Omit<WatchdogRuntimeEffect, "transactional"> => {
  let tickState: "idle" | "running" = "idle";
  let activeExecution: ActiveExecution | undefined;

  const ingest = (job: WatchdogQueuedJob) => store.ingest(job);
  const read = (jobId: string) =>
    store.read(jobId).pipe(
      Effect.map((result) =>
        result.status === "missing"
          ? result
          : {
              status: "found" as const,
              job: result.job,
              cancellation: result.cancellation,
            },
      ),
    );
  const readQueueHead = (queue: string) => store.readQueueHead(queue);
  const readSettled = (limit: number, afterSequence?: number) =>
    store.readSettled(limit, afterSequence);
  const cancel = (input: WatchdogCancellationIntent) =>
    store.recordCancellation(input).pipe(
      Effect.tap((result) => {
        if (result.status !== "recorded" && result.status !== "replayed")
          return Effect.void;
        const active = activeExecution;
        return active?.jobId === input.jobId
          ? Effect.sync(() => {
              // Tick retains the fiber and awaits cleanup before settlement.
              active.fiber.interruptUnsafe();
            })
          : options.wake.recompute();
      }),
    );
  const failLane = (input: WatchdogLaneFailure) =>
    store.failLane(input).pipe(
      Effect.tap((result) => {
        const active = activeExecution;
        const failed = result.jobs.find((job) => job.jobId === active?.jobId);
        if (active !== undefined && failed !== undefined) {
          active.failed = failed;
          active.fiber.interruptUnsafe();
        }
        return result.jobs.length === 0
          ? Effect.void
          : options.wake.recompute();
      }),
      Effect.map(({ jobs, more }) => ({ settled: jobs.length, more })),
    );
  const purge = (queue: string) => store.purgeQueued(queue);
  const claimNext = (): Effect.Effect<Selection, WatchdogError> =>
    Effect.try({
      try: () => watchdogIds.claim.generate(),
      catch: () => new WatchdogError("claim_unavailable"),
    }).pipe(Effect.flatMap((claimToken) => store.selectAndClaim(claimToken)));
  const inspectDisappearedClaim = (): Effect.Effect<
    Inspection,
    WatchdogError
  > => {
    return Effect.gen(function* () {
      const claim = yield* store.nextRunningClaim();
      if (claim === undefined) return { status: "none" } as const;

      const probe = yield* Effect.result(options.isAlive(claim.job));
      // A failed probe is not an answer: never alive, never dead.
      if (probe._tag === "Failure")
        return {
          status: "liveness_unknown",
          jobId: claim.job.jobId,
          reason: probe.failure.cause,
        } as const;
      if (probe.success) return { status: "none" } as const;

      const disposition: "requeue" | "interrupted" =
        claim.job.recovery !== undefined &&
        claim.recoveryCount < claim.job.recovery.maxRecoveries
          ? "requeue"
          : "interrupted";
      const result = yield* store.reconcileDisappearedClaim({
        ...claim,
        disposition,
      });
      switch (result.status) {
        case "requeued":
          yield* options.wake.recompute();
          return { status: "none" } as const;
        case "settled":
          yield* options.wake.recompute();
          return { status: "recovered", job: result.job } as const;
        case "conflict":
        case "missing": {
          // This is an observer's recovery attempt, not its own execution claim.
          // Another owner can advance the stored generation during the probe.
          const current = yield* store.read(claim.job.jobId);
          if (
            current.status === "found" &&
            (current.job.state !== "running" ||
              current.claimToken !== claim.claimToken ||
              current.recoveryCount !== claim.recoveryCount)
          )
            return { status: "none" } as const;
          return yield* Effect.fail(new WatchdogError("settlement_rejected"));
        }
      }
    });
  };
  const tick = (): Effect.Effect<WatchdogTickResult, WatchdogError> =>
    Effect.suspend(() => {
      if (tickState !== "idle") return Effect.succeed({ status: "busy" });
      tickState = "running";
      return inspectDisappearedClaim().pipe(
        Effect.flatMap((inspection) => {
          if (inspection.status === "recovered")
            return Effect.succeed<Selection>({
              status: "recovered",
              job: inspection.job,
            });
          // Other work still proceeds; only an otherwise idle tick has
          // nothing to say but the unanswered probe.
          return claimNext().pipe(
            Effect.map((selection): Selection =>
              selection.status === "no_ready" &&
              inspection.status === "liveness_unknown"
                ? inspection
                : selection,
            ),
          );
        }),
        Effect.flatMap((selection: Selection) => {
          if (selection.status === "no_ready")
            return Effect.succeed<WatchdogTickResult>({ status: "no_ready" });
          if (selection.status === "liveness_unknown")
            return Effect.succeed<WatchdogTickResult>(selection);
          if (selection.status === "claim_unavailable")
            return Effect.fail(new WatchdogError("claim_unavailable"));
          if (selection.status === "cancelled")
            return options.wake.recompute().pipe(
              Effect.as<WatchdogTickResult>({
                status: "settled",
                job: selection.job,
              }),
            );
          if (selection.status === "recovered")
            return Effect.succeed<WatchdogTickResult>({
              status: "settled",
              job: selection.job,
            });

          return Effect.gen(function* () {
            const start = yield* Deferred.make<undefined>();
            const fiber = yield* Effect.forkChild(
              Deferred.await(start).pipe(
                Effect.andThen(options.executor.execute(selection.job)),
              ),
            );
            const owned: ActiveExecution = {
              jobId: selection.job.jobId,
              fiber,
            };
            activeExecution = owned;
            const current = yield* store.read(selection.job.jobId);
            if (
              current.status !== "found" ||
              current.job.state !== "running" ||
              current.cancellation.status === "present"
            ) {
              if (
                current.status === "found" &&
                current.job.state === "settled" &&
                current.job.outcome === "failed"
              )
                owned.failed = current.job;
              yield* Fiber.interrupt(fiber);
            } else yield* Deferred.succeed(start, undefined);
            const execution = yield* Fiber.await(fiber).pipe(
              Effect.ensuring(
                Effect.sync(() => {
                  if (activeExecution?.fiber === fiber)
                    activeExecution = undefined;
                }),
              ),
            );
            if (owned.failed !== undefined)
              return yield* options.wake
                .recompute()
                .pipe(
                  Effect.as({ status: "settled" as const, job: owned.failed }),
                );
            const decision: WatchdogExecutionDecision = Exit.isSuccess(
              execution,
            )
              ? execution.value
              : { kind: "failed", reason: failureReasonOf(execution.cause) };
            const settled = yield* store.settleExactClaim({
              jobId: selection.job.jobId,
              claimToken: selection.claimToken,
              decision,
            });
            switch (settled.status) {
              case "settled":
              case "replayed":
                return yield* options.wake.recompute().pipe(
                  Effect.as({
                    status: "settled" as const,
                    job: settled.job,
                  }),
                );
              case "conflict":
              case "missing":
                return yield* Effect.fail(
                  new WatchdogError("settlement_rejected"),
                );
            }
          });
        }),
        Effect.ensuring(
          Effect.sync(() => {
            tickState = "idle";
          }),
        ),
      );
    });

  return Object.freeze({
    hasPendingWork: store.hasPendingWork,
    ingest,
    read,
    readQueueHead,
    readSettled,
    tick,
    cancel,
    failLane,
    purge,
  });
};
