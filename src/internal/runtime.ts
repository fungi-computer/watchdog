import { parseRetryPolicy } from "./retry.js";
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
  let lifecycle: "open" | "stopping" | "stopped" = "open";
  let currentTick: Deferred.Deferred<undefined, WatchdogError> | undefined;
  let closing: Deferred.Deferred<undefined, WatchdogError> | undefined;
  const close = Effect.suspend(() => {
    if (closing !== undefined) return Deferred.await(closing);
    const done = Deferred.makeUnsafe<undefined, WatchdogError>();
    closing = done;
    lifecycle = "stopping";
    const pendingTick = currentTick;
    activeExecution?.fiber.interruptUnsafe();
    return (
      pendingTick === undefined
        ? Effect.succeed(undefined)
        : Deferred.await(pendingTick)
    ).pipe(
      Effect.onExit((exit) => {
        lifecycle = "stopped";
        return Deferred.done(
          done,
          Exit.map(exit, () => undefined),
        );
      }),
      Effect.uninterruptible,
    );
  });

  const policyFor = (job: WatchdogRunningJob) =>
    (options.executor.retryPolicy?.(job) ?? Effect.succeed(undefined)).pipe(
      Effect.flatMap((policy) =>
        policy === undefined
          ? Effect.succeed(undefined)
          : Effect.try({
              try: () => parseRetryPolicy(policy),
              catch: (error) => new WatchdogError("store_invalid", error),
            }),
      ),
    );
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
  const readSettled = (limit: number, afterSequence?: number, lane?: string) =>
    store.readSettled(limit, afterSequence, lane);
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

      const policy = yield* policyFor(claim.job);
      const result = yield* store.reconcileDisappearedClaim({
        ...claim,
        requeue:
          policy !== undefined ||
          options.executor.replaySafe?.(claim.job) === true,
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
  const finishCompletion = (claim: { jobId: string; claimToken: string }) =>
    store.finishCompletingClaim(claim).pipe(
      Effect.flatMap((result) =>
        result.status === "settled" || result.status === "replayed"
          ? options.wake.recompute().pipe(
              Effect.as<WatchdogTickResult>({
                status: "settled",
                job: result.job,
              }),
            )
          : Effect.fail(new WatchdogError("settlement_rejected")),
      ),
    );
  const tick = (): Effect.Effect<WatchdogTickResult, WatchdogError> =>
    Effect.suspend(() => {
      if (lifecycle !== "open") return Effect.succeed({ status: "stopped" });
      if (tickState !== "idle") return Effect.succeed({ status: "busy" });
      tickState = "running";
      const done = Deferred.makeUnsafe<undefined, WatchdogError>();
      currentTick = done;
      return Effect.gen(function* () {
        // An execution decision survives projection failure. Retry only its
        // transaction, once per tick; it is never a disappeared execution.
        const completing = yield* store.nextCompletingClaim();
        if (completing !== undefined)
          return yield* finishCompletion({
            jobId: completing.job.jobId,
            claimToken: completing.claimToken,
          });
        return yield* inspectDisappearedClaim().pipe(
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
              const policy = yield* policyFor(selection.job);
              const replaySafe =
                options.executor.replaySafe?.(selection.job) === true;
              const start = yield* Deferred.make<undefined>();
              const dispatch = { started: false };
              const fiber = yield* Effect.forkChild(
                Deferred.await(start).pipe(
                  Effect.andThen(
                    Effect.sync(() => {
                      dispatch.started = true;
                    }),
                  ),
                  Effect.andThen(
                    policy === undefined
                      ? Effect.succeed(undefined)
                      : store.reserveAttempt({
                          jobId: selection.job.jobId,
                          claimToken: selection.claimToken,
                          policy,
                        }),
                  ),
                  Effect.flatMap((attempt) =>
                    options.executor.execute(selection.job, attempt),
                  ),
                ),
              );
              const owned: ActiveExecution = {
                jobId: selection.job.jobId,
                fiber,
              };
              activeExecution = owned;
              const current = yield* store.read(selection.job.jobId);
              if (
                lifecycle !== "open" ||
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
                return yield* options.wake.recompute().pipe(
                  Effect.as({
                    status: "settled" as const,
                    job: owned.failed,
                  }),
                );
              if (lifecycle !== "open" && Exit.isFailure(execution)) {
                const drained =
                  Cause.hasInterruptsOnly(execution.cause) &&
                  (!dispatch.started ||
                    (yield* options.executor.isDrained?.(selection.job) ??
                      Effect.succeed(false)));
                if (!drained)
                  return yield* Effect.fail(new WatchdogError("drain_failed"));
                const handoff = yield* store.handoffExactClaim({
                  jobId: selection.job.jobId,
                  claimToken: selection.claimToken,
                });
                if (handoff.status === "requeued")
                  return yield* options.wake
                    .recompute()
                    .pipe(Effect.as<WatchdogTickResult>({ status: "stopped" }));
                if (handoff.status !== "cancelled")
                  return yield* Effect.fail(
                    new WatchdogError("settlement_rejected"),
                  );
              }
              const decision: WatchdogExecutionDecision = Exit.isSuccess(
                execution,
              )
                ? execution.value
                : { kind: "failed", reason: failureReasonOf(execution.cause) };
              if (
                (policy !== undefined &&
                  (decision.kind === "retry" || Exit.isFailure(execution))) ||
                (replaySafe && Exit.isFailure(execution))
              ) {
                const retryInput = {
                  jobId: selection.job.jobId,
                  claimToken: selection.claimToken,
                };
                if (policy !== undefined) Object.assign(retryInput, { policy });
                const retried = yield* store.retryExactClaim(retryInput);
                if (
                  retried.status === "requeued" &&
                  replaySafe &&
                  Exit.isFailure(execution)
                )
                  return yield* Effect.failCause(execution.cause);
                if (retried.status === "requeued")
                  return yield* options.wake.recompute().pipe(
                    Effect.as<WatchdogTickResult>({
                      status: "requeued",
                      jobId: selection.job.jobId,
                    }),
                  );
                if (retried.status === "conflict")
                  return yield* Effect.fail(
                    new WatchdogError("settlement_rejected"),
                  );
                // A recorded cancellation wins over retry and follows normal terminal settlement.
              }
              const settled = yield* store.recordExecutionDecision({
                jobId: selection.job.jobId,
                claimToken: selection.claimToken,
                decision:
                  decision.kind === "retry"
                    ? { kind: "failed", reason: "retry_policy_missing" }
                    : decision,
              });
              switch (settled.status) {
                case "recorded":
                case "replayed":
                  return yield* finishCompletion({
                    jobId: selection.job.jobId,
                    claimToken: selection.claimToken,
                  });
                case "conflict":
                case "missing":
                  return yield* Effect.fail(
                    new WatchdogError("settlement_rejected"),
                  );
              }
            });
          }),
        );
      }).pipe(
        Effect.onExit((exit) => {
          tickState = "idle";
          currentTick = undefined;
          return Deferred.done(
            done,
            Exit.map(exit, () => undefined),
          );
        }),
      );
    });

  return Object.freeze({
    close,
    wakeState: store.wakeState,
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
