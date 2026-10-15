import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";

import * as core from "./effect.js";
import { adaptPromiseWatchdogHost } from "./internal/promise-watchdog-host.js";
import { WatchdogError } from "./internal/watchdog-error.js";

export { WatchdogError } from "./internal/watchdog-error.js";
export {
  WatchdogCancellationIntentSchema,
  WatchdogCancellationResultSchema,
  WatchdogExecutionDecisionSchema,
  WatchdogIngestResultSchema,
  WatchdogQueuedJobSchema,
  WatchdogQueueHeadSchema,
  WatchdogReadResultSchema,
  WatchdogRunningJobSchema,
  WatchdogSettledJobSchema,
  WatchdogStoredJobSchema,
} from "./effect.js";
export type {
  EffectWatchdogOptions,
  EffectWatchdogExecutor,
  WatchdogCancellationDecision,
  WatchdogCancellationIntent,
  WatchdogCancellationResult,
  WatchdogExecutionDecision,
  WatchdogIngestResult,
  WatchdogLivenessUnknown,
  WatchdogLaneFailure,
  WatchdogLaneFailureResult,
  WatchdogQueuedJob,
  WatchdogQueueHead,
  WatchdogQueueProjector,
  WatchdogQueueTransition,
  WatchdogReadResult,
  WatchdogRunningJob,
  WatchdogRuntimeEffect,
  ConcurrentWatchdogRuntimeEffect,
  WatchdogSettledJob,
  WatchdogSqliteCursor,
  WatchdogSqliteOwner,
  WatchdogSqliteValue,
  WatchdogSqliteRow,
  WatchdogStoredJob,
  WatchdogTickResult,
  WatchdogTransactionalProjection,
} from "./effect.js";

/** Promise host executor for one generic running job. */
export type WatchdogExecutor = (
  job: core.WatchdogRunningJob,
  signal: AbortSignal,
) => Promise<core.WatchdogExecutionDecision>;

/** Options for constructing the Promise facade. */
export type WatchdogOptions = Readonly<{
  /** Optional opaque lane with its own single-flight execution slot. */
  concurrentLane?: string;
  /** Owner-local SQLite and synchronous transaction capability. */
  owner: core.WatchdogSqliteOwner;
  /** Owner liveness probe for disappearance recovery. */
  isAlive: (job: core.WatchdogRunningJob) => Promise<boolean>;
  /** Executes one generic claimed job. */
  execute: WatchdogExecutor;
  /** Recomputes the owner wake after durable state changes. */
  wake: Readonly<{
    /** Arms the next wake from current owner state. */
    recompute: () => Promise<void>;
  }>;
  /** Optional owner projection written in each transition's transaction. */
  project?: core.WatchdogQueueProjector;
}>;

/** Promise facade over the canonical Effect runtime. */
export type WatchdogRuntime = Readonly<{
  /** Whether any queued or running job still needs owner reconciliation. */
  hasPendingWork: () => Promise<boolean>;
  /** Persists one queued job. */
  ingest: (job: core.WatchdogQueuedJob) => Promise<core.WatchdogIngestResult>;
  /** Reads one durable job. */
  read: (jobId: string) => Promise<core.WatchdogReadResult>;
  /** Reads the bounded current head of one generic queue. */
  readQueueHead: (queue: string) => Promise<core.WatchdogQueueHead>;
  /** Runs one bounded generic lifecycle tick. */
  tick: () => Promise<core.WatchdogTickResult>;
  /** Records cancellation and signals a live execution when present. */
  cancel: (
    input: core.WatchdogCancellationIntent,
  ) => Promise<core.WatchdogCancellationResult>;
  /** Terminally fail a bounded lane batch; repeat while more is true.
   * Refused claims are invalidated; both late success and failure reports are rejected.
   */
  failLane: (
    input: core.WatchdogLaneFailure,
  ) => Promise<core.WatchdogLaneFailureResult>;
  /** Removes queued jobs in one queue. */
  purge: (queue: string) => Promise<void>;
  /** Joins accepted enqueue and cancellation to an existing owner transaction. */
  transactional: core.WatchdogTransactionalProjection;
}>;

/** Promise runtime with the construction-time concurrent lane. */
export type ConcurrentWatchdogRuntime = WatchdogRuntime &
  Readonly<{
    /** Run one deep tick in the configured concurrent partition. */
    tickConcurrent: () => Promise<core.WatchdogTickResult>;
  }>;

/** Factory for the Promise facade. */
export type WatchdogApi = Readonly<{
  /** Constructs one Promise facade over the Effect runtime. */
  make: {
    (
      options: WatchdogOptions & { concurrentLane: string },
    ): Promise<ConcurrentWatchdogRuntime>;
    (options: WatchdogOptions): Promise<WatchdogRuntime>;
  };
}>;

const runPromise = <A>(effect: Effect.Effect<A, WatchdogError>): Promise<A> =>
  Effect.runPromiseExit(effect).then((exit) => {
    if (Exit.isSuccess(exit)) return exit.value;
    const failure = Option.getOrUndefined(Cause.findErrorOption(exit.cause));
    const error = failure ?? Cause.squash(exit.cause);
    return Promise.reject(
      error instanceof Error ? error : new Error(String(error)),
    );
  });

function make(
  options: WatchdogOptions & { concurrentLane: string },
): Promise<ConcurrentWatchdogRuntime>;
function make(options: WatchdogOptions): Promise<WatchdogRuntime>;
function make(
  options: WatchdogOptions,
): Promise<WatchdogRuntime | ConcurrentWatchdogRuntime> {
  const adapted = adaptPromiseWatchdogHost(options);
  if (options.concurrentLane === undefined)
    return runPromise(core.makeRuntime(adapted)).then(promiseRuntime);
  return runPromise(
    core.makeRuntime({ ...adapted, concurrentLane: options.concurrentLane }),
  ).then((runtime) =>
    Object.freeze({
      ...promiseRuntime(runtime),
      tickConcurrent: () => runPromise(runtime.tickConcurrent()),
    }),
  );
}

function promiseRuntime(runtime: core.WatchdogRuntimeEffect): WatchdogRuntime {
  return Object.freeze({
    hasPendingWork: () => runPromise(runtime.hasPendingWork),
    ingest: (job) => runPromise(runtime.ingest(job)),
    read: (jobId) => runPromise(runtime.read(jobId)),
    readQueueHead: (queue) => runPromise(runtime.readQueueHead(queue)),
    tick: () => runPromise(runtime.tick()),
    cancel: (input) => runPromise(runtime.cancel(input)),
    failLane: (input) => runPromise(runtime.failLane(input)),
    purge: (queue) => runPromise(runtime.purge(queue)),
    transactional: runtime.transactional,
  });
}

/** Promise facade over the canonical Effect-owned runtime. */
export const Watchdog: WatchdogApi = Object.freeze({ make });

export {
  watchdogSqliteValueSchema,
  watchdogSqliteRowSchema,
} from "./effect.js";
