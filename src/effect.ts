import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { bindWatchdogEngine } from "./internal/runtime.js";
import { openSqliteState } from "./internal/sqlite-store.js";
import { WatchdogError } from "./internal/watchdog-error.js";
import { laneSchema } from "./internal/lane.js";

const BoundedString = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(256),
);
/** Closed-vocabulary code for why work failed; never free-form provider text. */
const FailureReason = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(200),
);
const Priority = Schema.Int.check(
  Schema.isGreaterThanOrEqualTo(Number.MIN_SAFE_INTEGER),
  Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
);
const RecoveryCount = Schema.Int.check(
  Schema.isGreaterThanOrEqualTo(0),
  Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
);
const RecoveryPolicy = Schema.Struct({
  maxRecoveries: RecoveryCount,
});
const PublicJobFields = {
  jobId: BoundedString,
  queue: BoundedString,
  lane: laneSchema,
  priority: Priority,
  payload: Schema.Unknown,
  recovery: Schema.optional(RecoveryPolicy),
};

/** The public queued-job grammar. Payload is deliberately uninterpreted. */
export const WatchdogQueuedJobSchema = Schema.Struct({
  ...PublicJobFields,
  state: Schema.Literal("queued"),
});
/** Generic durable work waiting for execution. */
export type WatchdogQueuedJob = Schema.Schema.Type<
  typeof WatchdogQueuedJobSchema
>;

/** The public terminal-job grammar. */
export const WatchdogSettledJobSchema = Schema.Struct({
  ...PublicJobFields,
  state: Schema.Literal("settled"),
  outcome: Schema.Literals([
    "completed",
    "failed",
    "cancelled",
    "outcome_unknown",
    "interrupted",
  ]),
  /** Why a job failed or exhausted disappearance recovery, when known. */
  failureReason: Schema.optionalKey(FailureReason),
});
/** Generic durable work with a terminal outcome. */
export type WatchdogSettledJob = Schema.Schema.Type<
  typeof WatchdogSettledJobSchema
>;

/** One bounded terminal-job recovery record with its durable ordering cursor. */
export type WatchdogSettledRecord = Readonly<{
  sequence: number;
  job: WatchdogSettledJob;
}>;

/** The public running-job grammar. Private claim data is deliberately absent. */
export const WatchdogRunningJobSchema = Schema.Struct({
  ...PublicJobFields,
  state: Schema.Literal("running"),
});
/** Generic durable work held by Watchdog's private claim. */
export type WatchdogRunningJob = Schema.Schema.Type<
  typeof WatchdogRunningJobSchema
>;

/** The complete public job lifecycle grammar. */
export const WatchdogStoredJobSchema = Schema.Union([
  WatchdogQueuedJobSchema,
  WatchdogRunningJobSchema,
  WatchdogSettledJobSchema,
]);
/** Any publicly readable lifecycle state for a generic job. */
export type WatchdogStoredJob = Schema.Schema.Type<
  typeof WatchdogStoredJobSchema
>;

/** Terminal decisions returned by the execution adapter. */
export const WatchdogExecutionDecisionSchema = Schema.Union([
  Schema.Struct({ kind: Schema.Literal("completed") }),
  Schema.Struct({
    kind: Schema.Literal("failed"),
    reason: Schema.optionalKey(FailureReason),
  }),
  Schema.Struct({ kind: Schema.Literal("cancelled") }),
  Schema.Struct({ kind: Schema.Literal("outcome_unknown") }),
]);
/** Terminal decision returned by the host executor. */
export type WatchdogExecutionDecision = Schema.Schema.Type<
  typeof WatchdogExecutionDecisionSchema
>;

/** Durable cancellation requested by the enclosing owner. */
export const WatchdogCancellationIntentSchema = Schema.Struct({
  jobId: BoundedString,
  cancellationId: BoundedString,
  onlyIfQueued: Schema.optional(Schema.Boolean),
});
/** Owner-issued durable cancellation intent. */
export type WatchdogCancellationIntent = Schema.Schema.Type<
  typeof WatchdogCancellationIntentSchema
>;

/** Result of recording an owner cancellation in its surrounding transaction. */
export type WatchdogCancellationDecision =
  | Readonly<{
      status: "recorded" | "replayed";
      intent: WatchdogCancellationIntent;
    }>
  | Readonly<{ status: "conflict" | "missing" }>;

/** Narrow synchronous seam for owner-atomic queue inspection, ingest, and cancellation. */
export type WatchdogTransactionalProjection = Readonly<{
  /** Reads the bounded current queue head inside an enclosing owner transaction. */
  readQueueHead: (queue: string) => WatchdogQueueHead;
  /** Reads the most recently settled job of one queue, if any. */
  readLatestSettledJob: (queue: string) => WatchdogSettledJob | undefined;
  /** Applies the same durable queued-row mutation as store.ingest. */
  enqueueAcceptedJob: (job: WatchdogQueuedJob) => void;
  /** Records or replays cancellation without interpreting the payload. */
  recordCancellationIntent: (
    intent: WatchdogCancellationIntent,
  ) => WatchdogCancellationDecision;
}>;

const CancellationReadSchema = Schema.Union([
  Schema.Struct({ status: Schema.Literal("absent") }),
  Schema.Struct({
    status: Schema.Literal("present"),
    cancellation: WatchdogCancellationIntentSchema,
  }),
]);
/** Results returned by public job inspection. */
export const WatchdogReadResultSchema = Schema.Union([
  Schema.Struct({
    status: Schema.Literal("found"),
    job: WatchdogStoredJobSchema,
    cancellation: CancellationReadSchema,
  }),
  Schema.Struct({ status: Schema.Literal("missing") }),
]);
/** Result of reading one generic job and its cancellation state. */
export type WatchdogReadResult = Schema.Schema.Type<
  typeof WatchdogReadResultSchema
>;

/** Bounded current-work projection for one generic queue. */
export const WatchdogQueueHeadSchema = Schema.Union([
  Schema.Struct({ state: Schema.Literal("idle") }),
  Schema.Struct({
    state: Schema.Literals(["queued", "running"]),
    jobId: BoundedString,
    cancellationRequested: Schema.Boolean,
  }),
]);
/** Current queued or running job for one queue, excluding terminal history. */
export type WatchdogQueueHead = Schema.Schema.Type<
  typeof WatchdogQueueHeadSchema
>;

/** One queue's state after a durable transition. */
export type WatchdogQueueTransition = Readonly<{
  queue: string;
  /** Current queued or running head after the transition. */
  head: WatchdogQueueHead;
  /** The job this transition settled, when it settled one. */
  settled?: WatchdogSettledJob;
}>;

/**
 * Owner projection written inside the same SQLite transaction as every queue
 * transition: ingest, claim, cancellation, settlement, recovery and purge. It
 * must be a deterministic owner-local write. A throw fails the transition
 * exactly like any other storage failure.
 */
export type WatchdogQueueProjector = (
  transition: WatchdogQueueTransition,
) => void;

/** Results returned by idempotent public ingest. */
export const WatchdogIngestResultSchema = Schema.Union([
  Schema.Struct({ status: Schema.Literal("queued") }),
  Schema.Struct({ status: Schema.Literal("duplicate") }),
]);
/** Result of idempotent durable ingest. */
export type WatchdogIngestResult = Schema.Schema.Type<
  typeof WatchdogIngestResultSchema
>;

/** Results returned by durable cancellation recording. */
export const WatchdogCancellationResultSchema = Schema.Union([
  Schema.Struct({ status: Schema.Literals(["recorded", "replayed"]) }),
  Schema.Struct({ status: Schema.Literals(["conflict", "missing"]) }),
]);
/** Result of recording a cancellation through the asynchronous runtime. */
export type WatchdogCancellationResult = Schema.Schema.Type<
  typeof WatchdogCancellationResultSchema
>;

/** Effect-native host executor for one generic running job. */
export type EffectWatchdogExecutor = Readonly<{
  /** Runs one generic claimed job until a terminal decision. */
  execute: (
    job: WatchdogRunningJob,
  ) => Effect.Effect<WatchdogExecutionDecision, WatchdogError>;
}>;

/** Scalar grammar shared by native SQLite adapters and their storage owners. */
export const watchdogSqliteValueSchema = Schema.Union([
  Schema.instanceOf(ArrayBuffer),
  Schema.String,
  Schema.Number,
  Schema.Null,
]);
/** A native row is a scalar record; each query owner decodes its own projection. */
export const watchdogSqliteRowSchema = Schema.Record(
  Schema.String,
  watchdogSqliteValueSchema,
);
/** Parsed native SQLite scalar shared by the durable storage adapters. */
export type WatchdogSqliteValue = Schema.Schema.Type<
  typeof watchdogSqliteValueSchema
>;
/** Parsed scalar record; query-specific columns require their owner schema. */
export type WatchdogSqliteRow = Schema.Schema.Type<
  typeof watchdogSqliteRowSchema
>;

/** Structural result cursor required by Watchdog's SQLite implementation. */
export type WatchdogSqliteCursor = Readonly<{
  /** Materialize raw rows, never a caller-selected projection type. */
  toArray: () => readonly WatchdogSqliteRow[];
}>;

/** Native owner-local SQLite capability consumed by Watchdog. */
export type WatchdogSqliteOwner = Readonly<{
  /** Synchronous SQLite execution owned by the surrounding runtime. */
  sql: Readonly<{
    /** Execute one statement; its query owner parses the returned rows. */
    exec: (
      statement: string,
      ...bindings: WatchdogSqliteValue[]
    ) => WatchdogSqliteCursor;
  }>;
  /** Join one synchronous native owner transaction. */
  transactionSync: <A>(operation: () => A) => A;
}>;

/** Effect-native construction inputs for the deep SQLite Watchdog. */
export type EffectWatchdogOptions = Readonly<{
  /** Optional opaque lane, 1 to 512 characters, with its own single-flight slot. */
  concurrentLane?: string;
  /** Owner-local SQLite and synchronous transaction capability. */
  owner: WatchdogSqliteOwner;
  /** Answers whether a running job is still alive. */
  isAlive: (job: WatchdogRunningJob) => Effect.Effect<boolean, WatchdogError>;
  executor: EffectWatchdogExecutor;
  /** Wake scheduling owned by the host. */
  wake: Readonly<{
    /** Recomputes the next wake from durable state. */
    recompute: () => Effect.Effect<void, WatchdogError>;
  }>;
  /** Optional owner projection written in each transition's transaction. */
  project?: WatchdogQueueProjector | undefined;
}>;

/**
 * A running job's liveness probe failed. The job is left exactly as it was:
 * Watchdog neither treats it as alive nor fabricates a disappearance.
 */
export type WatchdogLivenessUnknown = Readonly<{
  status: "liveness_unknown";
  jobId: string;
  /** The closed category of the probe's failure. */
  reason: WatchdogError["cause"];
}>;

/** Result of one bounded deep tick. */
export type WatchdogTickResult =
  | Readonly<{ status: "busy" }>
  | Readonly<{ status: "no_ready" }>
  | WatchdogLivenessUnknown
  | Readonly<{ status: "settled"; job: WatchdogSettledJob }>;

/** Owner-issued terminal lane refusal; reason is an owner code, at most 200 characters. */
export type WatchdogLaneFailure = Readonly<{
  /** Exact opaque lane, 1 to 512 characters. */
  lane: string;
  /** Owner-defined failure code, 1 to 200 characters; never provider text. */
  reason: string;
}>;

/** One bounded refusal batch; call again only while more is true. */
export type WatchdogLaneFailureResult = Readonly<{
  /** Number newly settled in this call, at most 256. */
  settled: number;
  /** Whether this lane still has queued or running work. */
  more: boolean;
}>;

/** Effect-native Watchdog runtime surface. */
export type WatchdogRuntimeEffect = Readonly<{
  /** Whether any queued or running job still needs owner reconciliation. */
  hasPendingWork: Effect.Effect<boolean, WatchdogError>;
  /** Persists one queued job. */
  ingest: (
    job: WatchdogQueuedJob,
  ) => Effect.Effect<WatchdogIngestResult, WatchdogError>;
  /** Reads one durable job. */
  read: (jobId: string) => Effect.Effect<WatchdogReadResult, WatchdogError>;
  /** Reads the bounded current head of one generic queue. */
  readQueueHead: (
    queue: string,
  ) => Effect.Effect<WatchdogQueueHead, WatchdogError>;
  /** Reads a bounded terminal prefix of the serial partition for owner recovery. */
  readSettled: (
    limit: number,
    afterSequence?: number,
  ) => Effect.Effect<readonly WatchdogSettledRecord[], WatchdogError>;
  /** Runs one bounded generic lifecycle tick. */
  tick: () => Effect.Effect<WatchdogTickResult, WatchdogError>;
  /** Records cancellation and signals live execution. */
  cancel: (
    input: WatchdogCancellationIntent,
  ) => Effect.Effect<WatchdogCancellationResult, WatchdogError>;
  /** Fail up to 256 lane jobs, running first, with transactional projection.
   * Refused claims are invalidated; both late success and failure reports are rejected.
   */
  failLane: (
    input: WatchdogLaneFailure,
  ) => Effect.Effect<WatchdogLaneFailureResult, WatchdogError>;
  /** Removes queued jobs in one queue. */
  purge: (queue: string) => Effect.Effect<void, WatchdogError>;
  /** Joins accepted enqueue and cancellation to an existing owner transaction. */
  transactional: WatchdogTransactionalProjection;
}>;

/** Runtime configured with one independent host lane at construction. */
export type ConcurrentWatchdogRuntimeEffect = WatchdogRuntimeEffect &
  Readonly<{
    /** Run one deep tick in the configured concurrent partition. */
    tickConcurrent: () => Effect.Effect<WatchdogTickResult, WatchdogError>;
  }>;

/** Construct two private single-flight slots over the same owner and job table. */
export function makeRuntime(
  options: EffectWatchdogOptions & { concurrentLane: string },
): Effect.Effect<ConcurrentWatchdogRuntimeEffect, WatchdogError>;
/** Construct a serial owner-local runtime. */
export function makeRuntime(
  options: EffectWatchdogOptions,
): Effect.Effect<WatchdogRuntimeEffect, WatchdogError>;
export function makeRuntime(
  options: EffectWatchdogOptions,
): Effect.Effect<
  WatchdogRuntimeEffect | ConcurrentWatchdogRuntimeEffect,
  WatchdogError
> {
  return Effect.try({
    try: () => {
      const lane =
        options.concurrentLane === undefined
          ? undefined
          : Schema.decodeUnknownSync(laneSchema)(options.concurrentLane);
      const state = openSqliteState(
        options.owner,
        options.project,
        lane === undefined ? undefined : { lane, include: false },
      );
      const serial = bindWatchdogEngine(options, state.store);
      const base = { ...serial, transactional: state.transactional };
      if (lane === undefined) return Object.freeze(base);
      const concurrent = bindWatchdogEngine(
        options,
        state.partition({ lane, include: true }),
      );
      return Object.freeze({
        ...base,
        tickConcurrent: concurrent.tick,
        failLane: (input: WatchdogLaneFailure) =>
          input.lane === lane
            ? concurrent.failLane(input)
            : serial.failLane(input),
        cancel: (intent: WatchdogCancellationIntent) =>
          serial
            .read(intent.jobId)
            .pipe(
              Effect.flatMap((found) =>
                found.status === "found" && found.job.lane === lane
                  ? concurrent.cancel(intent)
                  : serial.cancel(intent),
              ),
            ),
      });
    },
    catch: (error) => new WatchdogError("store_unavailable", error),
  });
}
