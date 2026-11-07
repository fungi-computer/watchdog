import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { reserveRetry, retryDue } from "./retry.js";
import { disappearanceRecovery } from "./recovery.js";
import type {
  WatchdogCompletingJob,
  WatchdogLaneFailure,
  WatchdogCancellationDecision,
  WatchdogCancellationIntent,
  WatchdogCancellationResult,
  WatchdogExecutionDecision,
  WatchdogIngestResult,
  WatchdogQueuedJob,
  WatchdogQueueHead,
  WatchdogQueueProjector,
  WatchdogReadResult,
  WatchdogRunningJob,
  WatchdogSettledJob,
  WatchdogSettledRecord,
  WatchdogSqliteOwner,
  WatchdogSqliteRow,
  WatchdogStoredJob,
  WatchdogTransactionalProjection,
  WatchdogWakeState,
  WatchdogRetryPolicy,
  WatchdogAttempt,
} from "../effect.js";
import { WatchdogError } from "./watchdog-error.js";
import { laneSchema } from "./lane.js";

export type { WatchdogSqliteRow } from "../effect.js";

const Identifier = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(256),
);
const Priority = Schema.Int.check(
  Schema.isGreaterThanOrEqualTo(Number.MIN_SAFE_INTEGER),
  Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
);
const Count = Schema.Int.check(
  Schema.isGreaterThanOrEqualTo(0),
  Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
);
const FailureReason = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(200),
);
const LaneFailureSchema = Schema.Struct({
  lane: laneSchema,
  reason: FailureReason,
});
// A transition batch, not a lifetime limit on lane work; matches terminal reads.
export const laneFailureBatchSize = 256;

/** One strict owner input grammar shared by the storage implementations. */
export function parseLaneFailure(
  input: WatchdogLaneFailure,
): WatchdogLaneFailure {
  try {
    return Schema.decodeUnknownSync(LaneFailureSchema, {
      onExcessProperty: "error",
    })(input);
  } catch (cause) {
    throw new WatchdogError("store_invalid", cause);
  }
}

const JobRowSchema = Schema.Struct({
  sequence: Schema.Int,
  job_id: Identifier,
  queue_name: Identifier,
  lane: laneSchema,
  priority: Priority,
  payload_json: Schema.String,
  not_before: Count,
  attempt_count: Count,
  recovery_max: Schema.NullOr(Count),
  recovery_window_ms: Schema.NullOr(Count),
  recovery_window_started_at: Schema.NullOr(Count),
  state: Schema.Literals(["queued", "running", "completing", "settled"]),
  outcome: Schema.NullOr(
    Schema.Literals([
      "completed",
      "failed",
      "cancelled",
      "outcome_unknown",
      "interrupted",
    ]),
  ),
  claim_token: Schema.NullOr(Identifier),
  recovery_count: Count,
  cancellation_id: Schema.NullOr(Identifier),
  failure_reason: Schema.NullOr(FailureReason),
});
const PayloadTextSchema = Schema.fromJsonString(Schema.Unknown);
const decodeJobRow = Schema.decodeUnknownExit(JobRowSchema, {
  onExcessProperty: "error",
});
const decodePayload = Schema.decodeUnknownExit(PayloadTextSchema);
const encodePayload = Schema.encodeUnknownExit(PayloadTextSchema);

/** Decoded relational row used inside the SQLite adapter family. */
export type WatchdogSqliteJobRow = Schema.Schema.Type<typeof JobRowSchema>;
/** Decoded row and domain job shared with the async SQLite adapter. */
export type ParsedWatchdogSqliteJob = Readonly<{
  row: WatchdogSqliteJobRow;
  job: WatchdogStoredJob;
}>;
type OpaquePayload = WatchdogQueuedJob["payload"];

/** Package-private result of selecting or claiming one durable row. */
export type WatchdogSelection =
  | Readonly<{ status: "no_ready" }>
  | Readonly<{ status: "claim_unavailable" }>
  | Readonly<{ status: "cancelled"; job: WatchdogSettledJob }>
  | Readonly<{
      status: "claimed";
      job: WatchdogRunningJob;
      claimToken: string;
    }>;

/** One private execution partition; no payload or product meaning. */
export type LanePartition = Readonly<{ lane: string; include: boolean }>;

function laneQuery(partition: LanePartition | undefined) {
  return partition === undefined
    ? { predicate: "1 = 1", values: [] }
    : {
        predicate: partition.include ? "lane = ?" : "lane <> ?",
        values: [partition.lane],
      };
}

/** Package-private storage operations consumed by Watchdog's Effect engine. */
export type WatchdogStore = Readonly<{
  wakeState: (now: number) => Effect.Effect<WatchdogWakeState, WatchdogError>;
  ingest: (
    job: WatchdogQueuedJob,
  ) => Effect.Effect<WatchdogIngestResult, WatchdogError>;
  read: (jobId: string) => Effect.Effect<
    | (Extract<WatchdogReadResult, { status: "found" }> &
        Readonly<{
          claimToken: string | null;
          recoveryCount: number;
        }>)
    | Extract<WatchdogReadResult, { status: "missing" }>,
    WatchdogError
  >;
  readQueueHead: (
    queue: string,
  ) => Effect.Effect<WatchdogQueueHead, WatchdogError>;
  readSettled: (
    limit: number,
    afterSequence?: number,
    lane?: string,
  ) => Effect.Effect<readonly WatchdogSettledRecord[], WatchdogError>;
  selectAndClaim: (
    claimToken: string,
  ) => Effect.Effect<WatchdogSelection, WatchdogError>;
  reserveAttempt: (input: {
    jobId: string;
    claimToken: string;
    policy: WatchdogRetryPolicy;
  }) => Effect.Effect<WatchdogAttempt, WatchdogError>;
  retryExactClaim: (input: {
    jobId: string;
    claimToken: string;
    policy?: WatchdogRetryPolicy;
  }) => Effect.Effect<
    { status: "requeued" | "cancelled" | "conflict" },
    WatchdogError
  >;
  recordExecutionDecision: (
    input: Readonly<{
      jobId: string;
      claimToken: string;
      decision: Exclude<WatchdogExecutionDecision, { kind: "retry" }>;
    }>,
  ) => Effect.Effect<
    | Readonly<{
        status: "recorded" | "replayed";
        job: WatchdogCompletingJob | WatchdogSettledJob;
      }>
    | Readonly<{ status: "conflict" | "missing" }>,
    WatchdogError
  >;
  nextCompletingClaim: () => Effect.Effect<
    { job: WatchdogCompletingJob; claimToken: string } | undefined,
    WatchdogError
  >;
  finishCompletingClaim: (input: {
    jobId: string;
    claimToken: string;
  }) => Effect.Effect<
    | { status: "settled" | "replayed"; job: WatchdogSettledJob }
    | { status: "conflict" | "missing" },
    WatchdogError
  >;
  recordCancellation: (
    input: WatchdogCancellationIntent,
  ) => Effect.Effect<WatchdogCancellationResult, WatchdogError>;
  failLane: (
    input: WatchdogLaneFailure,
  ) => Effect.Effect<
    Readonly<{ jobs: readonly WatchdogSettledJob[]; more: boolean }>,
    WatchdogError
  >;
  purgeQueued: (queue: string) => Effect.Effect<void, WatchdogError>;
  handoffExactClaim: (input: {
    jobId: string;
    claimToken: string;
  }) => Effect.Effect<
    { status: "requeued" | "cancelled" | "conflict" | "missing" },
    WatchdogError
  >;
  nextRunningClaim: () => Effect.Effect<
    | Readonly<{
        job: WatchdogRunningJob;
        claimToken: string;
        recoveryCount: number;
      }>
    | undefined,
    WatchdogError
  >;
  reconcileDisappearedClaim: (
    input: Readonly<{
      job: WatchdogRunningJob;
      claimToken: string;
      recoveryCount: number;
      requeue?: boolean;
    }>,
  ) => Effect.Effect<
    | Readonly<{ status: "requeued" }>
    | Readonly<{ status: "settled"; job: WatchdogSettledJob }>
    | Readonly<{ status: "conflict" | "missing" }>,
    WatchdogError
  >;
}>;

/** Package-private SQLite state plus the one owner-transaction projection. */
export type WatchdogSqliteState = Readonly<{
  store: WatchdogStore;
  partition: (partition: LanePartition) => WatchdogStore;
  transactional: WatchdogTransactionalProjection;
}>;

const columns = `sequence, job_id, queue_name, lane, priority, payload_json, not_before, attempt_count,
                 recovery_max, recovery_window_ms, recovery_window_started_at, state, outcome, claim_token, recovery_count,
                 cancellation_id, failure_reason`;

/** Queue order is decided before readiness; a sleeping head still owns its queue. */
export const queuedHeadPredicate = `(cancellation_id IS NOT NULL OR NOT EXISTS (
  SELECT 1 FROM watchdog_jobs predecessor
  WHERE predecessor.queue_name = watchdog_jobs.queue_name
    AND predecessor.state IN ('queued', 'running', 'completing')
    AND (predecessor.state IN ('running', 'completing') OR
      (predecessor.state = 'queued' AND
        (predecessor.priority > watchdog_jobs.priority OR
          (predecessor.priority = watchdog_jobs.priority AND
            predecessor.sequence < watchdog_jobs.sequence))))))`;

const WakeRow = Schema.Struct({
  pending: Schema.Literals([0, 1]),
  next_wake_at: Schema.NullOr(Count),
});
export const parseWakeTime = Schema.decodeUnknownSync(Count);
export function parseWakeState(
  raw: WatchdogSqliteRow | undefined,
): WatchdogWakeState {
  const row = Schema.decodeUnknownSync(WakeRow)(raw);
  return { pending: row.pending === 1, nextWakeAt: row.next_wake_at };
}
function readWakeState(
  owner: WatchdogSqliteOwner,
  now: number,
): WatchdogWakeState {
  parseWakeTime(now);
  return parseWakeState(
    owner.sql
      .exec(
        `SELECT
    COALESCE(MAX(CASE WHEN state IN ('running','completing') OR cancellation_id IS NOT NULL OR not_before <= ? THEN 1 ELSE 0 END),0) AS pending,
    MIN(CASE WHEN state='queued' AND cancellation_id IS NULL AND not_before > ? THEN not_before END) AS next_wake_at
    FROM watchdog_jobs WHERE state IN ('running','completing') OR (state='queued' AND ${queuedHeadPredicate})`,
        now,
        now,
      )
      .toArray()[0],
  );
}

function unavailable(cause?: unknown): WatchdogError {
  return new WatchdogError("store_unavailable", cause);
}

function storeEffect<A>(operation: () => A): Effect.Effect<A, WatchdogError> {
  return Effect.try({
    try: operation,
    // The SQL error stays on the failure; a Watchdog failure raised inside
    // (an invariant or a decode) already carries its own.
    catch: (error) =>
      error instanceof WatchdogError ? error : unavailable(error),
  });
}

function encodeOpaquePayload(payload: OpaquePayload): string {
  const encoded = encodePayload(payload);
  if (encoded._tag === "Failure")
    throw unavailable(Cause.squash(encoded.cause));
  return encoded.value;
}

export function parseJobRow(raw: WatchdogSqliteRow): ParsedWatchdogSqliteJob {
  const decoded = decodeJobRow(raw);
  if (decoded._tag === "Failure")
    throw unavailable(Cause.squash(decoded.cause));
  const row = decoded.value;
  const payload = decodePayload(row.payload_json);
  if (payload._tag === "Failure")
    throw unavailable(Cause.squash(payload.cause));
  let common: Omit<WatchdogQueuedJob, "state"> = {
    jobId: row.job_id,
    queue: row.queue_name,
    lane: row.lane,
    priority: row.priority,
    payload: payload.value,
  };
  if (row.not_before !== 0) common = { ...common, notBefore: row.not_before };
  if (row.recovery_max !== null) {
    const recovery =
      row.recovery_window_ms === null
        ? { maxRecoveries: row.recovery_max }
        : { maxRecoveries: row.recovery_max, windowMs: row.recovery_window_ms };
    common = { ...common, recovery };
  }
  if (row.state === "queued")
    return { row, job: { ...common, state: "queued" } };
  if (row.state === "running") {
    if (row.claim_token === null) throw unavailable();
    return { row, job: { ...common, state: "running" } };
  }
  if (row.outcome === null) throw unavailable();
  const settled = {
    ...common,
    state: row.state,
    outcome: row.outcome,
  };
  return {
    row,
    job:
      (row.outcome === "failed" || row.outcome === "interrupted") &&
      row.failure_reason !== null
        ? { ...settled, failureReason: row.failure_reason }
        : settled,
  };
}

function readJob(
  owner: WatchdogSqliteOwner,
  jobId: string,
): ParsedWatchdogSqliteJob | undefined {
  const raw = owner.sql
    .exec(
      `SELECT ${columns}
         FROM watchdog_jobs
        WHERE job_id = ?`,
      jobId,
    )
    .toArray()[0];
  return raw === undefined ? undefined : parseJobRow(raw);
}

function readSettled(
  owner: WatchdogSqliteOwner,
  limit: number,
  afterSequence: number | undefined,
  partition: LanePartition | undefined,
  selectedLane?: string,
): readonly WatchdogSettledRecord[] {
  const partitionQuery = laneQuery(partition);
  const lane =
    selectedLane === undefined
      ? partitionQuery
      : {
          predicate: `${partitionQuery.predicate} AND lane = ?`,
          values: [
            ...partitionQuery.values,
            Schema.decodeUnknownSync(laneSchema)(selectedLane),
          ],
        };
  const rows = owner.sql
    .exec(
      `SELECT ${columns} FROM watchdog_jobs NOT INDEXED WHERE state = 'settled' AND ${lane.predicate} AND sequence > ?
       AND sequence < COALESCE(
        (SELECT MIN(sequence) FROM watchdog_jobs INDEXED BY idx_watchdog_ready
         WHERE state IN ('queued', 'running', 'completing') AND ${lane.predicate}),
        9223372036854775807)
       ORDER BY sequence ASC LIMIT ?`,
      ...lane.values,
      afterSequence ?? 0,
      ...lane.values,
      limit,
    )
    .toArray();
  return rows.map((raw) => {
    const parsed = parseJobRow(raw);
    if (parsed.job.state !== "settled") throw unavailable();
    return { sequence: parsed.row.sequence, job: parsed.job };
  });
}

function readQueueHead(
  owner: WatchdogSqliteOwner,
  queue: string,
): WatchdogQueueHead {
  const raw = owner.sql
    .exec(
      `SELECT ${columns}
         FROM watchdog_jobs
        WHERE queue_name = ? AND state IN ('running', 'completing', 'queued')
        ORDER BY CASE state WHEN 'queued' THEN 1 ELSE 0 END,
                 priority DESC, sequence ASC
        LIMIT 1`,
      queue,
    )
    .toArray()[0];
  if (raw === undefined) return { state: "idle" };
  const parsed = parseJobRow(raw);
  if (parsed.job.state === "settled") throw unavailable();
  return {
    state: parsed.job.state === "completing" ? "running" : parsed.job.state,
    jobId: parsed.job.jobId,
    cancellationRequested: parsed.row.cancellation_id !== null,
  };
}

function readLatestSettledJob(
  owner: WatchdogSqliteOwner,
  queue: string,
): WatchdogSettledJob | undefined {
  const raw = owner.sql
    .exec(
      `SELECT ${columns}
         FROM watchdog_jobs
        WHERE queue_name = ? AND state = 'settled'
        ORDER BY sequence DESC
        LIMIT 1`,
      queue,
    )
    .toArray()[0];
  if (raw === undefined) return undefined;
  const parsed = parseJobRow(raw);
  if (parsed.job.state !== "settled") throw unavailable();
  return parsed.job;
}

/** Tell the owner projection one queue changed, inside the same transaction. */
function projectQueue(
  owner: WatchdogSqliteOwner,
  project: WatchdogQueueProjector | undefined,
  queue: string,
  settled?: WatchdogSettledJob,
): void {
  if (project === undefined) return;
  const head = readQueueHead(owner, queue);
  project(settled === undefined ? { queue, head } : { queue, head, settled });
}

function prepare(owner: WatchdogSqliteOwner): void {
  owner.transactionSync(() => {
    owner.sql.exec(`
      CREATE TABLE IF NOT EXISTS watchdog_jobs (
        sequence INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id TEXT UNIQUE NOT NULL,
        queue_name TEXT NOT NULL,
        lane TEXT NOT NULL,
        priority INTEGER NOT NULL,
        payload_json TEXT NOT NULL,
        not_before INTEGER NOT NULL DEFAULT 0 CHECK (not_before >= 0),
        attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
        recovery_max INTEGER,
        recovery_window_ms INTEGER CHECK (recovery_window_ms IS NULL OR recovery_window_ms > 0),
        recovery_window_started_at INTEGER,
        state TEXT NOT NULL CHECK (state IN ('queued', 'running', 'completing', 'settled')),
        outcome TEXT CHECK (
          outcome IS NULL OR outcome IN (
            'completed', 'failed', 'cancelled', 'outcome_unknown', 'interrupted'
          )
        ),
        claim_token TEXT,
        recovery_count INTEGER NOT NULL DEFAULT 0,
        cancellation_id TEXT UNIQUE,
        failure_reason TEXT,
        CHECK (recovery_max IS NULL OR recovery_max >= 0),
        CONSTRAINT watchdog_execution_state CHECK (
          (state = 'queued' AND outcome IS NULL AND claim_token IS NULL) OR
          (state = 'running' AND outcome IS NULL AND claim_token IS NOT NULL) OR
          (state = 'completing' AND outcome IS NOT NULL AND claim_token IS NOT NULL) OR
          (state = 'settled' AND outcome IS NOT NULL)
        )
      )
    `);
    // Refuse the old state grammar before an executor can perform effects.
    if (
      owner.sql
        .exec(
          "SELECT 1 AS current FROM sqlite_schema WHERE name = 'watchdog_jobs' AND instr(sql, 'watchdog_execution_state') > 0",
        )
        .toArray().length !== 1
    )
      throw new WatchdogError(
        "store_invalid",
        new Error("Obsolete Watchdog execution schema"),
      );
    try {
      owner.sql.exec(`SELECT ${columns} FROM watchdog_jobs LIMIT 0`);
    } catch (error) {
      throw new WatchdogError("store_invalid", error);
    }
    owner.sql.exec(
      `CREATE INDEX IF NOT EXISTS idx_watchdog_lane_failure
         ON watchdog_jobs(lane, state DESC, sequence ASC)`,
    );
    owner.sql.exec(
      `CREATE INDEX IF NOT EXISTS idx_watchdog_ready
         ON watchdog_jobs(state, not_before, priority DESC, sequence ASC)`,
    );
    owner.sql.exec(
      `CREATE INDEX IF NOT EXISTS idx_watchdog_queue_head
         ON watchdog_jobs(queue_name, state, priority DESC, sequence ASC)`,
    );
  });
}

function enqueue(
  owner: WatchdogSqliteOwner,
  project: WatchdogQueueProjector | undefined,
  job: WatchdogQueuedJob,
): boolean {
  const payloadJson = encodeOpaquePayload(job.payload);
  const prior = readJob(owner, job.jobId);
  if (prior !== undefined) {
    const priorRecovery = prior.job.recovery?.maxRecoveries;
    const nextRecovery = job.recovery?.maxRecoveries;
    if (
      prior.job.queue !== job.queue ||
      prior.job.lane !== job.lane ||
      prior.job.priority !== job.priority ||
      prior.row.payload_json !== payloadJson ||
      priorRecovery !== nextRecovery ||
      prior.job.recovery?.windowMs !== job.recovery?.windowMs
    )
      throw unavailable();
    return false;
  }
  owner.sql.exec(
    `INSERT INTO watchdog_jobs
       (job_id, queue_name, lane, priority, payload_json, recovery_max, recovery_window_ms, not_before,
        state, outcome, claim_token, recovery_count, cancellation_id)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'queued', NULL, NULL, 0, NULL)`,
    job.jobId,
    job.queue,
    job.lane,
    job.priority,
    payloadJson,
    job.recovery?.maxRecoveries ?? null,
    job.recovery?.windowMs ?? null,
    job.notBefore ?? 0,
  );
  projectQueue(owner, project, job.queue);
  return true;
}

function recordCancellationIntent(
  owner: WatchdogSqliteOwner,
  project: WatchdogQueueProjector | undefined,
  intent: WatchdogCancellationIntent,
): WatchdogCancellationDecision {
  const prior = readJob(owner, intent.jobId);
  if (prior === undefined) return { status: "missing" };
  if (prior.job.state === "completing" || prior.job.state === "settled")
    return { status: "decided", outcome: prior.job.outcome };
  if (prior.row.cancellation_id !== null)
    return prior.row.cancellation_id === intent.cancellationId
      ? { status: "replayed", intent }
      : { status: "conflict" };
  if (intent.onlyIfQueued === true && prior.job.state !== "queued")
    return { status: "conflict" };
  owner.sql.exec(
    `UPDATE watchdog_jobs
        SET cancellation_id = ?
      WHERE job_id = ? AND cancellation_id IS NULL`,
    intent.cancellationId,
    intent.jobId,
  );
  projectQueue(owner, project, prior.job.queue);
  return { status: "recorded", intent };
}

function selectAndClaim(
  owner: WatchdogSqliteOwner,
  project: WatchdogQueueProjector | undefined,
  claimToken: string,
  partition: LanePartition | undefined,
): WatchdogSelection {
  const lane = laneQuery(partition);
  return owner.transactionSync(() => {
    const raw = owner.sql
      .exec(
        `SELECT ${columns}
           FROM watchdog_jobs
          WHERE state = 'queued' AND ${lane.predicate}
            AND (cancellation_id IS NOT NULL OR not_before <= ?)
            AND ${queuedHeadPredicate}
          ORDER BY priority DESC, sequence ASC
          LIMIT 1`,
        ...lane.values,
        Date.now(),
      )
      .toArray()[0];
    if (raw === undefined) return { status: "no_ready" };
    const selected = parseJobRow(raw);
    if (selected.job.state !== "queued") return { status: "claim_unavailable" };

    if (selected.row.cancellation_id !== null) {
      owner.sql.exec(
        `UPDATE watchdog_jobs
            SET state = 'settled', outcome = 'cancelled'
          WHERE job_id = ? AND state = 'queued' AND cancellation_id = ?`,
        selected.job.jobId,
        selected.row.cancellation_id,
      );
      const settled = readJob(owner, selected.job.jobId)?.job;
      if (settled?.state !== "settled" || settled.outcome !== "cancelled")
        return { status: "claim_unavailable" };
      projectQueue(owner, project, settled.queue, settled);
      return { status: "cancelled", job: settled };
    }

    owner.sql.exec(
      `UPDATE watchdog_jobs
          SET state = 'running', claim_token = ?
        WHERE job_id = ? AND state = 'queued'`,
      claimToken,
      selected.job.jobId,
    );
    const claimed = readJob(owner, selected.job.jobId)?.job;
    if (claimed?.state !== "running") return { status: "claim_unavailable" };
    projectQueue(owner, project, claimed.queue);
    return { status: "claimed", job: claimed, claimToken };
  });
}

function recordExecutionDecision(
  owner: WatchdogSqliteOwner,
  input: Readonly<{
    jobId: string;
    claimToken: string;
    decision: Exclude<WatchdogExecutionDecision, { kind: "retry" }>;
  }>,
):
  | {
      status: "recorded" | "replayed";
      job: WatchdogCompletingJob | WatchdogSettledJob;
    }
  | { status: "conflict" | "missing" } {
  return owner.transactionSync(() => {
    const prior = readJob(owner, input.jobId);
    if (prior === undefined) return { status: "missing" };
    const outcome =
      prior.row.cancellation_id === null ? input.decision.kind : "cancelled";
    if (prior.job.state === "completing" || prior.job.state === "settled")
      return prior.row.claim_token === input.claimToken &&
        prior.job.outcome === outcome
        ? { status: "replayed", job: prior.job }
        : { status: "conflict" };
    if (
      prior.job.state !== "running" ||
      prior.row.claim_token !== input.claimToken
    )
      return { status: "conflict" };
    owner.sql.exec(
      `UPDATE watchdog_jobs SET state = 'completing', outcome = ?, failure_reason = ?
       WHERE job_id = ? AND state = 'running' AND claim_token = ?`,
      outcome,
      outcome === "failed" && input.decision.kind === "failed"
        ? (input.decision.reason ?? null)
        : null,
      input.jobId,
      input.claimToken,
    );
    const decided = readJob(owner, input.jobId)?.job;
    if (decided?.state !== "completing") return { status: "conflict" };
    return { status: "recorded", job: decided };
  });
}

function finishCompletingClaim(
  owner: WatchdogSqliteOwner,
  project: WatchdogQueueProjector | undefined,
  input: { jobId: string; claimToken: string },
):
  | { status: "settled" | "replayed"; job: WatchdogSettledJob }
  | { status: "conflict" | "missing" } {
  return owner.transactionSync(() => {
    const prior = readJob(owner, input.jobId);
    if (prior === undefined) return { status: "missing" };
    if (prior.row.claim_token !== input.claimToken)
      return { status: "conflict" };
    if (prior.job.state === "settled")
      return { status: "replayed", job: prior.job };
    if (prior.job.state !== "completing") return { status: "conflict" };
    owner.sql.exec(
      "UPDATE watchdog_jobs SET state = 'settled' WHERE job_id = ? AND state = 'completing' AND claim_token = ?",
      input.jobId,
      input.claimToken,
    );
    const settled = readJob(owner, input.jobId)?.job;
    if (settled?.state !== "settled") return { status: "conflict" };
    projectQueue(owner, project, settled.queue, settled);
    return { status: "settled", job: settled };
  });
}

/** Construct Watchdog's private owner-local SQLite state. */
export function openSqliteState(
  owner: WatchdogSqliteOwner,
  project?: WatchdogQueueProjector,
  partition?: LanePartition,
): WatchdogSqliteState {
  prepare(owner);
  const transactional: WatchdogTransactionalProjection = Object.freeze({
    wakeState: (now) => readWakeState(owner, now),
    readJob: (jobId) => {
      const parsed = readJob(owner, jobId);
      return parsed === undefined
        ? undefined
        : { state: parsed.row.state, notBefore: parsed.row.not_before };
    },
    expediteQueuedJob: (jobId, now) => {
      parseWakeTime(now);
      return (
        owner.sql
          .exec(
            "UPDATE watchdog_jobs SET not_before=? WHERE job_id=? AND state='queued' AND not_before>? RETURNING job_id",
            now,
            jobId,
            now,
          )
          .toArray().length === 1
      );
    },
    readQueueHead: (queue) => readQueueHead(owner, queue),
    readLatestSettledJob: (queue) => readLatestSettledJob(owner, queue),
    enqueueAcceptedJob: (job) => {
      enqueue(owner, project, job);
    },
    recordCancellationIntent: (intent) =>
      recordCancellationIntent(owner, project, intent),
  });
  const createStore = (partition: LanePartition | undefined): WatchdogStore => {
    const lane = laneQuery(partition);
    return Object.freeze({
      wakeState: (now) => storeEffect(() => readWakeState(owner, now)),
      ingest: (job) =>
        storeEffect(() =>
          owner.transactionSync(() => ({
            status: enqueue(owner, project, job) ? "queued" : "duplicate",
          })),
        ),
      read: (jobId) =>
        storeEffect(() => {
          const parsed = readJob(owner, jobId);
          if (parsed === undefined) return { status: "missing" };
          return {
            status: "found",
            job: parsed.job,
            claimToken: parsed.row.claim_token,
            recoveryCount: parsed.row.recovery_count,
            cancellation:
              parsed.row.cancellation_id === null
                ? { status: "absent" }
                : {
                    status: "present",
                    cancellation: {
                      jobId,
                      cancellationId: parsed.row.cancellation_id,
                    },
                  },
          };
        }),
      readQueueHead: (queue) => storeEffect(() => readQueueHead(owner, queue)),
      readSettled: (limit, afterSequence, selectedLane) =>
        storeEffect(() =>
          readSettled(owner, limit, afterSequence, partition, selectedLane),
        ),
      selectAndClaim: (claimToken) =>
        storeEffect(() =>
          selectAndClaim(owner, project, claimToken, partition),
        ),
      reserveAttempt: (input) =>
        storeEffect(() =>
          owner.transactionSync(() => {
            const prior = readJob(owner, input.jobId);
            if (
              prior?.job.state !== "running" ||
              prior.row.claim_token !== input.claimToken ||
              prior.row.cancellation_id !== null
            )
              throw new WatchdogError("claim_unavailable");
            const reserved = reserveRetry(
              prior.row.attempt_count,
              Date.now(),
              input.policy,
            );
            owner.sql.exec(
              "UPDATE watchdog_jobs SET attempt_count=?, not_before=? WHERE job_id=? AND state='running' AND claim_token=?",
              reserved.attempt.count,
              reserved.notBefore,
              input.jobId,
              input.claimToken,
            );
            return reserved.attempt;
          }),
        ),
      retryExactClaim: (input) =>
        storeEffect(() =>
          owner.transactionSync(() => {
            const prior = readJob(owner, input.jobId);
            if (
              prior?.job.state !== "running" ||
              prior.row.claim_token !== input.claimToken
            )
              return { status: "conflict" as const };
            if (prior.row.cancellation_id !== null)
              return { status: "cancelled" as const };
            const due =
              input.policy === undefined
                ? prior.row.not_before
                : retryDue(prior.row.attempt_count, Date.now(), input.policy);
            owner.sql.exec(
              "UPDATE watchdog_jobs SET state='queued', claim_token=NULL, not_before=? WHERE job_id=? AND state='running' AND claim_token=?",
              due,
              input.jobId,
              input.claimToken,
            );
            projectQueue(owner, project, prior.job.queue);
            return { status: "requeued" as const };
          }),
        ),
      recordExecutionDecision: (input) =>
        storeEffect(() => recordExecutionDecision(owner, input)),
      nextCompletingClaim: () =>
        storeEffect(() => {
          const raw = owner.sql
            .exec(
              `SELECT ${columns} FROM watchdog_jobs WHERE state = 'completing' AND ${lane.predicate} ORDER BY sequence LIMIT 1`,
              ...lane.values,
            )
            .toArray()[0];
          if (raw === undefined) return undefined;
          const parsed = parseJobRow(raw);
          if (
            parsed.job.state !== "completing" ||
            parsed.row.claim_token === null
          )
            throw unavailable();
          return { job: parsed.job, claimToken: parsed.row.claim_token };
        }),
      finishCompletingClaim: (input) =>
        storeEffect(() => finishCompletingClaim(owner, project, input)),
      recordCancellation: (intent) =>
        storeEffect(() =>
          owner.transactionSync(() => {
            const result = recordCancellationIntent(owner, project, intent);
            return result.status === "decided"
              ? result
              : { status: result.status };
          }),
        ),
      failLane: (raw) =>
        storeEffect(() => {
          const input = parseLaneFailure(raw);
          return owner.transactionSync(() => {
            // Under the declared state grammar, DESC puts running before queued.
            const rows = owner.sql
              .exec(
                `SELECT ${columns} FROM watchdog_jobs
             WHERE lane = ? AND state IN ('queued', 'running', 'completing')
             ORDER BY state DESC, sequence ASC LIMIT ?`,
                input.lane,
                laneFailureBatchSize,
              )
              .toArray();
            const jobs: WatchdogSettledJob[] = [];
            for (const row of rows) {
              const prior = parseJobRow(row);
              owner.sql.exec(
                `UPDATE watchdog_jobs SET state = 'settled', outcome = 'failed', failure_reason = ?, claim_token = NULL
               WHERE job_id = ? AND state IN ('queued', 'running', 'completing')`,
                input.reason,
                prior.job.jobId,
              );
              const settled = readJob(owner, prior.job.jobId)?.job;
              if (settled?.state !== "settled" || settled.outcome !== "failed")
                throw unavailable();
              projectQueue(owner, project, settled.queue, settled);
              jobs.push(settled);
            }
            const more =
              owner.sql
                .exec(
                  "SELECT 1 FROM watchdog_jobs WHERE lane = ? AND state IN ('queued', 'running', 'completing') LIMIT 1",
                  input.lane,
                )
                .toArray().length !== 0;
            return { jobs, more };
          });
        }),
      purgeQueued: (queue) =>
        storeEffect(() => {
          owner.transactionSync(() => {
            owner.sql.exec(
              `DELETE FROM watchdog_jobs
              WHERE queue_name = ? AND state = 'queued'`,
              queue,
            );
            projectQueue(owner, project, queue);
          });
        }),
      handoffExactClaim: (input) =>
        storeEffect(() =>
          owner.transactionSync(() => {
            const prior = readJob(owner, input.jobId);
            if (prior === undefined) return { status: "missing" };
            if (
              prior.job.state !== "running" ||
              prior.row.claim_token !== input.claimToken
            )
              return { status: "conflict" };
            if (prior.row.cancellation_id !== null)
              return { status: "cancelled" };
            owner.sql.exec(
              "UPDATE watchdog_jobs SET state = 'queued', claim_token = NULL WHERE job_id = ? AND state = 'running' AND claim_token = ?",
              input.jobId,
              input.claimToken,
            );
            projectQueue(owner, project, prior.job.queue);
            return { status: "requeued" };
          }),
        ),
      nextRunningClaim: () =>
        storeEffect(() => {
          const raw = owner.sql
            .exec(
              `SELECT ${columns}
               FROM watchdog_jobs
              WHERE state = 'running' AND ${lane.predicate}
              ORDER BY sequence ASC
              LIMIT 1`,
              ...lane.values,
            )
            .toArray()[0];
          if (raw === undefined) return undefined;
          const parsed = parseJobRow(raw);
          if (parsed.job.state !== "running" || parsed.row.claim_token === null)
            throw unavailable();
          return {
            job: parsed.job,
            claimToken: parsed.row.claim_token,
            recoveryCount: parsed.row.recovery_count,
          };
        }),
      reconcileDisappearedClaim: (input) =>
        storeEffect(() =>
          owner.transactionSync(() => {
            const prior = readJob(owner, input.job.jobId);
            if (prior === undefined) return { status: "missing" as const };
            if (
              prior.job.state !== "running" ||
              prior.row.claim_token !== input.claimToken ||
              prior.row.recovery_count !== input.recoveryCount
            )
              return { status: "conflict" as const };
            const recovery = input.requeue
              ? {
                  kind: "requeue" as const,
                  recoveryCount: prior.row.recovery_count,
                  windowStartedAt: prior.row.recovery_window_started_at,
                }
              : disappearanceRecovery(
                  prior.job.recovery,
                  prior.row.recovery_count,
                  prior.row.recovery_window_started_at,
                  Date.now(),
                );
            if (
              recovery.kind === "requeue" &&
              prior.row.cancellation_id === null
            ) {
              owner.sql.exec(
                `UPDATE watchdog_jobs
                  SET state = 'queued', claim_token = NULL,
                      recovery_count = ?, recovery_window_started_at = ?
                WHERE job_id = ? AND state = 'running' AND claim_token = ?`,
                recovery.recoveryCount,
                recovery.windowStartedAt,
                input.job.jobId,
                input.claimToken,
              );
              projectQueue(owner, project, input.job.queue);
              return { status: "requeued" as const };
            }
            const cancelled = prior.row.cancellation_id !== null;
            const reason = cancelled
              ? null
              : prior.job.recovery === undefined
                ? "Execution disappeared without a recovery policy"
                : `Restart recovery budget exhausted (${String(prior.job.recovery.maxRecoveries)} recoveries)`;
            owner.sql.exec(
              `UPDATE watchdog_jobs
                SET state = 'settled', outcome = ?, failure_reason = ?
              WHERE job_id = ? AND state = 'running' AND claim_token = ?`,
              cancelled ? "cancelled" : "interrupted",
              reason,
              input.job.jobId,
              input.claimToken,
            );
            const settled = readJob(owner, input.job.jobId)?.job;
            if (settled?.state !== "settled")
              return { status: "conflict" as const };
            projectQueue(owner, project, settled.queue, settled);
            return { status: "settled" as const, job: settled };
          }),
        ),
    });
  };
  return Object.freeze({
    store: createStore(partition),
    partition: createStore,
    transactional,
  });
}
