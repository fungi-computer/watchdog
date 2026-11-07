import { laneSchema } from "./lane.js";
import { reserveRetry, retryDue } from "./retry.js";
import { disappearanceRecovery } from "./recovery.js";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type { WatchdogStore } from "./sqlite-store.js";
import {
  parseJobRow,
  queuedHeadPredicate,
  parseWakeState,
  parseWakeTime,
  parseLaneFailure,
  laneFailureBatchSize,
} from "./sqlite-store.js";
import { WatchdogError } from "./watchdog-error.js";
import {
  columns,
  enqueueStatement,
  type SqlDatabase,
  type Statement,
} from "./async-sql.js";

const leaseMs = 120_000;
export const heartbeatMs = 10_000;
const Alive = Schema.Struct({ alive: Schema.Literals([0, 1]) });

export const attempt = <A>(
  run: () => Promise<A>,
): Effect.Effect<A, WatchdogError> =>
  Effect.tryPromise({
    try: run,
    catch: (error) =>
      error instanceof WatchdogError
        ? error
        : new WatchdogError("store_unavailable", error),
  });

export function openAsyncStore(database: SqlDatabase, now: () => number) {
  // The database checks wall time when the mutation executes, so a delayed
  // renewal cannot resurrect a lease that expired while the request was in flight.
  const at = (parameter: number) =>
    database.clock === undefined
      ? `CAST($${String(parameter)} AS DOUBLE PRECISION)`
      : `(CASE WHEN $${String(parameter)}>${database.clock} THEN $${String(parameter)} ELSE ${database.clock} END)`;
  let schemaChecked = false;
  const query = async (text: string, values: Statement["values"] = []) => {
    if (!schemaChecked) {
      const current = await database.query({
        text:
          database.dialect === "postgres"
            ? "SELECT 1 AS current FROM pg_constraint WHERE conrelid = 'watchdog_jobs'::regclass AND conname = 'watchdog_execution_state'"
            : "SELECT 1 AS current FROM sqlite_schema WHERE name = 'watchdog_jobs' AND instr(sql, 'watchdog_execution_state') > 0",
        values: [],
      });
      if (current.length !== 1)
        throw new WatchdogError(
          "store_invalid",
          new Error("Obsolete Watchdog execution schema"),
        );
      try {
        await database.query({
          text: `SELECT ${columns} FROM watchdog_jobs LIMIT 0`,
          values: [],
        });
      } catch (error) {
        throw new WatchdogError("store_invalid", error);
      }
      schemaChecked = true;
    }
    return database.query({ text, values });
  };
  const row = async (id: string) => {
    const raw = (
      await query(`SELECT ${columns} FROM watchdog_jobs WHERE job_id=$1`, [id])
    )[0];
    return raw === undefined ? undefined : parseJobRow(raw);
  };
  // Private to this runtime: claims never escape through a public job.
  const claims = new Map<string, string>();
  const store: WatchdogStore = {
    wakeState: (instant) =>
      attempt(async () => {
        parseWakeTime(instant);
        return parseWakeState(
          (
            await query(
              `SELECT
        COALESCE(MAX(CASE WHEN state='completing' OR cancellation_id IS NOT NULL OR
          (state='running' AND lease_until <= ${at(1)}) OR
          (state='queued' AND not_before <= ${at(1)}) THEN 1 ELSE 0 END),0) AS pending,
        MIN(CASE WHEN state='running' AND lease_until > ${at(1)} THEN lease_until
          WHEN state='queued' AND cancellation_id IS NULL AND not_before > ${at(1)} THEN not_before END) AS next_wake_at
        FROM watchdog_jobs WHERE state IN ('running','completing') OR
          (state='queued' AND ${queuedHeadPredicate})`,
              [instant],
            )
          )[0],
        );
      }),
    ingest: (job) =>
      attempt(async () => {
        const statement = enqueueStatement(job);
        return {
          status:
            (await query(statement.text, statement.values)).length > 0
              ? "queued"
              : "duplicate",
        };
      }),
    read: (id) =>
      attempt(async () => {
        const observedClaim = claims.get(id);
        const found = await row(id);
        // A delayed claim reply can arrive after failLane deleted its entry.
        // Retire only this read's claim; a pre-claim snapshot cannot erase a newer one.
        if (
          (found === undefined || found.job.state === "settled") &&
          claims.get(id) === observedClaim
        )
          claims.delete(id);
        if (found === undefined) return { status: "missing" };
        return {
          status: "found",
          job: found.job,
          claimToken: found.row.claim_token,
          recoveryCount: found.row.recovery_count,
          cancellation:
            found.row.cancellation_id === null
              ? { status: "absent" }
              : {
                  status: "present",
                  cancellation: {
                    jobId: id,
                    cancellationId: found.row.cancellation_id,
                  },
                },
        };
      }),
    readQueueHead: (queue) =>
      attempt(async () => {
        const raw = (
          await query(
            `SELECT ${columns} FROM watchdog_jobs WHERE queue_name=$1
        AND state IN ('queued','running','completing') ORDER BY CASE state WHEN 'queued' THEN 1 ELSE 0 END,
        priority DESC,sequence LIMIT 1`,
            [queue],
          )
        )[0];
        if (raw === undefined) return { state: "idle" };
        const found = parseJobRow(raw);
        if (found.job.state === "settled")
          throw new WatchdogError("store_unavailable");
        return {
          state: found.job.state === "completing" ? "running" : found.job.state,
          jobId: found.job.jobId,
          cancellationRequested: found.row.cancellation_id !== null,
        };
      }),
    readSettled: (limit, afterSequence = 0, lane) =>
      attempt(async () => {
        Schema.decodeUnknownSync(
          Schema.Int.check(
            Schema.isGreaterThanOrEqualTo(1),
            Schema.isLessThanOrEqualTo(256),
          ),
        )(limit);
        const rows = await query(
          `SELECT ${columns} FROM watchdog_jobs WHERE state='settled'
        AND sequence>$1 ${lane === undefined ? "" : "AND lane=$3"} ORDER BY sequence LIMIT $2`,
          lane === undefined
            ? [afterSequence, limit]
            : [
                afterSequence,
                limit,
                Schema.decodeUnknownSync(laneSchema)(lane),
              ],
        );
        return rows.map((raw) => {
          const parsed = parseJobRow(raw);
          if (parsed.job.state !== "settled")
            throw new WatchdogError("store_unavailable");
          return { sequence: parsed.row.sequence, job: parsed.job };
        });
      }),
    selectAndClaim: (token) =>
      attempt(async () => {
        const raw = (
          await query(
            `UPDATE watchdog_jobs SET
        state=CASE WHEN cancellation_id IS NULL THEN 'running' ELSE 'settled' END,
        outcome=CASE WHEN cancellation_id IS NULL THEN NULL ELSE 'cancelled' END,
        claim_token=CASE WHEN cancellation_id IS NULL THEN $1 ELSE NULL END,
        lease_until=CASE WHEN cancellation_id IS NULL THEN ${at(2)}+${String(leaseMs)} ELSE NULL END
        WHERE job_id=(SELECT job_id FROM watchdog_jobs WHERE state='queued'
          AND (cancellation_id IS NOT NULL OR not_before<=${at(2)})
          AND ${queuedHeadPredicate}
          ORDER BY priority DESC,sequence LIMIT 1) AND state='queued' RETURNING ${columns}`,
            [token, now()],
          )
        )[0];
        if (raw === undefined) return { status: "no_ready" };
        const parsed = parseJobRow(raw);
        if (parsed.job.state === "settled")
          return { status: "cancelled", job: parsed.job };
        if (parsed.job.state !== "running")
          throw new WatchdogError("claim_unavailable");
        claims.set(parsed.job.jobId, token);
        return { status: "claimed", job: parsed.job, claimToken: token };
      }),
    reserveAttempt: (input) =>
      attempt(async () => {
        const prior = await row(input.jobId);
        if (
          prior?.job.state !== "running" ||
          prior.row.claim_token !== input.claimToken ||
          prior.row.cancellation_id !== null
        )
          throw new WatchdogError("claim_unavailable");
        const reserved = reserveRetry(
          prior.row.attempt_count,
          now(),
          input.policy,
        );
        const updated = await query(
          `UPDATE watchdog_jobs SET attempt_count=$1,not_before=$2 WHERE job_id=$3 AND state='running' AND claim_token=$4 AND attempt_count=$5 AND cancellation_id IS NULL AND lease_until>${at(6)} RETURNING job_id`,
          [
            reserved.attempt.count,
            reserved.notBefore,
            input.jobId,
            input.claimToken,
            prior.row.attempt_count,
            now(),
          ],
        );
        if (updated.length !== 1) throw new WatchdogError("claim_unavailable");
        return reserved.attempt;
      }),
    retryExactClaim: (input) =>
      attempt(async () => {
        const prior = await row(input.jobId);
        if (
          prior?.job.state !== "running" ||
          prior.row.claim_token !== input.claimToken
        )
          return { status: "conflict" };
        const due =
          input.policy === undefined
            ? prior.row.not_before
            : retryDue(prior.row.attempt_count, now(), input.policy);
        const updated = await query(
          `UPDATE watchdog_jobs SET state='queued',claim_token=NULL,lease_until=NULL,not_before=$1 WHERE job_id=$2 AND state='running' AND claim_token=$3 AND cancellation_id IS NULL AND lease_until>${at(4)} RETURNING job_id`,
          [due, input.jobId, input.claimToken, now()],
        );
        if (updated.length === 1) {
          claims.delete(input.jobId);
          return { status: "requeued" };
        }
        const current = await row(input.jobId);
        return {
          status:
            current?.job.state === "running" &&
            current.row.claim_token === input.claimToken &&
            current.row.cancellation_id !== null
              ? "cancelled"
              : "conflict",
        };
      }),
    recordExecutionDecision: (input) =>
      attempt(async () => {
        try {
          const raw = (
            await query(
              `UPDATE watchdog_jobs SET state='completing',
          outcome=CASE WHEN cancellation_id IS NULL THEN $1 ELSE 'cancelled' END,
          failure_reason=CASE WHEN cancellation_id IS NULL THEN $2 ELSE NULL END,lease_until=NULL
          WHERE job_id=$3 AND state='running' AND claim_token=$4 AND lease_until>${at(5)} RETURNING ${columns}`,
              [
                input.decision.kind,
                input.decision.kind === "failed"
                  ? (input.decision.reason ?? null)
                  : null,
                input.jobId,
                input.claimToken,
                now(),
              ],
            )
          )[0];
          if (raw !== undefined) {
            const parsed = parseJobRow(raw);
            if (parsed.job.state === "completing")
              return { status: "recorded", job: parsed.job };
          }
          const prior = await row(input.jobId);
          if (prior === undefined) return { status: "missing" };
          const outcome =
            prior.row.cancellation_id === null
              ? input.decision.kind
              : "cancelled";
          return (prior.job.state === "completing" ||
            prior.job.state === "settled") &&
            prior.row.claim_token === input.claimToken &&
            prior.job.outcome === outcome
            ? { status: "replayed", job: prior.job }
            : { status: "conflict" };
        } finally {
          claims.delete(input.jobId);
        }
      }),
    nextCompletingClaim: () =>
      attempt(async () => {
        const raw = (
          await query(
            `SELECT ${columns} FROM watchdog_jobs WHERE state='completing' ORDER BY sequence LIMIT 1`,
          )
        )[0];
        if (raw === undefined) return undefined;
        const found = parseJobRow(raw);
        if (found.job.state !== "completing" || found.row.claim_token === null)
          throw new WatchdogError("store_unavailable");
        return { job: found.job, claimToken: found.row.claim_token };
      }),
    finishCompletingClaim: (input) =>
      attempt(async () => {
        const raw = (
          await query(
            `UPDATE watchdog_jobs SET state='settled' WHERE job_id=$1 AND state='completing' AND claim_token=$2 RETURNING ${columns}`,
            [input.jobId, input.claimToken],
          )
        )[0];
        if (raw !== undefined) {
          const found = parseJobRow(raw);
          if (found.job.state !== "settled")
            throw new WatchdogError("store_unavailable");
          return { status: "settled", job: found.job };
        }
        const prior = await row(input.jobId);
        if (prior === undefined) return { status: "missing" };
        return prior.job.state === "settled" &&
          prior.row.claim_token === input.claimToken
          ? { status: "replayed", job: prior.job }
          : { status: "conflict" };
      }),
    recordCancellation: (intent) =>
      attempt(async () => {
        const updated = await query(
          `UPDATE watchdog_jobs SET cancellation_id=$1 WHERE job_id=$2
        AND state IN ('queued','running') AND cancellation_id IS NULL ${intent.onlyIfQueued === true ? "AND state='queued'" : ""}
        RETURNING job_id`,
          [intent.cancellationId, intent.jobId],
        );
        if (updated.length > 0) return { status: "recorded" };
        const prior = await row(intent.jobId);
        if (prior?.job.state === "completing" || prior?.job.state === "settled")
          return { status: "decided", outcome: prior.job.outcome };
        return {
          status:
            prior === undefined
              ? "missing"
              : prior.row.cancellation_id === intent.cancellationId
                ? "replayed"
                : "conflict",
        };
      }),
    failLane: (raw) =>
      attempt(async () => {
        const input = parseLaneFailure(raw);
        // Under the declared state grammar, DESC puts running before queued.
        const rows = await query(
          `UPDATE watchdog_jobs SET state='settled', outcome='failed', failure_reason=$1, lease_until=NULL, claim_token=NULL
         WHERE job_id IN (SELECT job_id FROM watchdog_jobs WHERE lane=$2 AND state IN ('queued','running','completing')
           ORDER BY state DESC, sequence ASC LIMIT $3)
           AND state IN ('queued','running','completing') RETURNING ${columns}`,
          [input.reason, input.lane, laneFailureBatchSize],
        );
        const jobs = rows.map((row) => {
          const parsed = parseJobRow(row);
          if (parsed.job.state !== "settled")
            throw new WatchdogError("store_unavailable");
          claims.delete(parsed.job.jobId);
          return parsed.job;
        });
        const more =
          (
            await query(
              "SELECT 1 AS pending FROM watchdog_jobs WHERE lane=$1 AND state IN ('queued','running','completing') LIMIT 1",
              [input.lane],
            )
          ).length !== 0;
        return { jobs, more };
      }),
    purgeQueued: (queue) =>
      attempt(async () => {
        await query(
          "DELETE FROM watchdog_jobs WHERE queue_name=$1 AND state='queued'",
          [queue],
        );
      }),
    handoffExactClaim: (input) =>
      attempt(async () => {
        const changed = await query(
          `UPDATE watchdog_jobs SET state='queued',claim_token=NULL,lease_until=NULL
        WHERE job_id=$1 AND state='running' AND claim_token=$2 AND lease_until>${at(3)} AND cancellation_id IS NULL RETURNING job_id`,
          [input.jobId, input.claimToken, now()],
        );
        if (changed.length > 0) {
          claims.delete(input.jobId);
          return { status: "requeued" };
        }
        const prior = await row(input.jobId);
        if (prior === undefined) return { status: "missing" };
        return prior.job.state === "running" &&
          prior.row.claim_token === input.claimToken &&
          prior.row.cancellation_id !== null
          ? { status: "cancelled" }
          : { status: "conflict" };
      }),
    nextRunningClaim: () =>
      attempt(async () => {
        const raw = (
          await query(`SELECT ${columns} FROM watchdog_jobs WHERE state='running'
        ORDER BY lease_until,sequence LIMIT 1`)
        )[0];
        if (raw === undefined) return undefined;
        const found = parseJobRow(raw);
        if (found.job.state !== "running" || found.row.claim_token === null)
          throw new WatchdogError("store_unavailable");
        return {
          job: found.job,
          claimToken: found.row.claim_token,
          recoveryCount: found.row.recovery_count,
        };
      }),
    reconcileDisappearedClaim: (input) =>
      attempt(async () => {
        const prior = await row(input.job.jobId);
        if (prior === undefined) return { status: "missing" };
        if (
          prior.job.state !== "running" ||
          prior.row.claim_token !== input.claimToken ||
          prior.row.recovery_count !== input.recoveryCount
        )
          return { status: "conflict" };
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
              now(),
            );
        const requeue = recovery.kind === "requeue";
        const reason =
          input.job.recovery === undefined
            ? "Execution disappeared without a recovery policy"
            : `Restart recovery budget exhausted (${String(input.job.recovery.maxRecoveries)} recoveries)`;
        const raw = (
          await query(
            `UPDATE watchdog_jobs SET
        state=CASE WHEN $1=1 AND cancellation_id IS NULL THEN 'queued' ELSE 'settled' END,
        outcome=CASE WHEN cancellation_id IS NOT NULL THEN 'cancelled' WHEN $1=1 THEN NULL ELSE 'interrupted' END,
        claim_token=CASE WHEN $1=1 AND cancellation_id IS NULL THEN NULL ELSE claim_token END,
        recovery_count=CASE WHEN $1=1 AND cancellation_id IS NULL THEN $6 ELSE recovery_count END,
        recovery_window_started_at=CASE WHEN $1=1 AND cancellation_id IS NULL THEN $7 ELSE recovery_window_started_at END,
        failure_reason=CASE WHEN $1=0 AND cancellation_id IS NULL THEN $2 ELSE NULL END,lease_until=NULL
        WHERE job_id=$3 AND state='running' AND claim_token=$4 AND recovery_count=$5 AND lease_until<=${at(8)}
        RETURNING ${columns}`,
            [
              requeue ? 1 : 0,
              reason,
              input.job.jobId,
              input.claimToken,
              input.recoveryCount,
              recovery.kind === "requeue"
                ? recovery.recoveryCount
                : input.recoveryCount,
              recovery.kind === "requeue"
                ? recovery.windowStartedAt
                : prior.row.recovery_window_started_at,
              now(),
            ],
          )
        )[0];
        if (raw === undefined) return { status: "conflict" };
        const parsed = parseJobRow(raw);
        if (parsed.job.state === "queued") return { status: "requeued" };
        if (parsed.job.state === "settled")
          return { status: "settled", job: parsed.job };
        throw new WatchdogError("store_unavailable");
      }),
  };
  return {
    store,
    isAlive: (jobId: string) =>
      attempt(async () => {
        const raw = (
          await query(
            `SELECT CASE WHEN lease_until>${at(2)} THEN 1 ELSE 0 END AS alive FROM watchdog_jobs WHERE job_id=$1 AND state='running'`,
            [jobId, now()],
          )
        )[0];
        return (
          raw !== undefined && Schema.decodeUnknownSync(Alive)(raw).alive === 1
        );
      }),
    renew: (jobId: string) =>
      attempt(async () => {
        const token = claims.get(jobId);
        if (token === undefined) throw new WatchdogError("claim_unavailable");
        const rows = await query(
          `UPDATE watchdog_jobs SET lease_until=${at(1)}+${String(leaseMs)} WHERE job_id=$2 AND state='running'
        AND claim_token=$3 AND lease_until>${at(4)} AND cancellation_id IS NULL RETURNING job_id`,
          [now(), jobId, token, now()],
        );
        if (rows.length === 0) throw new WatchdogError("claim_unavailable");
      }),
  };
}
