import * as Schema from "effect/Schema";
import { WatchdogQueuedJobSchema, type WatchdogQueuedJob } from "../effect.js";
import { laneMaxLength } from "./lane.js";

export type SqlValue = string | number | null;
export type SqlRow = Record<string, SqlValue>;
export type Statement = Readonly<{ text: string; values: readonly SqlValue[] }>;
export type SqlDatabase = Readonly<{
  dialect: "sqlite" | "postgres";
  clock?: string | undefined;
  query: (statement: Statement) => Promise<readonly SqlRow[]>;
}>;

export const columns = `sequence, job_id, queue_name, lane, priority, payload_json, not_before, attempt_count,
  recovery_max, recovery_window_ms, recovery_window_started_at, state, outcome, claim_token, recovery_count, cancellation_id, failure_reason`;

export function schemaStatements(postgres: boolean): readonly string[] {
  return [
    `CREATE TABLE IF NOT EXISTS watchdog_jobs (
      sequence ${postgres ? "SERIAL PRIMARY KEY" : "INTEGER PRIMARY KEY AUTOINCREMENT"},
      job_id TEXT NOT NULL UNIQUE CHECK(length(job_id) BETWEEN 1 AND 256),
      queue_name TEXT NOT NULL CHECK(length(queue_name) BETWEEN 1 AND 256),
      lane TEXT NOT NULL CHECK(length(lane) BETWEEN 1 AND ${String(laneMaxLength)}),
      priority ${postgres ? "DOUBLE PRECISION" : "INTEGER"} NOT NULL,
      payload_json TEXT NOT NULL,
      not_before ${postgres ? "DOUBLE PRECISION" : "INTEGER"} NOT NULL DEFAULT 0 CHECK(not_before>=0),
      attempt_count ${postgres ? "DOUBLE PRECISION" : "INTEGER"} NOT NULL DEFAULT 0 CHECK(attempt_count>=0),
      recovery_max ${postgres ? "DOUBLE PRECISION" : "INTEGER"} CHECK(recovery_max IS NULL OR recovery_max >= 0),
      recovery_window_ms ${postgres ? "DOUBLE PRECISION" : "INTEGER"} CHECK(recovery_window_ms IS NULL OR recovery_window_ms > 0),
      recovery_window_started_at ${postgres ? "DOUBLE PRECISION" : "INTEGER"},
      state TEXT NOT NULL CHECK(state IN ('queued','running','completing','settled')),
      outcome TEXT CHECK(outcome IN ('completed','failed','cancelled','outcome_unknown','interrupted')),
      claim_token TEXT,
      recovery_count ${postgres ? "DOUBLE PRECISION" : "INTEGER"} NOT NULL DEFAULT 0,
      cancellation_id TEXT UNIQUE,
      failure_reason TEXT,
      lease_until ${postgres ? "DOUBLE PRECISION" : "INTEGER"},
      CONSTRAINT watchdog_execution_state CHECK ((state='queued' AND outcome IS NULL AND claim_token IS NULL) OR
        (state='running' AND outcome IS NULL AND claim_token IS NOT NULL AND lease_until IS NOT NULL) OR
        (state='completing' AND outcome IS NOT NULL AND claim_token IS NOT NULL) OR
        (state='settled' AND outcome IS NOT NULL))
    )`,
    "CREATE INDEX IF NOT EXISTS idx_watchdog_lane_failure ON watchdog_jobs(lane,state DESC,sequence ASC)",
    "CREATE INDEX IF NOT EXISTS idx_watchdog_ready ON watchdog_jobs(state,not_before,priority DESC,sequence)",
    "CREATE INDEX IF NOT EXISTS idx_watchdog_queue_head ON watchdog_jobs(queue_name,state,priority DESC,sequence)",
    "CREATE INDEX IF NOT EXISTS idx_watchdog_lease ON watchdog_jobs(state,lease_until,sequence)",
  ];
}

// A conflicting replay must fail the caller's entire native transaction. Setting
// the NOT NULL identity to NULL is a portable constraint failure, not a silent
// DO NOTHING that would commit the producer fact with an unrelated old job.
export function enqueueSelect(source: Statement): Statement {
  return {
    text: `INSERT INTO watchdog_jobs
      (job_id,queue_name,lane,priority,payload_json,recovery_max,recovery_window_ms,not_before,state)
      SELECT job_id,queue_name,lane,priority,payload_json,recovery_max,recovery_window_ms,not_before,'queued'
      FROM (${source.text}) AS admission WHERE true
      ON CONFLICT(job_id) DO UPDATE SET job_id = NULL WHERE NOT (
        watchdog_jobs.queue_name=excluded.queue_name AND watchdog_jobs.lane=excluded.lane AND
        watchdog_jobs.priority=excluded.priority AND watchdog_jobs.payload_json=excluded.payload_json AND
        (watchdog_jobs.recovery_max=excluded.recovery_max OR
          (watchdog_jobs.recovery_max IS NULL AND excluded.recovery_max IS NULL)) AND
        (watchdog_jobs.recovery_window_ms=excluded.recovery_window_ms OR
          (watchdog_jobs.recovery_window_ms IS NULL AND excluded.recovery_window_ms IS NULL))
        )
      RETURNING ${columns}`,
    values: source.values,
  };
}

export function enqueueStatement(input: WatchdogQueuedJob): Statement {
  const job = Schema.decodeUnknownSync(WatchdogQueuedJobSchema)(input);
  return enqueueSelect({
    text: `SELECT CAST($1 AS TEXT) AS job_id,CAST($2 AS TEXT) AS queue_name,CAST($3 AS TEXT) AS lane,CAST($4 AS DOUBLE PRECISION) AS priority,
      CAST($5 AS TEXT) AS payload_json,CAST($6 AS DOUBLE PRECISION) AS recovery_max,CAST($7 AS DOUBLE PRECISION) AS recovery_window_ms,CAST($8 AS DOUBLE PRECISION) AS not_before`,
    values: [
      job.jobId,
      job.queue,
      job.lane,
      job.priority,
      Schema.encodeSync(Schema.fromJsonString(Schema.Unknown))(job.payload),
      job.recovery?.maxRecoveries ?? null,
      job.recovery?.windowMs ?? null,
      job.notBefore ?? 0,
    ],
  });
}
