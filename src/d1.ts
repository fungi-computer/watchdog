import type {
  D1Database,
  D1PreparedStatement,
} from "@cloudflare/workers-types";
import type { WatchdogQueuedJob } from "./effect.js";
import {
  createAsyncRuntime,
  type AsyncWatchdogOptions,
  type AsyncWatchdogRuntime,
} from "./internal/async-runtime.js";
import {
  enqueueSelect,
  enqueueStatement,
  schemaStatements,
  type SqlRow,
  type Statement,
} from "./internal/async-sql.js";

export type {
  AsyncWatchdogOptions,
  AsyncWatchdogRuntime,
} from "./internal/async-runtime.js";
/** Trusted parameter-bound SELECT yielding queued job fields; never lifecycle fields. */
export type D1AdmissionSource = Readonly<{
  /** Trusted native SELECT text with SQLite ?NNN bindings. */
  text: string;
  /** Scalar values bound by the native driver. */
  values: readonly (string | number | null)[];
}>;

/** D1 runtime with admission statements that join the producer's native batch. */
export type D1Admission = Readonly<{
  /** Prepare a typed queued job without executing or committing it. */
  enqueue: (job: WatchdogQueuedJob) => D1PreparedStatement;
  /** Prepare jobs selected inside the same batch as the producer writes. */
  enqueueFromSelect: (source: D1AdmissionSource) => D1PreparedStatement;
}>;

/** D1 lifecycle and native transaction admission. */
export type D1Watchdog = AsyncWatchdogRuntime & D1Admission;

/** Package-owned schema statements, to run through the deployment migration owner. */
export const watchdogD1Schema: readonly string[] = schemaStatements(false);

/** Open the shared Effect engine on a migrated D1 database; no implicit schema writes. */
export function createD1Watchdog(
  options: AsyncWatchdogOptions & Readonly<{ database: D1Database }>,
): D1Watchdog {
  // Only package-owned templates use $N; caller SELECTs remain native SQL.
  const prepare = (statement: Statement) =>
    options.database
      .prepare(statement.text.replace(/\$(\d+)/gu, "?$1"))
      .bind(...statement.values);
  const runtime = createAsyncRuntime(
    {
      dialect: "sqlite",
      clock: options.now === undefined ? "unixepoch('subsec')*1000" : undefined,
      query: async (statement) =>
        (await prepare(statement).all<SqlRow>()).results,
    },
    options,
  );
  return { ...runtime, ...prepareD1Admission(options.database) };
}

/** Prepare native admission without constructing an executor or performing I/O. */
export function prepareD1Admission(database: D1Database): D1Admission {
  return {
    enqueue: (job) => {
      const statement = enqueueStatement(job);
      return database
        .prepare(statement.text.replace(/\$(\d+)/gu, "?$1"))
        .bind(...statement.values);
    },
    enqueueFromSelect: (source) => {
      const statement = enqueueSelect(source);
      return database.prepare(statement.text).bind(...statement.values);
    },
  };
}

/** Parameterized native purge; caller commits it with its own erasure facts. */
export type D1PurgeStatement = Readonly<{
  text: string;
  values: readonly (string | number | null)[];
}>;
/** Purge every attempt in selected queues, fencing any executor still holding a claim.
 * The trusted SELECT yields queue_name. It never supplies lifecycle mutations. */
export function purgeD1Queues(
  source: D1PurgeStatement,
): readonly D1PurgeStatement[] {
  return [
    {
      text: `DELETE FROM watchdog_jobs WHERE queue_name IN (SELECT queue_name FROM (${source.text}) AS erasure)`,
      values: source.values,
    },
  ];
}
