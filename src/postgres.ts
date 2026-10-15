import type { Pool, PoolClient } from "pg";
import type { WatchdogQueuedJob } from "./effect.js";
import {
  createAsyncRuntime,
  type AsyncWatchdogOptions,
  type AsyncWatchdogRuntime,
} from "./internal/async-runtime.js";
import {
  enqueueStatement,
  schemaStatements,
  type SqlRow,
} from "./internal/async-sql.js";

export type {
  AsyncWatchdogOptions,
  AsyncWatchdogRuntime,
} from "./internal/async-runtime.js";
/** PostgreSQL runtime with admission joined to the caller's checked-out client. */
export type PostgresWatchdog = AsyncWatchdogRuntime &
  Readonly<{
    /** Enqueue on the existing transaction; never BEGIN, COMMIT or ROLLBACK here. */
    enqueue: (
      client: Pick<PoolClient, "query">,
      job: WatchdogQueuedJob,
    ) => Promise<void>;
  }>;
/** Package-owned schema statements, to run through the deployment migration owner. */
export const watchdogPostgresSchema: readonly string[] = schemaStatements(true);

/** Open the shared Effect engine on a migrated PostgreSQL database. */
export function createPostgresWatchdog(
  options: AsyncWatchdogOptions & Readonly<{ database: Pick<Pool, "query"> }>,
): PostgresWatchdog {
  const runtime = createAsyncRuntime(
    {
      clock:
        options.now === undefined
          ? "EXTRACT(EPOCH FROM clock_timestamp())*1000"
          : undefined,
      query: async (statement) =>
        (
          await options.database.query<SqlRow>(statement.text, [
            ...statement.values,
          ])
        ).rows,
    },
    options,
  );
  return {
    ...runtime,
    enqueue: async (client, job) => {
      const statement = enqueueStatement(job);
      await client.query(statement.text, [...statement.values]);
    },
  };
}

/** Parameterized queue purge joined to a producer's existing PostgreSQL transaction. */
export type PostgresPurgeStatement = Readonly<{
  text: string;
  values: readonly (string | number | null)[];
}>;
/** Purge every attempt in the queues yielded by trusted producer SQL. */
export function purgePostgresQueues(
  source: PostgresPurgeStatement,
): readonly PostgresPurgeStatement[] {
  return [
    {
      text: `DELETE FROM watchdog_jobs WHERE queue_name IN (SELECT queue_name FROM (${source.text}) AS erasure)`,
      values: source.values,
    },
  ];
}
