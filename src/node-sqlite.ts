import type { DatabaseSync, SQLOutputValue } from "node:sqlite";
import * as Schema from "effect/Schema";
import { watchdogSqliteRowSchema } from "./effect.js";
import type {
  WatchdogSqliteOwner,
  WatchdogSqliteCursor,
  WatchdogSqliteRow,
  WatchdogSqliteValue,
} from "./effect.js";

/** Materialized Node SQLite rows, including the exact-one native operation. */
export type NodeSqliteCursor = WatchdogSqliteCursor &
  Readonly<{
    /** Return the sole decoded row; throw for zero or multiple rows. */
    one: () => WatchdogSqliteRow;
  }>;

/** Concrete Node adapter; generic Watchdog owners need only toArray(). */
export type NodeSqliteOwner = Omit<WatchdogSqliteOwner, "sql"> &
  Readonly<{
    /** Statements execute synchronously against the caller-owned database. */
    sql: Readonly<{
      /** Decode materialized scalar rows without choosing the query owner’s schema. */
      exec: (
        statement: string,
        ...bindings: WatchdogSqliteValue[]
      ) => NodeSqliteCursor;
    }>;
  }>;

const decodeRow = Schema.decodeUnknownSync(watchdogSqliteRowSchema);

function fromSqlite(value: SQLOutputValue): WatchdogSqliteValue {
  if (Schema.is(Schema.BigInt)(value)) return Number(value);
  if (ArrayBuffer.isView(value)) return new Uint8Array(value).slice().buffer;
  return value;
}

/** Adapt a caller-owned Node SQLite database; the caller retains its lifetime. */
export function fromNodeSqlite(database: DatabaseSync): NodeSqliteOwner {
  let transaction = 0;
  return {
    sql: {
      exec: (statement, ...bindings) => {
        const prepared = database.prepare(statement);
        const values = bindings.map((value) =>
          value instanceof ArrayBuffer ? new Uint8Array(value) : value,
        );
        let rows: WatchdogSqliteRow[];
        if (prepared.columns().length > 0) {
          rows = prepared
            .all(...values)
            .map((row) =>
              decodeRow(
                Object.fromEntries(
                  Object.entries(row).map(([key, value]) => [
                    key,
                    fromSqlite(value),
                  ]),
                ),
              ),
            );
        } else {
          // Schema owners supply several unbound DDL statements in one call.
          // DatabaseSync.exec owns that grammar; prepare/run executes only one.
          if (values.length === 0) database.exec(statement);
          else prepared.run(...values);
          rows = [];
        }
        return {
          toArray: () => rows,
          one: () => {
            const row = rows[0];
            if (rows.length !== 1 || row === undefined)
              throw new Error(
                `Expected exactly one SQLite row; got ${String(rows.length)}`,
              );
            return row;
          },
        };
      },
    },
    transactionSync: (operation) => {
      const name = `watchdog_owner_${String(transaction++)}`;
      database.exec(`SAVEPOINT ${name}`);
      try {
        const result = operation();
        database.exec(`RELEASE ${name}`);
        return result;
      } catch (cause) {
        database.exec(`ROLLBACK TO ${name}`);
        database.exec(`RELEASE ${name}`);
        throw cause;
      }
    },
  };
}
