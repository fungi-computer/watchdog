import { DatabaseSync } from "node:sqlite";
import { fromNodeSqlite } from "../src/node-sqlite.js";

/** Real in-memory SQLite owner with the same structural shape as a DO owner. */
export function createSqliteOwnerFixture(filename = ":memory:") {
  const database = new DatabaseSync(filename);
  return Object.freeze({
    owner: fromNodeSqlite(database),
    close: () => {
      database.close();
    },
  });
}
