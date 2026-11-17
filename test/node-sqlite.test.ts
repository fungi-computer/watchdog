import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { expect, it } from "vitest";
import { fromNodeSqlite } from "../src/node-sqlite.js";

it("round-trips owner scalars and blobs through a returning write", () => {
  const database = new DatabaseSync(":memory:");
  try {
    const owner = fromNodeSqlite(database);
    owner.sql.exec(
      "CREATE TABLE sample (name TEXT, amount REAL, payload BLOB, absent TEXT)",
    );
    const bytes = new Uint8Array([0, 128, 255]).buffer;
    const rows = owner.sql
      .exec(
        "INSERT INTO sample VALUES (?, ?, ?, ?) RETURNING name, amount, payload, absent",
        "hello",
        2.5,
        bytes,
        null,
      )
      .toArray();
    expect(rows).toEqual([
      { name: "hello", amount: 2.5, payload: bytes, absent: null },
    ]);
    expect(rows[0]?.payload).toBeInstanceOf(ArrayBuffer);
    expect(owner.sql.exec("SELECT * FROM sample").toArray()).toEqual(rows);
  } finally {
    database.close();
  }
});

it("rolls back a failed inner transaction while committing its outer transaction", () => {
  const database = new DatabaseSync(":memory:");
  try {
    const owner = fromNodeSqlite(database);
    owner.sql.exec("CREATE TABLE sample (name TEXT)");
    const failure = new Error("inner failure");
    owner.transactionSync(() => {
      owner.sql.exec("INSERT INTO sample VALUES ('outer')");
      expect(() =>
        owner.transactionSync(() => {
          owner.sql.exec("INSERT INTO sample VALUES ('inner')");
          throw failure;
        }),
      ).toThrow(failure);
      owner.sql.exec("INSERT INTO sample VALUES ('after')");
    });
    expect(
      owner.sql.exec("SELECT name FROM sample ORDER BY rowid").toArray(),
    ).toEqual([{ name: "outer" }, { name: "after" }]);
    expect(() =>
      owner.transactionSync(() => {
        owner.sql.exec("INSERT INTO sample VALUES ('rolled back')");
        throw failure;
      }),
    ).toThrow(failure);
    expect(
      owner.sql.exec("SELECT COUNT(*) AS count FROM sample").toArray(),
    ).toEqual([{ count: 2 }]);
  } finally {
    database.close();
  }
});

it("one returns the decoded returning row and refuses zero or multiple rows", () => {
  const database = new DatabaseSync(":memory:");
  try {
    const owner = fromNodeSqlite(database);
    owner.sql.exec("CREATE TABLE sample (name TEXT, payload BLOB)");
    const bytes = new Uint8Array([0, 128, 255]).buffer;
    const row = owner.sql
      .exec(
        "INSERT INTO sample VALUES (?, ?) RETURNING name, payload",
        "first",
        bytes,
      )
      .one();
    expect(row).toEqual({ name: "first", payload: bytes });
    expect(row.payload).toBeInstanceOf(ArrayBuffer);
    expect(() =>
      owner.sql.exec("SELECT * FROM sample WHERE name = 'missing'").one(),
    ).toThrow("Expected exactly one SQLite row; got 0");
    owner.sql.exec("INSERT INTO sample VALUES ('second', NULL)");
    expect(() => owner.sql.exec("SELECT * FROM sample").one()).toThrow(
      "Expected exactly one SQLite row; got 2",
    );
    expect(owner.sql.exec("SELECT * FROM sample").toArray()).toHaveLength(2);
  } finally {
    database.close();
  }
});

it("executes the Team owner's multi-statement schema as one unbound call", () => {
  const database = new DatabaseSync(":memory:");
  try {
    const owner = fromNodeSqlite(database);
    expect(
      owner.sql
        .exec("CREATE TABLE first (id TEXT); CREATE TABLE second (id TEXT);")
        .toArray(),
    ).toEqual([]);
    expect(
      owner.sql
        .exec(
          "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name",
        )
        .toArray(),
    ).toEqual([{ name: "first" }, { name: "second" }]);
  } finally {
    database.close();
  }
});

it("a commit refused as busy rolls back: no open transaction is left, and others can write", () => {
  const directory = mkdtempSync(join(tmpdir(), "watchdog-busy-commit-"));
  const path = join(directory, "store.sqlite");
  const writer = new DatabaseSync(path, { timeout: 0 });
  const reader = new DatabaseSync(path, { timeout: 0 });
  const other = new DatabaseSync(path, { timeout: 0 });
  try {
    const owner = fromNodeSqlite(writer);
    owner.sql.exec("CREATE TABLE sample (name TEXT)");
    // A foreign reader holds a shared lock, so the writer's COMMIT is refused.
    reader.exec("BEGIN");
    reader.prepare("SELECT COUNT(*) FROM sample").all();
    expect(() => {
      owner.transactionSync(() => {
        owner.sql.exec("INSERT INTO sample VALUES ('refused')");
      });
    }).toThrow(/database is locked/u);
    // The connection is not left inside the transaction it could not commit.
    expect(writer.isTransaction).toBe(false);
    reader.exec("ROLLBACK");
    // Nothing of the refused transaction survives, and another connection can
    // write: before the fix the writer still held its lock.
    other.exec("INSERT INTO sample VALUES ('other')");
    owner.transactionSync(() => {
      owner.sql.exec("INSERT INTO sample VALUES ('retry')");
    });
    expect(
      other.prepare("SELECT name FROM sample ORDER BY rowid").all(),
    ).toEqual([{ name: "other" }, { name: "retry" }]);
  } finally {
    other.close();
    reader.close();
    writer.close();
    rmSync(directory, { recursive: true, force: true });
  }
});

it("a refused commit inside a caller's own transaction leaves that transaction to the caller", () => {
  const directory = mkdtempSync(join(tmpdir(), "watchdog-busy-nested-"));
  const path = join(directory, "store.sqlite");
  const writer = new DatabaseSync(path, { timeout: 0 });
  try {
    const owner = fromNodeSqlite(writer);
    owner.sql.exec("CREATE TABLE sample (name TEXT)");
    writer.exec("BEGIN");
    const failure = new Error("inner failure");
    expect(() =>
      owner.transactionSync(() => {
        owner.sql.exec("INSERT INTO sample VALUES ('inner')");
        throw failure;
      }),
    ).toThrow(failure);
    // Only the inner work rolled back; the caller's transaction is still open.
    expect(writer.isTransaction).toBe(true);
    writer.exec("ROLLBACK");
  } finally {
    writer.close();
    rmSync(directory, { recursive: true, force: true });
  }
});

it("surfaces the original error when SQLite already rolled the transaction back itself", () => {
  const database = new DatabaseSync(":memory:");
  try {
    const owner = fromNodeSqlite(database);
    owner.sql.exec("CREATE TABLE sample (name TEXT UNIQUE)");
    owner.sql.exec("INSERT INTO sample VALUES ('taken')");
    // OR ROLLBACK makes SQLite end the whole transaction itself, so there is
    // nothing left for the adapter to roll back.
    expect(() => {
      owner.transactionSync(() => {
        owner.sql.exec("INSERT OR ROLLBACK INTO sample VALUES ('taken')");
      });
    }).toThrow(/UNIQUE constraint failed/u);
    expect(database.isTransaction).toBe(false);
    owner.transactionSync(() => {
      owner.sql.exec("INSERT INTO sample VALUES ('next')");
    });
    expect(
      owner.sql.exec("SELECT COUNT(*) AS n FROM sample").toArray(),
    ).toEqual([{ n: 2 }]);
  } finally {
    database.close();
  }
});
