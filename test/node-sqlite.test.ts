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
