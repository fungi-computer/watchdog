import * as Effect from "effect/Effect";
import { expect, it } from "vitest";

import {
  makeRuntime,
  type WatchdogQueuedJob,
  type WatchdogSqliteOwner,
} from "../src/effect.js";
import { WatchdogError } from "../src/index.js";
import { createSqliteOwnerFixture } from "./sqlite-owner.fixture.js";

const job: WatchdogQueuedJob = {
  jobId: "job-store-cause",
  queue: "arbitrary-queue",
  lane: "arbitrary-lane",
  priority: 0,
  payload: { opaque: true },
  state: "queued",
};

it("keeps the driver and decode failures behind store_unavailable", async () => {
  const fixture = createSqliteOwnerFixture();
  const driverFailure = new Error("SQLITE_IOERR: disk I/O error");
  let broken = false;
  const owner: WatchdogSqliteOwner = {
    ...fixture.owner,
    sql: {
      exec: (statement, ...bindings) => {
        if (broken) throw driverFailure;
        return fixture.owner.sql.exec(statement, ...bindings);
      },
    },
  };
  try {
    const runtime = await Effect.runPromise(
      makeRuntime({
        owner,
        executor: { execute: () => Effect.succeed({ kind: "completed" }) },
        isAlive: () => Effect.succeed(true),
        wake: { recompute: () => Effect.void },
      }),
    );

    broken = true;
    const driver = await Effect.runPromise(Effect.flip(runtime.ingest(job)));
    expect(driver).toBeInstanceOf(WatchdogError);
    expect(driver).toMatchObject({ cause: "store_unavailable" });
    expect(driver.underlying).toBe(driverFailure);
    broken = false;

    // A stored row that violates the row grammar fails as a decode.
    fixture.owner.sql.exec(
      `INSERT INTO watchdog_jobs
         (job_id, queue_name, lane, priority, payload_json, state)
       VALUES ('job-corrupt', 'arbitrary-queue', '', 0, '{}', 'queued')`,
    );
    const decode = await Effect.runPromise(
      Effect.flip(runtime.read("job-corrupt")),
    );
    expect(decode).toMatchObject({ cause: "store_unavailable" });
    expect(decode.underlying).toMatchObject({ _tag: "SchemaError" });
  } finally {
    fixture.close();
  }
});
