import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import { expect, it } from "vitest";

import {
  WatchdogQueuedJobSchema,
  WatchdogRunningJobSchema,
  WatchdogSettledJobSchema,
} from "../src/effect.js";

const genericQueuedJob = {
  jobId: "job-opaque-1",
  queue: "arbitrary-queue",
  lane: "maintenance",
  priority: 4,
  payload: { kind: "opaque", values: ["alpha", 7] },
  state: "queued",
} as const;

it.each([256, 263, 265, 512])(
  "accepts a %i-character lane in every public job state",
  (length) => {
    const lane = "l".repeat(length);
    expect(
      Schema.decodeUnknownSync(WatchdogQueuedJobSchema)({
        ...genericQueuedJob,
        lane,
      }).lane,
    ).toBe(lane);
    expect(
      Schema.decodeUnknownSync(WatchdogRunningJobSchema)({
        ...genericQueuedJob,
        lane,
        state: "running",
      }).lane,
    ).toBe(lane);
    expect(
      Schema.decodeUnknownSync(WatchdogSettledJobSchema)({
        ...genericQueuedJob,
        lane,
        state: "settled",
        outcome: "completed",
      }).lane,
    ).toBe(lane);
  },
);

it("refuses lanes above 512 while keeping job ids and queues bounded at 256", () => {
  for (const invalid of [
    { ...genericQueuedJob, lane: "l".repeat(513) },
    { ...genericQueuedJob, jobId: "j".repeat(257) },
    { ...genericQueuedJob, queue: "q".repeat(257) },
  ])
    expect(() =>
      Schema.decodeUnknownSync(WatchdogQueuedJobSchema)(invalid),
    ).toThrow();
});

it("accepts a queued generic job with arbitrary routing and opaque payload", async () => {
  const decoded = await Effect.runPromiseExit(
    Schema.decodeUnknownEffect(WatchdogQueuedJobSchema)(genericQueuedJob),
  );

  expect(Exit.isSuccess(decoded)).toBe(true);
});

it("keeps the private recovery count out of public job data", async () => {
  const decoded = await Effect.runPromiseExit(
    Schema.decodeUnknownEffect(WatchdogQueuedJobSchema)({
      ...genericQueuedJob,
      recoveryCount: 99,
    }),
  );

  expect(Exit.isSuccess(decoded)).toBe(true);
  if (Exit.isSuccess(decoded))
    expect(decoded.value).not.toHaveProperty("recoveryCount");
});
