import { backoffDelay } from "@fungi.computer/almanac";
import * as Schema from "effect/Schema";
import type { WatchdogAttempt, WatchdogRetryPolicy } from "../effect.js";

const Count = Schema.Int.check(
  Schema.isGreaterThanOrEqualTo(0),
  Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
);
const Policy = Schema.Struct({
  baseMs: Count,
  maxMs: Count,
  limit: Schema.Union([
    Schema.Struct({ maxAttempts: Count }),
    Schema.Struct({ deadlineAt: Count }),
  ]),
});
export const parseRetryPolicy = Schema.decodeUnknownSync(Policy);

export function retryDue(
  count: number,
  now: number,
  policy: WatchdogRetryPolicy,
): number {
  return Math.min(
    now + backoffDelay(count, policy),
    "deadlineAt" in policy.limit
      ? policy.limit.deadlineAt
      : Number.MAX_SAFE_INTEGER,
  );
}

export function reserveRetry(
  count: number,
  now: number,
  policy: WatchdogRetryPolicy,
) {
  const exhausted =
    "maxAttempts" in policy.limit
      ? count >= policy.limit.maxAttempts
      : now >= policy.limit.deadlineAt;
  const charged = exhausted ? count : count + 1;
  return {
    attempt: {
      count: charged,
      status: exhausted ? "exhausted" : "admitted",
      final:
        exhausted ||
        ("maxAttempts" in policy.limit && charged >= policy.limit.maxAttempts),
    } satisfies WatchdogAttempt,
    notBefore: retryDue(charged, now, policy),
  };
}
