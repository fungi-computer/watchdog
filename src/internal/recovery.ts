import type { WatchdogRunningJob } from "../effect.js";

/** Decide one unexpected disappearance from its durable count and window. */
export function disappearanceRecovery(
  policy: WatchdogRunningJob["recovery"],
  recoveryCount: number,
  windowStartedAt: number | null,
  now: number,
):
  | Readonly<{ kind: "interrupted" }>
  | Readonly<{
      kind: "requeue";
      recoveryCount: number;
      windowStartedAt: number | null;
    }> {
  if (policy === undefined) return { kind: "interrupted" };
  const expired =
    policy.windowMs !== undefined &&
    windowStartedAt !== null &&
    now - windowStartedAt >= policy.windowMs;
  const count = expired ? 0 : recoveryCount;
  if (count >= policy.maxRecoveries) return { kind: "interrupted" };
  return {
    kind: "requeue",
    recoveryCount: count + 1,
    windowStartedAt:
      policy.windowMs === undefined
        ? null
        : expired || windowStartedAt === null
          ? now
          : windowStartedAt,
  };
}
