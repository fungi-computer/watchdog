import * as Effect from "effect/Effect";

import type { EffectWatchdogOptions } from "../effect.js";
import type { WatchdogOptions } from "../index.js";
import { WatchdogError } from "./watchdog-error.js";

/** Lift Promise host ports into the Effect-owned runtime. */
export const adaptPromiseWatchdogHost = (
  options: WatchdogOptions,
): EffectWatchdogOptions => {
  return {
    owner: options.owner,
    project: options.project,
    isAlive: (job) =>
      Effect.tryPromise({
        try: () => options.isAlive(job),
        catch: (error) => new WatchdogError("liveness_unavailable", error),
      }),
    executor: {
      retryPolicy: (job) => {
        const policy = options.retryPolicy;
        return policy === undefined
          ? Effect.succeed(undefined)
          : Effect.tryPromise({
              try: () => policy(job),
              catch: (error) => new WatchdogError("execution_failed", error),
            });
      },
      execute: (job, attempt) =>
        Effect.tryPromise({
          try: (signal) => options.execute(job, signal, attempt),
          catch: () => new WatchdogError("execution_failed"),
        }),
    },
    wake: {
      recompute: () =>
        Effect.tryPromise({
          try: () => options.wake.recompute(),
          catch: () => new WatchdogError("wake_unavailable"),
        }),
    },
  };
};
