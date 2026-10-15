import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import type { WatchdogExecutor, WatchdogRuntime } from "../index.js";
import { WatchdogError } from "./watchdog-error.js";
import { bindWatchdogEngine } from "./runtime.js";
import { openAsyncStore, heartbeatMs, attempt } from "./async-store.js";
import type { SqlDatabase } from "./async-sql.js";

/** Host execution and wake scheduling for a shared-database Watchdog. */
export type AsyncWatchdogOptions = Readonly<{
  /** Execute one claimed job; honour abort when its lease is lost. */
  execute: WatchdogExecutor;
  /** Recompute a durable wake after a transition commits. */
  wake: Readonly<{
    /** Arm the next durable owner wake. */
    recompute: () => Promise<void>;
  }>;
  /** Clock used consistently for durable leases; defaults to Date.now. */
  now?: () => number;
}>;
/** Shared-database runtime; native admission replaces the SQLite projection. */
export type AsyncWatchdogRuntime = Omit<WatchdogRuntime, "transactional">;

function run<A>(effect: Effect.Effect<A, WatchdogError>): Promise<A> {
  return Effect.runPromiseExit(effect).then((exit) => {
    if (Exit.isSuccess(exit)) return exit.value;
    throw (
      Option.getOrUndefined(Cause.findErrorOption(exit.cause)) ??
      Cause.squash(exit.cause)
    );
  });
}

export function createAsyncRuntime(
  database: SqlDatabase,
  options: AsyncWatchdogOptions,
): AsyncWatchdogRuntime {
  const state = openAsyncStore(database, options.now ?? Date.now);
  const renew = (jobId: string) =>
    state.renew(jobId).pipe(
      Effect.timeout(heartbeatMs),
      Effect.mapError(() => new WatchdogError("claim_unavailable")),
    );
  const runtime = bindWatchdogEngine(
    {
      isAlive: (job) => state.isAlive(job.jobId),
      executor: {
        execute: (job) =>
          renew(job.jobId).pipe(
            Effect.andThen(
              Effect.raceFirst(
                Effect.tryPromise({
                  try: (signal) => options.execute(job, signal),
                  catch: () => new WatchdogError("execution_failed"),
                }),
                Effect.forever(
                  Effect.sleep(heartbeatMs).pipe(
                    Effect.andThen(renew(job.jobId)),
                  ),
                ),
              ),
            ),
          ),
      },
      wake: { recompute: () => attempt(options.wake.recompute) },
    },
    state.store,
  );
  return {
    hasPendingWork: () => run(runtime.hasPendingWork),
    ingest: (job) => run(runtime.ingest(job)),
    read: (id) => run(runtime.read(id)),
    readQueueHead: (queue) => run(runtime.readQueueHead(queue)),
    tick: () => run(runtime.tick()),
    cancel: (intent) => run(runtime.cancel(intent)),
    failLane: (input) => run(runtime.failLane(input)),
    purge: (queue) => run(runtime.purge(queue)),
  };
}
