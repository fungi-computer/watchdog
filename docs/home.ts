import { DatabaseSync } from "node:sqlite";
import { fromNodeSqlite } from "@fungi.computer/watchdog/node-sqlite";
import { Watchdog } from "@fungi.computer/watchdog";

const watchdog = await Watchdog.make({
  owner: fromNodeSqlite(new DatabaseSync(":memory:")),
  execute: async () => (
    console.log("sending welcome-1"),
    { kind: "completed" }
  ),
  isAlive: async () => true,
  wake: { recompute: async () => {} },
});

await watchdog.ingest({
  jobId: "welcome-1",
  queue: "email",
  lane: "default",
  priority: 0,
  payload: {},
  state: "queued",
});
const tick = await watchdog.tick();
console.log(tick.status); // "settled"
