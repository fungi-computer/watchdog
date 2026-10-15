import { defineIdFamily } from "@fungi.computer/cairn";

export const watchdogIds = defineIdFamily({
  family: "watchdog",
  version: 1,
  kinds: {
    claim: { prefix: "watchdog_claim" },
  },
});
