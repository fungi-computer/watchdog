import * as Schema from "effect/Schema";

// A 256-character identity plus "team-key:" is 265 characters ("member:"
// needs 263). Leave composition headroom without changing the identity or
// widening job/queue identifiers. Admission, storage and runtime share this bound.
export const laneMaxLength = 512;
export const laneSchema = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(laneMaxLength),
);
