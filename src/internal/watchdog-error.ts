/** Typed failures owned by the generic Watchdog engine. */
export class WatchdogError extends Error {
  public readonly _tag = "WatchdogError" as const;
  public override readonly cause:
    | "store_unavailable"
    | "store_invalid"
    | "claim_unavailable"
    | "execution_failed"
    | "settlement_rejected"
    | "wake_unavailable"
    | "liveness_unavailable";
  public readonly underlying?: unknown;

  public constructor(
    failureKind:
      | "store_unavailable"
      | "store_invalid"
      | "claim_unavailable"
      | "execution_failed"
      | "settlement_rejected"
      | "wake_unavailable"
      | "liveness_unavailable",
    /**
     * The original thrown value, kept for server-side diagnosis only. `cause`
     * is already the closed category above, so this is a separate field.
     */
    cause?: unknown,
  ) {
    super(`Watchdog ${failureKind.replaceAll("_", " ")}`);
    this.name = this._tag;
    this.cause = failureKind;
    this.underlying = cause;
  }
}
