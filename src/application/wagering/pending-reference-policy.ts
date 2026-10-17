export const PENDING_REFERENCE_POLICY = Symbol('PendingReferencePolicy');

/**
 * Retry budget for transactions whose reference has not arrived yet.
 *
 * Default: 10 attempts, 2s base, doubling, capped at 5 min, with jitter —
 * roughly 15–25 minutes in total. Providers typically resend a missing BET
 * within seconds to a few minutes; past that window, keeping a REFUND/ROLLBACK
 * open only delays the provider's own reconciliation, so we reject with
 * REFERENCE_NOT_FOUND (a terminal, explicit answer) instead of waiting forever.
 */
export class PendingReferencePolicy {
  constructor(
    public readonly maxAttempts = 10,
    public readonly baseDelayMs = 2_000,
    public readonly maxDelayMs = 300_000,
    private readonly random: () => number = Math.random,
  ) {
    if (maxAttempts < 1) throw new Error('maxAttempts must be >= 1');
  }

  /** `attemptsSoFar` includes the attempt that just failed to resolve the reference. */
  isExhausted(attemptsSoFar: number): boolean {
    return attemptsSoFar >= this.maxAttempts;
  }

  nextAttemptAt(attemptsSoFar: number, now: Date): Date {
    const exponential = this.baseDelayMs * 2 ** Math.min(Math.max(attemptsSoFar - 1, 0), 30);
    const capped = Math.min(exponential, this.maxDelayMs);
    const jittered = capped / 2 + this.random() * (capped / 2);
    return new Date(now.getTime() + Math.round(jittered));
  }
}
