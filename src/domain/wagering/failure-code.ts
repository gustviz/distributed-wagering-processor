/**
 * Stable, machine-readable reasons for a terminal REJECTED / FAILED transaction.
 *
 * Each code tells the provider what to do next (see ARCHITECTURE.md §Failure codes):
 * - `resend`  — never: the decision is final for this idempotency key;
 * - `fix`     — submit a corrected operation under a NEW external id / idempotency key;
 * - `give up` — the operation can never succeed.
 */
export enum FailureCode {
  /** BET larger than the available balance. Fix: none — the player cannot afford it. */
  InsufficientFunds = 'INSUFFICIENT_FUNDS',
  /**
   * A reversal (ROLLBACK of a WIN/REFUND) would drive the balance negative.
   * Operationally different from a BET without funds: money was already paid
   * out and must be recovered out-of-band. Kept auditable.
   */
  ReversalInsufficientFunds = 'REVERSAL_INSUFFICIENT_FUNDS',
  /** Operation currency differs from the wallet currency. */
  CurrencyMismatch = 'CURRENCY_MISMATCH',
  /** `playerId` does not own the given wallet. */
  PlayerWalletMismatch = 'PLAYER_WALLET_MISMATCH',
  /** The referenced transaction never arrived within the retry budget. */
  ReferenceNotFound = 'REFERENCE_NOT_FOUND',
  /** Reference belongs to another player, wallet, currency or round. */
  ReferenceMismatch = 'REFERENCE_MISMATCH',
  /** Reference kind not allowed (REFUND→BET only; ROLLBACK→BET|WIN|REFUND; WIN/LOSS→BET). */
  ReferenceKindNotAllowed = 'REFERENCE_KIND_NOT_ALLOWED',
  /** Reference exists but is REJECTED/FAILED, or never left a pending state in time. */
  ReferenceNotProcessed = 'REFERENCE_NOT_PROCESSED',
  /** Reference was already reversed (by a REFUND or a ROLLBACK). */
  ReferenceAlreadyReversed = 'REFERENCE_ALREADY_REVERSED',
  /** Reversal amount differs from the reference amount (partial reversal is out of scope). */
  ReversalAmountMismatch = 'REVERSAL_AMOUNT_MISMATCH',
  /** Permanent, non-business failure while processing (terminal, auditable). */
  UnexpectedError = 'UNEXPECTED_ERROR',
}

export const FAILURE_CODES: readonly FailureCode[] = Object.values(FailureCode);
