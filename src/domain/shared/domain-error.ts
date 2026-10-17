/**
 * Base class for errors raised by the domain model.
 *
 * `code` is a stable, machine-readable identifier. The interface layer maps
 * codes to transport semantics (HTTP status, SQS ack/retry/DLQ); the domain
 * itself knows nothing about transports.
 */
export abstract class DomainError extends Error {
  abstract readonly code: string;

  protected constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

/** A value object received invalid input (contract violation, never persisted). */
export class InvalidMoneyError extends DomainError {
  readonly code = 'INVALID_MONEY';
  constructor(message: string) {
    super(message);
  }
}

export class CurrencyMismatchError extends DomainError {
  readonly code = 'CURRENCY_MISMATCH';
  constructor(expected: string, actual: string) {
    super(`Currency mismatch: expected ${expected}, got ${actual}`);
  }
}

/** Arithmetic left the representable range of the persistence column. */
export class MoneyOverflowError extends DomainError {
  readonly code = 'MONEY_OUT_OF_RANGE';
  constructor() {
    super('Monetary value exceeds the supported range');
  }
}

/** Invariant guard: a debit would make the balance negative. */
export class InsufficientFundsError extends DomainError {
  readonly code = 'INSUFFICIENT_FUNDS';
  constructor(walletId: string) {
    super(`Wallet ${walletId} has insufficient funds`);
  }
}

/** A command is structurally invalid for the domain (e.g. REFUND without reference). */
export class InvalidTransactionError extends DomainError {
  readonly code = 'INVALID_TRANSACTION';
  constructor(message: string) {
    super(message);
  }
}

/**
 * A state transition was attempted from a terminal (or otherwise invalid) state.
 * This is a programming error, not a business path.
 */
export class InvalidTransactionStateError extends DomainError {
  readonly code = 'INVALID_TRANSACTION_STATE';
  constructor(transactionId: string, from: string, to: string) {
    super(`Transaction ${transactionId} cannot transition from ${from} to ${to}`);
  }
}

/** A ledger entry whose arithmetic does not balance. */
export class UnbalancedLedgerEntryError extends DomainError {
  readonly code = 'UNBALANCED_LEDGER_ENTRY';
  constructor(message: string) {
    super(message);
  }
}
