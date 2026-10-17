/**
 * Application-level errors. Like domain errors they carry a stable `code`;
 * the interface layer maps them to HTTP statuses / SQS dispositions.
 */
export abstract class ApplicationError extends Error {
  abstract readonly code: string;

  protected constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

/** Same Idempotency-Key, different business payload. Never a replay. */
export class IdempotencyConflictError extends ApplicationError {
  readonly code = 'IDEMPOTENCY_KEY_CONFLICT';
  constructor(public readonly idempotencyKey: string) {
    super('Idempotency-Key was already used with a different payload');
  }
}

/** The (providerId, externalTransactionId) pair is already taken by another idempotency key. */
export class ExternalTransactionConflictError extends ApplicationError {
  readonly code = 'EXTERNAL_TRANSACTION_CONFLICT';
  constructor() {
    super('externalTransactionId was already submitted under a different Idempotency-Key');
  }
}

/** Same inbox messageId, different body: a producer bug, not a redelivery. */
export class InboxPayloadConflictError extends ApplicationError {
  readonly code = 'INBOX_PAYLOAD_CONFLICT';
  constructor(public readonly messageId: string) {
    super('Message id was already consumed with a different payload');
  }
}

export class WalletNotFoundError extends ApplicationError {
  readonly code = 'WALLET_NOT_FOUND';
  constructor(public readonly walletId: string) {
    super(`Wallet ${walletId} not found`);
  }
}

export class WalletAlreadyExistsError extends ApplicationError {
  readonly code = 'WALLET_ALREADY_EXISTS';
  constructor() {
    super('A wallet already exists for this player and currency');
  }
}

export class TransactionNotFoundError extends ApplicationError {
  readonly code = 'TRANSACTION_NOT_FOUND';
  constructor() {
    super('Transaction not found');
  }
}

export class InvalidCursorError extends ApplicationError {
  readonly code = 'INVALID_CURSOR';
  constructor() {
    super('Invalid pagination cursor');
  }
}

/**
 * A temporary infrastructure condition (database/queue unreachable, lock
 * timeout, deadlock, serialization failure). Safe to retry.
 * `contention` marks the subset caused by concurrent access to the same rows.
 */
export class TransientInfrastructureError extends ApplicationError {
  readonly code = 'TRANSIENT_FAILURE';
  constructor(
    message: string,
    public readonly contention = false,
    options?: { cause?: unknown },
  ) {
    super(message);
    if (options?.cause !== undefined) (this as { cause?: unknown }).cause = options.cause;
  }
}

/** Optimistic guard (`WHERE version = ?`) did not match. Retried by the use case. */
export class ConcurrencyConflictError extends TransientInfrastructureError {
  constructor(entity: string, id: string) {
    super(`Concurrent modification of ${entity} ${id}`, true);
  }
}

/** A unique constraint fired. `constraint` is the database constraint/index name. */
export class UniqueViolationError extends ApplicationError {
  readonly code = 'UNIQUE_VIOLATION';
  constructor(
    public readonly constraint: string,
    options?: { cause?: unknown },
  ) {
    super(`Unique constraint violated: ${constraint}`);
    if (options?.cause !== undefined) (this as { cause?: unknown }).cause = options.cause;
  }
}
