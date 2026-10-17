import {
  ApplicationError,
  ExternalTransactionConflictError,
  IdempotencyConflictError,
  InboxPayloadConflictError,
  InvalidCursorError,
  TransactionNotFoundError,
  TransientInfrastructureError,
  UniqueViolationError,
  WalletAlreadyExistsError,
  WalletNotFoundError,
} from '../application/errors';
import {
  DomainError,
  InvalidMoneyError,
  InvalidTransactionError,
  MoneyOverflowError,
} from '../domain/shared/domain-error';
import { ContractValidationError } from './contracts/wager-contracts';

/**
 * One classification, two transports. Keeping it in one place guarantees the
 * HTTP status and the SQS disposition never disagree about the same error.
 *
 * | category    | HTTP | SQS                         | provider action             |
 * |-------------|------|-----------------------------|-----------------------------|
 * | invalid     | 400  | DLQ (permanent)             | fix the payload             |
 * | conflict    | 409  | DLQ (permanent)             | do not resend this key      |
 * | not_found   | 404  | DLQ (permanent, replayable) | check identifiers           |
 * | transient   | 503  | retry with backoff, then DLQ| resend later, same key      |
 * | internal    | 500  | retry with backoff, then DLQ| resend later / contact us   |
 *
 * Business rejections (REJECTED transactions) are not errors: they are
 * persisted outcomes, returned as 422 over HTTP and acked on SQS. An unknown
 * wallet is different: nothing can be persisted for it, so over SQS it is
 * dead-lettered rather than acked, keeping the message auditable and replayable.
 */
export type ErrorCategory = 'invalid' | 'conflict' | 'not_found' | 'transient' | 'internal';

export interface ClassifiedError {
  category: ErrorCategory;
  code: string;
  message: string;
  retryable: boolean;
  details?: unknown;
}

export function classifyError(error: unknown): ClassifiedError {
  if (error instanceof ContractValidationError) {
    return { category: 'invalid', code: error.code, message: error.message, retryable: false, details: error.issues };
  }
  if (
    error instanceof InvalidMoneyError ||
    error instanceof InvalidTransactionError ||
    error instanceof MoneyOverflowError ||
    error instanceof InvalidCursorError
  ) {
    return { category: 'invalid', code: error.code, message: error.message, retryable: false };
  }
  if (
    error instanceof IdempotencyConflictError ||
    error instanceof ExternalTransactionConflictError ||
    error instanceof InboxPayloadConflictError ||
    error instanceof WalletAlreadyExistsError
  ) {
    return { category: 'conflict', code: error.code, message: error.message, retryable: false };
  }
  if (error instanceof WalletNotFoundError || error instanceof TransactionNotFoundError) {
    return { category: 'not_found', code: error.code, message: error.message, retryable: false };
  }
  if (error instanceof TransientInfrastructureError || error instanceof UniqueViolationError) {
    // A leftover unique violation means a concurrent duplicate won a race we gave up retrying: resend → replay.
    return { category: 'transient', code: 'TRANSIENT_FAILURE', message: 'Temporary failure, retry later', retryable: true };
  }
  if (error instanceof DomainError || error instanceof ApplicationError) {
    return { category: 'internal', code: error.code, message: 'Internal error', retryable: true };
  }
  return { category: 'internal', code: 'INTERNAL_ERROR', message: 'Internal error', retryable: true };
}
