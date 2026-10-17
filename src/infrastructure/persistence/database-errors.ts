import { ConnectionException } from '@mikro-orm/core';
import { ApplicationError, TransientInfrastructureError, UniqueViolationError } from '../../application/errors';
import { DomainError } from '../../domain/shared/domain-error';

/** SQLSTATEs caused by concurrent access to the same rows. Retrying is the right answer. */
const CONTENTION_CODES = new Set([
  '40001', // serialization_failure
  '40P01', // deadlock_detected
  '55P03', // lock_not_available (lock_timeout)
]);

/** SQLSTATEs / socket errors meaning "database temporarily unavailable". */
const UNAVAILABLE_CODES = new Set([
  '57P01', // admin_shutdown
  '57P02', // crash_shutdown
  '57P03', // cannot_connect_now
  '53300', // too_many_connections
  '53400', // configuration_limit_exceeded
  '57014', // query_canceled (statement_timeout)
  'ECONNREFUSED',
  'ECONNRESET',
  'ETIMEDOUT',
  'EPIPE',
  'ENOTFOUND',
  'EAI_AGAIN',
]);

/** Code-less driver errors: pg's connect timeout ("timeout expired"), knex pool exhaustion, dropped sockets. */
const UNAVAILABLE_MESSAGE =
  /^timeout expired$|Connection terminated|Timeout acquiring a connection|connection.*(closed|ended)|Client has encountered a connection error/i;

/**
 * Maps driver errors to application errors. Anything unknown (including
 * integrity violations raised by our triggers) is returned untouched and
 * surfaces as an internal error: it indicates a bug, not a retryable condition.
 */
export function translateDatabaseError(error: unknown): unknown {
  if (error instanceof ApplicationError || error instanceof DomainError) return error;
  if (!(error instanceof Error)) return error;

  const { code, constraint } = error as Error & { code?: unknown; constraint?: unknown };
  if (code === '23505') {
    return new UniqueViolationError(typeof constraint === 'string' ? constraint : 'unknown', { cause: error });
  }
  if (typeof code === 'string' && CONTENTION_CODES.has(code)) {
    return new TransientInfrastructureError(`Database contention (${code})`, true, { cause: error });
  }
  if (
    (typeof code === 'string' && (code.startsWith('08') || UNAVAILABLE_CODES.has(code))) ||
    error instanceof ConnectionException ||
    error.name === 'KnexTimeoutError' ||
    UNAVAILABLE_MESSAGE.test(error.message)
  ) {
    return new TransientInfrastructureError('Database unavailable', false, { cause: error });
  }
  return error;
}
