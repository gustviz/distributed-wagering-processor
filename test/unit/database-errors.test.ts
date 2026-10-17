import { describe, expect, test } from 'bun:test';
import { TransientInfrastructureError, UniqueViolationError, WalletNotFoundError } from '../../src/application/errors';
import { translateDatabaseError } from '../../src/infrastructure/persistence/database-errors';
import { classifyError } from '../../src/interface/error-classification';

const dbError = (message: string, props: Record<string, unknown> = {}) => Object.assign(new Error(message), props);

describe('database error translation', () => {
  test.each([
    ['pg connect timeout', dbError('timeout expired')],
    ['knex pool exhausted', Object.assign(dbError('Knex: Timeout acquiring a connection. The pool is probably full.'), { name: 'KnexTimeoutError' })],
    ['connection refused', dbError('connect ECONNREFUSED 10.0.0.2:5432', { code: 'ECONNREFUSED' })],
    ['host not found', dbError('getaddrinfo ENOTFOUND postgres', { code: 'ENOTFOUND' })],
    ['server shutting down', dbError('terminating connection due to administrator command', { code: '57P01' })],
    ['socket dropped', dbError('Connection terminated unexpectedly')],
  ])('%s → transient (503 / SQS retry), not contention', (_, error) => {
    const translated = translateDatabaseError(error);
    expect(translated).toBeInstanceOf(TransientInfrastructureError);
    expect((translated as TransientInfrastructureError).contention).toBe(false);
    expect(classifyError(translated).category).toBe('transient');
  });

  test.each(['40001', '40P01', '55P03'])('SQLSTATE %s → retryable contention', (code) => {
    const translated = translateDatabaseError(dbError('contention', { code }));
    expect((translated as TransientInfrastructureError).contention).toBe(true);
  });

  test('unique violation keeps the constraint name', () => {
    const translated = translateDatabaseError(dbError('dup', { code: '23505', constraint: 'uq_wallets_player_currency' }));
    expect(translated).toBeInstanceOf(UniqueViolationError);
    expect((translated as UniqueViolationError).constraint).toBe('uq_wallets_player_currency');
  });

  test('integrity violations and application errors pass through untouched', () => {
    const integrity = dbError('ledger chain broken', { code: '23000' });
    expect(translateDatabaseError(integrity)).toBe(integrity);
    expect(classifyError(integrity).category).toBe('internal');
    const notFound = new WalletNotFoundError('w');
    expect(translateDatabaseError(notFound)).toBe(notFound);
  });
});
