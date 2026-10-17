import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { TransactionScope, UnitOfWork } from '../../src/application/ports';
import { PendingReferencePolicy } from '../../src/application/wagering/pending-reference-policy';
import { type Core, command, createCore, newWallet, uniq } from '../support/core';
import { assertLedgerInvariant, one, q } from '../support/database';

/**
 * Pending-reference worker on real PostgreSQL: what happens to the transactions
 * waiting on a reference when that reference ends in FAILED.
 */
let core: Core;
/** Transaction id whose next `lockById` throws a non-transient error (once). */
let breakLockOf: string | undefined;

function breakOneLock(uow: UnitOfWork): UnitOfWork {
  return {
    transactional: (work, options) =>
      uow.transactional((scope) => {
        const transactions = Object.create(scope.transactions) as TransactionScope['transactions'];
        transactions.lockById = async (id) => {
          if (id === breakLockOf) {
            breakLockOf = undefined;
            throw new Error('corrupted row (simulated)');
          }
          return scope.transactions.lockById(id);
        };
        const sabotaged: TransactionScope = {
          wallets: scope.wallets,
          ledger: scope.ledger,
          inbox: scope.inbox,
          outbox: scope.outbox,
          transactions,
        };
        return work(sabotaged);
      }, options),
  };
}

beforeAll(async () => {
  // Long backoff: a dependent only resolves quickly if something wakes it.
  core = await createCore({ wrapUnitOfWork: breakOneLock, policy: new PendingReferencePolicy(10, 60_000, 60_000) });
});
afterAll(async () => {
  await core.close();
});

describe('pending-reference worker', () => {
  test('a reference that ends FAILED wakes its dependents, which then resolve as REFERENCE_NOT_PROCESSED', async () => {
    const w = await newWallet(core, '100.00');
    const missingBet = uniq('bet');
    const win = command(w, 'WIN', uniq('win'), '5.00', { reference: missingBet });
    const rollback = command(w, 'ROLLBACK', uniq('rb'), '5.00', { reference: win.externalTransactionId });

    const winResult = await core.submit.execute(win);
    expect(winResult.status as string).toBe('PENDING_REFERENCE');
    expect((await core.submit.execute(rollback)).status as string).toBe('PENDING_REFERENCE');

    // The WIN is due now and its resolution hits a permanent error → FAILED.
    await q("UPDATE wager_transactions SET next_attempt_at = now() - interval '1 second' WHERE id = $1", [
      winResult.transactionId,
    ]);
    breakLockOf = winResult.transactionId;
    await core.resolver.runOnce({ batchSize: 10, leaseMs: 60_000 });

    const failed = await one<{ status: string; failure_code: string }>(
      'SELECT status, failure_code FROM wager_transactions WHERE id = $1',
      [winResult.transactionId],
    );
    expect(failed).toEqual({ status: 'FAILED', failure_code: 'UNEXPECTED_ERROR' });

    // Without a wake-up the ROLLBACK would wait out its ~1 minute backoff.
    const dependent = await one<{ due: boolean }>(
      'SELECT next_attempt_at <= now() AS due FROM wager_transactions WHERE external_transaction_id = $1',
      [rollback.externalTransactionId],
    );
    expect(dependent.due).toBe(true);

    await core.resolver.runOnce({ batchSize: 10, leaseMs: 60_000 });
    const final = await core.submit.execute(rollback);
    expect(final).toMatchObject({ status: 'REJECTED', failureCode: 'REFERENCE_NOT_PROCESSED', idempotentReplay: true });
    expect((await assertLedgerInvariant(w.walletId)).balance).toBe('100.00');
  });
});
