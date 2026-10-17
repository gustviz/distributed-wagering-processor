import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { Money } from '../../src/domain/money/money';
import { type Core, command, createCore, newWallet, uniq } from '../support/core';
import { assertLedgerInvariant, one, q } from '../support/database';

/**
 * Real parallelism against PostgreSQL: every call below runs on its own pooled
 * connection, in its own SQL transaction, at the same time. No mocks, no
 * sequential fakes.
 */
let core: Core;

beforeAll(async () => {
  core = await createCore({ poolMax: 40 });
});
afterAll(async () => {
  await core.close();
});

const debitsOf = async (walletId: string) =>
  (await one<{ n: number }>("SELECT count(*)::int AS n FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'", [walletId])).n;

describe('concurrency', () => {
  test('1. the same BET sent 50 times in parallel → exactly one debit', async () => {
    const w = await newWallet(core, '100.00');
    const bet = command(w, 'BET', uniq('bet'), '10.00');
    const results = await Promise.allSettled(Array.from({ length: 50 }, () => core.submit.execute(bet)));

    const fulfilled = results.filter((r) => r.status === 'fulfilled').map((r) => r.value);
    expect(fulfilled).toHaveLength(50);
    expect(new Set(fulfilled.map((r) => r.transactionId)).size).toBe(1);
    expect(fulfilled.filter((r) => !r.idempotentReplay)).toHaveLength(1);
    expect(fulfilled.every((r) => r.status === 'PROCESSED' && r.balance?.amount === '90.00')).toBe(true);
    expect(await debitsOf(w.walletId)).toBe(1);
    expect((await assertLedgerInvariant(w.walletId)).balance).toBe('90.00');
  });

  test('2. section 8 scenario — 100.00 vs two simultaneous 80.00 BETs (repeated 25×)', async () => {
    for (let round = 0; round < 25; round++) {
      const w = await newWallet(core, '100.00');
      const betA = command(w, 'BET', uniq('bet-a'), '80.00');
      const betB = command(w, 'BET', uniq('bet-b'), '80.00');
      const [a, b] = await Promise.all([core.submit.execute(betA), core.submit.execute(betB)]);
      const statuses = [a.status as string, b.status as string].sort();
      expect(statuses).toEqual(['PROCESSED', 'REJECTED']);
      expect([a, b].find((r) => (r.status as string) === 'REJECTED')?.failureCode as string).toBe('INSUFFICIENT_FUNDS');
      expect(await debitsOf(w.walletId)).toBe(1);
      expect((await assertLedgerInvariant(w.walletId)).balance).toBe('20.00');

      // Providers retrying both, concurrently, get the original outcomes and never a second debit.
      const retries = await Promise.all([betA, betB, betA, betB, betA, betB].map((c) => core.submit.execute(c)));
      expect(retries.every((r) => r.idempotentReplay)).toBe(true);
      expect(retries.map((r) => r.status as string)).toEqual([a, b, a, b, a, b].map((r) => r.status as string));
      expect(await debitsOf(w.walletId)).toBe(1);
      expect((await assertLedgerInvariant(w.walletId)).balance).toBe('20.00');
    }
  });

  test('2b. hot wallet: 200 mixed concurrent operations never overdraw and always reconcile', async () => {
    const w = await newWallet(core, '50.00');
    const ops = Array.from({ length: 200 }, (_, i) => {
      const kind = i % 4 === 0 ? 'WIN' : 'BET';
      return command(w, kind, uniq(kind), kind === 'WIN' ? '3.00' : '7.00');
    });
    const results = await Promise.all(ops.map((op) => core.submit.execute(op)));
    const processedBets = results.filter((r, i) => ops[i]!.kind === 'BET' && r.status === 'PROCESSED').length;
    const wins = ops.filter((o) => o.kind === 'WIN').length;
    // Exact expected balance: 50.00 + wins × 3.00 − processed bets × 7.00, in Money (no floats).
    const brl = (amount: string) => Money.from({ amount, currency: 'BRL' });
    let expected = brl('50.00');
    for (let i = 0; i < wins; i++) expected = expected.add(brl('3.00'));
    for (let i = 0; i < processedBets; i++) expected = expected.subtract(brl('7.00'));
    const { balance, entries } = await assertLedgerInvariant(w.walletId); // also asserts non-negative
    expect(balance).toBe(expected.amount);
    expect(entries).toBe(1 + wins + processedBets);
    const versions = await q<{ wallet_version: number }>(
      'SELECT wallet_version FROM wallet_ledger_entries WHERE wallet_id = $1 ORDER BY wallet_version',
      [w.walletId],
    );
    expect(versions.map((v) => v.wallet_version)).toEqual(Array.from({ length: entries }, (_, i) => i + 1));
  });

  test('3. different wallets are processed in parallel (no global lock) and each stays consistent', async () => {
    const wallets = await Promise.all(Array.from({ length: 20 }, () => newWallet(core, '100.00')));
    const started = performance.now();
    await Promise.all(
      wallets.flatMap((w) => Array.from({ length: 10 }, () => core.submit.execute(command(w, 'BET', uniq('bet'), '5.00')))),
    );
    const elapsed = performance.now() - started;
    for (const w of wallets) expect((await assertLedgerInvariant(w.walletId)).balance).toBe('50.00');
    // 200 transactions over 20 independent wallets: if a global lock serialized them this would
    // be ~20x slower than one wallet's chain. Logged, not asserted (machine dependent).
    console.info(`[concurrency] 200 tx across 20 wallets in ${elapsed.toFixed(0)} ms`);
  });

  test('7. ROLLBACK and REFUND that arrive before their BET (concurrently with it) are applied exactly once', async () => {
    const w = await newWallet(core, '100.00');
    const bet1 = uniq('bet');
    const bet2 = uniq('bet');
    const reversals = [
      command(w, 'ROLLBACK', uniq('rb'), '10.00', { reference: bet1 }),
      command(w, 'REFUND', uniq('rf'), '20.00', { reference: bet2 }),
    ];
    const early = await Promise.all(reversals.map((r) => core.submit.execute(r)));
    expect(early.map((r) => r.status as string)).toEqual(['PENDING_REFERENCE', 'PENDING_REFERENCE']);

    await Promise.all([
      core.submit.execute(command(w, 'BET', bet1, '10.00')),
      core.submit.execute(command(w, 'BET', bet2, '20.00')),
      // three workers racing on the same pending rows
      core.resolver.runOnce({ batchSize: 50, leaseMs: 1_000 }),
      core.resolver.runOnce({ batchSize: 50, leaseMs: 1_000 }),
      core.resolver.runOnce({ batchSize: 50, leaseMs: 1_000 }),
    ]);
    for (let i = 0; i < 50; i++) {
      const pending = await q("SELECT 1 FROM wager_transactions WHERE wallet_id = $1 AND status = 'PENDING_REFERENCE'", [w.walletId]);
      if (pending.length === 0) break;
      await Promise.all([core.resolver.runOnce({ batchSize: 50, leaseMs: 1_000 }), core.resolver.runOnce({ batchSize: 50, leaseMs: 1_000 })]);
      await Bun.sleep(100);
    }
    const final = await Promise.all(reversals.map((r) => core.submit.execute(r)));
    expect(final.map((r) => r.status as string)).toEqual(['PROCESSED', 'PROCESSED']);
    expect((await assertLedgerInvariant(w.walletId)).balance).toBe('100.00');
  });
});
