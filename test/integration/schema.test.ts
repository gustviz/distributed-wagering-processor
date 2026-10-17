import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import pg from 'pg';
import { runMigrations } from '../../src/infrastructure/persistence/migrate';
import { type Core, command, createCore, newWallet } from '../support/core';
import { assertLedgerInvariant, inTransaction, one, q, recreateDatabase } from '../support/database';
import { TEST_DATABASE_URL } from '../support/env';

/**
 * The invariants are enforced by the SCHEMA, not only by the code: every test
 * here bypasses the application and attacks the database with raw SQL.
 */
let core: Core;

beforeAll(async () => {
  core = await createCore();
});
afterAll(async () => {
  await core.close();
});

/** Runs `sql` in its own transaction (BEGIN … COMMIT) and asserts the database refused it. */
async function rejects(sql: string, params: unknown[] = [], match?: RegExp): Promise<void> {
  // try/catch rather than `expect(promise).rejects`, which can stall on pg promises under bun test.
  let error: unknown;
  try {
    await inTransaction((c) => c.query(sql, params));
  } catch (e) {
    error = e;
  }
  expect(error).toBeInstanceOf(Error);
  if (match) expect((error as Error).message + String((error as { constraint?: string }).constraint)).toMatch(match);
}

describe('migrations', () => {
  test('are reversible: up → down (all) → up on a scratch database', async () => {
    const url = TEST_DATABASE_URL.replace(/\/([^/]+)$/, '/$1_migrations');
    await recreateDatabase(url);
    expect(await runMigrations(url, 'up')).toHaveLength(3);
    expect(await runMigrations(url, 'down:all')).toHaveLength(3);
    const client = new pg.Client({ connectionString: url });
    await client.connect();
    const tables = await client.query(
      "SELECT count(*)::int AS n FROM information_schema.tables WHERE table_schema = 'public' AND table_name <> 'mikro_orm_migrations'",
    );
    expect(tables.rows[0].n).toBe(0);
    await client.end();
    expect(await runMigrations(url, 'up')).toHaveLength(3);
    expect(await runMigrations(url, 'up')).toHaveLength(0);
  });
});

describe('wallets', () => {
  test('balance can never be negative', async () => {
    const w = await newWallet(core, '0.00');
    await rejects('UPDATE wallets SET balance = -0.01, version = version + 1 WHERE id = $1', [w.walletId], /ck_wallets_balance_non_negative/);
  });

  test('one wallet per (player, currency)', async () => {
    const w = await newWallet(core, '0.00');
    await rejects(
      `INSERT INTO wallets (id, player_id, currency, balance, version, created_at, updated_at)
       VALUES (gen_random_uuid(), $1, 'BRL', 0, 1, now(), now())`,
      [w.playerId],
      /uq_wallets_player_currency/,
    );
  });

  test('a balance change without its ledger entry fails at COMMIT (deferred check)', async () => {
    const w = await newWallet(core, '100.00');
    await rejects('UPDATE wallets SET balance = 200, version = version + 1 WHERE id = $1', [w.walletId], /has no ledger entry/);
  });

  test('version moves +1 if and only if the balance moves', async () => {
    const w = await newWallet(core, '100.00');
    await rejects('UPDATE wallets SET balance = 50 WHERE id = $1', [w.walletId], /without version increment/);
    await rejects('UPDATE wallets SET version = version + 1 WHERE id = $1', [w.walletId], /without balance change/);
  });

  test('identity is immutable and wallets cannot be deleted', async () => {
    const w = await newWallet(core, '0.00');
    await rejects("UPDATE wallets SET currency = 'USD' WHERE id = $1", [w.walletId], /identity is immutable/);
    await rejects('DELETE FROM wallets WHERE id = $1', [w.walletId], /not allowed/);
  });
});

describe('ledger', () => {
  test('entries are append-only: UPDATE, DELETE and TRUNCATE are refused', async () => {
    const w = await newWallet(core, '100.00');
    const entry = await one<{ id: string }>('SELECT id FROM wallet_ledger_entries WHERE wallet_id = $1', [w.walletId]);
    await rejects('UPDATE wallet_ledger_entries SET amount = 1 WHERE id = $1', [entry.id], /append-only/);
    await rejects('DELETE FROM wallet_ledger_entries WHERE id = $1', [entry.id], /append-only/);
    await rejects('TRUNCATE wallet_ledger_entries CASCADE', [], /append-only/);
  });

  test('arithmetic is checked: balance_after = balance_before ± amount', async () => {
    const w = await newWallet(core, '100.00');
    const bet = await core.submit.execute(command(w, 'BET', `bet-${w.playerId}`, '10.00')); // ledger v2: 100 → 90
    // Forged entry that passes the row trigger (processed BET, right direction, chains from 90)
    // but whose arithmetic is wrong: the CHECK constraint is what refuses it.
    await rejects(
      `INSERT INTO wallet_ledger_entries
         (id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, wallet_version, created_at)
       VALUES (gen_random_uuid(), $1, $2, 'DEBIT', 10, 'BRL', 90, 81, 3, now())`,
      [w.walletId, bet.transactionId],
      /ck_ledger_arithmetic/,
    );
  });

  test('an entry must belong to a processed, balance-moving transaction and be applied to the wallet', async () => {
    const w = await newWallet(core, '100.00');
    const loss = await core.submit.execute(command(w, 'LOSS', `loss-${w.playerId}`, '0.00'));
    // LOSS never moves the balance → no ledger entry allowed for it.
    await rejects(
      `INSERT INTO wallet_ledger_entries
         (id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, wallet_version, created_at)
       VALUES (gen_random_uuid(), $1, $2, 'CREDIT', 1, 'BRL', 100, 101, 2, now())`,
      [w.walletId, loss.transactionId],
      /does not match a processed balance-moving transaction/,
    );
  });

  test('at most one entry per (transaction, wallet)', async () => {
    const w = await newWallet(core, '100.00');
    const bet = await core.submit.execute(command(w, 'BET', `bet-${w.playerId}`, '10.00')); // v2: 100 → 90
    // A second entry for the same BET that passes every row-level rule (right direction,
    // next version, chains from 90): only the unique constraint can stop it.
    await rejects(
      `INSERT INTO wallet_ledger_entries
         (id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, wallet_version, created_at)
       VALUES (gen_random_uuid(), $1, $2, 'DEBIT', 10, 'BRL', 90, 80, 3, now())`,
      [w.walletId, bet.transactionId],
      /uq_ledger_transaction_wallet/,
    );
  });
});

describe('ledger version continuity', () => {
  /** Inserts a PROCESSED transaction row directly (the application is bypassed on purpose). */
  async function forgeProcessed(c: pg.PoolClient, w: { walletId: string; playerId: string }, kind: string, amount: string) {
    const opening = kind === 'OPENING';
    const row = await c.query<{ id: string }>(
      `INSERT INTO wager_transactions
         (id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id, round_id, game_id,
          kind, amount, currency, status, created_at, updated_at, processed_at)
       VALUES (gen_random_uuid(), $1, gen_random_uuid()::text, gen_random_uuid()::text, repeat('a', 64), $2, $3, 'r', 'g',
               $4, $5, 'BRL', 'PROCESSED', now(), now(), now())
       RETURNING id`,
      [opening ? 'internal' : 'forger', w.walletId, w.playerId, kind, amount],
    );
    return row.rows[0]!.id;
  }

  async function refused(work: (c: pg.PoolClient) => Promise<unknown>, match: RegExp): Promise<void> {
    let error: unknown;
    try {
      await inTransaction(work);
    } catch (e) {
      error = e;
    }
    expect(error).toBeInstanceOf(Error);
    expect((error as Error).message).toMatch(match);
  }

  test('a skipped version is refused, even when the wallet is advanced to match it', async () => {
    const w = await newWallet(core, '100.00');
    await core.submit.execute(command(w, 'BET', `bet-${w.playerId}`, '10.00')); // v2: 100 → 90
    // Two balance changes (v3, v4) but a ledger entry only for v4: v3 would have no entry.
    await refused(async (c) => {
      const tx = await forgeProcessed(c, w, 'BET', '5.00');
      await c.query('UPDATE wallets SET balance = 88, version = 3 WHERE id = $1', [w.walletId]);
      await c.query('UPDATE wallets SET balance = 85, version = 4 WHERE id = $1', [w.walletId]);
      await c.query(
        `INSERT INTO wallet_ledger_entries
           (id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, wallet_version, created_at)
         VALUES (gen_random_uuid(), $1, $2, 'DEBIT', 5, 'BRL', 90, 85, 4, now())`,
        [w.walletId, tx],
      );
    }, /ledger version gap for wallet .*: expected 3, got 4/);
    await assertLedgerInvariant(w.walletId);
  });

  test('a wallet opened empty starts its ledger at version 2 — never at 3', async () => {
    const empty = await newWallet(core, '0.00');
    await core.submit.execute(command(empty, 'WIN', `win-${empty.playerId}`, '10.00'));
    const versions = await q<{ wallet_version: number }>(
      'SELECT wallet_version FROM wallet_ledger_entries WHERE wallet_id = $1',
      [empty.walletId],
    );
    expect(versions.map((v) => v.wallet_version)).toEqual([2]);
    await assertLedgerInvariant(empty.walletId);

    const other = await newWallet(core, '0.00');
    await refused(async (c) => {
      const tx = await forgeProcessed(c, other, 'WIN', '10.00');
      await c.query('UPDATE wallets SET balance = 4, version = 2 WHERE id = $1', [other.walletId]);
      await c.query('UPDATE wallets SET balance = 10, version = 3 WHERE id = $1', [other.walletId]);
      await c.query(
        `INSERT INTO wallet_ledger_entries
           (id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, wallet_version, created_at)
         VALUES (gen_random_uuid(), $1, $2, 'CREDIT', 10, 'BRL', 0, 10, 3, now())`,
        [other.walletId, tx],
      );
    }, /first ledger entry of wallet .* must be version 2, got 3/);
  });

  test('only an OPENING entry may use version 1, and an OPENING must be the first entry', async () => {
    const empty = await newWallet(core, '0.00');
    await refused(async (c) => {
      const tx = await forgeProcessed(c, empty, 'WIN', '10.00');
      await c.query(
        `INSERT INTO wallet_ledger_entries
           (id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, wallet_version, created_at)
         VALUES (gen_random_uuid(), $1, $2, 'CREDIT', 10, 'BRL', 0, 10, 1, now())`,
        [empty.walletId, tx],
      );
    }, /must be version 2, got 1/);

    const funded = await newWallet(core, '100.00'); // OPENING already at v1
    await refused(async (c) => {
      const tx = await forgeProcessed(c, funded, 'OPENING', '5.00');
      await c.query('UPDATE wallets SET balance = 105, version = 2 WHERE id = $1', [funded.walletId]);
      await c.query(
        `INSERT INTO wallet_ledger_entries
           (id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, wallet_version, created_at)
         VALUES (gen_random_uuid(), $1, $2, 'CREDIT', 5, 'BRL', 100, 105, 2, now())`,
        [funded.walletId, tx],
      );
    }, /OPENING must be the first ledger entry/);
  });

  test('assertLedgerInvariant itself detects a version gap (triggers bypassed inside one transaction)', async () => {
    const w = await newWallet(core, '0.00');
    // DDL is transactional: no other session ever sees the triggers disabled.
    await inTransaction(async (c) => {
      await c.query('ALTER TABLE wallets DISABLE TRIGGER USER');
      await c.query('ALTER TABLE wallet_ledger_entries DISABLE TRIGGER USER');
      const tx = await forgeProcessed(c, w, 'WIN', '10.00');
      await c.query(
        `INSERT INTO wallet_ledger_entries
           (id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, wallet_version, created_at)
         VALUES (gen_random_uuid(), $1, $2, 'CREDIT', 10, 'BRL', 0, 10, 3, now())`,
        [w.walletId, tx],
      );
      await c.query('UPDATE wallets SET balance = 10, version = 3 WHERE id = $1', [w.walletId]);
      await c.query('ALTER TABLE wallet_ledger_entries ENABLE TRIGGER USER');
      await c.query('ALTER TABLE wallets ENABLE TRIGGER USER');
    });
    let error: unknown;
    try {
      await assertLedgerInvariant(w.walletId);
    } catch (e) {
      error = e;
    }
    expect((error as Error).message).toMatch(/version 3 where 2 was expected/);
  });
});

describe('wager transactions', () => {
  test('idempotency key and (provider, external id) are unique', async () => {
    const w = await newWallet(core, '100.00');
    const ext = `bet-${w.playerId}`;
    await core.submit.execute(command(w, 'BET', ext, '1.00'));
    const copy = `INSERT INTO wager_transactions
      SELECT gen_random_uuid(), provider_id, external_transaction_id || $2, idempotency_key || $3, payload_hash, wallet_id,
             player_id, round_id, game_id, kind, amount, currency, reference_external_transaction_id, NULL,
             'REJECTED', 'INSUFFICIENT_FUNDS', NULL, NULL, 0, NULL, created_at, updated_at, NULL
        FROM wager_transactions WHERE external_transaction_id = $1`;
    await rejects(copy, [ext, '', '-other'], /uq_wager_tx_provider_external/);
    await rejects(copy, [ext, '-other', ''], /uq_wager_tx_idempotency_key/);
  });

  test('terminal transactions are immutable', async () => {
    const w = await newWallet(core, '100.00');
    const r = await core.submit.execute(command(w, 'BET', `bet-${w.playerId}`, '1.00'));
    await rejects("UPDATE wager_transactions SET status = 'REJECTED', failure_code = 'X', processed_at = NULL WHERE id = $1", [r.transactionId], /terminal/);
    await rejects('DELETE FROM wager_transactions WHERE id = $1', [r.transactionId], /not allowed/);
  });

  test('a transaction can be reversed only once (partial unique index)', async () => {
    const w = await newWallet(core, '100.00');
    const bet = `bet-${w.playerId}`;
    await core.submit.execute(command(w, 'BET', bet, '10.00'));
    const refund = await core.submit.execute(command(w, 'REFUND', `refund-${w.playerId}`, '10.00', { reference: bet }));
    expect(refund.status as string).toBe('PROCESSED');
    // Forge a second processed ROLLBACK of the same BET, bypassing the application.
    await rejects(
      `INSERT INTO wager_transactions
        (id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id, round_id, game_id,
         kind, amount, currency, reference_external_transaction_id, reference_transaction_id, status, observed_balance,
         observed_currency, created_at, updated_at, processed_at)
       SELECT gen_random_uuid(), provider_id, 'forged-' || id, 'forged-' || id, payload_hash, wallet_id, player_id,
              round_id, game_id, 'ROLLBACK', amount, currency, $1, reference_transaction_id, 'PROCESSED', 0, 'BRL',
              now(), now(), now()
         FROM wager_transactions WHERE id = $2`,
      [bet, refund.transactionId],
      /uq_wager_tx_single_reversal/,
    );
  });

  test('status and fields stay coherent (failure code iff rejected/failed; reference required for reversals)', async () => {
    const w = await newWallet(core, '100.00');
    const base = `INSERT INTO wager_transactions
        (id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id, round_id, game_id,
         kind, amount, currency, reference_external_transaction_id, status, failure_code, created_at, updated_at, processed_at)
       VALUES (gen_random_uuid(), 'p', gen_random_uuid()::text, gen_random_uuid()::text, repeat('a', 64), $1, $2, 'r', 'g',
               $3, 1, 'BRL', $4, $5, $6, now(), now(), $7)`;
    await rejects(base, [w.walletId, w.playerId, 'REFUND', null, 'REJECTED', 'X', null], /ck_wager_tx_reference_required/);
    await rejects(base, [w.walletId, w.playerId, 'BET', null, 'REJECTED', null, null], /ck_wager_tx_failure_code/);
    await rejects(base, [w.walletId, w.playerId, 'BET', null, 'PROCESSED', null, null], /ck_wager_tx_processed_at/);
    await rejects(base, [w.walletId, w.playerId, 'OPENING', null, 'REJECTED', 'X', null], /ck_wager_tx_opening_internal/);
  });
});

test('wallet + OPENING + ledger + outbox are written atomically on wallet creation', async () => {
  const w = await newWallet(core, '1000.00');
  const rows = await q<{ kind: string; status: string }>('SELECT kind, status FROM wager_transactions WHERE wallet_id = $1', [w.walletId]);
  expect(rows).toEqual([{ kind: 'OPENING', status: 'PROCESSED' }]);
  const events = await q<{ event_type: string }>('SELECT event_type FROM outbox_messages WHERE aggregate_id = $1 ORDER BY seq', [w.walletId]);
  expect(events.map((e) => e.event_type)).toEqual(['WagerTransactionProcessed', 'WalletBalanceChanged']);
});
