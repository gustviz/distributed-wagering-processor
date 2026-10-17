import pg from 'pg';
import { Money } from '../../src/domain/money/money';
import { runMigrations } from '../../src/infrastructure/persistence/migrate';
import { TEST_DATABASE_URL } from './env';

let prepared: Promise<void> | undefined;
let pool: pg.Pool | undefined;

function withDatabase(url: string, database: string): string {
  const u = new URL(url);
  u.pathname = `/${database}`;
  return u.toString();
}

/** Drops and recreates a database whose name contains "test" (guard against accidents). */
export async function recreateDatabase(url: string): Promise<void> {
  const name = new URL(url).pathname.slice(1);
  if (!/test/i.test(name)) throw new Error(`Refusing to recreate non-test database "${name}"`);
  const admin = new pg.Client({ connectionString: withDatabase(url, 'postgres') });
  await admin.connect();
  try {
    await admin.query(`DROP DATABASE IF EXISTS "${name}" WITH (FORCE)`);
    await admin.query(`CREATE DATABASE "${name}"`);
  } finally {
    await admin.end();
  }
}

/** Fresh schema once per test process (all suites share it and use unique data). */
export function prepareDatabase(): Promise<void> {
  prepared ??= (async () => {
    await recreateDatabase(TEST_DATABASE_URL);
    await runMigrations(TEST_DATABASE_URL, 'up');
  })();
  return prepared;
}

export function testPool(): pg.Pool {
  pool ??= new pg.Pool({ connectionString: TEST_DATABASE_URL, max: 10 });
  return pool;
}

export async function q<T = Record<string, unknown>>(sql: string, params: unknown[] = []): Promise<T[]> {
  return (await testPool().query(sql, params)).rows as T[];
}

export async function one<T = Record<string, unknown>>(sql: string, params: unknown[] = []): Promise<T> {
  const rows = await q<T>(sql, params);
  if (rows.length !== 1) throw new Error(`Expected exactly one row, got ${rows.length}`);
  return rows[0]!;
}

/** Runs statements in one SQL transaction on a dedicated client (for constraint tests). */
export async function inTransaction<T>(fn: (client: pg.PoolClient) => Promise<T>): Promise<T> {
  const client = await testPool().connect();
  try {
    await client.query('BEGIN');
    const result = await fn(client);
    await client.query('COMMIT');
    return result;
  } catch (error) {
    await client.query('ROLLBACK').catch(() => undefined);
    throw error;
  } finally {
    client.release();
  }
}

/**
 * The invariant every test ends with. All money comparisons are exact (`Money`,
 * bigint minor units), never through JS numbers.
 *
 *  - versions are consecutive: 1, 2, 3, … starting at the OPENING entry, or
 *    2, 3, … for a wallet opened empty (version 1 has no entry), ending at the
 *    wallet's current version;
 *  - each entry chains on the previous one (balance_before = previous balance_after),
 *    and the running sum of signed amounts equals every balance_after;
 *  - the materialized balance equals the balance rebuilt from the ledger;
 *  - nothing is negative.
 */
export async function assertLedgerInvariant(walletId: string): Promise<{ balance: string; entries: number }> {
  const wallet = await one<{ balance: string; currency: string; version: number }>(
    'SELECT balance::text, currency, version FROM wallets WHERE id = $1',
    [walletId],
  );
  const entries = await q<{
    wallet_version: number;
    kind: string;
    direction: string;
    amount: string;
    balance_before: string;
    balance_after: string;
  }>(
    `SELECT e.wallet_version, t.kind, e.direction, e.amount::text, e.balance_before::text, e.balance_after::text
       FROM wallet_ledger_entries e
       JOIN wager_transactions t ON t.id = e.transaction_id
      WHERE e.wallet_id = $1
      ORDER BY e.wallet_version`,
    [walletId],
  );
  const money = (amount: string) => Money.fromSigned({ amount, currency: wallet.currency });
  const balance = money(wallet.balance);
  const problems: string[] = [];
  if (balance.isNegative()) problems.push(`negative balance ${balance}`);

  let rebuilt = Money.zero(wallet.currency);
  let expectedVersion = entries[0]?.kind === 'OPENING' ? 1 : 2;
  for (const e of entries) {
    if (e.wallet_version !== expectedVersion) {
      problems.push(`version ${e.wallet_version} where ${expectedVersion} was expected`);
    }
    if (!money(e.balance_before).equals(rebuilt)) {
      problems.push(`chain broken at v${e.wallet_version}: before ${e.balance_before}, expected ${rebuilt.amount}`);
    }
    const amount = money(e.amount);
    rebuilt = rebuilt.add(e.direction === 'CREDIT' ? amount : amount.negate());
    const after = money(e.balance_after);
    if (!after.equals(rebuilt)) problems.push(`v${e.wallet_version} balance_after ${after.amount} != running ${rebuilt.amount}`);
    if (after.isNegative()) problems.push(`negative balance_after at v${e.wallet_version}`);
    expectedVersion = e.wallet_version + 1;
  }

  const last = entries.at(-1);
  if (!last && !(wallet.version === 1 && balance.isZero())) {
    problems.push(`wallet v${wallet.version} with balance ${balance.amount} has no ledger entries`);
  }
  if (last && last.wallet_version !== wallet.version) {
    problems.push(`wallet v${wallet.version} not backed by last entry v${last.wallet_version}`);
  }
  if (!rebuilt.equals(balance)) problems.push(`balance ${balance.amount} != ledger ${rebuilt.amount}`);

  if (problems.length) throw new Error(`Ledger invariant violated for ${walletId}: ${problems.join('; ')}`);
  return { balance: balance.amount, entries: entries.length };
}
