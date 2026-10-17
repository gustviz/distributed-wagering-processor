import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { EventPublisher, FaultInjector, TransactionScope, UnitOfWork } from '../../src/application/ports';
import { type Core, command, createCore, newWallet } from '../support/core';
import { assertLedgerInvariant, one, q } from '../support/database';

/**
 * Atomicity between wallet, ledger, inbox and outbox, inbox redelivery, and
 * the transactional outbox under concurrent publishers — all on real PostgreSQL.
 */
let core: Core;
let failOutboxWrites = false;

/** Makes `outbox.add` fail AFTER the wallet, transaction, ledger and inbox writes of the same SQL transaction. */
function sabotageOutbox(uow: UnitOfWork): UnitOfWork {
  return {
    transactional: (work, options) =>
      uow.transactional((scope) => {
        if (!failOutboxWrites) return work(scope);
        const sabotaged: TransactionScope = {
          ...scope,
          wallets: scope.wallets,
          transactions: scope.transactions,
          ledger: scope.ledger,
          inbox: scope.inbox,
          outbox: { ...scope.outbox, add: async () => Promise.reject(new Error('disk full (simulated)')) },
        };
        return work(sabotaged);
      }, options),
  };
}

beforeAll(async () => {
  core = await createCore({ wrapUnitOfWork: sabotageOutbox });
});
afterAll(async () => {
  failOutboxWrites = false;
  await core.close();
});

async function snapshot(walletId: string) {
  return one<{ balance: string; version: number; txs: number; entries: number; events: number }>(
    `SELECT w.balance::text, w.version,
            (SELECT count(*)::int FROM wager_transactions t WHERE t.wallet_id = w.id) AS txs,
            (SELECT count(*)::int FROM wallet_ledger_entries e WHERE e.wallet_id = w.id) AS entries,
            (SELECT count(*)::int FROM outbox_messages o WHERE o.aggregate_id = w.id::text) AS events
       FROM wallets w WHERE w.id = $1`,
    [walletId],
  );
}

describe('atomicity: wallet + transaction + ledger + inbox + outbox', () => {
  test('a failure at the last write (outbox) rolls back every other write', async () => {
    const w = await newWallet(core, '100.00');
    const before = await snapshot(w.walletId);
    failOutboxWrites = true;
    const messageId = `msg-${w.playerId}`;
    let error: unknown;
    try {
      await core.submit.execute(
        command(w, 'BET', `bet-${w.playerId}`, '30.00', {
          source: 'sqs',
          inbox: { consumerName: 'atomicity', messageId, payloadHash: 'h' },
        }),
      );
    } catch (e) {
      error = e;
    } finally {
      failOutboxWrites = false;
    }
    expect((error as Error).message).toContain('disk full');
    expect(await snapshot(w.walletId)).toEqual(before);
    expect(await q('SELECT 1 FROM inbox_messages WHERE message_id = $1', [messageId])).toHaveLength(0);

    // The same message succeeds afterwards and is applied exactly once.
    const ok = await core.submit.execute(
      command(w, 'BET', `bet-${w.playerId}`, '30.00', {
        source: 'sqs',
        inbox: { consumerName: 'atomicity', messageId, payloadHash: 'h' },
      }),
    );
    expect(ok.status as string).toBe('PROCESSED');
    const after = await snapshot(w.walletId);
    expect(after).toMatchObject({ balance: '70.00', version: 2, txs: 2, entries: 2, events: 4 });
    await assertLedgerInvariant(w.walletId);
  });

  test('a crash right before COMMIT leaves no trace', async () => {
    const crashing: FaultInjector = {
      checkpoint: async (point) => {
        if (point === 'submit.before-commit') throw new Error('process died (simulated)');
      },
    };
    const crashingCore = await createCore({ faults: crashing, poolMax: 2 });
    try {
      const w = await newWallet(core, '100.00');
      const before = await snapshot(w.walletId);
      let error: unknown;
      try {
        await crashingCore.submit.execute(command(w, 'BET', `bet-${w.playerId}`, '10.00'));
      } catch (e) {
        error = e;
      }
      expect((error as Error).message).toContain('process died');
      expect(await snapshot(w.walletId)).toEqual(before);
    } finally {
      await crashingCore.close();
    }
  });
});

describe('inbox and redelivery', () => {
  test('the same message delivered 20 times concurrently is applied once', async () => {
    const w = await newWallet(core, '100.00');
    const inbox = { consumerName: 'redelivery', messageId: `msg-${w.playerId}`, payloadHash: 'same' };
    const results = await Promise.all(
      Array.from({ length: 20 }, () =>
        core.submit.execute(command(w, 'BET', `bet-${w.playerId}`, '10.00', { source: 'sqs', inbox })),
      ),
    );
    expect(new Set(results.map((r) => r.transactionId)).size).toBe(1);
    expect(results.filter((r) => !r.idempotentReplay)).toHaveLength(1);
    const inboxRow = await one<{ processed_at: Date | null }>(
      'SELECT processed_at FROM inbox_messages WHERE consumer_name = $1 AND message_id = $2',
      [inbox.consumerName, inbox.messageId],
    );
    expect(inboxRow.processed_at).not.toBeNull();
    expect((await assertLedgerInvariant(w.walletId)).balance).toBe('90.00');
  });

  test('a different message id carrying an already-applied operation is a replay (idempotency key wins)', async () => {
    const w = await newWallet(core, '100.00');
    const ext = `bet-${w.playerId}`;
    const a = await core.submit.execute(
      command(w, 'BET', ext, '10.00', { source: 'sqs', inbox: { consumerName: 'c', messageId: `${ext}-1`, payloadHash: 'h' } }),
    );
    const b = await core.submit.execute(
      command(w, 'BET', ext, '10.00', { source: 'sqs', inbox: { consumerName: 'c', messageId: `${ext}-2`, payloadHash: 'h' } }),
    );
    expect(b).toEqual({ ...a, idempotentReplay: true });
    expect((await assertLedgerInvariant(w.walletId)).balance).toBe('90.00');
  });
});

describe('transactional outbox', () => {
  function recordingPublisher(delayMs = 5) {
    const counts = new Map<string, number>();
    const publisher: EventPublisher = {
      publish: async (messages) => {
        await Bun.sleep(delayMs); // widen the race window
        for (const m of messages) counts.set(m.id, (counts.get(m.id) ?? 0) + 1);
        return messages.map((m) => ({ id: m.id, ok: true }));
      },
    };
    return { publisher, counts };
  }

  async function drainWith(relays: Core[]): Promise<void> {
    await Promise.all(
      relays.map(async (relay) => {
        while ((await relay.relay.runOnce(10)) > 0) {
          // keep draining
        }
      }),
    );
  }

  test('four concurrent publishers publish every event exactly once (FOR UPDATE SKIP LOCKED)', async () => {
    const wallets = await Promise.all(Array.from({ length: 30 }, () => newWallet(core, '10.00')));
    const ids = (
      await q<{ id: string }>('SELECT id FROM outbox_messages WHERE aggregate_id = ANY($1::text[])', [
        wallets.map((w) => w.walletId),
      ])
    ).map((r) => r.id);
    expect(ids).toHaveLength(60);

    const { publisher, counts } = recordingPublisher();
    const relays = await Promise.all(Array.from({ length: 4 }, () => createCore({ publisher, poolMax: 3 })));
    try {
      await drainWith(relays);
    } finally {
      await Promise.all(relays.map((r) => r.close()));
    }
    for (const id of ids) expect(counts.get(id)).toBe(1);
    const pending = await q('SELECT 1 FROM outbox_messages WHERE id = ANY($1::uuid[]) AND published_at IS NULL', [ids]);
    expect(pending).toHaveLength(0);
  });

  test('crash after publishing but before commit: another relay republishes the SAME event ids (at-least-once)', async () => {
    const w = await newWallet(core, '10.00');
    const ids = (await q<{ id: string }>('SELECT id FROM outbox_messages WHERE aggregate_id = $1', [w.walletId])).map(
      (r) => r.id,
    );
    const { publisher, counts } = recordingPublisher(0);
    let crashed = false;
    const dying = await createCore({
      publisher,
      poolMax: 2,
      faults: {
        checkpoint: async (point) => {
          if (point === 'outbox.after-publish-before-commit' && !crashed) {
            crashed = true;
            throw new Error('killed (simulated)');
          }
        },
      },
    });
    const survivor = await createCore({ publisher, poolMax: 2 });
    try {
      // The dying relay may pick other suites' rows first; loop until it has crashed on ours.
      while (!crashed) await dying.relay.runOnce(500).catch(() => undefined);
      const stillPending = await q('SELECT 1 FROM outbox_messages WHERE id = ANY($1::uuid[]) AND published_at IS NULL', [ids]);
      expect(stillPending.length).toBeGreaterThan(0); // nothing was marked published
      await drainWith([survivor]);
    } finally {
      await Promise.all([dying.close(), survivor.close()]);
    }
    for (const id of ids) expect(counts.get(id)).toBeGreaterThanOrEqual(1);
    expect([...ids].some((id) => (counts.get(id) ?? 0) >= 2)).toBe(true); // republished with the same eventId
    expect(await q('SELECT 1 FROM outbox_messages WHERE id = ANY($1::uuid[]) AND published_at IS NULL', [ids])).toHaveLength(0);
  });

  test('publish failures back off and are retried until published', async () => {
    const w = await newWallet(core, '10.00');
    const ids = (await q<{ id: string }>('SELECT id FROM outbox_messages WHERE aggregate_id = $1', [w.walletId])).map(
      (r) => r.id,
    );
    let failuresLeft = 2;
    const flaky = await createCore({
      poolMax: 2,
      publisher: {
        publish: async (messages) =>
          messages.map((m) =>
            ids.includes(m.id) && failuresLeft-- > 0 ? { id: m.id, ok: false, error: 'throttled' } : { id: m.id, ok: true },
          ),
      },
    });
    try {
      for (let i = 0; i < 50; i++) {
        await flaky.relay.runOnce(500);
        const left = await q('SELECT 1 FROM outbox_messages WHERE id = ANY($1::uuid[]) AND published_at IS NULL', [ids]);
        if (left.length === 0) break;
        await Bun.sleep(60);
      }
    } finally {
      await flaky.close();
    }
    const rows = await q<{ attempts: number; published_at: Date | null }>(
      'SELECT attempts, published_at FROM outbox_messages WHERE id = ANY($1::uuid[])',
      [ids],
    );
    expect(rows.every((r) => r.published_at !== null)).toBe(true);
    expect(rows.reduce((n, r) => n + r.attempts, 0)).toBe(2);
  });
});
