import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, test } from 'bun:test';
import { type Core, createCore, uniq, wagerPayload, waitFor } from '../support/core';
import { assertLedgerInvariant, one, q } from '../support/database';
import { type Instance, postJson, startInstance } from '../support/instances';
import { approximateCount, createTestQueues, drain, send, type TestQueues, wagerMessage } from '../support/sqs';

/**
 * Separate OS processes (`bun run src/main.ts`), each with its own connection
 * pool, SQS consumer, outbox relay and pending-reference worker — the same
 * topology as N replicas in production. Crashes are SIGKILLs at precise points.
 */
let core: Core;
const running: Instance[] = [];

beforeAll(async () => {
  core = await createCore({ poolMax: 5 });
});
beforeEach(async () => {
  // Other suites leave thousands of unpublished events behind (their cores run no relay).
  // Park them so each test here measures its own traffic instead of draining that backlog.
  await q("UPDATE outbox_messages SET published_at = now(), last_error = 'parked by test isolation' WHERE published_at IS NULL");
});
afterEach(async () => {
  await Promise.all(running.splice(0).map((i) => i.kill().catch(() => undefined)));
});
afterAll(async () => {
  await core.close();
});

async function start(name: string, env: Record<string, string>, waitReady = true): Promise<Instance> {
  const instance = await startInstance(uniq(name), env, waitReady);
  running.push(instance);
  return instance;
}

async function createWallet(instance: Instance, amount: string) {
  const playerId = uniq('player');
  const r = await postJson(`${instance.url}/wallets`, { playerId, initialBalance: { amount, currency: 'BRL' } });
  expect(r.status).toBe(201);
  return { walletId: r.body.id as string, playerId };
}

async function submitHttp(instance: Instance, payload: ReturnType<typeof wagerPayload>) {
  return postJson(`${instance.url}/wagering/transactions`, payload, {
    'Idempotency-Key': `${payload.providerId}:${payload.externalTransactionId}`,
  });
}

async function quiesced(queues: TestQueues, walletIds: string[]): Promise<boolean> {
  const [pendingRefs, unpublished] = await Promise.all([
    q("SELECT 1 FROM wager_transactions WHERE wallet_id = ANY($1::uuid[]) AND status = 'PENDING_REFERENCE'", [walletIds]),
    q('SELECT 1 FROM outbox_messages WHERE aggregate_id = ANY($1::text[]) AND published_at IS NULL', [walletIds]),
  ]);
  return pendingRefs.length === 0 && unpublished.length === 0 && (await approximateCount(queues, 'transactions')) === 0;
}

describe('multiple instances', () => {
  test('4. three instances process HTTP + SQS traffic for shared hot wallets correctly', async () => {
    const queues = await createTestQueues('multi');
    const instances = await Promise.all([1, 2, 3].map((n) => start(`node${n}`, queues.env)));
    const wallets = await Promise.all(Array.from({ length: 6 }, (_, i) => createWallet(instances[i % 3]!, '200.00')));

    // The same BET sent 50 times, spread across the three instances.
    const hot = wallets[0]!;
    const duplicated = wagerPayload(hot, 'BET', uniq('dup'), '10.00');
    // Out-of-order: ROLLBACKs queued before their BETs.
    const late = wallets.map((w) => ({ w, bet: uniq('late-bet') }));

    await Promise.all([
      ...Array.from({ length: 50 }, (_, i) => submitHttp(instances[i % 3]!, duplicated)),
      ...wallets.flatMap((w, wi) =>
        Array.from({ length: 15 }, (_, i) => submitHttp(instances[(wi + i) % 3]!, wagerPayload(w, 'BET', uniq('bet'), '7.00'))),
      ),
      (async () => {
        for (const { w, bet } of late) {
          await send(queues, wagerMessage(wagerPayload(w, 'ROLLBACK', uniq('rb'), '5.00', { reference: bet })), w.walletId);
        }
        for (const w of wallets) {
          for (let i = 0; i < 10; i++) {
            const msg = wagerMessage(wagerPayload(w, i % 2 ? 'WIN' : 'BET', uniq('sqs'), '3.00'));
            await send(queues, msg, w.walletId);
            if (i % 3 === 0) await send(queues, msg, w.walletId); // redelivery
          }
        }
        for (const { w, bet } of late) {
          await send(queues, wagerMessage(wagerPayload(w, 'BET', bet, '5.00')), w.walletId);
        }
      })(),
    ]);

    const ids = wallets.map((w) => w.walletId);
    await waitFor(() => quiesced(queues, ids), { timeoutMs: 60_000, message: 'all instances quiesced' });

    const dupRows = await q('SELECT 1 FROM wager_transactions WHERE external_transaction_id = $1', [
      duplicated.externalTransactionId,
    ]);
    expect(dupRows).toHaveLength(1);
    for (const w of wallets) {
      await assertLedgerInvariant(w.walletId);
      const rollbacks = await q<{ status: string }>(
        "SELECT status FROM wager_transactions WHERE wallet_id = $1 AND kind = 'ROLLBACK'",
        [w.walletId],
      );
      expect(rollbacks.map((r) => r.status)).toEqual(['PROCESSED']);
      const sqsTx = await one<{ n: number }>(
        "SELECT count(*)::int AS n FROM wager_transactions WHERE wallet_id = $1 AND external_transaction_id LIKE 'sqs-%'",
        [w.walletId],
      );
      expect(sqsTx.n).toBe(10);
    }
    // Every committed event of these wallets reached the events queue.
    const events = await drain(queues, 'events');
    const ourEvents = await q<{ id: string }>('SELECT id FROM outbox_messages WHERE aggregate_id = ANY($1::text[])', [ids]);
    const publishedIds = new Set(events.map((m) => m.MessageAttributes?.eventId?.StringValue));
    for (const e of ourEvents) expect(publishedIds.has(e.id)).toBe(true);
  }, 120_000);

  test('5. worker killed AFTER commit and BEFORE ack: redelivery is absorbed by the inbox', async () => {
    const queues = await createTestQueues('crash-ack');
    const env = { ...queues.env, SQS_VISIBILITY_TIMEOUT_SECONDS: '3' };
    const w = await (async () => {
      const setup = await start('setup', { ...env, CONSUMER_ENABLED: 'false' });
      const created = await createWallet(setup, '100.00');
      await setup.kill();
      return created;
    })();

    const victim = await start('victim', { ...env, FAULT_INJECTION: 'sqs.after-commit-before-ack' });
    const msg = wagerMessage(wagerPayload(w, 'BET', uniq('bet'), '30.00'));
    await send(queues, msg, w.walletId);
    await victim.exited; // SIGKILLed itself right after COMMIT

    const committed = await one<{ status: string }>(
      'SELECT status FROM wager_transactions WHERE external_transaction_id = $1',
      [msg.data.externalTransactionId],
    );
    expect(committed.status).toBe('PROCESSED');
    expect(await approximateCount(queues, 'transactions')).toBe(1); // never acked

    const survivor = await start('survivor', env);
    await waitFor(async () => (await approximateCount(queues, 'transactions')) === 0, {
      timeoutMs: 30_000,
      message: 'redelivered message acked',
    });
    const metrics = await (await fetch(`${survivor.url}/metrics`)).text();
    expect(metrics).toMatch(/wagering_duplicates_total\{kind="inbox_redelivery"\} 1/);
    const debits = await one<{ n: number }>(
      "SELECT count(*)::int AS n FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'",
      [w.walletId],
    );
    expect(debits.n).toBe(1);
    expect((await assertLedgerInvariant(w.walletId)).balance).toBe('70.00');
  }, 90_000);

  test('6. two publishers on the same outbox, one killed between publish and commit', async () => {
    const queues = await createTestQueues('crash-outbox');
    const relayOnly = { ...queues.env, CONSUMER_ENABLED: 'false', PENDING_REFERENCE_WORKER_ENABLED: 'false' };
    const setup = await start('setup', { ...relayOnly, OUTBOX_RELAY_ENABLED: 'false' });
    const wallets = await Promise.all(Array.from({ length: 10 }, () => createWallet(setup, '10.00')));
    await setup.kill();
    const ids = wallets.map((w) => w.walletId);
    const eventIds = (await q<{ id: string }>('SELECT id FROM outbox_messages WHERE aggregate_id = ANY($1::text[])', [ids])).map(
      (r) => r.id,
    );

    // Publisher A publishes a batch to SQS and dies before marking it published.
    const a = await start('publisher-a', { ...relayOnly, FAULT_INJECTION: 'outbox.after-publish-before-commit' }, false);
    await a.exited;
    // Publisher B and C take over concurrently.
    await Promise.all([start('publisher-b', relayOnly), start('publisher-c', relayOnly)]);
    await waitFor(
      async () =>
        (await q('SELECT 1 FROM outbox_messages WHERE id = ANY($1::uuid[]) AND published_at IS NULL', [eventIds])).length === 0,
      { timeoutMs: 30_000, message: 'outbox drained' },
    );

    const received = await drain(queues, 'events');
    const byEventId = new Map<string, number>();
    for (const m of received) {
      const id = m.MessageAttributes?.eventId?.StringValue ?? '';
      byEventId.set(id, (byEventId.get(id) ?? 0) + 1);
      expect(JSON.parse(m.Body ?? '{}').eventId).toBe(id); // envelope carries the stable id
    }
    for (const id of eventIds) expect(byEventId.get(id) ?? 0).toBeGreaterThanOrEqual(1);
    // Duplicates (if the broker did not drop them) share the eventId → consumers de-duplicate.
  }, 90_000);

  test('8. kill every instance mid-traffic, restart, and prove final consistency', async () => {
    const queues = await createTestQueues('restart');
    const first = await Promise.all([1, 2, 3].map((n) => start(`gen1-${n}`, queues.env)));
    const wallets = await Promise.all(Array.from({ length: 5 }, (_, i) => createWallet(first[i % 3]!, '100.00')));

    const sqsOps = wallets.flatMap((w) =>
      Array.from({ length: 20 }, (_, i) => ({ w, msg: wagerMessage(wagerPayload(w, i % 3 ? 'BET' : 'WIN', uniq('op'), '4.00')) })),
    );
    for (const { w, msg } of sqsOps.slice(0, 50)) await send(queues, msg, w.walletId);
    const httpOps = wallets.flatMap((w) => Array.from({ length: 10 }, () => wagerPayload(w, 'BET', uniq('http'), '2.00')));
    const inflight = Promise.allSettled(httpOps.map((p, i) => submitHttp(first[i % 3]!, p)));

    await Bun.sleep(400);
    await Promise.all(first.map((i) => i.kill())); // crash everything mid-flight
    await inflight;
    for (const { w, msg } of sqsOps.slice(50)) await send(queues, msg, w.walletId);

    const second = await Promise.all([1, 2, 3].map((n) => start(`gen2-${n}`, queues.env)));
    // Providers retry every HTTP call they did not get an answer for (same idempotency key).
    const retried = await Promise.all(httpOps.map((p, i) => submitHttp(second[i % 3]!, p)));
    expect(retried.every((r) => [200, 422].includes(r.status))).toBe(true);

    const ids = wallets.map((w) => w.walletId);
    await waitFor(() => quiesced(queues, ids), { timeoutMs: 60_000, message: 'recovered and quiesced' });

    for (const w of wallets) await assertLedgerInvariant(w.walletId);
    // Each operation exists exactly once, and every balance move has exactly one ledger entry.
    const keys = [...sqsOps.map((o) => o.msg.data.externalTransactionId), ...httpOps.map((p) => p.externalTransactionId)];
    const rows = await q<{ external_transaction_id: string }>(
      'SELECT external_transaction_id FROM wager_transactions WHERE external_transaction_id = ANY($1::text[])',
      [keys],
    );
    expect(rows).toHaveLength(keys.length);
    const orphan = await q(
      `SELECT t.id FROM wager_transactions t
         LEFT JOIN wallet_ledger_entries e ON e.transaction_id = t.id
        WHERE t.wallet_id = ANY($1::uuid[]) AND t.status = 'PROCESSED' AND t.kind <> 'LOSS' AND e.id IS NULL`,
      [ids],
    );
    expect(orphan).toHaveLength(0);
    for (const w of wallets) {
      const r = await postJson(`${second[0]!.url}/wallets/${w.walletId}/reconciliation`, {});
      expect(r.body.consistent).toBe(true);
    }
  }, 150_000);
});
