import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { NestExpressApplication } from '@nestjs/platform-express';
import { createApp } from '../../src/main';
import { uniq, wagerPayload, waitFor } from '../support/core';
import { assertLedgerInvariant, prepareDatabase } from '../support/database';
import { testConfig } from '../support/env';
import { createTestQueues } from '../support/sqs';

/** The HTTP contract: one status per situation, consistently across endpoints. */
let app: NestExpressApplication;
let base: string;

beforeAll(async () => {
  await prepareDatabase();
  const queues = await createTestQueues('http');
  app = await createApp(testConfig({ ...queues.env, CONSUMER_ENABLED: 'false' }));
  await app.listen(0, '127.0.0.1');
  base = (await app.getUrl()).replace('[::1]', '127.0.0.1');
});
afterAll(async () => {
  await app.close();
});

async function call(method: string, path: string, body?: unknown, headers: Record<string, string> = {}) {
  const r = await fetch(`${base}${path}`, {
    method,
    headers: { 'content-type': 'application/json', ...headers },
    body: body === undefined ? undefined : typeof body === 'string' ? body : JSON.stringify(body),
  });
  const text = await r.text();
  return { status: r.status, body: (text ? JSON.parse(text) : {}) as Record<string, any>, headers: r.headers };
}

async function wallet(amount = '100.00') {
  const playerId = uniq('player');
  const r = await call('POST', '/wallets', { playerId, initialBalance: { amount, currency: 'BRL' } });
  expect(r.status).toBe(201);
  return { walletId: r.body.id as string, playerId };
}

const submit = (payload: ReturnType<typeof wagerPayload>, key = `${payload.providerId}:${payload.externalTransactionId}`) =>
  call('POST', '/wagering/transactions', payload, { 'Idempotency-Key': key });

describe('wallets', () => {
  test('POST /wallets → 201 with version 1; duplicate player+currency → 409', async () => {
    const playerId = uniq('player');
    const created = await call('POST', '/wallets', { playerId, initialBalance: { amount: '1000.00', currency: 'BRL' } });
    expect(created.status).toBe(201);
    expect(created.body).toMatchObject({ playerId, balance: { amount: '1000.00', currency: 'BRL' }, version: 1 });
    const dup = await call('POST', '/wallets', { playerId, initialBalance: { amount: '0', currency: 'BRL' } });
    expect(dup.status).toBe(409);
    expect(dup.body.error.code).toBe('WALLET_ALREADY_EXISTS');
    const otherCurrency = await call('POST', '/wallets', { playerId, initialBalance: { amount: '0', currency: 'USD' } });
    expect(otherCurrency.status).toBe(201);
  });

  test('invalid payloads → 400 INVALID_PAYLOAD / INVALID_MONEY', async () => {
    expect((await call('POST', '/wallets', '{"broken json')).body.error.code).toBe('INVALID_PAYLOAD');
    const r = await call('POST', '/wallets', { playerId: 'x', initialBalance: { amount: '-5.00', currency: 'BRL' } });
    expect(r.status).toBe(400);
    expect(r.body.error).toMatchObject({ code: 'INVALID_MONEY', retryable: false });
  });

  test('GET /wallets/:id → 200; unknown → 404; malformed id → 400', async () => {
    const w = await wallet();
    expect((await call('GET', `/wallets/${w.walletId}`)).body.balance.amount).toBe('100.00');
    expect((await call('GET', `/wallets/${crypto.randomUUID()}`)).status).toBe(404);
    expect((await call('GET', '/wallets/not-a-uuid')).status).toBe(400);
  });

  test('ledger pagination: stable opaque cursor, newest first, no gaps or duplicates', async () => {
    const w = await wallet('100.00');
    for (let i = 0; i < 7; i++) await submit(wagerPayload(w, 'BET', uniq('bet'), '1.00'));
    const seen: number[] = [];
    let cursor: string | null = null;
    do {
      const page = await call('GET', `/wallets/${w.walletId}/ledger?limit=3${cursor ? `&cursor=${cursor}` : ''}`);
      expect(page.status).toBe(200);
      seen.push(...page.body.items.map((e: { walletVersion: number }) => e.walletVersion));
      cursor = page.body.nextCursor;
      // New entries appended meanwhile must not shift the remaining pages.
      if (seen.length === 3) await submit(wagerPayload(w, 'WIN', uniq('win'), '1.00'));
    } while (cursor);
    expect(seen).toEqual([8, 7, 6, 5, 4, 3, 2, 1]);
    expect((await call('GET', `/wallets/${w.walletId}/ledger?cursor=garbage`)).status).toBe(400);
    expect((await call('GET', `/wallets/${w.walletId}/ledger?limit=1000`)).status).toBe(400);
  });

  test('POST /wallets/:id/reconciliation reports stored vs ledger balance', async () => {
    const w = await wallet('975.00');
    const r = await call('POST', `/wallets/${w.walletId}/reconciliation`);
    expect(r.status).toBe(200);
    expect(r.body).toMatchObject({
      walletId: w.walletId,
      storedBalance: { amount: '975.00', currency: 'BRL' },
      calculatedBalance: { amount: '975.00', currency: 'BRL' },
      difference: { amount: '0.00', currency: 'BRL' },
      consistent: true,
      checkedEntries: 1,
    });
  });
});

describe('POST /wagering/transactions — status mapping', () => {
  test('200 PROCESSED, identical replay → same body with idempotentReplay=true', async () => {
    const w = await wallet('1000.00');
    const payload = wagerPayload(w, 'BET', uniq('bet'), '25.00');
    const first = await submit(payload);
    expect(first.status).toBe(200);
    expect(first.body).toMatchObject({ status: 'PROCESSED', balance: { amount: '975.00', currency: 'BRL' }, idempotentReplay: false });
    const replay = await submit(payload);
    expect(replay.status).toBe(200);
    expect(replay.body).toEqual({ ...first.body, idempotentReplay: true });
  });

  test('422 business rejection (REJECTED + failureCode), also on replay', async () => {
    const w = await wallet('10.00');
    const payload = wagerPayload(w, 'BET', uniq('bet'), '25.00');
    const r = await submit(payload);
    expect(r.status).toBe(422);
    expect(r.body).toMatchObject({ status: 'REJECTED', failureCode: 'INSUFFICIENT_FUNDS', balance: { amount: '10.00' } });
    expect((await submit(payload)).status).toBe(422);
  });

  test('202 accepted with pending processing (reference not yet known)', async () => {
    const w = await wallet('10.00');
    const r = await submit(wagerPayload(w, 'ROLLBACK', uniq('rb'), '1.00', { reference: uniq('missing') }));
    expect(r.status).toBe(202);
    expect(r.body.status).toBe('PENDING_REFERENCE');
  });

  test('409 idempotency conflict (same key, different payload) — never a replay', async () => {
    const w = await wallet('100.00');
    const payload = wagerPayload(w, 'BET', uniq('bet'), '25.00');
    await submit(payload);
    const r = await submit({ ...payload, money: { amount: '26.00', currency: 'BRL' } });
    expect(r.status).toBe(409);
    expect(r.body.error).toMatchObject({ code: 'IDEMPOTENCY_KEY_CONFLICT', retryable: false });
    const otherKey = await submit(payload, 'some-other-key');
    expect(otherKey.status).toBe(409);
    expect(otherKey.body.error.code).toBe('EXTERNAL_TRANSACTION_CONFLICT');
    await assertLedgerInvariant(w.walletId);
  });

  test('400 for missing Idempotency-Key, invalid money, OPENING or reserved provider', async () => {
    const w = await wallet();
    const payload = wagerPayload(w, 'BET', uniq('bet'), '1.00');
    expect((await call('POST', '/wagering/transactions', payload)).status).toBe(400);
    for (const amount of ['1e3', 'NaN', '1.001', '-1.00', '']) {
      const r = await submit({ ...payload, money: { amount, currency: 'BRL' } });
      expect(r.status).toBe(400);
    }
    expect((await submit({ ...payload, kind: 'OPENING' as never })).status).toBe(400);
    expect((await submit({ ...payload, providerId: 'internal' })).status).toBe(400);
    expect((await submit({ ...payload, money: { amount: 1 as never, currency: 'BRL' } })).status).toBe(400);
  });

  test('404 for an unknown wallet', async () => {
    const ghost = { walletId: crypto.randomUUID(), playerId: 'ghost' };
    const r = await submit(wagerPayload(ghost, 'BET', uniq('bet'), '1.00'));
    expect(r.status).toBe(404);
    expect(r.body.error.code).toBe('WALLET_NOT_FOUND');
  });

  test('GET by internal id and by (provider, external id)', async () => {
    const w = await wallet();
    const payload = wagerPayload(w, 'BET', uniq('bet'), '1.00');
    const r = await submit(payload);
    const byId = await call('GET', `/wagering/transactions/${r.body.transactionId}`);
    expect(byId.status).toBe(200);
    expect(byId.body).toMatchObject({ id: r.body.transactionId, kind: 'BET', status: 'PROCESSED' });
    const byExt = await call('GET', `/providers/${payload.providerId}/wagering/transactions/${payload.externalTransactionId}`);
    expect(byExt.body.id).toBe(r.body.transactionId);
    expect((await call('GET', `/wagering/transactions/${crypto.randomUUID()}`)).status).toBe(404);
  });
});

describe('operability', () => {
  test('health endpoints are public; readiness checks PostgreSQL and SQS', async () => {
    expect((await call('GET', '/health/live')).body).toEqual({ status: 'ok' });
    const ready = await call('GET', '/health/ready');
    expect(ready.status).toBe(200);
    expect(ready.body.checks.database.status).toBe('up');
    expect(ready.body.checks.queue.status).toBe('up');
  });

  test('metrics expose the required series', async () => {
    const text = await (await fetch(`${base}/metrics`)).text();
    for (const metric of [
      'wagering_transactions_total',
      'wagering_duplicates_total',
      'wagering_retries_total',
      'wagering_dlq_messages_total',
      'wagering_lock_conflicts_total',
      'wagering_outbox_lag_seconds',
      'wagering_processing_duration_seconds',
    ]) {
      expect(text).toContain(metric);
    }
  });

  test('correlation id is echoed back', async () => {
    const r = await call('GET', '/health/live', undefined, { 'X-Correlation-Id': 'abc-123' });
    expect(r.headers.get('x-correlation-id')).toBe('abc-123');
  });

  test('the in-process relay publishes committed events', async () => {
    const w = await wallet('5.00');
    await waitFor(
      async () => {
        const r = await fetch(`${base}/metrics`);
        return (await r.text()).match(/wagering_outbox_published_total (\d+)/)?.[1] !== '0';
      },
      { message: 'events published' },
    );
    expect(w.walletId).toBeString();
  });
});
