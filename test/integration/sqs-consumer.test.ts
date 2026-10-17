import type { Message } from '@aws-sdk/client-sqs';
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { TransientInfrastructureError } from '../../src/application/errors';
import type { SubmitWagerTransactionUseCase } from '../../src/application/wagering/submit-wager-transaction.use-case';
import { WagerTransactionsConsumer } from '../../src/interface/messaging/wager-transactions.consumer';
import { type Core, createCore, newWallet, uniq, wagerPayload, waitFor } from '../support/core';
import { assertLedgerInvariant, q } from '../support/database';
import { testConfig } from '../support/env';
import { noFaults, silentLogger } from '../support/in-memory';
import { approximateCount, createTestQueues, drain, send, type TestQueues, wagerMessage } from '../support/sqs';

/** Real SQS (LocalStack/MiniStack) + real PostgreSQL. */
let core: Core;

beforeAll(async () => {
  core = await createCore();
});
afterAll(async () => {
  await core.close();
});

async function withConsumer<T>(
  fn: (ctx: { queues: TestQueues; consumer: WagerTransactionsConsumer }) => Promise<T>,
  submit: Pick<SubmitWagerTransactionUseCase, 'execute'> = core.submit,
  env: Record<string, string> = {},
): Promise<T> {
  const queues = await createTestQueues('consumer');
  const config = testConfig({ ...queues.env, CONSUMER_NAME: uniq('consumer'), ...env });
  const consumer = new WagerTransactionsConsumer(
    queues.client,
    queues.urls,
    submit as SubmitWagerTransactionUseCase,
    config,
    core.metrics,
    silentLogger,
    noFaults,
  );
  consumer.start();
  try {
    return await fn({ queues, consumer });
  } finally {
    await consumer.stop();
  }
}

const processedCount = async (walletId: string) =>
  (
    await q<{ n: number }>(
      "SELECT count(*)::int AS n FROM wager_transactions WHERE wallet_id = $1 AND kind <> 'OPENING'",
      [walletId],
    )
  )[0]!.n;

describe('SQS consumer', () => {
  test('processes a message via the same use case, records the inbox and acks after commit', async () => {
    await withConsumer(async ({ queues }) => {
      const w = await newWallet(core, '100.00');
      const msg = wagerMessage(wagerPayload(w, 'BET', uniq('bet'), '25.00'));
      await send(queues, msg, w.walletId);
      await waitFor(async () => (await processedCount(w.walletId)) === 1, { message: 'bet processed' });
      await waitFor(async () => (await approximateCount(queues, 'transactions')) === 0, { message: 'ack' });
      const inbox = await q('SELECT processed_at FROM inbox_messages WHERE message_id = $1', [msg.messageId]);
      expect(inbox).toHaveLength(1);
      expect((await assertLedgerInvariant(w.walletId)).balance).toBe('75.00');
    });
  });

  test('redelivered / duplicated messages do not duplicate effects', async () => {
    await withConsumer(async ({ queues }) => {
      const w = await newWallet(core, '100.00');
      const msg = wagerMessage(wagerPayload(w, 'BET', uniq('bet'), '10.00'));
      for (let i = 0; i < 5; i++) await send(queues, msg, w.walletId);
      await waitFor(async () => (await approximateCount(queues, 'transactions')) === 0, { message: 'drained' });
      expect(await processedCount(w.walletId)).toBe(1);
      expect((await assertLedgerInvariant(w.walletId)).balance).toBe('90.00');
    });
  });

  test('business rejections are acked (terminal), not retried and not dead-lettered', async () => {
    await withConsumer(async ({ queues }) => {
      const w = await newWallet(core, '5.00');
      await send(queues, wagerMessage(wagerPayload(w, 'BET', uniq('bet'), '10.00')), w.walletId);
      await waitFor(async () => (await approximateCount(queues, 'transactions')) === 0, { message: 'ack' });
      const [tx] = await q<{ status: string; failure_code: string }>(
        "SELECT status, failure_code FROM wager_transactions WHERE wallet_id = $1 AND kind = 'BET'",
        [w.walletId],
      );
      expect(tx).toEqual({ status: 'REJECTED', failure_code: 'INSUFFICIENT_FUNDS' });
      expect(await drain(queues, 'transactionsDlq', 1)).toHaveLength(0);
    });
  });

  test('permanent errors go straight to the DLQ with the reason attached', async () => {
    await withConsumer(async ({ queues }) => {
      const w = await newWallet(core, '100.00');
      await send(queues, 'not json at all', w.walletId);
      await send(queues, wagerMessage(wagerPayload(w, 'BET', uniq('bet'), '1e3')), w.walletId);
      const ext = uniq('bet');
      await send(queues, wagerMessage(wagerPayload(w, 'BET', ext, '1.00')), w.walletId);
      await send(queues, wagerMessage(wagerPayload(w, 'BET', ext, '2.00')), w.walletId); // same key, other payload

      const collected: Message[] = [];
      const dead = await waitFor(
        async () => {
          const messages = await drain(queues, 'transactionsDlq', 1);
          collected.push(...messages);
          return collected.length >= 3 && collected;
        },
        { message: '3 messages in DLQ' },
      );
      const codes = dead.map((m) => m.MessageAttributes?.errorCode?.StringValue).sort();
      expect(codes).toEqual(['IDEMPOTENCY_KEY_CONFLICT', 'INVALID_MONEY', 'INVALID_PAYLOAD']);
      expect(dead.every((m) => ['invalid', 'conflict'].includes(m.MessageAttributes?.failureReason?.StringValue ?? ''))).toBe(true);
      expect((await assertLedgerInvariant(w.walletId)).balance).toBe('99.00');
    });
  });
  test('transient failures are retried with backoff and then succeed', async () => {
    let failures = 2;
    const flaky = {
      execute: async (...args: Parameters<SubmitWagerTransactionUseCase['execute']>) => {
        if (failures-- > 0) throw new TransientInfrastructureError('database unavailable (simulated)');
        return core.submit.execute(...args);
      },
    };
    await withConsumer(
      async ({ queues }) => {
        const w = await newWallet(core, '100.00');
        await send(queues, wagerMessage(wagerPayload(w, 'BET', uniq('bet'), '10.00')), w.walletId);
        await waitFor(async () => (await processedCount(w.walletId)) === 1, { message: 'eventual success', timeoutMs: 30_000 });
        expect(failures).toBeLessThan(0);
        expect(await drain(queues, 'transactionsDlq', 1)).toHaveLength(0);
      },
      flaky,
    );
  });

  test('a message that keeps failing is dead-lettered after SQS_MAX_ATTEMPTS deliveries', async () => {
    let calls = 0;
    const broken = {
      execute: async () => {
        calls++;
        throw new TransientInfrastructureError('still down (simulated)');
      },
    };
    await withConsumer(
      async ({ queues }) => {
        const w = await newWallet(core, '100.00');
        await send(queues, wagerMessage(wagerPayload(w, 'BET', uniq('bet'), '10.00')), w.walletId);
        const dead = await waitFor(async () => {
          const m = await drain(queues, 'transactionsDlq', 1);
          return m.length > 0 && m;
        }, { message: 'DLQ', timeoutMs: 40_000 });
        expect(dead[0]!.MessageAttributes?.failureReason?.StringValue).toBe('max_attempts_exceeded');
        expect(calls).toBe(3);
      },
      broken,
      { SQS_MAX_ATTEMPTS: '3' },
    );
  });

  test('unknown wallet goes to the DLQ (nothing can be persisted for it, so it must not be acked away)', async () => {
    await withConsumer(async ({ queues }) => {
      const ghost = { walletId: crypto.randomUUID(), playerId: 'ghost' };
      const message = wagerMessage(wagerPayload(ghost, 'BET', uniq('bet'), '1.00'));
      await send(queues, message, ghost.walletId);
      const dead = await waitFor(
        async () => {
          const m = await drain(queues, 'transactionsDlq', 1);
          return m.length > 0 && m;
        },
        { message: 'unknown-wallet message dead-lettered' },
      );
      expect(dead).toHaveLength(1);
      expect(dead[0]!.MessageAttributes?.errorCode?.StringValue).toBe('WALLET_NOT_FOUND');
      expect(dead[0]!.MessageAttributes?.failureReason?.StringValue).toBe('not_found');
      expect(dead[0]!.MessageAttributes?.receiveCount?.StringValue).toBe('1'); // no pointless retries
      expect(JSON.parse(dead[0]!.Body ?? '{}').messageId).toBe(message.messageId); // original body kept for replay
      expect(await approximateCount(queues, 'transactions')).toBe(0);
      expect(await q('SELECT 1 FROM wager_transactions WHERE wallet_id = $1', [ghost.walletId])).toHaveLength(0);
    });
  });

  test('graceful shutdown: in-flight messages finish and are acked; nothing is lost', async () => {
    let release!: () => void;
    const gate = new Promise<void>((r) => {
      release = r;
    });
    let started = 0;
    const slow = {
      execute: async (...args: Parameters<SubmitWagerTransactionUseCase['execute']>) => {
        started++;
        await gate;
        return core.submit.execute(...args);
      },
    };
    const queues = await createTestQueues('shutdown');
    const config = testConfig({ ...queues.env, CONSUMER_NAME: uniq('consumer') });
    const consumer = new WagerTransactionsConsumer(
      queues.client,
      queues.urls,
      slow as unknown as SubmitWagerTransactionUseCase,
      config,
      core.metrics,
      silentLogger,
      noFaults,
    );
    const w = await newWallet(core, '100.00');
    await send(queues, wagerMessage(wagerPayload(w, 'BET', uniq('bet'), '10.00')), w.walletId);
    consumer.start();
    await waitFor(async () => started === 1, { message: 'in flight' });

    const stopping = consumer.stop(); // SIGTERM path
    await Bun.sleep(200);
    release();
    await stopping;

    expect(await processedCount(w.walletId)).toBe(1);
    expect(await approximateCount(queues, 'transactions')).toBe(0);
  });

  test('graceful shutdown: a message still in flight when the grace period ends is returned to the queue', async () => {
    let release!: () => void;
    const gate = new Promise<void>((r) => {
      release = r;
    });
    let started = 0;
    const stuck = {
      execute: async (...args: Parameters<SubmitWagerTransactionUseCase['execute']>) => {
        started++;
        await gate;
        return core.submit.execute(...args);
      },
    };
    const queues = await createTestQueues('shutdown-timeout');
    // Long visibility timeout: without an explicit release the message would stay hidden for 30 s.
    const config = testConfig({
      ...queues.env,
      CONSUMER_NAME: uniq('consumer'),
      SHUTDOWN_GRACE_MS: '300',
      SQS_VISIBILITY_TIMEOUT_SECONDS: '30',
    });
    const consumer = new WagerTransactionsConsumer(
      queues.client,
      queues.urls,
      stuck as unknown as SubmitWagerTransactionUseCase,
      config,
      core.metrics,
      silentLogger,
      noFaults,
    );
    const w = await newWallet(core, '100.00');
    const msg = wagerMessage(wagerPayload(w, 'BET', uniq('bet'), '10.00'));
    await send(queues, msg, w.walletId);
    consumer.start();
    await waitFor(async () => started === 1, { message: 'in flight' });

    await consumer.stop(); // grace period expires with the message still in flight
    const redelivered = await drain(queues, 'transactions', 2);
    expect(redelivered.map((m) => JSON.parse(m.Body ?? '{}').messageId)).toEqual([msg.messageId]);

    release(); // the stuck handler finishes late; its work is still applied exactly once
    await waitFor(async () => (await processedCount(w.walletId)) === 1, { message: 'late commit' });
    expect((await assertLedgerInvariant(w.walletId)).balance).toBe('90.00');
  });
});
