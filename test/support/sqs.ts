import { randomUUID } from 'node:crypto';
import {
  DeleteMessageCommand,
  GetQueueAttributesCommand,
  type Message,
  ReceiveMessageCommand,
  SendMessageCommand,
  type SQSClient,
} from '@aws-sdk/client-sqs';
import { createQueues } from '../../scripts/create-queues';
import { createSqsClient, QueueUrls } from '../../src/infrastructure/messaging/sqs-client';
import { testConfig } from './env';

export interface TestQueues {
  client: SQSClient;
  urls: QueueUrls;
  names: { transactions: string; transactionsDlq: string; events: string };
  env: Record<string, string>;
  url(name: keyof TestQueues['names']): Promise<string>;
}

/** Isolated FIFO queues per suite, so suites never steal each other's messages. */
export async function createTestQueues(prefix: string): Promise<TestQueues> {
  const id = `${prefix}-${randomUUID().slice(0, 8)}`;
  const names = {
    transactions: `${id}-wager-transactions.fifo`,
    transactionsDlq: `${id}-wager-transactions-dlq.fifo`,
    events: `${id}-wager-events.fifo`,
  };
  const client = createSqsClient(testConfig());
  await createQueues(client, names);
  const urls = new QueueUrls(client);
  return {
    client,
    urls,
    names,
    env: {
      SQS_TRANSACTIONS_QUEUE: names.transactions,
      SQS_TRANSACTIONS_DLQ: names.transactionsDlq,
      SQS_EVENTS_QUEUE: names.events,
    },
    url: (name) => urls.get(names[name]),
  };
}

export function wagerMessage<T extends { providerId: string; externalTransactionId: string }>(
  data: T,
  messageId = randomUUID(),
) {
  return {
    messageId,
    type: 'WagerTransactionRequested',
    occurredAt: new Date().toISOString(),
    data: { idempotencyKey: `${data.providerId}:${data.externalTransactionId}`, ...data },
  };
}

/** Sends with a fresh SQS deduplication id, so even identical bodies are delivered again (simulated redelivery). */
export async function send(queues: TestQueues, body: unknown, groupId: string): Promise<void> {
  await queues.client.send(
    new SendMessageCommand({
      QueueUrl: await queues.url('transactions'),
      MessageBody: typeof body === 'string' ? body : JSON.stringify(body),
      MessageGroupId: groupId,
      MessageDeduplicationId: randomUUID(),
    }),
  );
}

export async function approximateCount(queues: TestQueues, name: keyof TestQueues['names']): Promise<number> {
  const attrs = await queues.client.send(
    new GetQueueAttributesCommand({
      QueueUrl: await queues.url(name),
      AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'],
    }),
  );
  return (
    Number(attrs.Attributes?.ApproximateNumberOfMessages ?? 0) +
    Number(attrs.Attributes?.ApproximateNumberOfMessagesNotVisible ?? 0)
  );
}

/** Receives (and deletes) everything currently visible in a queue. */
export async function drain(queues: TestQueues, name: keyof TestQueues['names'], idleRounds = 2): Promise<Message[]> {
  const url = await queues.url(name);
  const all: Message[] = [];
  let idle = 0;
  while (idle < idleRounds) {
    const r = await queues.client.send(
      new ReceiveMessageCommand({
        QueueUrl: url,
        MaxNumberOfMessages: 10,
        WaitTimeSeconds: 1,
        MessageAttributeNames: ['All'],
        MessageSystemAttributeNames: ['All'],
      }),
    );
    const messages = r.Messages ?? [];
    if (messages.length === 0) {
      idle++;
      continue;
    }
    idle = 0;
    all.push(...messages);
    for (const m of messages) {
      await queues.client.send(new DeleteMessageCommand({ QueueUrl: url, ReceiptHandle: m.ReceiptHandle }));
    }
  }
  return all;
}
