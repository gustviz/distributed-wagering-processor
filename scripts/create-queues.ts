import { CreateQueueCommand, GetQueueAttributesCommand, SQSClient } from '@aws-sdk/client-sqs';
import { loadConfig } from '../src/config/app-config';
import { createSqsClient } from '../src/infrastructure/messaging/sqs-client';

/**
 * Idempotently provisions the FIFO queues:
 *   wager-transactions-dlq.fifo   dead letters (14 days retention)
 *   wager-transactions.fifo       inbound operations; redrive → DLQ after 10 receives
 *                                 (safety net: the consumer itself dead-letters after SQS_MAX_ATTEMPTS)
 *   wager-events.fifo             integration events published by the outbox relay
 */
export async function createQueues(client: SQSClient, names = loadConfig().queues): Promise<void> {
  const dlq = await client.send(
    new CreateQueueCommand({
      QueueName: names.transactionsDlq,
      Attributes: { FifoQueue: 'true', MessageRetentionPeriod: '1209600' },
    }),
  );
  const dlqArn = (
    await client.send(new GetQueueAttributesCommand({ QueueUrl: dlq.QueueUrl, AttributeNames: ['QueueArn'] }))
  ).Attributes?.QueueArn;
  if (!dlqArn) throw new Error('DLQ ARN not available');

  await client.send(
    new CreateQueueCommand({
      QueueName: names.transactions,
      Attributes: {
        FifoQueue: 'true',
        ContentBasedDeduplication: 'false',
        VisibilityTimeout: '30',
        RedrivePolicy: JSON.stringify({ deadLetterTargetArn: dlqArn, maxReceiveCount: 10 }),
      },
    }),
  );
  await client.send(
    new CreateQueueCommand({
      QueueName: names.events,
      Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'false' },
    }),
  );
}

if (import.meta.main) {
  const config = loadConfig();
  const client = createSqsClient(config);
  const deadline = Date.now() + 60_000;
  for (;;) {
    try {
      await createQueues(client, config.queues);
      console.log(JSON.stringify({ level: 'info', msg: 'queues ready', queues: config.queues }));
      process.exit(0);
    } catch (error) {
      if (Date.now() > deadline) {
        console.error(JSON.stringify({ level: 'error', msg: 'queue provisioning failed', error: String(error) }));
        process.exit(1);
      }
      await new Promise((r) => setTimeout(r, 1000));
    }
  }
}
