import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  type Message,
  ReceiveMessageCommand,
  SendMessageCommand,
  type SQSClient,
} from '@aws-sdk/client-sqs';
import { canonicalJson, sha256Hex } from '../../application/idempotency/payload-hash';
import type { AppLogger, FaultInjector, Metrics } from '../../application/ports';
import type { SubmitWagerTransactionUseCase } from '../../application/wagering/submit-wager-transaction.use-case';
import type { AppConfig } from '../../config/app-config';
import type { QueueUrls } from '../../infrastructure/messaging/sqs-client';
import { runWithLogContext } from '../../infrastructure/observability/logger';
import { parseContract, toSubmitCommand, wagerMessageSchema } from '../contracts/wager-contracts';
import { classifyError } from '../error-classification';

type Disposition =
  | { type: 'ack'; outcome: string }
  | { type: 'retry'; reason: string }
  | { type: 'dlq'; reason: string; code: string };

/**
 * Long-polling consumer of `wager-transactions.fifo`.
 *
 *  - Reuses SubmitWagerTransactionUseCase (same rules as HTTP).
 *  - De-duplicates through the persistent inbox `(consumerName, messageId)`,
 *    written in the same SQL transaction as the effects.
 *  - Deletes (acks) a message only after that transaction committed.
 *  - Business outcomes (PROCESSED/REJECTED/PENDING_REFERENCE) are persisted,
 *    then acked; transient errors are retried with exponential backoff through
 *    the visibility timeout; permanent errors — and anything still failing
 *    after `maxAttempts` deliveries — go to the DLQ with the reason attached.
 *    The queue's redrive policy is a second safety net.
 *  - An unknown wallet is permanent too, but cannot be persisted as a REJECTED
 *    transaction (the row needs an existing wallet). Acking it would erase the
 *    only record of the operation, so it goes to the DLQ (`not_found`), where it
 *    stays auditable and can be replayed once the wallet exists or the id is fixed.
 *  - FIFO: messages of the same MessageGroupId (wallet) are handled in order;
 *    different groups run concurrently. If one message of a group is not acked,
 *    the rest of that group in the batch is released, preserving order.
 *  - SIGTERM: stops polling, lets in-flight messages finish within the grace
 *    period, and returns not-yet-started messages to the queue (visibility 0).
 */
export class WagerTransactionsConsumer {
  private stopping = false;
  private abort = new AbortController();
  private loop?: Promise<void>;
  /** Messages whose handling has started and not yet settled (acked, retried or dead-lettered). */
  private readonly inFlight = new Map<Message, string>();

  constructor(
    private readonly client: SQSClient,
    private readonly urls: QueueUrls,
    private readonly submit: SubmitWagerTransactionUseCase,
    private readonly config: AppConfig,
    private readonly metrics: Metrics,
    private readonly logger: AppLogger,
    private readonly faults: FaultInjector,
  ) {}

  start(): void {
    if (this.loop) return;
    this.stopping = false;
    this.abort = new AbortController();
    this.loop = this.run();
    this.logger.info('SQS consumer started', { queue: this.config.queues.transactions });
  }

  async stop(): Promise<void> {
    if (!this.loop) return;
    this.stopping = true;
    this.abort.abort();
    const grace = new Promise<'timeout'>((resolve) =>
      setTimeout(() => resolve('timeout'), this.config.consumer.shutdownGraceMs).unref(),
    );
    const result = await Promise.race([this.loop.then(() => 'done' as const), grace]);
    if (result === 'timeout' && this.inFlight.size > 0) {
      // Still running after the grace period: hand them back now instead of after the
      // visibility timeout. If one still commits, its redelivery is absorbed by the inbox.
      const pending = [...this.inFlight];
      this.inFlight.clear();
      await Promise.all(pending.map(([message, queueUrl]) => this.releaseAll(queueUrl, [message], 0)));
      this.logger.warn('Returned in-flight messages to the queue after the grace period', { count: pending.length });
    }
    this.loop = undefined;
    this.logger.info('SQS consumer stopped', { result });
  }

  private async run(): Promise<void> {
    let failures = 0;
    while (!this.stopping) {
      try {
        const queueUrl = await this.urls.get(this.config.queues.transactions);
        const response = await this.client.send(
          new ReceiveMessageCommand({
            QueueUrl: queueUrl,
            MaxNumberOfMessages: this.config.consumer.maxMessages,
            WaitTimeSeconds: this.config.consumer.waitTimeSeconds,
            VisibilityTimeout: this.config.consumer.visibilityTimeoutSeconds,
            MessageSystemAttributeNames: ['ApproximateReceiveCount', 'MessageGroupId', 'SentTimestamp'],
            MessageAttributeNames: ['All'],
          }),
          { abortSignal: this.abort.signal },
        );
        failures = 0;
        const messages = response.Messages ?? [];
        if (messages.length > 0) await this.handleBatch(queueUrl, messages);
      } catch (error) {
        if (this.stopping) break;
        failures++;
        this.logger.warn('SQS receive failed; backing off', { failures, err: String(error) });
        await sleep(Math.min(1000 * 2 ** Math.min(failures - 1, 5), 30_000));
      }
    }
  }

  private async handleBatch(queueUrl: string, messages: Message[]): Promise<void> {
    const groups = new Map<string, Message[]>();
    for (const message of messages) {
      const group = message.Attributes?.MessageGroupId ?? message.MessageId ?? 'default';
      groups.set(group, [...(groups.get(group) ?? []), message]);
    }
    await Promise.all([...groups.values()].map((group) => this.handleGroup(queueUrl, group)));
  }

  private async handleGroup(queueUrl: string, group: Message[]): Promise<void> {
    for (let i = 0; i < group.length; i++) {
      const message = group[i]!;
      if (this.stopping) {
        await this.releaseAll(queueUrl, group.slice(i), 0);
        return;
      }
      this.inFlight.set(message, queueUrl);
      let disposition: Disposition;
      try {
        disposition = await this.handleMessage(queueUrl, message);
      } finally {
        this.inFlight.delete(message);
      }
      if (disposition.type === 'retry') {
        // Keep FIFO order: later messages of this group wait for the failed one.
        await this.releaseAll(queueUrl, group.slice(i + 1), this.backoffSeconds(message));
        return;
      }
    }
  }

  private async handleMessage(queueUrl: string, message: Message): Promise<Disposition> {
    let disposition: Disposition;
    let body: unknown;
    try {
      body = JSON.parse(message.Body ?? '');
    } catch {
      body = undefined;
    }
    const raw = (body ?? {}) as { messageId?: unknown; correlationId?: unknown; data?: Record<string, unknown> };
    const messageId = typeof raw.messageId === 'string' ? raw.messageId : message.MessageId;
    const context = {
      correlationId: typeof raw.correlationId === 'string' ? raw.correlationId : messageId,
      messageId,
      walletId: typeof raw.data?.walletId === 'string' ? raw.data.walletId : undefined,
      providerId: typeof raw.data?.providerId === 'string' ? raw.data.providerId : undefined,
    };

    return runWithLogContext(context, async () => {
      try {
        const envelope = parseContract(wagerMessageSchema, body);
        const { idempotencyKey, ...payload } = envelope.data;
        const result = await this.submit.execute(
          toSubmitCommand(payload, {
            idempotencyKey,
            correlationId: envelope.correlationId ?? envelope.messageId,
            source: 'sqs',
            inbox: {
              consumerName: this.config.consumer.name,
              messageId: envelope.messageId,
              payloadHash: sha256Hex(canonicalJson(envelope.data)),
            },
          }),
        );
        disposition = { type: 'ack', outcome: result.idempotentReplay ? 'replay' : result.status };
      } catch (error) {
        disposition = this.dispositionFor(error, message);
      }

      try {
        await this.apply(queueUrl, message, disposition);
      } catch (error) {
        // Ack/DLQ failed: the message becomes visible again and is redelivered;
        // the inbox makes that redelivery harmless.
        this.logger.warn('Failed to settle SQS message', { err: String(error), disposition: disposition.type });
        this.metrics.sqsMessage('settle_failed');
        return { type: 'retry', reason: 'settle_failed' };
      }
      return disposition;
    });
  }

  private dispositionFor(error: unknown, message: Message): Disposition {
    const classified = classifyError(error);
    const receiveCount = Number(message.Attributes?.ApproximateReceiveCount ?? '1');
    switch (classified.category) {
      case 'not_found':
      case 'invalid':
      case 'conflict':
        // Permanent: redelivery cannot help. Dead-lettered (never silently acked) so the
        // operation stays auditable and recoverable — see the class comment for not_found.
        this.logger.warn('Permanent message failure', { code: classified.code, reason: classified.category });
        return { type: 'dlq', reason: classified.category, code: classified.code };
      case 'transient':
      case 'internal':
        if (classified.category === 'internal') {
          this.logger.error('Unexpected error consuming message', { code: classified.code }, error);
        }
        if (receiveCount >= this.config.consumer.maxAttempts) {
          return { type: 'dlq', reason: 'max_attempts_exceeded', code: classified.code };
        }
        return { type: 'retry', reason: classified.category };
    }
  }

  private async apply(queueUrl: string, message: Message, disposition: Disposition): Promise<void> {
    switch (disposition.type) {
      case 'ack':
        await this.faults.checkpoint('sqs.after-commit-before-ack');
        await this.delete(queueUrl, message);
        this.metrics.sqsMessage(`ack_${disposition.outcome.toLowerCase()}`);
        return;
      case 'retry': {
        const delay = this.backoffSeconds(message);
        await this.client.send(
          new ChangeMessageVisibilityCommand({
            QueueUrl: queueUrl,
            ReceiptHandle: message.ReceiptHandle,
            VisibilityTimeout: delay,
          }),
        );
        this.metrics.retry('sqs_message', disposition.reason);
        this.metrics.sqsMessage('retry');
        this.logger.warn('Message scheduled for retry', { delaySeconds: delay, reason: disposition.reason });
        return;
      }
      case 'dlq': {
        const dlqUrl = await this.urls.get(this.config.queues.transactionsDlq);
        await this.client.send(
          new SendMessageCommand({
            QueueUrl: dlqUrl,
            MessageBody: message.Body ?? '',
            MessageGroupId: message.Attributes?.MessageGroupId ?? 'dead-letter',
            MessageDeduplicationId: message.MessageId,
            MessageAttributes: {
              failureReason: { DataType: 'String', StringValue: disposition.reason },
              errorCode: { DataType: 'String', StringValue: disposition.code },
              receiveCount: {
                DataType: 'Number',
                StringValue: message.Attributes?.ApproximateReceiveCount ?? '1',
              },
            },
          }),
        );
        await this.delete(queueUrl, message);
        this.metrics.dlqMessage(disposition.reason);
        this.metrics.sqsMessage('dlq');
        this.logger.warn('Message moved to DLQ', { reason: disposition.reason, code: disposition.code });
        return;
      }
    }
  }

  private async delete(queueUrl: string, message: Message): Promise<void> {
    await this.client.send(new DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: message.ReceiptHandle }));
  }

  private async releaseAll(queueUrl: string, messages: Message[], visibilitySeconds: number): Promise<void> {
    for (const message of messages) {
      try {
        await this.client.send(
          new ChangeMessageVisibilityCommand({
            QueueUrl: queueUrl,
            ReceiptHandle: message.ReceiptHandle,
            VisibilityTimeout: visibilitySeconds,
          }),
        );
      } catch (error) {
        this.logger.warn('Failed to release message', { err: String(error) });
      }
    }
  }

  /** Exponential backoff by delivery count, capped (visibility timeout max is 12h in SQS). */
  private backoffSeconds(message: Message): number {
    const receiveCount = Number(message.Attributes?.ApproximateReceiveCount ?? '1');
    const { retryBaseDelaySeconds, retryMaxDelaySeconds } = this.config.consumer;
    return Math.min(retryBaseDelaySeconds * 2 ** Math.max(receiveCount - 1, 0), retryMaxDelaySeconds);
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
