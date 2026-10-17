import { SendMessageBatchCommand, type SQSClient } from '@aws-sdk/client-sqs';
import type { EventPublisher, PublishResult } from '../../application/ports';
import type { OutboxMessage } from '../../domain/messaging/outbox-message';
import type { QueueUrls } from './sqs-client';

/**
 * Publishes outbox events to an SQS FIFO queue.
 *  - MessageGroupId = aggregateId (walletId): per-wallet ordering downstream;
 *    (best-effort: parallel chunks/relays may interleave; `walletVersion` is the real order);
 *  - MessageDeduplicationId = eventId: SQS drops re-publications within its
 *    5-minute window. This is an optimization only — consumers must still
 *    de-duplicate by eventId (at-least-once).
 * Never throws: failures are reported per message so the relay can back off.
 */
export class SqsEventPublisher implements EventPublisher {
  constructor(
    private readonly client: SQSClient,
    private readonly urls: QueueUrls,
    private readonly queueName: string,
  ) {}

  async publish(messages: readonly OutboxMessage[]): Promise<PublishResult[]> {
    const results: PublishResult[] = [];
    let queueUrl: string;
    try {
      queueUrl = await this.urls.get(this.queueName);
    } catch (error) {
      return messages.map((m) => ({ id: m.id, ok: false, error: describe(error) }));
    }

    // SQS batches hold at most 10 entries; chunks go out in parallel. Ordering across
    // chunks (and across relays) is best-effort: consumers order by `walletVersion`.
    const chunks: OutboxMessage[][] = [];
    for (let i = 0; i < messages.length; i += 10) chunks.push(messages.slice(i, i + 10));
    const perChunk = await Promise.all(chunks.map((chunk) => this.publishChunk(queueUrl, chunk)));
    for (const chunkResults of perChunk) results.push(...chunkResults);
    return results;
  }

  private async publishChunk(queueUrl: string, chunk: OutboxMessage[]): Promise<PublishResult[]> {
    try {
      const response = await this.client.send(
        new SendMessageBatchCommand({
          QueueUrl: queueUrl,
          Entries: chunk.map((m, index) => ({
            Id: `m${index}`,
            MessageBody: JSON.stringify(m.payload),
            MessageGroupId: m.aggregateId,
            MessageDeduplicationId: m.id,
            MessageAttributes: {
              eventType: { DataType: 'String', StringValue: m.eventType },
              eventId: { DataType: 'String', StringValue: m.id },
            },
          })),
        }),
      );
      const failed = new Map((response.Failed ?? []).map((f) => [f.Id, f.Message ?? f.Code ?? 'failed']));
      return chunk.map((m, index) => {
        const error = failed.get(`m${index}`);
        return error ? { id: m.id, ok: false, error } : { id: m.id, ok: true };
      });
    } catch (error) {
      return chunk.map((m) => ({ id: m.id, ok: false, error: describe(error) }));
    }
  }
}

function describe(error: unknown): string {
  return error instanceof Error ? `${error.name}: ${error.message}` : String(error);
}
