import type { OutboxRetryPolicy } from '../../domain/messaging/outbox-message';
import type { AppLogger, Clock, EventPublisher, FaultInjector, Metrics, UnitOfWork } from '../ports';

/**
 * One tick of the outbox relay.
 *
 * Claims due rows with `FOR UPDATE SKIP LOCKED` inside a transaction, publishes
 * them, marks them published (or schedules a retry) and commits. Concurrent
 * relays (any number of instances) never pick the same rows. If the process
 * dies after publishing but before committing, the row lock is released, the
 * rows are still unpublished, and another relay publishes them again: delivery
 * is at-least-once, and the stable `eventId` (also the FIFO deduplication id)
 * lets consumers drop the duplicate.
 *
 * The publish call happens while row locks are held; this is bounded by the
 * SQS client timeout and the small batch size, and it is what guarantees that
 * two relays can never publish the same row concurrently.
 */
export class RelayOutboxUseCase {
  constructor(
    private readonly uow: UnitOfWork,
    private readonly publisher: EventPublisher,
    private readonly clock: Clock,
    private readonly metrics: Metrics,
    private readonly logger: AppLogger,
    private readonly faults: FaultInjector,
    private readonly retryPolicy: OutboxRetryPolicy,
  ) {}

  async runOnce(batchSize: number): Promise<number> {
    return this.uow.transactional(async (scope) => {
      const batch = await scope.outbox.claimDue(this.clock.now(), batchSize);
      if (batch.length === 0) return 0;

      const results = await this.publisher.publish(batch);
      await this.faults.checkpoint('outbox.after-publish-before-commit');

      const byId = new Map(results.map((r) => [r.id, r]));
      const now = this.clock.now();
      let failed = 0;
      for (const message of batch) {
        const result = byId.get(message.id);
        if (result?.ok) {
          message.markPublished(now);
        } else {
          failed++;
          message.scheduleRetry(now, result?.error ?? 'no publish result', this.retryPolicy);
        }
      }
      await scope.outbox.save(batch);

      this.metrics.outboxPublished(batch.length - failed);
      if (failed > 0) {
        this.metrics.outboxPublishFailed(failed);
        this.metrics.retry('outbox_publish', 'publish_failed');
        this.logger.warn('Outbox publish failures; rescheduled', { failed, batch: batch.length });
      }
      return batch.length;
    });
  }

  async refreshStats(): Promise<void> {
    const stats = await this.uow.transactional((scope) => scope.outbox.stats(this.clock.now()), { readOnly: true });
    this.metrics.outboxState(stats);
  }
}
