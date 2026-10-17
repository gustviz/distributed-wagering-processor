import { Counter, Gauge, Histogram, Registry } from 'prom-client';
import type { Metrics, OutboxStats, TransactionSource } from '../../application/ports';

/** Prometheus implementation of the metrics port, exposed on GET /metrics. */
export class PrometheusMetrics implements Metrics {
  readonly registry = new Registry();

  private readonly transactions = new Counter({
    name: 'wagering_transactions_total',
    help: 'Wager transactions by kind, final status of the attempt and entry point',
    labelNames: ['kind', 'status', 'source'] as const,
    registers: [this.registry],
  });
  private readonly duration = new Histogram({
    name: 'wagering_processing_duration_seconds',
    help: 'End-to-end processing latency of a wager submission (including retries)',
    labelNames: ['source'] as const,
    buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
    registers: [this.registry],
  });
  private readonly duplicates = new Counter({
    name: 'wagering_duplicates_total',
    help: 'Duplicates detected: idempotent replays, inbox redeliveries and idempotency conflicts',
    labelNames: ['kind'] as const,
    registers: [this.registry],
  });
  private readonly retries = new Counter({
    name: 'wagering_retries_total',
    help: 'Retries by operation and reason',
    labelNames: ['operation', 'reason'] as const,
    registers: [this.registry],
  });
  private readonly lockConflicts = new Counter({
    name: 'wagering_lock_conflicts_total',
    help: 'Lock timeouts, deadlocks, serialization failures and optimistic version conflicts',
    registers: [this.registry],
  });
  private readonly dlq = new Counter({
    name: 'wagering_dlq_messages_total',
    help: 'Messages moved to the dead-letter queue, by reason',
    labelNames: ['reason'] as const,
    registers: [this.registry],
  });
  private readonly sqsMessages = new Counter({
    name: 'wagering_sqs_messages_total',
    help: 'Consumed SQS messages by disposition',
    labelNames: ['outcome'] as const,
    registers: [this.registry],
  });
  private readonly outboxPublishedCounter = new Counter({
    name: 'wagering_outbox_published_total',
    help: 'Outbox events published',
    registers: [this.registry],
  });
  private readonly outboxFailedCounter = new Counter({
    name: 'wagering_outbox_publish_failures_total',
    help: 'Outbox publish failures (rescheduled with backoff)',
    registers: [this.registry],
  });
  private readonly outboxPending = new Gauge({
    name: 'wagering_outbox_pending',
    help: 'Unpublished outbox events',
    registers: [this.registry],
  });
  private readonly outboxLag = new Gauge({
    name: 'wagering_outbox_lag_seconds',
    help: 'Age of the oldest unpublished outbox event',
    registers: [this.registry],
  });
  private readonly pendingReferences = new Counter({
    name: 'wagering_pending_reference_attempts_total',
    help: 'Pending-reference resolution attempts by resulting status',
    labelNames: ['outcome'] as const,
    registers: [this.registry],
  });
  private readonly reconciliations = new Counter({
    name: 'wagering_reconciliations_total',
    help: 'Wallet reconciliations by result',
    labelNames: ['result'] as const,
    registers: [this.registry],
  });

  transactionFinished(kind: string, status: string, source: TransactionSource): void {
    this.transactions.inc({ kind, status, source });
  }
  processingDuration(source: TransactionSource, seconds: number): void {
    this.duration.observe({ source }, seconds);
  }
  duplicateDetected(kind: 'idempotent_replay' | 'inbox_redelivery' | 'idempotency_conflict'): void {
    this.duplicates.inc({ kind });
  }
  retry(operation: string, reason: string): void {
    this.retries.inc({ operation, reason });
  }
  lockConflict(): void {
    this.lockConflicts.inc();
  }
  dlqMessage(reason: string): void {
    this.dlq.inc({ reason });
  }
  sqsMessage(outcome: string): void {
    this.sqsMessages.inc({ outcome });
  }
  outboxPublished(count: number): void {
    if (count > 0) this.outboxPublishedCounter.inc(count);
  }
  outboxPublishFailed(count: number): void {
    if (count > 0) this.outboxFailedCounter.inc(count);
  }
  outboxState(stats: OutboxStats): void {
    this.outboxPending.set(stats.pending);
    this.outboxLag.set(stats.oldestPendingAgeSeconds);
  }
  pendingReferenceResolution(outcome: string): void {
    this.pendingReferences.inc({ outcome });
  }
  reconciliation(consistent: boolean): void {
    this.reconciliations.inc({ result: consistent ? 'consistent' : 'mismatch' });
  }
}
