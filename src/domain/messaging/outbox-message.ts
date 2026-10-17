import type { IntegrationEvent } from '../events/integration-event';

export interface OutboxMessageState {
  id: string;
  aggregateId: string;
  eventType: string;
  payload: Readonly<Record<string, unknown>>;
  occurredAt: Date;
  attempts: number;
  nextAttemptAt?: Date;
  publishedAt?: Date;
  lastError?: string;
}

export interface OutboxRetryPolicy {
  baseDelayMs: number;
  maxDelayMs: number;
}

export const DEFAULT_OUTBOX_RETRY_POLICY: OutboxRetryPolicy = { baseDelayMs: 1_000, maxDelayMs: 60_000 };

/**
 * An integration event persisted in the same SQL transaction as the state
 * change that produced it, published later by the relay.
 *
 * The outbox never gives up on a message: a confirmed event must not be lost,
 * so failures back off (capped) and are surfaced through metrics/lag.
 */
export class OutboxMessage {
  private constructor(
    public readonly id: string,
    public readonly aggregateId: string,
    public readonly eventType: string,
    public readonly payload: Readonly<Record<string, unknown>>,
    public readonly occurredAt: Date,
    private _attempts: number,
    private _nextAttemptAt?: Date,
    private _publishedAt?: Date,
    private _lastError?: string,
  ) {}

  /** The event id doubles as the outbox id: one row per event, natural de-dup key downstream. */
  static enqueue(event: IntegrationEvent<unknown>): OutboxMessage {
    return new OutboxMessage(
      event.eventId,
      event.aggregateId,
      event.eventType,
      event.toJSON() as unknown as Record<string, unknown>,
      event.occurredAt,
      0,
    );
  }

  static rehydrate(state: OutboxMessageState): OutboxMessage {
    return new OutboxMessage(
      state.id,
      state.aggregateId,
      state.eventType,
      state.payload,
      state.occurredAt,
      state.attempts,
      state.nextAttemptAt,
      state.publishedAt,
      state.lastError,
    );
  }

  get attempts(): number {
    return this._attempts;
  }
  get nextAttemptAt(): Date | undefined {
    return this._nextAttemptAt;
  }
  get publishedAt(): Date | undefined {
    return this._publishedAt;
  }
  get lastError(): string | undefined {
    return this._lastError;
  }

  isPending(): boolean {
    return this._publishedAt === undefined;
  }

  isDue(now: Date): boolean {
    return this.isPending() && (!this._nextAttemptAt || this._nextAttemptAt.getTime() <= now.getTime());
  }

  markPublished(at: Date): void {
    if (this._publishedAt) return; // idempotent
    this._publishedAt = at;
    this._nextAttemptAt = undefined;
    this._lastError = undefined;
  }

  /** Increments attempts and schedules the next one with capped exponential backoff + jitter. */
  scheduleRetry(now: Date, error: string, policy: OutboxRetryPolicy = DEFAULT_OUTBOX_RETRY_POLICY, random = Math.random): void {
    this._attempts += 1;
    const exponential = policy.baseDelayMs * 2 ** Math.min(this._attempts - 1, 30);
    const capped = Math.min(exponential, policy.maxDelayMs);
    const jittered = capped / 2 + random() * (capped / 2); // "equal jitter"
    this._nextAttemptAt = new Date(now.getTime() + Math.round(jittered));
    this._lastError = error.slice(0, 500);
  }

  toState(): OutboxMessageState {
    return {
      id: this.id,
      aggregateId: this.aggregateId,
      eventType: this.eventType,
      payload: this.payload,
      occurredAt: this.occurredAt,
      attempts: this._attempts,
      nextAttemptAt: this._nextAttemptAt,
      publishedAt: this._publishedAt,
      lastError: this._lastError,
    };
  }
}
