import { hostname } from 'node:os';

export const APP_CONFIG = Symbol('AppConfig');

export interface AppConfig {
  serviceName: string;
  instanceId: string;
  http: { enabled: boolean; port: number };
  database: { url: string; poolMax: number; lockTimeoutMs: number; statementTimeoutMs: number };
  aws: { region: string; endpoint?: string; accessKeyId: string; secretAccessKey: string };
  queues: {
    transactions: string;
    transactionsDlq: string;
    events: string;
  };
  consumer: {
    enabled: boolean;
    name: string;
    waitTimeSeconds: number;
    visibilityTimeoutSeconds: number;
    maxMessages: number;
    /** Deliveries before a failing message is moved to the DLQ by the consumer. */
    maxAttempts: number;
    retryBaseDelaySeconds: number;
    retryMaxDelaySeconds: number;
    shutdownGraceMs: number;
  };
  outbox: {
    enabled: boolean;
    intervalMs: number;
    batchSize: number;
    retryBaseDelayMs: number;
    retryMaxDelayMs: number;
  };
  pendingReferences: {
    enabled: boolean;
    intervalMs: number;
    batchSize: number;
    leaseMs: number;
    maxAttempts: number;
    baseDelayMs: number;
    maxDelayMs: number;
  };
  logLevel: 'debug' | 'info' | 'warn' | 'error';
  faultInjection?: string;
}

function str(env: NodeJS.ProcessEnv, key: string, fallback?: string): string {
  const value = env[key] ?? fallback;
  if (value === undefined || value === '') throw new Error(`Missing required env var ${key}`);
  return value;
}

function int(env: NodeJS.ProcessEnv, key: string, fallback: number): number {
  const raw = env[key];
  if (raw === undefined || raw === '') return fallback;
  const value = Number.parseInt(raw, 10);
  if (!Number.isFinite(value)) throw new Error(`Env var ${key} must be an integer`);
  return value;
}

function bool(env: NodeJS.ProcessEnv, key: string, fallback: boolean): boolean {
  const raw = env[key];
  if (raw === undefined || raw === '') return fallback;
  return ['1', 'true', 'yes', 'on'].includes(raw.toLowerCase());
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): AppConfig {
  const logLevel = (env.LOG_LEVEL ?? 'info') as AppConfig['logLevel'];
  return {
    serviceName: env.SERVICE_NAME ?? 'wagering-processor',
    // Hostname = container id under Docker (every container's PID is 1), plus PID for bare-metal runs.
    instanceId: env.INSTANCE_ID ?? `${hostname()}:${process.pid}`,
    http: { enabled: bool(env, 'HTTP_ENABLED', true), port: int(env, 'PORT', 3000) },
    database: {
      url: str(env, 'DATABASE_URL', 'postgres://postgres:postgres@localhost:5432/wagering'),
      poolMax: int(env, 'DB_POOL_MAX', 20),
      lockTimeoutMs: int(env, 'DB_LOCK_TIMEOUT_MS', 5_000),
      statementTimeoutMs: int(env, 'DB_STATEMENT_TIMEOUT_MS', 15_000),
    },
    aws: {
      region: env.AWS_REGION ?? 'us-east-1',
      endpoint: env.AWS_ENDPOINT_URL || undefined,
      accessKeyId: env.AWS_ACCESS_KEY_ID ?? 'test',
      secretAccessKey: env.AWS_SECRET_ACCESS_KEY ?? 'test',
    },
    queues: {
      transactions: env.SQS_TRANSACTIONS_QUEUE ?? 'wager-transactions.fifo',
      transactionsDlq: env.SQS_TRANSACTIONS_DLQ ?? 'wager-transactions-dlq.fifo',
      events: env.SQS_EVENTS_QUEUE ?? 'wager-events.fifo',
    },
    consumer: {
      enabled: bool(env, 'CONSUMER_ENABLED', true),
      name: env.CONSUMER_NAME ?? 'wager-transactions-consumer',
      waitTimeSeconds: int(env, 'SQS_WAIT_TIME_SECONDS', 10),
      visibilityTimeoutSeconds: int(env, 'SQS_VISIBILITY_TIMEOUT_SECONDS', 30),
      maxMessages: int(env, 'SQS_MAX_MESSAGES', 10),
      maxAttempts: int(env, 'SQS_MAX_ATTEMPTS', 5),
      retryBaseDelaySeconds: int(env, 'SQS_RETRY_BASE_DELAY_SECONDS', 2),
      retryMaxDelaySeconds: int(env, 'SQS_RETRY_MAX_DELAY_SECONDS', 60),
      shutdownGraceMs: int(env, 'SHUTDOWN_GRACE_MS', 20_000),
    },
    outbox: {
      enabled: bool(env, 'OUTBOX_RELAY_ENABLED', true),
      intervalMs: int(env, 'OUTBOX_INTERVAL_MS', 500),
      batchSize: int(env, 'OUTBOX_BATCH_SIZE', 100),
      retryBaseDelayMs: int(env, 'OUTBOX_RETRY_BASE_DELAY_MS', 1_000),
      retryMaxDelayMs: int(env, 'OUTBOX_RETRY_MAX_DELAY_MS', 60_000),
    },
    pendingReferences: {
      enabled: bool(env, 'PENDING_REFERENCE_WORKER_ENABLED', true),
      intervalMs: int(env, 'PENDING_REFERENCE_INTERVAL_MS', 1_000),
      batchSize: int(env, 'PENDING_REFERENCE_BATCH_SIZE', 20),
      leaseMs: int(env, 'PENDING_REFERENCE_LEASE_MS', 30_000),
      maxAttempts: int(env, 'PENDING_REFERENCE_MAX_ATTEMPTS', 10),
      baseDelayMs: int(env, 'PENDING_REFERENCE_BASE_DELAY_MS', 2_000),
      maxDelayMs: int(env, 'PENDING_REFERENCE_MAX_DELAY_MS', 300_000),
    },
    logLevel: ['debug', 'info', 'warn', 'error'].includes(logLevel) ? logLevel : 'info',
    faultInjection: env.FAULT_INJECTION || undefined,
  };
}
