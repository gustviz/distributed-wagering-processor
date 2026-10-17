import { type AppConfig, loadConfig } from '../../src/config/app-config';

/**
 * Integration/concurrency suites run against REAL PostgreSQL and SQS
 * (docker compose: `docker compose up -d postgres localstack`).
 * Defaults match docker-compose.yml; override with env vars.
 */
export const TEST_DATABASE_URL =
  process.env.TEST_DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5433/wagering_test';
export const AWS_ENDPOINT_URL = process.env.AWS_ENDPOINT_URL ?? 'http://localhost:4566';

export function testEnv(overrides: Record<string, string> = {}): Record<string, string> {
  return {
    DATABASE_URL: TEST_DATABASE_URL,
    AWS_ENDPOINT_URL,
    AWS_REGION: 'us-east-1',
    AWS_ACCESS_KEY_ID: 'test',
    AWS_SECRET_ACCESS_KEY: 'test',
    LOG_LEVEL: process.env.TEST_LOG_LEVEL ?? 'error',
    SQS_WAIT_TIME_SECONDS: '1',
    SQS_VISIBILITY_TIMEOUT_SECONDS: '5',
    SQS_RETRY_BASE_DELAY_SECONDS: '1',
    SQS_RETRY_MAX_DELAY_SECONDS: '2',
    SQS_MAX_ATTEMPTS: '3',
    OUTBOX_INTERVAL_MS: '100',
    PENDING_REFERENCE_INTERVAL_MS: '200',
    PENDING_REFERENCE_BASE_DELAY_MS: '300',
    PENDING_REFERENCE_MAX_DELAY_MS: '1000',
    PENDING_REFERENCE_MAX_ATTEMPTS: '20',
    PENDING_REFERENCE_LEASE_MS: '2000',
    SHUTDOWN_GRACE_MS: '5000',
    ...overrides,
  };
}

export function testConfig(overrides: Record<string, string> = {}): AppConfig {
  return loadConfig(testEnv(overrides) as NodeJS.ProcessEnv);
}
