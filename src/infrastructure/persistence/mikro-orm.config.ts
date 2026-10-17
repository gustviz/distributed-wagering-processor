import { Migrator } from '@mikro-orm/migrations';
import { defineConfig, PostgreSqlDriver } from '@mikro-orm/postgresql';
import { Migration20261016000001_initial_schema } from './migrations/Migration20261016000001_initial_schema';
import { Migration20261016000002_integrity_triggers } from './migrations/Migration20261016000002_integrity_triggers';
import { Migration20261016000003_ledger_version_continuity } from './migrations/Migration20261016000003_ledger_version_continuity';

export interface OrmConfigInput {
  url: string;
  poolMax: number;
  debug?: boolean;
}

/**
 * MikroORM is used for connection pooling, transaction demarcation
 * (`em.transactional` with explicit isolation level) and versioned, reversible
 * migrations. The schema is owned by hand-written migrations (constraints and
 * triggers are part of the design), never by the schema generator.
 *
 * Migrations are listed explicitly (no filesystem discovery), which keeps them
 * working identically under Bun, in tests and in the container image.
 */
export function ormConfig(input: OrmConfigInput): ReturnType<typeof defineConfig> {
  return defineConfig({
    driver: PostgreSqlDriver,
    clientUrl: input.url,
    entities: [],
    discovery: { warnWhenNoEntities: false },
    ensureDatabase: false,
    pool: { min: 0, max: input.poolMax },
    // Fail fast when PostgreSQL is unreachable (defaults: no connect timeout, long pool wait),
    // so callers get a retryable 503 / SQS backoff within seconds instead of hanging requests.
    driverOptions: {
      acquireConnectionTimeout: 5_000,
      connection: { connectionTimeoutMillis: 3_000 },
    },
    forceUtcTimezone: true,
    allowGlobalContext: false,
    debug: input.debug ?? false,
    extensions: [Migrator],
    migrations: {
      tableName: 'mikro_orm_migrations',
      transactional: true,
      allOrNothing: true,
      snapshot: false,
      migrationsList: [
        { name: 'Migration20261016000001_initial_schema', class: Migration20261016000001_initial_schema },
        { name: 'Migration20261016000002_integrity_triggers', class: Migration20261016000002_integrity_triggers },
        {
          name: 'Migration20261016000003_ledger_version_continuity',
          class: Migration20261016000003_ledger_version_continuity,
        },
      ],
    },
  });
}
