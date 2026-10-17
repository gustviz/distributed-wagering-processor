import { IsolationLevel } from '@mikro-orm/core';
import type { MikroORM } from '@mikro-orm/postgresql';
import type { TransactionOptions, TransactionScope, UnitOfWork } from '../../application/ports';
import { translateDatabaseError } from './database-errors';
import { SqlTransactionScope } from './repositories';
import { Sql } from './sql';

export interface UnitOfWorkSettings {
  lockTimeoutMs: number;
  statementTimeoutMs: number;
}

/**
 * Transaction boundary. Each call forks a fresh EntityManager (no shared
 * identity map between requests) and runs the work inside
 * `em.transactional()`, so wallet, transaction, ledger, inbox and outbox
 * writes commit or roll back as one unit. Deferred integrity triggers run at
 * COMMIT, inside this same boundary.
 *
 * `lock_timeout` bounds how long a writer waits for a hot wallet's row lock;
 * expiring it surfaces as a retryable contention error instead of a hang.
 */
export class MikroOrmUnitOfWork implements UnitOfWork {
  constructor(
    private readonly orm: MikroORM,
    private readonly settings: UnitOfWorkSettings,
  ) {}

  async transactional<T>(work: (scope: TransactionScope) => Promise<T>, options?: TransactionOptions): Promise<T> {
    const em = this.orm.em.fork({ clear: true });
    try {
      return await em.transactional(
        async (txEm) => {
          const sql = new Sql(txEm);
          await sql.rows(
            "SELECT set_config('lock_timeout', ?, true), set_config('statement_timeout', ?, true), set_config('TimeZone', 'UTC', true)",
            [
              `${this.settings.lockTimeoutMs}ms`,
              `${this.settings.statementTimeoutMs}ms`,
            ],
          );
          return work(new SqlTransactionScope(sql));
        },
        {
          isolationLevel:
            options?.isolation === 'repeatable read' ? IsolationLevel.REPEATABLE_READ : IsolationLevel.READ_COMMITTED,
          readOnly: options?.readOnly ?? false,
        },
      );
    } catch (error) {
      throw translateDatabaseError(error);
    }
  }
}
