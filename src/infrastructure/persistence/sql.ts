import type { EntityManager } from '@mikro-orm/postgresql';

type Param = string | number | boolean | Date | null | undefined;

/**
 * Thin wrapper over the transaction-bound EntityManager. All statements run on
 * the connection of the current `em.transactional()` block.
 * `undefined` parameters are sent as SQL NULL.
 */
export class Sql {
  constructor(private readonly em: EntityManager) {}

  async rows<T>(query: string, params: readonly Param[] = []): Promise<T[]> {
    return (await this.em.execute(query, normalize(params), 'all')) as T[];
  }

  async one<T>(query: string, params: readonly Param[] = []): Promise<T | undefined> {
    const rows = await this.rows<T>(query, params);
    return rows[0];
  }

  /** Returns the number of affected rows. */
  async run(query: string, params: readonly Param[] = []): Promise<number> {
    const result = (await this.em.execute(query, normalize(params), 'run')) as { affectedRows?: number };
    return result.affectedRows ?? 0;
  }
}

function normalize(params: readonly Param[]): unknown[] {
  return params.map((p) => (p === undefined ? null : p));
}
