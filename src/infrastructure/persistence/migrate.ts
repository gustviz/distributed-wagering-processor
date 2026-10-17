import { MikroORM } from '@mikro-orm/postgresql';
import { loadConfig } from '../../config/app-config';
import { ormConfig } from './mikro-orm.config';

/**
 * Usage:
 *   bun run src/infrastructure/persistence/migrate.ts up         # apply all pending
 *   bun run src/infrastructure/persistence/migrate.ts down       # revert the last one
 *   bun run src/infrastructure/persistence/migrate.ts down:all   # revert everything
 *   bun run src/infrastructure/persistence/migrate.ts status
 */
export async function runMigrations(url: string, command: string): Promise<string[]> {
  const orm = await MikroORM.init(ormConfig({ url, poolMax: 2 }));
  try {
    const migrator = orm.getMigrator();
    switch (command) {
      case 'up':
        return (await migrator.up()).map((m) => m.name);
      case 'down':
        return (await migrator.down()).map((m) => m.name);
      case 'down:all':
        return (await migrator.down({ to: 0 })).map((m) => m.name);
      case 'status': {
        const executed = (await migrator.getExecutedMigrations()).map((m) => `executed: ${m.name}`);
        const pending = (await migrator.getPendingMigrations()).map((m) => `pending:  ${m.name}`);
        return [...executed, ...pending];
      }
      default:
        throw new Error(`Unknown migration command "${command}"`);
    }
  } finally {
    await orm.close(true);
  }
}

if (import.meta.main) {
  const command = process.argv[2] ?? 'up';
  runMigrations(loadConfig().database.url, command)
    .then((names) => {
      console.log(JSON.stringify({ level: 'info', msg: `migrate ${command}`, migrations: names }));
      process.exit(0);
    })
    .catch((error: unknown) => {
      console.error(JSON.stringify({ level: 'error', msg: 'migration failed', error: String(error) }));
      process.exit(1);
    });
}
