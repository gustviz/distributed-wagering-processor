import { AsyncLocalStorage } from 'node:async_hooks';
import type { LoggerService } from '@nestjs/common';
import type { AppLogger, LogFields } from '../../application/ports';

/** Correlation fields propagated through async boundaries (HTTP request, SQS message, worker tick). */
export interface LogContext {
  correlationId?: string;
  messageId?: string;
  transactionId?: string;
  walletId?: string;
  providerId?: string;
}

const storage = new AsyncLocalStorage<LogContext>();

export function runWithLogContext<T>(context: LogContext, fn: () => T): T {
  return storage.run({ ...storage.getStore(), ...context }, fn);
}

export function currentLogContext(): LogContext {
  return storage.getStore() ?? {};
}

const LEVELS = { debug: 10, info: 20, warn: 30, error: 40 } as const;
type Level = keyof typeof LEVELS;

/**
 * Keys that must never reach the logs. Callers already pass identifiers only;
 * this is a second line of defence against accidentally logging payloads.
 */
const REDACTED_KEYS = /^(amount|money|balance|payload|body|data|authorization|password|token|secret)/i;

/**
 * One JSON object per line on stdout. Every line carries the async context
 * (correlationId, messageId, transactionId, walletId, providerId when known).
 */
export class JsonLogger implements AppLogger, LoggerService {
  constructor(
    private readonly base: { service: string; instanceId: string },
    private readonly minLevel: Level = 'info',
    private readonly sink: (line: string) => void = (line) => process.stdout.write(`${line}\n`),
  ) {}

  debug(message: string, fields?: LogFields): void {
    this.write('debug', message, fields);
  }
  info(message: string, fields?: LogFields): void {
    this.write('info', message, fields);
  }
  warn(message: string, fields?: LogFields): void {
    this.write('warn', message, fields);
  }
  /** Port signature `(message, fields, err)`; also accepts Nest's `(message, stack?, context?)`. */
  error(message: unknown, fieldsOrStack?: unknown, errOrContext?: unknown): void {
    if (typeof fieldsOrStack === 'string' || fieldsOrStack === undefined) {
      const context = typeof errOrContext === 'string' ? errOrContext : undefined;
      this.write('error', String(message), { context, stack: fieldsOrStack });
      return;
    }
    this.write('error', String(message), fieldsOrStack as LogFields, errOrContext);
  }

  // ---- Nest LoggerService --------------------------------------------------
  log(message: unknown, ...optional: unknown[]): void {
    this.write('info', String(message), nestContext(optional));
  }
  verbose(message: unknown, ...optional: unknown[]): void {
    this.write('debug', String(message), nestContext(optional));
  }
  fatal(message: unknown, ...optional: unknown[]): void {
    this.write('error', String(message), nestContext(optional));
  }

  private write(level: Level, msg: string, fields?: LogFields, err?: unknown): void {
    if (LEVELS[level] < LEVELS[this.minLevel]) return;
    const safe: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(fields ?? {})) {
      if (value === undefined) continue;
      safe[key] = REDACTED_KEYS.test(key) ? '[redacted]' : value;
    }
    const line: Record<string, unknown> = {
      time: new Date().toISOString(),
      level,
      msg,
      service: this.base.service,
      instanceId: this.base.instanceId,
      ...currentLogContext(),
      ...safe,
    };
    if (err !== undefined) line.error = serializeError(err);
    this.sink(JSON.stringify(line));
  }
}

function nestContext(optional: unknown[]): LogFields {
  const context = optional.at(-1);
  return typeof context === 'string' ? { context } : {};
}

/** Driver errors embed the full SQL with bound values ("<sql> - <reason>"): keep only the reason. */
const SQL_PREFIX = /^\s*(select|insert|update|delete|with)\b[\s\S]* - /i;

function serializeError(err: unknown): Record<string, unknown> {
  if (err instanceof Error) {
    const code = (err as { code?: unknown }).code;
    return {
      name: err.name,
      message: err.message.replace(SQL_PREFIX, '[sql redacted] - '),
      ...(typeof code === 'string' ? { code } : {}),
      // Frames only: the first stack line repeats the (possibly SQL-bearing) message.
      stack: err.stack
        ?.split('\n')
        .filter((l) => l.trim().startsWith('at '))
        .slice(0, 8)
        .join('\n'),
    };
  }
  return { message: '[non-error thrown]' };
}
