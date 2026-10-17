import type { Clock, FaultInjector, FaultPoint, IdGenerator } from '../application/ports';

export class SystemClock implements Clock {
  now(): Date {
    return new Date();
  }
}

/** UUIDv7: time-ordered, so primary-key inserts stay append-friendly in B-tree indexes. */
export class UuidV7Generator implements IdGenerator {
  next(): string {
    return Bun.randomUUIDv7();
  }
}

/**
 * Test seam for crash scenarios. `FAULT_INJECTION=<point>` makes the process
 * die abruptly the first time it reaches that point: no shutdown hooks, no SQS
 * ack, no commit of an open transaction (the OS closes its sockets).
 * Inert in production (env var unset).
 *
 * `process.exit` rather than `kill(self, SIGKILL)`: inside a container the app
 * is PID 1, and the kernel ignores signals PID 1 has no handler for — a
 * self-SIGKILL would leave the process hanging instead of crashing.
 */
export class EnvFaultInjector implements FaultInjector {
  constructor(private readonly armed: string | undefined) {}

  async checkpoint(name: FaultPoint): Promise<void> {
    if (this.armed !== name) return;
    process.stderr.write(`${JSON.stringify({ level: 'warn', msg: 'fault injection: crashing', point: name })}\n`);
    process.exit(137);
  }
}

export const noFaults: FaultInjector = { checkpoint: async () => undefined };
