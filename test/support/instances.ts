import { mkdirSync } from 'node:fs';
import { join } from 'node:path';
import type { Subprocess } from 'bun';
import { testEnv } from './env';

const ROOT = join(import.meta.dir, '..', '..');
const LOG_DIR = join(ROOT, 'test', '.logs');

export interface Instance {
  name: string;
  url: string;
  proc: Subprocess;
  /** SIGKILL: no graceful shutdown, no cleanup — a crash. */
  kill(): Promise<void>;
  exited: Promise<number>;
}

function freePort(): number {
  const server = Bun.serve({ port: 0, fetch: () => new Response() });
  const port = server.port as number;
  server.stop(true);
  return port;
}

/**
 * Starts a REAL, separate OS process running the service (`bun run src/main.ts`),
 * i.e. its own connection pool, consumer, relay and worker — used for
 * multi-instance and crash tests. Logs go to test/.logs/<name>.log.
 */
export async function startInstance(name: string, env: Record<string, string> = {}, waitReady = true): Promise<Instance> {
  mkdirSync(LOG_DIR, { recursive: true });
  const port = freePort();
  // process.execPath = the real bun binary (a `bun` shim on PATH would spawn a child we could not kill).
  const proc = Bun.spawn([process.execPath, 'run', 'src/main.ts'], {
    cwd: ROOT,
    env: { ...process.env, ...testEnv({ PORT: String(port), INSTANCE_ID: name, ...env }) },
    stdout: Bun.file(join(LOG_DIR, `${name}.log`)),
    stderr: Bun.file(join(LOG_DIR, `${name}.err.log`)),
  });
  const instance: Instance = {
    name,
    url: `http://127.0.0.1:${port}`,
    proc,
    exited: proc.exited,
    kill: async () => {
      proc.kill('SIGKILL');
      await proc.exited;
    },
  };
  if (waitReady) await waitUntilLive(instance);
  return instance;
}

export async function waitUntilLive(instance: Instance, timeoutMs = 30_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (instance.proc.exitCode !== null) throw new Error(`${instance.name} exited with ${instance.proc.exitCode}`);
    try {
      const r = await fetch(`${instance.url}/health/live`);
      if (r.ok) return;
    } catch {
      // not listening yet
    }
    await Bun.sleep(150);
  }
  throw new Error(`${instance.name} did not become live`);
}

export async function postJson(url: string, body: unknown, headers: Record<string, string> = {}) {
  const response = await fetch(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...headers },
    body: JSON.stringify(body),
  });
  const text = await response.text();
  return { status: response.status, body: text ? (JSON.parse(text) as Record<string, unknown>) : {} };
}
