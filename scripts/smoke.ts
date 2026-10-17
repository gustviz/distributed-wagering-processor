/**
 * End-to-end smoke test against a running instance (default: the nginx load balancer).
 *
 *   bun run smoke                      # http://localhost:3000
 *   bun run smoke http://localhost:3001
 *
 * Walks through the main flows and checks every response; exits 1 on any mismatch.
 * Pure Bun (no bash/curl), so it behaves the same in cmd, PowerShell and Git Bash.
 */
const base = (process.argv[2] ?? process.env.BASE_URL ?? 'http://localhost:3000').replace(/\/$/, '');
const player = `smoke-${Date.now().toString(36)}`;
let failures = 0;
let walletId = '';

type Json = Record<string, any>;

async function call(method: string, path: string, body?: unknown, key?: string): Promise<{ status: number; body: Json }> {
  const response = await fetch(`${base}${path}`, {
    method,
    headers: { 'content-type': 'application/json', ...(key ? { 'Idempotency-Key': key } : {}) },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await response.text();
  return { status: response.status, body: text ? (JSON.parse(text) as Json) : {} };
}

function check(label: string, ok: boolean, detail: string): void {
  if (!ok) failures++;
  console.log(`${ok ? '\x1b[32mPASS\x1b[0m' : '\x1b[31mFAIL\x1b[0m'}  ${label.padEnd(34)} ${detail}`);
}

function wager(kind: string, ext: string, amount: string, reference?: string) {
  return {
    providerId: 'smoke',
    externalTransactionId: `${player}-${ext}`,
    playerId: player,
    walletId,
    roundId: 'r1',
    gameId: 'g1',
    kind,
    money: { amount, currency: 'BRL' },
    ...(reference ? { referenceExternalTransactionId: `${player}-${reference}` } : {}),
  };
}

async function submit(label: string, payload: ReturnType<typeof wager>, expect: { status: number; balance?: string; code?: string; replay?: boolean }) {
  const r = await call('POST', '/wagering/transactions', payload, `smoke:${payload.externalTransactionId}`);
  const code = r.body.failureCode ?? r.body.error?.code;
  const ok =
    r.status === expect.status &&
    (expect.balance === undefined || r.body.balance?.amount === expect.balance) &&
    (expect.code === undefined || code === expect.code) &&
    (expect.replay === undefined || r.body.idempotentReplay === expect.replay);
  check(label, ok, `${r.status} ${r.body.status ?? ''} ${code ?? ''} ${r.body.balance?.amount ?? ''}${r.body.idempotentReplay ? ' (replay)' : ''}`);
  return r;
}

async function main(): Promise<void> {
  console.log(`Smoke test against ${base}\n`);
  const ready = await call('GET', '/health/ready');
  check('readiness', ready.status === 200, `${ready.status} ${JSON.stringify(ready.body.checks ?? ready.body)}`);

  const wallet = await call('POST', '/wallets', { playerId: player, initialBalance: { amount: '100.00', currency: 'BRL' } });
  walletId = wallet.body.id;
  check('create wallet 100.00', wallet.status === 201 && wallet.body.version === 1, `${wallet.status} ${walletId}`);

  await submit('BET 80.00', wager('BET', 'b1', '80.00'), { status: 200, balance: '20.00' });
  await submit('same BET again (replay)', wager('BET', 'b1', '80.00'), { status: 200, balance: '20.00', replay: true });
  await submit('same key, other amount', wager('BET', 'b1', '81.00'), { status: 409, code: 'IDEMPOTENCY_KEY_CONFLICT' });
  await submit('BET 80.00 without funds', wager('BET', 'b2', '80.00'), { status: 422, code: 'INSUFFICIENT_FUNDS' });
  await submit('ROLLBACK before its BET', wager('ROLLBACK', 'rb3', '10.00', 'b3'), { status: 202 });
  await submit('the referenced BET 10.00', wager('BET', 'b3', '10.00'), { status: 200, balance: '10.00' });
  await submit('REFUND of BET 80.00', wager('REFUND', 'rf1', '80.00', 'b1'), { status: 200, balance: '90.00' });
  await submit('WIN 5.00', wager('WIN', 'w1', '5.00', 'b1'), { status: 200, balance: '95.00' });
  await submit('LOSS', wager('LOSS', 'l1', '0.00'), { status: 200, balance: '95.00' });
  await submit('invalid amount 1e3', wager('BET', 'x', '1e3'), { status: 400, code: 'INVALID_MONEY' });

  // The early ROLLBACK is resolved in the background once its BET exists.
  let rollback: { status: number; body: Json } = { status: 0, body: {} };
  for (let i = 0; i < 20; i++) {
    rollback = await call('GET', `/providers/smoke/wagering/transactions/${player}-rb3`);
    if (rollback.body.status === 'PROCESSED') break;
    await Bun.sleep(500);
  }
  check('early ROLLBACK resolved', rollback.body.status === 'PROCESSED', `${rollback.status} ${rollback.body.status}`);

  const final = await call('GET', `/wallets/${walletId}`);
  check('final balance', final.body.balance?.amount === '105.00', `${final.body.balance?.amount} (version ${final.body.version})`);
  const ledger = await call('GET', `/wallets/${walletId}/ledger?limit=3`);
  check('ledger page', ledger.status === 200 && ledger.body.items?.length === 3, `${ledger.status} ${ledger.body.items?.length} items, nextCursor=${ledger.body.nextCursor ? 'yes' : 'no'}`);
  const recon = await call('POST', `/wallets/${walletId}/reconciliation`);
  check('reconciliation', recon.body.consistent === true, `consistent=${recon.body.consistent} entries=${recon.body.checkedEntries}`);

  console.log(failures === 0 ? '\n\x1b[32mAll checks passed.\x1b[0m' : `\n\x1b[31m${failures} check(s) failed.\x1b[0m`);
  process.exit(failures === 0 ? 0 : 1);
}

main().catch((error: unknown) => {
  console.error(`\x1b[31mSmoke test could not run: ${error instanceof Error ? error.message : String(error)}\x1b[0m`);
  console.error(`Is the stack up? Try: docker compose up -d`);
  process.exit(1);
});
