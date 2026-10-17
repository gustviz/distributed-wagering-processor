import { Migration } from '@mikro-orm/migrations';

/**
 * Ledger versions must be consecutive, not merely increasing.
 *
 * Migration …0002 only required `wallet_version > previous`. A transaction that
 * moved the wallet twice (v3, v4) but wrote a ledger entry for v4 alone passed
 * every check, leaving the v3 balance change without an entry. Now:
 *
 *  - with a previous entry:  wallet_version = previous + 1, and the entry is
 *    not an OPENING (an OPENING can only be first);
 *  - first entry of a wallet: version 1 for the OPENING credit of a funded
 *    wallet; version 2 otherwise (a wallet opened empty is at version 1 with
 *    no entry, so its first movement produces version 2).
 *
 * Together with the deferred "wallet backed by ledger" check, every wallet
 * version after the first has exactly one ledger entry.
 */
export class Migration20261016000003_ledger_version_continuity extends Migration {
  override async up(): Promise<void> {
    this.addSql(`
      CREATE OR REPLACE FUNCTION guard_ledger_insert() RETURNS trigger LANGUAGE plpgsql AS $$
      DECLARE
        tx         wager_transactions%ROWTYPE;
        ref_kind   text;
        expected   text;
        w_currency text;
        prev       RECORD;
      BEGIN
        SELECT currency INTO w_currency FROM wallets WHERE id = NEW.wallet_id;
        IF w_currency IS DISTINCT FROM NEW.currency THEN
          RAISE EXCEPTION 'ledger entry currency differs from wallet currency'
            USING ERRCODE = 'integrity_constraint_violation';
        END IF;

        SELECT * INTO tx FROM wager_transactions WHERE id = NEW.transaction_id;
        IF tx.status IS DISTINCT FROM 'PROCESSED' OR tx.kind = 'LOSS'
           OR tx.wallet_id <> NEW.wallet_id OR tx.amount <> NEW.amount OR tx.currency <> NEW.currency THEN
          RAISE EXCEPTION 'ledger entry does not match a processed balance-moving transaction'
            USING ERRCODE = 'integrity_constraint_violation';
        END IF;

        IF tx.kind = 'BET' THEN
          expected := 'DEBIT';
        ELSIF tx.kind IN ('WIN','REFUND','OPENING') THEN
          expected := 'CREDIT';
        ELSE -- ROLLBACK: inverse of the reference
          SELECT kind INTO ref_kind FROM wager_transactions WHERE id = tx.reference_transaction_id;
          expected := CASE WHEN ref_kind = 'BET' THEN 'CREDIT' ELSE 'DEBIT' END;
        END IF;
        IF NEW.direction <> expected THEN
          RAISE EXCEPTION 'ledger direction % does not match transaction kind %', NEW.direction, tx.kind
            USING ERRCODE = 'integrity_constraint_violation';
        END IF;

        SELECT wallet_version, balance_after INTO prev
          FROM wallet_ledger_entries
         WHERE wallet_id = NEW.wallet_id
         ORDER BY wallet_version DESC
         LIMIT 1;
        IF FOUND THEN
          IF tx.kind = 'OPENING' THEN
            RAISE EXCEPTION 'OPENING must be the first ledger entry of wallet %', NEW.wallet_id
              USING ERRCODE = 'integrity_constraint_violation';
          END IF;
          IF NEW.wallet_version <> prev.wallet_version + 1 THEN
            RAISE EXCEPTION 'ledger version gap for wallet %: expected %, got %',
              NEW.wallet_id, prev.wallet_version + 1, NEW.wallet_version
              USING ERRCODE = 'integrity_constraint_violation';
          END IF;
          IF NEW.balance_before <> prev.balance_after THEN
            RAISE EXCEPTION 'ledger chain broken for wallet %', NEW.wallet_id
              USING ERRCODE = 'integrity_constraint_violation';
          END IF;
        ELSE
          IF NEW.balance_before <> 0 THEN
            RAISE EXCEPTION 'first ledger entry of wallet % must start from zero', NEW.wallet_id
              USING ERRCODE = 'integrity_constraint_violation';
          END IF;
          IF tx.kind = 'OPENING' AND NEW.wallet_version <> 1 THEN
            RAISE EXCEPTION 'OPENING ledger entry of wallet % must be version 1, got %', NEW.wallet_id, NEW.wallet_version
              USING ERRCODE = 'integrity_constraint_violation';
          END IF;
          IF tx.kind <> 'OPENING' AND NEW.wallet_version <> 2 THEN
            RAISE EXCEPTION 'first ledger entry of wallet % must be version 2, got % (a wallet opened empty is version 1 without an entry)',
              NEW.wallet_id, NEW.wallet_version
              USING ERRCODE = 'integrity_constraint_violation';
          END IF;
        END IF;
        RETURN NEW;
      END;
      $$;
    `);
  }

  /** Restores the function exactly as migration …0002 created it. */
  override async down(): Promise<void> {
    this.addSql(`
      CREATE OR REPLACE FUNCTION guard_ledger_insert() RETURNS trigger LANGUAGE plpgsql AS $$
      DECLARE
        tx         wager_transactions%ROWTYPE;
        ref_kind   text;
        expected   text;
        w_currency text;
        prev       RECORD;
      BEGIN
        SELECT currency INTO w_currency FROM wallets WHERE id = NEW.wallet_id;
        IF w_currency IS DISTINCT FROM NEW.currency THEN
          RAISE EXCEPTION 'ledger entry currency differs from wallet currency'
            USING ERRCODE = 'integrity_constraint_violation';
        END IF;

        SELECT * INTO tx FROM wager_transactions WHERE id = NEW.transaction_id;
        IF tx.status IS DISTINCT FROM 'PROCESSED' OR tx.kind = 'LOSS'
           OR tx.wallet_id <> NEW.wallet_id OR tx.amount <> NEW.amount OR tx.currency <> NEW.currency THEN
          RAISE EXCEPTION 'ledger entry does not match a processed balance-moving transaction'
            USING ERRCODE = 'integrity_constraint_violation';
        END IF;

        IF tx.kind = 'BET' THEN
          expected := 'DEBIT';
        ELSIF tx.kind IN ('WIN','REFUND','OPENING') THEN
          expected := 'CREDIT';
        ELSE -- ROLLBACK: inverse of the reference
          SELECT kind INTO ref_kind FROM wager_transactions WHERE id = tx.reference_transaction_id;
          expected := CASE WHEN ref_kind = 'BET' THEN 'CREDIT' ELSE 'DEBIT' END;
        END IF;
        IF NEW.direction <> expected THEN
          RAISE EXCEPTION 'ledger direction % does not match transaction kind %', NEW.direction, tx.kind
            USING ERRCODE = 'integrity_constraint_violation';
        END IF;

        SELECT wallet_version, balance_after INTO prev
          FROM wallet_ledger_entries
         WHERE wallet_id = NEW.wallet_id
         ORDER BY wallet_version DESC
         LIMIT 1;
        IF FOUND THEN
          IF NEW.wallet_version <= prev.wallet_version OR NEW.balance_before <> prev.balance_after THEN
            RAISE EXCEPTION 'ledger chain broken for wallet %', NEW.wallet_id
              USING ERRCODE = 'integrity_constraint_violation';
          END IF;
        ELSIF NEW.balance_before <> 0 THEN
          RAISE EXCEPTION 'first ledger entry of wallet % must start from zero', NEW.wallet_id
            USING ERRCODE = 'integrity_constraint_violation';
        END IF;
        RETURN NEW;
      END;
      $$;
    `);
  }
}
