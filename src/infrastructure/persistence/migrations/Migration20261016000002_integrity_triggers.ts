import { Migration } from '@mikro-orm/migrations';

/**
 * Invariants that CHECK constraints cannot express, enforced by triggers so
 * that no code path (including manual SQL) can violate them:
 *
 *  1. Append-only: ledger entries cannot be updated/deleted/truncated;
 *     wallets and wager transactions cannot be deleted.
 *  2. Terminal wager transactions (PROCESSED/REJECTED/FAILED) are immutable,
 *     and business columns of any transaction never change.
 *  3. Wallet identity is immutable; `version` moves +1 iff `balance` moves.
 *  4. A ledger entry must belong to a PROCESSED, balance-moving transaction of
 *     the same wallet/amount/currency, have the right direction, and chain onto
 *     the previous entry (balance_before == previous balance_after).
 *  5. At COMMIT (deferred): every wallet state (balance, version) is backed by
 *     the ledger entry of that version, and every ledger entry was applied to
 *     the wallet — i.e. balance changes and ledger entries are 1:1.
 */
export class Migration20261016000002_integrity_triggers extends Migration {
  override async up(): Promise<void> {
    // 1. Append-only / no-delete --------------------------------------------
    this.addSql(`
      CREATE FUNCTION forbid_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        RAISE EXCEPTION '% on % is not allowed (append-only)', TG_OP, TG_TABLE_NAME
          USING ERRCODE = 'integrity_constraint_violation';
      END;
      $$;
    `);
    this.addSql(`
      CREATE TRIGGER tg_ledger_append_only
        BEFORE UPDATE OR DELETE ON wallet_ledger_entries
        FOR EACH ROW EXECUTE FUNCTION forbid_mutation();
    `);
    this.addSql(`
      CREATE TRIGGER tg_ledger_no_truncate
        BEFORE TRUNCATE ON wallet_ledger_entries
        FOR EACH STATEMENT EXECUTE FUNCTION forbid_mutation();
    `);
    this.addSql(`
      CREATE TRIGGER tg_wager_tx_no_delete
        BEFORE DELETE ON wager_transactions
        FOR EACH ROW EXECUTE FUNCTION forbid_mutation();
    `);
    this.addSql(`
      CREATE TRIGGER tg_wager_tx_no_truncate
        BEFORE TRUNCATE ON wager_transactions
        FOR EACH STATEMENT EXECUTE FUNCTION forbid_mutation();
    `);
    this.addSql(`
      CREATE TRIGGER tg_wallets_no_delete
        BEFORE DELETE ON wallets
        FOR EACH ROW EXECUTE FUNCTION forbid_mutation();
    `);
    this.addSql(`
      CREATE TRIGGER tg_wallets_no_truncate
        BEFORE TRUNCATE ON wallets
        FOR EACH STATEMENT EXECUTE FUNCTION forbid_mutation();
    `);

    // 2. Wager transaction immutability ---------------------------------------
    this.addSql(`
      CREATE FUNCTION guard_wager_transaction_update() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF OLD.status IN ('PROCESSED','REJECTED','FAILED') THEN
          RAISE EXCEPTION 'wager transaction % is terminal (%) and immutable', OLD.id, OLD.status
            USING ERRCODE = 'integrity_constraint_violation';
        END IF;
        IF (NEW.id, NEW.provider_id, NEW.external_transaction_id, NEW.idempotency_key, NEW.payload_hash,
            NEW.wallet_id, NEW.player_id, NEW.round_id, NEW.game_id, NEW.kind, NEW.amount, NEW.currency,
            NEW.reference_external_transaction_id, NEW.created_at)
           IS DISTINCT FROM
           (OLD.id, OLD.provider_id, OLD.external_transaction_id, OLD.idempotency_key, OLD.payload_hash,
            OLD.wallet_id, OLD.player_id, OLD.round_id, OLD.game_id, OLD.kind, OLD.amount, OLD.currency,
            OLD.reference_external_transaction_id, OLD.created_at) THEN
          RAISE EXCEPTION 'business columns of wager transaction % are immutable', OLD.id
            USING ERRCODE = 'integrity_constraint_violation';
        END IF;
        RETURN NEW;
      END;
      $$;
    `);
    this.addSql(`
      CREATE TRIGGER tg_wager_tx_guard_update
        BEFORE UPDATE ON wager_transactions
        FOR EACH ROW EXECUTE FUNCTION guard_wager_transaction_update();
    `);

    // 3. Wallet identity + version discipline ----------------------------------
    this.addSql(`
      CREATE FUNCTION guard_wallet_update() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF (NEW.id, NEW.player_id, NEW.currency, NEW.created_at)
           IS DISTINCT FROM (OLD.id, OLD.player_id, OLD.currency, OLD.created_at) THEN
          RAISE EXCEPTION 'wallet % identity is immutable', OLD.id
            USING ERRCODE = 'integrity_constraint_violation';
        END IF;
        IF NEW.balance <> OLD.balance AND NEW.version <> OLD.version + 1 THEN
          RAISE EXCEPTION 'wallet % balance changed without version increment', OLD.id
            USING ERRCODE = 'integrity_constraint_violation';
        END IF;
        IF NEW.balance = OLD.balance AND NEW.version <> OLD.version THEN
          RAISE EXCEPTION 'wallet % version changed without balance change', OLD.id
            USING ERRCODE = 'integrity_constraint_violation';
        END IF;
        RETURN NEW;
      END;
      $$;
    `);
    this.addSql(`
      CREATE TRIGGER tg_wallets_guard_update
        BEFORE UPDATE ON wallets
        FOR EACH ROW EXECUTE FUNCTION guard_wallet_update();
    `);

    // 4. Ledger entry validity + chain ------------------------------------------
    this.addSql(`
      CREATE FUNCTION guard_ledger_insert() RETURNS trigger LANGUAGE plpgsql AS $$
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
    this.addSql(`
      CREATE TRIGGER tg_ledger_guard_insert
        BEFORE INSERT ON wallet_ledger_entries
        FOR EACH ROW EXECUTE FUNCTION guard_ledger_insert();
    `);

    // 5. Deferred cross-table checks (evaluated at COMMIT) ----------------------
    this.addSql(`
      CREATE FUNCTION check_wallet_backed_by_ledger() RETURNS trigger LANGUAGE plpgsql AS $$
      DECLARE
        w     RECORD;
        entry RECORD;
      BEGIN
        SELECT id, balance, version INTO w FROM wallets WHERE id = NEW.id;
        SELECT balance_after INTO entry
          FROM wallet_ledger_entries
         WHERE wallet_id = w.id AND wallet_version = w.version;
        IF FOUND THEN
          IF entry.balance_after <> w.balance THEN
            RAISE EXCEPTION 'wallet % balance differs from ledger at version %', w.id, w.version
              USING ERRCODE = 'integrity_constraint_violation';
          END IF;
        ELSIF NOT (w.version = 1 AND w.balance = 0) THEN
          RAISE EXCEPTION 'wallet % balance change at version % has no ledger entry', w.id, w.version
            USING ERRCODE = 'integrity_constraint_violation';
        END IF;
        RETURN NULL;
      END;
      $$;
    `);
    this.addSql(`
      CREATE CONSTRAINT TRIGGER tg_wallets_backed_by_ledger
        AFTER INSERT OR UPDATE ON wallets
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION check_wallet_backed_by_ledger();
    `);
    this.addSql(`
      CREATE FUNCTION check_ledger_entry_applied() RETURNS trigger LANGUAGE plpgsql AS $$
      DECLARE
        w RECORD;
      BEGIN
        SELECT balance, version INTO w FROM wallets WHERE id = NEW.wallet_id;
        IF w.version < NEW.wallet_version
           OR (w.version = NEW.wallet_version AND w.balance <> NEW.balance_after) THEN
          RAISE EXCEPTION 'ledger entry % was not applied to wallet %', NEW.id, NEW.wallet_id
            USING ERRCODE = 'integrity_constraint_violation';
        END IF;
        RETURN NULL;
      END;
      $$;
    `);
    this.addSql(`
      CREATE CONSTRAINT TRIGGER tg_ledger_entry_applied
        AFTER INSERT ON wallet_ledger_entries
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION check_ledger_entry_applied();
    `);
  }

  override async down(): Promise<void> {
    this.addSql('DROP TRIGGER IF EXISTS tg_ledger_entry_applied ON wallet_ledger_entries;');
    this.addSql('DROP TRIGGER IF EXISTS tg_wallets_backed_by_ledger ON wallets;');
    this.addSql('DROP TRIGGER IF EXISTS tg_ledger_guard_insert ON wallet_ledger_entries;');
    this.addSql('DROP TRIGGER IF EXISTS tg_wallets_guard_update ON wallets;');
    this.addSql('DROP TRIGGER IF EXISTS tg_wager_tx_guard_update ON wager_transactions;');
    this.addSql('DROP TRIGGER IF EXISTS tg_wallets_no_truncate ON wallets;');
    this.addSql('DROP TRIGGER IF EXISTS tg_wallets_no_delete ON wallets;');
    this.addSql('DROP TRIGGER IF EXISTS tg_wager_tx_no_truncate ON wager_transactions;');
    this.addSql('DROP TRIGGER IF EXISTS tg_wager_tx_no_delete ON wager_transactions;');
    this.addSql('DROP TRIGGER IF EXISTS tg_ledger_no_truncate ON wallet_ledger_entries;');
    this.addSql('DROP TRIGGER IF EXISTS tg_ledger_append_only ON wallet_ledger_entries;');
    this.addSql('DROP FUNCTION IF EXISTS check_ledger_entry_applied();');
    this.addSql('DROP FUNCTION IF EXISTS check_wallet_backed_by_ledger();');
    this.addSql('DROP FUNCTION IF EXISTS guard_ledger_insert();');
    this.addSql('DROP FUNCTION IF EXISTS guard_wallet_update();');
    this.addSql('DROP FUNCTION IF EXISTS guard_wager_transaction_update();');
    this.addSql('DROP FUNCTION IF EXISTS forbid_mutation();');
  }
}
