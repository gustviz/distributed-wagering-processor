import { Migration } from '@mikro-orm/migrations';

/**
 * Core tables. Every invariant that can be declared is declared here:
 * uniqueness, non-negativity, ledger arithmetic, status/field coherence and
 * "a reference is reversed at most once" (partial unique index).
 *
 * Money: `numeric(20,2)` (exact decimal, 18 integer digits) + a separate
 * ISO-4217 `currency` column; rehydrated as `Money` by the mapper.
 *
 * NOTE: no `?` characters in this SQL — the query layer treats them as bindings.
 */
export class Migration20261016000001_initial_schema extends Migration {
  override async up(): Promise<void> {
    this.addSql(`
      CREATE TABLE wallets (
        id          uuid           PRIMARY KEY,
        player_id   text           NOT NULL,
        currency    text           NOT NULL,
        balance     numeric(20,2)  NOT NULL,
        version     integer        NOT NULL,
        created_at  timestamptz    NOT NULL,
        updated_at  timestamptz    NOT NULL,
        CONSTRAINT uq_wallets_player_currency   UNIQUE (player_id, currency),
        CONSTRAINT ck_wallets_balance_non_negative CHECK (balance >= 0),
        CONSTRAINT ck_wallets_version_positive  CHECK (version >= 1),
        CONSTRAINT ck_wallets_currency_format   CHECK (currency ~ '^[A-Z]{3}$'),
        CONSTRAINT ck_wallets_player_id         CHECK (length(player_id) BETWEEN 1 AND 128)
      );
    `);

    this.addSql(`
      CREATE TABLE wager_transactions (
        id                                uuid           PRIMARY KEY,
        provider_id                       text           NOT NULL,
        external_transaction_id           text           NOT NULL,
        idempotency_key                   text           NOT NULL,
        payload_hash                      text           NOT NULL,
        wallet_id                         uuid           NOT NULL REFERENCES wallets (id),
        player_id                         text           NOT NULL,
        round_id                          text           NOT NULL,
        game_id                           text           NOT NULL,
        kind                              text           NOT NULL,
        amount                            numeric(20,2)  NOT NULL,
        currency                          text           NOT NULL,
        reference_external_transaction_id text,
        reference_transaction_id          uuid           REFERENCES wager_transactions (id),
        status                            text           NOT NULL,
        failure_code                      text,
        observed_balance                  numeric(20,2),
        observed_currency                 text,
        reference_attempts                integer        NOT NULL DEFAULT 0,
        next_attempt_at                   timestamptz,
        created_at                        timestamptz    NOT NULL,
        updated_at                        timestamptz    NOT NULL,
        processed_at                      timestamptz,

        -- Idempotency: the key is the source of truth; the provider's id is unique per provider.
        CONSTRAINT uq_wager_tx_idempotency_key   UNIQUE (idempotency_key),
        CONSTRAINT uq_wager_tx_provider_external UNIQUE (provider_id, external_transaction_id),

        CONSTRAINT ck_wager_tx_kind   CHECK (kind IN ('OPENING','BET','WIN','LOSS','REFUND','ROLLBACK')),
        CONSTRAINT ck_wager_tx_status CHECK (status IN ('PENDING','PENDING_REFERENCE','PROCESSED','REJECTED','FAILED')),
        CONSTRAINT ck_wager_tx_amount CHECK (amount >= 0 AND (kind = 'LOSS' OR amount > 0)),
        CONSTRAINT ck_wager_tx_currency_format CHECK (currency ~ '^[A-Z]{3}$'),
        CONSTRAINT ck_wager_tx_payload_hash CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
        CONSTRAINT ck_wager_tx_reference_required
          CHECK (kind NOT IN ('REFUND','ROLLBACK') OR reference_external_transaction_id IS NOT NULL),
        CONSTRAINT ck_wager_tx_reference_allowed
          CHECK (kind IN ('WIN','LOSS','REFUND','ROLLBACK') OR reference_external_transaction_id IS NULL),
        CONSTRAINT ck_wager_tx_opening_internal CHECK ((kind = 'OPENING') = (provider_id = 'internal')),
        CONSTRAINT ck_wager_tx_failure_code
          CHECK ((status IN ('REJECTED','FAILED')) = (failure_code IS NOT NULL)),
        CONSTRAINT ck_wager_tx_processed_at CHECK ((status = 'PROCESSED') = (processed_at IS NOT NULL)),
        CONSTRAINT ck_wager_tx_pending_schedule
          CHECK ((status = 'PENDING_REFERENCE') = (next_attempt_at IS NOT NULL)),
        CONSTRAINT ck_wager_tx_resolved_reference
          CHECK (reference_transaction_id IS NULL OR status = 'PROCESSED'),
        CONSTRAINT ck_wager_tx_observed_balance CHECK (observed_balance IS NULL OR observed_balance >= 0),
        CONSTRAINT ck_wager_tx_observed_currency CHECK ((observed_balance IS NULL) = (observed_currency IS NULL)),
        CONSTRAINT ck_wager_tx_attempts CHECK (reference_attempts >= 0)
      );
    `);
    // A transaction is reversed (REFUND or ROLLBACK) at most once. Stricter than
    // "once per operation type": a BET both refunded and rolled back would be a double credit.
    this.addSql(`
      CREATE UNIQUE INDEX uq_wager_tx_single_reversal
        ON wager_transactions (reference_transaction_id)
        WHERE kind IN ('REFUND','ROLLBACK') AND status = 'PROCESSED';
    `);
    this.addSql(`
      CREATE INDEX ix_wager_tx_pending_due
        ON wager_transactions (next_attempt_at)
        WHERE status = 'PENDING_REFERENCE';
    `);
    this.addSql(`
      CREATE INDEX ix_wager_tx_pending_dependents
        ON wager_transactions (provider_id, reference_external_transaction_id)
        WHERE status = 'PENDING_REFERENCE';
    `);
    this.addSql(`CREATE INDEX ix_wager_tx_wallet ON wager_transactions (wallet_id, created_at);`);
    this.addSql(`CREATE INDEX ix_wager_tx_reference ON wager_transactions (reference_transaction_id) WHERE reference_transaction_id IS NOT NULL;`);

    this.addSql(`
      CREATE TABLE wallet_ledger_entries (
        id              uuid           PRIMARY KEY,
        wallet_id       uuid           NOT NULL REFERENCES wallets (id),
        transaction_id  uuid           NOT NULL REFERENCES wager_transactions (id),
        direction       text           NOT NULL,
        amount          numeric(20,2)  NOT NULL,
        currency        text           NOT NULL,
        balance_before  numeric(20,2)  NOT NULL,
        balance_after   numeric(20,2)  NOT NULL,
        wallet_version  integer        NOT NULL,
        created_at      timestamptz    NOT NULL,
        -- At most one entry per (transaction, wallet).
        CONSTRAINT uq_ledger_transaction_wallet UNIQUE (transaction_id, wallet_id),
        -- Dense, gap-free version chain per wallet; also the pagination key.
        CONSTRAINT uq_ledger_wallet_version UNIQUE (wallet_id, wallet_version),
        CONSTRAINT ck_ledger_direction CHECK (direction IN ('DEBIT','CREDIT')),
        CONSTRAINT ck_ledger_amount_positive CHECK (amount > 0),
        CONSTRAINT ck_ledger_balances_non_negative CHECK (balance_before >= 0 AND balance_after >= 0),
        CONSTRAINT ck_ledger_version_positive CHECK (wallet_version >= 1),
        CONSTRAINT ck_ledger_currency_format CHECK (currency ~ '^[A-Z]{3}$'),
        CONSTRAINT ck_ledger_arithmetic CHECK (
          (direction = 'CREDIT' AND balance_after = balance_before + amount) OR
          (direction = 'DEBIT'  AND balance_after = balance_before - amount)
        )
      );
    `);

    this.addSql(`
      CREATE TABLE inbox_messages (
        consumer_name  text         NOT NULL,
        message_id     text         NOT NULL,
        payload_hash   text         NOT NULL,
        received_at    timestamptz  NOT NULL,
        processed_at   timestamptz,
        CONSTRAINT pk_inbox_messages PRIMARY KEY (consumer_name, message_id)
      );
    `);

    this.addSql(`
      CREATE TABLE outbox_messages (
        seq              bigint       GENERATED ALWAYS AS IDENTITY,
        id               uuid         PRIMARY KEY,
        aggregate_id     text         NOT NULL,
        event_type       text         NOT NULL,
        payload          jsonb        NOT NULL,
        occurred_at      timestamptz  NOT NULL,
        attempts         integer      NOT NULL DEFAULT 0,
        next_attempt_at  timestamptz,
        published_at     timestamptz,
        last_error       text,
        CONSTRAINT uq_outbox_seq UNIQUE (seq),
        CONSTRAINT ck_outbox_attempts CHECK (attempts >= 0)
      );
    `);
    this.addSql(`
      CREATE INDEX ix_outbox_unpublished ON outbox_messages (seq) WHERE published_at IS NULL;
    `);
  }

  override async down(): Promise<void> {
    this.addSql('DROP TABLE IF EXISTS outbox_messages;');
    this.addSql('DROP TABLE IF EXISTS inbox_messages;');
    this.addSql('DROP TABLE IF EXISTS wallet_ledger_entries;');
    this.addSql('DROP TABLE IF EXISTS wager_transactions;');
    this.addSql('DROP TABLE IF EXISTS wallets;');
  }
}
