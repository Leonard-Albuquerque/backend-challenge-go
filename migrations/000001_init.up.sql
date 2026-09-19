-- Wallets: one per (player, currency). Balance in minor units (BIGINT), never
-- negative; version starts at 1 and only changes with a balance change.
CREATE TABLE wallets (
    id            UUID PRIMARY KEY,
    player_id     UUID        NOT NULL,
    currency      CHAR(3)     NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    balance_minor BIGINT      NOT NULL CHECK (balance_minor >= 0),
    version       BIGINT      NOT NULL CHECK (version >= 1),
    created_at    TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL,
    CONSTRAINT wallets_player_currency_unique UNIQUE (player_id, currency)
);

-- Wager transactions: internal OPENING and external provider operations share
-- the table; the origin column drives the nullability rules below.
CREATE TABLE wager_transactions (
    id                                UUID PRIMARY KEY,
    origin                            TEXT        NOT NULL CHECK (origin IN ('INTERNAL', 'EXTERNAL')),
    provider_id                       TEXT,
    external_transaction_id           TEXT,
    idempotency_key                   TEXT,
    payload_hash                      TEXT,
    wallet_id                         UUID        NOT NULL REFERENCES wallets (id),
    player_id                         UUID        NOT NULL,
    round_id                          TEXT,
    game_id                           TEXT,
    kind                              TEXT        NOT NULL CHECK (kind IN ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK')),
    amount_minor                      BIGINT      NOT NULL CHECK (amount_minor >= 0),
    currency                          CHAR(3)     NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    reference_external_transaction_id TEXT,
    reference_transaction_id          UUID        REFERENCES wager_transactions (id),
    status                            TEXT        NOT NULL CHECK (status IN ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED')),
    failure_code                      TEXT,
    result_balance_minor              BIGINT      CHECK (result_balance_minor IS NULL OR result_balance_minor >= 0),
    reference_attempts                INTEGER     NOT NULL DEFAULT 0 CHECK (reference_attempts >= 0),
    next_reference_retry_at           TIMESTAMPTZ,
    correlation_id                    TEXT,
    created_at                        TIMESTAMPTZ NOT NULL,
    updated_at                        TIMESTAMPTZ NOT NULL,
    processed_at                      TIMESTAMPTZ,

    -- Internal vs external metadata.
    CONSTRAINT wager_transactions_origin_kind CHECK (
        (origin = 'INTERNAL' AND kind = 'OPENING') OR (origin = 'EXTERNAL' AND kind <> 'OPENING')
    ),
    CONSTRAINT wager_transactions_external_metadata CHECK (
        (origin = 'EXTERNAL'
            AND provider_id IS NOT NULL AND external_transaction_id IS NOT NULL
            AND idempotency_key IS NOT NULL AND payload_hash IS NOT NULL
            AND round_id IS NOT NULL AND game_id IS NOT NULL)
        OR
        (origin = 'INTERNAL'
            AND provider_id IS NULL AND external_transaction_id IS NULL
            AND idempotency_key IS NULL AND payload_hash IS NULL
            AND round_id IS NULL AND game_id IS NULL
            AND reference_external_transaction_id IS NULL AND reference_transaction_id IS NULL)
    ),
    -- Zero-value policy: LOSS is exactly zero, everything else positive.
    CONSTRAINT wager_transactions_amount_policy CHECK (
        (kind = 'LOSS' AND amount_minor = 0) OR (kind <> 'LOSS' AND amount_minor > 0)
    ),
    -- Reversals always carry a reference.
    CONSTRAINT wager_transactions_reversal_reference CHECK (
        kind NOT IN ('REFUND', 'ROLLBACK') OR reference_external_transaction_id IS NOT NULL
    ),
    -- Terminal rows carry their result; rejections carry a failure code.
    CONSTRAINT wager_transactions_terminal_shape CHECK (
        (status = 'PROCESSED' AND result_balance_minor IS NOT NULL AND failure_code IS NULL AND processed_at IS NOT NULL)
        OR (status = 'REJECTED' AND failure_code IS NOT NULL AND processed_at IS NOT NULL)
        OR (status = 'FAILED' AND failure_code IS NOT NULL AND processed_at IS NOT NULL)
        OR (status = 'PENDING_REFERENCE' AND next_reference_retry_at IS NOT NULL AND reference_attempts > 0)
        OR (status = 'PENDING')
    )
);

-- Idempotency: one row per provider key and per provider external id.
CREATE UNIQUE INDEX wager_transactions_provider_key_unique
    ON wager_transactions (provider_id, idempotency_key) WHERE origin = 'EXTERNAL';
CREATE UNIQUE INDEX wager_transactions_provider_external_unique
    ON wager_transactions (provider_id, external_transaction_id) WHERE origin = 'EXTERNAL';
-- One OPENING per wallet.
CREATE UNIQUE INDEX wager_transactions_opening_unique
    ON wager_transactions (wallet_id) WHERE kind = 'OPENING';
-- At most one successful reversal per referenced transaction.
CREATE UNIQUE INDEX wager_transactions_single_reversal
    ON wager_transactions (reference_transaction_id)
    WHERE status = 'PROCESSED' AND kind IN ('REFUND', 'ROLLBACK');
-- Worker scan for due references.
CREATE INDEX wager_transactions_pending_reference_due
    ON wager_transactions (next_reference_retry_at) WHERE status = 'PENDING_REFERENCE';
CREATE INDEX wager_transactions_wallet_idx ON wager_transactions (wallet_id, created_at);

-- Terminal transactions are frozen; only non-terminal rows may change.
CREATE FUNCTION wager_transactions_guard() RETURNS trigger AS $$
BEGIN
    IF OLD.status IN ('PROCESSED', 'REJECTED', 'FAILED') THEN
        RAISE EXCEPTION 'wager_transactions: row % is terminal (%) and cannot change', OLD.id, OLD.status
            USING ERRCODE = 'restrict_violation';
    END IF;
    IF NEW.id <> OLD.id OR NEW.origin <> OLD.origin OR NEW.kind <> OLD.kind OR NEW.wallet_id <> OLD.wallet_id
       OR NEW.amount_minor <> OLD.amount_minor OR NEW.currency <> OLD.currency
       OR NEW.provider_id IS DISTINCT FROM OLD.provider_id
       OR NEW.external_transaction_id IS DISTINCT FROM OLD.external_transaction_id
       OR NEW.idempotency_key IS DISTINCT FROM OLD.idempotency_key
       OR NEW.payload_hash IS DISTINCT FROM OLD.payload_hash THEN
        RAISE EXCEPTION 'wager_transactions: identity fields of % are immutable', OLD.id
            USING ERRCODE = 'restrict_violation';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER wager_transactions_guard_update
    BEFORE UPDATE ON wager_transactions
    FOR EACH ROW EXECUTE FUNCTION wager_transactions_guard();

CREATE FUNCTION forbid_delete() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION '%: rows are append-only and cannot be deleted', TG_TABLE_NAME
        USING ERRCODE = 'restrict_violation';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER wager_transactions_forbid_delete
    BEFORE DELETE ON wager_transactions
    FOR EACH ROW EXECUTE FUNCTION forbid_delete();

-- Ledger: append-only, one entry per (wallet, transaction), arithmetic checked.
CREATE TABLE wallet_ledger_entries (
    seq                  BIGSERIAL   PRIMARY KEY,
    id                   UUID        NOT NULL UNIQUE,
    wallet_id            UUID        NOT NULL REFERENCES wallets (id),
    transaction_id       UUID        NOT NULL REFERENCES wager_transactions (id),
    direction            TEXT        NOT NULL CHECK (direction IN ('DEBIT', 'CREDIT')),
    amount_minor         BIGINT      NOT NULL CHECK (amount_minor > 0),
    balance_before_minor BIGINT      NOT NULL CHECK (balance_before_minor >= 0),
    balance_after_minor  BIGINT      NOT NULL CHECK (balance_after_minor >= 0),
    currency             CHAR(3)     NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL,
    CONSTRAINT wallet_ledger_entries_wallet_tx_unique UNIQUE (wallet_id, transaction_id),
    CONSTRAINT wallet_ledger_entries_arithmetic CHECK (
        (direction = 'CREDIT' AND balance_after_minor = balance_before_minor + amount_minor)
        OR (direction = 'DEBIT' AND balance_after_minor = balance_before_minor - amount_minor)
    )
);
CREATE INDEX wallet_ledger_entries_wallet_seq ON wallet_ledger_entries (wallet_id, seq);

CREATE FUNCTION forbid_update() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION '%: rows are immutable', TG_TABLE_NAME USING ERRCODE = 'restrict_violation';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER wallet_ledger_entries_immutable_update
    BEFORE UPDATE ON wallet_ledger_entries FOR EACH ROW EXECUTE FUNCTION forbid_update();
CREATE TRIGGER wallet_ledger_entries_immutable_delete
    BEFORE DELETE ON wallet_ledger_entries FOR EACH ROW EXECUTE FUNCTION forbid_delete();

-- Inbox: durable dedup of consumed messages, keyed by consumer and message id.
CREATE TABLE inbox_messages (
    consumer_name TEXT        NOT NULL,
    message_id    TEXT        NOT NULL,
    payload_hash  TEXT        NOT NULL,
    received_at   TIMESTAMPTZ NOT NULL,
    completed_at  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (consumer_name, message_id)
);

-- Outbox: events committed with the domain change, published by workers.
CREATE TABLE outbox_events (
    event_id        UUID        PRIMARY KEY,
    aggregate_type  TEXT        NOT NULL,
    aggregate_id    UUID        NOT NULL,
    event_type      TEXT        NOT NULL,
    payload         JSONB       NOT NULL,
    occurred_at     TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL,
    attempts        INTEGER     NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL,
    locked_by       TEXT,
    locked_until    TIMESTAMPTZ,
    published_at    TIMESTAMPTZ,
    last_error      TEXT
);
CREATE INDEX outbox_events_pending_idx ON outbox_events (next_attempt_at) WHERE published_at IS NULL;
