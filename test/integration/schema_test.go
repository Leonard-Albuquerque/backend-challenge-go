//go:build integration

package integration

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jamesmachome/backend-challenge-go/internal/adapters/postgres"
)

func TestMigrationsUpAndDown(t *testing.T) {
	ctx := context.Background()
	// Use a dedicated database so the running cluster is unaffected.
	dbName := "wager_mig_" + runID
	if _, err := pool.Exec(ctx, "CREATE DATABASE "+dbName); err != nil {
		t.Fatal(err)
	}
	defer pool.Exec(ctx, "DROP DATABASE "+dbName)
	dsn := strings.Replace(dbURL, "/wager?", "/"+dbName+"?", 1)
	m, err := postgres.NewMigrator(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if err := m.Up(); err != nil {
		t.Fatal(err)
	}
	if v, dirty, _ := m.Version(); v != 1 || dirty {
		t.Fatalf("version %d dirty=%v", v, dirty)
	}
	p, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	var tables int
	_ = p.QueryRow(ctx, `SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ('wallets','wager_transactions','wallet_ledger_entries','inbox_messages','outbox_events')`).Scan(&tables)
	if tables != 5 {
		t.Fatalf("tables after up: %d", tables)
	}
	if err := m.Down(); err != nil {
		t.Fatal(err)
	}
	_ = p.QueryRow(ctx, `SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ('wallets','wager_transactions','wallet_ledger_entries','inbox_messages','outbox_events')`).Scan(&tables)
	if tables != 0 {
		t.Fatalf("tables after down: %d", tables)
	}
	if err := m.Up(); err != nil {
		t.Fatal("re-up:", err)
	}
}

func expectPgError(t *testing.T, err error, codeOrConstraint string) {
	t.Helper()
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("expected postgres error %s, got %v", codeOrConstraint, err)
	}
	if pgErr.Code != codeOrConstraint && pgErr.ConstraintName != codeOrConstraint && !strings.Contains(pgErr.Message, codeOrConstraint) {
		t.Fatalf("expected %s, got %s (%s / %s)", codeOrConstraint, pgErr.Code, pgErr.ConstraintName, pgErr.Message)
	}
}

func TestSchemaConstraintsAndImmutability(t *testing.T) {
	ctx := context.Background()
	w := openWallet(t, cluster.Next(), "10.00")
	b := bet(w, "tx-schema-"+w.ID[24:], "4.00")
	if r := postTx(t, cluster.Next(), Token(t, clientProviderA), b); r.Status != 200 {
		t.Fatalf("bet: %d %s", r.Status, r.Raw)
	}
	var txID, entryID uuid.UUID
	if err := pool.QueryRow(ctx, `SELECT t.id, l.id FROM wager_transactions t JOIN wallet_ledger_entries l ON l.transaction_id = t.id WHERE t.provider_id = 'provider-a' AND t.external_transaction_id = $1`, b.ExternalTransactionID).Scan(&txID, &entryID); err != nil {
		t.Fatal(err)
	}

	// Negative balance is impossible even bypassing the application.
	_, err := pool.Exec(ctx, `UPDATE wallets SET balance_minor = -1 WHERE id = $1`, w.ID)
	expectPgError(t, err, "23514")
	// Ledger is immutable.
	_, err = pool.Exec(ctx, `UPDATE wallet_ledger_entries SET amount_minor = 1 WHERE id = $1`, entryID)
	expectPgError(t, err, "immutable")
	_, err = pool.Exec(ctx, `DELETE FROM wallet_ledger_entries WHERE id = $1`, entryID)
	expectPgError(t, err, "append-only")
	// One ledger entry per (wallet, transaction) and arithmetic is checked.
	_, err = pool.Exec(ctx, `INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount_minor, balance_before_minor, balance_after_minor, currency, created_at) VALUES ($1,$2,$3,'DEBIT',400,1000,600,'BRL',now())`, uuid.New(), w.ID, txID)
	expectPgError(t, err, "wallet_ledger_entries_wallet_tx_unique")
	_, err = pool.Exec(ctx, `INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount_minor, balance_before_minor, balance_after_minor, currency, created_at) VALUES ($1,$2,$3,'DEBIT',400,1000,700,'BRL',now())`, uuid.New(), w.ID, uuid.New())
	if !errors.As(err, new(*pgconn.PgError)) {
		t.Fatal("arithmetic/fk check expected")
	}
	// Terminal transactions cannot change or be deleted.
	_, err = pool.Exec(ctx, `UPDATE wager_transactions SET status = 'PENDING' WHERE id = $1`, txID)
	expectPgError(t, err, "terminal")
	_, err = pool.Exec(ctx, `DELETE FROM wager_transactions WHERE id = $1`, txID)
	expectPgError(t, err, "append-only")
	// Unique idempotency key / external id per provider, unique OPENING per wallet.
	_, err = pool.Exec(ctx, `INSERT INTO wager_transactions (id, origin, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id, round_id, game_id, kind, amount_minor, currency, status, result_balance_minor, created_at, updated_at, processed_at)
		VALUES ($1,'EXTERNAL','provider-a',$2,'other-key','h',$3,$4,'r','g','BET',100,'BRL','PROCESSED',0,now(),now(),now())`, uuid.New(), b.ExternalTransactionID, w.ID, w.PlayerID)
	expectPgError(t, err, "wager_transactions_provider_external_unique")
	_, err = pool.Exec(ctx, `INSERT INTO wager_transactions (id, origin, wallet_id, player_id, kind, amount_minor, currency, status, result_balance_minor, created_at, updated_at, processed_at)
		VALUES ($1,'INTERNAL',$2,$3,'OPENING',100,'BRL','PROCESSED',100,now(),now(),now())`, uuid.New(), w.ID, w.PlayerID)
	expectPgError(t, err, "wager_transactions_opening_unique")
	// OPENING cannot carry external metadata; external rows need it.
	_, err = pool.Exec(ctx, `INSERT INTO wager_transactions (id, origin, provider_id, wallet_id, player_id, kind, amount_minor, currency, status, created_at, updated_at)
		VALUES ($1,'INTERNAL','provider-a',$2,$3,'OPENING',100,'BRL','PENDING',now(),now())`, uuid.New(), uuid.New(), w.PlayerID)
	expectPgError(t, err, "23514")
	_, err = pool.Exec(ctx, `INSERT INTO wager_transactions (id, origin, wallet_id, player_id, kind, amount_minor, currency, status, created_at, updated_at)
		VALUES ($1,'EXTERNAL',$2,$3,'BET',100,'BRL','PENDING',now(),now())`, uuid.New(), w.ID, w.PlayerID)
	expectPgError(t, err, "23514")
	// A reference can only have one successful reversal.
	var refundID uuid.UUID
	if err := pool.QueryRow(ctx, `INSERT INTO wager_transactions (id, origin, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id, round_id, game_id, kind, amount_minor, currency, reference_external_transaction_id, reference_transaction_id, status, result_balance_minor, created_at, updated_at, processed_at)
		VALUES ($1,'EXTERNAL','provider-a',$2,$2,'h',$3,$4,'r','g','REFUND',400,'BRL',$5,$6,'PROCESSED',1000,now(),now(),now()) RETURNING id`, uuid.New(), "schema-refund-"+w.ID[24:], w.ID, w.PlayerID, b.ExternalTransactionID, txID).Scan(&refundID); err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, `INSERT INTO wager_transactions (id, origin, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id, round_id, game_id, kind, amount_minor, currency, reference_external_transaction_id, reference_transaction_id, status, result_balance_minor, created_at, updated_at, processed_at)
		VALUES ($1,'EXTERNAL','provider-a',$2,$2,'h',$3,$4,'r','g','ROLLBACK',400,'BRL',$5,$6,'PROCESSED',1000,now(),now(),now())`, uuid.New(), "schema-rollback-"+w.ID[24:], w.ID, w.PlayerID, b.ExternalTransactionID, txID)
	expectPgError(t, err, "wager_transactions_single_reversal")
	// Inbox primary key.
	_, _ = pool.Exec(ctx, `INSERT INTO inbox_messages VALUES ('c', $1, 'h', now(), now())`, "schema-"+w.ID)
	_, err = pool.Exec(ctx, `INSERT INTO inbox_messages VALUES ('c', $1, 'h2', now(), now())`, "schema-"+w.ID)
	expectPgError(t, err, "23505")

	// Atomicity: a failing ledger insert rolls back the wallet update too.
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE wallets SET balance_minor = 0, version = version + 1 WHERE id = $1`, w.ID); err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount_minor, balance_before_minor, balance_after_minor, currency, created_at) VALUES ($1,$2,$3,'DEBIT',600,600,0,'BRL',now())`, uuid.New(), w.ID, txID)
	if err == nil {
		t.Fatal("expected unique violation")
	}
	_ = tx.Rollback(ctx)
	if getWallet(t, cluster.Next(), w.ID).money("balance") != "6.00" {
		t.Fatal("wallet update leaked out of the failed transaction")
	}
	_ = time.Now
}
