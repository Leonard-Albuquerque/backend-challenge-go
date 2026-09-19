package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/jamesmachome/backend-challenge-go/internal/app"
	"github.com/jamesmachome/backend-challenge-go/internal/domain/money"
	"github.com/jamesmachome/backend-challenge-go/internal/domain/wallet"
)

type ledgerRepo struct{ tx pgx.Tx }

func (r *ledgerRepo) Append(ctx context.Context, e wallet.LedgerEntry) error {
	_, err := r.tx.Exec(ctx, `INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount_minor, balance_before_minor, balance_after_minor, currency, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
		e.ID(), e.WalletID(), e.TransactionID(), string(e.Direction()), e.Amount().Minor(), e.BalanceBefore().Minor(), e.BalanceAfter().Minor(), string(e.Amount().Currency()), e.CreatedAt())
	if isUniqueViolation(err, "") {
		return fmt.Errorf("%w: ledger entry: %v", app.ErrDuplicateTransaction, err)
	}
	if err != nil {
		return fmt.Errorf("postgres: append ledger: %w", err)
	}
	return nil
}

func (r *ledgerRepo) List(ctx context.Context, walletID uuid.UUID, afterSeq int64, limit int) ([]app.LedgerRecord, error) {
	rows, err := r.tx.Query(ctx, `SELECT seq, id, wallet_id, transaction_id, direction, amount_minor, balance_before_minor, balance_after_minor, currency, created_at
		FROM wallet_ledger_entries WHERE wallet_id = $1 AND seq > $2 ORDER BY seq LIMIT $3`, walletID, afterSeq, limit)
	if err != nil {
		return nil, fmt.Errorf("postgres: list ledger: %w", err)
	}
	defer rows.Close()
	var out []app.LedgerRecord
	for rows.Next() {
		var (
			seq                   int64
			id, wid, tid          uuid.UUID
			dir, currency         string
			amount, before, after int64
			createdAt             time.Time
		)
		if err := rows.Scan(&seq, &id, &wid, &tid, &dir, &amount, &before, &after, &currency, &createdAt); err != nil {
			return nil, fmt.Errorf("postgres: scan ledger: %w", err)
		}
		cur := money.Currency(currency)
		entry, err := wallet.NewLedgerEntry(id, wid, tid, wallet.Direction(dir), money.MustFromMinor(amount, cur), money.MustFromMinor(before, cur), money.MustFromMinor(after, cur), createdAt)
		if err != nil {
			return nil, err
		}
		out = append(out, app.LedgerRecord{Seq: seq, Entry: entry})
	}
	return out, rows.Err()
}

func (r *ledgerRepo) Totals(ctx context.Context, walletID uuid.UUID) (app.LedgerTotals, error) {
	var t app.LedgerTotals
	err := r.tx.QueryRow(ctx, `SELECT
		COALESCE(SUM(amount_minor) FILTER (WHERE direction = 'CREDIT'), 0)::BIGINT,
		COALESCE(SUM(amount_minor) FILTER (WHERE direction = 'DEBIT'), 0)::BIGINT,
		COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID).Scan(&t.Credits, &t.Debits, &t.Count)
	if err != nil {
		return t, fmt.Errorf("postgres: ledger totals: %w", err)
	}
	return t, nil
}
