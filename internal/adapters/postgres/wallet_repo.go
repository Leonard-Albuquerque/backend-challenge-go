package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/jamesmachome/backend-challenge-go/internal/app"
	"github.com/jamesmachome/backend-challenge-go/internal/domain/money"
	"github.com/jamesmachome/backend-challenge-go/internal/domain/wallet"
)

type walletRepo struct{ tx pgx.Tx }

const walletColumns = `id, player_id, currency, balance_minor, version, created_at, updated_at`

func (r *walletRepo) Create(ctx context.Context, w *wallet.Wallet) error {
	_, err := r.tx.Exec(ctx, `INSERT INTO wallets (`+walletColumns+`) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		w.ID(), w.PlayerID(), string(w.Currency()), w.Balance().Minor(), w.Version(), w.CreatedAt(), w.UpdatedAt())
	if isUniqueViolation(err, "wallets_player_currency_unique") {
		return app.ErrWalletExists
	}
	if err != nil {
		return fmt.Errorf("postgres: create wallet: %w", err)
	}
	return nil
}

func (r *walletRepo) Get(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error) {
	return r.scan(r.tx.QueryRow(ctx, `SELECT `+walletColumns+` FROM wallets WHERE id = $1`, id))
}

func (r *walletRepo) GetForUpdate(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error) {
	return r.scan(r.tx.QueryRow(ctx, `SELECT `+walletColumns+` FROM wallets WHERE id = $1 FOR UPDATE`, id))
}

func (r *walletRepo) scan(row pgx.Row) (*wallet.Wallet, error) {
	var (
		id, playerID         uuid.UUID
		currency             string
		balance, version     int64
		createdAt, updatedAt time.Time
	)
	if err := row.Scan(&id, &playerID, &currency, &balance, &version, &createdAt, &updatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, app.ErrWalletNotFound
		}
		return nil, fmt.Errorf("postgres: scan wallet: %w", err)
	}
	m, err := money.FromMinor(balance, money.Currency(currency))
	if err != nil {
		return nil, err
	}
	return wallet.Rehydrate(id, playerID, m, version, createdAt, updatedAt)
}

func (r *walletRepo) Update(ctx context.Context, w *wallet.Wallet, expectedVersion int64) error {
	tag, err := r.tx.Exec(ctx, `UPDATE wallets SET balance_minor = $2, version = $3, updated_at = $4 WHERE id = $1 AND version = $5`,
		w.ID(), w.Balance().Minor(), w.Version(), w.UpdatedAt(), expectedVersion)
	if err != nil {
		return fmt.Errorf("postgres: update wallet: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return app.ErrConcurrentModification
	}
	return nil
}
