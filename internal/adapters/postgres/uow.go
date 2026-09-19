package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jamesmachome/backend-challenge-go/internal/app"
)

// UnitOfWork runs use-case callbacks inside pgx transactions.
type UnitOfWork struct {
	pool *pgxpool.Pool
}

// NewUnitOfWork builds a UnitOfWork over the pool.
func NewUnitOfWork(pool *pgxpool.Pool) *UnitOfWork { return &UnitOfWork{pool: pool} }

// Do runs fn in a READ COMMITTED transaction; row locks are explicit.
func (u *UnitOfWork) Do(ctx context.Context, fn func(ctx context.Context, s app.Store) error) error {
	return u.run(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, fn)
}

// DoSnapshot runs fn in a REPEATABLE READ, read-only transaction.
func (u *UnitOfWork) DoSnapshot(ctx context.Context, fn func(ctx context.Context, s app.Store) error) error {
	return u.run(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, fn)
}

func (u *UnitOfWork) run(ctx context.Context, opts pgx.TxOptions, fn func(ctx context.Context, s app.Store) error) (err error) {
	tx, err := u.pool.BeginTx(ctx, opts)
	if err != nil {
		return fmt.Errorf("postgres: begin: %w", err)
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback(context.WithoutCancel(ctx))
			panic(p)
		}
	}()
	if err := fn(ctx, &store{tx: tx}); err != nil {
		if rbErr := tx.Rollback(context.WithoutCancel(ctx)); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			return errors.Join(err, rbErr)
		}
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("postgres: commit: %w", err)
	}
	return nil
}

type store struct{ tx pgx.Tx }

func (s *store) Wallets() app.WalletRepository           { return &walletRepo{tx: s.tx} }
func (s *store) Transactions() app.TransactionRepository { return &transactionRepo{tx: s.tx} }
func (s *store) Ledger() app.LedgerRepository            { return &ledgerRepo{tx: s.tx} }
func (s *store) Outbox() app.OutboxRepository            { return &outboxRepo{tx: s.tx} }
func (s *store) Inbox() app.InboxRepository              { return &inboxRepo{tx: s.tx} }
