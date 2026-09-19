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
	"github.com/jamesmachome/backend-challenge-go/internal/domain/wagering"
)

type transactionRepo struct{ tx pgx.Tx }

const txColumns = `id, origin, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id,
	round_id, game_id, kind, amount_minor, currency, reference_external_transaction_id, reference_transaction_id, status,
	failure_code, result_balance_minor, reference_attempts, next_reference_retry_at, correlation_id, created_at, updated_at, processed_at`

func nullStr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func derefStr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func (r *transactionRepo) Insert(ctx context.Context, t *wagering.Transaction) error {
	s := t.Snapshot()
	var result *int64
	if s.ResultBalance != nil {
		v := s.ResultBalance.Minor()
		result = &v
	}
	_, err := r.tx.Exec(ctx, `INSERT INTO wager_transactions (`+txColumns+`) VALUES
		($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24)`,
		s.ID, string(s.Origin), nullStr(s.ProviderID), nullStr(s.ExternalTransactionID), nullStr(s.IdempotencyKey), nullStr(s.PayloadHash),
		s.WalletID, s.PlayerID, nullStr(s.RoundID), nullStr(s.GameID), string(s.Kind), s.Money.Minor(), string(s.Money.Currency()),
		nullStr(s.ReferenceExternalID), s.ReferenceID, string(s.Status), nullStr(string(s.FailureCode)), result, s.ReferenceAttempts,
		s.NextReferenceRetryAt, nullStr(s.CorrelationID), s.CreatedAt, s.UpdatedAt, s.ProcessedAt)
	if isUniqueViolation(err, "") {
		return fmt.Errorf("%w: %v", app.ErrDuplicateTransaction, err)
	}
	if err != nil {
		return fmt.Errorf("postgres: insert transaction: %w", err)
	}
	return nil
}

func (r *transactionRepo) Update(ctx context.Context, t *wagering.Transaction) error {
	s := t.Snapshot()
	var result *int64
	if s.ResultBalance != nil {
		v := s.ResultBalance.Minor()
		result = &v
	}
	tag, err := r.tx.Exec(ctx, `UPDATE wager_transactions SET reference_transaction_id = $2, status = $3, failure_code = $4,
		result_balance_minor = $5, reference_attempts = $6, next_reference_retry_at = $7, updated_at = $8, processed_at = $9
		WHERE id = $1`,
		s.ID, s.ReferenceID, string(s.Status), nullStr(string(s.FailureCode)), result, s.ReferenceAttempts, s.NextReferenceRetryAt, s.UpdatedAt, s.ProcessedAt)
	if isUniqueViolation(err, "") {
		return fmt.Errorf("%w: %v", app.ErrDuplicateTransaction, err)
	}
	if err != nil {
		return fmt.Errorf("postgres: update transaction: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return app.ErrTransactionNotFound
	}
	return nil
}

func (r *transactionRepo) Get(ctx context.Context, id uuid.UUID) (*wagering.Transaction, error) {
	return r.scan(r.tx.QueryRow(ctx, `SELECT `+txColumns+` FROM wager_transactions WHERE id = $1`, id))
}

func (r *transactionRepo) FindByIdempotencyKey(ctx context.Context, providerID, key string) (*wagering.Transaction, error) {
	return r.scan(r.tx.QueryRow(ctx, `SELECT `+txColumns+` FROM wager_transactions WHERE origin = 'EXTERNAL' AND provider_id = $1 AND idempotency_key = $2`, providerID, key))
}

func (r *transactionRepo) FindByExternalID(ctx context.Context, providerID, externalID string) (*wagering.Transaction, error) {
	return r.scan(r.tx.QueryRow(ctx, `SELECT `+txColumns+` FROM wager_transactions WHERE origin = 'EXTERNAL' AND provider_id = $1 AND external_transaction_id = $2`, providerID, externalID))
}

func (r *transactionRepo) HasProcessedReversal(ctx context.Context, referenceID uuid.UUID) (bool, error) {
	var exists bool
	err := r.tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM wager_transactions WHERE reference_transaction_id = $1 AND status = 'PROCESSED' AND kind IN ('REFUND','ROLLBACK'))`, referenceID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("postgres: reversal lookup: %w", err)
	}
	return exists, nil
}

func (r *transactionRepo) ClaimDuePendingReference(ctx context.Context, now time.Time) (*wagering.Transaction, error) {
	return r.scan(r.tx.QueryRow(ctx, `SELECT `+txColumns+` FROM wager_transactions
		WHERE status = 'PENDING_REFERENCE' AND next_reference_retry_at <= $1
		ORDER BY next_reference_retry_at LIMIT 1 FOR UPDATE SKIP LOCKED`, now))
}

func (r *transactionRepo) scan(row pgx.Row) (*wagering.Transaction, error) {
	var (
		s                                                                   wagering.Snapshot
		origin, kind, status, currency                                      string
		providerID, externalID, key, hash, roundID, gameID, refExt, failure *string
		corr                                                                *string
		amount                                                              int64
		result                                                              *int64
	)
	err := row.Scan(&s.ID, &origin, &providerID, &externalID, &key, &hash, &s.WalletID, &s.PlayerID, &roundID, &gameID, &kind, &amount, &currency,
		&refExt, &s.ReferenceID, &status, &failure, &result, &s.ReferenceAttempts, &s.NextReferenceRetryAt, &corr, &s.CreatedAt, &s.UpdatedAt, &s.ProcessedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, app.ErrTransactionNotFound
		}
		return nil, fmt.Errorf("postgres: scan transaction: %w", err)
	}
	m, err := money.FromMinor(amount, money.Currency(currency))
	if err != nil {
		return nil, err
	}
	s.Origin, s.Kind, s.Status, s.Money = wagering.Origin(origin), wagering.Kind(kind), wagering.Status(status), m
	s.ProviderID, s.ExternalTransactionID, s.IdempotencyKey, s.PayloadHash = derefStr(providerID), derefStr(externalID), derefStr(key), derefStr(hash)
	s.RoundID, s.GameID, s.ReferenceExternalID, s.FailureCode, s.CorrelationID = derefStr(roundID), derefStr(gameID), derefStr(refExt), wagering.FailureCode(derefStr(failure)), derefStr(corr)
	if result != nil {
		rb, err := money.FromMinor(*result, money.Currency(currency))
		if err != nil {
			return nil, err
		}
		s.ResultBalance = &rb
	}
	return wagering.Rehydrate(s)
}
