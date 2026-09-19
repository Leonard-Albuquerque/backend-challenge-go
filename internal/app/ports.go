// Package app contains the use cases shared by the HTTP API and the SQS
// consumer. It depends on the domain and on the repository ports declared here.
package app

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/jamesmachome/backend-challenge-go/internal/domain/events"
	"github.com/jamesmachome/backend-challenge-go/internal/domain/wagering"
	"github.com/jamesmachome/backend-challenge-go/internal/domain/wallet"
)

var (
	ErrWalletNotFound         = errors.New("wallet not found")
	ErrWalletExists           = errors.New("wallet already exists for player and currency")
	ErrTransactionNotFound    = errors.New("transaction not found")
	ErrIdempotencyConflict    = errors.New("idempotency key reused with a different payload")
	ErrExternalIDConflict     = errors.New("external transaction id already used with another idempotency key")
	ErrDuplicateTransaction   = errors.New("duplicate transaction insert")
	ErrConcurrentModification = errors.New("concurrent wallet modification detected")
	ErrInvalidCursor          = errors.New("invalid cursor")
)

// Store groups the repositories bound to one SQL transaction.
type Store interface {
	Wallets() WalletRepository
	Transactions() TransactionRepository
	Ledger() LedgerRepository
	Outbox() OutboxRepository
	Inbox() InboxRepository
}

// UnitOfWork runs fn inside one SQL transaction. Do uses READ COMMITTED with
// explicit row locks; DoSnapshot uses REPEATABLE READ for consistent reads.
type UnitOfWork interface {
	Do(ctx context.Context, fn func(ctx context.Context, s Store) error) error
	DoSnapshot(ctx context.Context, fn func(ctx context.Context, s Store) error) error
}

type WalletRepository interface {
	Create(ctx context.Context, w *wallet.Wallet) error
	Get(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error)
	// GetForUpdate locks the wallet row for the rest of the transaction.
	GetForUpdate(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error)
	// Update persists balance/version and fails with ErrConcurrentModification
	// when the stored version differs from expectedVersion.
	Update(ctx context.Context, w *wallet.Wallet, expectedVersion int64) error
}

type TransactionRepository interface {
	Insert(ctx context.Context, tx *wagering.Transaction) error
	Update(ctx context.Context, tx *wagering.Transaction) error
	Get(ctx context.Context, id uuid.UUID) (*wagering.Transaction, error)
	FindByIdempotencyKey(ctx context.Context, providerID, key string) (*wagering.Transaction, error)
	FindByExternalID(ctx context.Context, providerID, externalID string) (*wagering.Transaction, error)
	HasProcessedReversal(ctx context.Context, referenceID uuid.UUID) (bool, error)
	// ClaimDuePendingReference locks and returns one PENDING_REFERENCE
	// transaction whose retry is due, skipping rows locked by other workers.
	ClaimDuePendingReference(ctx context.Context, now time.Time) (*wagering.Transaction, error)
}

// LedgerRecord pairs an entry with its stable sequence used for cursors.
type LedgerRecord struct {
	Seq   int64
	Entry wallet.LedgerEntry
}

// LedgerTotals is the aggregate of a wallet's ledger.
type LedgerTotals struct {
	Credits int64
	Debits  int64
	Count   int64
}

type LedgerRepository interface {
	Append(ctx context.Context, e wallet.LedgerEntry) error
	List(ctx context.Context, walletID uuid.UUID, afterSeq int64, limit int) ([]LedgerRecord, error)
	Totals(ctx context.Context, walletID uuid.UUID) (LedgerTotals, error)
}

// OutboxRecord is a claimed outbox row.
type OutboxRecord struct {
	EventID     uuid.UUID
	AggregateID uuid.UUID
	EventType   string
	Payload     []byte
	Attempts    int
	OccurredAt  time.Time
}

type OutboxRepository interface {
	Add(ctx context.Context, ev events.Event) error
	// Claim leases up to limit unpublished, due rows for publisherID.
	Claim(ctx context.Context, publisherID string, now time.Time, lease time.Duration, limit int) ([]OutboxRecord, error)
	MarkPublished(ctx context.Context, eventID uuid.UUID, now time.Time) error
	MarkPublishedBatch(ctx context.Context, eventIDs []uuid.UUID, now time.Time) error
	// Reschedule releases the lease and sets the next attempt.
	Reschedule(ctx context.Context, eventID uuid.UUID, nextAttempt time.Time, lastError string) error
	// Lag returns the age of the oldest unpublished event and the pending count.
	Lag(ctx context.Context, now time.Time) (time.Duration, int64, error)
}

// InboxResult describes the outcome of recording a message.
type InboxResult struct {
	Inserted     bool
	ExistingHash string
}

type InboxRepository interface {
	Record(ctx context.Context, consumer, messageID, payloadHash string, now time.Time) (InboxResult, error)
}

// Metrics is the subset of observability the use cases report to.
type Metrics interface {
	TransactionOutcome(kind, status, source string)
	Duplicate(source string)
	ConcurrencyConflict()
	ReconciliationDivergence()
	ProcessingDuration(source string, d time.Duration)
	PendingReferenceRetry()
}

// Clock abstracts time for tests.
type Clock func() time.Time
