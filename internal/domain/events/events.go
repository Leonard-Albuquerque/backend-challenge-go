// Package events defines the integration events emitted through the outbox.
// Every event carries a versioned envelope; type and version are fixed by the
// constructor so producers cannot emit inconsistent payloads.
package events

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/jamesmachome/backend-challenge-go/internal/domain/money"
	"github.com/jamesmachome/backend-challenge-go/internal/domain/wagering"
	"github.com/jamesmachome/backend-challenge-go/internal/domain/wallet"
)

var ErrInvalidEvent = errors.New("events: invalid event")

const (
	TypeWagerTransactionProcessed        = "WagerTransactionProcessed"
	TypeWagerTransactionRejected         = "WagerTransactionRejected"
	TypeWalletBalanceChanged             = "WalletBalanceChanged"
	TypeWagerTransactionPendingReference = "WagerTransactionPendingReference"
)

// Envelope is the transport representation of an event.
type Envelope struct {
	EventID       uuid.UUID       `json:"eventId"`
	EventType     string          `json:"eventType"`
	AggregateType string          `json:"aggregateType"`
	AggregateID   uuid.UUID       `json:"aggregateId"`
	CorrelationID string          `json:"correlationId"`
	CausationID   string          `json:"causationId,omitempty"`
	OccurredAt    time.Time       `json:"occurredAt"`
	Version       int             `json:"version"`
	Data          json.RawMessage `json:"data"`
}

// Event is an immutable domain event ready to be stored in the outbox.
type Event struct {
	envelope Envelope
}

func (e Event) Envelope() Envelope { return e.envelope }
func (e Event) ID() uuid.UUID      { return e.envelope.EventID }
func (e Event) Type() string       { return e.envelope.EventType }

// Marshal renders the full envelope as JSON.
func (e Event) Marshal() ([]byte, error) { return json.Marshal(e.envelope) }

func newEvent(eventType string, version int, aggregateType string, aggregateID uuid.UUID, correlationID, causationID string, occurredAt time.Time, data any) (Event, error) {
	if aggregateID == uuid.Nil {
		return Event{}, fmt.Errorf("%w: aggregate id is required", ErrInvalidEvent)
	}
	if occurredAt.IsZero() {
		return Event{}, fmt.Errorf("%w: occurredAt is required", ErrInvalidEvent)
	}
	if correlationID == "" {
		correlationID = uuid.NewString()
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return Event{}, fmt.Errorf("%w: %v", ErrInvalidEvent, err)
	}
	id, err := uuid.NewV7()
	if err != nil {
		return Event{}, err
	}
	return Event{envelope: Envelope{
		EventID: id, EventType: eventType, AggregateType: aggregateType, AggregateID: aggregateID,
		CorrelationID: correlationID, CausationID: causationID, OccurredAt: occurredAt.UTC().Truncate(time.Millisecond),
		Version: version, Data: raw,
	}}, nil
}

// WagerTransactionProcessedData is the payload of WagerTransactionProcessed.
type WagerTransactionProcessedData struct {
	TransactionID          uuid.UUID  `json:"transactionId"`
	Origin                 string     `json:"origin"`
	ProviderID             string     `json:"providerId,omitempty"`
	ExternalTransactionID  string     `json:"externalTransactionId,omitempty"`
	WalletID               uuid.UUID  `json:"walletId"`
	PlayerID               uuid.UUID  `json:"playerId"`
	RoundID                string     `json:"roundId,omitempty"`
	GameID                 string     `json:"gameId,omitempty"`
	Kind                   string     `json:"kind"`
	Money                  money.JSON `json:"money"`
	Balance                money.JSON `json:"balance"`
	ReferenceTransactionID *uuid.UUID `json:"referenceTransactionId,omitempty"`
	ProcessedAt            time.Time  `json:"processedAt"`
}

// NewWagerTransactionProcessed builds the event for a PROCESSED transaction.
func NewWagerTransactionProcessed(tx *wagering.Transaction) (Event, error) {
	if tx.Status() != wagering.StatusProcessed || tx.ResultBalance() == nil || tx.ProcessedAt() == nil {
		return Event{}, fmt.Errorf("%w: transaction is not processed", ErrInvalidEvent)
	}
	data := WagerTransactionProcessedData{
		TransactionID: tx.ID(), Origin: string(tx.Origin()), ProviderID: tx.ProviderID(), ExternalTransactionID: tx.ExternalTransactionID(),
		WalletID: tx.WalletID(), PlayerID: tx.PlayerID(), RoundID: tx.RoundID(), GameID: tx.GameID(), Kind: string(tx.Kind()),
		Money: tx.Money().ToJSON(), Balance: tx.ResultBalance().ToJSON(), ReferenceTransactionID: tx.ReferenceID(), ProcessedAt: tx.ProcessedAt().UTC(),
	}
	return newEvent(TypeWagerTransactionProcessed, 1, "WagerTransaction", tx.ID(), tx.CorrelationID(), tx.ID().String(), *tx.ProcessedAt(), data)
}

// WagerTransactionRejectedData is the payload of WagerTransactionRejected.
type WagerTransactionRejectedData struct {
	TransactionID         uuid.UUID  `json:"transactionId"`
	ProviderID            string     `json:"providerId"`
	ExternalTransactionID string     `json:"externalTransactionId"`
	WalletID              uuid.UUID  `json:"walletId"`
	PlayerID              uuid.UUID  `json:"playerId"`
	RoundID               string     `json:"roundId"`
	GameID                string     `json:"gameId"`
	Kind                  string     `json:"kind"`
	Money                 money.JSON `json:"money"`
	FailureCode           string     `json:"failureCode"`
	RejectedAt            time.Time  `json:"rejectedAt"`
}

// NewWagerTransactionRejected builds the event for a REJECTED transaction.
func NewWagerTransactionRejected(tx *wagering.Transaction) (Event, error) {
	if tx.Status() != wagering.StatusRejected || tx.ProcessedAt() == nil {
		return Event{}, fmt.Errorf("%w: transaction is not rejected", ErrInvalidEvent)
	}
	data := WagerTransactionRejectedData{
		TransactionID: tx.ID(), ProviderID: tx.ProviderID(), ExternalTransactionID: tx.ExternalTransactionID(), WalletID: tx.WalletID(),
		PlayerID: tx.PlayerID(), RoundID: tx.RoundID(), GameID: tx.GameID(), Kind: string(tx.Kind()), Money: tx.Money().ToJSON(),
		FailureCode: string(tx.FailureCode()), RejectedAt: tx.ProcessedAt().UTC(),
	}
	return newEvent(TypeWagerTransactionRejected, 1, "WagerTransaction", tx.ID(), tx.CorrelationID(), tx.ID().String(), *tx.ProcessedAt(), data)
}

// WalletBalanceChangedData is the payload of WalletBalanceChanged.
type WalletBalanceChangedData struct {
	WalletID      uuid.UUID  `json:"walletId"`
	TransactionID uuid.UUID  `json:"transactionId"`
	Direction     string     `json:"direction"`
	Money         money.JSON `json:"money"`
	BalanceBefore money.JSON `json:"balanceBefore"`
	BalanceAfter  money.JSON `json:"balanceAfter"`
	WalletVersion int64      `json:"walletVersion"`
}

// NewWalletBalanceChanged builds the event for an effective balance change.
func NewWalletBalanceChanged(entry wallet.LedgerEntry, walletVersion int64, correlationID string) (Event, error) {
	if walletVersion < 1 {
		return Event{}, fmt.Errorf("%w: balance change requires a wallet version >= 1", ErrInvalidEvent)
	}
	data := WalletBalanceChangedData{
		WalletID: entry.WalletID(), TransactionID: entry.TransactionID(), Direction: string(entry.Direction()),
		Money: entry.Amount().ToJSON(), BalanceBefore: entry.BalanceBefore().ToJSON(), BalanceAfter: entry.BalanceAfter().ToJSON(),
		WalletVersion: walletVersion,
	}
	return newEvent(TypeWalletBalanceChanged, 1, "Wallet", entry.WalletID(), correlationID, entry.TransactionID().String(), entry.CreatedAt(), data)
}

// WagerTransactionPendingReferenceData is the payload of WagerTransactionPendingReference.
type WagerTransactionPendingReferenceData struct {
	TransactionID                  uuid.UUID  `json:"transactionId"`
	ProviderID                     string     `json:"providerId"`
	ExternalTransactionID          string     `json:"externalTransactionId"`
	ReferenceExternalTransactionID string     `json:"referenceExternalTransactionId"`
	WalletID                       uuid.UUID  `json:"walletId"`
	Kind                           string     `json:"kind"`
	Money                          money.JSON `json:"money"`
	Attempt                        int        `json:"attempt"`
	NextRetryAt                    time.Time  `json:"nextRetryAt"`
}

// NewWagerTransactionPendingReference builds the event for a parked transaction.
func NewWagerTransactionPendingReference(tx *wagering.Transaction) (Event, error) {
	if tx.Status() != wagering.StatusPendingReference || tx.NextReferenceRetryAt() == nil {
		return Event{}, fmt.Errorf("%w: transaction is not pending reference", ErrInvalidEvent)
	}
	data := WagerTransactionPendingReferenceData{
		TransactionID: tx.ID(), ProviderID: tx.ProviderID(), ExternalTransactionID: tx.ExternalTransactionID(),
		ReferenceExternalTransactionID: tx.ReferenceExternalID(), WalletID: tx.WalletID(), Kind: string(tx.Kind()),
		Money: tx.Money().ToJSON(), Attempt: tx.ReferenceAttempts(), NextRetryAt: tx.NextReferenceRetryAt().UTC(),
	}
	return newEvent(TypeWagerTransactionPendingReference, 1, "WagerTransaction", tx.ID(), tx.CorrelationID(), tx.ID().String(), tx.UpdatedAt(), data)
}
