// Package wagering models the provider operations applied to wallets and their
// state machine. The package is independent of Fx, HTTP, SQS and persistence.
package wagering

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/jamesmachome/backend-challenge-go/internal/domain/money"
)

// Kind of transaction. OPENING is reserved for internal wallet opening.
type Kind string

const (
	KindOpening  Kind = "OPENING"
	KindBet      Kind = "BET"
	KindWin      Kind = "WIN"
	KindLoss     Kind = "LOSS"
	KindRefund   Kind = "REFUND"
	KindRollback Kind = "ROLLBACK"
)

func ParseKind(s string) (Kind, error) {
	switch Kind(s) {
	case KindOpening, KindBet, KindWin, KindLoss, KindRefund, KindRollback:
		return Kind(s), nil
	}
	return "", fmt.Errorf("%w: unknown kind %q", ErrValidation, s)
}

// IsReversal reports whether the kind undoes another transaction.
func (k Kind) IsReversal() bool { return k == KindRefund || k == KindRollback }

// Status of a transaction.
type Status string

const (
	StatusPending          Status = "PENDING"
	StatusPendingReference Status = "PENDING_REFERENCE"
	StatusProcessed        Status = "PROCESSED"
	StatusRejected         Status = "REJECTED"
	StatusFailed           Status = "FAILED"
)

func ParseStatus(s string) (Status, error) {
	switch Status(s) {
	case StatusPending, StatusPendingReference, StatusProcessed, StatusRejected, StatusFailed:
		return Status(s), nil
	}
	return "", fmt.Errorf("%w: unknown status %q", ErrValidation, s)
}

// IsTerminal reports whether no further transition is allowed.
func (s Status) IsTerminal() bool {
	return s == StatusProcessed || s == StatusRejected || s == StatusFailed
}

// Origin distinguishes internal (OPENING) from external provider operations.
type Origin string

const (
	OriginInternal Origin = "INTERNAL"
	OriginExternal Origin = "EXTERNAL"
)

// FailureCode is the stable, documented reason for a rejection or failure.
type FailureCode string

const (
	// Definitive business rejections.
	FailureInsufficientFunds         FailureCode = "INSUFFICIENT_FUNDS"
	FailureReversalInsufficientFunds FailureCode = "REVERSAL_INSUFFICIENT_FUNDS"
	FailureReferenceNotFound         FailureCode = "REFERENCE_NOT_FOUND"
	FailureReferenceNotProcessed     FailureCode = "REFERENCE_NOT_PROCESSED"
	FailureReferenceKindMismatch     FailureCode = "REFERENCE_KIND_MISMATCH"
	FailureReferenceMismatch         FailureCode = "REFERENCE_MISMATCH"
	FailureReferenceAmountMismatch   FailureCode = "REFERENCE_AMOUNT_MISMATCH"
	FailureReferenceAlreadyReversed  FailureCode = "REFERENCE_ALREADY_REVERSED"
	FailureWalletPlayerMismatch      FailureCode = "WALLET_PLAYER_MISMATCH"
	FailureCurrencyMismatch          FailureCode = "CURRENCY_MISMATCH"
	// Permanent infrastructure failure recorded for audit.
	FailureInfrastructure FailureCode = "INFRASTRUCTURE_FAILURE"
)

var (
	ErrValidation        = errors.New("wagering: validation error")
	ErrInvalidTransition = errors.New("wagering: invalid state transition")
	ErrTerminal          = errors.New("wagering: transaction is terminal")
)

// ExternalRequest is a validated operation received from a provider through
// HTTP or SQS. It carries only business fields; transport metadata such as the
// idempotency key is handled separately.
type ExternalRequest struct {
	ProviderID                     string
	ExternalTransactionID          string
	PlayerID                       uuid.UUID
	WalletID                       uuid.UUID
	RoundID                        string
	GameID                         string
	Kind                           Kind
	Money                          money.Money
	ReferenceExternalTransactionID string
}

const maxIdentifierLen = 256

func validIdentifier(field, v string) error {
	if strings.TrimSpace(v) == "" {
		return fmt.Errorf("%w: %s is required", ErrValidation, field)
	}
	if len(v) > maxIdentifierLen || strings.TrimSpace(v) != v {
		return fmt.Errorf("%w: %s is malformed", ErrValidation, field)
	}
	return nil
}

// NewExternalRequest validates the business fields of a provider operation.
// It enforces the zero-value policy of each kind: LOSS requires exactly
// "0.00" and every other external kind requires a positive amount.
func NewExternalRequest(providerID, externalTransactionID string, playerID, walletID uuid.UUID, roundID, gameID string, kind Kind, m money.Money, reference string) (ExternalRequest, error) {
	for _, f := range []struct{ n, v string }{{"providerId", providerID}, {"externalTransactionId", externalTransactionID}, {"roundId", roundID}, {"gameId", gameID}} {
		if err := validIdentifier(f.n, f.v); err != nil {
			return ExternalRequest{}, err
		}
	}
	if playerID == uuid.Nil {
		return ExternalRequest{}, fmt.Errorf("%w: playerId is required", ErrValidation)
	}
	if walletID == uuid.Nil {
		return ExternalRequest{}, fmt.Errorf("%w: walletId is required", ErrValidation)
	}
	if _, err := ParseKind(string(kind)); err != nil {
		return ExternalRequest{}, err
	}
	if kind == KindOpening {
		return ExternalRequest{}, fmt.Errorf("%w: kind OPENING is reserved for internal use", ErrValidation)
	}
	if !m.IsValid() {
		return ExternalRequest{}, fmt.Errorf("%w: money is required", ErrValidation)
	}
	if m.IsNegative() {
		return ExternalRequest{}, fmt.Errorf("%w: money must not be negative", ErrValidation)
	}
	switch kind {
	case KindLoss:
		if !m.IsZero() {
			return ExternalRequest{}, fmt.Errorf("%w: LOSS requires money.amount \"0.00\"", ErrValidation)
		}
	default:
		if !m.IsPositive() {
			return ExternalRequest{}, fmt.Errorf("%w: %s requires a positive amount", ErrValidation, kind)
		}
	}
	switch {
	case kind.IsReversal() && reference == "":
		return ExternalRequest{}, fmt.Errorf("%w: referenceExternalTransactionId is required for %s", ErrValidation, kind)
	case (kind == KindBet || kind == KindLoss) && reference != "":
		return ExternalRequest{}, fmt.Errorf("%w: referenceExternalTransactionId is not allowed for %s", ErrValidation, kind)
	}
	if reference != "" {
		if err := validIdentifier("referenceExternalTransactionId", reference); err != nil {
			return ExternalRequest{}, err
		}
		if reference == externalTransactionID {
			return ExternalRequest{}, fmt.Errorf("%w: a transaction cannot reference itself", ErrValidation)
		}
	}
	return ExternalRequest{
		ProviderID: providerID, ExternalTransactionID: externalTransactionID, PlayerID: playerID, WalletID: walletID,
		RoundID: roundID, GameID: gameID, Kind: kind, Money: m, ReferenceExternalTransactionID: reference,
	}, nil
}

// Transaction is the WagerTransaction aggregate.
type Transaction struct {
	id                    uuid.UUID
	origin                Origin
	providerID            string
	externalTransactionID string
	idempotencyKey        string
	payloadHash           string
	walletID              uuid.UUID
	playerID              uuid.UUID
	roundID               string
	gameID                string
	kind                  Kind
	money                 money.Money
	referenceExternalID   string
	referenceID           *uuid.UUID
	status                Status
	failureCode           FailureCode
	resultBalance         *money.Money
	referenceAttempts     int
	nextReferenceRetryAt  *time.Time
	correlationID         string
	createdAt             time.Time
	updatedAt             time.Time
	processedAt           *time.Time
}

// NewExternal creates a PENDING external transaction.
func NewExternal(id uuid.UUID, req ExternalRequest, idempotencyKey, payloadHash, correlationID string, now time.Time) (*Transaction, error) {
	if id == uuid.Nil {
		return nil, fmt.Errorf("%w: id is required", ErrValidation)
	}
	if err := validIdentifier("Idempotency-Key", idempotencyKey); err != nil {
		return nil, err
	}
	if payloadHash == "" {
		return nil, fmt.Errorf("%w: payload hash is required", ErrValidation)
	}
	if now.IsZero() {
		return nil, fmt.Errorf("%w: zero timestamp", ErrValidation)
	}
	// Re-run the request validation so that a hand-built struct cannot bypass it.
	if _, err := NewExternalRequest(req.ProviderID, req.ExternalTransactionID, req.PlayerID, req.WalletID, req.RoundID, req.GameID, req.Kind, req.Money, req.ReferenceExternalTransactionID); err != nil {
		return nil, err
	}
	return &Transaction{
		id: id, origin: OriginExternal, providerID: req.ProviderID, externalTransactionID: req.ExternalTransactionID,
		idempotencyKey: idempotencyKey, payloadHash: payloadHash, walletID: req.WalletID, playerID: req.PlayerID,
		roundID: req.RoundID, gameID: req.GameID, kind: req.Kind, money: req.Money,
		referenceExternalID: req.ReferenceExternalTransactionID, status: StatusPending, correlationID: correlationID,
		createdAt: now.UTC(), updatedAt: now.UTC(),
	}, nil
}

// NewOpening creates the internal OPENING transaction for a wallet's initial
// positive credit. Provider metadata does not apply to this origin.
func NewOpening(id, walletID, playerID uuid.UUID, initial money.Money, correlationID string, now time.Time) (*Transaction, error) {
	if id == uuid.Nil || walletID == uuid.Nil || playerID == uuid.Nil {
		return nil, fmt.Errorf("%w: identity is required", ErrValidation)
	}
	if !initial.IsValid() || !initial.IsPositive() {
		return nil, fmt.Errorf("%w: OPENING requires a positive amount", ErrValidation)
	}
	if now.IsZero() {
		return nil, fmt.Errorf("%w: zero timestamp", ErrValidation)
	}
	return &Transaction{
		id: id, origin: OriginInternal, walletID: walletID, playerID: playerID, kind: KindOpening, money: initial,
		status: StatusPending, correlationID: correlationID, createdAt: now.UTC(), updatedAt: now.UTC(),
	}, nil
}

// Snapshot is the persisted shape used for rehydration.
type Snapshot struct {
	ID                    uuid.UUID
	Origin                Origin
	ProviderID            string
	ExternalTransactionID string
	IdempotencyKey        string
	PayloadHash           string
	WalletID              uuid.UUID
	PlayerID              uuid.UUID
	RoundID               string
	GameID                string
	Kind                  Kind
	Money                 money.Money
	ReferenceExternalID   string
	ReferenceID           *uuid.UUID
	Status                Status
	FailureCode           FailureCode
	ResultBalance         *money.Money
	ReferenceAttempts     int
	NextReferenceRetryAt  *time.Time
	CorrelationID         string
	CreatedAt             time.Time
	UpdatedAt             time.Time
	ProcessedAt           *time.Time
}

// Rehydrate rebuilds a transaction from persisted state without transitions.
func Rehydrate(s Snapshot) (*Transaction, error) {
	if s.ID == uuid.Nil || s.WalletID == uuid.Nil || s.PlayerID == uuid.Nil {
		return nil, fmt.Errorf("%w: identity is required", ErrValidation)
	}
	if _, err := ParseKind(string(s.Kind)); err != nil {
		return nil, err
	}
	if _, err := ParseStatus(string(s.Status)); err != nil {
		return nil, err
	}
	if !s.Money.IsValid() {
		return nil, fmt.Errorf("%w: money is required", ErrValidation)
	}
	if s.Origin != OriginInternal && s.Origin != OriginExternal {
		return nil, fmt.Errorf("%w: unknown origin %q", ErrValidation, s.Origin)
	}
	if (s.Origin == OriginInternal) != (s.Kind == KindOpening) {
		return nil, fmt.Errorf("%w: origin %s does not match kind %s", ErrValidation, s.Origin, s.Kind)
	}
	if s.CreatedAt.IsZero() || s.UpdatedAt.IsZero() {
		return nil, fmt.Errorf("%w: zero timestamp", ErrValidation)
	}
	return &Transaction{
		id: s.ID, origin: s.Origin, providerID: s.ProviderID, externalTransactionID: s.ExternalTransactionID,
		idempotencyKey: s.IdempotencyKey, payloadHash: s.PayloadHash, walletID: s.WalletID, playerID: s.PlayerID,
		roundID: s.RoundID, gameID: s.GameID, kind: s.Kind, money: s.Money, referenceExternalID: s.ReferenceExternalID,
		referenceID: s.ReferenceID, status: s.Status, failureCode: s.FailureCode, resultBalance: s.ResultBalance,
		referenceAttempts: s.ReferenceAttempts, nextReferenceRetryAt: utcPtr(s.NextReferenceRetryAt), correlationID: s.CorrelationID,
		createdAt: s.CreatedAt.UTC(), updatedAt: s.UpdatedAt.UTC(), processedAt: utcPtr(s.ProcessedAt),
	}, nil
}

// Snapshot exports the state for persistence.
func (t *Transaction) Snapshot() Snapshot {
	return Snapshot{
		ID: t.id, Origin: t.origin, ProviderID: t.providerID, ExternalTransactionID: t.externalTransactionID,
		IdempotencyKey: t.idempotencyKey, PayloadHash: t.payloadHash, WalletID: t.walletID, PlayerID: t.playerID,
		RoundID: t.roundID, GameID: t.gameID, Kind: t.kind, Money: t.money, ReferenceExternalID: t.referenceExternalID,
		ReferenceID: t.referenceID, Status: t.status, FailureCode: t.failureCode, ResultBalance: t.resultBalance,
		ReferenceAttempts: t.referenceAttempts, NextReferenceRetryAt: t.nextReferenceRetryAt, CorrelationID: t.correlationID,
		CreatedAt: t.createdAt, UpdatedAt: t.updatedAt, ProcessedAt: t.processedAt,
	}
}

func (t *Transaction) ID() uuid.UUID                    { return t.id }
func (t *Transaction) Origin() Origin                   { return t.origin }
func (t *Transaction) ProviderID() string               { return t.providerID }
func (t *Transaction) ExternalTransactionID() string    { return t.externalTransactionID }
func (t *Transaction) IdempotencyKey() string           { return t.idempotencyKey }
func (t *Transaction) PayloadHash() string              { return t.payloadHash }
func (t *Transaction) WalletID() uuid.UUID              { return t.walletID }
func (t *Transaction) PlayerID() uuid.UUID              { return t.playerID }
func (t *Transaction) RoundID() string                  { return t.roundID }
func (t *Transaction) GameID() string                   { return t.gameID }
func (t *Transaction) Kind() Kind                       { return t.kind }
func (t *Transaction) Money() money.Money               { return t.money }
func (t *Transaction) ReferenceExternalID() string      { return t.referenceExternalID }
func (t *Transaction) ReferenceID() *uuid.UUID          { return t.referenceID }
func (t *Transaction) Status() Status                   { return t.status }
func (t *Transaction) FailureCode() FailureCode         { return t.failureCode }
func (t *Transaction) ResultBalance() *money.Money      { return t.resultBalance }
func (t *Transaction) ReferenceAttempts() int           { return t.referenceAttempts }
func (t *Transaction) NextReferenceRetryAt() *time.Time { return t.nextReferenceRetryAt }
func (t *Transaction) CorrelationID() string            { return t.correlationID }
func (t *Transaction) CreatedAt() time.Time             { return t.createdAt }
func (t *Transaction) UpdatedAt() time.Time             { return t.updatedAt }
func (t *Transaction) ProcessedAt() *time.Time          { return t.processedAt }

func (t *Transaction) guard(target Status) error {
	if t.status.IsTerminal() {
		return fmt.Errorf("%w: %s -> %s", ErrTerminal, t.status, target)
	}
	return nil
}

// MarkProcessed completes the transaction with the balance observed after
// processing. Allowed from PENDING and PENDING_REFERENCE.
func (t *Transaction) MarkProcessed(balance money.Money, now time.Time) error {
	if err := t.guard(StatusProcessed); err != nil {
		return err
	}
	if !balance.IsValid() || balance.Currency() != t.money.Currency() {
		return fmt.Errorf("%w: result balance is invalid", ErrInvalidTransition)
	}
	t.status = StatusProcessed
	t.failureCode = ""
	t.resultBalance = &balance
	t.nextReferenceRetryAt = nil
	n := now.UTC()
	t.processedAt = &n
	t.updatedAt = n
	return nil
}

// MarkRejected records a definitive business rejection. The observed balance
// is kept so that replays return what the provider saw originally.
func (t *Transaction) MarkRejected(code FailureCode, balance money.Money, now time.Time) error {
	if err := t.guard(StatusRejected); err != nil {
		return err
	}
	if code == "" || code == FailureInfrastructure {
		return fmt.Errorf("%w: rejection requires a business failure code", ErrInvalidTransition)
	}
	t.status = StatusRejected
	t.failureCode = code
	if balance.IsValid() {
		b := balance
		t.resultBalance = &b
	}
	t.nextReferenceRetryAt = nil
	n := now.UTC()
	t.processedAt = &n
	t.updatedAt = n
	return nil
}

// MarkFailed records a permanent infrastructure failure for audit.
func (t *Transaction) MarkFailed(code FailureCode, now time.Time) error {
	if err := t.guard(StatusFailed); err != nil {
		return err
	}
	if code == "" {
		code = FailureInfrastructure
	}
	t.status = StatusFailed
	t.failureCode = code
	t.nextReferenceRetryAt = nil
	n := now.UTC()
	t.processedAt = &n
	t.updatedAt = n
	return nil
}

// AwaitReference parks the transaction until its reference arrives. The
// attempt counter is incremented and the next retry is scheduled.
func (t *Transaction) AwaitReference(nextRetry, now time.Time) error {
	if err := t.guard(StatusPendingReference); err != nil {
		return err
	}
	if !t.kind.IsReversal() && t.kind != KindWin {
		return fmt.Errorf("%w: %s cannot wait for a reference", ErrInvalidTransition, t.kind)
	}
	if t.referenceExternalID == "" {
		return fmt.Errorf("%w: no reference to wait for", ErrInvalidTransition)
	}
	t.status = StatusPendingReference
	t.referenceAttempts++
	nr := nextRetry.UTC()
	t.nextReferenceRetryAt = &nr
	t.updatedAt = now.UTC()
	return nil
}

// ResolveReference records the internal id of the referenced transaction.
func (t *Transaction) ResolveReference(id uuid.UUID) error {
	if t.status.IsTerminal() {
		return fmt.Errorf("%w: cannot resolve reference", ErrTerminal)
	}
	if id == uuid.Nil {
		return fmt.Errorf("%w: nil reference", ErrInvalidTransition)
	}
	t.referenceID = &id
	return nil
}

// RejectionError is returned by use cases when a transaction was rejected by a
// business rule. It is a normal outcome, persisted as REJECTED.
type RejectionError struct {
	Code FailureCode
	Msg  string
}

func (e *RejectionError) Error() string { return fmt.Sprintf("rejected (%s): %s", e.Code, e.Msg) }

// Reject builds a RejectionError.
func Reject(code FailureCode, format string, a ...any) *RejectionError {
	return &RejectionError{Code: code, Msg: fmt.Sprintf(format, a...)}
}

func utcPtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}
