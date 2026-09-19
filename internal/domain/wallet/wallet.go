// Package wallet holds the financial aggregate root and its ledger entries.
// It depends only on the money package and the standard library.
package wallet

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/jamesmachome/backend-challenge-go/internal/domain/money"
)

var (
	ErrInvalidWallet     = errors.New("wallet: invalid wallet")
	ErrInsufficientFunds = errors.New("wallet: insufficient funds")
	ErrCurrencyMismatch  = errors.New("wallet: currency mismatch")
	ErrNonPositiveAmount = errors.New("wallet: amount must be positive")
	ErrInvalidLedger     = errors.New("wallet: invalid ledger entry")
)

// Wallet is the aggregate root that owns a player's balance in one currency.
// Its state is private; balance changes are only possible through Debit and
// Credit, which also produce the ledger entry that must be persisted in the
// same SQL transaction.
type Wallet struct {
	id        uuid.UUID
	playerID  uuid.UUID
	currency  money.Currency
	balance   money.Money
	version   int64
	createdAt time.Time
	updatedAt time.Time
}

// Open creates a new wallet with the given initial balance. Version starts at 1.
// The initial credit ledger entry (if any) is produced by the caller through
// the OPENING transaction so that creation and rehydration stay separate.
func Open(id, playerID uuid.UUID, initial money.Money, now time.Time) (*Wallet, error) {
	if id == uuid.Nil || playerID == uuid.Nil {
		return nil, fmt.Errorf("%w: missing identity", ErrInvalidWallet)
	}
	if !initial.IsValid() {
		return nil, fmt.Errorf("%w: %v", ErrInvalidWallet, money.ErrUninitialized)
	}
	if initial.IsNegative() {
		return nil, fmt.Errorf("%w: negative initial balance", ErrInvalidWallet)
	}
	if now.IsZero() {
		return nil, fmt.Errorf("%w: zero timestamp", ErrInvalidWallet)
	}
	return &Wallet{
		id: id, playerID: playerID, currency: initial.Currency(), balance: initial,
		version: 1, createdAt: now.UTC(), updatedAt: now.UTC(),
	}, nil
}

// Rehydrate rebuilds a wallet from persisted state without applying any
// movement or emitting events.
func Rehydrate(id, playerID uuid.UUID, balance money.Money, version int64, createdAt, updatedAt time.Time) (*Wallet, error) {
	if id == uuid.Nil || playerID == uuid.Nil {
		return nil, fmt.Errorf("%w: missing identity", ErrInvalidWallet)
	}
	if !balance.IsValid() || balance.IsNegative() {
		return nil, fmt.Errorf("%w: invalid balance", ErrInvalidWallet)
	}
	if version < 1 {
		return nil, fmt.Errorf("%w: version must be >= 1", ErrInvalidWallet)
	}
	if createdAt.IsZero() || updatedAt.IsZero() {
		return nil, fmt.Errorf("%w: zero timestamp", ErrInvalidWallet)
	}
	return &Wallet{id: id, playerID: playerID, currency: balance.Currency(), balance: balance, version: version, createdAt: createdAt.UTC(), updatedAt: updatedAt.UTC()}, nil
}

func (w *Wallet) ID() uuid.UUID            { return w.id }
func (w *Wallet) PlayerID() uuid.UUID      { return w.playerID }
func (w *Wallet) Currency() money.Currency { return w.currency }
func (w *Wallet) Balance() money.Money     { return w.balance }
func (w *Wallet) Version() int64           { return w.version }
func (w *Wallet) CreatedAt() time.Time     { return w.createdAt }
func (w *Wallet) UpdatedAt() time.Time     { return w.updatedAt }

func (w *Wallet) validateMovement(amount money.Money) error {
	if !amount.IsValid() {
		return money.ErrUninitialized
	}
	if amount.Currency() != w.currency {
		return fmt.Errorf("%w: wallet %s, movement %s", ErrCurrencyMismatch, w.currency, amount.Currency())
	}
	if !amount.IsPositive() {
		return ErrNonPositiveAmount
	}
	return nil
}

// Debit removes amount from the balance, failing when the result would be
// negative. On success the version is incremented and the ledger entry is
// returned; the caller must persist wallet and entry atomically.
func (w *Wallet) Debit(entryID, transactionID uuid.UUID, amount money.Money, now time.Time) (LedgerEntry, error) {
	if err := w.validateMovement(amount); err != nil {
		return LedgerEntry{}, err
	}
	after, err := w.balance.Sub(amount)
	if err != nil {
		return LedgerEntry{}, err
	}
	if after.IsNegative() {
		return LedgerEntry{}, fmt.Errorf("%w: balance %s, debit %s", ErrInsufficientFunds, w.balance.Amount(), amount.Amount())
	}
	entry, err := NewLedgerEntry(entryID, w.id, transactionID, Debit, amount, w.balance, after, now)
	if err != nil {
		return LedgerEntry{}, err
	}
	w.apply(after, now)
	return entry, nil
}

// Credit adds amount to the balance.
func (w *Wallet) Credit(entryID, transactionID uuid.UUID, amount money.Money, now time.Time) (LedgerEntry, error) {
	if err := w.validateMovement(amount); err != nil {
		return LedgerEntry{}, err
	}
	after, err := w.balance.Add(amount)
	if err != nil {
		return LedgerEntry{}, err
	}
	entry, err := NewLedgerEntry(entryID, w.id, transactionID, Credit, amount, w.balance, after, now)
	if err != nil {
		return LedgerEntry{}, err
	}
	w.apply(after, now)
	return entry, nil
}

// CanDebit reports whether a debit of amount would keep the balance >= 0.
func (w *Wallet) CanDebit(amount money.Money) (bool, error) {
	if err := w.validateMovement(amount); err != nil {
		return false, err
	}
	c, err := w.balance.Compare(amount)
	if err != nil {
		return false, err
	}
	return c >= 0, nil
}

func (w *Wallet) apply(after money.Money, now time.Time) {
	w.balance = after
	w.version++
	w.updatedAt = now.UTC()
}

// Direction of a ledger entry.
type Direction string

const (
	Debit  Direction = "DEBIT"
	Credit Direction = "CREDIT"
)

func ParseDirection(s string) (Direction, error) {
	switch Direction(s) {
	case Debit, Credit:
		return Direction(s), nil
	}
	return "", fmt.Errorf("%w: unknown direction %q", ErrInvalidLedger, s)
}

// LedgerEntry is an immutable, append-only record of one balance change.
type LedgerEntry struct {
	id            uuid.UUID
	walletID      uuid.UUID
	transactionID uuid.UUID
	direction     Direction
	amount        money.Money
	balanceBefore money.Money
	balanceAfter  money.Money
	createdAt     time.Time
}

// NewLedgerEntry validates balanceAfter = balanceBefore ± amount according to
// the direction. It is used both for creation and rehydration, because the
// invariant must hold for persisted rows as well.
func NewLedgerEntry(id, walletID, transactionID uuid.UUID, dir Direction, amount, before, after money.Money, now time.Time) (LedgerEntry, error) {
	if id == uuid.Nil || walletID == uuid.Nil || transactionID == uuid.Nil {
		return LedgerEntry{}, fmt.Errorf("%w: missing identity", ErrInvalidLedger)
	}
	if _, err := ParseDirection(string(dir)); err != nil {
		return LedgerEntry{}, err
	}
	if !amount.IsValid() || !before.IsValid() || !after.IsValid() {
		return LedgerEntry{}, fmt.Errorf("%w: %v", ErrInvalidLedger, money.ErrUninitialized)
	}
	if !amount.SameCurrency(before) || !amount.SameCurrency(after) {
		return LedgerEntry{}, fmt.Errorf("%w: %v", ErrInvalidLedger, money.ErrCurrencyMismatch)
	}
	if !amount.IsPositive() {
		return LedgerEntry{}, fmt.Errorf("%w: amount must be positive", ErrInvalidLedger)
	}
	if before.IsNegative() || after.IsNegative() {
		return LedgerEntry{}, fmt.Errorf("%w: balances must be non-negative", ErrInvalidLedger)
	}
	var expected money.Money
	var err error
	if dir == Credit {
		expected, err = before.Add(amount)
	} else {
		expected, err = before.Sub(amount)
	}
	if err != nil {
		return LedgerEntry{}, fmt.Errorf("%w: %v", ErrInvalidLedger, err)
	}
	if !expected.Equal(after) {
		return LedgerEntry{}, fmt.Errorf("%w: balanceAfter %s != balanceBefore %s %s %s", ErrInvalidLedger, after.Amount(), before.Amount(), dir, amount.Amount())
	}
	if now.IsZero() {
		return LedgerEntry{}, fmt.Errorf("%w: zero timestamp", ErrInvalidLedger)
	}
	return LedgerEntry{id: id, walletID: walletID, transactionID: transactionID, direction: dir, amount: amount, balanceBefore: before, balanceAfter: after, createdAt: now.UTC()}, nil
}

func (e LedgerEntry) ID() uuid.UUID              { return e.id }
func (e LedgerEntry) WalletID() uuid.UUID        { return e.walletID }
func (e LedgerEntry) TransactionID() uuid.UUID   { return e.transactionID }
func (e LedgerEntry) Direction() Direction       { return e.direction }
func (e LedgerEntry) Amount() money.Money        { return e.amount }
func (e LedgerEntry) BalanceBefore() money.Money { return e.balanceBefore }
func (e LedgerEntry) BalanceAfter() money.Money  { return e.balanceAfter }
func (e LedgerEntry) CreatedAt() time.Time       { return e.createdAt }
