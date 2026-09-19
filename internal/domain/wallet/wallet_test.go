package wallet

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/jamesmachome/backend-challenge-go/internal/domain/money"
)

var now = time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)

func brl(minor int64) money.Money { return money.MustFromMinor(minor, money.BRL) }

func newWallet(t *testing.T, minor int64) *Wallet {
	t.Helper()
	w, err := Open(uuid.New(), uuid.New(), brl(minor), now)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func TestOpen(t *testing.T) {
	w := newWallet(t, 10000)
	if w.Version() != 1 || w.Balance().Minor() != 10000 || w.Currency() != money.BRL {
		t.Fatalf("unexpected state %+v", w)
	}
	if _, err := Open(uuid.Nil, uuid.New(), brl(1), now); !errors.Is(err, ErrInvalidWallet) {
		t.Error("nil id")
	}
	if _, err := Open(uuid.New(), uuid.New(), money.Money{}, now); !errors.Is(err, ErrInvalidWallet) {
		t.Error("uninitialized money")
	}
	if _, err := Open(uuid.New(), uuid.New(), brl(-1), now); !errors.Is(err, ErrInvalidWallet) {
		t.Error("negative")
	}
	if _, err := Open(uuid.New(), uuid.New(), brl(0), now); err != nil {
		t.Error("zero initial balance is allowed")
	}
}

func TestRehydrateDoesNotReapply(t *testing.T) {
	id := uuid.New()
	w, err := Rehydrate(id, uuid.New(), brl(500), 7, now, now)
	if err != nil {
		t.Fatal(err)
	}
	if w.Version() != 7 || w.Balance().Minor() != 500 {
		t.Fatal("rehydration changed state")
	}
	if _, err := Rehydrate(id, uuid.New(), brl(500), 0, now, now); !errors.Is(err, ErrInvalidWallet) {
		t.Error("version 0")
	}
	if _, err := Rehydrate(id, uuid.New(), brl(-5), 1, now, now); !errors.Is(err, ErrInvalidWallet) {
		t.Error("negative balance")
	}
}

func TestDebitCredit(t *testing.T) {
	w := newWallet(t, 10000)
	txID := uuid.New()
	e, err := w.Debit(uuid.New(), txID, brl(2500), now)
	if err != nil {
		t.Fatal(err)
	}
	if e.Direction() != Debit || e.BalanceBefore().Minor() != 10000 || e.BalanceAfter().Minor() != 7500 || e.TransactionID() != txID {
		t.Fatalf("bad entry %+v", e)
	}
	if w.Balance().Minor() != 7500 || w.Version() != 2 {
		t.Fatalf("bad wallet: %d v%d", w.Balance().Minor(), w.Version())
	}
	e, err = w.Credit(uuid.New(), uuid.New(), brl(100), now)
	if err != nil || e.Direction() != Credit || e.BalanceAfter().Minor() != 7600 {
		t.Fatal(err)
	}
	if w.Version() != 3 {
		t.Fatal("version")
	}
}

func TestDebitInsufficient(t *testing.T) {
	w := newWallet(t, 10000)
	if _, err := w.Debit(uuid.New(), uuid.New(), brl(10001), now); !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("expected insufficient funds, got %v", err)
	}
	if w.Balance().Minor() != 10000 || w.Version() != 1 {
		t.Fatal("failed debit must not mutate")
	}
	if _, err := w.Debit(uuid.New(), uuid.New(), brl(10000), now); err != nil {
		t.Fatal("debit to exactly zero must succeed")
	}
	ok, _ := w.CanDebit(brl(1))
	if ok {
		t.Fatal("CanDebit on empty wallet")
	}
}

func TestMovementValidation(t *testing.T) {
	w := newWallet(t, 100)
	if _, err := w.Credit(uuid.New(), uuid.New(), money.MustFromMinor(1, "USD"), now); !errors.Is(err, ErrCurrencyMismatch) {
		t.Error("currency")
	}
	if _, err := w.Credit(uuid.New(), uuid.New(), brl(0), now); !errors.Is(err, ErrNonPositiveAmount) {
		t.Error("zero")
	}
	if _, err := w.Debit(uuid.New(), uuid.New(), brl(-1), now); !errors.Is(err, ErrNonPositiveAmount) {
		t.Error("negative")
	}
	if _, err := w.Debit(uuid.New(), uuid.New(), money.Money{}, now); !errors.Is(err, money.ErrUninitialized) {
		t.Error("uninitialized")
	}
	if w.Version() != 1 {
		t.Error("version changed on rejected movement")
	}
}

func TestLedgerEntryInvariant(t *testing.T) {
	id, wid, tid := uuid.New(), uuid.New(), uuid.New()
	if _, err := NewLedgerEntry(id, wid, tid, Credit, brl(10), brl(100), brl(110), now); err != nil {
		t.Fatal(err)
	}
	if _, err := NewLedgerEntry(id, wid, tid, Debit, brl(10), brl(100), brl(90), now); err != nil {
		t.Fatal(err)
	}
	bad := []struct {
		name    string
		dir     Direction
		a, b, c money.Money
	}{
		{"credit wrong after", Credit, brl(10), brl(100), brl(90)},
		{"debit wrong after", Debit, brl(10), brl(100), brl(110)},
		{"zero amount", Credit, brl(0), brl(100), brl(100)},
		{"negative before", Credit, brl(10), brl(-10), brl(0)},
		{"currency", Credit, money.MustFromMinor(10, "USD"), brl(100), brl(110)},
		{"bad direction", Direction("SIDEWAYS"), brl(10), brl(100), brl(110)},
	}
	for _, c := range bad {
		if _, err := NewLedgerEntry(id, wid, tid, c.dir, c.a, c.b, c.c, now); !errors.Is(err, ErrInvalidLedger) {
			t.Errorf("%s: expected ErrInvalidLedger, got %v", c.name, err)
		}
	}
	if _, err := NewLedgerEntry(uuid.Nil, wid, tid, Credit, brl(10), brl(100), brl(110), now); !errors.Is(err, ErrInvalidLedger) {
		t.Error("nil id")
	}
}
