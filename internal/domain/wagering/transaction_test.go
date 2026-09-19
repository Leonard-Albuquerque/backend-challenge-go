package wagering

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/jamesmachome/backend-challenge-go/internal/domain/money"
)

var now = time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)

func brl(minor int64) money.Money { return money.MustFromMinor(minor, money.BRL) }

func req(t *testing.T, kind Kind, minor int64, ref string) ExternalRequest {
	t.Helper()
	r, err := NewExternalRequest("provider-a", "tx-1", uuid.New(), uuid.New(), "round-1", "game-1", kind, brl(minor), ref)
	if err != nil {
		t.Fatalf("%s: %v", kind, err)
	}
	return r
}

func TestExternalRequestRules(t *testing.T) {
	req(t, KindBet, 100, "")
	req(t, KindWin, 100, "")
	req(t, KindWin, 100, "bet-1")
	req(t, KindLoss, 0, "")
	req(t, KindRefund, 100, "bet-1")
	req(t, KindRollback, 100, "bet-1")

	bad := []struct {
		name string
		kind Kind
		m    int64
		ref  string
	}{
		{"bet zero", KindBet, 0, ""},
		{"win zero", KindWin, 0, ""},
		{"loss non-zero", KindLoss, 1, ""},
		{"refund zero", KindRefund, 0, "bet-1"},
		{"rollback zero", KindRollback, 0, "bet-1"},
		{"refund without reference", KindRefund, 100, ""},
		{"rollback without reference", KindRollback, 100, ""},
		{"bet with reference", KindBet, 100, "bet-1"},
		{"loss with reference", KindLoss, 0, "bet-1"},
		{"opening via external", KindOpening, 100, ""},
		{"unknown kind", Kind("DEPOSIT"), 100, ""},
		{"self reference", KindRefund, 100, "tx-1"},
	}
	for _, c := range bad {
		_, err := NewExternalRequest("provider-a", "tx-1", uuid.New(), uuid.New(), "round-1", "game-1", c.kind, brl(c.m), c.ref)
		if !errors.Is(err, ErrValidation) {
			t.Errorf("%s: expected validation error, got %v", c.name, err)
		}
	}
	if _, err := NewExternalRequest("", "tx-1", uuid.New(), uuid.New(), "r", "g", KindBet, brl(1), ""); !errors.Is(err, ErrValidation) {
		t.Error("empty provider")
	}
	if _, err := NewExternalRequest("p", "tx-1", uuid.Nil, uuid.New(), "r", "g", KindBet, brl(1), ""); !errors.Is(err, ErrValidation) {
		t.Error("nil player")
	}
	if _, err := NewExternalRequest("p", "tx-1", uuid.New(), uuid.New(), "r", "g", KindBet, money.Money{}, ""); !errors.Is(err, ErrValidation) {
		t.Error("uninitialized money")
	}
}

func newTx(t *testing.T, kind Kind, minor int64, ref string) *Transaction {
	t.Helper()
	tx, err := NewExternal(uuid.New(), req(t, kind, minor, ref), "provider-a:tx-1", "hash", "corr", now)
	if err != nil {
		t.Fatal(err)
	}
	return tx
}

func TestStateMachine(t *testing.T) {
	tx := newTx(t, KindBet, 100, "")
	if tx.Status() != StatusPending || tx.Origin() != OriginExternal {
		t.Fatal("initial state")
	}
	if err := tx.MarkProcessed(brl(900), now); err != nil {
		t.Fatal(err)
	}
	if tx.Status() != StatusProcessed || tx.ResultBalance().Minor() != 900 || tx.ProcessedAt() == nil {
		t.Fatal("processed state")
	}
	for _, err := range []error{
		tx.MarkRejected(FailureInsufficientFunds, brl(1), now),
		tx.MarkFailed(FailureInfrastructure, now),
		tx.MarkProcessed(brl(1), now),
		tx.AwaitReference(now, now),
		tx.ResolveReference(uuid.New()),
	} {
		if !errors.Is(err, ErrTerminal) {
			t.Errorf("terminal transition allowed: %v", err)
		}
	}
	if tx.Status() != StatusProcessed {
		t.Fatal("terminal state mutated")
	}

	rej := newTx(t, KindBet, 100, "")
	if err := rej.MarkRejected(FailureInsufficientFunds, brl(50), now); err != nil {
		t.Fatal(err)
	}
	if rej.Status() != StatusRejected || rej.FailureCode() != FailureInsufficientFunds {
		t.Fatal("rejected state")
	}
	if err := newTx(t, KindBet, 100, "").MarkRejected("", brl(1), now); !errors.Is(err, ErrInvalidTransition) {
		t.Error("rejection without code")
	}
	if err := newTx(t, KindBet, 100, "").MarkProcessed(money.MustFromMinor(1, "USD"), now); !errors.Is(err, ErrInvalidTransition) {
		t.Error("processed with wrong currency")
	}

	failed := newTx(t, KindBet, 100, "")
	if err := failed.MarkFailed("", now); err != nil || failed.FailureCode() != FailureInfrastructure {
		t.Fatal("failed default code")
	}
}

func TestPendingReference(t *testing.T) {
	tx := newTx(t, KindRefund, 100, "bet-1")
	next := now.Add(time.Second)
	if err := tx.AwaitReference(next, now); err != nil {
		t.Fatal(err)
	}
	if tx.Status() != StatusPendingReference || tx.ReferenceAttempts() != 1 || !tx.NextReferenceRetryAt().Equal(next) {
		t.Fatal("pending state")
	}
	if err := tx.AwaitReference(next.Add(time.Second), now); err != nil || tx.ReferenceAttempts() != 2 {
		t.Fatal("second attempt")
	}
	ref := uuid.New()
	if err := tx.ResolveReference(ref); err != nil || *tx.ReferenceID() != ref {
		t.Fatal("resolve")
	}
	if err := tx.MarkProcessed(brl(200), now); err != nil || tx.NextReferenceRetryAt() != nil {
		t.Fatal("processed from pending reference")
	}
	pend := newTx(t, KindRollback, 100, "bet-1")
	_ = pend.AwaitReference(next, now)
	if err := pend.MarkRejected(FailureReferenceNotFound, brl(0), now); err != nil || pend.Status() != StatusRejected {
		t.Fatal("rejected from pending reference")
	}
	if err := newTx(t, KindBet, 100, "").AwaitReference(next, now); !errors.Is(err, ErrInvalidTransition) {
		t.Error("BET cannot await a reference")
	}
}

func TestOpening(t *testing.T) {
	op, err := NewOpening(uuid.New(), uuid.New(), uuid.New(), brl(1000), "corr", now)
	if err != nil {
		t.Fatal(err)
	}
	if op.Origin() != OriginInternal || op.Kind() != KindOpening || op.ProviderID() != "" || op.IdempotencyKey() != "" || op.RoundID() != "" {
		t.Fatal("opening metadata must be empty")
	}
	if _, err := NewOpening(uuid.New(), uuid.New(), uuid.New(), brl(0), "corr", now); !errors.Is(err, ErrValidation) {
		t.Error("zero opening")
	}
	if _, err := NewOpening(uuid.New(), uuid.Nil, uuid.New(), brl(1), "corr", now); !errors.Is(err, ErrValidation) {
		t.Error("nil wallet")
	}
}

func TestRehydrate(t *testing.T) {
	tx := newTx(t, KindWin, 100, "bet-1")
	_ = tx.MarkProcessed(brl(300), now)
	snap := tx.Snapshot()
	re, err := Rehydrate(snap)
	if err != nil {
		t.Fatal(err)
	}
	if re.Status() != StatusProcessed || re.ResultBalance().Minor() != 300 || re.Kind() != KindWin || re.ReferenceExternalID() != "bet-1" {
		t.Fatal("rehydration lost state")
	}
	bad := snap
	bad.Origin = OriginInternal
	if _, err := Rehydrate(bad); !errors.Is(err, ErrValidation) {
		t.Error("origin/kind mismatch")
	}
	bad = snap
	bad.Status = "WEIRD"
	if _, err := Rehydrate(bad); !errors.Is(err, ErrValidation) {
		t.Error("unknown status")
	}
}

func TestNewExternalValidation(t *testing.T) {
	r := req(t, KindBet, 100, "")
	if _, err := NewExternal(uuid.New(), r, "", "hash", "c", now); !errors.Is(err, ErrValidation) {
		t.Error("empty key")
	}
	if _, err := NewExternal(uuid.New(), r, "k", "", "c", now); !errors.Is(err, ErrValidation) {
		t.Error("empty hash")
	}
	r.Kind = KindOpening
	if _, err := NewExternal(uuid.New(), r, "k", "h", "c", now); !errors.Is(err, ErrValidation) {
		t.Error("hand-built OPENING")
	}
}
