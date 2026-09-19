package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/jamesmachome/backend-challenge-go/internal/domain/events"
	"github.com/jamesmachome/backend-challenge-go/internal/domain/money"
	"github.com/jamesmachome/backend-challenge-go/internal/domain/wagering"
	"github.com/jamesmachome/backend-challenge-go/internal/domain/wallet"
)

type fixture struct {
	t     *testing.T
	store *memStore
	svc   *Service
	clock time.Time
	ctx   context.Context
	w     *wallet.Wallet
}

func brl(minor int64) money.Money { return money.MustFromMinor(minor, money.BRL) }

func newFixture(t *testing.T, initial int64) *fixture {
	t.Helper()
	f := &fixture{t: t, store: newMemStore(), clock: time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC), ctx: context.Background()}
	cfg := DefaultConfig()
	cfg.PendingReferenceMaxAttempts = 3
	cfg.PendingReferenceBaseBackoff = time.Second
	f.svc = NewService(f.store, nil, nil, cfg, func() time.Time { return f.clock })
	w, err := f.svc.OpenWallet(f.ctx, OpenWalletCommand{PlayerID: uuid.New(), InitialBalance: brl(initial), CorrelationID: "corr-open"})
	if err != nil {
		t.Fatal(err)
	}
	f.w = w
	return f
}

func (f *fixture) req(ext string, kind wagering.Kind, minor int64, ref string) wagering.ExternalRequest {
	f.t.Helper()
	r, err := wagering.NewExternalRequest("provider-a", ext, f.w.PlayerID(), f.w.ID(), "round-1", "game-1", kind, brl(minor), ref)
	if err != nil {
		f.t.Fatal(err)
	}
	return r
}

func (f *fixture) send(ext string, kind wagering.Kind, minor int64, ref string) ProcessResult {
	f.t.Helper()
	res, err := f.svc.Process(f.ctx, ProcessCommand{Request: f.req(ext, kind, minor, ref), IdempotencyKey: "provider-a:" + ext, CorrelationID: "corr-" + ext, Source: SourceHTTP})
	if err != nil {
		f.t.Fatalf("%s %s: %v", kind, ext, err)
	}
	return res
}

func (f *fixture) balance() int64 {
	w, err := f.svc.GetWallet(f.ctx, f.w.ID())
	if err != nil {
		f.t.Fatal(err)
	}
	return w.Balance().Minor()
}

func (f *fixture) version() int64 {
	w, _ := f.svc.GetWallet(f.ctx, f.w.ID())
	return w.Version()
}

func expectStatus(t *testing.T, res ProcessResult, status wagering.Status, code wagering.FailureCode) {
	t.Helper()
	if res.Transaction.Status() != status || res.Transaction.FailureCode() != code {
		t.Fatalf("expected %s/%s, got %s/%s", status, code, res.Transaction.Status(), res.Transaction.FailureCode())
	}
}

func TestOpeningCreatesLedgerAndEvents(t *testing.T) {
	f := newFixture(t, 100000)
	if f.w.Version() != 1 || f.w.Balance().Minor() != 100000 {
		t.Fatal("wallet state")
	}
	if len(f.store.ledger) != 1 || f.store.ledger[0].Entry.Direction() != wallet.Credit || f.store.ledger[0].Entry.BalanceBefore().Minor() != 0 {
		t.Fatalf("ledger: %+v", f.store.ledger)
	}
	var opening *wagering.Snapshot
	for _, s := range f.store.txs {
		s := s
		opening = &s
	}
	if opening == nil || opening.Kind != wagering.KindOpening || opening.Origin != wagering.OriginInternal || opening.Status != wagering.StatusProcessed || opening.ProviderID != "" || opening.IdempotencyKey != "" {
		t.Fatalf("opening: %+v", opening)
	}
	types := f.store.eventTypes()
	if len(types) != 2 || types[0] != events.TypeWagerTransactionProcessed || types[1] != events.TypeWalletBalanceChanged {
		t.Fatalf("events: %v", types)
	}
	var env = f.store.outbox[1].Envelope()
	if env.Version != 1 || env.AggregateID != f.w.ID() || env.CorrelationID != "corr-open" {
		t.Fatalf("envelope: %+v", env)
	}
	// Duplicate wallet for the same player/currency conflicts.
	_, err := f.svc.OpenWallet(f.ctx, OpenWalletCommand{PlayerID: f.w.PlayerID(), InitialBalance: brl(1)})
	if !errors.Is(err, ErrWalletExists) {
		t.Fatalf("expected ErrWalletExists, got %v", err)
	}
}

func TestOpeningZeroBalanceHasNoSideEffects(t *testing.T) {
	f := newFixture(t, 0)
	if len(f.store.ledger) != 0 || len(f.store.txs) != 0 || len(f.store.outbox) != 0 {
		t.Fatal("zero opening must not create transaction, ledger or events")
	}
	if f.w.Version() != 1 {
		t.Fatal("version")
	}
}

func TestBetWinLossRefundRollback(t *testing.T) {
	f := newFixture(t, 10000)

	bet := f.send("bet-1", wagering.KindBet, 2500, "")
	expectStatus(t, bet, wagering.StatusProcessed, "")
	if bet.Transaction.ResultBalance().Minor() != 7500 || f.balance() != 7500 || f.version() != 2 {
		t.Fatal("bet")
	}

	win := f.send("win-1", wagering.KindWin, 5000, "bet-1")
	expectStatus(t, win, wagering.StatusProcessed, "")
	if f.balance() != 12500 || *win.Transaction.ReferenceID() != bet.Transaction.ID() {
		t.Fatal("win")
	}

	loss := f.send("loss-1", wagering.KindLoss, 0, "")
	expectStatus(t, loss, wagering.StatusProcessed, "")
	if f.balance() != 12500 || f.version() != 3 || len(f.store.ledger) != 3 {
		t.Fatal("loss must not move money nor bump the version")
	}
	if last := f.store.outbox[len(f.store.outbox)-1]; last.Type() != events.TypeWagerTransactionProcessed {
		t.Fatal("loss emits WagerTransactionProcessed only")
	}
	if prev := f.store.outbox[len(f.store.outbox)-2].Envelope(); prev.AggregateID == loss.Transaction.ID() {
		t.Fatal("loss must not emit WalletBalanceChanged")
	}

	bet2 := f.send("bet-2", wagering.KindBet, 1000, "")
	expectStatus(t, bet2, wagering.StatusProcessed, "")
	refund := f.send("refund-1", wagering.KindRefund, 1000, "bet-2")
	expectStatus(t, refund, wagering.StatusProcessed, "")
	if f.balance() != 12500 {
		t.Fatal("refund")
	}

	rbWin := f.send("rb-win", wagering.KindRollback, 5000, "win-1")
	expectStatus(t, rbWin, wagering.StatusProcessed, "")
	if f.balance() != 7500 {
		t.Fatal("rollback of win must debit")
	}
	rbBet := f.send("rb-bet", wagering.KindRollback, 2500, "bet-1")
	expectStatus(t, rbBet, wagering.StatusProcessed, "")
	if f.balance() != 10000 {
		t.Fatal("rollback of bet must credit")
	}

	rec, err := f.svc.Reconcile(f.ctx, f.w.ID())
	if err != nil || !rec.Consistent || rec.CheckedEntries != 7 || rec.Difference.Amount() != "0.00" {
		t.Fatalf("reconciliation: %+v %v", rec, err)
	}
}

func TestBetInsufficientFunds(t *testing.T) {
	f := newFixture(t, 10000)
	res := f.send("bet-big", wagering.KindBet, 10001, "")
	expectStatus(t, res, wagering.StatusRejected, wagering.FailureInsufficientFunds)
	if f.balance() != 10000 || f.version() != 1 || len(f.store.ledger) != 1 {
		t.Fatal("rejection must not move money")
	}
	if last := f.store.outbox[len(f.store.outbox)-1]; last.Type() != events.TypeWagerTransactionRejected {
		t.Fatal("rejected event")
	}
	if res.Transaction.ResultBalance().Minor() != 10000 {
		t.Fatal("observed balance on rejection")
	}
}

func TestIdempotentReplayReturnsOriginalBalance(t *testing.T) {
	f := newFixture(t, 10000)
	first := f.send("bet-1", wagering.KindBet, 2500, "")
	f.send("bet-2", wagering.KindBet, 1000, "")
	replay := f.send("bet-1", wagering.KindBet, 2500, "")
	if !replay.IdempotentReplay || replay.Transaction.ID() != first.Transaction.ID() {
		t.Fatal("replay")
	}
	if replay.Transaction.ResultBalance().Minor() != 7500 || f.balance() != 6500 {
		t.Fatal("replay must return the balance observed originally")
	}
	if len(f.store.ledger) != 3 {
		t.Fatal("replay must not create movements")
	}
}

func TestIdempotencyConflicts(t *testing.T) {
	f := newFixture(t, 10000)
	f.send("bet-1", wagering.KindBet, 2500, "")

	// Same key, different payload.
	_, err := f.svc.Process(f.ctx, ProcessCommand{Request: f.req("bet-1", wagering.KindBet, 2600, ""), IdempotencyKey: "provider-a:bet-1", Source: SourceHTTP})
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("expected ErrIdempotencyConflict, got %v", err)
	}
	// Same external id, another key.
	_, err = f.svc.Process(f.ctx, ProcessCommand{Request: f.req("bet-1", wagering.KindBet, 2500, ""), IdempotencyKey: "other-key", Source: SourceHTTP})
	if !errors.Is(err, ErrExternalIDConflict) {
		t.Fatalf("expected ErrExternalIDConflict, got %v", err)
	}
	if f.balance() != 7500 || len(f.store.ledger) != 2 {
		t.Fatal("conflicts must not move money")
	}
	// Same operation through another source with the same key replays.
	res, err := f.svc.Process(f.ctx, ProcessCommand{Request: f.req("bet-1", wagering.KindBet, 2500, ""), IdempotencyKey: "provider-a:bet-1", Source: SourceSQS})
	if err != nil || !res.IdempotentReplay {
		t.Fatal("cross-source replay")
	}
}

func TestPayloadHashCanonical(t *testing.T) {
	f := newFixture(t, 1)
	a := f.req("x", wagering.KindBet, 2550, "")
	b := a
	b.Money, _ = money.Parse("25.5", "BRL")
	if PayloadHash(a) != PayloadHash(b) {
		t.Fatal("normalized money must hash equally")
	}
	c := a
	c.RoundID = "round-2"
	if PayloadHash(a) == PayloadHash(c) {
		t.Fatal("different payloads must differ")
	}
}

func TestWalletMismatchRejections(t *testing.T) {
	f := newFixture(t, 10000)
	other := uuid.New()
	r, _ := wagering.NewExternalRequest("provider-a", "bet-x", other, f.w.ID(), "r", "g", wagering.KindBet, brl(100), "")
	res, err := f.svc.Process(f.ctx, ProcessCommand{Request: r, IdempotencyKey: "k1", Source: SourceHTTP})
	if err != nil {
		t.Fatal(err)
	}
	expectStatus(t, res, wagering.StatusRejected, wagering.FailureWalletPlayerMismatch)

	r, _ = wagering.NewExternalRequest("provider-a", "bet-y", f.w.PlayerID(), f.w.ID(), "r", "g", wagering.KindBet, money.MustFromMinor(100, "USD"), "")
	res, err = f.svc.Process(f.ctx, ProcessCommand{Request: r, IdempotencyKey: "k2", Source: SourceHTTP})
	if err != nil {
		t.Fatal(err)
	}
	expectStatus(t, res, wagering.StatusRejected, wagering.FailureCurrencyMismatch)

	r, _ = wagering.NewExternalRequest("provider-a", "bet-z", f.w.PlayerID(), uuid.New(), "r", "g", wagering.KindBet, brl(100), "")
	if _, err := f.svc.Process(f.ctx, ProcessCommand{Request: r, IdempotencyKey: "k3", Source: SourceHTTP}); !errors.Is(err, ErrWalletNotFound) {
		t.Fatalf("expected ErrWalletNotFound, got %v", err)
	}
}

func TestReferenceRules(t *testing.T) {
	f := newFixture(t, 10000)
	f.send("bet-1", wagering.KindBet, 2500, "")

	expectStatus(t, f.send("refund-wrong-amount", wagering.KindRefund, 2400, "bet-1"), wagering.StatusRejected, wagering.FailureReferenceAmountMismatch)

	r, _ := wagering.NewExternalRequest("provider-a", "refund-wrong-round", f.w.PlayerID(), f.w.ID(), "round-2", "game-1", wagering.KindRefund, brl(2500), "bet-1")
	res, _ := f.svc.Process(f.ctx, ProcessCommand{Request: r, IdempotencyKey: "k-round", Source: SourceHTTP})
	expectStatus(t, res, wagering.StatusRejected, wagering.FailureReferenceMismatch)

	f.send("win-1", wagering.KindWin, 100, "")
	expectStatus(t, f.send("refund-of-win", wagering.KindRefund, 100, "win-1"), wagering.StatusRejected, wagering.FailureReferenceKindMismatch)

	f.send("bet-rejected", wagering.KindBet, 999999, "")
	expectStatus(t, f.send("refund-of-rejected", wagering.KindRefund, 999999, "bet-rejected"), wagering.StatusRejected, wagering.FailureReferenceNotProcessed)

	expectStatus(t, f.send("refund-1", wagering.KindRefund, 2500, "bet-1"), wagering.StatusProcessed, "")
	expectStatus(t, f.send("refund-again", wagering.KindRefund, 2500, "bet-1"), wagering.StatusRejected, wagering.FailureReferenceAlreadyReversed)
	expectStatus(t, f.send("rollback-after-refund", wagering.KindRollback, 2500, "bet-1"), wagering.StatusRejected, wagering.FailureReferenceAlreadyReversed)
	if f.balance() != 10100 {
		t.Fatalf("balance %d", f.balance())
	}
}

func TestReversalInsufficientFundsHasDistinctCode(t *testing.T) {
	f := newFixture(t, 0)
	f.send("win-1", wagering.KindWin, 5000, "")
	f.send("bet-1", wagering.KindBet, 5000, "")
	res := f.send("rb-win", wagering.KindRollback, 5000, "win-1")
	expectStatus(t, res, wagering.StatusRejected, wagering.FailureReversalInsufficientFunds)
	if f.balance() != 0 {
		t.Fatal("balance")
	}
}

func TestReversalBeforeReferenceIsParkedAndResolved(t *testing.T) {
	f := newFixture(t, 10000)
	res := f.send("refund-early", wagering.KindRefund, 2500, "bet-late")
	expectStatus(t, res, wagering.StatusPendingReference, "")
	if res.Transaction.ReferenceAttempts() != 1 || !res.Transaction.NextReferenceRetryAt().Equal(f.clock.Add(time.Second)) {
		t.Fatalf("attempt/backoff: %d %v", res.Transaction.ReferenceAttempts(), res.Transaction.NextReferenceRetryAt())
	}
	if last := f.store.outbox[len(f.store.outbox)-1]; last.Type() != events.TypeWagerTransactionPendingReference {
		t.Fatal("pending event")
	}
	// Replay while pending returns the pending state without side effects.
	replay := f.send("refund-early", wagering.KindRefund, 2500, "bet-late")
	if !replay.IdempotentReplay || replay.Transaction.Status() != wagering.StatusPendingReference {
		t.Fatal("pending replay")
	}

	// Worker retries before the reference arrives: still pending, attempts grow with backoff.
	f.clock = f.clock.Add(time.Second)
	if n, err := f.svc.ResolvePendingReferences(f.ctx); err != nil || n != 1 {
		t.Fatalf("worker: %d %v", n, err)
	}
	tx, _ := f.svc.GetTransaction(f.ctx, res.Transaction.ID())
	if tx.Status() != wagering.StatusPendingReference || tx.ReferenceAttempts() != 2 || !tx.NextReferenceRetryAt().Equal(f.clock.Add(2*time.Second)) {
		t.Fatalf("after retry: %s %d %v", tx.Status(), tx.ReferenceAttempts(), tx.NextReferenceRetryAt())
	}
	if n, _ := f.svc.ResolvePendingReferences(f.ctx); n != 0 {
		t.Fatal("not due yet")
	}

	// The bet arrives, and the next retry completes the refund.
	f.send("bet-late", wagering.KindBet, 2500, "")
	f.clock = f.clock.Add(2 * time.Second)
	if n, err := f.svc.ResolvePendingReferences(f.ctx); err != nil || n != 1 {
		t.Fatalf("worker: %d %v", n, err)
	}
	tx, _ = f.svc.GetTransaction(f.ctx, res.Transaction.ID())
	if tx.Status() != wagering.StatusProcessed || tx.ResultBalance().Minor() != 10000 || f.balance() != 10000 {
		t.Fatalf("resolved: %s %v", tx.Status(), tx.ResultBalance())
	}
	pendingEvents := 0
	for _, e := range f.store.outbox {
		if e.Type() == events.TypeWagerTransactionPendingReference {
			pendingEvents++
		}
	}
	if pendingEvents != 1 {
		t.Fatal("pending event must be emitted once")
	}
}

func TestReferenceExpiresAsRejected(t *testing.T) {
	f := newFixture(t, 10000)
	res := f.send("rb-never", wagering.KindRollback, 100, "ghost")
	expectStatus(t, res, wagering.StatusPendingReference, "")
	for i := 0; i < 5; i++ {
		f.clock = f.clock.Add(time.Minute)
		if _, err := f.svc.ResolvePendingReferences(f.ctx); err != nil {
			t.Fatal(err)
		}
	}
	tx, _ := f.svc.GetTransaction(f.ctx, res.Transaction.ID())
	expectStatus(t, ProcessResult{Transaction: tx}, wagering.StatusRejected, wagering.FailureReferenceNotFound)
	if tx.ReferenceAttempts() != 3 {
		t.Fatalf("attempts %d", tx.ReferenceAttempts())
	}
	if last := f.store.outbox[len(f.store.outbox)-1]; last.Type() != events.TypeWagerTransactionRejected {
		t.Fatal("rejected event")
	}
	// A reference that only exists as PENDING_REFERENCE keeps the dependent waiting.
	f.send("rb-of-pending", wagering.KindRollback, 100, "rb-never-2")
	expectStatus(t, f.send("rb-never-2", wagering.KindRollback, 100, "ghost-2"), wagering.StatusPendingReference, "")
	f.clock = f.clock.Add(time.Second)
	_, _ = f.svc.ResolvePendingReferences(f.ctx)
	dep, _ := f.svc.GetTransactionByExternalID(f.ctx, "provider-a", "rb-of-pending")
	if dep.Status() != wagering.StatusPendingReference {
		t.Fatal("dependent of a pending reference must keep waiting")
	}
}

func TestLedgerPagination(t *testing.T) {
	f := newFixture(t, 10000)
	for i := 0; i < 5; i++ {
		f.send("bet-"+string(rune('a'+i)), wagering.KindBet, 100, "")
	}
	page, err := f.svc.ListLedger(f.ctx, f.w.ID(), "", 4)
	if err != nil || len(page.Entries) != 4 || page.NextCursor == "" {
		t.Fatalf("page1: %d %q %v", len(page.Entries), page.NextCursor, err)
	}
	page2, err := f.svc.ListLedger(f.ctx, f.w.ID(), page.NextCursor, 4)
	if err != nil || len(page2.Entries) != 2 || page2.NextCursor != "" {
		t.Fatalf("page2: %d %q %v", len(page2.Entries), page2.NextCursor, err)
	}
	if page2.Entries[0].TransactionID() == page.Entries[3].TransactionID() {
		t.Fatal("overlap")
	}
	if _, err := f.svc.ListLedger(f.ctx, f.w.ID(), "!!!", 4); !errors.Is(err, ErrInvalidCursor) {
		t.Fatal("invalid cursor")
	}
}

func TestBackoff(t *testing.T) {
	s := NewService(newMemStore(), nil, nil, Config{PendingReferenceBaseBackoff: time.Second, PendingReferenceMaxBackoff: 10 * time.Second, PendingReferenceMaxAttempts: 5}, nil)
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 10 * time.Second, 10 * time.Second}
	for i, w := range want {
		if got := s.backoff(i + 1); got != w {
			t.Errorf("attempt %d: %v != %v", i+1, got, w)
		}
	}
}
