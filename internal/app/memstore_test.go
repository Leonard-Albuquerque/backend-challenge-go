package app

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/jamesmachome/backend-challenge-go/internal/domain/events"
	"github.com/jamesmachome/backend-challenge-go/internal/domain/wagering"
	"github.com/jamesmachome/backend-challenge-go/internal/domain/wallet"
)

// memStore is a serialized in-memory Store used only by unit tests. It
// mimics the constraints of the schema closely enough to exercise use-case
// rules; the real constraints are verified by the integration tests.
type memStore struct {
	mu      sync.Mutex
	wallets map[uuid.UUID]wallet.Wallet
	txs     map[uuid.UUID]wagering.Snapshot
	ledger  []LedgerRecord
	outbox  []events.Event
	inbox   map[string]string
}

func newMemStore() *memStore {
	return &memStore{wallets: map[uuid.UUID]wallet.Wallet{}, txs: map[uuid.UUID]wagering.Snapshot{}, inbox: map[string]string{}}
}

func (m *memStore) Do(ctx context.Context, fn func(context.Context, Store) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Snapshot for rollback on error.
	backup := m.clone()
	if err := fn(ctx, m); err != nil {
		m.wallets, m.txs, m.ledger, m.outbox, m.inbox = backup.wallets, backup.txs, backup.ledger, backup.outbox, backup.inbox
		return err
	}
	return nil
}

func (m *memStore) DoSnapshot(ctx context.Context, fn func(context.Context, Store) error) error {
	return m.Do(ctx, fn)
}

func (m *memStore) clone() *memStore {
	c := newMemStore()
	for k, v := range m.wallets {
		c.wallets[k] = v
	}
	for k, v := range m.txs {
		c.txs[k] = v
	}
	c.ledger = append([]LedgerRecord(nil), m.ledger...)
	c.outbox = append([]events.Event(nil), m.outbox...)
	for k, v := range m.inbox {
		c.inbox[k] = v
	}
	return c
}

type memWallets struct{ *memStore }
type memTxs struct{ *memStore }

func (m *memStore) Wallets() WalletRepository           { return memWallets{m} }
func (m *memStore) Transactions() TransactionRepository { return memTxs{m} }
func (m *memStore) Ledger() LedgerRepository            { return m }
func (m *memStore) Outbox() OutboxRepository            { return m }
func (m *memStore) Inbox() InboxRepository              { return m }

func (m memWallets) Create(_ context.Context, w *wallet.Wallet) error {
	for _, e := range m.wallets {
		if e.PlayerID() == w.PlayerID() && e.Currency() == w.Currency() {
			return ErrWalletExists
		}
	}
	m.wallets[w.ID()] = *w
	return nil
}

func (m memWallets) Get(_ context.Context, id uuid.UUID) (*wallet.Wallet, error) {
	w, ok := m.wallets[id]
	if !ok {
		return nil, ErrWalletNotFound
	}
	return wallet.Rehydrate(w.ID(), w.PlayerID(), w.Balance(), w.Version(), w.CreatedAt(), w.UpdatedAt())
}

func (m memWallets) GetForUpdate(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error) {
	return m.Get(ctx, id)
}

func (m memWallets) Update(_ context.Context, w *wallet.Wallet, expected int64) error {
	cur, ok := m.wallets[w.ID()]
	if !ok || cur.Version() != expected {
		return ErrConcurrentModification
	}
	m.wallets[w.ID()] = *w
	return nil
}

func (m memTxs) Insert(_ context.Context, tx *wagering.Transaction) error {
	s := tx.Snapshot()
	for _, e := range m.txs {
		if s.Origin == wagering.OriginExternal && e.ProviderID == s.ProviderID && (e.ExternalTransactionID == s.ExternalTransactionID || e.IdempotencyKey == s.IdempotencyKey) {
			return ErrDuplicateTransaction
		}
		if s.Kind == wagering.KindOpening && e.Kind == wagering.KindOpening && e.WalletID == s.WalletID {
			return ErrDuplicateTransaction
		}
	}
	m.txs[s.ID] = s
	return nil
}

func (m memTxs) Update(_ context.Context, tx *wagering.Transaction) error {
	m.txs[tx.ID()] = tx.Snapshot()
	return nil
}

func (m memTxs) Get(_ context.Context, id uuid.UUID) (*wagering.Transaction, error) {
	s, ok := m.txs[id]
	if !ok {
		return nil, ErrTransactionNotFound
	}
	return wagering.Rehydrate(s)
}

func (m memTxs) FindByIdempotencyKey(_ context.Context, providerID, key string) (*wagering.Transaction, error) {
	for _, s := range m.txs {
		if s.Origin == wagering.OriginExternal && s.ProviderID == providerID && s.IdempotencyKey == key {
			return wagering.Rehydrate(s)
		}
	}
	return nil, ErrTransactionNotFound
}

func (m memTxs) FindByExternalID(_ context.Context, providerID, ext string) (*wagering.Transaction, error) {
	for _, s := range m.txs {
		if s.Origin == wagering.OriginExternal && s.ProviderID == providerID && s.ExternalTransactionID == ext {
			return wagering.Rehydrate(s)
		}
	}
	return nil, ErrTransactionNotFound
}

func (m memTxs) HasProcessedReversal(_ context.Context, ref uuid.UUID) (bool, error) {
	for _, s := range m.txs {
		if s.ReferenceID != nil && *s.ReferenceID == ref && s.Status == wagering.StatusProcessed && s.Kind.IsReversal() {
			return true, nil
		}
	}
	return false, nil
}

func (m memTxs) ClaimDuePendingReference(_ context.Context, now time.Time) (*wagering.Transaction, error) {
	var due []wagering.Snapshot
	for _, s := range m.txs {
		if s.Status == wagering.StatusPendingReference && s.NextReferenceRetryAt != nil && !s.NextReferenceRetryAt.After(now) {
			due = append(due, s)
		}
	}
	if len(due) == 0 {
		return nil, ErrTransactionNotFound
	}
	sort.Slice(due, func(i, j int) bool { return due[i].NextReferenceRetryAt.Before(*due[j].NextReferenceRetryAt) })
	return wagering.Rehydrate(due[0])
}

func (m *memStore) Append(_ context.Context, e wallet.LedgerEntry) error {
	for _, r := range m.ledger {
		if r.Entry.WalletID() == e.WalletID() && r.Entry.TransactionID() == e.TransactionID() {
			return ErrDuplicateTransaction
		}
	}
	m.ledger = append(m.ledger, LedgerRecord{Seq: int64(len(m.ledger) + 1), Entry: e})
	return nil
}

func (m *memStore) List(_ context.Context, walletID uuid.UUID, after int64, limit int) ([]LedgerRecord, error) {
	var out []LedgerRecord
	for _, r := range m.ledger {
		if r.Entry.WalletID() == walletID && r.Seq > after && len(out) < limit {
			out = append(out, r)
		}
	}
	return out, nil
}

func (m *memStore) Totals(_ context.Context, walletID uuid.UUID) (LedgerTotals, error) {
	var t LedgerTotals
	for _, r := range m.ledger {
		if r.Entry.WalletID() != walletID {
			continue
		}
		t.Count++
		if r.Entry.Direction() == wallet.Credit {
			t.Credits += r.Entry.Amount().Minor()
		} else {
			t.Debits += r.Entry.Amount().Minor()
		}
	}
	return t, nil
}

func (m *memStore) Add(_ context.Context, ev events.Event) error {
	m.outbox = append(m.outbox, ev)
	return nil
}
func (m *memStore) Claim(context.Context, string, time.Time, time.Duration, int) ([]OutboxRecord, error) {
	return nil, nil
}
func (m *memStore) MarkPublished(context.Context, uuid.UUID, time.Time) error      { return nil }
func (m *memStore) Reschedule(context.Context, uuid.UUID, time.Time, string) error { return nil }
func (m *memStore) Lag(context.Context, time.Time) (time.Duration, int64, error)   { return 0, 0, nil }

func (m *memStore) Record(_ context.Context, consumer, id, hash string, _ time.Time) (InboxResult, error) {
	k := consumer + "/" + id
	if h, ok := m.inbox[k]; ok {
		return InboxResult{ExistingHash: h}, nil
	}
	m.inbox[k] = hash
	return InboxResult{Inserted: true}, nil
}

func (m *memStore) eventTypes() []string {
	var out []string
	for _, e := range m.outbox {
		out = append(out, e.Type())
	}
	return out
}
