package app

import (
	"context"
	"encoding/base64"
	"errors"
	"strconv"

	"github.com/google/uuid"

	"github.com/jamesmachome/backend-challenge-go/internal/domain/events"
	"github.com/jamesmachome/backend-challenge-go/internal/domain/money"
	"github.com/jamesmachome/backend-challenge-go/internal/domain/wagering"
	"github.com/jamesmachome/backend-challenge-go/internal/domain/wallet"
)

// OpenWalletCommand opens a wallet for a player in one currency.
type OpenWalletCommand struct {
	PlayerID       uuid.UUID
	InitialBalance money.Money
	CorrelationID  string
}

// OpenWallet creates the wallet and, for a positive initial balance, the
// OPENING transaction, its credit ledger entry and both integration events in
// the same commit.
func (s *Service) OpenWallet(ctx context.Context, cmd OpenWalletCommand) (*wallet.Wallet, error) {
	now := s.now()
	w, err := wallet.Open(newID(), cmd.PlayerID, cmd.InitialBalance, now)
	if err != nil {
		return nil, err
	}
	err = s.uow.Do(ctx, func(ctx context.Context, st Store) error {
		if err := st.Wallets().Create(ctx, w); err != nil {
			return err
		}
		if w.Balance().IsZero() {
			return nil
		}
		tx, err := wagering.NewOpening(newID(), w.ID(), w.PlayerID(), w.Balance(), cmd.CorrelationID, now)
		if err != nil {
			return err
		}
		if err := tx.MarkProcessed(w.Balance(), now); err != nil {
			return err
		}
		zero, err := money.Zero(w.Currency())
		if err != nil {
			return err
		}
		entry, err := wallet.NewLedgerEntry(newID(), w.ID(), tx.ID(), wallet.Credit, w.Balance(), zero, w.Balance(), now)
		if err != nil {
			return err
		}
		if err := st.Transactions().Insert(ctx, tx); err != nil {
			return err
		}
		if err := st.Ledger().Append(ctx, entry); err != nil {
			return err
		}
		processed, err := events.NewWagerTransactionProcessed(tx)
		if err != nil {
			return err
		}
		changed, err := events.NewWalletBalanceChanged(entry, w.Version(), cmd.CorrelationID)
		if err != nil {
			return err
		}
		if err := st.Outbox().Add(ctx, processed); err != nil {
			return err
		}
		return st.Outbox().Add(ctx, changed)
	})
	if err != nil {
		return nil, err
	}
	s.metrics.TransactionOutcome(string(wagering.KindOpening), string(wagering.StatusProcessed), "http")
	s.log.InfoContext(ctx, "wallet opened", "walletId", w.ID(), "playerId", w.PlayerID(), "currency", w.Currency(), "correlationId", cmd.CorrelationID)
	return w, nil
}

// GetWallet reads a wallet.
func (s *Service) GetWallet(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error) {
	var w *wallet.Wallet
	err := s.uow.Do(ctx, func(ctx context.Context, st Store) error {
		var err error
		w, err = st.Wallets().Get(ctx, id)
		return err
	})
	return w, err
}

// GetTransaction reads a transaction by internal id.
func (s *Service) GetTransaction(ctx context.Context, id uuid.UUID) (*wagering.Transaction, error) {
	var tx *wagering.Transaction
	err := s.uow.Do(ctx, func(ctx context.Context, st Store) error {
		var err error
		tx, err = st.Transactions().Get(ctx, id)
		return err
	})
	return tx, err
}

// GetTransactionByExternalID reads a provider's transaction.
func (s *Service) GetTransactionByExternalID(ctx context.Context, providerID, externalID string) (*wagering.Transaction, error) {
	var tx *wagering.Transaction
	err := s.uow.Do(ctx, func(ctx context.Context, st Store) error {
		var err error
		tx, err = st.Transactions().FindByExternalID(ctx, providerID, externalID)
		return err
	})
	return tx, err
}

// LedgerPage is one page of ledger entries.
type LedgerPage struct {
	Entries    []wallet.LedgerEntry
	NextCursor string
}

// ListLedger pages a wallet's ledger with an opaque cursor in stable order.
func (s *Service) ListLedger(ctx context.Context, walletID uuid.UUID, cursor string, limit int) (LedgerPage, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	after, err := decodeCursor(cursor)
	if err != nil {
		return LedgerPage{}, err
	}
	var page LedgerPage
	err = s.uow.Do(ctx, func(ctx context.Context, st Store) error {
		if _, err := st.Wallets().Get(ctx, walletID); err != nil {
			return err
		}
		records, err := st.Ledger().List(ctx, walletID, after, limit+1)
		if err != nil {
			return err
		}
		if len(records) > limit {
			page.NextCursor = encodeCursor(records[limit-1].Seq)
			records = records[:limit]
		}
		for _, r := range records {
			page.Entries = append(page.Entries, r.Entry)
		}
		return nil
	})
	return page, err
}

func encodeCursor(seq int64) string {
	return base64.RawURLEncoding.EncodeToString([]byte("seq:" + strconv.FormatInt(seq, 10)))
}

func decodeCursor(c string) (int64, error) {
	if c == "" {
		return 0, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(c)
	if err != nil || len(raw) < 5 || string(raw[:4]) != "seq:" {
		return 0, ErrInvalidCursor
	}
	n, err := strconv.ParseInt(string(raw[4:]), 10, 64)
	if err != nil || n < 0 {
		return 0, ErrInvalidCursor
	}
	return n, nil
}

// Reconciliation compares the stored balance with the ledger.
type Reconciliation struct {
	WalletID          uuid.UUID
	StoredBalance     money.Money
	CalculatedBalance money.Money
	Difference        money.Money
	Consistent        bool
	CheckedEntries    int64
}

// Reconcile rebuilds the balance from the ledger inside a REPEATABLE READ
// snapshot so wallet and ledger are read consistently. It never writes.
func (s *Service) Reconcile(ctx context.Context, walletID uuid.UUID) (Reconciliation, error) {
	var out Reconciliation
	err := s.uow.DoSnapshot(ctx, func(ctx context.Context, st Store) error {
		w, err := st.Wallets().Get(ctx, walletID)
		if err != nil {
			return err
		}
		totals, err := st.Ledger().Totals(ctx, walletID)
		if err != nil {
			return err
		}
		credits, err := money.FromMinor(totals.Credits, w.Currency())
		if err != nil {
			return err
		}
		debits, err := money.FromMinor(totals.Debits, w.Currency())
		if err != nil {
			return err
		}
		calculated, err := credits.Sub(debits)
		if err != nil {
			return err
		}
		diff, err := w.Balance().Sub(calculated)
		if err != nil {
			return err
		}
		out = Reconciliation{WalletID: w.ID(), StoredBalance: w.Balance(), CalculatedBalance: calculated, Difference: diff, Consistent: diff.IsZero(), CheckedEntries: totals.Count}
		return nil
	})
	if err != nil {
		return Reconciliation{}, err
	}
	if !out.Consistent {
		s.metrics.ReconciliationDivergence()
		s.log.ErrorContext(ctx, "reconciliation divergence", "walletId", walletID, "stored", out.StoredBalance.Amount(), "calculated", out.CalculatedBalance.Amount(), "difference", out.Difference.Amount())
	}
	return out, nil
}

// IsNotFound reports whether err is a not-found error of the use cases.
func IsNotFound(err error) bool {
	return errors.Is(err, ErrWalletNotFound) || errors.Is(err, ErrTransactionNotFound)
}
