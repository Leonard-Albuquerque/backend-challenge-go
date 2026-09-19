package app

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jamesmachome/backend-challenge-go/internal/domain/events"
	"github.com/jamesmachome/backend-challenge-go/internal/domain/wagering"
	"github.com/jamesmachome/backend-challenge-go/internal/domain/wallet"
)

// Source identifies the entry point of a command, for metrics and logs.
type Source string

const (
	SourceHTTP Source = "http"
	SourceSQS  Source = "sqs"
)

// ProcessCommand is a provider operation plus its transport metadata.
type ProcessCommand struct {
	Request        wagering.ExternalRequest
	IdempotencyKey string
	CorrelationID  string
	Source         Source
}

// ProcessResult is the persisted outcome returned to the caller.
type ProcessResult struct {
	Transaction      *wagering.Transaction
	IdempotentReplay bool
}

// Process runs the operation in its own SQL transaction. A lost race on the
// unique constraints is retried once so the caller receives the replay.
func (s *Service) Process(ctx context.Context, cmd ProcessCommand) (ProcessResult, error) {
	var res ProcessResult
	run := func() error {
		return s.uow.Do(ctx, func(ctx context.Context, st Store) error {
			r, err := s.ProcessIn(ctx, st, cmd)
			res = r
			return err
		})
	}
	err := run()
	if errors.Is(err, ErrDuplicateTransaction) {
		s.metrics.ConcurrencyConflict()
		err = run()
	}
	return res, err
}

// ProcessIn runs the operation inside an existing store transaction. It is the
// single implementation shared by HTTP and SQS; the SQS consumer calls it after
// recording the inbox row in the same transaction.
func (s *Service) ProcessIn(ctx context.Context, st Store, cmd ProcessCommand) (ProcessResult, error) {
	started := s.now()
	req := cmd.Request
	hash := PayloadHash(req)
	if cmd.IdempotencyKey == "" {
		return ProcessResult{}, fmt.Errorf("%w: Idempotency-Key is required", wagering.ErrValidation)
	}

	// 1. Fast path: idempotent replay or conflict, before taking the wallet lock.
	if res, done, err := s.lookupExisting(ctx, st, cmd, hash); done || err != nil {
		return res, err
	}

	// 2. Coordinate per wallet: every writer of this wallet serializes here.
	w, err := st.Wallets().GetForUpdate(ctx, req.WalletID)
	if err != nil {
		return ProcessResult{}, err
	}

	// 3. Re-check under the lock: a concurrent duplicate may have committed
	// while we were waiting for the wallet row.
	if res, done, err := s.lookupExisting(ctx, st, cmd, hash); done || err != nil {
		return res, err
	}

	now := s.now()
	tx, err := wagering.NewExternal(newID(), req, cmd.IdempotencyKey, hash, cmd.CorrelationID, now)
	if err != nil {
		return ProcessResult{}, err
	}
	entry, err := s.apply(ctx, st, tx, w, now)
	if err != nil {
		return ProcessResult{}, err
	}
	if err := st.Transactions().Insert(ctx, tx); err != nil {
		return ProcessResult{}, err
	}
	if err := s.persistOutcome(ctx, st, tx, w, entry); err != nil {
		return ProcessResult{}, err
	}
	s.metrics.TransactionOutcome(string(tx.Kind()), string(tx.Status()), string(cmd.Source))
	s.metrics.ProcessingDuration(string(cmd.Source), s.now().Sub(started))
	s.log.InfoContext(ctx, "transaction processed",
		"transactionId", tx.ID(), "walletId", tx.WalletID(), "providerId", tx.ProviderID(),
		"correlationId", tx.CorrelationID(), "kind", tx.Kind(), "status", tx.Status(), "failureCode", tx.FailureCode(), "source", cmd.Source)
	return ProcessResult{Transaction: tx}, nil
}

// lookupExisting returns (result, true, nil) for a replay, an error for a
// conflict, and (_, false, nil) when the operation is new.
func (s *Service) lookupExisting(ctx context.Context, st Store, cmd ProcessCommand, hash string) (ProcessResult, bool, error) {
	req := cmd.Request
	existing, err := st.Transactions().FindByIdempotencyKey(ctx, req.ProviderID, cmd.IdempotencyKey)
	switch {
	case err == nil:
		if existing.PayloadHash() != hash {
			return ProcessResult{}, false, fmt.Errorf("%w: key %q", ErrIdempotencyConflict, cmd.IdempotencyKey)
		}
		s.metrics.Duplicate(string(cmd.Source))
		s.log.InfoContext(ctx, "idempotent replay", "transactionId", existing.ID(), "providerId", req.ProviderID, "correlationId", cmd.CorrelationID, "source", cmd.Source)
		return ProcessResult{Transaction: existing, IdempotentReplay: true}, true, nil
	case !errors.Is(err, ErrTransactionNotFound):
		return ProcessResult{}, false, err
	}
	byExt, err := st.Transactions().FindByExternalID(ctx, req.ProviderID, req.ExternalTransactionID)
	switch {
	case err == nil && byExt.IdempotencyKey() == cmd.IdempotencyKey:
		// Committed by another writer between the two lookups (READ COMMITTED
		// sees each statement's snapshot): it is the same operation.
		if byExt.PayloadHash() != hash {
			return ProcessResult{}, false, fmt.Errorf("%w: key %q", ErrIdempotencyConflict, cmd.IdempotencyKey)
		}
		s.metrics.Duplicate(string(cmd.Source))
		return ProcessResult{Transaction: byExt, IdempotentReplay: true}, true, nil
	case err == nil:
		return ProcessResult{}, false, fmt.Errorf("%w: %q already recorded under key %q", ErrExternalIDConflict, req.ExternalTransactionID, byExt.IdempotencyKey())
	case !errors.Is(err, ErrTransactionNotFound):
		return ProcessResult{}, false, err
	}
	return ProcessResult{}, false, nil
}

// apply runs the business rules for tx against the locked wallet and moves tx
// to its resulting state. It returns the ledger entry when the balance changed.
// Business rejections are recorded on tx, not returned as errors; only
// infrastructure errors are returned.
func (s *Service) apply(ctx context.Context, st Store, tx *wagering.Transaction, w *wallet.Wallet, now time.Time) (*wallet.LedgerEntry, error) {
	if w.PlayerID() != tx.PlayerID() {
		return nil, tx.MarkRejected(wagering.FailureWalletPlayerMismatch, w.Balance(), now)
	}
	if w.Currency() != tx.Money().Currency() {
		return nil, tx.MarkRejected(wagering.FailureCurrencyMismatch, w.Balance(), now)
	}

	var (
		entry wallet.LedgerEntry
		err   error
	)
	switch tx.Kind() {
	case wagering.KindBet:
		entry, err = w.Debit(newID(), tx.ID(), tx.Money(), now)
		if errors.Is(err, wallet.ErrInsufficientFunds) {
			return nil, tx.MarkRejected(wagering.FailureInsufficientFunds, w.Balance(), now)
		}
	case wagering.KindLoss:
		return nil, tx.MarkProcessed(w.Balance(), now)
	case wagering.KindWin:
		if tx.ReferenceExternalID() != "" {
			if done, err := s.resolveReference(ctx, st, tx, w, now); done || err != nil {
				return nil, err
			}
		}
		entry, err = w.Credit(newID(), tx.ID(), tx.Money(), now)
	case wagering.KindRefund:
		if done, err := s.resolveReference(ctx, st, tx, w, now); done || err != nil {
			return nil, err
		}
		entry, err = w.Credit(newID(), tx.ID(), tx.Money(), now)
	case wagering.KindRollback:
		ref, done, err := s.resolveReferenceTx(ctx, st, tx, w, now)
		if done || err != nil {
			return nil, err
		}
		if ref.Kind() == wagering.KindBet {
			entry, err = w.Credit(newID(), tx.ID(), tx.Money(), now)
		} else {
			entry, err = w.Debit(newID(), tx.ID(), tx.Money(), now)
			if errors.Is(err, wallet.ErrInsufficientFunds) {
				return nil, tx.MarkRejected(wagering.FailureReversalInsufficientFunds, w.Balance(), now)
			}
		}
	default:
		return nil, fmt.Errorf("%w: unsupported kind %s", wagering.ErrValidation, tx.Kind())
	}
	if err != nil {
		return nil, err
	}
	if err := tx.MarkProcessed(w.Balance(), now); err != nil {
		return nil, err
	}
	return &entry, nil
}

// resolveReference is resolveReferenceTx without the resolved transaction.
func (s *Service) resolveReference(ctx context.Context, st Store, tx *wagering.Transaction, w *wallet.Wallet, now time.Time) (bool, error) {
	_, done, err := s.resolveReferenceTx(ctx, st, tx, w, now)
	return done, err
}

// resolveReferenceTx locates and validates the referenced transaction. It
// returns done=true when tx reached a state (PENDING_REFERENCE or REJECTED)
// that stops further processing.
func (s *Service) resolveReferenceTx(ctx context.Context, st Store, tx *wagering.Transaction, w *wallet.Wallet, now time.Time) (*wagering.Transaction, bool, error) {
	ref, err := st.Transactions().FindByExternalID(ctx, tx.ProviderID(), tx.ReferenceExternalID())
	if errors.Is(err, ErrTransactionNotFound) {
		return nil, true, s.park(tx, w, now)
	}
	if err != nil {
		return nil, false, err
	}
	reject := func(code wagering.FailureCode) (*wagering.Transaction, bool, error) {
		return nil, true, tx.MarkRejected(code, w.Balance(), now)
	}
	switch ref.Status() {
	case wagering.StatusProcessed:
	case wagering.StatusPending, wagering.StatusPendingReference:
		// The reference exists but has not completed yet: keep waiting.
		return nil, true, s.park(tx, w, now)
	default:
		return reject(wagering.FailureReferenceNotProcessed)
	}
	if !allowedReference(tx.Kind(), ref.Kind()) {
		return reject(wagering.FailureReferenceKindMismatch)
	}
	if ref.PlayerID() != tx.PlayerID() || ref.WalletID() != tx.WalletID() || ref.RoundID() != tx.RoundID() || ref.Money().Currency() != tx.Money().Currency() {
		return reject(wagering.FailureReferenceMismatch)
	}
	if tx.Kind().IsReversal() {
		if !ref.Money().Equal(tx.Money()) {
			return reject(wagering.FailureReferenceAmountMismatch)
		}
		reversed, err := st.Transactions().HasProcessedReversal(ctx, ref.ID())
		if err != nil {
			return nil, false, err
		}
		if reversed {
			return reject(wagering.FailureReferenceAlreadyReversed)
		}
	}
	if err := tx.ResolveReference(ref.ID()); err != nil {
		return nil, false, err
	}
	return ref, false, nil
}

func allowedReference(kind, refKind wagering.Kind) bool {
	switch kind {
	case wagering.KindWin, wagering.KindRefund:
		return refKind == wagering.KindBet
	case wagering.KindRollback:
		return refKind == wagering.KindBet || refKind == wagering.KindWin || refKind == wagering.KindRefund
	}
	return false
}

// park moves tx to PENDING_REFERENCE or, when attempts are exhausted, to
// REJECTED with REFERENCE_NOT_FOUND.
func (s *Service) park(tx *wagering.Transaction, w *wallet.Wallet, now time.Time) error {
	if tx.ReferenceAttempts() >= s.cfg.PendingReferenceMaxAttempts {
		return tx.MarkRejected(wagering.FailureReferenceNotFound, w.Balance(), now)
	}
	next := now.Add(s.backoff(tx.ReferenceAttempts() + 1))
	return tx.AwaitReference(next, now)
}

// persistOutcome writes ledger, wallet and outbox rows according to the final
// state of tx. The transaction row itself must already be inserted/updated.
func (s *Service) persistOutcome(ctx context.Context, st Store, tx *wagering.Transaction, w *wallet.Wallet, entry *wallet.LedgerEntry) error {
	switch tx.Status() {
	case wagering.StatusProcessed:
		if entry != nil {
			if err := st.Ledger().Append(ctx, *entry); err != nil {
				return err
			}
			// The aggregate already incremented its version; the previous one
			// is the expected stored value.
			if err := st.Wallets().Update(ctx, w, w.Version()-1); err != nil {
				if errors.Is(err, ErrConcurrentModification) {
					s.metrics.ConcurrencyConflict()
				}
				return err
			}
			ev, err := events.NewWalletBalanceChanged(*entry, w.Version(), tx.CorrelationID())
			if err != nil {
				return err
			}
			if err := st.Outbox().Add(ctx, ev); err != nil {
				return err
			}
		}
		ev, err := events.NewWagerTransactionProcessed(tx)
		if err != nil {
			return err
		}
		return st.Outbox().Add(ctx, ev)
	case wagering.StatusRejected:
		ev, err := events.NewWagerTransactionRejected(tx)
		if err != nil {
			return err
		}
		return st.Outbox().Add(ctx, ev)
	case wagering.StatusPendingReference:
		if tx.ReferenceAttempts() == 1 {
			ev, err := events.NewWagerTransactionPendingReference(tx)
			if err != nil {
				return err
			}
			return st.Outbox().Add(ctx, ev)
		}
		return nil
	}
	return fmt.Errorf("%w: unexpected final status %s", wagering.ErrInvalidTransition, tx.Status())
}

// ResolvePendingReferences retries due PENDING_REFERENCE transactions, one per
// SQL transaction, until none is due or the context ends. It returns the
// number of transactions examined.
func (s *Service) ResolvePendingReferences(ctx context.Context) (int, error) {
	handled := 0
	for {
		if err := ctx.Err(); err != nil {
			return handled, err
		}
		var found bool
		err := s.uow.Do(ctx, func(ctx context.Context, st Store) error {
			tx, err := st.Transactions().ClaimDuePendingReference(ctx, s.now())
			if errors.Is(err, ErrTransactionNotFound) {
				return nil
			}
			if err != nil {
				return err
			}
			found = true
			s.metrics.PendingReferenceRetry()
			w, err := st.Wallets().GetForUpdate(ctx, tx.WalletID())
			if err != nil {
				return err
			}
			now := s.now()
			entry, err := s.apply(ctx, st, tx, w, now)
			if err != nil {
				return err
			}
			if err := st.Transactions().Update(ctx, tx); err != nil {
				return err
			}
			if err := s.persistOutcome(ctx, st, tx, w, entry); err != nil {
				return err
			}
			if tx.Status() != wagering.StatusPendingReference {
				s.metrics.TransactionOutcome(string(tx.Kind()), string(tx.Status()), "pending-worker")
			}
			s.log.InfoContext(ctx, "pending reference retried", "transactionId", tx.ID(), "walletId", tx.WalletID(),
				"providerId", tx.ProviderID(), "correlationId", tx.CorrelationID(), "status", tx.Status(), "attempts", tx.ReferenceAttempts(), "failureCode", tx.FailureCode())
			return nil
		})
		if err != nil {
			return handled, err
		}
		if !found {
			return handled, nil
		}
		handled++
	}
}
