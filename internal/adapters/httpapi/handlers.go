package httpapi

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/jamesmachome/backend-challenge-go/internal/adapters/auth"
	"github.com/jamesmachome/backend-challenge-go/internal/app"
	"github.com/jamesmachome/backend-challenge-go/internal/domain/money"
	"github.com/jamesmachome/backend-challenge-go/internal/domain/wagering"
	"github.com/jamesmachome/backend-challenge-go/internal/domain/wallet"
	"github.com/jamesmachome/backend-challenge-go/internal/observability"
)

// --- DTOs ---

type openWalletRequest struct {
	PlayerID       string     `json:"playerId"`
	InitialBalance money.JSON `json:"initialBalance"`
}

type walletResponse struct {
	ID        uuid.UUID  `json:"id"`
	PlayerID  uuid.UUID  `json:"playerId"`
	Balance   money.JSON `json:"balance"`
	Version   int64      `json:"version"`
	CreatedAt time.Time  `json:"createdAt"`
	UpdatedAt time.Time  `json:"updatedAt"`
}

func toWalletResponse(w *wallet.Wallet) walletResponse {
	return walletResponse{ID: w.ID(), PlayerID: w.PlayerID(), Balance: w.Balance().ToJSON(), Version: w.Version(), CreatedAt: w.CreatedAt(), UpdatedAt: w.UpdatedAt()}
}

type transactionRequest struct {
	ProviderID                     string     `json:"providerId"`
	ExternalTransactionID          string     `json:"externalTransactionId"`
	PlayerID                       string     `json:"playerId"`
	WalletID                       string     `json:"walletId"`
	RoundID                        string     `json:"roundId"`
	GameID                         string     `json:"gameId"`
	Kind                           string     `json:"kind"`
	Money                          money.JSON `json:"money"`
	ReferenceExternalTransactionID string     `json:"referenceExternalTransactionId,omitempty"`
}

// ToRequest validates the DTO into a domain request. Shared with the SQS
// consumer so both entry points apply identical rules.
func ToRequest(in transactionRequest) (wagering.ExternalRequest, error) {
	playerID, err := uuid.Parse(in.PlayerID)
	if err != nil {
		return wagering.ExternalRequest{}, fmt.Errorf("%w: playerId must be a UUID", wagering.ErrValidation)
	}
	walletID, err := uuid.Parse(in.WalletID)
	if err != nil {
		return wagering.ExternalRequest{}, fmt.Errorf("%w: walletId must be a UUID", wagering.ErrValidation)
	}
	m, err := money.FromJSON(in.Money)
	if err != nil {
		return wagering.ExternalRequest{}, fmt.Errorf("%w: money: %v", wagering.ErrValidation, err)
	}
	return wagering.NewExternalRequest(in.ProviderID, in.ExternalTransactionID, playerID, walletID, in.RoundID, in.GameID, wagering.Kind(in.Kind), m, in.ReferenceExternalTransactionID)
}

// TransactionRequest is exported for the SQS payload decoder.
type TransactionRequest = transactionRequest

type transactionResponse struct {
	TransactionID                  uuid.UUID   `json:"transactionId"`
	Status                         string      `json:"status"`
	Balance                        *money.JSON `json:"balance,omitempty"`
	IdempotentReplay               bool        `json:"idempotentReplay"`
	FailureCode                    string      `json:"failureCode,omitempty"`
	Kind                           string      `json:"kind"`
	ProviderID                     string      `json:"providerId,omitempty"`
	ExternalTransactionID          string      `json:"externalTransactionId,omitempty"`
	WalletID                       uuid.UUID   `json:"walletId"`
	PlayerID                       uuid.UUID   `json:"playerId"`
	RoundID                        string      `json:"roundId,omitempty"`
	GameID                         string      `json:"gameId,omitempty"`
	Money                          money.JSON  `json:"money"`
	ReferenceExternalTransactionID string      `json:"referenceExternalTransactionId,omitempty"`
	ReferenceTransactionID         *uuid.UUID  `json:"referenceTransactionId,omitempty"`
	ReferenceAttempts              int         `json:"referenceAttempts,omitempty"`
	NextReferenceRetryAt           *time.Time  `json:"nextReferenceRetryAt,omitempty"`
	CreatedAt                      time.Time   `json:"createdAt"`
	UpdatedAt                      time.Time   `json:"updatedAt"`
	ProcessedAt                    *time.Time  `json:"processedAt,omitempty"`
}

func toTransactionResponse(tx *wagering.Transaction, replay bool) transactionResponse {
	out := transactionResponse{
		TransactionID: tx.ID(), Status: string(tx.Status()), IdempotentReplay: replay, FailureCode: string(tx.FailureCode()),
		Kind: string(tx.Kind()), ProviderID: tx.ProviderID(), ExternalTransactionID: tx.ExternalTransactionID(), WalletID: tx.WalletID(),
		PlayerID: tx.PlayerID(), RoundID: tx.RoundID(), GameID: tx.GameID(), Money: tx.Money().ToJSON(),
		ReferenceExternalTransactionID: tx.ReferenceExternalID(), ReferenceTransactionID: tx.ReferenceID(), ReferenceAttempts: tx.ReferenceAttempts(),
		NextReferenceRetryAt: tx.NextReferenceRetryAt(), CreatedAt: tx.CreatedAt(), UpdatedAt: tx.UpdatedAt(), ProcessedAt: tx.ProcessedAt(),
	}
	if b := tx.ResultBalance(); b != nil {
		j := b.ToJSON()
		out.Balance = &j
	}
	return out
}

// statusCodeFor maps the transaction outcome to the HTTP status: processed
// 200, business rejection 422, pending reference 202. Replays keep the code
// of the original outcome.
func statusCodeFor(tx *wagering.Transaction) int {
	switch tx.Status() {
	case wagering.StatusProcessed:
		return http.StatusOK
	case wagering.StatusRejected:
		return http.StatusUnprocessableEntity
	case wagering.StatusFailed:
		return http.StatusInternalServerError
	default:
		return http.StatusAccepted
	}
}

type ledgerEntryResponse struct {
	ID            uuid.UUID  `json:"id"`
	WalletID      uuid.UUID  `json:"walletId"`
	TransactionID uuid.UUID  `json:"transactionId"`
	Direction     string     `json:"direction"`
	Money         money.JSON `json:"money"`
	BalanceBefore money.JSON `json:"balanceBefore"`
	BalanceAfter  money.JSON `json:"balanceAfter"`
	CreatedAt     time.Time  `json:"createdAt"`
}

type ledgerResponse struct {
	Entries    []ledgerEntryResponse `json:"entries"`
	NextCursor string                `json:"nextCursor,omitempty"`
}

type reconciliationResponse struct {
	WalletID          uuid.UUID  `json:"walletId"`
	StoredBalance     money.JSON `json:"storedBalance"`
	CalculatedBalance money.JSON `json:"calculatedBalance"`
	Difference        money.JSON `json:"difference"`
	Consistent        bool       `json:"consistent"`
	CheckedEntries    int64      `json:"checkedEntries"`
}

// --- handlers ---

func (s *Server) openWallet(w http.ResponseWriter, r *http.Request) {
	var in openWalletRequest
	if err := decodeJSON(r, &in); err != nil {
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", "invalid JSON body: "+err.Error())
		return
	}
	playerID, err := uuid.Parse(in.PlayerID)
	if err != nil || playerID == uuid.Nil {
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", "playerId must be a UUID")
		return
	}
	initial, err := money.FromJSON(in.InitialBalance)
	if err != nil {
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", "initialBalance: "+err.Error())
		return
	}
	wlt, err := s.svc.OpenWallet(r.Context(), app.OpenWalletCommand{PlayerID: playerID, InitialBalance: initial, CorrelationID: observability.CorrelationID(r.Context())})
	if err != nil {
		s.writeAppError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, toWalletResponse(wlt))
}

func pathUUID(r *http.Request, name string) (uuid.UUID, error) {
	id, err := uuid.Parse(r.PathValue(name))
	if err != nil {
		return uuid.Nil, fmt.Errorf("%w: %s must be a UUID", wagering.ErrValidation, name)
	}
	return id, nil
}

func (s *Server) getWallet(w http.ResponseWriter, r *http.Request) {
	id, err := pathUUID(r, "walletId")
	if err != nil {
		s.writeAppError(w, r, err)
		return
	}
	wlt, err := s.svc.GetWallet(r.Context(), id)
	if err != nil {
		s.writeAppError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toWalletResponse(wlt))
}

func (s *Server) getLedger(w http.ResponseWriter, r *http.Request) {
	id, err := pathUUID(r, "walletId")
	if err != nil {
		s.writeAppError(w, r, err)
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	page, err := s.svc.ListLedger(r.Context(), id, r.URL.Query().Get("cursor"), limit)
	if err != nil {
		s.writeAppError(w, r, err)
		return
	}
	out := ledgerResponse{Entries: []ledgerEntryResponse{}, NextCursor: page.NextCursor}
	for _, e := range page.Entries {
		out.Entries = append(out.Entries, ledgerEntryResponse{ID: e.ID(), WalletID: e.WalletID(), TransactionID: e.TransactionID(), Direction: string(e.Direction()),
			Money: e.Amount().ToJSON(), BalanceBefore: e.BalanceBefore().ToJSON(), BalanceAfter: e.BalanceAfter().ToJSON(), CreatedAt: e.CreatedAt()})
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) reconcile(w http.ResponseWriter, r *http.Request) {
	id, err := pathUUID(r, "walletId")
	if err != nil {
		s.writeAppError(w, r, err)
		return
	}
	rec, err := s.svc.Reconcile(r.Context(), id)
	if err != nil {
		s.writeAppError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, reconciliationResponse{WalletID: rec.WalletID, StoredBalance: rec.StoredBalance.ToJSON(), CalculatedBalance: rec.CalculatedBalance.ToJSON(),
		Difference: rec.Difference.ToJSON(), Consistent: rec.Consistent, CheckedEntries: rec.CheckedEntries})
}

func (s *Server) postTransaction(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		writeError(w, http.StatusBadRequest, "MISSING_IDEMPOTENCY_KEY", "Idempotency-Key header is required")
		return
	}
	var in transactionRequest
	if err := decodeJSON(r, &in); err != nil {
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", "invalid JSON body: "+err.Error())
		return
	}
	p, _ := auth.PrincipalFrom(r.Context())
	if in.ProviderID != p.ProviderID {
		s.log.WarnContext(r.Context(), "provider mismatch", "tokenProviderId", p.ProviderID, "bodyProviderId", in.ProviderID)
		writeError(w, http.StatusForbidden, "PROVIDER_MISMATCH", "providerId does not match the authenticated provider")
		return
	}
	req, err := ToRequest(in)
	if err != nil {
		s.writeAppError(w, r, err)
		return
	}
	res, err := s.svc.Process(r.Context(), app.ProcessCommand{Request: req, IdempotencyKey: key, CorrelationID: observability.CorrelationID(r.Context()), Source: app.SourceHTTP})
	if err != nil {
		s.writeAppError(w, r, err)
		return
	}
	writeJSON(w, statusCodeFor(res.Transaction), toTransactionResponse(res.Transaction, res.IdempotentReplay))
}

func (s *Server) getTransaction(w http.ResponseWriter, r *http.Request) {
	id, err := pathUUID(r, "transactionId")
	if err != nil {
		s.writeAppError(w, r, err)
		return
	}
	p, _ := auth.PrincipalFrom(r.Context())
	if !p.IsInternal() && !p.IsProvider() {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "caller lacks the required role")
		return
	}
	tx, err := s.svc.GetTransaction(r.Context(), id)
	if err != nil {
		s.writeAppError(w, r, err)
		return
	}
	// Providers only see their own transactions; others look non-existent.
	if !p.IsInternal() && tx.ProviderID() != p.ProviderID {
		s.log.WarnContext(r.Context(), "cross-provider access denied", "providerId", p.ProviderID, "transactionId", id)
		writeError(w, http.StatusNotFound, "NOT_FOUND", app.ErrTransactionNotFound.Error())
		return
	}
	writeJSON(w, http.StatusOK, toTransactionResponse(tx, false))
}

func (s *Server) getProviderTransaction(w http.ResponseWriter, r *http.Request) {
	providerID := r.PathValue("providerId")
	p, _ := auth.PrincipalFrom(r.Context())
	if !p.IsInternal() && !(p.IsProvider() && p.ProviderID == providerID) {
		s.log.WarnContext(r.Context(), "cross-provider access denied", "providerId", p.ProviderID, "requestedProviderId", providerID)
		writeError(w, http.StatusForbidden, "FORBIDDEN", "provider may only query its own transactions")
		return
	}
	tx, err := s.svc.GetTransactionByExternalID(r.Context(), providerID, r.PathValue("externalTransactionId"))
	if err != nil {
		s.writeAppError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toTransactionResponse(tx, false))
}
