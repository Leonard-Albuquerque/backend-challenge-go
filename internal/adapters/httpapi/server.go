// Package httpapi exposes the use cases over HTTP with net/http routing.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/jamesmachome/backend-challenge-go/internal/adapters/auth"
	"github.com/jamesmachome/backend-challenge-go/internal/adapters/postgres"
	"github.com/jamesmachome/backend-challenge-go/internal/app"
	"github.com/jamesmachome/backend-challenge-go/internal/domain/money"
	"github.com/jamesmachome/backend-challenge-go/internal/domain/wagering"
	"github.com/jamesmachome/backend-challenge-go/internal/observability"
)

// ReadinessChecker reports whether a dependency is usable.
type ReadinessChecker interface {
	Name() string
	Ready(ctx context.Context) error
}

// Server wires the handlers.
type Server struct {
	svc      *app.Service
	verifier *auth.Verifier
	metrics  *observability.Metrics
	log      *slog.Logger
	checks   []ReadinessChecker
	mux      *http.ServeMux
}

// NewServer builds the router.
func NewServer(svc *app.Service, verifier *auth.Verifier, metrics *observability.Metrics, log *slog.Logger, checks []ReadinessChecker) *Server {
	s := &Server{svc: svc, verifier: verifier, metrics: metrics, log: log, checks: checks, mux: http.NewServeMux()}
	s.routes()
	return s
}

// Handler returns the root handler with the global middleware applied.
func (s *Server) Handler() http.Handler {
	return s.correlation(s.mux)
}

func (s *Server) routes() {
	s.mux.Handle("GET /health/live", s.instrument("health_live", http.HandlerFunc(s.live)))
	s.mux.Handle("GET /health/ready", s.instrument("health_ready", http.HandlerFunc(s.ready)))
	s.mux.Handle("GET /metrics", s.metrics.Handler())

	internal := func(name string, h http.HandlerFunc) http.Handler {
		return s.instrument(name, s.authenticate(s.requireRole(auth.RoleInternal, h)))
	}
	s.mux.Handle("POST /wallets", internal("open_wallet", s.openWallet))
	s.mux.Handle("GET /wallets/{walletId}", internal("get_wallet", s.getWallet))
	s.mux.Handle("GET /wallets/{walletId}/ledger", internal("get_ledger", s.getLedger))
	s.mux.Handle("POST /wallets/{walletId}/reconciliation", internal("reconcile", s.reconcile))

	s.mux.Handle("POST /wagering/transactions", s.instrument("post_transaction", s.authenticate(s.requireRole(auth.RoleProvider, s.postTransaction))))
	s.mux.Handle("GET /wagering/transactions/{transactionId}", s.instrument("get_transaction", s.authenticate(http.HandlerFunc(s.getTransaction))))
	s.mux.Handle("GET /providers/{providerId}/wagering/transactions/{externalTransactionId}", s.instrument("get_provider_transaction", s.authenticate(http.HandlerFunc(s.getProviderTransaction))))
}

// --- middleware ---

func (s *Server) correlation(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Correlation-Id")
		if id == "" || len(id) > 128 {
			id = uuid.NewString()
		}
		w.Header().Set("X-Correlation-Id", id)
		next.ServeHTTP(w, r.WithContext(observability.WithCorrelationID(r.Context(), id)))
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (s *Server) instrument(route string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)
		s.metrics.HTTPRequest(route, strconv.Itoa(sw.status), time.Since(start))
		s.log.DebugContext(r.Context(), "http request", "route", route, "method", r.Method, "status", sw.status, "durationMs", time.Since(start).Milliseconds())
	})
}

func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, err := s.verifier.FromRequest(r)
		if err != nil {
			w.Header().Set("WWW-Authenticate", `Bearer realm="wager"`)
			code := "INVALID_TOKEN"
			if errors.Is(err, auth.ErrMissingToken) {
				code = "MISSING_TOKEN"
			}
			s.log.WarnContext(r.Context(), "authentication failed", "reason", code)
			writeError(w, http.StatusUnauthorized, code, "authentication required")
			return
		}
		next.ServeHTTP(w, r.WithContext(auth.WithPrincipal(r.Context(), p)))
	})
}

func (s *Server) requireRole(role string, next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, _ := auth.PrincipalFrom(r.Context())
		if !p.HasRole(role) || (role == auth.RoleProvider && p.ProviderID == "") {
			writeError(w, http.StatusForbidden, "FORBIDDEN", "caller lacks the required role")
			return
		}
		next(w, r)
	})
}

// --- health ---

func (s *Server) live(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "UP"})
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	out := map[string]string{}
	status := http.StatusOK
	for _, c := range s.checks {
		if err := c.Ready(ctx); err != nil {
			out[c.Name()] = "DOWN: " + err.Error()
			status = http.StatusServiceUnavailable
		} else {
			out[c.Name()] = "UP"
		}
	}
	writeJSON(w, status, map[string]any{"status": map[bool]string{true: "UP", false: "DOWN"}[status == http.StatusOK], "checks": out})
}

// --- responses ---

type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	if status == http.StatusServiceUnavailable {
		w.Header().Set("Retry-After", "1")
	}
	writeJSON(w, status, errorBody{Error: errorDetail{Code: code, Message: msg}})
}

// writeAppError maps use-case errors to the documented HTTP contract.
func (s *Server) writeAppError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, wagering.ErrValidation), errors.Is(err, money.ErrInvalidAmount), errors.Is(err, money.ErrInvalidCurrency),
		errors.Is(err, money.ErrNegativeAmount), errors.Is(err, money.ErrOverflow), errors.Is(err, app.ErrInvalidCursor):
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
	case errors.Is(err, app.ErrWalletNotFound) && r.Method == http.MethodPost && r.URL.Path == "/wagering/transactions":
		writeError(w, http.StatusUnprocessableEntity, "WALLET_NOT_FOUND", err.Error())
	case app.IsNotFound(err):
		writeError(w, http.StatusNotFound, "NOT_FOUND", err.Error())
	case errors.Is(err, app.ErrWalletExists):
		writeError(w, http.StatusConflict, "WALLET_EXISTS", err.Error())
	case errors.Is(err, app.ErrIdempotencyConflict):
		writeError(w, http.StatusConflict, "IDEMPOTENCY_KEY_CONFLICT", err.Error())
	case errors.Is(err, app.ErrExternalIDConflict):
		writeError(w, http.StatusConflict, "EXTERNAL_TRANSACTION_ID_CONFLICT", err.Error())
	case errors.Is(err, app.ErrDuplicateTransaction), errors.Is(err, app.ErrConcurrentModification):
		writeError(w, http.StatusConflict, "CONCURRENT_UPDATE", "the operation raced with another writer; retry with the same idempotency key")
	case postgres.IsTransient(err):
		s.log.WarnContext(r.Context(), "transient failure", "error", err.Error())
		writeError(w, http.StatusServiceUnavailable, "TEMPORARILY_UNAVAILABLE", "a dependency is unavailable; retry with the same idempotency key")
	default:
		s.log.ErrorContext(r.Context(), "unexpected failure", "error", err.Error())
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "unexpected failure")
	}
}

func decodeJSON(r *http.Request, v any) error {
	dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	return nil
}
