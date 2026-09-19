package app

import (
	"log/slog"
	"time"

	"github.com/google/uuid"
)

// Config holds the tunables of the use cases.
type Config struct {
	// PendingReference retry policy.
	PendingReferenceBaseBackoff time.Duration
	PendingReferenceMaxBackoff  time.Duration
	PendingReferenceMaxAttempts int
}

// DefaultConfig returns sensible local defaults.
func DefaultConfig() Config {
	return Config{
		PendingReferenceBaseBackoff: time.Second,
		PendingReferenceMaxBackoff:  time.Minute,
		PendingReferenceMaxAttempts: 10,
	}
}

// Service implements the wagering use cases.
type Service struct {
	uow     UnitOfWork
	metrics Metrics
	log     *slog.Logger
	now     Clock
	cfg     Config
}

// NewService builds the service. A nil metrics or logger falls back to no-ops.
func NewService(uow UnitOfWork, metrics Metrics, log *slog.Logger, cfg Config, clock Clock) *Service {
	if metrics == nil {
		metrics = NopMetrics{}
	}
	if log == nil {
		log = slog.Default()
	}
	if clock == nil {
		clock = func() time.Time { return time.Now().UTC() }
	}
	if cfg.PendingReferenceBaseBackoff <= 0 {
		cfg.PendingReferenceBaseBackoff = time.Second
	}
	if cfg.PendingReferenceMaxBackoff <= 0 {
		cfg.PendingReferenceMaxBackoff = time.Minute
	}
	if cfg.PendingReferenceMaxAttempts <= 0 {
		cfg.PendingReferenceMaxAttempts = 10
	}
	return &Service{uow: uow, metrics: metrics, log: log, now: clock, cfg: cfg}
}

// Config exposes the effective configuration.
func (s *Service) Config() Config { return s.cfg }

// NopMetrics discards every observation.
type NopMetrics struct{}

func (NopMetrics) TransactionOutcome(string, string, string) {}
func (NopMetrics) Duplicate(string)                          {}
func (NopMetrics) ConcurrencyConflict()                      {}
func (NopMetrics) ReconciliationDivergence()                 {}
func (NopMetrics) ProcessingDuration(string, time.Duration)  {}
func (NopMetrics) PendingReferenceRetry()                    {}

func newID() uuid.UUID {
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.New()
	}
	return id
}

// backoff computes the exponential delay for the given attempt (1-based).
func (s *Service) backoff(attempt int) time.Duration {
	d := s.cfg.PendingReferenceBaseBackoff
	for i := 1; i < attempt && d < s.cfg.PendingReferenceMaxBackoff; i++ {
		d *= 2
	}
	if d > s.cfg.PendingReferenceMaxBackoff {
		d = s.cfg.PendingReferenceMaxBackoff
	}
	return d
}
