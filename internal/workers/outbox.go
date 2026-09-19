// Package workers hosts the background loops: outbox publication and pending
// reference resolution. Both are safe to run in many instances concurrently.
package workers

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/jamesmachome/backend-challenge-go/internal/app"
)

// EventPublisher sends a claimed outbox record to the broker.
type EventPublisher interface {
	Publish(ctx context.Context, rec app.OutboxRecord) error
}

// OutboxMetrics is what the publisher reports.
type OutboxMetrics interface {
	Retry(component string)
	OutboxPublished()
	OutboxLag(lag time.Duration, pending int64)
}

// OutboxConfig tunes the publisher.
type OutboxConfig struct {
	PublisherID  string
	PollInterval time.Duration
	BatchSize    int
	Lease        time.Duration
	BaseBackoff  time.Duration
	MaxBackoff   time.Duration
	// Test hooks: exit the process at the named point.
	CrashAfterClaim   func()
	CrashAfterPublish func()
}

// OutboxPublisher claims pending events with a lease and publishes them.
type OutboxPublisher struct {
	uow       app.UnitOfWork
	publisher EventPublisher
	metrics   OutboxMetrics
	log       *slog.Logger
	cfg       OutboxConfig
	loop      loop
}

// NewOutboxPublisher builds the worker.
func NewOutboxPublisher(uow app.UnitOfWork, publisher EventPublisher, metrics OutboxMetrics, log *slog.Logger, cfg OutboxConfig) *OutboxPublisher {
	if cfg.PublisherID == "" {
		cfg.PublisherID = uuid.NewString()
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 500 * time.Millisecond
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 50
	}
	if cfg.Lease <= 0 {
		cfg.Lease = 30 * time.Second
	}
	if cfg.BaseBackoff <= 0 {
		cfg.BaseBackoff = time.Second
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = time.Minute
	}
	return &OutboxPublisher{uow: uow, publisher: publisher, metrics: metrics, log: log.With("component", "outbox-publisher", "publisherId", cfg.PublisherID), cfg: cfg}
}

// Start launches the loop.
func (p *OutboxPublisher) Start() {
	p.loop.start(p.cfg.PollInterval, p.tick)
	p.log.Info("outbox publisher started")
}

// Stop cancels the loop and waits for the current tick until ctx expires.
func (p *OutboxPublisher) Stop(ctx context.Context) error {
	err := p.loop.stop(ctx)
	p.log.Info("outbox publisher stopped")
	return err
}

// tick publishes one batch. It returns true when more work may be pending.
func (p *OutboxPublisher) tick(ctx context.Context) bool {
	now := time.Now().UTC()
	var batch []app.OutboxRecord
	err := p.uow.Do(ctx, func(ctx context.Context, st app.Store) error {
		var err error
		batch, err = st.Outbox().Claim(ctx, p.cfg.PublisherID, now, p.cfg.Lease, p.cfg.BatchSize)
		if err != nil {
			return err
		}
		lag, pending, err := st.Outbox().Lag(ctx, now)
		if err == nil {
			p.metrics.OutboxLag(lag, pending)
		}
		return err
	})
	if err != nil {
		p.log.Warn("outbox claim failed", "error", err.Error())
		p.metrics.Retry("outbox_claim")
		return false
	}
	if len(batch) == 0 {
		return false
	}
	if p.cfg.CrashAfterClaim != nil {
		p.cfg.CrashAfterClaim()
	}
	for _, rec := range batch {
		if ctx.Err() != nil {
			// Leave the lease to expire; another instance will take over.
			return false
		}
		p.publishOne(ctx, rec)
	}
	return len(batch) == p.cfg.BatchSize
}

func (p *OutboxPublisher) publishOne(ctx context.Context, rec app.OutboxRecord) {
	log := p.log.With("eventId", rec.EventID, "eventType", rec.EventType, "aggregateId", rec.AggregateID, "attempt", rec.Attempts)
	if err := p.publisher.Publish(ctx, rec); err != nil {
		next := time.Now().UTC().Add(p.backoff(rec.Attempts))
		log.Warn("publish failed; rescheduled", "error", err.Error(), "nextAttemptAt", next)
		p.metrics.Retry("outbox_publish")
		if rerr := p.uow.Do(ctx, func(ctx context.Context, st app.Store) error {
			return st.Outbox().Reschedule(ctx, rec.EventID, next, err.Error())
		}); rerr != nil {
			log.Warn("reschedule failed; lease will expire", "error", rerr.Error())
		}
		return
	}
	if p.cfg.CrashAfterPublish != nil {
		p.cfg.CrashAfterPublish()
	}
	if err := p.uow.Do(ctx, func(ctx context.Context, st app.Store) error {
		return st.Outbox().MarkPublished(ctx, rec.EventID, time.Now().UTC())
	}); err != nil {
		// The event was delivered; if the ack is lost it is republished with
		// the same eventId and deduplicated downstream.
		log.Warn("mark published failed; event may be republished", "error", err.Error())
		return
	}
	p.metrics.OutboxPublished()
	log.Debug("event published")
}

func (p *OutboxPublisher) backoff(attempt int) time.Duration {
	d := p.cfg.BaseBackoff
	for i := 1; i < attempt && d < p.cfg.MaxBackoff; i++ {
		d *= 2
	}
	if d > p.cfg.MaxBackoff {
		d = p.cfg.MaxBackoff
	}
	return d
}

// loop is a cancellable periodic runner shared by the workers.
type loop struct {
	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

func (l *loop) start(interval time.Duration, tick func(ctx context.Context) bool) {
	ctx, cancel := context.WithCancel(context.Background())
	l.cancel = cancel
	l.done = make(chan struct{})
	go func() {
		defer close(l.done)
		timer := time.NewTimer(0)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			}
			more := tick(ctx)
			if ctx.Err() != nil {
				return
			}
			if more {
				timer.Reset(0)
			} else {
				timer.Reset(interval)
			}
		}
	}()
}

func (l *loop) stop(ctx context.Context) error {
	if l.cancel == nil {
		return nil
	}
	l.once.Do(l.cancel)
	select {
	case <-l.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
