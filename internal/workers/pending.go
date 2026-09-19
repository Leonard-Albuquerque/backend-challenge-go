package workers

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/jamesmachome/backend-challenge-go/internal/app"
)

// PendingReferenceWorker retries PENDING_REFERENCE transactions.
type PendingReferenceWorker struct {
	svc  *app.Service
	log  *slog.Logger
	poll time.Duration
	loop loop
}

// NewPendingReferenceWorker builds the worker.
func NewPendingReferenceWorker(svc *app.Service, log *slog.Logger, poll time.Duration) *PendingReferenceWorker {
	if poll <= 0 {
		poll = time.Second
	}
	return &PendingReferenceWorker{svc: svc, log: log.With("component", "pending-reference-worker"), poll: poll}
}

// Start launches the loop.
func (w *PendingReferenceWorker) Start() {
	w.loop.start(w.poll, w.tick)
	w.log.Info("pending reference worker started")
}

// Stop cancels the loop and waits until ctx expires.
func (w *PendingReferenceWorker) Stop(ctx context.Context) error {
	err := w.loop.stop(ctx)
	w.log.Info("pending reference worker stopped")
	return err
}

func (w *PendingReferenceWorker) tick(ctx context.Context) bool {
	n, err := w.svc.ResolvePendingReferences(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		w.log.Warn("pending reference pass failed", "error", err.Error(), "handled", n)
	}
	return false
}
