// Package observability provides JSON logging, Prometheus metrics and the
// correlation-id plumbing shared by every adapter.
package observability

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

type ctxKey int

const (
	keyCorrelation ctxKey = iota
	keyMessageID
)

// NewLogger builds a JSON slog logger at the given level.
func NewLogger(level, instanceID string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
	return slog.New(&ctxHandler{Handler: h}).With("instance", instanceID)
}

// ctxHandler injects correlation identifiers stored in the context.
type ctxHandler struct{ slog.Handler }

func (h *ctxHandler) Handle(ctx context.Context, r slog.Record) error {
	present := map[string]bool{}
	r.Attrs(func(a slog.Attr) bool { present[a.Key] = true; return true })
	if v, ok := ctx.Value(keyCorrelation).(string); ok && v != "" && !present["correlationId"] {
		r.AddAttrs(slog.String("correlationId", v))
	}
	if v, ok := ctx.Value(keyMessageID).(string); ok && v != "" && !present["messageId"] {
		r.AddAttrs(slog.String("messageId", v))
	}
	return h.Handler.Handle(ctx, r)
}

func (h *ctxHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &ctxHandler{Handler: h.Handler.WithAttrs(attrs)}
}

func (h *ctxHandler) WithGroup(name string) slog.Handler {
	return &ctxHandler{Handler: h.Handler.WithGroup(name)}
}

// WithCorrelationID stores the correlation id in the context.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, keyCorrelation, id)
}

// CorrelationID reads the correlation id from the context.
func CorrelationID(ctx context.Context) string {
	v, _ := ctx.Value(keyCorrelation).(string)
	return v
}

// WithMessageID stores the SQS message id in the context.
func WithMessageID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, keyMessageID, id)
}
