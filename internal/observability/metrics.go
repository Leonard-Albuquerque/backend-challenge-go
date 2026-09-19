package observability

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds every Prometheus collector of the service.
type Metrics struct {
	registry *prometheus.Registry

	transactions     *prometheus.CounterVec
	duplicates       *prometheus.CounterVec
	retries          *prometheus.CounterVec
	dlq              *prometheus.CounterVec
	conflicts        prometheus.Counter
	reconDivergences prometheus.Counter
	processing       *prometheus.HistogramVec
	outboxLag        prometheus.Gauge
	outboxPending    prometheus.Gauge
	outboxPublished  prometheus.Counter
	httpRequests     *prometheus.CounterVec
	httpDuration     *prometheus.HistogramVec
}

// NewMetrics registers the collectors on a private registry.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		registry:         reg,
		transactions:     prometheus.NewCounterVec(prometheus.CounterOpts{Name: "wager_transactions_total", Help: "Transactions by kind, final status and source."}, []string{"kind", "status", "source"}),
		duplicates:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: "wager_duplicates_total", Help: "Idempotent replays and inbox duplicates by source."}, []string{"source"}),
		retries:          prometheus.NewCounterVec(prometheus.CounterOpts{Name: "wager_retries_total", Help: "Retries by component (outbox, pending_reference, sqs)."}, []string{"component"}),
		dlq:              prometheus.NewCounterVec(prometheus.CounterOpts{Name: "wager_dlq_messages_total", Help: "Messages sent to a dead-letter queue by reason."}, []string{"reason"}),
		conflicts:        prometheus.NewCounter(prometheus.CounterOpts{Name: "wager_concurrency_conflicts_total", Help: "Lost races on unique constraints or wallet versions."}),
		reconDivergences: prometheus.NewCounter(prometheus.CounterOpts{Name: "wager_reconciliation_divergences_total", Help: "Reconciliations whose stored balance differs from the ledger."}),
		processing:       prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "wager_processing_duration_seconds", Help: "Use-case processing latency.", Buckets: prometheus.DefBuckets}, []string{"source"}),
		outboxLag:        prometheus.NewGauge(prometheus.GaugeOpts{Name: "wager_outbox_lag_seconds", Help: "Age of the oldest unpublished outbox event."}),
		outboxPending:    prometheus.NewGauge(prometheus.GaugeOpts{Name: "wager_outbox_pending", Help: "Unpublished outbox events."}),
		outboxPublished:  prometheus.NewCounter(prometheus.CounterOpts{Name: "wager_outbox_published_total", Help: "Events published from the outbox."}),
		httpRequests:     prometheus.NewCounterVec(prometheus.CounterOpts{Name: "http_requests_total", Help: "HTTP requests by route and status."}, []string{"route", "status"}),
		httpDuration:     prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "http_request_duration_seconds", Help: "HTTP latency by route.", Buckets: prometheus.DefBuckets}, []string{"route"}),
	}
	reg.MustRegister(m.transactions, m.duplicates, m.retries, m.dlq, m.conflicts, m.reconDivergences, m.processing, m.outboxLag, m.outboxPending, m.outboxPublished, m.httpRequests, m.httpDuration)
	reg.MustRegister(prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	return m
}

// Handler serves the registry.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// Registry exposes the registry for tests.
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

func (m *Metrics) TransactionOutcome(kind, status, source string) {
	m.transactions.WithLabelValues(kind, status, source).Inc()
}
func (m *Metrics) Duplicate(source string)   { m.duplicates.WithLabelValues(source).Inc() }
func (m *Metrics) ConcurrencyConflict()      { m.conflicts.Inc() }
func (m *Metrics) ReconciliationDivergence() { m.reconDivergences.Inc() }
func (m *Metrics) ProcessingDuration(source string, d time.Duration) {
	m.processing.WithLabelValues(source).Observe(d.Seconds())
}
func (m *Metrics) PendingReferenceRetry() { m.retries.WithLabelValues("pending_reference").Inc() }
func (m *Metrics) Retry(component string) { m.retries.WithLabelValues(component).Inc() }
func (m *Metrics) DLQ(reason string)      { m.dlq.WithLabelValues(reason).Inc() }
func (m *Metrics) OutboxPublished()       { m.outboxPublished.Inc() }
func (m *Metrics) OutboxLag(lag time.Duration, pending int64) {
	m.outboxLag.Set(lag.Seconds())
	m.outboxPending.Set(float64(pending))
}
func (m *Metrics) HTTPRequest(route, status string, d time.Duration) {
	m.httpRequests.WithLabelValues(route, status).Inc()
	m.httpDuration.WithLabelValues(route).Observe(d.Seconds())
}
