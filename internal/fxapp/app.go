// Package fxapp composes the service with Uber Fx: configuration, connections,
// repositories, use cases, HTTP handlers and workers, each with lifecycle
// hooks so shutdown drains work before dependencies close.
package fxapp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"

	"github.com/jamesmachome/backend-challenge-go/internal/adapters/auth"
	"github.com/jamesmachome/backend-challenge-go/internal/adapters/httpapi"
	"github.com/jamesmachome/backend-challenge-go/internal/adapters/postgres"
	sqsadapter "github.com/jamesmachome/backend-challenge-go/internal/adapters/sqs"
	"github.com/jamesmachome/backend-challenge-go/internal/app"
	"github.com/jamesmachome/backend-challenge-go/internal/config"
	"github.com/jamesmachome/backend-challenge-go/internal/observability"
	"github.com/jamesmachome/backend-challenge-go/internal/workers"
)

// Options returns the full Fx graph. Tests may append overrides.
func Options(cfg config.Config) fx.Option {
	return fx.Options(
		fx.Supply(cfg),
		fx.WithLogger(func(log *slog.Logger) fxevent.Logger {
			return &fxevent.SlogLogger{Logger: log.With("component", "fx")}
		}),
		ObservabilityModule,
		PostgresModule,
		AuthModule,
		SQSModule,
		AppModule,
		HTTPModule,
		WorkersModule,
	)
}

// New builds the application.
func New(cfg config.Config) *fx.App {
	return fx.New(Options(cfg), fx.StartTimeout(60*time.Second), fx.StopTimeout(cfg.WorkerShutdownTimeout+cfg.HTTPShutdownTimeout+5*time.Second))
}

// ObservabilityModule provides the logger and metrics.
var ObservabilityModule = fx.Module("observability",
	fx.Provide(
		func(cfg config.Config) *slog.Logger { return observability.NewLogger(cfg.LogLevel, cfg.InstanceID) },
		observability.NewMetrics,
		func(m *observability.Metrics) app.Metrics { return m },
	),
)

// PostgresModule provides the pool, migrations and the unit of work.
var PostgresModule = fx.Module("postgres",
	fx.Provide(newPool, postgres.NewUnitOfWork, func(u *postgres.UnitOfWork) app.UnitOfWork { return u }),
	fx.Invoke(migrate),
)

func newPool(lc fx.Lifecycle, cfg config.Config, log *slog.Logger) (*pgxpool.Pool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := postgres.NewPool(ctx, postgres.Config{DSN: cfg.DatabaseURL, MaxConns: cfg.DBMaxConns, ConnectTimeout: 10 * time.Second})
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{OnStop: func(context.Context) error {
		// Runs last: every component that uses the pool has already stopped.
		pool.Close()
		log.Info("postgres pool closed")
		return nil
	}})
	return pool, nil
}

func migrate(cfg config.Config, log *slog.Logger, _ *pgxpool.Pool) error {
	if !cfg.DBAutoMigrate {
		return nil
	}
	m, err := postgres.NewMigrator(cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer m.Close()
	if err := m.Up(); err != nil {
		return fmt.Errorf("migrations: %w", err)
	}
	v, _, _ := m.Version()
	log.Info("migrations applied", "version", v)
	return nil
}

// AuthModule provides the OIDC verifier.
var AuthModule = fx.Module("auth",
	fx.Provide(func(cfg config.Config) (*auth.Verifier, error) {
		return auth.NewVerifier(context.Background(), auth.Config{Issuer: cfg.OIDCIssuer, JWKSURL: cfg.OIDCJWKSURL, Audience: cfg.OIDCAudience})
	}),
)

// SQSModule provides the client, publisher and consumer.
var SQSModule = fx.Module("sqs",
	fx.Provide(
		func(cfg config.Config) (*sqsadapter.Client, error) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			c, err := sqsadapter.NewClient(ctx, sqsadapter.Config{Region: cfg.AWSRegion, Endpoint: cfg.SQSEndpoint, AccessKeyID: os.Getenv("AWS_ACCESS_KEY_ID"), SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY")})
			if err != nil {
				return nil, err
			}
			if err := c.Resolve(ctx, cfg.SQSWagerQueue, cfg.SQSWagerDLQ, cfg.SQSEventsQueue); err != nil {
				return nil, err
			}
			return c, nil
		},
		func(c *sqsadapter.Client, cfg config.Config) *sqsadapter.Publisher {
			return sqsadapter.NewPublisher(c, cfg.SQSEventsQueue)
		},
		func(c *sqsadapter.Client, uow *postgres.UnitOfWork, svc *app.Service, m *observability.Metrics, log *slog.Logger, cfg config.Config) *sqsadapter.Consumer {
			return sqsadapter.NewConsumer(c, uow, svc, m, log, sqsadapter.ConsumerConfig{
				QueueName: cfg.SQSWagerQueue, DLQName: cfg.SQSWagerDLQ, ConsumerName: cfg.SQSConsumerName,
				WaitTime: cfg.SQSWaitTime, VisibilityTimeout: cfg.SQSVisibilityTimeout, MaxMessages: cfg.SQSMaxMessages, Workers: cfg.SQSConsumerWorkers,
				CrashAfterCommit: crashPoint(cfg, log, "consumer.after_commit"),
			})
		},
	),
	fx.Invoke(func(lc fx.Lifecycle, c *sqsadapter.Consumer, cfg config.Config) {
		if !cfg.SQSConsumerEnabled {
			return
		}
		lc.Append(fx.Hook{
			OnStart: c.Start,
			OnStop: func(ctx context.Context) error {
				ctx, cancel := context.WithTimeout(ctx, cfg.WorkerShutdownTimeout)
				defer cancel()
				return c.Stop(ctx)
			},
		})
	}),
)

// AppModule provides the use cases.
var AppModule = fx.Module("app",
	fx.Provide(func(uow app.UnitOfWork, m app.Metrics, log *slog.Logger, cfg config.Config) *app.Service {
		return app.NewService(uow, m, log, app.Config{
			PendingReferenceBaseBackoff: cfg.PendingBaseBackoff,
			PendingReferenceMaxBackoff:  cfg.PendingMaxBackoff,
			PendingReferenceMaxAttempts: cfg.PendingMaxAttempts,
		}, nil)
	}),
)

// HTTPModule provides the server and its lifecycle.
var HTTPModule = fx.Module("http",
	fx.Provide(
		newReadinessChecks,
		func(svc *app.Service, v *auth.Verifier, m *observability.Metrics, log *slog.Logger, checks []httpapi.ReadinessChecker) *httpapi.Server {
			return httpapi.NewServer(svc, v, m, log, checks)
		},
		func(cfg config.Config, s *httpapi.Server) *http.Server {
			return &http.Server{Addr: cfg.HTTPAddr, Handler: s.Handler(), ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
		},
	),
	fx.Invoke(func(lc fx.Lifecycle, srv *http.Server, cfg config.Config, log *slog.Logger) {
		var ln net.Listener
		lc.Append(fx.Hook{
			OnStart: func(context.Context) error {
				var err error
				ln, err = net.Listen("tcp", srv.Addr)
				if err != nil {
					return fmt.Errorf("http listen %s: %w", srv.Addr, err)
				}
				log.Info("http server listening", "addr", ln.Addr().String())
				go func() {
					if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
						log.Error("http server failed", "error", err.Error())
					}
				}()
				return nil
			},
			OnStop: func(ctx context.Context) error {
				// Stops accepting new requests and waits for in-flight ones.
				ctx, cancel := context.WithTimeout(ctx, cfg.HTTPShutdownTimeout)
				defer cancel()
				err := srv.Shutdown(ctx)
				log.Info("http server stopped")
				return err
			},
		})
	}),
)

type readiness struct {
	name string
	fn   func(ctx context.Context) error
}

func (r readiness) Name() string                    { return r.name }
func (r readiness) Ready(ctx context.Context) error { return r.fn(ctx) }

func newReadinessChecks(pool *pgxpool.Pool, c *sqsadapter.Client, v *auth.Verifier, cfg config.Config) []httpapi.ReadinessChecker {
	return []httpapi.ReadinessChecker{
		readiness{"postgres", pool.Ping},
		readiness{"sqs", func(ctx context.Context) error { return c.Ready(ctx, cfg.SQSWagerQueue) }},
		readiness{"oidc", v.Ready},
	}
}

// WorkersModule provides the outbox publisher and the pending worker.
var WorkersModule = fx.Module("workers",
	fx.Provide(
		func(uow app.UnitOfWork, p *sqsadapter.Publisher, m *observability.Metrics, log *slog.Logger, cfg config.Config) *workers.OutboxPublisher {
			return workers.NewOutboxPublisher(uow, p, m, log, workers.OutboxConfig{
				PublisherID: cfg.InstanceID, PollInterval: cfg.OutboxPollInterval, BatchSize: cfg.OutboxBatchSize, Lease: cfg.OutboxLease,
				BaseBackoff: cfg.OutboxBaseBackoff, MaxBackoff: cfg.OutboxMaxBackoff,
				CrashAfterClaim: crashPoint(cfg, log, "outbox.after_claim"), CrashAfterPublish: crashPoint(cfg, log, "outbox.after_publish"),
			})
		},
		func(svc *app.Service, log *slog.Logger, cfg config.Config) *workers.PendingReferenceWorker {
			return workers.NewPendingReferenceWorker(svc, log, cfg.PendingPollInterval)
		},
	),
	fx.Invoke(func(lc fx.Lifecycle, ob *workers.OutboxPublisher, pw *workers.PendingReferenceWorker, cfg config.Config) {
		if cfg.OutboxEnabled {
			lc.Append(fx.Hook{
				OnStart: func(context.Context) error { ob.Start(); return nil },
				OnStop: func(ctx context.Context) error {
					ctx, cancel := context.WithTimeout(ctx, cfg.WorkerShutdownTimeout)
					defer cancel()
					return ob.Stop(ctx)
				},
			})
		}
		if cfg.PendingWorkerEnabled {
			lc.Append(fx.Hook{
				OnStart: func(context.Context) error { pw.Start(); return nil },
				OnStop: func(ctx context.Context) error {
					ctx, cancel := context.WithTimeout(ctx, cfg.WorkerShutdownTimeout)
					defer cancel()
					return pw.Stop(ctx)
				},
			})
		}
	}),
)

// crashPoint returns a function that terminates the process abruptly when
// cfg.CrashPoint equals name. It exists only to demonstrate recovery in the
// integration tests and is a no-op otherwise.
func crashPoint(cfg config.Config, log *slog.Logger, name string) func() {
	if cfg.CrashPoint != name {
		return nil
	}
	return func() {
		log.Error("CRASH_POINT reached; exiting abruptly", "crashPoint", name)
		os.Exit(137)
	}
}
