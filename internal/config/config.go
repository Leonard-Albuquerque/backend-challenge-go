// Package config loads and validates the process configuration from the
// environment. Every value has a documented default suitable for the local
// Docker Compose environment, except credentials that must be explicit.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the validated configuration of one instance.
type Config struct {
	InstanceID string
	Env        string
	LogLevel   string

	HTTPAddr            string
	HTTPShutdownTimeout time.Duration

	DatabaseURL   string
	DBMaxConns    int32
	DBAutoMigrate bool

	OIDCIssuer   string
	OIDCJWKSURL  string
	OIDCAudience string

	AWSRegion       string
	SQSEndpoint     string
	SQSWagerQueue   string
	SQSWagerDLQ     string
	SQSEventsQueue  string
	SQSConsumerName string
	// Consumer tuning.
	SQSWaitTime          time.Duration
	SQSVisibilityTimeout time.Duration
	SQSMaxMessages       int32
	SQSConsumerWorkers   int
	SQSConsumerEnabled   bool

	OutboxEnabled      bool
	OutboxPollInterval time.Duration
	OutboxBatchSize    int
	OutboxLease        time.Duration
	OutboxBaseBackoff  time.Duration
	OutboxMaxBackoff   time.Duration

	PendingWorkerEnabled  bool
	PendingPollInterval   time.Duration
	PendingBaseBackoff    time.Duration
	PendingMaxBackoff     time.Duration
	PendingMaxAttempts    int
	WorkerShutdownTimeout time.Duration

	// CrashPoint is a test hook: when set, the process exits abruptly at the
	// named point (see internal/fxapp/crash.go). Never set in production.
	CrashPoint string
}

// Load reads the environment.
func Load() (Config, error) {
	c := Config{
		InstanceID:            env("INSTANCE_ID", hostname()),
		Env:                   env("APP_ENV", "local"),
		LogLevel:              env("LOG_LEVEL", "info"),
		HTTPAddr:              env("HTTP_ADDR", ":8080"),
		HTTPShutdownTimeout:   duration("HTTP_SHUTDOWN_TIMEOUT", 15*time.Second),
		DatabaseURL:           env("DATABASE_URL", "postgres://wager:wager@localhost:5432/wager?sslmode=disable"),
		DBMaxConns:            int32(integer("DB_MAX_CONNS", 20)),
		DBAutoMigrate:         boolean("DB_AUTO_MIGRATE", true),
		OIDCIssuer:            env("OIDC_ISSUER", "http://localhost:8081/realms/wager"),
		OIDCJWKSURL:           env("OIDC_JWKS_URL", "http://localhost:8081/realms/wager/protocol/openid-connect/certs"),
		OIDCAudience:          env("OIDC_AUDIENCE", "wager-api"),
		AWSRegion:             env("AWS_REGION", "us-east-1"),
		SQSEndpoint:           env("SQS_ENDPOINT", "http://localhost:4566"),
		SQSWagerQueue:         env("SQS_WAGER_QUEUE", "wager-transactions.fifo"),
		SQSWagerDLQ:           env("SQS_WAGER_DLQ", "wager-transactions-dlq.fifo"),
		SQSEventsQueue:        env("SQS_EVENTS_QUEUE", "wallet-events.fifo"),
		SQSConsumerName:       env("SQS_CONSUMER_NAME", "wager-transactions-consumer"),
		SQSWaitTime:           duration("SQS_WAIT_TIME", 5*time.Second),
		SQSVisibilityTimeout:  duration("SQS_VISIBILITY_TIMEOUT", 30*time.Second),
		SQSMaxMessages:        int32(integer("SQS_MAX_MESSAGES", 10)),
		SQSConsumerWorkers:    integer("SQS_CONSUMER_WORKERS", 4),
		SQSConsumerEnabled:    boolean("SQS_CONSUMER_ENABLED", true),
		OutboxEnabled:         boolean("OUTBOX_ENABLED", true),
		OutboxPollInterval:    duration("OUTBOX_POLL_INTERVAL", 500*time.Millisecond),
		OutboxBatchSize:       integer("OUTBOX_BATCH_SIZE", 50),
		OutboxLease:           duration("OUTBOX_LEASE", 30*time.Second),
		OutboxBaseBackoff:     duration("OUTBOX_BASE_BACKOFF", time.Second),
		OutboxMaxBackoff:      duration("OUTBOX_MAX_BACKOFF", time.Minute),
		PendingWorkerEnabled:  boolean("PENDING_WORKER_ENABLED", true),
		PendingPollInterval:   duration("PENDING_POLL_INTERVAL", time.Second),
		PendingBaseBackoff:    duration("PENDING_BASE_BACKOFF", time.Second),
		PendingMaxBackoff:     duration("PENDING_MAX_BACKOFF", time.Minute),
		PendingMaxAttempts:    integer("PENDING_MAX_ATTEMPTS", 10),
		WorkerShutdownTimeout: duration("WORKER_SHUTDOWN_TIMEOUT", 20*time.Second),
		CrashPoint:            env("CRASH_POINT", ""),
	}
	return c, c.Validate()
}

// Validate checks the configuration is coherent.
func (c Config) Validate() error {
	var errs []error
	if c.HTTPAddr == "" {
		errs = append(errs, errors.New("HTTP_ADDR is required"))
	}
	if !strings.HasPrefix(c.DatabaseURL, "postgres://") && !strings.HasPrefix(c.DatabaseURL, "postgresql://") {
		errs = append(errs, errors.New("DATABASE_URL must be a postgres:// URL"))
	}
	if c.OIDCIssuer == "" || c.OIDCJWKSURL == "" || c.OIDCAudience == "" {
		errs = append(errs, errors.New("OIDC_ISSUER, OIDC_JWKS_URL and OIDC_AUDIENCE are required"))
	}
	if c.SQSWagerQueue == "" || c.SQSEventsQueue == "" || c.SQSWagerDLQ == "" {
		errs = append(errs, errors.New("SQS_WAGER_QUEUE, SQS_WAGER_DLQ and SQS_EVENTS_QUEUE are required"))
	}
	if c.SQSMaxMessages < 1 || c.SQSMaxMessages > 10 {
		errs = append(errs, errors.New("SQS_MAX_MESSAGES must be between 1 and 10"))
	}
	if c.SQSConsumerWorkers < 1 {
		errs = append(errs, errors.New("SQS_CONSUMER_WORKERS must be >= 1"))
	}
	if c.OutboxBatchSize < 1 || c.OutboxLease <= 0 {
		errs = append(errs, errors.New("OUTBOX_BATCH_SIZE must be >= 1 and OUTBOX_LEASE > 0"))
	}
	if c.PendingMaxAttempts < 1 {
		errs = append(errs, errors.New("PENDING_MAX_ATTEMPTS must be >= 1"))
	}
	if c.DBMaxConns < 1 {
		errs = append(errs, errors.New("DB_MAX_CONNS must be >= 1"))
	}
	return errors.Join(errs...)
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func integer(key string, def int) int {
	v := env(key, "")
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func boolean(key string, def bool) bool {
	v := strings.ToLower(env(key, ""))
	switch v {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	}
	return def
}

func duration(key string, def time.Duration) time.Duration {
	v := env(key, "")
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		return fmt.Sprintf("pid-%d", os.Getpid())
	}
	return h
}
