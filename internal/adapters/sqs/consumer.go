package sqs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/jamesmachome/backend-challenge-go/internal/adapters/httpapi"
	"github.com/jamesmachome/backend-challenge-go/internal/adapters/postgres"
	"github.com/jamesmachome/backend-challenge-go/internal/app"
	"github.com/jamesmachome/backend-challenge-go/internal/domain/wagering"
	"github.com/jamesmachome/backend-challenge-go/internal/observability"
)

// MessageType accepted on the wager queue.
const MessageType = "WagerTransactionRequested"

// Envelope is the body of a message on wager-transactions.fifo.
type Envelope struct {
	MessageID  string    `json:"messageId"`
	Type       string    `json:"type"`
	OccurredAt time.Time `json:"occurredAt"`
	Data       Payload   `json:"data"`
}

// Payload is the business content: the HTTP DTO plus the idempotency key.
type Payload struct {
	httpapi.TransactionRequest
	IdempotencyKey string `json:"idempotencyKey"`
}

// ConsumerConfig tunes the consumer.
type ConsumerConfig struct {
	QueueName         string
	DLQName           string
	ConsumerName      string
	WaitTime          time.Duration
	VisibilityTimeout time.Duration
	MaxMessages       int32
	Workers           int
	// CrashAfterCommit is a test hook: exit the process after the commit and
	// before deleting the message.
	CrashAfterCommit func()
}

// ConsumerMetrics is what the consumer reports.
type ConsumerMetrics interface {
	Duplicate(source string)
	Retry(component string)
	DLQ(reason string)
}

// Consumer pulls provider operations and applies them through the shared
// use case, deduplicating with the inbox inside the same SQL transaction.
type Consumer struct {
	client  *Client
	uow     *postgres.UnitOfWork
	svc     *app.Service
	metrics ConsumerMetrics
	log     *slog.Logger
	cfg     ConsumerConfig

	queueURL string
	dlqURL   string
	wg       sync.WaitGroup
	cancel   context.CancelFunc
	done     chan struct{}
}

// NewConsumer builds the consumer.
func NewConsumer(client *Client, uow *postgres.UnitOfWork, svc *app.Service, metrics ConsumerMetrics, log *slog.Logger, cfg ConsumerConfig) *Consumer {
	if cfg.Workers < 1 {
		cfg.Workers = 1
	}
	if cfg.MaxMessages < 1 {
		cfg.MaxMessages = 10
	}
	if cfg.WaitTime <= 0 {
		cfg.WaitTime = 5 * time.Second
	}
	return &Consumer{client: client, uow: uow, svc: svc, metrics: metrics, log: log.With("component", "sqs-consumer"), cfg: cfg}
}

// Start resolves the queues and launches the worker goroutines.
func (c *Consumer) Start(ctx context.Context) error {
	var err error
	if c.queueURL, err = c.client.QueueURL(ctx, c.cfg.QueueName); err != nil {
		return err
	}
	if c.dlqURL, err = c.client.QueueURL(ctx, c.cfg.DLQName); err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.done = make(chan struct{})
	for i := 0; i < c.cfg.Workers; i++ {
		c.wg.Add(1)
		go c.loop(runCtx, i)
	}
	go func() { c.wg.Wait(); close(c.done) }()
	c.log.Info("consumer started", "queue", c.cfg.QueueName, "workers", c.cfg.Workers)
	return nil
}

// Stop halts polling and waits for in-flight messages until ctx expires.
// Messages still in flight after the deadline are left to the visibility
// timeout, which redelivers them safely thanks to the inbox.
func (c *Consumer) Stop(ctx context.Context) error {
	if c.cancel == nil {
		return nil
	}
	c.cancel()
	select {
	case <-c.done:
		c.log.Info("consumer stopped")
		return nil
	case <-ctx.Done():
		c.log.Warn("consumer stop timed out; in-flight messages will be redelivered")
		return ctx.Err()
	}
}

func (c *Consumer) loop(ctx context.Context, worker int) {
	defer c.wg.Done()
	for ctx.Err() == nil {
		out, err := c.client.api.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:              aws.String(c.queueURL),
			MaxNumberOfMessages:   c.cfg.MaxMessages,
			WaitTimeSeconds:       int32(c.cfg.WaitTime / time.Second),
			VisibilityTimeout:     int32(c.cfg.VisibilityTimeout / time.Second),
			MessageAttributeNames: []string{"All"},
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{
				types.MessageSystemAttributeNameApproximateReceiveCount,
				types.MessageSystemAttributeNameMessageGroupId,
			},
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.log.Warn("receive failed", "error", err.Error(), "worker", worker)
			c.metrics.Retry("sqs_receive")
			select {
			case <-time.After(time.Second):
			case <-ctx.Done():
			}
			continue
		}
		for _, m := range out.Messages {
			// Finish the message even if shutdown started: the handling is
			// bounded by the SQL transaction and the stop deadline.
			c.handle(context.WithoutCancel(ctx), m)
		}
	}
}

// Outcome of one message.
type outcome int

const (
	outcomeDone outcome = iota
	outcomeTransient
	outcomePermanent
)

var errAlreadyProcessed = errors.New("message already processed")

func (c *Consumer) handle(ctx context.Context, m types.Message) {
	receipt := aws.ToString(m.ReceiptHandle)
	body := aws.ToString(m.Body)
	receiveCount, _ := strconv.Atoi(m.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
	log := c.log.With("sqsMessageId", aws.ToString(m.MessageId), "receiveCount", receiveCount)

	var env Envelope
	if err := json.Unmarshal([]byte(body), &env); err != nil || env.MessageID == "" || env.Type != MessageType {
		log.Error("invalid envelope", "error", fmt.Sprint(err), "type", env.Type)
		c.deadLetter(ctx, m, "invalid_envelope")
		return
	}
	ctx = observability.WithCorrelationID(observability.WithMessageID(ctx, env.MessageID), env.MessageID)
	log = log.With("messageId", env.MessageID)
	if env.Data.IdempotencyKey == "" {
		log.Error("missing idempotency key")
		c.deadLetter(ctx, m, "missing_idempotency_key")
		return
	}
	req, err := httpapi.ToRequest(env.Data.TransactionRequest)
	if err != nil {
		log.Error("invalid payload", "error", err.Error())
		c.deadLetter(ctx, m, "invalid_payload")
		return
	}
	// The inbox hash covers the business payload and the idempotency key, so
	// a redelivery (same messageId) with different content is detected while
	// envelope metadata such as occurredAt does not matter.
	sum := sha256.Sum256([]byte(app.PayloadHash(req) + "|" + env.Data.IdempotencyKey))
	bodyHash := hex.EncodeToString(sum[:])
	corr := env.MessageID

	var res app.ProcessResult
	err = c.uow.Do(ctx, func(ctx context.Context, st app.Store) error {
		in, err := st.Inbox().Record(ctx, c.cfg.ConsumerName, env.MessageID, bodyHash, time.Now().UTC())
		if err != nil {
			return err
		}
		if !in.Inserted {
			if in.ExistingHash != bodyHash {
				return fmt.Errorf("%w: messageId %q redelivered with a different body", wagering.ErrValidation, env.MessageID)
			}
			return errAlreadyProcessed
		}
		r, err := c.svc.ProcessIn(ctx, st, app.ProcessCommand{Request: req, IdempotencyKey: env.Data.IdempotencyKey, CorrelationID: corr, Source: app.SourceSQS})
		res = r
		return err
	})

	switch classify(err) {
	case outcomeDone:
		if errors.Is(err, errAlreadyProcessed) {
			c.metrics.Duplicate("sqs-inbox")
			log.Info("duplicate delivery ignored by inbox")
		} else {
			log.Info("message processed", "transactionId", res.Transaction.ID(), "walletId", res.Transaction.WalletID(), "providerId", res.Transaction.ProviderID(),
				"status", res.Transaction.Status(), "failureCode", res.Transaction.FailureCode(), "idempotentReplay", res.IdempotentReplay)
		}
		if c.cfg.CrashAfterCommit != nil {
			c.cfg.CrashAfterCommit()
		}
		c.delete(ctx, receipt, log)
	case outcomePermanent:
		log.Error("permanent failure; sending to DLQ", "error", err.Error())
		c.deadLetter(ctx, m, "permanent_error")
	case outcomeTransient:
		c.metrics.Retry("sqs_message")
		backoff := visibilityBackoff(receiveCount)
		log.Warn("transient failure; message will be redelivered", "error", err.Error(), "retryIn", backoff.String())
		_, verr := c.client.api.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{QueueUrl: aws.String(c.queueURL), ReceiptHandle: aws.String(receipt), VisibilityTimeout: int32(backoff / time.Second)})
		if verr != nil {
			log.Warn("change visibility failed", "error", verr.Error())
		}
	}
}

// classify decides whether the message is done, must be retried, or is
// permanently unprocessable. Business rejections are committed as REJECTED
// and therefore count as done; idempotency conflicts cannot be fixed by
// retrying and go to the DLQ for investigation.
func classify(err error) outcome {
	switch {
	case err == nil, errors.Is(err, errAlreadyProcessed):
		return outcomeDone
	case errors.Is(err, wagering.ErrValidation), errors.Is(err, app.ErrIdempotencyConflict), errors.Is(err, app.ErrExternalIDConflict), errors.Is(err, app.ErrWalletNotFound):
		return outcomePermanent
	case errors.Is(err, app.ErrDuplicateTransaction), errors.Is(err, app.ErrConcurrentModification):
		return outcomeTransient
	case postgres.IsTransient(err):
		return outcomeTransient
	}
	return outcomeTransient
}

func visibilityBackoff(receiveCount int) time.Duration {
	d := 2 * time.Second
	for i := 1; i < receiveCount && d < 5*time.Minute; i++ {
		d *= 2
	}
	if d > 5*time.Minute {
		d = 5 * time.Minute
	}
	return d
}

func (c *Consumer) delete(ctx context.Context, receipt string, log *slog.Logger) {
	if _, err := c.client.api.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(c.queueURL), ReceiptHandle: aws.String(receipt)}); err != nil {
		// The inbox makes the redelivery harmless.
		log.Warn("delete failed; inbox will absorb the redelivery", "error", err.Error())
	}
}

// deadLetter copies the message to the DLQ and deletes it from the source
// queue. The DLQ message keeps the original body and dedup id.
func (c *Consumer) deadLetter(ctx context.Context, m types.Message, reason string) {
	c.metrics.DLQ(reason)
	group := m.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)]
	if group == "" {
		group = "unknown"
	}
	_, err := c.client.api.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(c.dlqURL),
		MessageBody:            m.Body,
		MessageGroupId:         aws.String(group),
		MessageDeduplicationId: aws.String(aws.ToString(m.MessageId)),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"dlqReason": {DataType: aws.String("String"), StringValue: aws.String(reason)},
		},
	})
	if err != nil {
		c.log.Error("dead-letter send failed; message will be redelivered", "error", err.Error(), "reason", reason)
		return
	}
	c.delete(ctx, aws.ToString(m.ReceiptHandle), c.log.With("reason", reason))
}
