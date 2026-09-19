// Package sqs contains the message consumer for provider operations and the
// publisher used by the outbox worker.
package sqs

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/smithy-go"
)

// Config for the SQS client.
type Config struct {
	Region          string
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
}

// Client wraps the AWS SQS client with the resolved queue URLs.
type Client struct {
	api  *sqs.Client
	urls map[string]string
}

// NewClient builds a client. Credentials come from the explicit config or the
// default AWS chain (env, profile, instance role); the queue policies of the
// broker decide what the identity may do.
func NewClient(ctx context.Context, cfg Config) (*Client, error) {
	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.Region),
		awsconfig.WithRetryer(func() aws.Retryer { return retry.AddWithMaxAttempts(retry.NewStandard(), 3) }),
	}
	if cfg.AccessKeyID != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, "")))
	}
	ac, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("sqs: aws config: %w", err)
	}
	api := sqs.NewFromConfig(ac, func(o *sqs.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
	})
	return &Client{api: api, urls: map[string]string{}}, nil
}

// API exposes the underlying client (tests and tooling).
func (c *Client) API() *sqs.Client { return c.api }

// QueueURL resolves and caches a queue URL by name.
func (c *Client) QueueURL(ctx context.Context, name string) (string, error) {
	if u, ok := c.urls[name]; ok {
		return u, nil
	}
	out, err := c.api.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(name)})
	if err != nil {
		return "", fmt.Errorf("sqs: resolve queue %q: %w", name, err)
	}
	c.urls[name] = *out.QueueUrl
	return *out.QueueUrl, nil
}

// Resolve eagerly resolves the given queues, failing startup when one is missing.
func (c *Client) Resolve(ctx context.Context, names ...string) error {
	for _, n := range names {
		if _, err := c.QueueURL(ctx, n); err != nil {
			return err
		}
	}
	return nil
}

// Ready checks the broker is reachable by listing the resolved queues.
func (c *Client) Ready(ctx context.Context, queue string) error {
	url, err := c.QueueURL(ctx, queue)
	if err != nil {
		return err
	}
	_, err = c.api.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: aws.String(url), AttributeNames: []sqsAttr{"QueueArn"}})
	return err
}

// IsTransient reports whether an SQS error is worth retrying.
func IsTransient(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := apiErr.ErrorCode()
		switch {
		case strings.Contains(code, "Throttl"), strings.Contains(code, "ServiceUnavailable"), strings.Contains(code, "InternalError"), strings.Contains(code, "RequestTimeout"):
			return true
		}
		return false
	}
	return true
}
