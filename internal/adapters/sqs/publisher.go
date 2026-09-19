package sqs

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/jamesmachome/backend-challenge-go/internal/app"
)

// Publisher sends outbox events to the events FIFO queue.
type Publisher struct {
	client   *Client
	queue    string
	queueURL string
}

// NewPublisher builds a publisher for the queue name.
func NewPublisher(client *Client, queue string) *Publisher {
	return &Publisher{client: client, queue: queue}
}

// Publish sends one event. MessageGroupId is the aggregate id (ordering per
// wallet/transaction) and MessageDeduplicationId is the eventId, so a
// republication after a crash is deduplicated by the broker and, beyond the
// 5-minute window, by consumers keyed on eventId.
func (p *Publisher) Publish(ctx context.Context, rec app.OutboxRecord) error {
	if p.queueURL == "" {
		u, err := p.client.QueueURL(ctx, p.queue)
		if err != nil {
			return err
		}
		p.queueURL = u
	}
	_, err := p.client.api.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(p.queueURL),
		MessageBody:            aws.String(string(rec.Payload)),
		MessageGroupId:         aws.String(rec.AggregateID.String()),
		MessageDeduplicationId: aws.String(rec.EventID.String()),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"eventType": {DataType: aws.String("String"), StringValue: aws.String(rec.EventType)},
			"eventId":   {DataType: aws.String("String"), StringValue: aws.String(rec.EventID.String())},
		},
	})
	if err != nil {
		return fmt.Errorf("sqs: publish %s: %w", rec.EventID, err)
	}
	return nil
}
