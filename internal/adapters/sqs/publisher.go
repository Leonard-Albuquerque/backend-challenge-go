package sqs

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"

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

// PublishBatch sends up to 10 events in one SendMessageBatch call and returns
// the ids that were accepted; per-entry failures come back in errs.
func (p *Publisher) PublishBatch(ctx context.Context, recs []app.OutboxRecord) (published []uuid.UUID, errs map[uuid.UUID]error, err error) {
	if len(recs) == 0 {
		return nil, nil, nil
	}
	if len(recs) > 10 {
		return nil, nil, errors.New("sqs: batch larger than 10")
	}
	if p.queueURL == "" {
		u, err := p.client.QueueURL(ctx, p.queue)
		if err != nil {
			return nil, nil, err
		}
		p.queueURL = u
	}
	entries := make([]types.SendMessageBatchRequestEntry, 0, len(recs))
	byID := make(map[string]uuid.UUID, len(recs))
	for i, rec := range recs {
		id := fmt.Sprintf("e%d", i)
		byID[id] = rec.EventID
		entries = append(entries, types.SendMessageBatchRequestEntry{
			Id:                     aws.String(id),
			MessageBody:            aws.String(string(rec.Payload)),
			MessageGroupId:         aws.String(rec.AggregateID.String()),
			MessageDeduplicationId: aws.String(rec.EventID.String()),
			MessageAttributes: map[string]types.MessageAttributeValue{
				"eventType": {DataType: aws.String("String"), StringValue: aws.String(rec.EventType)},
				"eventId":   {DataType: aws.String("String"), StringValue: aws.String(rec.EventID.String())},
			},
		})
	}
	out, err := p.client.api.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{QueueUrl: aws.String(p.queueURL), Entries: entries})
	if err != nil {
		return nil, nil, fmt.Errorf("sqs: publish batch: %w", err)
	}
	errs = map[uuid.UUID]error{}
	for _, ok := range out.Successful {
		published = append(published, byID[aws.ToString(ok.Id)])
	}
	for _, f := range out.Failed {
		errs[byID[aws.ToString(f.Id)]] = fmt.Errorf("sqs: %s: %s", aws.ToString(f.Code), aws.ToString(f.Message))
	}
	return published, errs, nil
}
