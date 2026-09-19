package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/jamesmachome/backend-challenge-go/internal/app"
	"github.com/jamesmachome/backend-challenge-go/internal/domain/events"
)

type outboxRepo struct{ tx pgx.Tx }

func (r *outboxRepo) Add(ctx context.Context, ev events.Event) error {
	payload, err := ev.Marshal()
	if err != nil {
		return err
	}
	env := ev.Envelope()
	_, err = r.tx.Exec(ctx, `INSERT INTO outbox_events (event_id, aggregate_type, aggregate_id, event_type, payload, occurred_at, created_at, next_attempt_at)
		VALUES ($1,$2,$3,$4,$5,$6,now(),now())`, env.EventID, env.AggregateType, env.AggregateID, env.EventType, payload, env.OccurredAt)
	if err != nil {
		return fmt.Errorf("postgres: add outbox: %w", err)
	}
	return nil
}

// Claim leases due rows with SKIP LOCKED so concurrent publishers never pick
// the same event. Expired leases (crashed publishers) become claimable again.
func (r *outboxRepo) Claim(ctx context.Context, publisherID string, now time.Time, lease time.Duration, limit int) ([]app.OutboxRecord, error) {
	rows, err := r.tx.Query(ctx, `UPDATE outbox_events SET locked_by = $1, locked_until = $2, attempts = attempts + 1
		WHERE event_id IN (
			SELECT event_id FROM outbox_events
			WHERE published_at IS NULL AND next_attempt_at <= $3 AND (locked_until IS NULL OR locked_until < $3)
			ORDER BY next_attempt_at, created_at LIMIT $4 FOR UPDATE SKIP LOCKED)
		RETURNING event_id, aggregate_id, event_type, payload, attempts, occurred_at`, publisherID, now.Add(lease), now, limit)
	if err != nil {
		return nil, fmt.Errorf("postgres: claim outbox: %w", err)
	}
	defer rows.Close()
	var out []app.OutboxRecord
	for rows.Next() {
		var rec app.OutboxRecord
		if err := rows.Scan(&rec.EventID, &rec.AggregateID, &rec.EventType, &rec.Payload, &rec.Attempts, &rec.OccurredAt); err != nil {
			return nil, fmt.Errorf("postgres: scan outbox: %w", err)
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

func (r *outboxRepo) MarkPublished(ctx context.Context, eventID uuid.UUID, now time.Time) error {
	_, err := r.tx.Exec(ctx, `UPDATE outbox_events SET published_at = $2, locked_until = NULL, last_error = NULL WHERE event_id = $1 AND published_at IS NULL`, eventID, now)
	if err != nil {
		return fmt.Errorf("postgres: mark published: %w", err)
	}
	return nil
}

func (r *outboxRepo) Reschedule(ctx context.Context, eventID uuid.UUID, nextAttempt time.Time, lastError string) error {
	_, err := r.tx.Exec(ctx, `UPDATE outbox_events SET next_attempt_at = $2, locked_by = NULL, locked_until = NULL, last_error = left($3, 500) WHERE event_id = $1 AND published_at IS NULL`, eventID, nextAttempt, lastError)
	if err != nil {
		return fmt.Errorf("postgres: reschedule: %w", err)
	}
	return nil
}

func (r *outboxRepo) Lag(ctx context.Context, now time.Time) (time.Duration, int64, error) {
	var (
		oldest *time.Time
		count  int64
	)
	err := r.tx.QueryRow(ctx, `SELECT MIN(created_at), COUNT(*) FROM outbox_events WHERE published_at IS NULL`).Scan(&oldest, &count)
	if err != nil {
		return 0, 0, fmt.Errorf("postgres: outbox lag: %w", err)
	}
	if oldest == nil {
		return 0, 0, nil
	}
	return now.Sub(*oldest), count, nil
}

type inboxRepo struct{ tx pgx.Tx }

// Record inserts the inbox row for the message. Inserting and completing
// happen in the same transaction as the domain changes, so a row only exists
// for messages whose handling was committed.
func (r *inboxRepo) Record(ctx context.Context, consumer, messageID, payloadHash string, now time.Time) (app.InboxResult, error) {
	tag, err := r.tx.Exec(ctx, `INSERT INTO inbox_messages (consumer_name, message_id, payload_hash, received_at, completed_at)
		VALUES ($1,$2,$3,$4,$4) ON CONFLICT (consumer_name, message_id) DO NOTHING`, consumer, messageID, payloadHash, now)
	if err != nil {
		return app.InboxResult{}, fmt.Errorf("postgres: inbox insert: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return app.InboxResult{Inserted: true}, nil
	}
	var existing string
	if err := r.tx.QueryRow(ctx, `SELECT payload_hash FROM inbox_messages WHERE consumer_name = $1 AND message_id = $2`, consumer, messageID).Scan(&existing); err != nil {
		return app.InboxResult{}, fmt.Errorf("postgres: inbox lookup: %w", err)
	}
	return app.InboxResult{ExistingHash: existing}, nil
}
