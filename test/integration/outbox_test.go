//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/google/uuid"
)

// insertOutboxEvent writes a synthetic event directly into the outbox table,
// as a committed business transaction would.
func insertOutboxEvent(t testing.TB, eventID, aggregateID uuid.UUID) {
	t.Helper()
	payload, _ := json.Marshal(map[string]any{"eventId": eventID, "eventType": "WalletBalanceChanged", "aggregateId": aggregateID, "version": 1, "data": map[string]string{"test": "outbox"}})
	_, err := pool.Exec(context.Background(), `INSERT INTO outbox_events (event_id, aggregate_type, aggregate_id, event_type, payload, occurred_at, created_at, next_attempt_at)
		VALUES ($1, 'Wallet', $2, 'WalletBalanceChanged', $3, now(), now(), now())`, eventID, aggregateID, payload)
	if err != nil {
		t.Fatal(err)
	}
}

func outboxPublished(t testing.TB, eventID uuid.UUID) (bool, int) {
	t.Helper()
	var published *time.Time
	var attempts int
	if err := pool.QueryRow(context.Background(), `SELECT published_at, attempts FROM outbox_events WHERE event_id = $1`, eventID).Scan(&published, &attempts); err != nil {
		t.Fatal(err)
	}
	return published != nil, attempts
}

func TestOutboxEventsPublishedAfterCommitWithStableIDs(t *testing.T) {
	inst := cluster.Next()
	w := openWallet(t, inst, "50.00")
	b := bet(w, "tx-outbox-"+w.ID[24:], "20.00")
	if r := postTx(t, inst, Token(t, clientProviderA), b); r.Status != 200 {
		t.Fatalf("bet: %d %s", r.Status, r.Raw)
	}
	// Collect events for this wallet/transaction from the cluster events queue.
	types := map[string]int{}
	ids := map[string]bool{}
	eventually(t, 30*time.Second, func() bool {
		for _, m := range receiveAll(t, cluster.Queues.Events, 50, 2*time.Second) {
			var env struct {
				EventID     string `json:"eventId"`
				EventType   string `json:"eventType"`
				AggregateID string `json:"aggregateId"`
				Version     int    `json:"version"`
				Data        map[string]any
			}
			_ = json.Unmarshal([]byte(aws.ToString(m.Body)), &env)
			related := env.AggregateID == w.ID
			if d, ok := env.Data["walletId"].(string); ok && d == w.ID {
				related = true
			}
			if related && env.Version == 1 {
				types[env.EventType]++
				ids[env.EventID] = true
			}
		}
		return types["WalletBalanceChanged"] >= 2 && types["WagerTransactionProcessed"] >= 2
	}, "opening + bet events published")
	if len(ids) != 4 {
		t.Fatalf("expected 4 distinct event ids, got %d (%v)", len(ids), types)
	}
	var unpublished int
	_ = pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = $1 AND published_at IS NULL`, w.ID).Scan(&unpublished)
	if unpublished != 0 {
		t.Fatal("outbox rows must be marked published")
	}
}

func TestOutboxPublishersCompeteAndRecover(t *testing.T) {
	ctx := context.Background()
	q, err := CreateQueues(ctx, "outbox")
	if err != nil {
		t.Fatal(err)
	}
	// The cluster shares the outbox table; freeze its publishers so the
	// crash scenarios are deterministic.
	for _, i := range cluster.Instances {
		i.Pause()
	}
	defer func() {
		for _, i := range cluster.Instances {
			i.Resume()
		}
	}()
	// Crash after claim: the lease expires and another publisher takes over.
	e1 := uuid.New()
	agg := uuid.New()
	crashClaim, err := StartInstance(ctx, "outbox-crash-claim", q, map[string]string{"CRASH_POINT": "outbox.after_claim", "SQS_CONSUMER_ENABLED": "false", "PENDING_WORKER_ENABLED": "false", "OUTBOX_LEASE": "2s"}, true)
	if err != nil {
		t.Fatal(err)
	}
	defer crashClaim.Kill()
	insertOutboxEvent(t, e1, agg)
	if code, err := crashClaim.WaitExit(30 * time.Second); err != nil || code != 137 {
		t.Fatalf("expected crash after claim: %d %v", code, err)
	}
	if pub, attempts := outboxPublished(t, e1); pub || attempts != 1 {
		t.Fatalf("event must be claimed but unpublished: published=%v attempts=%d", pub, attempts)
	}

	// Crash after publish: the message is in the queue but the row is not
	// acknowledged; the takeover republishes with the same eventId and the
	// FIFO dedup (plus consumer dedup on eventId) yields a single delivery.
	e2 := uuid.New()
	crashPublish, err := StartInstance(ctx, "outbox-crash-publish", q, map[string]string{"CRASH_POINT": "outbox.after_publish", "SQS_CONSUMER_ENABLED": "false", "PENDING_WORKER_ENABLED": "false", "OUTBOX_LEASE": "2s"}, true)
	if err != nil {
		t.Fatal(err)
	}
	defer crashPublish.Kill()
	insertOutboxEvent(t, e2, agg)
	if code, err := crashPublish.WaitExit(30 * time.Second); err != nil || code != 137 {
		t.Fatalf("expected crash after publish: %d %v", code, err)
	}
	if pub, _ := outboxPublished(t, e2); pub {
		t.Fatal("event must not be acknowledged")
	}

	// Two competing publishers recover both events and share the rest.
	p1, err := StartInstance(ctx, "outbox-pub-1", q, map[string]string{"SQS_CONSUMER_ENABLED": "false", "PENDING_WORKER_ENABLED": "false", "OUTBOX_BATCH_SIZE": "5"}, true)
	if err != nil {
		t.Fatal(err)
	}
	defer p1.Terminate()
	p2, err := StartInstance(ctx, "outbox-pub-2", q, map[string]string{"SQS_CONSUMER_ENABLED": "false", "PENDING_WORKER_ENABLED": "false", "OUTBOX_BATCH_SIZE": "5"}, true)
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Terminate()
	// Insert in waves: with batch publishing a single publisher drains a
	// burst before the other one polls, so spread the work over time.
	var extra []uuid.UUID
	for i := 0; i < 40; i++ {
		id := uuid.New()
		extra = append(extra, id)
		insertOutboxEvent(t, id, uuid.New())
		if i%4 == 3 {
			time.Sleep(150 * time.Millisecond)
		}
	}
	eventually(t, 40*time.Second, func() bool {
		var n int
		_ = pool.QueryRow(ctx, `SELECT COUNT(*) FROM outbox_events WHERE published_at IS NULL AND (event_id = ANY($1) OR event_id IN ($2, $3))`, extra, e1, e2).Scan(&n)
		return n == 0
	}, "all events published")

	// Every event is delivered exactly once; e1/e2 keep their ids.
	seen := map[string]int{}
	for _, m := range receiveAll(t, q.Events, 42, 15*time.Second) {
		seen[aws.ToString(m.MessageAttributes["eventId"].StringValue)]++
	}
	if seen[e1.String()] != 1 || seen[e2.String()] != 1 {
		t.Fatalf("recovered events delivered %d/%d times", seen[e1.String()], seen[e2.String()])
	}
	for _, id := range extra {
		if seen[id.String()] != 1 {
			t.Fatalf("event %s delivered %d times", id, seen[id.String()])
		}
	}
	// Both publishers did work (locked_by keeps the publisher that acknowledged the row).
	var byP1, byP2 int
	_ = pool.QueryRow(ctx, `SELECT COUNT(*) FILTER (WHERE locked_by = 'outbox-pub-1'), COUNT(*) FILTER (WHERE locked_by = 'outbox-pub-2') FROM outbox_events WHERE event_id = ANY($1)`, extra).Scan(&byP1, &byP2)
	if byP1 == 0 || byP2 == 0 || byP1+byP2 != len(extra) {
		t.Fatalf("publishers did not share the work: p1=%d p2=%d", byP1, byP2)
	}
	var attemptsE2 int
	_ = pool.QueryRow(ctx, `SELECT attempts FROM outbox_events WHERE event_id = $1`, e2).Scan(&attemptsE2)
	if attemptsE2 < 2 {
		t.Fatalf("e2 should have been republished, attempts=%d", attemptsE2)
	}
}
