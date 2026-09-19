//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

func TestSQSConsumerInboxAndCrossSourceIdempotency(t *testing.T) {
	inst := cluster.Next()
	w := openWallet(t, inst, "500.00")
	q := cluster.Queues

	// 1. Operation via SQS is processed by one of the consumers.
	b := bet(w, "tx-sqs-"+w.ID[24:], "40.00")
	msgID := "msg-" + b.ExternalTransactionID
	sendRaw(t, q.Wager, sqsEnvelope(msgID, b), w.ID, msgID+"-1")
	eventually(t, 20*time.Second, func() bool { s, _ := txStatus(t, "provider-a", b.ExternalTransactionID); return s == "PROCESSED" }, "sqs bet processed")
	if getWallet(t, inst, w.ID).money("balance") != "460.00" {
		t.Fatal("balance after sqs bet")
	}
	var completed int
	_ = pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM inbox_messages WHERE message_id = $1 AND completed_at IS NOT NULL`, msgID).Scan(&completed)
	if completed != 1 {
		t.Fatal("inbox row must be committed with the domain change")
	}

	// 2. Redelivery of the same messageId (different SQS dedup id) is absorbed by the inbox.
	sendRaw(t, q.Wager, sqsEnvelope(msgID, b), w.ID, msgID+"-2")
	// 3. Same operation, new messageId: application-level idempotent replay.
	sendRaw(t, q.Wager, sqsEnvelope(msgID+"-other", b), w.ID, msgID+"-3")
	// 4. Same operation through HTTP: replay with the original balance.
	eventually(t, 20*time.Second, func() bool {
		var n int
		_ = pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM inbox_messages WHERE message_id = $1`, msgID+"-other").Scan(&n)
		return n == 1
	}, "second message consumed")
	r := postTx(t, cluster.Next(), Token(t, clientProviderA), b)
	if r.Status != http.StatusOK || r.Body["idempotentReplay"] != true || r.money("balance") != "460.00" {
		t.Fatalf("http replay of sqs operation: %d %s", r.Status, r.Raw)
	}
	eventually(t, 10*time.Second, func() bool { return approxMessages(t, q.Wager) == 0 }, "queue drained")
	if countLedger(t, w.ID, "DEBIT") != 1 || getWallet(t, inst, w.ID).money("balance") != "460.00" {
		t.Fatal("duplicates moved money")
	}

	// 5. HTTP first, then SQS for the same operation: no second debit.
	b2 := bet(w, "tx-http-then-sqs-"+w.ID[24:], "10.00")
	if r := postTx(t, inst, Token(t, clientProviderA), b2); r.Status != http.StatusOK {
		t.Fatalf("http bet: %d %s", r.Status, r.Raw)
	}
	sendRaw(t, q.Wager, sqsEnvelope("msg-"+b2.ExternalTransactionID, b2), w.ID, "dedup-"+b2.ExternalTransactionID)
	eventually(t, 20*time.Second, func() bool {
		var n int
		_ = pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM inbox_messages WHERE message_id = $1`, "msg-"+b2.ExternalTransactionID).Scan(&n)
		return n == 1
	}, "sqs duplicate consumed")
	if countLedger(t, w.ID, "DEBIT") != 2 || getWallet(t, inst, w.ID).money("balance") != "450.00" {
		t.Fatal("cross-source duplicate moved money")
	}

	// 6. Concurrent HTTP and SQS for the same new operation.
	b3 := bet(w, "tx-race-"+w.ID[24:], "10.00")
	done := make(chan response, 1)
	go func() { done <- postTx(t, cluster.Next(), Token(t, clientProviderA), b3) }()
	sendRaw(t, q.Wager, sqsEnvelope("msg-"+b3.ExternalTransactionID, b3), w.ID, "dedup-"+b3.ExternalTransactionID)
	if r := <-done; r.Status != http.StatusOK {
		t.Fatalf("http race: %d %s", r.Status, r.Raw)
	}
	eventually(t, 20*time.Second, func() bool {
		var n int
		_ = pool.QueryRow(context.Background(), `SELECT COUNT(*) FROM inbox_messages WHERE message_id = $1`, "msg-"+b3.ExternalTransactionID).Scan(&n)
		return n == 1
	}, "race message consumed")
	if countLedger(t, w.ID, "DEBIT") != 3 || getWallet(t, inst, w.ID).money("balance") != "440.00" {
		t.Fatal("http/sqs race produced a duplicate movement")
	}

	// 7. Business rejection via SQS is terminal: REJECTED committed, message removed, no DLQ.
	big := bet(w, "tx-sqs-big-"+w.ID[24:], "9999.00")
	sendRaw(t, q.Wager, sqsEnvelope("msg-"+big.ExternalTransactionID, big), w.ID, "dedup-"+big.ExternalTransactionID)
	eventually(t, 20*time.Second, func() bool {
		s, c := txStatus(t, "provider-a", big.ExternalTransactionID)
		return s == "REJECTED" && c == "INSUFFICIENT_FUNDS"
	}, "sqs rejection")

	// 8. Invalid messages go straight to the DLQ.
	sendRaw(t, q.Wager, `{"messageId":"msg-bad-`+w.ID[24:]+`","type":"WagerTransactionRequested","data":{"providerId":"provider-a","externalTransactionId":"bad","idempotencyKey":"k","playerId":"`+w.PlayerID+`","walletId":"`+w.ID+`","roundId":"r","gameId":"g","kind":"BET","money":{"amount":"1e5","currency":"BRL"}}}`, w.ID, "dedup-bad-"+w.ID[24:])
	sendRaw(t, q.Wager, `not json`, w.ID, "dedup-notjson-"+w.ID[24:])
	msgs := receiveAll(t, q.WagerDLQ, 2, 20*time.Second)
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages in DLQ, got %d", len(msgs))
	}
	reasons := map[string]int{}
	for _, m := range msgs {
		reasons[aws.ToString(m.MessageAttributes["dlqReason"].StringValue)]++
	}
	if reasons["invalid_payload"] != 1 || reasons["invalid_envelope"] != 1 {
		t.Fatalf("dlq reasons: %v", reasons)
	}
	eventually(t, 10*time.Second, func() bool { return approxMessages(t, q.Wager) == 0 }, "queue drained after DLQ")
	if rec := reconcile(t, inst, w.ID); rec.Body["consistent"] != true {
		t.Fatalf("reconciliation: %s", rec.Raw)
	}
}

func TestConsumerCrashAfterCommitBeforeDelete(t *testing.T) {
	ctx := context.Background()
	q, err := CreateQueues(ctx, "crash-consumer")
	if err != nil {
		t.Fatal(err)
	}
	w := openWallet(t, cluster.Next(), "300.00")
	b := bet(w, "tx-crash-"+w.ID[24:], "30.00")
	msgID := "msg-" + b.ExternalTransactionID

	// A consumer that exits right after committing, before DeleteMessage.
	crasher, err := StartInstance(ctx, "crash-consumer", q, map[string]string{"CRASH_POINT": "consumer.after_commit", "OUTBOX_ENABLED": "false", "PENDING_WORKER_ENABLED": "false"}, true)
	if err != nil {
		t.Fatal(err)
	}
	defer crasher.Kill()
	sendRaw(t, q.Wager, sqsEnvelope(msgID, b), w.ID, msgID)
	if code, err := crasher.WaitExit(30 * time.Second); err != nil || code != 137 {
		t.Fatalf("expected crash exit 137, got %d %v\n%s", code, err, crasher.Logs())
	}
	if s, _ := txStatus(t, "provider-a", b.ExternalTransactionID); s != "PROCESSED" {
		t.Fatalf("commit must have happened before the crash, status=%q", s)
	}
	if n := approxMessages(t, q.Wager); n != 1 {
		t.Fatalf("message must still be in the queue (in flight), got %d", n)
	}

	// A healthy instance receives the redelivery and the inbox absorbs it.
	healthy, err := StartInstance(ctx, "crash-consumer-recovery", q, map[string]string{"OUTBOX_ENABLED": "false", "PENDING_WORKER_ENABLED": "false"}, true)
	if err != nil {
		t.Fatal(err)
	}
	defer healthy.Terminate()
	eventually(t, 30*time.Second, func() bool { return approxMessages(t, q.Wager) == 0 }, "redelivered message consumed and deleted")
	eventually(t, 10*time.Second, func() bool { return contains(healthy.Logs(), "duplicate delivery ignored by inbox") }, "inbox duplicate logged")
	if countLedger(t, w.ID, "DEBIT") != 1 || getWallet(t, cluster.Next(), w.ID).money("balance") != "270.00" {
		t.Fatal("redelivery must not move money twice")
	}
	if n := len(receiveAll(t, q.WagerDLQ, 1, 2*time.Second)); n != 0 {
		t.Fatal("nothing should reach the DLQ")
	}
}

func TestTransientFailuresReachDLQAfterRetries(t *testing.T) {
	// A message whose handling always fails transiently is retried with
	// visibility backoff and, after maxReceiveCount, redriven by SQS to the DLQ.
	// We simulate the transient failure with an unreachable database.
	ctx := context.Background()
	q, err := CreateQueues(ctx, "dlq")
	if err != nil {
		t.Fatal(err)
	}
	w := openWallet(t, cluster.Next(), "10.00")
	b := bet(w, "tx-dlq-"+w.ID[24:], "1.00")
	// Instance starts fine, then we point it at a database that rejects
	// queries by revoking its connections: simplest portable approach is a
	// statement timeout of 1ms on the role used by this instance.
	inst, err := StartInstance(ctx, "dlq-consumer", q, map[string]string{
		"DATABASE_URL": dbURL + "&options=-c%20statement_timeout%3D1", "DB_AUTO_MIGRATE": "false", "OUTBOX_ENABLED": "false", "PENDING_WORKER_ENABLED": "false", "SQS_CONSUMER_WORKERS": "1",
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	defer inst.Kill()
	// Readiness may flap because of the timeout; wait for the consumer log instead.
	eventually(t, 60*time.Second, func() bool { return contains(inst.Logs(), "consumer started") }, "consumer started")
	msgID := "msg-" + b.ExternalTransactionID
	sendRaw(t, q.Wager, sqsEnvelope(msgID, b), w.ID, msgID)
	msgs := receiveAll(t, q.WagerDLQ, 1, 90*time.Second)
	if len(msgs) != 1 {
		t.Fatalf("expected the message in the DLQ after retries\n%s", inst.Logs())
	}
	var env struct {
		MessageID string `json:"messageId"`
	}
	_ = json.Unmarshal([]byte(aws.ToString(msgs[0].Body)), &env)
	if env.MessageID != msgID {
		t.Fatalf("dlq message id %q", env.MessageID)
	}
	if !contains(inst.Logs(), "transient failure; message will be redelivered") {
		t.Fatal("expected transient retry logs")
	}
	if s, _ := txStatus(t, "provider-a", b.ExternalTransactionID); s != "" {
		t.Fatal("no transaction should have been committed")
	}
}
