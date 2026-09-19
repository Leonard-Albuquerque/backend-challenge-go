//go:build integration

package integration

import (
	"context"
	"net/http"
	"testing"
	"time"
)

func TestRestartPreservesIdempotencyPendingAndConsistency(t *testing.T) {
	ctx := context.Background()
	q, err := CreateQueues(ctx, "restart")
	if err != nil {
		t.Fatal(err)
	}
	a, err := StartInstance(ctx, "restart-a", q, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Kill()

	w := openWallet(t, a, "200.00")
	b := bet(w, "tx-restart-"+w.ID[24:], "50.00")
	if r := postTx(t, a, Token(t, clientProviderA), b); r.Status != http.StatusOK {
		t.Fatalf("bet: %d %s", r.Status, r.Raw)
	}
	// Park a reversal whose reference has not arrived.
	rb := bet(w, "tx-restart-rb-"+w.ID[24:], "10.00")
	rb.Kind, rb.ReferenceExternalTransactionID = "ROLLBACK", "tx-restart-late-"+w.ID[24:]
	if r := postTx(t, a, Token(t, clientProviderA), rb); r.Status != http.StatusAccepted {
		t.Fatalf("park: %d %s", r.Status, r.Raw)
	}

	// Graceful stop with SIGTERM: exits 0 and drains components in order.
	a.Terminate()
	if code, err := a.WaitExit(30 * time.Second); err != nil || code != 0 {
		t.Fatalf("graceful exit: %d %v\n%s", code, err, a.Logs())
	}
	logs := a.Logs()
	for _, s := range []string{"consumer stopped", "outbox publisher stopped", "pending reference worker stopped", "http server stopped", "postgres pool closed"} {
		if !contains(logs, s) {
			t.Fatalf("missing shutdown log %q\n%s", s, logs)
		}
	}
	if idx := indexOf(logs, "postgres pool closed"); idx < indexOf(logs, "consumer stopped") || idx < indexOf(logs, "http server stopped") {
		t.Fatal("pool must close after the components that use it")
	}

	// A new process (fresh memory) sees the persisted idempotency and pending state.
	bInst, err := StartInstance(ctx, "restart-b", q, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	defer bInst.Terminate()
	r := postTx(t, bInst, Token(t, clientProviderA), b)
	if r.Status != http.StatusOK || r.Body["idempotentReplay"] != true || r.money("balance") != "150.00" {
		t.Fatalf("replay after restart: %d %s", r.Status, r.Raw)
	}
	if r := postTx(t, bInst, Token(t, clientProviderA), rb); r.Status != http.StatusAccepted || r.Body["idempotentReplay"] != true {
		t.Fatalf("pending replay after restart: %d %s", r.Status, r.Raw)
	}
	late := bet(w, rb.ReferenceExternalTransactionID, "10.00")
	late.RoundID = rb.RoundID
	if r := postTx(t, bInst, Token(t, clientProviderA), late); r.Status != http.StatusOK {
		t.Fatalf("late bet: %d %s", r.Status, r.Raw)
	}
	eventually(t, 30*time.Second, func() bool { s, _ := txStatus(t, "provider-a", rb.ExternalTransactionID); return s == "PROCESSED" }, "pending resumed by the new instance")
	if getWallet(t, bInst, w.ID).money("balance") != "150.00" || countLedger(t, w.ID, "") != 4 {
		t.Fatal("balance after resume")
	}
	if rec := reconcile(t, bInst, w.ID); rec.Body["consistent"] != true {
		t.Fatalf("reconciliation: %s", rec.Raw)
	}
	// Outbox events written by the dead instance were published by the new one.
	eventually(t, 30*time.Second, func() bool {
		var n int
		_ = pool.QueryRow(ctx, `SELECT COUNT(*) FROM outbox_events WHERE published_at IS NULL AND aggregate_id IN (SELECT id FROM wager_transactions WHERE wallet_id = $1 UNION SELECT $1::uuid)`, w.ID).Scan(&n)
		return n == 0
	}, "events published after restart")
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func TestSIGTERMWhileConsuming(t *testing.T) {
	ctx := context.Background()
	q, err := CreateQueues(ctx, "sigterm")
	if err != nil {
		t.Fatal(err)
	}
	inst, err := StartInstance(ctx, "sigterm", q, map[string]string{"SQS_CONSUMER_WORKERS": "1"}, true)
	if err != nil {
		t.Fatal(err)
	}
	defer inst.Kill()
	w := openWallet(t, inst, "100.00")
	for i := 0; i < 5; i++ {
		b := bet(w, "tx-sigterm-"+w.ID[24:]+"-"+string(rune('a'+i)), "1.00")
		sendRaw(t, q.Wager, sqsEnvelope("msg-"+b.ExternalTransactionID, b), w.ID, "dedup-"+b.ExternalTransactionID)
	}
	time.Sleep(300 * time.Millisecond)
	inst.Terminate()
	if code, err := inst.WaitExit(30 * time.Second); err != nil || code != 0 {
		t.Fatalf("exit: %d %v", code, err)
	}
	// Whatever was committed is consistent, and nothing was lost: a new
	// instance finishes the remaining messages.
	inst2, err := StartInstance(ctx, "sigterm-2", q, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	defer inst2.Terminate()
	eventually(t, 40*time.Second, func() bool { return countLedger(t, w.ID, "DEBIT") == 5 }, "all messages processed exactly once")
	if getWallet(t, inst2, w.ID).money("balance") != "95.00" {
		t.Fatal("balance")
	}
	eventually(t, 10*time.Second, func() bool { return approxMessages(t, q.Wager) == 0 }, "queue drained")
}
