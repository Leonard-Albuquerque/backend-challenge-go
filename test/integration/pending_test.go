//go:build integration

package integration

import (
	"net/http"
	"testing"
	"time"
)

func TestReversalBeforeReferenceResolvesLater(t *testing.T) {
	inst := cluster.Next()
	w := openWallet(t, inst, "100.00")
	betB := bet(w, "tx-late-bet-"+w.ID[24:], "30.00")
	refund := bet(w, "tx-early-refund-"+w.ID[24:], "30.00")
	refund.Kind, refund.ReferenceExternalTransactionID, refund.RoundID = "REFUND", betB.ExternalTransactionID, betB.RoundID

	// Refund arrives first through SQS: parked as PENDING_REFERENCE.
	sendRaw(t, cluster.Queues.Wager, sqsEnvelope("msg-"+refund.ExternalTransactionID, refund), w.ID, "dedup-"+refund.ExternalTransactionID)
	eventually(t, 20*time.Second, func() bool {
		s, _ := txStatus(t, "provider-a", refund.ExternalTransactionID)
		return s == "PENDING_REFERENCE"
	}, "refund parked")
	r := do(t, cluster.Next(), http.MethodGet, "/providers/provider-a/wagering/transactions/"+refund.ExternalTransactionID, Token(t, clientProviderA), nil, nil)
	if r.Status != http.StatusOK || r.str("status") != "PENDING_REFERENCE" || r.Body["referenceAttempts"].(float64) < 1 {
		t.Fatalf("pending query: %d %s", r.Status, r.Raw)
	}
	// HTTP replay of the parked operation returns 202 with the pending state.
	if r := postTx(t, cluster.Next(), Token(t, clientProviderA), refund); r.Status != http.StatusAccepted || r.Body["idempotentReplay"] != true {
		t.Fatalf("pending replay: %d %s", r.Status, r.Raw)
	}
	// The bet arrives on another instance; the worker (on any instance) completes the refund.
	if r := postTx(t, cluster.Next(), Token(t, clientProviderA), betB); r.Status != http.StatusOK {
		t.Fatalf("bet: %d %s", r.Status, r.Raw)
	}
	eventually(t, 30*time.Second, func() bool { s, _ := txStatus(t, "provider-a", refund.ExternalTransactionID); return s == "PROCESSED" }, "refund resolved by worker")
	if getWallet(t, inst, w.ID).money("balance") != "100.00" || countLedger(t, w.ID, "") != 3 {
		t.Fatal("refund must restore the balance with one credit entry")
	}
	r = do(t, inst, http.MethodGet, "/providers/provider-a/wagering/transactions/"+refund.ExternalTransactionID, Token(t, clientProviderA), nil, nil)
	if r.str("referenceTransactionId") == "" {
		t.Fatal("resolved reference must be recorded")
	}
	if rec := reconcile(t, inst, w.ID); rec.Body["consistent"] != true {
		t.Fatalf("reconciliation: %s", rec.Raw)
	}
}

func TestReversalExpiresAsRejected(t *testing.T) {
	inst := cluster.Next()
	w := openWallet(t, inst, "100.00")
	rb := bet(w, "tx-expire-rb-"+w.ID[24:], "5.00")
	rb.Kind, rb.ReferenceExternalTransactionID = "ROLLBACK", "never-arrives-"+w.ID[24:]
	r := postTx(t, inst, Token(t, clientProviderA), rb)
	if r.Status != http.StatusAccepted || r.str("status") != "PENDING_REFERENCE" {
		t.Fatalf("park: %d %s", r.Status, r.Raw)
	}
	// Cluster config: 5 attempts with 200ms..1s backoff => rejected in a few seconds.
	eventually(t, 40*time.Second, func() bool {
		s, c := txStatus(t, "provider-a", rb.ExternalTransactionID)
		return s == "REJECTED" && c == "REFERENCE_NOT_FOUND"
	}, "expired as REFERENCE_NOT_FOUND")
	r = do(t, inst, http.MethodGet, "/providers/provider-a/wagering/transactions/"+rb.ExternalTransactionID, Token(t, clientProviderA), nil, nil)
	if r.Body["referenceAttempts"].(float64) != 5 {
		t.Fatalf("attempts: %v", r.Body["referenceAttempts"])
	}
	if getWallet(t, inst, w.ID).money("balance") != "100.00" {
		t.Fatal("expired reversal must not move money")
	}
	// Reversal that would overdraw: distinct failure code.
	win := bet(w, "tx-win-"+w.ID[24:], "50.00")
	win.Kind = "WIN"
	if r := postTx(t, inst, Token(t, clientProviderA), win); r.Status != http.StatusOK {
		t.Fatalf("win: %d %s", r.Status, r.Raw)
	}
	spend := bet(w, "tx-spend-"+w.ID[24:], "120.00")
	if r := postTx(t, inst, Token(t, clientProviderA), spend); r.Status != http.StatusOK {
		t.Fatalf("spend: %d %s", r.Status, r.Raw)
	}
	rbWin := bet(w, "tx-rb-win-"+w.ID[24:], "50.00")
	rbWin.Kind, rbWin.ReferenceExternalTransactionID, rbWin.RoundID = "ROLLBACK", win.ExternalTransactionID, win.RoundID
	if r := postTx(t, inst, Token(t, clientProviderA), rbWin); r.Status != http.StatusUnprocessableEntity || r.str("failureCode") != "REVERSAL_INSUFFICIENT_FUNDS" {
		t.Fatalf("overdraw reversal: %d %s", r.Status, r.Raw)
	}
}
