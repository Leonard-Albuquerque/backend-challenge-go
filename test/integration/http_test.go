//go:build integration

package integration

import (
	"net/http"
	"testing"
	"time"
)

func TestOpeningAndBasicFlow(t *testing.T) {
	inst := cluster.Next()
	w := openWallet(t, inst, "1000.00")

	// Opening created OPENING/ledger; zero opening creates nothing.
	if n := countLedger(t, w.ID, "CREDIT"); n != 1 {
		t.Fatalf("opening ledger entries: %d", n)
	}
	var openings int
	_ = pool.QueryRow(t.Context(), `SELECT COUNT(*) FROM wager_transactions WHERE wallet_id = $1 AND kind = 'OPENING' AND origin = 'INTERNAL' AND status = 'PROCESSED'`, w.ID).Scan(&openings)
	if openings != 1 {
		t.Fatalf("openings: %d", openings)
	}
	zero := openWallet(t, cluster.Next(), "0.00")
	if countLedger(t, zero.ID, "") != 0 {
		t.Fatal("zero opening must not create ledger")
	}
	// Same player/currency conflicts.
	r := do(t, inst, http.MethodPost, "/wallets", Token(t, clientInternal), map[string]any{"playerId": w.PlayerID, "initialBalance": map[string]string{"amount": "1.00", "currency": "BRL"}}, nil)
	if r.Status != http.StatusConflict || r.errCode() != "WALLET_EXISTS" {
		t.Fatalf("duplicate wallet: %d %s", r.Status, r.Raw)
	}

	// BET on instance 2, replay on instance 3 with another movement in between.
	b := bet(w, "tx-"+w.ID[24:]+"-1", "25.00")
	r = postTx(t, cluster.Next(), Token(t, clientProviderA), b)
	if r.Status != http.StatusOK || r.str("status") != "PROCESSED" || r.money("balance") != "975.00" || r.Body["idempotentReplay"] != false {
		t.Fatalf("bet: %d %s", r.Status, r.Raw)
	}
	txID := r.str("transactionId")
	b2 := bet(w, "tx-"+w.ID[24:]+"-2", "10.00")
	if r := postTx(t, cluster.Next(), Token(t, clientProviderA), b2); r.Status != http.StatusOK {
		t.Fatalf("bet2: %d %s", r.Status, r.Raw)
	}
	r = postTx(t, cluster.Next(), Token(t, clientProviderA), b)
	if r.Status != http.StatusOK || r.Body["idempotentReplay"] != true || r.money("balance") != "975.00" || r.str("transactionId") != txID {
		t.Fatalf("replay must return the original balance: %d %s", r.Status, r.Raw)
	}

	// Same key, different payload -> 409; same external id, other key -> 409.
	changed := b
	changed.Money = map[string]string{"amount": "26.00", "currency": "BRL"}
	if r := postTx(t, inst, Token(t, clientProviderA), changed); r.Status != http.StatusConflict || r.errCode() != "IDEMPOTENCY_KEY_CONFLICT" {
		t.Fatalf("payload conflict: %d %s", r.Status, r.Raw)
	}
	if r := do(t, inst, http.MethodPost, "/wagering/transactions", Token(t, clientProviderA), b, map[string]string{"Idempotency-Key": "another-key"}); r.Status != http.StatusConflict || r.errCode() != "EXTERNAL_TRANSACTION_ID_CONFLICT" {
		t.Fatalf("external id conflict: %d %s", r.Status, r.Raw)
	}
	// Missing key -> 400.
	if r := do(t, inst, http.MethodPost, "/wagering/transactions", Token(t, clientProviderA), b, nil); r.Status != http.StatusBadRequest || r.errCode() != "MISSING_IDEMPOTENCY_KEY" {
		t.Fatalf("missing key: %d %s", r.Status, r.Raw)
	}

	// Rejection: insufficient funds -> 422 with failureCode, no movement.
	big := bet(w, "tx-"+w.ID[24:]+"-big", "5000.00")
	r = postTx(t, inst, Token(t, clientProviderA), big)
	if r.Status != http.StatusUnprocessableEntity || r.str("status") != "REJECTED" || r.str("failureCode") != "INSUFFICIENT_FUNDS" {
		t.Fatalf("rejection: %d %s", r.Status, r.Raw)
	}
	// LOSS with zero, no ledger, no version bump.
	loss := bet(w, "tx-"+w.ID[24:]+"-loss", "0.00")
	loss.Kind = "LOSS"
	before := getWallet(t, inst, w.ID).Body["version"]
	if r := postTx(t, inst, Token(t, clientProviderA), loss); r.Status != http.StatusOK || r.str("status") != "PROCESSED" {
		t.Fatalf("loss: %d %s", r.Status, r.Raw)
	}
	if after := getWallet(t, inst, w.ID).Body["version"]; after != before {
		t.Fatalf("loss changed version %v -> %v", before, after)
	}
	// WIN, REFUND, ROLLBACK.
	win := bet(w, "tx-"+w.ID[24:]+"-win", "100.00")
	win.Kind = "WIN"
	if r := postTx(t, inst, Token(t, clientProviderA), win); r.Status != http.StatusOK || r.money("balance") != "1065.00" {
		t.Fatalf("win: %d %s", r.Status, r.Raw)
	}
	refund := bet(w, "tx-"+w.ID[24:]+"-refund", "10.00")
	refund.Kind, refund.ReferenceExternalTransactionID, refund.RoundID = "REFUND", b2.ExternalTransactionID, b2.RoundID
	if r := postTx(t, inst, Token(t, clientProviderA), refund); r.Status != http.StatusOK || r.money("balance") != "1075.00" {
		t.Fatalf("refund: %d %s", r.Status, r.Raw)
	}
	again := refund
	again.ExternalTransactionID = refund.ExternalTransactionID + "-again"
	if r := postTx(t, inst, Token(t, clientProviderA), again); r.Status != http.StatusUnprocessableEntity || r.str("failureCode") != "REFERENCE_ALREADY_REVERSED" {
		t.Fatalf("double refund: %d %s", r.Status, r.Raw)
	}
	rb := bet(w, "tx-"+w.ID[24:]+"-rb", "100.00")
	rb.Kind, rb.ReferenceExternalTransactionID, rb.RoundID = "ROLLBACK", win.ExternalTransactionID, win.RoundID
	if r := postTx(t, inst, Token(t, clientProviderA), rb); r.Status != http.StatusOK || r.money("balance") != "975.00" {
		t.Fatalf("rollback: %d %s", r.Status, r.Raw)
	}

	// Validation errors.
	for name, body := range map[string]string{
		"float":      `{"providerId":"provider-a","externalTransactionId":"x","playerId":"` + w.PlayerID + `","walletId":"` + w.ID + `","roundId":"r","gameId":"g","kind":"BET","money":{"amount":25.5,"currency":"BRL"}}`,
		"scientific": `{"providerId":"provider-a","externalTransactionId":"x","playerId":"` + w.PlayerID + `","walletId":"` + w.ID + `","roundId":"r","gameId":"g","kind":"BET","money":{"amount":"1e2","currency":"BRL"}}`,
		"negative":   `{"providerId":"provider-a","externalTransactionId":"x","playerId":"` + w.PlayerID + `","walletId":"` + w.ID + `","roundId":"r","gameId":"g","kind":"BET","money":{"amount":"-1.00","currency":"BRL"}}`,
		"scale":      `{"providerId":"provider-a","externalTransactionId":"x","playerId":"` + w.PlayerID + `","walletId":"` + w.ID + `","roundId":"r","gameId":"g","kind":"BET","money":{"amount":"1.001","currency":"BRL"}}`,
		"opening":    `{"providerId":"provider-a","externalTransactionId":"x","playerId":"` + w.PlayerID + `","walletId":"` + w.ID + `","roundId":"r","gameId":"g","kind":"OPENING","money":{"amount":"1.00","currency":"BRL"}}`,
		"unknown":    `{"providerId":"provider-a","externalTransactionId":"x","playerId":"` + w.PlayerID + `","walletId":"` + w.ID + `","roundId":"r","gameId":"g","kind":"BET","money":{"amount":"1.00","currency":"BRL"},"extra":1}`,
	} {
		r := do(t, inst, http.MethodPost, "/wagering/transactions", Token(t, clientProviderA), body, map[string]string{"Idempotency-Key": "provider-a:x"})
		if r.Status != http.StatusBadRequest || r.errCode() != "VALIDATION_ERROR" {
			t.Errorf("%s: expected 400 VALIDATION_ERROR, got %d %s", name, r.Status, r.Raw)
		}
	}
	// Unknown wallet -> 422 WALLET_NOT_FOUND.
	ghost := bet(testWallet{ID: "0192f291-27dd-7d3f-8071-5f8685deef37", PlayerID: w.PlayerID}, "tx-ghost-"+w.ID[24:], "1.00")
	if r := postTx(t, inst, Token(t, clientProviderA), ghost); r.Status != http.StatusUnprocessableEntity || r.errCode() != "WALLET_NOT_FOUND" {
		t.Fatalf("ghost wallet: %d %s", r.Status, r.Raw)
	}

	// Queries and pagination.
	r = do(t, inst, http.MethodGet, "/wagering/transactions/"+txID, Token(t, clientProviderA), nil, nil)
	if r.Status != http.StatusOK || r.str("externalTransactionId") != b.ExternalTransactionID {
		t.Fatalf("get tx: %d %s", r.Status, r.Raw)
	}
	r = do(t, inst, http.MethodGet, "/providers/provider-a/wagering/transactions/"+big.ExternalTransactionID, Token(t, clientProviderA), nil, nil)
	if r.Status != http.StatusOK || r.str("failureCode") != "INSUFFICIENT_FUNDS" {
		t.Fatalf("get rejected tx: %d %s", r.Status, r.Raw)
	}
	seen := map[string]bool{}
	cursor := ""
	pages := 0
	for {
		path := "/wallets/" + w.ID + "/ledger?limit=2"
		if cursor != "" {
			path += "&cursor=" + cursor
		}
		r := do(t, inst, http.MethodGet, path, Token(t, clientInternal), nil, nil)
		if r.Status != http.StatusOK {
			t.Fatalf("ledger: %d %s", r.Status, r.Raw)
		}
		pages++
		for _, e := range r.Body["entries"].([]any) {
			id := e.(map[string]any)["id"].(string)
			if seen[id] {
				t.Fatal("duplicate entry across pages")
			}
			seen[id] = true
		}
		cursor = r.str("nextCursor")
		if cursor == "" {
			break
		}
	}
	if len(seen) != 6 || pages != 3 {
		t.Fatalf("ledger pagination: %d entries in %d pages", len(seen), pages)
	}
	if r := do(t, inst, http.MethodGet, "/wallets/"+w.ID+"/ledger?cursor=%21%21", Token(t, clientInternal), nil, nil); r.Status != http.StatusBadRequest {
		t.Fatalf("bad cursor: %d", r.Status)
	}

	rec := reconcile(t, cluster.Next(), w.ID)
	if rec.Body["consistent"] != true || rec.money("storedBalance") != "975.00" || rec.money("difference") != "0.00" || rec.Body["checkedEntries"].(float64) != 6 {
		t.Fatalf("reconciliation: %s", rec.Raw)
	}
	// Correlation id is echoed.
	if r := do(t, inst, http.MethodGet, "/health/live", "", nil, map[string]string{"X-Correlation-Id": "corr-abc"}); r.Header.Get("X-Correlation-Id") != "corr-abc" {
		t.Fatal("correlation id not echoed")
	}
}

func TestAuthentication(t *testing.T) {
	inst := cluster.Next()
	w := openWallet(t, inst, "100.00")
	body := bet(w, "tx-auth-"+w.ID[24:], "10.00")
	key := map[string]string{"Idempotency-Key": "provider-a:" + body.ExternalTransactionID}

	// Missing, malformed, wrong signature and expired tokens are rejected.
	if r := do(t, inst, http.MethodPost, "/wagering/transactions", "", body, key); r.Status != http.StatusUnauthorized || r.errCode() != "MISSING_TOKEN" {
		t.Fatalf("missing token: %d %s", r.Status, r.Raw)
	}
	if r := do(t, inst, http.MethodPost, "/wagering/transactions", "not-a-jwt", body, key); r.Status != http.StatusUnauthorized || r.errCode() != "INVALID_TOKEN" {
		t.Fatalf("malformed token: %d %s", r.Status, r.Raw)
	}
	tampered := Token(t, clientProviderA)
	tampered = tampered[:len(tampered)-4] + "AAAA"
	if r := do(t, inst, http.MethodPost, "/wagering/transactions", tampered, body, key); r.Status != http.StatusUnauthorized {
		t.Fatalf("tampered token: %d %s", r.Status, r.Raw)
	}
	short, err := fetchToken("short-lived")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(2500 * time.Millisecond)
	if r := do(t, inst, http.MethodPost, "/wagering/transactions", short, body, key); r.Status != http.StatusUnauthorized || r.errCode() != "INVALID_TOKEN" {
		t.Fatalf("expired token: %d %s", r.Status, r.Raw)
	}
	// Authenticated but without role.
	if r := do(t, inst, http.MethodPost, "/wagering/transactions", Token(t, "no-role"), body, key); r.Status != http.StatusForbidden {
		t.Fatalf("no role: %d %s", r.Status, r.Raw)
	}
	// Provider cannot use internal endpoints; internal cannot submit operations.
	if r := do(t, inst, http.MethodPost, "/wallets", Token(t, clientProviderA), map[string]any{"playerId": w.PlayerID, "initialBalance": map[string]string{"amount": "1.00", "currency": "BRL"}}, nil); r.Status != http.StatusForbidden {
		t.Fatalf("provider open wallet: %d", r.Status)
	}
	if r := do(t, inst, http.MethodGet, "/wallets/"+w.ID, Token(t, clientProviderA), nil, nil); r.Status != http.StatusForbidden {
		t.Fatalf("provider get wallet: %d", r.Status)
	}
	if r := do(t, inst, http.MethodPost, "/wagering/transactions", Token(t, clientInternal), body, key); r.Status != http.StatusForbidden {
		t.Fatalf("internal post tx: %d", r.Status)
	}
	// Provider B cannot impersonate provider A in the body.
	if r := do(t, inst, http.MethodPost, "/wagering/transactions", Token(t, clientProviderB), body, key); r.Status != http.StatusForbidden || r.errCode() != "PROVIDER_MISMATCH" {
		t.Fatalf("impersonation: %d %s", r.Status, r.Raw)
	}
	// None of the above had financial effects.
	if s, _ := txStatus(t, "provider-a", body.ExternalTransactionID); s != "" {
		t.Fatal("unauthorized requests must not create transactions")
	}
	if getWallet(t, inst, w.ID).money("balance") != "100.00" || countLedger(t, w.ID, "") != 1 {
		t.Fatal("unauthorized requests must not move money")
	}

	// Legitimate call, then isolation on reads and replays.
	r := postTx(t, inst, Token(t, clientProviderA), body)
	if r.Status != http.StatusOK {
		t.Fatalf("bet: %d %s", r.Status, r.Raw)
	}
	txID := r.str("transactionId")
	if r := do(t, inst, http.MethodGet, "/wagering/transactions/"+txID, Token(t, clientProviderB), nil, nil); r.Status != http.StatusNotFound {
		t.Fatalf("cross-provider by id: %d %s", r.Status, r.Raw)
	}
	if r := do(t, inst, http.MethodGet, "/providers/provider-a/wagering/transactions/"+body.ExternalTransactionID, Token(t, clientProviderB), nil, nil); r.Status != http.StatusForbidden {
		t.Fatalf("cross-provider by external id: %d %s", r.Status, r.Raw)
	}
	if r := do(t, inst, http.MethodGet, "/wagering/transactions/"+txID, Token(t, clientInternal), nil, nil); r.Status != http.StatusOK {
		t.Fatalf("internal read: %d", r.Status)
	}
	// Provider B replaying A's key/external id gets its own namespace: the
	// body must carry providerId=provider-b, and a wallet of another player
	// yields a rejection without touching A's transaction.
	bBody := body
	bBody.ProviderID = "provider-b"
	r = do(t, inst, http.MethodPost, "/wagering/transactions", Token(t, clientProviderB), bBody, key)
	if r.Status != http.StatusOK || r.Body["idempotentReplay"] != false {
		t.Fatalf("provider b namespace: %d %s", r.Status, r.Raw)
	}
	if getWallet(t, inst, w.ID).money("balance") != "80.00" {
		t.Fatal("expected two independent bets")
	}
	if r := do(t, inst, http.MethodGet, "/health/ready", "", nil, nil); r.Status != http.StatusOK {
		t.Fatal("health must be public")
	}
	if r := do(t, inst, http.MethodGet, "/metrics", "", nil, nil); r.Status != http.StatusOK || !contains(string(r.Raw), "wager_transactions_total") {
		t.Fatal("metrics must be exposed")
	}
}
