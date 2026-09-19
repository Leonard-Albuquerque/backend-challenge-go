//go:build integration

package integration

import (
	"fmt"
	"net/http"
	"sync"
	"testing"
)

// parallel runs fn n times concurrently against round-robin instances.
func parallel(n int, fn func(i int, inst *Instance)) {
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			fn(i, cluster.Next())
		}(i)
	}
	close(start)
	wg.Wait()
}

func TestSameBet50TimesInParallel(t *testing.T) {
	w := openWallet(t, cluster.Next(), "1000.00")
	b := bet(w, "tx-50-"+w.ID[24:], "25.00")
	results := make([]response, 50)
	parallel(50, func(i int, inst *Instance) { results[i] = postTx(t, inst, Token(t, clientProviderA), b) })

	processed, replays := 0, 0
	txID := ""
	for i, r := range results {
		if r.Status != http.StatusOK || r.str("status") != "PROCESSED" || r.money("balance") != "975.00" {
			t.Fatalf("request %d: %d %s", i, r.Status, r.Raw)
		}
		if txID == "" {
			txID = r.str("transactionId")
		} else if r.str("transactionId") != txID {
			t.Fatal("different transaction ids for the same operation")
		}
		if r.Body["idempotentReplay"] == true {
			replays++
		} else {
			processed++
		}
	}
	if processed != 1 || replays != 49 {
		t.Fatalf("processed=%d replays=%d", processed, replays)
	}
	if getWallet(t, cluster.Next(), w.ID).money("balance") != "975.00" || countLedger(t, w.ID, "DEBIT") != 1 {
		t.Fatal("expected exactly one debit")
	}
	if rec := reconcile(t, cluster.Next(), w.ID); rec.Body["consistent"] != true {
		t.Fatalf("reconciliation: %s", rec.Raw)
	}
}

func TestTwo80BetsOn100(t *testing.T) {
	for round := 0; round < 3; round++ {
		w := openWallet(t, cluster.Next(), "100.00")
		b1 := bet(w, fmt.Sprintf("tx-80a-%s-%d", w.ID[24:], round), "80.00")
		b2 := bet(w, fmt.Sprintf("tx-80b-%s-%d", w.ID[24:], round), "80.00")
		results := make([]response, 2)
		parallel(2, func(i int, inst *Instance) {
			if i == 0 {
				results[i] = postTx(t, inst, Token(t, clientProviderA), b1)
			} else {
				results[i] = postTx(t, inst, Token(t, clientProviderA), b2)
			}
		})
		statuses := map[string]int{}
		codes := map[string]int{}
		for _, r := range results {
			statuses[r.str("status")]++
			codes[r.str("failureCode")]++
			if r.Status != http.StatusOK && r.Status != http.StatusUnprocessableEntity {
				t.Fatalf("unexpected http status %d %s", r.Status, r.Raw)
			}
		}
		if statuses["PROCESSED"] != 1 || statuses["REJECTED"] != 1 || codes["INSUFFICIENT_FUNDS"] != 1 {
			t.Fatalf("round %d: statuses=%v codes=%v", round, statuses, codes)
		}
		if getWallet(t, cluster.Next(), w.ID).money("balance") != "20.00" || countLedger(t, w.ID, "DEBIT") != 1 {
			t.Fatalf("round %d: balance/ledger wrong", round)
		}
		// Resending both does not change the outcome.
		r1 := postTx(t, cluster.Next(), Token(t, clientProviderA), b1)
		r2 := postTx(t, cluster.Next(), Token(t, clientProviderA), b2)
		if r1.Body["idempotentReplay"] != true || r2.Body["idempotentReplay"] != true {
			t.Fatal("resend must replay")
		}
		if getWallet(t, cluster.Next(), w.ID).money("balance") != "20.00" || countLedger(t, w.ID, "DEBIT") != 1 {
			t.Fatal("resend changed the result")
		}
		if rec := reconcile(t, cluster.Next(), w.ID); rec.Body["consistent"] != true {
			t.Fatalf("reconciliation: %s", rec.Raw)
		}
	}
}

func TestDistinctWalletsProgressInParallel(t *testing.T) {
	const wallets, betsPerWallet = 12, 8
	ws := make([]testWallet, wallets)
	for i := range ws {
		ws[i] = openWallet(t, cluster.Next(), "100.00")
	}
	parallel(wallets*betsPerWallet, func(i int, inst *Instance) {
		w := ws[i%wallets]
		r := postTx(t, inst, Token(t, clientProviderA), bet(w, fmt.Sprintf("tx-par-%s-%d", w.ID[24:], i/wallets), "5.00"))
		if r.Status != http.StatusOK {
			t.Errorf("wallet %d: %d %s", i%wallets, r.Status, r.Raw)
		}
	})
	for i, w := range ws {
		if getWallet(t, cluster.Next(), w.ID).money("balance") != "60.00" || countLedger(t, w.ID, "DEBIT") != betsPerWallet {
			t.Fatalf("wallet %d: wrong balance or ledger", i)
		}
		if rec := reconcile(t, cluster.Next(), w.ID); rec.Body["consistent"] != true {
			t.Fatalf("wallet %d reconciliation: %s", i, rec.Raw)
		}
	}
}
