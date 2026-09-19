package app

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/jamesmachome/backend-challenge-go/internal/domain/wagering"
)

// PayloadHash returns the deterministic SHA-256 of the business fields of a
// request. The JSON is canonical: keys sorted lexicographically, no
// whitespace, money normalized to two decimals, and optional fields omitted
// when empty. Idempotency key, correlation id and transport metadata are not
// part of the hash, so HTTP and SQS produce the same value.
func PayloadHash(r wagering.ExternalRequest) string {
	fields := map[string]any{
		"providerId":            r.ProviderID,
		"externalTransactionId": r.ExternalTransactionID,
		"playerId":              r.PlayerID.String(),
		"walletId":              r.WalletID.String(),
		"roundId":               r.RoundID,
		"gameId":                r.GameID,
		"kind":                  string(r.Kind),
		"money": map[string]string{
			"amount":   r.Money.Amount(),
			"currency": string(r.Money.Currency()),
		},
	}
	if r.ReferenceExternalTransactionID != "" {
		fields["referenceExternalTransactionId"] = r.ReferenceExternalTransactionID
	}
	// encoding/json sorts map keys, which yields the canonical form.
	raw, err := json.Marshal(fields)
	if err != nil {
		panic(err) // only strings and maps: cannot fail
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
