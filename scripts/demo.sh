#!/usr/bin/env bash
# End-to-end walkthrough against a running stack (docker compose up --build).
set -euo pipefail
API="${API:-http://localhost:8080}"
cd "$(dirname "$0")/.."
INTERNAL=$(scripts/token.sh wallet-service)
PROVIDER=$(scripts/token.sh provider-a)
PLAYER=$(python3 -c 'import uuid; print(uuid.uuid4())')

echo "## open wallet"
WALLET_JSON=$(curl -sf -X POST "$API/wallets" -H "Authorization: Bearer $INTERNAL" -H 'Content-Type: application/json' \
  -d "{\"playerId\":\"$PLAYER\",\"initialBalance\":{\"amount\":\"1000.00\",\"currency\":\"BRL\"}}")
echo "$WALLET_JSON"
WALLET=$(echo "$WALLET_JSON" | python3 -c 'import sys,json; print(json.load(sys.stdin)["id"])')

post() { # ext kind amount [ref]
  local ref=""; [ -n "${4:-}" ] && ref=",\"referenceExternalTransactionId\":\"$4\""
  curl -s -X POST "$API/wagering/transactions" -H "Authorization: Bearer $PROVIDER" -H 'Content-Type: application/json' \
    -H "Idempotency-Key: provider-a:$1" \
    -d "{\"providerId\":\"provider-a\",\"externalTransactionId\":\"$1\",\"playerId\":\"$PLAYER\",\"walletId\":\"$WALLET\",\"roundId\":\"round-987\",\"gameId\":\"fortune-chimp\",\"kind\":\"$2\",\"money\":{\"amount\":\"$3\",\"currency\":\"BRL\"}$ref}"
  echo
}
echo "## BET 25.00";            post "$PLAYER-bet-1" BET 25.00
echo "## same BET again (replay)"; post "$PLAYER-bet-1" BET 25.00
echo "## REFUND before its BET (parked)"; post "$PLAYER-refund-2" REFUND 10.00 "$PLAYER-bet-2"
echo "## BET 10.00 (reference arrives)"; post "$PLAYER-bet-2" BET 10.00
sleep 3
echo "## refund status after worker"; curl -s "$API/providers/provider-a/wagering/transactions/$PLAYER-refund-2" -H "Authorization: Bearer $PROVIDER"; echo
echo "## BET 5000.00 (rejected)";   post "$PLAYER-bet-3" BET 5000.00
echo "## LOSS 0.00";               post "$PLAYER-loss-1" LOSS 0.00
echo "## reconciliation"; curl -s -X POST "$API/wallets/$WALLET/reconciliation" -H "Authorization: Bearer $INTERNAL"; echo
echo "## ledger"; curl -s "$API/wallets/$WALLET/ledger?limit=10" -H "Authorization: Bearer $INTERNAL"; echo
echo "## events published to wallet-events.fifo"
docker compose exec -T localstack awslocal sqs receive-message --queue-url http://localhost:4566/000000000000/wallet-events.fifo \
  --max-number-of-messages 10 --message-attribute-names All --visibility-timeout 0 \
  | python3 -c 'import sys,json; d=json.load(sys.stdin); [print(m["MessageAttributes"]["eventType"]["StringValue"]) for m in d.get("Messages",[])]' || true
