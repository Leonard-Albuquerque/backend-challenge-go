#!/usr/bin/env bash
# Publishes a WagerTransactionRequested message to wager-transactions.fifo.
#   scripts/send-sqs.sh <playerId> <walletId> <externalTransactionId> [kind] [amount] [referenceExternalTransactionId]
set -euo pipefail
PLAYER="$1"; WALLET="$2"; EXT="$3"; KIND="${4:-BET}"; AMOUNT="${5:-25.00}"; REF="${6:-}"
ENDPOINT="${SQS_ENDPOINT:-http://localhost:4566}"
MSG_ID="msg-$EXT"
REF_JSON=""
[ -n "$REF" ] && REF_JSON=",\"referenceExternalTransactionId\":\"$REF\""
BODY=$(cat <<JSON
{"messageId":"$MSG_ID","type":"WagerTransactionRequested","occurredAt":"$(date -u +%Y-%m-%dT%H:%M:%S.000Z)","data":{"providerId":"provider-a","externalTransactionId":"$EXT","idempotencyKey":"provider-a:$EXT","playerId":"$PLAYER","walletId":"$WALLET","roundId":"round-$EXT","gameId":"fortune-chimp","kind":"$KIND","money":{"amount":"$AMOUNT","currency":"BRL"}$REF_JSON}}
JSON
)
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 \
aws --endpoint-url "$ENDPOINT" sqs send-message \
  --queue-url "$ENDPOINT/000000000000/wager-transactions.fifo" \
  --message-group-id "$WALLET" --message-deduplication-id "$MSG_ID-$(date +%s)" \
  --message-body "$BODY"
