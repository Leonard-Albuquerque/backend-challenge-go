#!/usr/bin/env bash
# Prints an access token for one of the provisioned Keycloak clients.
#   scripts/token.sh provider-a | provider-b | wallet-service
set -euo pipefail
CLIENT="${1:-provider-a}"
KEYCLOAK_URL="${KEYCLOAK_URL:-http://localhost:8081}"
curl -sf -X POST "$KEYCLOAK_URL/realms/wager/protocol/openid-connect/token" \
  -d grant_type=client_credentials -d "client_id=$CLIENT" -d "client_secret=${CLIENT}-secret" \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["access_token"])'
