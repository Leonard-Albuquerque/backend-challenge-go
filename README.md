# Wager Service — processamento distribuído de apostas em Go

Serviço HTTP + consumidor SQS que movimenta carteiras de jogadores com idempotência persistente, ledger append-only, transactional outbox e coordenação por carteira no PostgreSQL. Composto com Uber Fx, autenticado por Keycloak (OIDC), rodando em três instâncias no Docker Compose.

O enunciado original está em [CHALLENGE.md](CHALLENGE.md). As decisões de arquitetura, limitações e interpretações estão em [ARCHITECTURE.md](ARCHITECTURE.md).

## Pré-requisitos

- Docker e Docker Compose v2 (`docker compose`)
- Go 1.27+ (para rodar testes e a aplicação fora do container)
- `curl`/`python3` para os scripts de exemplo (opcional); `aws` CLI para `scripts/send-sqs.sh` (opcional — o `awslocal` dentro do container também funciona)

## Subir tudo

```bash
docker compose up --build
```

Sobe PostgreSQL 16, Keycloak 26 (realm `wager` importado automaticamente), LocalStack (filas FIFO provisionadas por `deploy/localstack/init-sqs.sh`) e **três instâncias** da aplicação:

| Instância | URL |
| --- | --- |
| app1 | http://localhost:8080 |
| app2 | http://localhost:8082 |
| app3 | http://localhost:8083 |

Keycloak fica em http://localhost:8081 (admin/admin) e o LocalStack em http://localhost:4566. As migrations são aplicadas automaticamente por cada instância na inicialização (`DB_AUTO_MIGRATE=true`; a aplicação é idempotente e o `golang-migrate` serializa com lock).

Passeio completo pelos fluxos (abertura, aposta, replay, reversão antecipada, rejeição, `LOSS`, reconciliação, eventos):

```bash
scripts/demo.sh
```

## Variáveis de ambiente

Todas com valores locais em [.env.example](.env.example). As principais:

| Variável | Padrão | Uso |
| --- | --- | --- |
| `HTTP_ADDR` | `:8080` | Endereço do servidor HTTP |
| `DATABASE_URL` | `postgres://wager:wager@localhost:5432/wager?sslmode=disable` | PostgreSQL |
| `DB_AUTO_MIGRATE` | `true` | Aplica migrations no start |
| `OIDC_ISSUER` | `http://localhost:8081/realms/wager` | Issuer esperado nos tokens |
| `OIDC_JWKS_URL` | `.../protocol/openid-connect/certs` | JWKS (pode usar host interno) |
| `OIDC_AUDIENCE` | `wager-api` | Audience exigida |
| `SQS_ENDPOINT` | `http://localhost:4566` | Endpoint SQS (vazio = AWS real) |
| `SQS_WAGER_QUEUE` / `SQS_WAGER_DLQ` / `SQS_EVENTS_QUEUE` | `wager-transactions.fifo` / `wager-transactions-dlq.fifo` / `wallet-events.fifo` | Filas |
| `SQS_VISIBILITY_TIMEOUT` | `30s` | Visibility timeout na recepção |
| `SQS_CONSUMER_WORKERS` | `4` | Goroutines de long-poll |
| `OUTBOX_POLL_INTERVAL` / `OUTBOX_LEASE` / `OUTBOX_BATCH_SIZE` | `500ms` / `30s` / `50` | Publisher da outbox |
| `PENDING_BASE_BACKOFF` / `PENDING_MAX_BACKOFF` / `PENDING_MAX_ATTEMPTS` | `1s` / `1m` / `10` | Retentativa de referências pendentes |
| `WORKER_SHUTDOWN_TIMEOUT` / `HTTP_SHUTDOWN_TIMEOUT` | `20s` / `15s` | Prazos de drenagem no shutdown |
| `CRASH_POINT` | vazio | **Só para testes**: `consumer.after_commit`, `outbox.after_claim`, `outbox.after_publish` |

Credenciais AWS vêm da cadeia padrão do SDK (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, profile, role). Localmente use `test`/`test`.

## Migrations

Versionadas em [migrations/](migrations/) (`golang-migrate`, embutidas no binário):

```bash
export DATABASE_URL=postgres://wager:wager@localhost:5432/wager?sslmode=disable
go run ./cmd/migrate up
```

```bash
go run ./cmd/migrate down
```

Também: `go run ./cmd/migrate steps -1` e `go run ./cmd/migrate version`. No container: `docker compose run --rm --entrypoint /migrate app1 up`.

## Filas

Provisionadas automaticamente pelo script de init do LocalStack:

| Fila | Papel |
| --- | --- |
| `wager-transactions.fifo` | Entrada de operações (`WagerTransactionRequested`); redrive para a DLQ após 5 recepções; visibility timeout 30s; policy restringindo `SendMessage` aos provedores e consumo ao serviço |
| `wager-transactions-dlq.fifo` | DLQ de entrada (mensagens inválidas, erros permanentes, tentativas esgotadas) |
| `wallet-events.fifo` | Destino dos eventos de integração publicados pela outbox |
| `wallet-events-dlq.fifo` | DLQ do destino de eventos (redrive após 5 recepções pelos consumidores downstream) |

Para reprovisionar manualmente: `docker compose exec localstack /etc/localstack/init/ready.d/init-sqs.sh`.

## Autenticação

Keycloak importa `deploy/keycloak/wager-realm.json` com as identidades de teste (`client_credentials`):

| Client | Secret | Role | Uso |
| --- | --- | --- | --- |
| `provider-a` | `provider-a-secret` | `provider` (claim `providerId=provider-a`) | Enviar/consultar operações do provedor A |
| `provider-b` | `provider-b-secret` | `provider` (`providerId=provider-b`) | Isolamento entre provedores |
| `wallet-service` | `wallet-service-secret` | `internal` | Abrir/ler/reconciliar carteiras; ler qualquer transação |
| `short-lived` | `short-lived-secret` | `provider` | Tokens de 1s (teste de expiração) |
| `no-role` | `no-role-secret` | nenhuma | Autenticado sem autorização |

```bash
scripts/token.sh provider-a
```

Matriz de autorização:

| Endpoint | `internal` | `provider` |
| --- | --- | --- |
| `POST /wallets`, `GET /wallets/:id`, `GET /wallets/:id/ledger`, `POST /wallets/:id/reconciliation` | ✅ | ❌ 403 |
| `POST /wagering/transactions` | ❌ 403 | ✅ somente com `providerId` do corpo igual ao do token (senão 403 `PROVIDER_MISMATCH`) |
| `GET /wagering/transactions/:id` | ✅ | ✅ apenas as próprias (outras respondem 404) |
| `GET /providers/:providerId/wagering/transactions/:ext` | ✅ | ✅ apenas o próprio `providerId` (senão 403) |
| `GET /health/*`, `GET /metrics` | público | público |

## Exemplos de chamadas

```bash
INTERNAL=$(scripts/token.sh wallet-service); PROVIDER=$(scripts/token.sh provider-a)
PLAYER=0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1
```

Abrir carteira (201; 409 `WALLET_EXISTS` para o mesmo jogador/moeda):

```bash
curl -s -X POST localhost:8080/wallets -H "Authorization: Bearer $INTERNAL" -H 'Content-Type: application/json' \
  -d "{\"playerId\":\"$PLAYER\",\"initialBalance\":{\"amount\":\"1000.00\",\"currency\":\"BRL\"}}"
```

Enviar aposta (substitua `WALLET`):

```bash
curl -s -X POST localhost:8080/wagering/transactions -H "Authorization: Bearer $PROVIDER" -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: provider-a:transaction-123' \
  -d "{\"providerId\":\"provider-a\",\"externalTransactionId\":\"transaction-123\",\"playerId\":\"$PLAYER\",\"walletId\":\"WALLET\",\"roundId\":\"round-987\",\"gameId\":\"fortune-chimp\",\"kind\":\"BET\",\"money\":{\"amount\":\"25.00\",\"currency\":\"BRL\"}}"
```

Reversão (`REFUND`/`ROLLBACK`): acrescente `"referenceExternalTransactionId":"transaction-123"` ao corpo. `WIN` pode opcionalmente referenciar a `BET` da mesma rodada.

Consultas e reconciliação:

```bash
curl -s localhost:8080/wagering/transactions/TRANSACTION_ID -H "Authorization: Bearer $PROVIDER"
```

```bash
curl -s localhost:8080/providers/provider-a/wagering/transactions/transaction-123 -H "Authorization: Bearer $PROVIDER"
```

```bash
curl -s "localhost:8080/wallets/WALLET/ledger?limit=50" -H "Authorization: Bearer $INTERNAL"
```

```bash
curl -s -X POST localhost:8080/wallets/WALLET/reconciliation -H "Authorization: Bearer $INTERNAL"
```

Mensagem SQS (mesmo caso de uso, chave em `data.idempotencyKey`):

```bash
scripts/send-sqs.sh $PLAYER WALLET transaction-456 BET 25.00
```

### Contrato de respostas de `POST /wagering/transactions`

| Situação | HTTP | Corpo |
| --- | --- | --- |
| Processada | 200 | `{"transactionId","status":"PROCESSED","balance","idempotentReplay",...}` |
| Rejeitada por regra de negócio | 422 | mesmo corpo com `"status":"REJECTED"` e `failureCode` |
| Aguardando referência | 202 | `"status":"PENDING_REFERENCE"`, `referenceAttempts`, `nextReferenceRetryAt` |
| Replay (chave + conteúdo iguais) | código do resultado original | `"idempotentReplay":true` e o saldo observado no processamento original |
| Entrada inválida (JSON, dinheiro, campos, `OPENING`, cursor) | 400 | `{"error":{"code":"VALIDATION_ERROR","message"}}` |
| Sem `Idempotency-Key` | 400 | `MISSING_IDEMPOTENCY_KEY` |
| Carteira inexistente | 422 | `WALLET_NOT_FOUND` |
| Chave reutilizada com conteúdo diferente | 409 | `IDEMPOTENCY_KEY_CONFLICT` |
| `(providerId, externalTransactionId)` já usado com outra chave | 409 | `EXTERNAL_TRANSACTION_ID_CONFLICT` |
| Corrida perdida em constraint (raro; reenviar com a mesma chave) | 409 | `CONCURRENT_UPDATE` |
| Sem token / token inválido ou expirado | 401 | `MISSING_TOKEN` / `INVALID_TOKEN` |
| Sem permissão | 403 | `FORBIDDEN` / `PROVIDER_MISMATCH` |
| Dependência indisponível (PostgreSQL/SQS) | 503 + `Retry-After` | `TEMPORARILY_UNAVAILABLE` |

Códigos de falha (`failureCode`) das rejeições, todos definitivos para aquela transação:

| Código | Significado | Corrigível? |
| --- | --- | --- |
| `INSUFFICIENT_FUNDS` | `BET` sem saldo | Nova transação após crédito |
| `REVERSAL_INSUFFICIENT_FUNDS` | `ROLLBACK` de `WIN`/`REFUND` debitaria mais que o saldo | Nova transação após crédito |
| `REFERENCE_NOT_FOUND` | Referência não chegou dentro do TTL de tentativas | Reenviar após a referência ser processada |
| `REFERENCE_NOT_PROCESSED` | Referência terminou `REJECTED`/`FAILED` | Definitivo |
| `REFERENCE_KIND_MISMATCH` | Ex.: `REFUND` apontando para `WIN` | Definitivo (payload errado) |
| `REFERENCE_MISMATCH` | Jogador, carteira, moeda ou rodada divergem da referência | Definitivo (payload errado) |
| `REFERENCE_AMOUNT_MISMATCH` | Valor da reversão ≠ valor referenciado | Definitivo (payload errado) |
| `REFERENCE_ALREADY_REVERSED` | A referência já recebeu uma reversão bem-sucedida | Definitivo |
| `WALLET_PLAYER_MISMATCH` | Carteira não pertence ao `playerId` | Definitivo (payload errado) |
| `CURRENCY_MISMATCH` | Moeda diferente da carteira | Definitivo (payload errado) |

## Health checks e métricas

- `GET /health/live` — liveness do processo.
- `GET /health/ready` — readiness (ping no PostgreSQL, `GetQueueAttributes` no SQS, JWKS do IdP); 503 com o detalhe por dependência.
- `GET /metrics` — Prometheus: `wager_transactions_total{kind,status,source}`, `wager_duplicates_total{source}`, `wager_retries_total{component}`, `wager_dlq_messages_total{reason}`, `wager_concurrency_conflicts_total`, `wager_outbox_lag_seconds`, `wager_outbox_pending`, `wager_outbox_published_total`, `wager_processing_duration_seconds{source}`, `wager_reconciliation_divergences_total`, `http_requests_total{route,status}`, `http_request_duration_seconds{route}`.

Logs são JSON (`slog`) com `correlationId`, `messageId`, `transactionId`, `walletId`, `providerId` quando disponíveis. O `X-Correlation-Id` recebido é propagado e ecoado; se ausente, é gerado. Em SQS, o `messageId` do envelope é a correlação.

## Testes

Unitários e estáticos (não precisam de infraestrutura):

```bash
go test ./...
```

```bash
go test -race ./...
```

```bash
go vet ./...
```

### Integração, múltiplas instâncias e simulações de falha

Os testes de integração ficam atrás da build tag `integration` em [test/integration](test/integration). Eles precisam apenas das dependências (não das instâncias `app*`, que compartilhariam a outbox e as pendências com os processos de teste):

```bash
make deps
```

```bash
go test -tags integration -count=1 -timeout 20m ./test/integration/...
```

```bash
go test -race -tags integration -count=1 -timeout 30m ./test/integration/...
```

O `TestMain` compila `./cmd/wager` **com `-race`** e sobe um cluster de **três processos independentes** (memória e pools próprios) mais instâncias auxiliares por cenário, cada uma com filas FIFO exclusivas da execução (`it-<runId>-*`). Cobertura:

| Teste | Cenário |
| --- | --- |
| `TestMigrationsUpAndDown` | Aplicação e reversão em banco dedicado |
| `TestSchemaConstraintsAndImmutability` | Saldo negativo, ledger imutável (update/delete), unicidade, aritmética, terminalidade, `OPENING` único, reversão única, inbox, atomicidade |
| `TestOpeningAndBasicFlow` | Abertura, os cinco tipos, replay com saldo original, conflitos, validação, paginação, reconciliação |
| `TestAuthentication` | Token ausente/malformado/adulterado/expirado, sem role, isolamento entre provedores em envio, consulta e replay, ausência de efeitos financeiros |
| `TestSameBet50TimesInParallel` | 50 envios paralelos em 3 instâncias → 1 débito, 49 replays |
| `TestTwo80BetsOn100` | Duas apostas de 80.00 sobre 100.00 (3 rodadas): 1 processada, 1 `INSUFFICIENT_FUNDS`, saldo 20.00, 1 débito; reenvios não alteram |
| `TestDistinctWalletsProgressInParallel` | 12 carteiras × 8 apostas simultâneas |
| `TestSQSConsumerInboxAndCrossSourceIdempotency` | Consumo, reentrega do mesmo `messageId`, mesma operação com outro `messageId`, HTTP↔SQS (antes, depois e simultâneo), rejeição terminal, mensagens inválidas na DLQ |
| `TestConsumerCrashAfterCommitBeforeDelete` | `CRASH_POINT=consumer.after_commit`: processo morre após o commit; reentrega absorvida pela inbox por outra instância |
| `TestTransientFailuresReachDLQAfterRetries` | Falha transitória (banco com `statement_timeout=1ms`) → backoff de visibilidade → redrive para a DLQ |
| `TestOutboxEventsPublishedAfterCommitWithStableIDs` | Eventos de abertura e aposta chegam em `wallet-events.fifo` com `eventId` estáveis |
| `TestOutboxPublishersCompeteAndRecover` | Crash após claim e após publish (`outbox.after_claim` / `outbox.after_publish`); dois publishers disputando 42 eventos; entrega única por `eventId` |
| `TestReversalBeforeReferenceResolvesLater` | `REFUND` via SQS antes da `BET` → `PENDING_REFERENCE` → resolvido pelo worker de outra instância |
| `TestReversalExpiresAsRejected` | Esgotamento de tentativas → `REFERENCE_NOT_FOUND`; `REVERSAL_INSUFFICIENT_FUNDS` |
| `TestRestartPreservesIdempotencyPendingAndConsistency` | SIGTERM (ordem de shutdown verificada nos logs) e novo processo retoma idempotência, pendência, outbox e reconciliação |
| `TestSIGTERMWhileConsuming` | SIGTERM com mensagens em voo; nada perdido, nada duplicado |
| `TestFxLifecycle` | Composição Fx real em processo: validação de config, start, readiness, stop ordenado, porta liberada, falha rápida com fila inexistente |

Logs de cada processo ficam em um diretório temporário impresso no início da execução (mantido em caso de falha).

## Layout

```
cmd/wager            binário do serviço (-healthcheck para o Docker)
cmd/migrate          CLI de migrations
internal/domain      money, wallet (+ ledger), wagering (transação e estados), events — sem dependências de infra
internal/app         casos de uso, ports, hash canônico
internal/adapters    postgres (pgx + SQL explícito), sqs (consumer/publisher), httpapi, auth (OIDC)
internal/workers     outbox publisher, pending reference worker
internal/fxapp       módulos Fx e lifecycle
internal/config      configuração por ambiente
internal/observability  slog JSON, métricas Prometheus
migrations/          SQL versionado (up/down)
deploy/              realm do Keycloak, init do LocalStack
test/integration     suíte com containers reais e múltiplos processos
```
