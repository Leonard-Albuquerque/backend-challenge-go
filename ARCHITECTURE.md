# Arquitetura e decisões

## Visão geral

```
 provedores ──HTTP──▶ httpapi ─┐                         ┌─▶ outbox publisher ──▶ wallet-events.fifo
                               ├─▶ app.Service ──▶ PostgreSQL (wallets, wager_transactions,
 wager-transactions.fifo ─SQS─▶ consumer ─┘        ledger, inbox, outbox)   └─▶ pending reference worker
```

Um único caso de uso (`app.Service.ProcessIn`) atende HTTP e SQS. Cada operação roda em **uma transação SQL** que grava, de forma atômica: a transação de aposta, o lançamento no ledger, o novo saldo/versão da carteira, os eventos na outbox e, no caminho SQS, o registro da inbox. Nada é publicado antes do commit; workers separados publicam a outbox e retentam referências pendentes, em qualquer instância.

Pacotes: `internal/domain` (independente de Fx/HTTP/SQS/pgx), `internal/app` (casos de uso + ports), `internal/adapters/*`, `internal/workers`, `internal/fxapp`.

## Dinheiro

`money.Money` é um value object imutável: `int64` em **unidades mínimas** (centavos) + moeda ISO 4217 (`[A-Z]{3}`), escala fixa 2. O zero value é inválido e rejeitado por todas as operações (`ErrUninitialized`).

- **Parsing** (`money.Parse`): aceita apenas `^[0-9]+(\.[0-9]{1,2})?$`. Rejeita vazio, `NaN`, `Infinity`, notação científica, sinal `+`, vírgula, mais de duas casas (sem arredondar), negativos (`ErrNegativeAmount`) e JSON numérico (o DTO exige string). Formas equivalentes aceitas: `25`, `25.5`, `25.50` — **normalizadas para `25.50`** antes do hash de idempotência (`Amount()`).
- **Limites**: máximo `92233720368547758.07`. Overflow é detectado no parsing (por comprimento e por multiplicação segura), em `Add`, `Sub` e `Negate` (`MinInt64`), retornando `ErrOverflow`.
- Aritmética e comparação exigem a mesma moeda (`ErrCurrencyMismatch`). Negativos existem apenas em cálculos internos (diferença da reconciliação); `ParseSigned` só é usado internamente.
- **Persistência**: `BIGINT` para valores (`amount_minor`, `balance_minor`, ...) e `CHAR(3)` para moeda; nada passa por `float`. A serialização externa é sempre `{"amount":"25.00","currency":"BRL"}`.
- Cenários principais operam em BRL; o tipo carrega a moeda e há testes de incompatibilidade.

## Acesso ao banco e transações

`pgx/v5` com SQL explícito (sem ORM). `postgres.UnitOfWork` abre transações `READ COMMITTED` (`Do`) ou `REPEATABLE READ` somente leitura (`DoSnapshot`, usada na reconciliação). Os repositórios (`WalletRepository`, `TransactionRepository`, `LedgerRepository`, `OutboxRepository`, `InboxRepository`) recebem a mesma `pgx.Tx` através de `app.Store`; a delimitação da transação é do caso de uso, nunca dos repositórios. Pool: `pgxpool` (fechado no último hook de shutdown).

Fluxo de `ProcessIn` (uma transação):

1. Busca por `(providerId, idempotencyKey)`: se existir, compara o hash → replay (`idempotentReplay: true`) ou `ErrIdempotencyConflict`. Se existir `(providerId, externalTransactionId)` com outra chave → `ErrExternalIDConflict` (se for a mesma chave, é a mesma operação commitada entre os dois SELECTs → replay).
2. `SELECT ... FOR UPDATE` na carteira — **ponto de serialização por carteira**.
3. Repete a busca do passo 1 sob o lock (duplicatas concorrentes na mesma carteira são absorvidas aqui, sem violar constraint).
4. Aplica as regras (agregado `Wallet` + `Transaction`), `INSERT` da transação já no estado final, `INSERT` no ledger, `UPDATE wallets ... WHERE id = $1 AND version = $expected`, `INSERT`s na outbox. Commit.

Uma corrida perdida em constraint única (só possível entre carteiras diferentes com a mesma chave) é retentada uma vez em `Process` e, se persistir, vira 409 `CONCURRENT_UPDATE`/retry no SQS.

## Concorrência e lost updates

Estratégia: **lock pessimista por linha da carteira** (`FOR UPDATE`) combinado com **verificação otimista de versão** no `UPDATE` (`AND version = $expected`; 0 linhas ⇒ `ErrConcurrentModification`, contado em métrica) e **constraints no banco** como última linha de defesa (`balance_minor >= 0`, unicidade do ledger, índices únicos de idempotência).

- Locks são por carteira; carteiras diferentes avançam em paralelo (não há lock global nem `LOCK TABLE`).
- O lock vive apenas durante a transação SQL (síncrona; sem commit intermediário `PENDING`).
- As garantias não dependem da deduplicação do SQS FIFO nem de memória local: três processos independentes são exercitados nos testes.
- Ordem de locks: fluxo principal trava carteira → lê transações (sem lock). Worker de pendências trava a transação pendente (`FOR UPDATE SKIP LOCKED`) → trava a carteira. Não há ciclo.

## Idempotência

- `Idempotency-Key` é obrigatória e persistida como recebida (nunca substituída pela calculada). No SQS, a chave é `data.idempotencyKey`.
- **Hash do payload** (`app.PayloadHash`): SHA-256 do JSON canônico (chaves ordenadas, sem espaços) de `providerId, externalTransactionId, playerId, walletId, roundId, gameId, kind, money{amount,currency}` e `referenceExternalTransactionId` (omitido se vazio). `money.amount` é normalizado para duas casas; UUIDs para minúsculas canônicas. Chave, correlação e metadados de transporte ficam fora do hash — HTTP e SQS produzem o mesmo valor.
- Índices únicos parciais: `(provider_id, idempotency_key)` e `(provider_id, external_transaction_id)` para `origin = 'EXTERNAL'`.
- Replay devolve o estado persistido, incluindo `result_balance_minor` (saldo observado no processamento original) e `failureCode`; nunca reaplica.
- Namespaces por provedor: a mesma chave/ID externo em provedores diferentes são operações diferentes.

## Máquina de estados

```
PENDING ──▶ PROCESSED | REJECTED | FAILED | PENDING_REFERENCE
PENDING_REFERENCE ──▶ PROCESSED | REJECTED | FAILED   (e PENDING_REFERENCE → PENDING_REFERENCE a cada tentativa)
```

As transições vivem em `wagering.Transaction` (`MarkProcessed`, `MarkRejected`, `MarkFailed`, `AwaitReference`, `ResolveReference`); estados terminais rejeitam qualquer transição (`ErrTerminal`) e o banco reforça com o trigger `wager_transactions_guard` (bloqueia `UPDATE` de linhas terminais e alterações de campos de identidade) e `forbid_delete`. A constraint `wager_transactions_terminal_shape` garante que `PROCESSED` tem saldo resultante, `REJECTED`/`FAILED` têm `failure_code` e `PENDING_REFERENCE` tem próximo retry.

**Transitório vs. permanente**: erros de infraestrutura (conexão, timeout, serialização, deadlock — `postgres.IsTransient`/`sqs.IsTransient`) não geram registro: a transação SQL é abortada e o cliente recebe 503 (`Retry-After`) ou a mensagem volta à fila com backoff. `FAILED` está reservado a falhas permanentes de infraestrutura registradas para auditoria; no desenho atual nenhum caminho automático o produz, porque tudo que falha antes do commit é retentável — está modelado e protegido pelo schema para uso operacional.

Não há aceite assíncrono: operações sem dependência são concluídas na mesma transação, sem commit intermediário `PENDING`. O único estado não terminal persistido é `PENDING_REFERENCE`, retomado por qualquer instância.

## Referências e reversões

`REFUND` e `ROLLBACK` exigem `referenceExternalTransactionId`, resolvido por `(providerId, referenceExternalTransactionId)`. `WIN` pode referenciar opcionalmente uma `BET` da mesma rodada (validada com as mesmas regras, sem exigir valor igual).

Regras, na ordem em que são avaliadas:

| Situação da referência | Resultado |
| --- | --- |
| Não existe | `PENDING_REFERENCE` (ou `REJECTED/REFERENCE_NOT_FOUND` se as tentativas esgotaram) |
| Existe em `PENDING`/`PENDING_REFERENCE` | Continua aguardando (`PENDING_REFERENCE`) |
| `REJECTED`/`FAILED` | `REJECTED/REFERENCE_NOT_PROCESSED` |
| Tipo incompatível (`WIN`/`REFUND` → só `BET`; `ROLLBACK` → `BET`, `WIN`, `REFUND`) | `REFERENCE_KIND_MISMATCH` |
| Jogador, carteira, moeda ou rodada divergem | `REFERENCE_MISMATCH` |
| Valor da reversão ≠ valor referenciado | `REFERENCE_AMOUNT_MISMATCH` |
| Já existe reversão `PROCESSED` para a referência | `REFERENCE_ALREADY_REVERSED` |

Movimentação: `REFUND` credita; `ROLLBACK` de `BET` credita, de `WIN`/`REFUND` debita. Um `ROLLBACK` que debitaria além do saldo é rejeitado com `REVERSAL_INSUFFICIENT_FUNDS` (distinto de `INSUFFICIENT_FUNDS`), auditável como qualquer rejeição.

**Combinações de `REFUND` e `ROLLBACK` sobre a mesma aposta**: adotamos "uma reversão bem-sucedida por referência, de qualquer tipo". Depois de um `REFUND` processado, um `ROLLBACK` da mesma `BET` é rejeitado (`REFERENCE_ALREADY_REVERSED`) — devolveria o mesmo débito duas vezes. O caminho correto para desfazer o refund é `ROLLBACK` referenciando o `REFUND`, que re-debita (sujeito a saldo). Isso é imposto no banco pelo índice único parcial `wager_transactions_single_reversal (reference_transaction_id) WHERE status = 'PROCESSED' AND kind IN ('REFUND','ROLLBACK')`, independentemente da verificação da aplicação.

**Referências ainda indisponíveis**: a transação é persistida como `PENDING_REFERENCE` com `reference_attempts` e `next_reference_retry_at` (backoff exponencial `PENDING_BASE_BACKOFF · 2^(n-1)` limitado a `PENDING_MAX_BACKOFF`). O evento `WagerTransactionPendingReference` é emitido apenas na primeira espera. O worker (`workers.PendingReferenceWorker`, em toda instância) usa `SELECT ... FOR UPDATE SKIP LOCKED` sobre as pendências vencidas, uma por transação SQL, reaplicando as mesmas regras. Ao atingir `PENDING_MAX_ATTEMPTS` (padrão 10; ≈ 5 min com os padrões), finaliza `REJECTED/REFERENCE_NOT_FOUND` e emite `WagerTransactionRejected`. Sobrevive a reinícios porque o estado e o agendamento estão no banco. Replays de uma operação pendente retornam 202 com o estado atual.

## Wallet e ledger

`wallet.Wallet` encapsula id, jogador, moeda, saldo, versão e timestamps. `Open` (versão 1) e `Rehydrate` são separados; `Debit`/`Credit` validam moeda e positividade, impedem saldo negativo e devolvem o `LedgerEntry` que precisa ser persistido junto. A versão só muda com alteração de saldo (`LOSS` não cria ledger nem incrementa versão).

`LedgerEntry` é imutável e sua construção valida `balanceAfter = balanceBefore ± amount`; a mesma validação roda na reidratação. No banco: `UNIQUE (wallet_id, transaction_id)`, `CHECK` aritmético, `CHECK` de não negatividade, triggers que bloqueiam `UPDATE` e `DELETE`. O cursor da paginação é `base64url("seq:<bigserial>")`, ordenado por `seq`, estável e opaco.

Abertura com saldo positivo grava, no mesmo commit da carteira: `OPENING` (`origin = 'INTERNAL'`, `PROCESSED`), lançamento de crédito com `balanceBefore = 0`, e eventos `WagerTransactionProcessed` + `WalletBalanceChanged` (`walletVersion = 1`). Saldo zero cria apenas a carteira. O schema distingue as origens (`wager_transactions_external_metadata`: metadados externos obrigatórios em `EXTERNAL` e proibidos em `INTERNAL`), impede `OPENING` externo (`origin_kind`) e crédito inicial duplicado (`wager_transactions_opening_unique`). Rejeitamos `kind = OPENING` na entrada HTTP/SQS com 400/DLQ.

## Inbox e outbox

**Inbox** (`inbox_messages`, PK `(consumer_name, message_id)`): o consumidor insere a linha (com `completed_at`) **na mesma transação** das alterações de domínio, ledger e outbox; a linha só existe para mensagens cujo tratamento foi commitado. Em reentrega, `ON CONFLICT DO NOTHING` sinaliza duplicata e a mensagem é removida sem reprocessar; o hash guardado (SHA-256 do hash canônico do `data` + `idempotencyKey`) detecta um `messageId` reutilizado com outro conteúdo (vai para a DLQ). Para uma referência pendente, a mensagem é concluída assim que a pendência está persistida; o worker assume dali em diante.

**Outbox** (`outbox_events`): `event_id` (UUIDv7, PK), agregado, tipo, payload JSONB (snapshot imutável do envelope), `occurred_at`, `attempts`, `next_attempt_at`, `locked_by`/`locked_until` (lease), `published_at`, `last_error`. O publisher (`workers.OutboxPublisher`, em toda instância):

1. `UPDATE ... WHERE event_id IN (SELECT ... WHERE published_at IS NULL AND next_attempt_at <= now AND (locked_until IS NULL OR locked_until < now) ... FOR UPDATE SKIP LOCKED) RETURNING` — claim com lease (`OUTBOX_LEASE`, 30s) e `attempts + 1`, commitado antes de publicar. Vários publishers disputam sem colisão; um lease expirado (processo morto) é reclamado por outro.
2. `SendMessageBatch` em `wallet-events.fifo`, em lotes de até 10, com `MessageGroupId = aggregateId` e `MessageDeduplicationId = eventId`. Falha do lote inteiro cai para envios individuais; falhas por entrada são reagendadas individualmente. (O envio unitário original não acompanhava a carga — ver `loadtest/README.md`.)
3. `UPDATE ... SET published_at` para os ids aceitos, em uma transação por lote (mantém `locked_by` como o publisher que confirmou). Se o processo morre entre 2 e 3, os eventos são republicados com o **mesmo `eventId`**: o FIFO deduplica na janela de 5 min e consumidores devem deduplicar por `eventId` além dela.
4. Falha de publicação: `Reschedule` com backoff exponencial (`OUTBOX_BASE_BACKOFF` → `OUTBOX_MAX_BACKOFF`) e `last_error`.

Métricas `wager_outbox_lag_seconds` (idade do evento pendente mais antigo) e `wager_outbox_pending` são atualizadas a cada ciclo.

### Eventos

Envelope: `eventId`, `eventType`, `aggregateType`, `aggregateId`, `correlationId`, `causationId` (id da transação), `occurredAt` (RFC 3339 UTC), `version` (1) e `data` tipado. Tipo e versão são fixados pelos construtores em `internal/domain/events`:

| Evento | Agregado | Gatilho | `data` |
| --- | --- | --- | --- |
| `WagerTransactionProcessed` | `WagerTransaction` | Conclusão, inclusive `LOSS` e `OPENING` | transactionId, origin, providerId*, externalTransactionId*, walletId, playerId, roundId*, gameId*, kind, money, balance, referenceTransactionId*, processedAt |
| `WagerTransactionRejected` | `WagerTransaction` | Rejeição definitiva | ids, kind, money, `failureCode`, rejectedAt |
| `WalletBalanceChanged` | `Wallet` | Alteração efetiva de saldo | walletId, transactionId, direction, money, balanceBefore, balanceAfter, walletVersion |
| `WagerTransactionPendingReference` | `WagerTransaction` | Primeira espera por referência | ids, referenceExternalTransactionId, kind, money, attempt, nextRetryAt |

(*) omitidos na origem interna. Roteamento: fila `wallet-events.fifo`, atributos de mensagem `eventType` e `eventId`; ordenação por `aggregateId` (`MessageGroupId`); DLQ `wallet-events-dlq.fifo` após 5 recepções. Consumidores devem ser idempotentes por `eventId`.

## Consumidor SQS

- Long-poll (`WaitTimeSeconds` = `SQS_WAIT_TIME`, `MaxNumberOfMessages` ≤ 10, `VisibilityTimeout` = `SQS_VISIBILITY_TIMEOUT`, 30s) com `SQS_CONSUMER_WORKERS` goroutines. O FIFO garante que mensagens do mesmo `MessageGroupId` não são entregues em paralelo; recomendamos **`MessageGroupId = walletId`** (ordem por carteira) e **`MessageDeduplicationId = messageId`** (ou `messageId` + tentativa, quando o produtor precisa forçar reentrega). A deduplicação do broker (5 min) é apenas otimização: a inbox e a idempotência no banco são as garantias.
- Remoção da mensagem **somente após o commit** do tratamento (inbox + domínio). Crash entre commit e `DeleteMessage` ⇒ reentrega absorvida pela inbox.
- Classificação (`classify`): sucesso ou rejeição de negócio commitada ⇒ `DeleteMessage`; erro **permanente** (envelope inválido, payload inválido, `idempotencyKey` ausente, `messageId` reutilizado com conteúdo diferente, conflito de idempotência, carteira inexistente) ⇒ cópia para `wager-transactions-dlq.fifo` com atributo `dlqReason` e remoção; erro **transitório** ⇒ `ChangeMessageVisibility` com backoff exponencial (2s·2^(n-1), máx. 5 min) e, após `maxReceiveCount` (5 no compose, 3 nos testes), redrive automático para a DLQ.
- `SIGTERM`: o hook de parada cancela o long-poll, deixa de buscar trabalho e espera as mensagens em voo até `WORKER_SHUTDOWN_TIMEOUT`; o que não terminar volta pela visibilidade e é absorvido pela inbox.
- Concorrência HTTP × SQS para a mesma operação é validada em teste: as duas entradas convergem no lock da carteira e na busca por chave sob o lock.

## Autenticação e autorização

IdP externo **Keycloak 26** (OIDC), realm `wager` importado do JSON em `deploy/keycloak`, com `client_credentials` para comunicação serviço-a-serviço (não há usuários finais nem senhas gerenciadas pelo serviço).

**Por que Keycloak, especificamente:**

1. **Reprodutibilidade a partir de um checkout limpo.** O item 15 exige que outra pessoa reproduza a solução com `docker compose up --build`, sem passos manuais nem contas externas. Keycloak roda inteiramente em container e importa realm, clients, roles e claim mappers de um único JSON versionado (`--import-realm` + `deploy/keycloak/wager-realm.json`) — as identidades de teste (`provider-a`, `provider-b`, `wallet-service`, `short-lived`, `no-role`) já existem no primeiro `up`. Um IdP gerenciado (Auth0, Okta, Cognito) exigiria conta na nuvem e segredos reais fora do repositório, quebrando a reprodutibilidade pedida.
2. **O modelo de permissões exigido é nativo no Keycloak.** O desafio exige que a identidade autenticada determine o `providerId` autorizado e que o chamador não possa escolher o próprio `providerId`. Keycloak resolve isso com um *hardcoded claim mapper* por client: o `providerId` é fixado no token pelo IdP, não enviado pelo cliente. Isso evita manter, dentro da aplicação, uma tabela `clientId → providerId` como fonte da verdade — o IdP já é essa fonte, que é o desenho sugerido pelo enunciado.

A aplicação valida JWTs com `go-oidc`: assinatura contra o JWKS (cache com rotação), `iss = OIDC_ISSUER`, `aud` contém `wager-api` (mapper de audience nos clients), `exp`/`nbf`. Separar issuer e URL do JWKS permite o container acessar o Keycloak pelo host interno enquanto os tokens carregam o issuer público.

Modelo de permissões: realm roles `provider` e `internal`, lidas de `realm_access.roles`; a identidade do provedor vem do claim **`providerId`** fixado por um *hardcoded claim mapper* em cada client — o cliente não pode escolher seu `providerId`. O corpo de `POST /wagering/transactions` precisa ter `providerId` igual ao do token (403 `PROVIDER_MISMATCH`), e consultas são filtradas pelo mesmo valor (404 por id interno para não revelar existência; 403 na rota do provedor). Operações de carteira (abrir, ler, ledger, reconciliar) exigem `internal`. Verificações acontecem antes de qualquer acesso ao banco, portanto sem efeitos financeiros.

Mensageria: o consumidor usa credenciais AWS (cadeia padrão do SDK) e a fila de entrada carrega uma *queue policy* que permite `SendMessage` apenas aos principals dos provedores e o consumo apenas ao role do serviço (`deploy/localstack/init-sqs.sh`). O LocalStack community armazena mas não aplica IAM; as validações de domínio (`providerId` do payload, carteira, jogador) permanecem no consumidor.

## Uber Fx e ciclo de vida

`fxapp.Options` compõe módulos `observability`, `postgres`, `auth`, `sqs`, `app`, `http`, `workers` com `fx.Provide`/`fx.Invoke`. Início: `config.Load` valida a configuração (erro aborta antes de abrir conexões); o pool faz `Ping`; as migrations rodam; o cliente SQS resolve as três filas (fila inexistente ⇒ start falha); o verificador OIDC é criado (JWKS carregado sob demanda e verificado no readiness).

Hooks de `fx.Lifecycle`: consumidor SQS, servidor HTTP, outbox publisher e pending worker registram `OnStart`/`OnStop`; o pool registra apenas `OnStop`. Como o Fx executa os `OnStop` em ordem inversa, a parada é: workers → HTTP (`Shutdown`, para de aceitar e espera em voo) → consumidor (cancela o polling e espera o em voo) → pool. Cada parada tem prazo (`WORKER_SHUTDOWN_TIMEOUT`, `HTTP_SHUTDOWN_TIMEOUT`) e é observável nos logs (`... stopped`). O `TestFxLifecycle` sobe a composição real em processo e verifica ordem, liberação da porta e falha rápida.

## Observabilidade

Logs JSON via `slog` com `correlationId`/`messageId` injetados pelo contexto; identificadores de negócio nos eventos relevantes; sem tokens, segredos ou payloads completos. Métricas Prometheus em `/metrics` (lista no README). Health: `/health/live` e `/health/ready` (PostgreSQL, SQS, JWKS). Tracing OpenTelemetry e dashboards não foram implementados.

## Limitações, interpretações e trabalho não concluído

- **`FAILED`** está modelado, protegido e exposto, mas nenhum caminho automático o produz: toda falha de infraestrutura ocorre antes do commit e é retentável. Um operador pode marcar `FAILED` manualmente se necessário.
- **`WIN` com referência ausente** aguarda como as reversões (interpretação: se o provedor informa a referência, ela deve existir). `WIN` sem referência credita imediatamente.
- **Mensagens com carteira inexistente** vão para a DLQ (não há como gravar a transação sem a FK); pelo HTTP respondem 422.
- **Deduplicação downstream**: eventos podem ser republicados após crash entre publicação e confirmação; o FIFO deduplica por 5 min e consumidores devem deduplicar por `eventId`.
- **IAM no LocalStack** não é aplicado (limitação do community); a policy está provisionada e documentada.
- **Partidas dobradas e tracing** (diferenciais opcionais) não foram implementados. O teste de carga está em `loadtest/`, com as ressalvas de ambiente descritas lá.
- O *fault injection* (`CRASH_POINT`) é um hook de teste no processo real; em produção a variável deve ficar vazia.
- O teste de falha transitória usa `statement_timeout=1ms` no DSN para simular indisponibilidade do PostgreSQL de forma portátil, em vez de derrubar o container.
