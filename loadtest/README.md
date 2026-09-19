# Teste de carga (k6)

Objetivo: medir throughput, latência (p50/p95/p99), erros, conflitos e atraso da outbox do serviço rodando em **três instâncias**, com carga que exercita as garantias do sistema (idempotência sob replay, contenção por carteira) e não apenas o caminho feliz. Não há meta de RPS no desafio; o valor está na medição reproduzível e nas ressalvas explícitas.

## Comando reproduzível

```bash
docker compose up --build -d          # stack completa: postgres, keycloak, localstack, app1..3
make load                              # = docker compose --profile load run --rm k6
```

O k6 (`grafana/k6:0.54.0`) roda como serviço do Compose na mesma rede, executa [wager.js](wager.js) e grava `results/summary.json` (bruto) e `results/summary.md` (tabela). Parâmetros via variáveis de ambiente:

| Variável | Padrão | Significado |
| --- | ---: | --- |
| `DURATION` | `60s` | Duração de todos os cenários |
| `STEADY_RPS` | `150` | Taxa de chegada do cenário `steady` |
| `WALLETS` | `200` | Carteiras do pool (criadas no `setup`, saldo 100000.00) |
| `HOT_WALLETS` / `HOT_RPS` | `3` / `40` | Carteiras e taxa do cenário de contenção |
| `HOT_VUS` | `30` | VUs pré-alocados para `hot_wallet` |

Exemplo: `DURATION=120s STEADY_RPS=300 make load`. Para rodar o k6 no host: `k6 run -e BASE_URLS=http://localhost:8080,http://localhost:8082,http://localhost:8083 -e KEYCLOAK_URL=http://localhost:8081 loadtest/wager.js`.

## Ambiente da execução registrada

| Item | Valor |
| --- | --- |
| Máquina | MacBook Apple M1, 8 núcleos, 8 GB RAM |
| Docker Desktop | 29.7.2, VM com 8 vCPU e 4 GB RAM (compartilhada por **todos** os containers e pelo k6) |
| Serviço | 3 instâncias (imagem distroless, Go 1.27, sem `-race`), `DB_MAX_CONNS=20` cada |
| PostgreSQL | 16-alpine, `max_connections=200`, sem tuning |
| SQS | LocalStack 3 (community) |
| IdP | Keycloak 26 (tokens obtidos uma vez no `setup`) |
| k6 | 0.54.0, no mesmo host |
| Data | 2026-09-19 |

## Metodologia

Quatro cenários concorrentes durante 60 s, cada requisição enviada para uma das três instâncias sorteada aleatoriamente:

| Cenário | Executor | Carga | O que mede |
| --- | --- | --- | --- |
| `steady` | `constant-arrival-rate` | 150 req/s: 75% `BET` 5.00, 15% `WIN` 4.00, 10% `LOSS` 0.00, sobre 200 carteiras | Throughput e latência sem contenção relevante |
| `duplicates` | `constant-arrival-rate` | 50 req/s; 30% são reenvios de operações já enviadas (mesma `Idempotency-Key` e payload) | Custo do replay idempotente e ausência de dupla movimentação |
| `hot_wallet` | `constant-arrival-rate` | 40 req/s alternando `BET`/`WIN` de 20.00 em apenas 3 carteiras | Contenção do `FOR UPDATE` por carteira, rejeições, conflitos 409 |
| `outbox` | 1 req/s | Scrape de `/metrics` das 3 instâncias | `wager_outbox_lag_seconds` e `wager_outbox_pending` durante a carga |

Cada resposta de `POST /wagering/transactions` alimenta contadores próprios (`PROCESSED`, `REJECTED`, replays, `PENDING_REFERENCE`, 409, 503, 5xx) e a Trend `post_transaction_duration` com tags de cenário e tipo. Ao final, o `teardown` chama `POST /wallets/:id/reconciliation` em **todas** as 203 carteiras e falha o teste se alguma divergir: o teste de carga também é um teste de integridade financeira.

Thresholds (falham a execução): `http_req_failed < 1%`, zero 5xx, `wager_unexpected_status < 0.1%` (503 conta como inesperado), `p95 < 500 ms` no cenário `steady`.

O `constant-arrival-rate` foi escolhido em vez de VUs fixos porque mede o sistema sob uma taxa alvo (open model): se a latência sobe, o k6 aloca mais VUs em vez de reduzir a carga; `dropped_iterations` indica quando nem isso foi suficiente.

## Resultados (execução de 2026-09-19, `results/summary-2026-09-19.md`)

| Métrica | Valor |
| --- | ---: |
| Duração | 60.9 s |
| `POST /wagering/transactions` | 14 284 (234.5 req/s) |
| Requisições HTTP totais | 14 875 (244.2 req/s) |
| `PROCESSED` | 14 284 |
| `REJECTED` (negócio) | 0 |
| Replays idempotentes | 892 |
| `PENDING_REFERENCE` | 0 |
| 409 (conflitos) | 0 |
| 503 / 5xx | 0 / 0 |
| `http_req_failed` | 0.000 % |
| Latência POST p50 / p95 / p99 | 2.9 / 180.0 / 591.2 ms |
| Latência POST média / máx | 30.4 / 2097.5 ms |
| Atraso da outbox p50 / p95 / máx | 0.23 / 0.67 / 2.17 s |
| Outbox pendente p95 / máx | 276 / 732 eventos |
| Reconciliação | 203/203 carteiras consistentes |
| Thresholds | todos aprovados |

Por cenário (ms):

| Cenário | p50 | p95 | p99 |
| --- | ---: | ---: | ---: |
| `steady` | 2.8 | 172.9 | 522.4 |
| `duplicates` (novas + replays) | 2.7 | 148.7 | 521.9 |
| somente replays | 1.1 | 40.6 | 303.8 |
| `hot_wallet` (3 carteiras) | 3.1 | 295.2 | 1112.0 |

### Leitura dos números

- **Mediana de ~3 ms** com três instâncias e commit síncrono de transação + ledger + saldo + outbox: o caminho crítico é uma transação SQL curta com um lock de linha.
- **Cauda longa (p95/p99)** é dominada pelo ambiente: gerador de carga, três apps, Postgres, LocalStack e Keycloak dividem 8 vCPU/4 GB na VM do Docker Desktop. Entre execuções idênticas o p95 do `steady` variou de ~95 ms a ~180 ms com a mesma mediana; trate a cauda como ordem de grandeza, não como número absoluto.
- **Replays custam ~1 ms** (mediana): dois `SELECT`s por índice único, sem lock de carteira.
- **`hot_wallet`** mostra o preço da serialização por carteira: com 40 req/s em 3 carteiras o p99 passa de 1 s enquanto a mediana continua em 3 ms — o lock funciona como fila. Não houve 409 nem rejeição porque a alternância `BET`/`WIN` mantém saldo e o re-check sob lock absorve duplicatas sem violar constraints.
- **Outbox**: a versão inicial do publisher (um `SendMessage` por evento) acumulou lag de 5–13 s e 11 k eventos pendentes em uma execução de fumaça a 50 req/s. Trocar para `SendMessageBatch` (10 eventos por chamada, confirmação em lote) levou o p95 do lag a **0.67 s** com ~470 eventos/s produzidos. Esse foi o principal ajuste motivado pelo teste de carga.
- **Zero erros de transporte, zero 5xx, zero 503** e reconciliação limpa em todas as carteiras: nenhuma movimentação duplicada ou perdida sob carga concorrente entre três processos.

## Limitações e ressalvas

- Gerador de carga e sistema sob teste no mesmo laptop: números absolutos não são comparáveis a um ambiente de produção; o objetivo é relativo e de regressão.
- LocalStack é Python single-process; o atraso da outbox medido inclui a latência do próprio LocalStack, que é maior que a do SQS real.
- Os tokens são obtidos uma vez no `setup` (validade 300 s). Para execuções acima de ~4 min, reduza o tempo ou renove o token por VU.
- O cenário `duplicates` reenvia operações recentes do próprio VU (janela de 50); não cobre replays de operações antigas, que teriam o mesmo custo (índice único).
- Não foi feito teste de saturação (descobrir o RPS máximo) nem de longa duração (soak). Para saturação: aumentar `STEADY_RPS` até `dropped_iterations` ou 503 aparecerem, observando `wager_concurrency_conflicts_total` e o lag da outbox.
