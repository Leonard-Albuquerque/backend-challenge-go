# Load test summary

| Metric | Value |
| --- | ---: |
| Test duration | 60.9 s |
| POST /wagering/transactions | 14284 (234.5 req/s) |
| HTTP requests total | 14875 (244.2 req/s) |
| PROCESSED | 14284 |
| REJECTED (business) | 0 |
| Idempotent replays | 892 |
| PENDING_REFERENCE | 0 |
| 409 conflicts | 0 |
| 503 unavailable | 0 |
| 5xx | 0 |
| http_req_failed | 0.000 % |
| POST latency p50 / p95 / p99 (ms) | 2.9 / 180.0 / 591.2 |
| POST latency avg / max (ms) | 30.4 / 2097.5 |
| Outbox lag p50 / p95 / max (s) | 0.23 / 0.67 / 2.17 |
| Outbox pending p95 / max | 276 / 732 |

Per scenario POST latency (ms):

| Scenario | p50 | p95 | p99 |
| --- | ---: | ---: | ---: |
| steady | 2.8 | 172.9 | 522.4 |
| duplicates (new + replays) | 2.7 | 148.7 | 521.9 |
| replays only | 1.1 | 40.6 | 303.8 |
| hot_wallet | 3.1 | 295.2 | 1112.0 |
