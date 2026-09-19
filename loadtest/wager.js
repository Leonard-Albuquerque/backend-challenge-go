// k6 load test for the wager service.
//
// Scenarios:
//   steady      constant arrival rate of BET/WIN/LOSS spread over WALLETS wallets
//   duplicates  replays (same Idempotency-Key) mixed with new bets
//   hot_wallet  many VUs hammering HOT_WALLETS wallets (lock contention, rejections)
//   outbox      scrapes /metrics every second to record outbox lag/pending
//
// Every request picks one of BASE_URLS at random, so the three instances share
// the load without a load balancer.
import http from 'k6/http';
import { check } from 'k6';
import { Counter, Trend, Rate } from 'k6/metrics';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

const BASE_URLS = (__ENV.BASE_URLS || 'http://localhost:8080,http://localhost:8082,http://localhost:8083').split(',');
const KEYCLOAK = __ENV.KEYCLOAK_URL || 'http://localhost:8081';
const WALLETS = Number(__ENV.WALLETS || 200);
const HOT_WALLETS = Number(__ENV.HOT_WALLETS || 3);
const STEADY_RPS = Number(__ENV.STEADY_RPS || 150);
const DURATION = __ENV.DURATION || '60s';
const HOT_VUS = Number(__ENV.HOT_VUS || 30);
const HOT_RPS = Number(__ENV.HOT_RPS || 40);
const DUP_RATIO = Number(__ENV.DUP_RATIO || 0.3);

// Custom metrics required by the challenge write-up.
const processed = new Counter('wager_processed');
const rejected = new Counter('wager_rejected');
const replays = new Counter('wager_replays');
const pending = new Counter('wager_pending_reference');
const conflicts = new Counter('wager_conflicts_409');
const unavailable = new Counter('wager_unavailable_503');
const serverErrors = new Counter('wager_server_errors_5xx');
const businessErrorRate = new Rate('wager_unexpected_status');
const outboxLag = new Trend('outbox_lag_seconds');
const outboxPending = new Trend('outbox_pending');
const postDuration = new Trend('post_transaction_duration', true);

export const options = {
  scenarios: {
    steady: {
      executor: 'constant-arrival-rate', exec: 'steady',
      rate: STEADY_RPS, timeUnit: '1s', duration: DURATION, preAllocatedVUs: 50, maxVUs: 300,
    },
    duplicates: {
      executor: 'constant-arrival-rate', exec: 'duplicates',
      rate: Math.max(10, Math.round(STEADY_RPS / 3)), timeUnit: '1s', duration: DURATION, preAllocatedVUs: 20, maxVUs: 100,
    },
    hot_wallet: {
      executor: 'constant-arrival-rate', exec: 'hotWallet',
      rate: HOT_RPS, timeUnit: '1s', duration: DURATION, preAllocatedVUs: HOT_VUS, maxVUs: HOT_VUS * 3,
    },
    outbox: {
      executor: 'constant-arrival-rate', exec: 'scrapeOutbox', rate: 1, timeUnit: '1s', duration: DURATION, preAllocatedVUs: 1,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],                 // transport/5xx failures
    wager_server_errors_5xx: ['count==0'],
    'post_transaction_duration{scenario:steady}': ['p(95)<500'],
    // Informational: registering thresholds makes the per-scenario
    // sub-metrics appear in the summary; the bounds are deliberately loose.
    'post_transaction_duration{scenario:duplicates}': ['p(99)<60000'],
    'post_transaction_duration{scenario:hot_wallet}': ['p(99)<60000'],
    'post_transaction_duration{kind:replay}': ['p(99)<60000'],
    wager_unexpected_status: ['rate<0.001'],
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(50)', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

function token(clientId) {
  const res = http.post(`${KEYCLOAK}/realms/wager/protocol/openid-connect/token`,
    { grant_type: 'client_credentials', client_id: clientId, client_secret: `${clientId}-secret` });
  if (res.status !== 200) throw new Error(`token ${clientId}: ${res.status} ${res.body}`);
  return res.json('access_token');
}

function pick(arr) { return arr[Math.floor(Math.random() * arr.length)]; }

export function setup() {
  const internal = token('wallet-service');
  const provider = token('provider-a');
  const wallets = [];
  for (let i = 0; i < WALLETS; i++) {
    const playerId = uuidv4();
    const res = http.post(`${pick(BASE_URLS)}/wallets`, JSON.stringify({ playerId, initialBalance: { amount: '100000.00', currency: 'BRL' } }),
      { headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${internal}` } });
    if (res.status !== 201) throw new Error(`open wallet: ${res.status} ${res.body}`);
    wallets.push({ id: res.json('id'), playerId });
  }
  const hot = [];
  for (let i = 0; i < HOT_WALLETS; i++) {
    const playerId = uuidv4();
    const res = http.post(`${pick(BASE_URLS)}/wallets`, JSON.stringify({ playerId, initialBalance: { amount: '1000.00', currency: 'BRL' } }),
      { headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${internal}` } });
    hot.push({ id: res.json('id'), playerId });
  }
  return { provider, internal, wallets, hot, runId: uuidv4().slice(0, 8) };
}

function post(data, body, key, tags) {
  const res = http.post(`${pick(BASE_URLS)}/wagering/transactions`, JSON.stringify(body), {
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${data.provider}`, 'Idempotency-Key': key },
    tags,
  });
  postDuration.add(res.timings.duration, tags);
  let status = '';
  try { status = res.json('status') || ''; } catch (_) { /* non-JSON */ }
  if (res.status === 200 || res.status === 422 || res.status === 202) {
    if (res.json('idempotentReplay')) replays.add(1);
    if (status === 'PROCESSED') processed.add(1);
    else if (status === 'REJECTED') rejected.add(1);
    else if (status === 'PENDING_REFERENCE') pending.add(1);
    businessErrorRate.add(0);
  } else if (res.status === 409) {
    conflicts.add(1); businessErrorRate.add(0);
  } else if (res.status === 503) {
    unavailable.add(1); businessErrorRate.add(1);
  } else {
    if (res.status >= 500) serverErrors.add(1);
    businessErrorRate.add(1);
  }
  check(res, { 'expected status': (r) => [200, 202, 422, 409].includes(r.status) });
  return res;
}

function body(w, ext, kind, amount) {
  return { providerId: 'provider-a', externalTransactionId: ext, playerId: w.playerId, walletId: w.id,
    roundId: `round-${ext}`, gameId: 'fortune-chimp', kind, money: { amount, currency: 'BRL' } };
}

export function steady(data) {
  const w = pick(data.wallets);
  const ext = `${data.runId}-s-${__VU}-${__ITER}`;
  const r = Math.random();
  let kind = 'BET', amount = '5.00';
  if (r < 0.15) { kind = 'WIN'; amount = '4.00'; } else if (r < 0.25) { kind = 'LOSS'; amount = '0.00'; }
  post(data, body(w, ext, kind, amount), `provider-a:${ext}`, { scenario: 'steady', kind });
}

// Keeps a per-VU ring of already-sent operations and replays one of them
// DUP_RATIO of the time.
const sent = [];
export function duplicates(data) {
  if (sent.length > 0 && Math.random() < DUP_RATIO) {
    const s = pick(sent);
    post(data, s.body, s.key, { scenario: 'duplicates', kind: 'replay' });
    return;
  }
  const w = pick(data.wallets);
  const ext = `${data.runId}-d-${__VU}-${__ITER}`;
  const b = body(w, ext, 'BET', '1.00');
  const key = `provider-a:${ext}`;
  post(data, b, key, { scenario: 'duplicates', kind: 'BET' });
  sent.push({ body: b, key });
  if (sent.length > 50) sent.shift();
}

export function hotWallet(data) {
  const w = pick(data.hot);
  const ext = `${data.runId}-h-${__VU}-${__ITER}`;
  // Alternate credits and debits so the wallet neither drains nor grows unbounded.
  const kind = __ITER % 2 === 0 ? 'BET' : 'WIN';
  post(data, body(w, ext, kind, '20.00'), `provider-a:${ext}`, { scenario: 'hot_wallet', kind });
}

export function scrapeOutbox() {
  for (const base of BASE_URLS) {
    const res = http.get(`${base}/metrics`, { tags: { scenario: 'outbox' } });
    if (res.status !== 200) continue;
    const lag = /^wager_outbox_lag_seconds (\S+)/m.exec(res.body);
    const pend = /^wager_outbox_pending (\S+)/m.exec(res.body);
    if (lag) outboxLag.add(Number(lag[1]), { instance: base });
    if (pend) outboxPending.add(Number(pend[1]), { instance: base });
  }
}

export function teardown(data) {
  // Reconcile every wallet used: the load test must leave the books balanced.
  let inconsistent = 0;
  for (const w of data.wallets.concat(data.hot)) {
    const res = http.post(`${pick(BASE_URLS)}/wallets/${w.id}/reconciliation`, null,
      { headers: { Authorization: `Bearer ${data.internal}` } });
    if (res.status !== 200 || res.json('consistent') !== true) inconsistent++;
  }
  if (inconsistent > 0) throw new Error(`${inconsistent} wallets inconsistent after load`);
  console.log(`reconciliation: ${data.wallets.length + data.hot.length} wallets consistent`);
}

export function handleSummary(data) {
  const m = data.metrics;
  const t = (name) => (m[name] && m[name].values) || {};
  const dur = t('post_transaction_duration');
  const totalPosts = ['wager_processed', 'wager_rejected', 'wager_pending_reference', 'wager_conflicts_409', 'wager_unavailable_503', 'wager_server_errors_5xx']
    .reduce((acc, n) => acc + (t(n).count || 0), 0);
  const seconds = (data.state.testRunDurationMs || 1) / 1000;
  const md = `# Load test summary

| Metric | Value |
| --- | ---: |
| Test duration | ${seconds.toFixed(1)} s |
| POST /wagering/transactions | ${totalPosts} (${(totalPosts / seconds).toFixed(1)} req/s) |
| HTTP requests total | ${t('http_reqs').count} (${(t('http_reqs').rate || 0).toFixed(1)} req/s) |
| PROCESSED | ${t('wager_processed').count || 0} |
| REJECTED (business) | ${t('wager_rejected').count || 0} |
| Idempotent replays | ${t('wager_replays').count || 0} |
| PENDING_REFERENCE | ${t('wager_pending_reference').count || 0} |
| 409 conflicts | ${t('wager_conflicts_409').count || 0} |
| 503 unavailable | ${t('wager_unavailable_503').count || 0} |
| 5xx | ${t('wager_server_errors_5xx').count || 0} |
| http_req_failed | ${((t('http_req_failed').rate || 0) * 100).toFixed(3)} % |
| POST latency p50 / p95 / p99 (ms) | ${(dur['p(50)'] || 0).toFixed(1)} / ${(dur['p(95)'] || 0).toFixed(1)} / ${(dur['p(99)'] || 0).toFixed(1)} |
| POST latency avg / max (ms) | ${(dur.avg || 0).toFixed(1)} / ${(dur.max || 0).toFixed(1)} |
| Outbox lag p50 / p95 / max (s) | ${(t('outbox_lag_seconds')['p(50)'] || 0).toFixed(2)} / ${(t('outbox_lag_seconds')['p(95)'] || 0).toFixed(2)} / ${(t('outbox_lag_seconds').max || 0).toFixed(2)} |
| Outbox pending p95 / max | ${(t('outbox_pending')['p(95)'] || 0).toFixed(0)} / ${(t('outbox_pending').max || 0).toFixed(0)} |

Per scenario POST latency (ms):

| Scenario | p50 | p95 | p99 |
| --- | ---: | ---: | ---: |
${[['steady', 'scenario:steady'], ['duplicates (new + replays)', 'scenario:duplicates'], ['replays only', 'kind:replay'], ['hot_wallet', 'scenario:hot_wallet']].map(([label, tag]) => {
    const v = t(`post_transaction_duration{${tag}}`);
    return `| ${label} | ${(v['p(50)'] || 0).toFixed(1)} | ${(v['p(95)'] || 0).toFixed(1)} | ${(v['p(99)'] || 0).toFixed(1)} |`;
  }).join('\n')}
`;
  return {
    stdout: textSummary(data),
    'results/summary.json': JSON.stringify(data, null, 2),
    'results/summary.md': md,
  };
}

import { textSummary } from 'https://jslib.k6.io/k6-summary/0.1.0/index.js';
