//go:build integration

// Package integration runs the service against real PostgreSQL, Keycloak and
// LocalStack containers (see docker-compose.yml). Most scenarios drive three
// independent processes built from ./cmd/wager, each with its own memory and
// connection pool, sharing the database and per-run FIFO queues.
package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	sqsadapter "github.com/jamesmachome/backend-challenge-go/internal/adapters/sqs"
)

// Environment (defaults match docker-compose.yml port mappings).
var (
	dbURL       = envOr("IT_DATABASE_URL", "postgres://wager:wager@localhost:5432/wager?sslmode=disable")
	keycloakURL = envOr("IT_KEYCLOAK_URL", "http://localhost:8081")
	sqsEndpoint = envOr("IT_SQS_ENDPOINT", "http://localhost:4566")
	realm       = "wager"
)

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

// Shared state built once in TestMain.
var (
	binPath   string
	runID     string
	pool      *pgxpool.Pool
	sqsClient *sqsadapter.Client
	cluster   *Cluster
	tokens    = map[string]string{}
	tokensMu  sync.Mutex
	logDir    string
)

func TestMain(m *testing.M) {
	os.Exit(runMain(m))
}

func runMain(m *testing.M) int {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	b := make([]byte, 4)
	_, _ = rand.Read(b)
	runID = hex.EncodeToString(b)

	var err error
	logDir, err = os.MkdirTemp("", "wager-it-"+runID+"-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	fmt.Println("integration run", runID, "logs in", logDir)

	// Build the service binary with the race detector so the child processes
	// are also race-checked.
	binPath = filepath.Join(logDir, "wager")
	build := exec.Command("go", "build", "-race", "-o", binPath, "../../cmd/wager")
	build.Stdout, build.Stderr = os.Stdout, os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "build:", err)
		return 1
	}

	pool, err = pgxpool.New(ctx, dbURL)
	if err != nil {
		fmt.Fprintln(os.Stderr, "db:", err)
		return 1
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "db ping (is `make deps` running?):", err)
		return 1
	}
	sqsClient, err = sqsadapter.NewClient(ctx, sqsadapter.Config{Region: "us-east-1", Endpoint: sqsEndpoint, AccessKeyID: "test", SecretAccessKey: "test"})
	if err != nil {
		fmt.Fprintln(os.Stderr, "sqs:", err)
		return 1
	}

	cluster, err = StartCluster(ctx, "cluster", 3, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, "cluster:", err)
		return 1
	}
	code := m.Run()
	cluster.Stop()
	if code != 0 {
		fmt.Println("logs kept in", logDir)
	}
	return code
}

// --- queues ---

// Queues is a per-scenario set of FIFO queues.
type Queues struct {
	Wager, WagerDLQ, Events, EventsDLQ string
}

// CreateQueues provisions FIFO queues with redrive (maxReceiveCount=3).
func CreateQueues(ctx context.Context, name string) (Queues, error) {
	q := Queues{
		Wager: fmt.Sprintf("it-%s-%s-wager.fifo", runID, name), WagerDLQ: fmt.Sprintf("it-%s-%s-wager-dlq.fifo", runID, name),
		Events: fmt.Sprintf("it-%s-%s-events.fifo", runID, name), EventsDLQ: fmt.Sprintf("it-%s-%s-events-dlq.fifo", runID, name),
	}
	api := sqsClient.API()
	mk := func(n string, attrs map[string]string) (string, error) {
		attrs["FifoQueue"] = "true"
		attrs["ContentBasedDeduplication"] = "false"
		out, err := api.CreateQueue(ctx, &awssqs.CreateQueueInput{QueueName: aws.String(n), Attributes: attrs})
		if err != nil {
			return "", err
		}
		a, err := api.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{QueueUrl: out.QueueUrl, AttributeNames: []types.QueueAttributeName{"QueueArn"}})
		if err != nil {
			return "", err
		}
		return a.Attributes["QueueArn"], nil
	}
	dlqArn, err := mk(q.WagerDLQ, map[string]string{})
	if err != nil {
		return q, err
	}
	if _, err := mk(q.Wager, map[string]string{"VisibilityTimeout": "5", "RedrivePolicy": fmt.Sprintf(`{"deadLetterTargetArn":"%s","maxReceiveCount":"3"}`, dlqArn)}); err != nil {
		return q, err
	}
	evDlqArn, err := mk(q.EventsDLQ, map[string]string{})
	if err != nil {
		return q, err
	}
	if _, err := mk(q.Events, map[string]string{"VisibilityTimeout": "5", "RedrivePolicy": fmt.Sprintf(`{"deadLetterTargetArn":"%s","maxReceiveCount":"3"}`, evDlqArn)}); err != nil {
		return q, err
	}
	return q, nil
}

func queueURL(t testing.TB, name string) string {
	t.Helper()
	u, err := sqsClient.QueueURL(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

// sendRaw publishes a raw body to a FIFO queue.
func sendRaw(t testing.TB, queue, body, group, dedup string) {
	t.Helper()
	_, err := sqsClient.API().SendMessage(context.Background(), &awssqs.SendMessageInput{
		QueueUrl: aws.String(queueURL(t, queue)), MessageBody: aws.String(body), MessageGroupId: aws.String(group), MessageDeduplicationId: aws.String(dedup),
	})
	if err != nil {
		t.Fatal(err)
	}
}

// receiveAll drains up to max messages within the deadline, deleting them.
func receiveAll(t testing.TB, queue string, max int, wait time.Duration) []types.Message {
	t.Helper()
	url := queueURL(t, queue)
	deadline := time.Now().Add(wait)
	var out []types.Message
	for time.Now().Before(deadline) && len(out) < max {
		res, err := sqsClient.API().ReceiveMessage(context.Background(), &awssqs.ReceiveMessageInput{
			QueueUrl: aws.String(url), MaxNumberOfMessages: 10, WaitTimeSeconds: 1, MessageAttributeNames: []string{"All"},
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range res.Messages {
			out = append(out, m)
			_, _ = sqsClient.API().DeleteMessage(context.Background(), &awssqs.DeleteMessageInput{QueueUrl: aws.String(url), ReceiptHandle: m.ReceiptHandle})
		}
	}
	return out
}

func approxMessages(t testing.TB, queue string) int {
	t.Helper()
	a, err := sqsClient.API().GetQueueAttributes(context.Background(), &awssqs.GetQueueAttributesInput{QueueUrl: aws.String(queueURL(t, queue)),
		AttributeNames: []types.QueueAttributeName{"ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"}})
	if err != nil {
		t.Fatal(err)
	}
	n, _ := strconv.Atoi(a.Attributes["ApproximateNumberOfMessages"])
	nv, _ := strconv.Atoi(a.Attributes["ApproximateNumberOfMessagesNotVisible"])
	return n + nv
}

// --- processes ---

// Instance is one service process.
type Instance struct {
	ID      string
	BaseURL string
	Port    int
	cmd     *exec.Cmd
	logFile string
	done    chan error
}

// Cluster is a set of instances sharing queues.
type Cluster struct {
	Name      string
	Queues    Queues
	Instances []*Instance
	next      int
	mu        sync.Mutex
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// baseEnv is the tuned configuration for fast tests.
func baseEnv(q Queues) map[string]string {
	return map[string]string{
		"DATABASE_URL": dbURL, "DB_AUTO_MIGRATE": "true", "LOG_LEVEL": "info",
		"OIDC_ISSUER": keycloakURL + "/realms/" + realm, "OIDC_JWKS_URL": keycloakURL + "/realms/" + realm + "/protocol/openid-connect/certs", "OIDC_AUDIENCE": "wager-api",
		"AWS_REGION": "us-east-1", "AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test", "SQS_ENDPOINT": sqsEndpoint,
		"SQS_WAGER_QUEUE": q.Wager, "SQS_WAGER_DLQ": q.WagerDLQ, "SQS_EVENTS_QUEUE": q.Events,
		"SQS_WAIT_TIME": "1s", "SQS_VISIBILITY_TIMEOUT": "5s", "SQS_CONSUMER_WORKERS": "2",
		"OUTBOX_POLL_INTERVAL": "200ms", "OUTBOX_LEASE": "3s", "OUTBOX_BASE_BACKOFF": "200ms",
		"PENDING_POLL_INTERVAL": "200ms", "PENDING_BASE_BACKOFF": "200ms", "PENDING_MAX_BACKOFF": "1s", "PENDING_MAX_ATTEMPTS": "5",
		"HTTP_SHUTDOWN_TIMEOUT": "5s", "WORKER_SHUTDOWN_TIMEOUT": "5s",
	}
}

// StartInstance launches one process and waits for readiness (unless wait is false).
func StartInstance(ctx context.Context, id string, q Queues, overrides map[string]string, wait bool) (*Instance, error) {
	port, err := freePort()
	if err != nil {
		return nil, err
	}
	env := baseEnv(q)
	env["INSTANCE_ID"] = id
	env["HTTP_ADDR"] = fmt.Sprintf("127.0.0.1:%d", port)
	for k, v := range overrides {
		env[k] = v
	}
	cmd := exec.Command(binPath)
	cmd.Env = append(os.Environ(), "GORACE=halt_on_error=1")
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	logFile := filepath.Join(logDir, id+".log")
	f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	cmd.Stdout, cmd.Stderr = f, f
	if err := cmd.Start(); err != nil {
		f.Close()
		return nil, err
	}
	inst := &Instance{ID: id, BaseURL: fmt.Sprintf("http://127.0.0.1:%d", port), Port: port, cmd: cmd, logFile: logFile, done: make(chan error, 1)}
	go func() { inst.done <- cmd.Wait(); f.Close() }()
	if !wait {
		return inst, nil
	}
	if err := inst.WaitReady(ctx); err != nil {
		inst.Kill()
		return nil, fmt.Errorf("instance %s not ready: %w (see %s)", id, err, logFile)
	}
	return inst, nil
}

// WaitReady polls /health/ready.
func (i *Instance) WaitReady(ctx context.Context) error {
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-i.done:
			return fmt.Errorf("process exited: %v", err)
		default:
		}
		resp, err := http.Get(i.BaseURL + "/health/ready")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	return fmt.Errorf("timeout")
}

// WaitExit waits for the process to end and returns its exit code.
func (i *Instance) WaitExit(timeout time.Duration) (int, error) {
	select {
	case err := <-i.done:
		if err == nil {
			return 0, nil
		}
		var ee *exec.ExitError
		if errorsAs(err, &ee) {
			return ee.ExitCode(), nil
		}
		return -1, err
	case <-time.After(timeout):
		return -1, fmt.Errorf("instance %s did not exit within %s", i.ID, timeout)
	}
}

func errorsAs(err error, target **exec.ExitError) bool {
	ee, ok := err.(*exec.ExitError)
	if ok {
		*target = ee
	}
	return ok
}

// Terminate sends SIGTERM.
func (i *Instance) Terminate() { _ = i.cmd.Process.Signal(syscall.SIGTERM) }

// Pause/Resume freeze the process (SIGSTOP/SIGCONT) so a scenario can take
// the shared outbox table for itself without stopping the cluster.
func (i *Instance) Pause()  { _ = i.cmd.Process.Signal(syscall.SIGSTOP) }
func (i *Instance) Resume() { _ = i.cmd.Process.Signal(syscall.SIGCONT) }

// Kill sends SIGKILL.
func (i *Instance) Kill() { _ = i.cmd.Process.Kill(); _, _ = i.WaitExit(5 * time.Second) }

// Logs returns the captured output.
func (i *Instance) Logs() string {
	b, _ := os.ReadFile(i.logFile)
	return string(b)
}

// StartCluster creates queues and n instances.
func StartCluster(ctx context.Context, name string, n int, overrides map[string]string) (*Cluster, error) {
	q, err := CreateQueues(ctx, name)
	if err != nil {
		return nil, err
	}
	c := &Cluster{Name: name, Queues: q}
	for i := 1; i <= n; i++ {
		inst, err := StartInstance(ctx, fmt.Sprintf("%s-%d", name, i), q, overrides, true)
		if err != nil {
			c.Stop()
			return nil, err
		}
		c.Instances = append(c.Instances, inst)
	}
	return c, nil
}

// Next returns the next instance round-robin.
func (c *Cluster) Next() *Instance {
	c.mu.Lock()
	defer c.mu.Unlock()
	i := c.Instances[c.next%len(c.Instances)]
	c.next++
	return i
}

// Stop terminates every instance.
func (c *Cluster) Stop() {
	for _, i := range c.Instances {
		i.Terminate()
	}
	for _, i := range c.Instances {
		_, _ = i.WaitExit(15 * time.Second)
	}
}

// --- auth ---

// Token fetches a client_credentials token from Keycloak (cached per client).
func Token(t testing.TB, clientID string) string {
	t.Helper()
	tokensMu.Lock()
	defer tokensMu.Unlock()
	if tok, ok := tokens[clientID]; ok {
		return tok
	}
	tok, err := fetchToken(clientID)
	if err != nil {
		t.Fatal(err)
	}
	tokens[clientID] = tok
	return tok
}

func fetchToken(clientID string) (string, error) {
	form := url.Values{"grant_type": {"client_credentials"}, "client_id": {clientID}, "client_secret": {clientID + "-secret"}}
	resp, err := http.PostForm(keycloakURL+"/realms/"+realm+"/protocol/openid-connect/token", form)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var body struct {
		AccessToken string `json:"access_token"`
		Error       string `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", err
	}
	if body.AccessToken == "" {
		return "", fmt.Errorf("token for %s: %s (%s)", clientID, body.Error, resp.Status)
	}
	return body.AccessToken, nil
}

const (
	clientInternal  = "wallet-service"
	clientProviderA = "provider-a"
	clientProviderB = "provider-b"
)

// --- HTTP helpers ---

type response struct {
	Status int
	Body   map[string]any
	Raw    []byte
	Header http.Header
}

func (r response) str(k string) string {
	v, _ := r.Body[k].(string)
	return v
}

func (r response) money(k string) string {
	m, _ := r.Body[k].(map[string]any)
	a, _ := m["amount"].(string)
	return a
}

func (r response) errCode() string {
	e, _ := r.Body["error"].(map[string]any)
	c, _ := e["code"].(string)
	return c
}

var httpClient = &http.Client{Timeout: 30 * time.Second}

func do(t testing.TB, inst *Instance, method, path, token string, body any, headers map[string]string) response {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if s, ok := body.(string); ok {
			buf.WriteString(s)
		} else if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	req, err := http.NewRequest(method, inst.BaseURL+path, &buf)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s on %s: %v", method, path, inst.ID, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	out := response{Status: resp.StatusCode, Raw: raw, Header: resp.Header}
	_ = json.Unmarshal(raw, &out.Body)
	return out
}

type txBody struct {
	ProviderID                     string            `json:"providerId"`
	ExternalTransactionID          string            `json:"externalTransactionId"`
	PlayerID                       string            `json:"playerId"`
	WalletID                       string            `json:"walletId"`
	RoundID                        string            `json:"roundId"`
	GameID                         string            `json:"gameId"`
	Kind                           string            `json:"kind"`
	Money                          map[string]string `json:"money"`
	ReferenceExternalTransactionID string            `json:"referenceExternalTransactionId,omitempty"`
}

// wallet opened for a test.
type testWallet struct {
	ID       string
	PlayerID string
}

func openWallet(t testing.TB, inst *Instance, amount string) testWallet {
	t.Helper()
	player := uuid.NewString()
	r := do(t, inst, http.MethodPost, "/wallets", Token(t, clientInternal), map[string]any{"playerId": player, "initialBalance": map[string]string{"amount": amount, "currency": "BRL"}}, nil)
	if r.Status != http.StatusCreated {
		t.Fatalf("open wallet: %d %s", r.Status, r.Raw)
	}
	return testWallet{ID: r.str("id"), PlayerID: player}
}

func bet(w testWallet, ext, amount string) txBody {
	return txBody{ProviderID: "provider-a", ExternalTransactionID: ext, PlayerID: w.PlayerID, WalletID: w.ID, RoundID: "round-" + ext, GameID: "fortune-chimp", Kind: "BET", Money: map[string]string{"amount": amount, "currency": "BRL"}}
}

func postTx(t testing.TB, inst *Instance, token string, b txBody) response {
	t.Helper()
	return do(t, inst, http.MethodPost, "/wagering/transactions", token, b, map[string]string{"Idempotency-Key": b.ProviderID + ":" + b.ExternalTransactionID})
}

func getWallet(t testing.TB, inst *Instance, id string) response {
	t.Helper()
	return do(t, inst, http.MethodGet, "/wallets/"+id, Token(t, clientInternal), nil, nil)
}

func reconcile(t testing.TB, inst *Instance, id string) response {
	t.Helper()
	r := do(t, inst, http.MethodPost, "/wallets/"+id+"/reconciliation", Token(t, clientInternal), nil, nil)
	if r.Status != http.StatusOK {
		t.Fatalf("reconcile: %d %s", r.Status, r.Raw)
	}
	return r
}

// --- DB helpers ---

func countLedger(t testing.TB, walletID string, direction string) int {
	t.Helper()
	var n int
	q := `SELECT COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = $1`
	args := []any{walletID}
	if direction != "" {
		q += ` AND direction = $2`
		args = append(args, direction)
	}
	if err := pool.QueryRow(context.Background(), q, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func txStatus(t testing.TB, providerID, ext string) (status, failure string) {
	t.Helper()
	var f *string
	err := pool.QueryRow(context.Background(), `SELECT status, failure_code FROM wager_transactions WHERE provider_id = $1 AND external_transaction_id = $2`, providerID, ext).Scan(&status, &f)
	if err != nil {
		return "", ""
	}
	if f != nil {
		failure = *f
	}
	return status, failure
}

// eventually polls until fn returns true.
func eventually(t testing.TB, timeout time.Duration, fn func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s: %s", timeout, msg)
}

// sqsEnvelope builds a wager-transactions message body.
func sqsEnvelope(messageID string, b txBody) string {
	data := map[string]any{
		"providerId": b.ProviderID, "externalTransactionId": b.ExternalTransactionID, "idempotencyKey": b.ProviderID + ":" + b.ExternalTransactionID,
		"playerId": b.PlayerID, "walletId": b.WalletID, "roundId": b.RoundID, "gameId": b.GameID, "kind": b.Kind, "money": b.Money,
	}
	if b.ReferenceExternalTransactionID != "" {
		data["referenceExternalTransactionId"] = b.ReferenceExternalTransactionID
	}
	env := map[string]any{"messageId": messageID, "type": "WagerTransactionRequested", "occurredAt": time.Now().UTC().Format(time.RFC3339Nano), "data": data}
	raw, _ := json.Marshal(env)
	return string(raw)
}

func contains(s, sub string) bool { return strings.Contains(s, sub) }
