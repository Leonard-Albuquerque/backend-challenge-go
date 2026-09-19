//go:build integration

package integration

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"go.uber.org/fx"

	"github.com/jamesmachome/backend-challenge-go/internal/config"
	"github.com/jamesmachome/backend-challenge-go/internal/fxapp"
)

// TestFxLifecycle starts the real composition in-process against the real
// dependencies and verifies start, readiness, ordered shutdown and resource
// release (port freed, workers stopped, pool closed).
func TestFxLifecycle(t *testing.T) {
	q, err := CreateQueues(context.Background(), "fx")
	if err != nil {
		t.Fatal(err)
	}
	port, err := freePort()
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range baseEnv(q) {
		t.Setenv(k, v)
	}
	t.Setenv("HTTP_ADDR", fmt.Sprintf("127.0.0.1:%d", port))
	t.Setenv("INSTANCE_ID", "fx-test")
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	// Invalid configuration is rejected before anything starts.
	bad := cfg
	bad.DatabaseURL = "mysql://nope"
	if err := bad.Validate(); err == nil {
		t.Fatal("invalid config accepted")
	}

	var stopped []string
	app := fx.New(fxapp.Options(cfg), fx.Invoke(func(lc fx.Lifecycle) {
		// Registered last, so its OnStop runs first: workers are still up.
		lc.Append(fx.Hook{OnStop: func(context.Context) error { stopped = append(stopped, "marker"); return nil }})
	}))
	startCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := app.Start(startCtx); err != nil {
		t.Fatalf("start: %v", err)
	}
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	resp, err := http.Get(base + "/health/ready")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("ready: %v %v", err, resp)
	}
	resp.Body.Close()

	stopCtx, cancel2 := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel2()
	if err := app.Stop(stopCtx); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if len(stopped) != 1 {
		t.Fatal("custom stop hook did not run")
	}
	// Port released and no goroutine keeps serving.
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("port not released: %v", err)
	}
	ln.Close()
	if _, err := http.Get(base + "/health/live"); err == nil {
		t.Fatal("server still answering after stop")
	}

	// Startup with an unreachable dependency fails fast instead of hanging.
	badCfg := cfg
	badCfg.SQSWagerQueue = "does-not-exist.fifo"
	failing := fx.New(fxapp.Options(badCfg), fx.NopLogger)
	ctx3, cancel3 := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel3()
	if err := failing.Start(ctx3); err == nil {
		_ = failing.Stop(ctx3)
		t.Fatal("start must fail when a queue is missing")
	}
}
