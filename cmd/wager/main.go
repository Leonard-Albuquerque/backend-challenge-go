// Command wager runs the wallet/wagering service.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/jamesmachome/backend-challenge-go/internal/config"
	"github.com/jamesmachome/backend-challenge-go/internal/fxapp"
)

func main() {
	healthcheck := flag.Bool("healthcheck", false, "probe the local readiness endpoint and exit")
	flag.Parse()

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid configuration:", err)
		os.Exit(2)
	}
	if *healthcheck {
		os.Exit(probe(cfg))
	}
	fxapp.New(cfg).Run()
}

func probe(cfg config.Config) int {
	addr := cfg.HTTPAddr
	if addr[0] == ':' {
		addr = "127.0.0.1" + addr
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/health/ready", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintln(os.Stderr, "not ready:", resp.Status)
		return 1
	}
	return 0
}
