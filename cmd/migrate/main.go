// Command migrate applies or reverts the versioned SQL migrations.
//
//	go run ./cmd/migrate up
//	go run ./cmd/migrate down
//	go run ./cmd/migrate steps -1
//	go run ./cmd/migrate version
package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"

	"github.com/jamesmachome/backend-challenge-go/internal/adapters/postgres"
)

func main() {
	dsn := flag.String("dsn", os.Getenv("DATABASE_URL"), "PostgreSQL DSN (defaults to DATABASE_URL)")
	flag.Parse()
	if *dsn == "" {
		fmt.Fprintln(os.Stderr, "DATABASE_URL or -dsn is required")
		os.Exit(2)
	}
	if flag.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "usage: migrate [-dsn ...] up|down|steps N|version")
		os.Exit(2)
	}
	m, err := postgres.NewMigrator(*dsn)
	if err != nil {
		fail(err)
	}
	defer m.Close()
	switch flag.Arg(0) {
	case "up":
		err = m.Up()
	case "down":
		err = m.Down()
	case "steps":
		n, convErr := strconv.Atoi(flag.Arg(1))
		if convErr != nil {
			fail(fmt.Errorf("steps requires an integer: %w", convErr))
		}
		err = m.Steps(n)
	case "version":
		v, dirty, vErr := m.Version()
		if vErr != nil {
			fail(vErr)
		}
		fmt.Printf("version=%d dirty=%v\n", v, dirty)
		return
	default:
		fail(fmt.Errorf("unknown command %q", flag.Arg(0)))
	}
	if err != nil {
		fail(err)
	}
	fmt.Println(flag.Arg(0), "ok")
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "migrate:", err)
	os.Exit(1)
}
