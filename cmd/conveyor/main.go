// Command conveyor runs the job queue's HTTP API, manages API keys, runs a demo
// worker, and benchmarks throughput.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/manideepvarma1999/conveyor/internal/api"
	"github.com/manideepvarma1999/conveyor/internal/auth"
	"github.com/manideepvarma1999/conveyor/queue"
)

const usage = `conveyor - a PostgreSQL-backed job queue

Usage:
  conveyor migrate                                   apply database migrations
  conveyor serve [-addr :8080]                       run the HTTP API and dashboard
  conveyor keys create -name NAME -scopes a,b        create an API key (shown once)
  conveyor keys list                                 list API keys
  conveyor keys revoke -id ID                        revoke an API key
  conveyor demo-worker [-queue default]              run a worker with example handlers
  conveyor bench [-jobs 10000] [-workers 8]          measure throughput and latency

DATABASE_URL must point at PostgreSQL.
`

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel()}))
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, log, os.Args[1], os.Args[2:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, log *slog.Logger, cmd string, args []string) error {
	if cmd == "help" || cmd == "-h" || cmd == "--help" {
		fmt.Print(usage)
		return nil
	}
	pool, err := connect(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()

	switch cmd {
	case "migrate":
		if err := queue.Migrate(ctx, pool); err != nil {
			return err
		}
		fmt.Println("migrations applied")
		return nil
	case "serve":
		return serve(ctx, log, pool, args)
	case "keys":
		return keysCmd(ctx, pool, args)
	case "demo-worker":
		return demoWorker(ctx, log, pool, args)
	case "bench":
		return bench(ctx, pool, args)
	default:
		fmt.Fprint(os.Stderr, usage)
		return fmt.Errorf("unknown command %q", cmd)
	}
}

func connect(ctx context.Context) (*pgxpool.Pool, error) {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		return nil, errors.New("DATABASE_URL is not set (see .env.example)")
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, errors.New("DATABASE_URL is not a valid PostgreSQL URL") // don't echo it: it holds a password
	}
	cfg.MaxConns = 20
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connecting to PostgreSQL: %w", err)
	}
	return pool, nil
}

func serve(ctx context.Context, log *slog.Logger, pool *pgxpool.Pool, args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := fs.String("addr", envOr("ADDR", "127.0.0.1:8080"), "listen address")
	retention := fs.Duration("retention", 7*24*time.Hour, "delete succeeded/cancelled jobs older than this")
	fs.Parse(args) //nolint:errcheck

	store := queue.NewStore(pool)
	server := &http.Server{
		Addr:              *addr,
		Handler:           api.New(store, auth.NewKeys(pool), log),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}

	go maintenance(ctx, log, store, *retention)

	errc := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", *addr)
		errc <- server.ListenAndServe()
	}()
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	log.Info("shutting down")
	return server.Shutdown(shutdownCtx)
}

// maintenance recovers expired leases (in case no worker is running its own reaper)
// and enforces retention.
func maintenance(ctx context.Context, log *slog.Logger, store *queue.Store, retention time.Duration) {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if requeued, dead, err := store.ReapExpired(ctx); err != nil {
				log.Error("reap", "err", err)
			} else if requeued+dead > 0 {
				log.Info("recovered expired leases", "requeued", requeued, "dead", dead)
			}
			if n, err := store.Purge(ctx, retention); err != nil {
				log.Error("purge", "err", err)
			} else if n > 0 {
				log.Info("purged finished jobs", "count", n)
			}
		}
	}
}

func keysCmd(ctx context.Context, pool *pgxpool.Pool, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: conveyor keys create|list|revoke")
	}
	keys := auth.NewKeys(pool)
	switch args[0] {
	case "create":
		fs := flag.NewFlagSet("keys create", flag.ExitOnError)
		name := fs.String("name", "", "a label for the key, e.g. billing-service")
		scopes := fs.String("scopes", "", "comma-separated: enqueue, work, admin")
		fs.Parse(args[1:]) //nolint:errcheck
		parsed, err := auth.ParseScopes(*scopes)
		if err != nil {
			return err
		}
		key, secret, err := keys.Create(ctx, *name, parsed)
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "Created key %d (%s) with scopes %v.\nStore it now; it will not be shown again:\n\n", key.ID, key.Name, key.Scopes)
		fmt.Println(secret)
		return nil
	case "list":
		list, err := keys.List(ctx)
		if err != nil {
			return err
		}
		for _, k := range list {
			state := "active"
			if k.RevokedAt != nil {
				state = "revoked"
			}
			scopes := make([]string, len(k.Scopes))
			for i, s := range k.Scopes {
				scopes[i] = string(s)
			}
			fmt.Printf("%d\t%s\t%s\t%s\n", k.ID, k.Name, strings.Join(scopes, ","), state)
		}
		return nil
	case "revoke":
		fs := flag.NewFlagSet("keys revoke", flag.ExitOnError)
		id := fs.Int64("id", 0, "key id from `keys list`")
		fs.Parse(args[1:]) //nolint:errcheck
		if err := keys.Revoke(ctx, *id); err != nil {
			return err
		}
		fmt.Println("revoked")
		return nil
	default:
		return fmt.Errorf("unknown keys command %q", args[0])
	}
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func logLevel() slog.Level {
	if os.Getenv("LOG_LEVEL") == "debug" {
		return slog.LevelDebug
	}
	return slog.LevelInfo
}
