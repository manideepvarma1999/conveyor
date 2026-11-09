package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/manideepvarma1999/conveyor/queue"
	"github.com/manideepvarma1999/conveyor/worker"
)

// demoWorker shows the worker library with handlers that succeed, fail transiently and
// reject bad input, so the dashboard has something to show.
func demoWorker(ctx context.Context, log *slog.Logger, pool *pgxpool.Pool, args []string) error {
	fs := flag.NewFlagSet("demo-worker", flag.ExitOnError)
	q := fs.String("queue", "default", "queue to work")
	concurrency := fs.Int("concurrency", 4, "parallel jobs")
	fs.Parse(args) //nolint:errcheck

	w := worker.New(queue.NewStore(pool), worker.Config{Queue: *q, Concurrency: *concurrency, Logger: log})
	w.Handle("email.send", func(ctx context.Context, job queue.ClaimedJob) error {
		var p struct {
			To string `json:"to"`
		}
		if err := json.Unmarshal(job.Payload, &p); err != nil || p.To == "" {
			return worker.Permanent(errors.New("payload needs a \"to\" address"))
		}
		time.Sleep(time.Duration(50+rand.IntN(200)) * time.Millisecond)
		return nil
	})
	w.Handle("report.generate", func(ctx context.Context, job queue.ClaimedJob) error {
		if rand.Float64() < 0.4 {
			return errors.New("upstream API timed out")
		}
		time.Sleep(500 * time.Millisecond)
		return nil
	})
	return w.Run(ctx)
}

// bench measures two things:
//   - throughput: drain a backlog of no-op jobs with several in-process workers;
//   - wake latency: with workers idle, enqueue jobs one at a time and time how long
//     each takes to reach a handler (this exercises LISTEN/NOTIFY, not polling).
func bench(ctx context.Context, pool *pgxpool.Pool, args []string) error {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	jobs := fs.Int("jobs", 10_000, "jobs for the throughput test")
	workers := fs.Int("workers", 8, "worker instances (in-process)")
	concurrency := fs.Int("concurrency", 8, "concurrency per worker")
	samples := fs.Int("samples", 200, "jobs for the latency test")
	fs.Parse(args) //nolint:errcheck

	if err := queue.Migrate(ctx, pool); err != nil {
		return err
	}
	store := queue.NewStore(pool)
	quiet := slog.New(slog.DiscardHandler)

	// Throughput.
	q := fmt.Sprintf("bench-%d", time.Now().UnixNano())
	enqStart := time.Now()
	var wg sync.WaitGroup
	sem := make(chan struct{}, 32)
	for i := range *jobs {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer func() { <-sem; wg.Done() }()
			if _, _, err := store.Enqueue(ctx, queue.EnqueueParams{Queue: q, Kind: "noop", Payload: fmt.Appendf(nil, `{"n":%d}`, i)}); err != nil {
				fmt.Println("enqueue:", err)
			}
		}()
	}
	wg.Wait()
	enqElapsed := time.Since(enqStart)

	var processed atomic.Int64
	done := make(chan struct{})
	elapsed, err := runWorkers(ctx, store, q, *workers, *concurrency, quiet, func(queue.ClaimedJob) {
		if processed.Add(1) == int64(*jobs) {
			close(done)
		}
	}, done)
	if err != nil {
		return err
	}
	pool.Exec(context.Background(), "delete from conveyor_jobs where queue = $1", q) //nolint:errcheck
	fmt.Printf("enqueue:     %d jobs in %v (%.0f jobs/s with 32 concurrent producers)\n", *jobs, enqElapsed.Round(time.Millisecond), float64(*jobs)/enqElapsed.Seconds())
	fmt.Printf("throughput:  %d jobs in %v (%.0f jobs/s with %d workers x %d concurrency)\n", *jobs, elapsed.Round(time.Millisecond), float64(*jobs)/elapsed.Seconds(), *workers, *concurrency)

	// Wake latency.
	q = fmt.Sprintf("bench-latency-%d", time.Now().UnixNano())
	var mu sync.Mutex
	sent := map[int64]time.Time{}
	latencies := make([]time.Duration, 0, *samples)
	done = make(chan struct{})
	ctxLat, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		time.Sleep(500 * time.Millisecond) // let workers start listening
		for range *samples {
			mu.Lock()
			job, _, err := store.Enqueue(ctxLat, queue.EnqueueParams{Queue: q, Kind: "noop"})
			if err == nil {
				sent[job.ID] = time.Now()
			}
			mu.Unlock()
			time.Sleep(20 * time.Millisecond)
		}
	}()
	_, err = runWorkers(ctxLat, store, q, 2, 4, quiet, func(job queue.ClaimedJob) {
		now := time.Now()
		mu.Lock()
		if t, ok := sent[job.ID]; ok {
			latencies = append(latencies, now.Sub(t))
		}
		finished := len(latencies) == *samples
		mu.Unlock()
		if finished {
			close(done)
		}
	}, done)
	if err != nil {
		return err
	}
	pool.Exec(context.Background(), "delete from conveyor_jobs where queue = $1", q) //nolint:errcheck
	slices.Sort(latencies)
	pct := func(p float64) time.Duration {
		return latencies[int(float64(len(latencies)-1)*p)].Round(100 * time.Microsecond)
	}
	fmt.Printf("wake latency (enqueue committed -> handler start, idle workers): p50 %v  p95 %v  p99 %v\n", pct(0.5), pct(0.95), pct(0.99))
	return nil
}

func runWorkers(ctx context.Context, store *queue.Store, q string, workers, concurrency int, log *slog.Logger,
	onJob func(queue.ClaimedJob), done <-chan struct{}) (time.Duration, error) {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	start := time.Now()
	var wg sync.WaitGroup
	for range workers {
		// Polling is set high so the latency test measures notification wake-ups.
		w := worker.New(store, worker.Config{Queue: q, Concurrency: concurrency, PollInterval: 5 * time.Second, Logger: log})
		w.Handle("noop", func(ctx context.Context, job queue.ClaimedJob) error { onJob(job); return nil })
		wg.Add(1)
		go func() { defer wg.Done(); w.Run(runCtx) }() //nolint:errcheck
	}
	select {
	case <-done:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	elapsed := time.Since(start)
	cancel()
	wg.Wait()
	return elapsed, nil
}
