package worker_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/manideepvarma1999/conveyor/internal/testdb"
	"github.com/manideepvarma1999/conveyor/queue"
	"github.com/manideepvarma1999/conveyor/worker"
)

var pool *pgxpool.Pool

func TestMain(m *testing.M) { os.Exit(testdb.Run(m, &pool)) }

func setup(t *testing.T) (*queue.Store, string, worker.Config) {
	t.Helper()
	s := queue.NewStore(pool)
	s.Backoff = func(int) time.Duration { return 0 }
	q := testdb.QueueName()
	return s, q, worker.Config{
		Queue: q, Concurrency: 4, Lease: 3 * time.Second, PollInterval: 50 * time.Millisecond,
		ReapInterval: 100 * time.Millisecond, ShutdownTimeout: 5 * time.Second,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func waitFor(t *testing.T, s *queue.Store, id int64, want queue.Status) queue.Job {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		j, err := s.Get(context.Background(), id)
		if err == nil && j.Status == want {
			return j
		}
		time.Sleep(25 * time.Millisecond)
	}
	j, _ := s.Get(context.Background(), id)
	t.Fatalf("job %d status = %s, want %s", id, j.Status, want)
	return j
}

func start(t *testing.T, w *worker.Worker) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }() //nolint:errcheck
	return func() { cancel(); <-done }
}

func TestProcessesJobsAndRetriesFailures(t *testing.T) {
	t.Parallel()
	s, q, cfg := setup(t)
	w := worker.New(s, cfg)
	var calls atomic.Int32
	w.Handle("flaky", func(ctx context.Context, job queue.ClaimedJob) error {
		if calls.Add(1) < 3 {
			return errors.New("temporary outage")
		}
		return nil
	})
	stop := start(t, w)
	defer stop()

	job, _, _ := s.Enqueue(context.Background(), queue.EnqueueParams{Queue: q, Kind: "flaky"})
	done := waitFor(t, s, job.ID, queue.StatusSucceeded)
	if done.Attempts != 3 {
		t.Fatalf("attempts = %d, want 3", done.Attempts)
	}
}

func TestPermanentErrorsUnknownKindsAndPanicsAreHandled(t *testing.T) {
	t.Parallel()
	s, q, cfg := setup(t)
	w := worker.New(s, cfg)
	w.Handle("bad-input", func(context.Context, queue.ClaimedJob) error {
		return worker.Permanent(errors.New("missing field"))
	})
	w.Handle("panics", func(context.Context, queue.ClaimedJob) error { panic("nil map") })
	stop := start(t, w)
	defer stop()

	ctx := context.Background()
	bad, _, _ := s.Enqueue(ctx, queue.EnqueueParams{Queue: q, Kind: "bad-input", MaxAttempts: 5})
	unknown, _, _ := s.Enqueue(ctx, queue.EnqueueParams{Queue: q, Kind: "nobody-handles-this", MaxAttempts: 5})
	panics, _, _ := s.Enqueue(ctx, queue.EnqueueParams{Queue: q, Kind: "panics", MaxAttempts: 2})

	if j := waitFor(t, s, bad.ID, queue.StatusDead); j.Attempts != 1 {
		t.Fatalf("permanent error retried: attempts = %d", j.Attempts)
	}
	waitFor(t, s, unknown.ID, queue.StatusDead)
	j := waitFor(t, s, panics.ID, queue.StatusDead)
	if j.Attempts != 2 || j.LastError == nil || *j.LastError != "handler panicked: nil map" {
		t.Fatalf("panic handling: attempts=%d err=%v", j.Attempts, j.LastError)
	}
}

func TestHeartbeatKeepsLongJobsAlive(t *testing.T) {
	t.Parallel()
	s, q, cfg := setup(t)
	cfg.Lease = 1 * time.Second
	w := worker.New(s, cfg)
	var runs atomic.Int32
	w.Handle("slow", func(ctx context.Context, job queue.ClaimedJob) error {
		runs.Add(1)
		select {
		case <-time.After(3 * time.Second): // three times the lease
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	stop := start(t, w)
	defer stop()

	job, _, _ := s.Enqueue(context.Background(), queue.EnqueueParams{Queue: q, Kind: "slow"})
	done := waitFor(t, s, job.ID, queue.StatusSucceeded)
	if done.Attempts != 1 || runs.Load() != 1 {
		t.Fatalf("job was reclaimed mid-run: attempts=%d runs=%d", done.Attempts, runs.Load())
	}
}

func TestGracefulShutdownFinishesInFlightJobs(t *testing.T) {
	t.Parallel()
	s, q, cfg := setup(t)
	w := worker.New(s, cfg)
	started := make(chan struct{})
	w.Handle("work", func(ctx context.Context, job queue.ClaimedJob) error {
		close(started)
		time.Sleep(500 * time.Millisecond)
		return nil
	})
	stop := start(t, w)

	job, _, _ := s.Enqueue(context.Background(), queue.EnqueueParams{Queue: q, Kind: "work"})
	<-started
	stop() // returns only after the in-flight job is done
	got, _ := s.Get(context.Background(), job.ID)
	if got.Status != queue.StatusSucceeded {
		t.Fatalf("in-flight job status after shutdown = %s", got.Status)
	}
}

func TestReaperRecoversJobsFromCrashedWorkers(t *testing.T) {
	t.Parallel()
	s, q, cfg := setup(t)
	ctx := context.Background()
	job, _, _ := s.Enqueue(ctx, queue.EnqueueParams{Queue: q, Kind: "work"})
	// Simulate a worker that claimed the job and died.
	if _, err := s.Claim(ctx, q, 1, time.Second); err != nil {
		t.Fatal(err)
	}

	w := worker.New(s, cfg)
	w.Handle("work", func(context.Context, queue.ClaimedJob) error { return nil })
	stop := start(t, w)
	defer stop()
	if j := waitFor(t, s, job.ID, queue.StatusSucceeded); j.Attempts != 2 {
		t.Fatalf("attempts = %d, want 2", j.Attempts)
	}
}
