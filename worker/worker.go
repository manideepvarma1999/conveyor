// Package worker runs handlers for jobs claimed from a queue.
//
// A Worker claims jobs as slots free up, extends each job's lease while its handler
// runs, recovers panics, retries failures with backoff (via the queue), wakes
// immediately on new jobs through LISTEN/NOTIFY, and shuts down gracefully.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/manideepvarma1999/conveyor/queue"
)

// Handler processes one job. Returning nil completes it; an error schedules a retry
// unless it is wrapped with Permanent.
type Handler func(ctx context.Context, job queue.ClaimedJob) error

type permanentError struct{ err error }

func (p permanentError) Error() string { return p.err.Error() }
func (p permanentError) Unwrap() error { return p.err }

// Permanent marks an error as not worth retrying (bad input, for example); the job goes
// straight to the dead-letter state.
func Permanent(err error) error { return permanentError{err} }

type Config struct {
	Queue           string
	Concurrency     int           // parallel jobs (default 4)
	Lease           time.Duration // lease length, renewed while a handler runs (default 30s)
	JobTimeout      time.Duration // per-job deadline (default 5m)
	PollInterval    time.Duration // fallback polling when no notification arrives (default 2s)
	ReapInterval    time.Duration // how often to recover expired leases (default 15s)
	ShutdownTimeout time.Duration // how long to let in-flight jobs finish on shutdown (default 30s)
	Logger          *slog.Logger
}

type Worker struct {
	store    *queue.Store
	cfg      Config
	handlers map[string]Handler
	wake     chan struct{}
}

func New(store *queue.Store, cfg Config) *Worker {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 4
	}
	if cfg.Lease <= 0 {
		cfg.Lease = 30 * time.Second
	}
	if cfg.JobTimeout <= 0 {
		cfg.JobTimeout = 5 * time.Minute
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 2 * time.Second
	}
	if cfg.ReapInterval <= 0 {
		cfg.ReapInterval = 15 * time.Second
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 30 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Worker{store: store, cfg: cfg, handlers: map[string]Handler{}, wake: make(chan struct{}, 1)}
}

// Handle registers the handler for a job kind. Call before Run.
func (w *Worker) Handle(kind string, h Handler) { w.handlers[kind] = h }

// Run processes jobs until ctx is cancelled, then waits up to ShutdownTimeout for
// in-flight jobs. Jobs still running after that keep their lease until it expires and
// are then retried by any worker's reaper.
func (w *Worker) Run(ctx context.Context) error {
	log := w.cfg.Logger.With("queue", w.cfg.Queue)
	// Jobs get their own context so that shutdown lets them finish instead of killing them.
	jobsCtx, cancelJobs := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelJobs()

	go w.listen(ctx, log)
	go w.reapLoop(ctx, log)

	slots := make(chan struct{}, w.cfg.Concurrency)
	var inflight sync.WaitGroup
	poll := time.NewTicker(w.cfg.PollInterval)
	defer poll.Stop()

	log.Info("worker started", "concurrency", w.cfg.Concurrency)
	for ctx.Err() == nil {
		free := w.cfg.Concurrency - len(slots)
		var jobs []queue.ClaimedJob
		if free > 0 {
			var err error
			jobs, err = w.store.Claim(ctx, w.cfg.Queue, free, w.cfg.Lease)
			if err != nil && ctx.Err() == nil {
				log.Error("claim failed", "err", err)
			}
		}
		for _, job := range jobs {
			slots <- struct{}{}
			inflight.Add(1)
			go func() {
				defer func() { <-slots; inflight.Done(); w.signal() }()
				w.process(jobsCtx, job, log)
			}()
		}
		if len(jobs) == free && free > 0 {
			continue // likely more work waiting; claim again right away
		}
		select {
		case <-ctx.Done():
		case <-w.wake:
		case <-poll.C:
		}
	}

	log.Info("worker stopping, waiting for in-flight jobs", "timeout", w.cfg.ShutdownTimeout)
	done := make(chan struct{})
	go func() { inflight.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(w.cfg.ShutdownTimeout):
		cancelJobs()
		<-done
	}
	log.Info("worker stopped")
	return nil
}

func (w *Worker) signal() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func (w *Worker) process(ctx context.Context, job queue.ClaimedJob, log *slog.Logger) {
	log = log.With("job_id", job.ID, "kind", job.Kind, "attempt", job.Attempts)
	start := time.Now()

	runCtx, cancel := context.WithTimeout(ctx, w.cfg.JobTimeout)
	defer cancel()
	stopHeartbeat := w.heartbeat(runCtx, cancel, job, log)
	err := w.run(runCtx, job)
	stopHeartbeat()

	// Record the outcome even if the job context was cancelled.
	finishCtx, finishCancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer finishCancel()

	if err == nil {
		if cerr := w.store.Complete(finishCtx, job.ID, job.LeaseToken); cerr != nil {
			log.Warn("could not mark job complete", "err", cerr)
			return
		}
		log.Info("job succeeded", "duration", time.Since(start))
		return
	}

	var perm permanentError
	retryable := !errors.As(err, &perm)
	status, ferr := w.store.Fail(finishCtx, job.ID, job.LeaseToken, err.Error(), retryable)
	if ferr != nil {
		log.Warn("could not record job failure", "err", ferr, "cause", err)
		return
	}
	log.Warn("job failed", "err", err, "next_status", status, "duration", time.Since(start))
}

// run calls the handler, turning panics and unknown kinds into errors.
func (w *Worker) run(ctx context.Context, job queue.ClaimedJob) (err error) {
	h, ok := w.handlers[job.Kind]
	if !ok {
		return Permanent(fmt.Errorf("no handler registered for kind %q", job.Kind))
	}
	defer func() {
		if r := recover(); r != nil {
			w.cfg.Logger.Error("handler panicked", "job_id", job.ID, "panic", r, "stack", string(debug.Stack()))
			err = fmt.Errorf("handler panicked: %v", r)
		}
	}()
	return h(ctx, job)
}

// heartbeat renews the lease every third of its length. If the lease is lost (another
// worker took the job over), the handler's context is cancelled.
func (w *Worker) heartbeat(ctx context.Context, cancel context.CancelFunc, job queue.ClaimedJob, log *slog.Logger) (stop func()) {
	done := make(chan struct{})
	var once sync.Once
	go func() {
		t := time.NewTicker(w.cfg.Lease / 3)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				if err := w.store.Heartbeat(ctx, job.ID, job.LeaseToken, w.cfg.Lease); err != nil {
					if errors.Is(err, queue.ErrLeaseLost) {
						log.Warn("lease lost, cancelling handler")
						cancel()
						return
					}
					log.Warn("heartbeat failed", "err", err)
				}
			}
		}
	}()
	return func() { once.Do(func() { close(done) }) }
}

func (w *Worker) reapLoop(ctx context.Context, log *slog.Logger) {
	t := time.NewTicker(w.cfg.ReapInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			requeued, dead, err := w.store.ReapExpired(ctx)
			if err != nil && ctx.Err() == nil {
				log.Error("reaper failed", "err", err)
			} else if requeued+dead > 0 {
				log.Info("recovered expired leases", "requeued", requeued, "dead", dead)
				w.signal()
			}
		}
	}
}

// listen wakes the claim loop when a job is enqueued on this queue. Polling covers
// any notification missed while reconnecting.
func (w *Worker) listen(ctx context.Context, log *slog.Logger) {
	for ctx.Err() == nil {
		conn, err := w.store.Pool().Acquire(ctx)
		if err != nil {
			if ctx.Err() == nil {
				log.Warn("listen: acquire failed", "err", err)
				sleep(ctx, 2*time.Second)
			}
			continue
		}
		if _, err := conn.Exec(ctx, "listen "+queue.NotifyChannel); err != nil {
			conn.Release()
			sleep(ctx, 2*time.Second)
			continue
		}
		for {
			n, err := conn.Conn().WaitForNotification(ctx)
			if err != nil {
				break
			}
			if n.Payload == w.cfg.Queue {
				w.signal()
			}
		}
		// The connection had LISTEN state; close it rather than return it to the pool.
		conn.Hijack().Close(context.Background()) //nolint:errcheck
	}
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
