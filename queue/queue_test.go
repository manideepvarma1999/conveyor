package queue_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/manideepvarma1999/conveyor/internal/testdb"
	"github.com/manideepvarma1999/conveyor/queue"
)

var pool *pgxpool.Pool

func TestMain(m *testing.M) { os.Exit(testdb.Run(m, &pool)) }

func newStore() *queue.Store {
	s := queue.NewStore(pool)
	s.Backoff = func(int) time.Duration { return 0 } // retry immediately in tests
	return s
}

func enqueue(t *testing.T, s *queue.Store, p queue.EnqueueParams) queue.Job {
	t.Helper()
	if p.Kind == "" {
		p.Kind = "test"
	}
	job, _, err := s.Enqueue(context.Background(), p)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	return job
}

func claimOne(t *testing.T, s *queue.Store, q string) queue.ClaimedJob {
	t.Helper()
	jobs, err := s.Claim(context.Background(), q, 1, time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 claimed job, got %d", len(jobs))
	}
	return jobs[0]
}

func TestEnqueueClaimComplete(t *testing.T) {
	t.Parallel()
	ctx, s, q := context.Background(), newStore(), testdb.QueueName()
	job := enqueue(t, s, queue.EnqueueParams{Queue: q, Payload: json.RawMessage(`{"to":"a@example.com"}`)})
	if job.Status != queue.StatusQueued {
		t.Fatalf("status = %s", job.Status)
	}

	c := claimOne(t, s, q)
	if c.ID != job.ID || c.Attempts != 1 || c.Status != queue.StatusRunning || c.LeaseToken == "" {
		t.Fatalf("unexpected claim: %+v", c)
	}
	if err := s.Complete(ctx, c.ID, c.LeaseToken); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get(ctx, c.ID)
	if got.Status != queue.StatusSucceeded || got.FinishedAt == nil {
		t.Fatalf("after complete: %+v", got)
	}
	if err := s.Complete(ctx, c.ID, c.LeaseToken); !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatalf("second complete should lose lease, got %v", err)
	}
}

func TestPriorityThenFIFO(t *testing.T) {
	t.Parallel()
	s, q := newStore(), testdb.QueueName()
	low := enqueue(t, s, queue.EnqueueParams{Queue: q, Priority: -5})
	first := enqueue(t, s, queue.EnqueueParams{Queue: q})
	second := enqueue(t, s, queue.EnqueueParams{Queue: q})
	high := enqueue(t, s, queue.EnqueueParams{Queue: q, Priority: 10})

	var order []int64
	for range 4 {
		order = append(order, claimOne(t, s, q).ID)
	}
	want := []int64{high.ID, first.ID, second.ID, low.ID}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("claim order = %v, want %v", order, want)
		}
	}
}

func TestScheduledJobsWaitUntilRunAt(t *testing.T) {
	t.Parallel()
	ctx, s, q := context.Background(), newStore(), testdb.QueueName()
	enqueue(t, s, queue.EnqueueParams{Queue: q, RunAt: time.Now().Add(time.Hour)})
	jobs, err := s.Claim(ctx, q, 10, time.Minute)
	if err != nil || len(jobs) != 0 {
		t.Fatalf("future job claimed early: %v %v", jobs, err)
	}
}

func TestIdempotentEnqueue(t *testing.T) {
	t.Parallel()
	ctx, s, q := context.Background(), newStore(), testdb.QueueName()
	a, created, err := s.Enqueue(ctx, queue.EnqueueParams{Queue: q, Kind: "charge", IdempotencyKey: "order-42"})
	if err != nil || !created {
		t.Fatalf("first enqueue: %v created=%v", err, created)
	}
	b, created, err := s.Enqueue(ctx, queue.EnqueueParams{Queue: q, Kind: "charge", IdempotencyKey: "order-42"})
	if err != nil || created || b.ID != a.ID {
		t.Fatalf("duplicate enqueue: id %d vs %d, created=%v, err=%v", b.ID, a.ID, created, err)
	}
	// The same key on another queue is independent.
	_, created, _ = s.Enqueue(ctx, queue.EnqueueParams{Queue: testdb.QueueName(), Kind: "charge", IdempotencyKey: "order-42"})
	if !created {
		t.Fatal("key should be scoped per queue")
	}
}

func TestRetriesThenDeadLetter(t *testing.T) {
	t.Parallel()
	ctx, s, q := context.Background(), newStore(), testdb.QueueName()
	job := enqueue(t, s, queue.EnqueueParams{Queue: q, MaxAttempts: 3})

	for attempt := 1; attempt <= 3; attempt++ {
		c := claimOne(t, s, q)
		if c.Attempts != attempt {
			t.Fatalf("attempt = %d, want %d", c.Attempts, attempt)
		}
		status, err := s.Fail(ctx, c.ID, c.LeaseToken, "boom", true)
		if err != nil {
			t.Fatal(err)
		}
		want := queue.StatusQueued
		if attempt == 3 {
			want = queue.StatusDead
		}
		if status != want {
			t.Fatalf("after attempt %d status = %s, want %s", attempt, status, want)
		}
	}
	dead, _ := s.Get(ctx, job.ID)
	if dead.LastError == nil || *dead.LastError != "boom" {
		t.Fatalf("last_error = %v", dead.LastError)
	}

	retried, err := s.Retry(ctx, job.ID)
	if err != nil || retried.Status != queue.StatusQueued || retried.Attempts != 0 {
		t.Fatalf("retry: %+v %v", retried, err)
	}
}

func TestNonRetryableFailureGoesStraightToDead(t *testing.T) {
	t.Parallel()
	ctx, s, q := context.Background(), newStore(), testdb.QueueName()
	enqueue(t, s, queue.EnqueueParams{Queue: q, MaxAttempts: 10})
	c := claimOne(t, s, q)
	status, err := s.Fail(ctx, c.ID, c.LeaseToken, "invalid payload", false)
	if err != nil || status != queue.StatusDead {
		t.Fatalf("status = %s, err = %v", status, err)
	}
}

func TestBackoffDelaysRetry(t *testing.T) {
	t.Parallel()
	ctx, q := context.Background(), testdb.QueueName()
	s := queue.NewStore(pool)
	s.Backoff = func(int) time.Duration { return time.Hour }
	enqueue(t, s, queue.EnqueueParams{Queue: q})
	c := claimOne(t, s, q)
	if _, err := s.Fail(ctx, c.ID, c.LeaseToken, "later", true); err != nil {
		t.Fatal(err)
	}
	jobs, _ := s.Claim(ctx, q, 1, time.Minute)
	if len(jobs) != 0 {
		t.Fatal("job retried before its backoff elapsed")
	}
}

// A worker that stalls past its lease must not be able to complete the job after
// it has been handed to another worker: the lease token acts as a fencing token.
func TestExpiredLeaseIsReapedAndFenced(t *testing.T) {
	t.Parallel()
	ctx, s, q := context.Background(), newStore(), testdb.QueueName()
	job := enqueue(t, s, queue.EnqueueParams{Queue: q})
	stale := claimOne(t, s, q)

	if _, err := pool.Exec(ctx, `update conveyor_jobs set lease_expires_at = now() - interval '1 second' where id = $1`, job.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.ReapExpired(ctx); err != nil {
		t.Fatal(err)
	}
	fresh := claimOne(t, s, q)
	if fresh.ID != job.ID || fresh.Attempts != 2 || fresh.LeaseToken == stale.LeaseToken {
		t.Fatalf("reclaimed job: %+v", fresh)
	}
	if err := s.Complete(ctx, stale.ID, stale.LeaseToken); !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatalf("stale worker completed the job: %v", err)
	}
	if err := s.Heartbeat(ctx, stale.ID, stale.LeaseToken, time.Minute); !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatalf("stale heartbeat accepted: %v", err)
	}
	if err := s.Complete(ctx, fresh.ID, fresh.LeaseToken); err != nil {
		t.Fatal(err)
	}
}

func TestReapingAnExhaustedJobKillsIt(t *testing.T) {
	t.Parallel()
	ctx, s, q := context.Background(), newStore(), testdb.QueueName()
	job := enqueue(t, s, queue.EnqueueParams{Queue: q, MaxAttempts: 1})
	claimOne(t, s, q)
	pool.Exec(ctx, `update conveyor_jobs set lease_expires_at = now() - interval '1 second' where id = $1`, job.ID) //nolint:errcheck
	if _, _, err := s.ReapExpired(ctx); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get(ctx, job.ID)
	if got.Status != queue.StatusDead {
		t.Fatalf("status = %s", got.Status)
	}
}

func TestBadLeaseTokensAreRejected(t *testing.T) {
	t.Parallel()
	ctx, s, q := context.Background(), newStore(), testdb.QueueName()
	enqueue(t, s, queue.EnqueueParams{Queue: q})
	c := claimOne(t, s, q)
	for _, token := range []string{"", "not-a-uuid", "00000000-0000-0000-0000-000000000000"} {
		if err := s.Complete(ctx, c.ID, token); !errors.Is(err, queue.ErrLeaseLost) {
			t.Fatalf("token %q: %v", token, err)
		}
		if _, err := s.Fail(ctx, c.ID, token, "x", true); !errors.Is(err, queue.ErrLeaseLost) {
			t.Fatalf("fail with token %q: %v", token, err)
		}
	}
}

func TestCancelOnlyQueuedJobs(t *testing.T) {
	t.Parallel()
	ctx, s, q := context.Background(), newStore(), testdb.QueueName()
	a := enqueue(t, s, queue.EnqueueParams{Queue: q})
	if j, err := s.Cancel(ctx, a.ID); err != nil || j.Status != queue.StatusCancelled {
		t.Fatalf("cancel: %+v %v", j, err)
	}
	b := enqueue(t, s, queue.EnqueueParams{Queue: q})
	claimOne(t, s, q)
	if _, err := s.Cancel(ctx, b.ID); !errors.Is(err, queue.ErrConflict) {
		t.Fatalf("cancelling a running job: %v", err)
	}
	if _, err := s.Cancel(ctx, 999_999_999); !errors.Is(err, queue.ErrNotFound) {
		t.Fatalf("cancelling a missing job: %v", err)
	}
}

func TestValidation(t *testing.T) {
	t.Parallel()
	ctx, s := context.Background(), newStore()
	cases := []queue.EnqueueParams{
		{Queue: "Bad Queue", Kind: "x"},
		{Queue: "ok", Kind: ""},
		{Queue: "ok", Kind: "x", Payload: json.RawMessage(`{not json`)},
		{Queue: "ok", Kind: "x", Priority: 1000},
		{Queue: "ok", Kind: "x", MaxAttempts: 500},
		{Queue: "ok", Kind: "x", Payload: json.RawMessage(`"` + string(make([]byte, queue.MaxPayloadBytes)) + `"`)},
	}
	for i, p := range cases {
		var verr *queue.ValidationError
		if _, _, err := s.Enqueue(ctx, p); !errors.As(err, &verr) {
			t.Errorf("case %d: expected validation error, got %v", i, err)
		}
	}
}

// Many workers racing on one queue must process every job exactly once.
func TestConcurrentClaimsNeverDoubleProcess(t *testing.T) {
	t.Parallel()
	ctx, s, q := context.Background(), newStore(), testdb.QueueName()
	const jobs, workers = 300, 12
	for range jobs {
		enqueue(t, s, queue.EnqueueParams{Queue: q})
	}

	var mu sync.Mutex
	seen := map[int64]int{}
	var done atomic.Int64
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for done.Load() < jobs {
				claimed, err := s.Claim(ctx, q, 5, time.Minute)
				if err != nil {
					t.Error(err)
					return
				}
				if len(claimed) == 0 {
					return
				}
				for _, c := range claimed {
					mu.Lock()
					seen[c.ID]++
					mu.Unlock()
					if err := s.Complete(ctx, c.ID, c.LeaseToken); err != nil {
						t.Error(err)
					}
					done.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	if len(seen) != jobs {
		t.Fatalf("processed %d distinct jobs, want %d", len(seen), jobs)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("job %d processed %d times", id, n)
		}
	}
}

func TestStatsAndPurge(t *testing.T) {
	t.Parallel()
	ctx, s, q := context.Background(), newStore(), testdb.QueueName()
	enqueue(t, s, queue.EnqueueParams{Queue: q})
	enqueue(t, s, queue.EnqueueParams{Queue: q})
	c := claimOne(t, s, q)
	s.Complete(ctx, c.ID, c.LeaseToken) //nolint:errcheck

	stats, err := s.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, st := range stats {
		if st.Queue == q {
			found = true
			if st.Counts[queue.StatusQueued] != 1 || st.Counts[queue.StatusSucceeded] != 1 {
				t.Fatalf("counts = %v", st.Counts)
			}
		}
	}
	if !found {
		t.Fatal("queue missing from stats")
	}

	pool.Exec(ctx, `update conveyor_jobs set finished_at = now() - interval '2 days' where id = $1`, c.ID) //nolint:errcheck
	n, err := s.Purge(ctx, 24*time.Hour)
	if err != nil || n < 1 {
		t.Fatalf("purge removed %d, err %v", n, err)
	}
}

func TestExponentialBackoff(t *testing.T) {
	t.Parallel()
	one := func() float64 { return 1 }
	cases := map[int]time.Duration{1: time.Second, 2: 2 * time.Second, 4: 8 * time.Second, 30: time.Minute, 5000: time.Minute}
	for attempt, want := range cases {
		if got := queue.ExponentialBackoff(attempt, time.Second, time.Minute, one); got != want {
			t.Errorf("attempt %d: got %v want %v", attempt, got, want)
		}
	}
	if got := queue.ExponentialBackoff(3, time.Second, time.Minute, func() float64 { return 0.5 }); got != 2*time.Second {
		t.Errorf("jitter: got %v", got)
	}
}
