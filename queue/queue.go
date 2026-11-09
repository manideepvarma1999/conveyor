// Package queue is a durable job queue on PostgreSQL.
//
// Workers claim jobs with SELECT ... FOR UPDATE SKIP LOCKED, so any number of
// workers can share a queue without blocking each other or double-claiming.
// A claim holds a time-limited lease identified by a random token; completing or
// failing a job requires that token, so a worker whose lease expired (and whose
// job was handed to someone else) can't overwrite the new attempt's result.
package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Status string

const (
	StatusQueued    Status = "queued"
	StatusRunning   Status = "running"
	StatusSucceeded Status = "succeeded"
	StatusDead      Status = "dead"
	StatusCancelled Status = "cancelled"
)

// NotifyChannel is the LISTEN/NOTIFY channel used to wake workers on enqueue.
const NotifyChannel = "conveyor_jobs"

const (
	MaxPayloadBytes = 256 << 10
	maxErrorBytes   = 2000
)

var (
	ErrNotFound  = errors.New("job not found")
	ErrLeaseLost = errors.New("lease lost: the job was reclaimed or already finished")
	ErrConflict  = errors.New("job is not in a state that allows this")

	queueRe = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]{0,62}$`)
)

// ValidationError is returned for bad input; its message is safe to show to API callers.
type ValidationError struct{ Msg string }

func (e *ValidationError) Error() string { return e.Msg }

func invalid(format string, args ...any) error {
	return &ValidationError{Msg: fmt.Sprintf(format, args...)}
}

type Job struct {
	ID             int64           `json:"id"`
	Queue          string          `json:"queue"`
	Kind           string          `json:"kind"`
	Payload        json.RawMessage `json:"payload"`
	Status         Status          `json:"status"`
	Priority       int             `json:"priority"`
	Attempts       int             `json:"attempts"`
	MaxAttempts    int             `json:"max_attempts"`
	RunAt          time.Time       `json:"run_at"`
	LeaseExpiresAt *time.Time      `json:"lease_expires_at,omitempty"`
	IdempotencyKey *string         `json:"idempotency_key,omitempty"`
	LastError      *string         `json:"last_error,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
	FinishedAt     *time.Time      `json:"finished_at,omitempty"`
}

// ClaimedJob is a job plus the lease token needed to complete or fail it.
type ClaimedJob struct {
	Job
	LeaseToken string `json:"lease_token"`
}

const jobColumns = "id, queue, kind, payload, status, priority, attempts, max_attempts, run_at, " +
	"lease_expires_at, idempotency_key, last_error, created_at, updated_at, finished_at"

func scanJob(row pgx.Row, extra ...any) (Job, error) {
	var j Job
	dest := append([]any{&j.ID, &j.Queue, &j.Kind, &j.Payload, &j.Status, &j.Priority, &j.Attempts, &j.MaxAttempts,
		&j.RunAt, &j.LeaseExpiresAt, &j.IdempotencyKey, &j.LastError, &j.CreatedAt, &j.UpdatedAt, &j.FinishedAt}, extra...)
	err := row.Scan(dest...)
	return j, err
}

// Store is the queue's data access layer. It is safe for concurrent use.
type Store struct {
	pool *pgxpool.Pool
	// Backoff decides how long to wait before retry number `attempt` (1-based).
	Backoff func(attempt int) time.Duration
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool, Backoff: func(attempt int) time.Duration {
		return ExponentialBackoff(attempt, 2*time.Second, time.Hour, rand.Float64)
	}}
}

func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// ExponentialBackoff returns base*2^(attempt-1), capped at max, with full jitter:
// a uniform random delay in [0, that value]. Full jitter spreads retries out so a
// burst of failures doesn't come back as a synchronized burst.
func ExponentialBackoff(attempt int, base, max time.Duration, random func() float64) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	ceiling := float64(base) * math.Pow(2, float64(attempt-1))
	if ceiling > float64(max) || math.IsInf(ceiling, 0) {
		ceiling = float64(max)
	}
	return time.Duration(random() * ceiling)
}

type EnqueueParams struct {
	Queue          string          `json:"queue"`
	Kind           string          `json:"kind"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int             `json:"priority"`
	MaxAttempts    int             `json:"max_attempts"`
	RunAt          time.Time       `json:"run_at"`
	IdempotencyKey string          `json:"idempotency_key"`
}

func (p *EnqueueParams) validate() error {
	if !queueRe.MatchString(p.Queue) {
		return invalid("queue must be 1-63 lowercase letters, digits, '.', '_' or '-'")
	}
	if p.Kind == "" || len(p.Kind) > 128 {
		return invalid("kind must be 1-128 characters")
	}
	if len(p.Payload) == 0 {
		p.Payload = json.RawMessage(`{}`)
	}
	if len(p.Payload) > MaxPayloadBytes {
		return invalid("payload must be at most %d bytes", MaxPayloadBytes)
	}
	if !json.Valid(p.Payload) {
		return invalid("payload must be valid JSON")
	}
	if p.Priority < -100 || p.Priority > 100 {
		return invalid("priority must be between -100 and 100")
	}
	if p.MaxAttempts == 0 {
		p.MaxAttempts = 5
	}
	if p.MaxAttempts < 1 || p.MaxAttempts > 100 {
		return invalid("max_attempts must be between 1 and 100")
	}
	if len(p.IdempotencyKey) > 200 {
		return invalid("idempotency_key must be at most 200 characters")
	}
	return nil
}

// Enqueue adds a job. With an idempotency key, enqueueing the same key on the same
// queue again returns the existing job and created=false instead of a duplicate.
func (s *Store) Enqueue(ctx context.Context, p EnqueueParams) (job Job, created bool, err error) {
	if err := p.validate(); err != nil {
		return Job{}, false, err
	}
	var runAt any // nil means now(), using the database clock
	if !p.RunAt.IsZero() {
		runAt = p.RunAt
	}
	var key any
	if p.IdempotencyKey != "" {
		key = p.IdempotencyKey
	}

	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		job, err = scanJob(tx.QueryRow(ctx, `
			insert into conveyor_jobs (queue, kind, payload, priority, max_attempts, run_at, idempotency_key)
			values ($1, $2, $3, $4, $5, coalesce($6, now()), $7)
			on conflict (queue, idempotency_key) where idempotency_key is not null do nothing
			returning `+jobColumns,
			p.Queue, p.Kind, p.Payload, p.Priority, p.MaxAttempts, runAt, key))
		if errors.Is(err, pgx.ErrNoRows) {
			job, err = scanJob(tx.QueryRow(ctx,
				`select `+jobColumns+` from conveyor_jobs where queue = $1 and idempotency_key = $2`, p.Queue, key))
			return err
		}
		if err != nil {
			return err
		}
		created = true
		// Delivered when the transaction commits, so workers never wake for an uncommitted job.
		_, err = tx.Exec(ctx, "select pg_notify($1, $2)", NotifyChannel, p.Queue)
		return err
	})
	return job, created, err
}

// Claim leases up to `limit` ready jobs from a queue for `lease`.
func (s *Store) Claim(ctx context.Context, queue string, limit int, lease time.Duration) ([]ClaimedJob, error) {
	if !queueRe.MatchString(queue) {
		return nil, invalid("invalid queue name")
	}
	if limit < 1 || limit > 100 {
		return nil, invalid("limit must be between 1 and 100")
	}
	if lease < time.Second || lease > 12*time.Hour {
		return nil, invalid("lease must be between 1s and 12h")
	}
	rows, err := s.pool.Query(ctx, `
		update conveyor_jobs j
		set status = 'running', attempts = j.attempts + 1, lease_token = gen_random_uuid(),
		    lease_expires_at = now() + $3 * interval '1 millisecond', updated_at = now()
		from (
			select id from conveyor_jobs
			where queue = $1 and status = 'queued' and run_at <= now()
			order by priority desc, run_at, id
			for update skip locked
			limit $2
		) ready
		where j.id = ready.id
		returning `+prefixed("j", jobColumns)+`, j.lease_token::text`,
		queue, limit, lease.Milliseconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ClaimedJob
	for rows.Next() {
		var c ClaimedJob
		c.Job, err = scanJob(rows, &c.LeaseToken)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// Complete marks a job succeeded. It fails with ErrLeaseLost if the lease token no longer matches.
func (s *Store) Complete(ctx context.Context, id int64, leaseToken string) error {
	tag, err := s.pool.Exec(ctx, `
		update conveyor_jobs
		set status = 'succeeded', lease_token = null, lease_expires_at = null, last_error = null,
		    finished_at = now(), updated_at = now()
		where id = $1 and status = 'running' and lease_token = $2::uuid`, id, leaseToken)
	if err != nil {
		if isInvalidUUID(err) {
			return ErrLeaseLost
		}
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

// Fail records a failed attempt. The job is retried after a backoff unless it has used
// all its attempts or retryable is false, in which case it moves to the dead-letter state.
func (s *Store) Fail(ctx context.Context, id int64, leaseToken string, cause string, retryable bool) (Status, error) {
	if len(cause) > maxErrorBytes {
		cause = cause[:maxErrorBytes]
	}
	var attempts int
	err := s.pool.QueryRow(ctx, `select attempts from conveyor_jobs where id = $1 and status = 'running' and lease_token = $2::uuid`,
		id, leaseToken).Scan(&attempts)
	if errors.Is(err, pgx.ErrNoRows) || isInvalidUUID(err) {
		return "", ErrLeaseLost
	}
	if err != nil {
		return "", err
	}
	delay := s.Backoff(attempts)

	var status Status
	err = s.pool.QueryRow(ctx, `
		update conveyor_jobs
		set status = case when $4 and attempts < max_attempts then 'queued' else 'dead' end,
		    run_at = case when $4 and attempts < max_attempts then now() + $5 * interval '1 millisecond' else run_at end,
		    finished_at = case when $4 and attempts < max_attempts then null else now() end,
		    lease_token = null, lease_expires_at = null, last_error = $3, updated_at = now()
		where id = $1 and status = 'running' and lease_token = $2::uuid
		returning status`, id, leaseToken, cause, retryable, delay.Milliseconds()).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrLeaseLost
	}
	return status, err
}

// Heartbeat extends a running job's lease, for handlers that run longer than one lease.
func (s *Store) Heartbeat(ctx context.Context, id int64, leaseToken string, lease time.Duration) error {
	tag, err := s.pool.Exec(ctx, `
		update conveyor_jobs set lease_expires_at = now() + $3 * interval '1 millisecond', updated_at = now()
		where id = $1 and status = 'running' and lease_token = $2::uuid`, id, leaseToken, lease.Milliseconds())
	if err != nil {
		if isInvalidUUID(err) {
			return ErrLeaseLost
		}
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

// ReapExpired returns jobs whose worker vanished (lease expired) to the queue, or to
// the dead-letter state if they've used all their attempts. Safe to run from many processes.
func (s *Store) ReapExpired(ctx context.Context) (requeued, dead int64, err error) {
	rows, err := s.pool.Query(ctx, `
		update conveyor_jobs j
		set status = case when j.attempts < j.max_attempts then 'queued' else 'dead' end,
		    finished_at = case when j.attempts < j.max_attempts then null else now() end,
		    run_at = now(), lease_token = null, lease_expires_at = null,
		    last_error = 'lease expired: worker stopped responding', updated_at = now()
		from (
			select id from conveyor_jobs
			where status = 'running' and lease_expires_at < now()
			for update skip locked
			limit 1000
		) expired
		where j.id = expired.id
		returning j.status`)
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()
	for rows.Next() {
		var st Status
		if err := rows.Scan(&st); err != nil {
			return 0, 0, err
		}
		if st == StatusDead {
			dead++
		} else {
			requeued++
		}
	}
	return requeued, dead, rows.Err()
}

func (s *Store) Get(ctx context.Context, id int64) (Job, error) {
	job, err := scanJob(s.pool.QueryRow(ctx, `select `+jobColumns+` from conveyor_jobs where id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	return job, err
}

type ListFilter struct {
	Queue    string
	Status   Status
	Limit    int
	BeforeID int64 // keyset pagination: only jobs with id < BeforeID
}

func (s *Store) List(ctx context.Context, f ListFilter) ([]Job, error) {
	if f.Queue != "" && !queueRe.MatchString(f.Queue) {
		return nil, invalid("invalid queue name")
	}
	switch f.Status {
	case "", StatusQueued, StatusRunning, StatusSucceeded, StatusDead, StatusCancelled:
	default:
		return nil, invalid("invalid status")
	}
	if f.Limit < 1 || f.Limit > 200 {
		f.Limit = 50
	}
	rows, err := s.pool.Query(ctx, `
		select `+jobColumns+` from conveyor_jobs
		where ($1 = '' or queue = $1) and ($2 = '' or status = $2) and ($3 = 0 or id < $3)
		order by id desc limit $4`, f.Queue, string(f.Status), f.BeforeID, f.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	jobs := []Job{}
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

// Retry puts a dead or cancelled job back in the queue with a fresh set of attempts.
func (s *Store) Retry(ctx context.Context, id int64) (Job, error) {
	return s.transition(ctx, id, `
		update conveyor_jobs set status = 'queued', attempts = 0, run_at = now(), finished_at = null, updated_at = now()
		where id = $1 and status in ('dead', 'cancelled') returning `+jobColumns)
}

// Cancel stops a job that hasn't started yet.
func (s *Store) Cancel(ctx context.Context, id int64) (Job, error) {
	return s.transition(ctx, id, `
		update conveyor_jobs set status = 'cancelled', finished_at = now(), updated_at = now()
		where id = $1 and status = 'queued' returning `+jobColumns)
}

func (s *Store) transition(ctx context.Context, id int64, query string) (Job, error) {
	job, err := scanJob(s.pool.QueryRow(ctx, query, id))
	if errors.Is(err, pgx.ErrNoRows) {
		if _, getErr := s.Get(ctx, id); errors.Is(getErr, ErrNotFound) {
			return Job{}, ErrNotFound
		}
		return Job{}, ErrConflict
	}
	return job, err
}

// Purge deletes finished jobs (succeeded or cancelled) older than the cutoff. Dead jobs are kept for inspection.
func (s *Store) Purge(ctx context.Context, olderThan time.Duration) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		delete from conveyor_jobs where status in ('succeeded', 'cancelled')
		and finished_at < now() - $1 * interval '1 millisecond'`, olderThan.Milliseconds())
	return tag.RowsAffected(), err
}

type QueueStats struct {
	Queue             string           `json:"queue"`
	Counts            map[Status]int64 `json:"counts"`
	OldestReadySecond float64          `json:"oldest_ready_seconds"`
}

func (s *Store) Stats(ctx context.Context) ([]QueueStats, error) {
	rows, err := s.pool.Query(ctx, `
		select queue, status, count(*),
		       coalesce(extract(epoch from now() - min(run_at) filter (where status = 'queued' and run_at <= now())), 0)
		from conveyor_jobs group by queue, status order by queue`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	byQueue := map[string]*QueueStats{}
	var order []string
	for rows.Next() {
		var q string
		var st Status
		var n int64
		var oldest float64
		if err := rows.Scan(&q, &st, &n, &oldest); err != nil {
			return nil, err
		}
		qs, ok := byQueue[q]
		if !ok {
			qs = &QueueStats{Queue: q, Counts: map[Status]int64{}}
			byQueue[q] = qs
			order = append(order, q)
		}
		qs.Counts[st] = n
		if oldest > qs.OldestReadySecond {
			qs.OldestReadySecond = oldest
		}
	}
	out := make([]QueueStats, 0, len(order))
	for _, q := range order {
		out = append(out, *byQueue[q])
	}
	return out, rows.Err()
}

// prefixed qualifies every column with a table alias, e.g. "j.id, j.queue, ...".
func prefixed(alias, columns string) string {
	return alias + "." + strings.ReplaceAll(columns, ", ", ", "+alias+".")
}

func isInvalidUUID(err error) bool {
	var pgErr interface{ SQLState() string }
	return errors.As(err, &pgErr) && pgErr.SQLState() == "22P02"
}
