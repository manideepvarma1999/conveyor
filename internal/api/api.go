// Package api exposes the queue over HTTP for producers and workers in any language,
// plus stats, Prometheus metrics and a small dashboard.
package api

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/manideepvarma1999/conveyor/internal/auth"
	"github.com/manideepvarma1999/conveyor/queue"
)

//go:embed dashboard
var dashboardFS embed.FS

const maxBodyBytes = queue.MaxPayloadBytes + 16<<10

type Server struct {
	store   *queue.Store
	keys    *auth.Keys
	log     *slog.Logger
	perKey  *limiter
	authBad *limiter
	mux     *http.ServeMux
}

type ctxKey struct{}

func New(store *queue.Store, keys *auth.Keys, log *slog.Logger) *Server {
	s := &Server{
		store:   store,
		keys:    keys,
		log:     log,
		perKey:  newLimiter(200, 400), // requests per second per API key
		authBad: newLimiter(1, 10),    // failed authentications per second per client IP
		mux:     http.NewServeMux(),
	}
	s.routes()
	return s
}

func (s *Server) routes() {
	static, _ := fs.Sub(dashboardFS, "dashboard")
	s.mux.Handle("GET /", http.FileServerFS(static))
	s.mux.HandleFunc("GET /healthz", s.health)

	s.mux.Handle("POST /v1/jobs", s.authed(auth.ScopeEnqueue, s.enqueue))
	s.mux.Handle("GET /v1/jobs/{id}", s.authed(auth.ScopeEnqueue, s.getJob))
	s.mux.Handle("GET /v1/jobs", s.authed(auth.ScopeAdmin, s.listJobs))
	s.mux.Handle("POST /v1/jobs/{id}/retry", s.authed(auth.ScopeAdmin, s.retry))
	s.mux.Handle("POST /v1/jobs/{id}/cancel", s.authed(auth.ScopeAdmin, s.cancel))
	s.mux.Handle("GET /v1/stats", s.authed(auth.ScopeAdmin, s.stats))
	s.mux.Handle("GET /metrics", s.authed(auth.ScopeAdmin, s.metrics))

	s.mux.Handle("POST /v1/queues/{queue}/claim", s.authed(auth.ScopeWork, s.claim))
	s.mux.Handle("POST /v1/jobs/{id}/complete", s.authed(auth.ScopeWork, s.complete))
	s.mux.Handle("POST /v1/jobs/{id}/fail", s.authed(auth.ScopeWork, s.fail))
	s.mux.Handle("POST /v1/jobs/{id}/heartbeat", s.authed(auth.ScopeWork, s.heartbeat))
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h := w.Header()
	h.Set("Content-Security-Policy", "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; base-uri 'none'; form-action 'self'; frame-ancestors 'none'")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Referrer-Policy", "no-referrer")
	h.Set("Cross-Origin-Opener-Policy", "same-origin")
	h.Set("Cache-Control", "no-store")

	start := time.Now()
	rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
	defer func() {
		if p := recover(); p != nil {
			s.log.Error("panic serving request", "panic", p, "path", r.URL.Path)
			writeError(rec, http.StatusInternalServerError, "internal error")
		}
		// Never log headers: they carry API keys.
		s.log.Debug("request", "method", r.Method, "path", r.URL.Path, "status", rec.status, "duration", time.Since(start))
	}()
	r.Body = http.MaxBytesReader(rec, r.Body, maxBodyBytes)
	s.mux.ServeHTTP(rec, r)
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// authed requires a Bearer API key with the given scope (admin keys pass every check).
func (s *Server) authed(scope auth.Scope, next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := clientIP(r)
		token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || token == "" {
			w.Header().Set("WWW-Authenticate", `Bearer realm="conveyor"`)
			writeError(w, http.StatusUnauthorized, "missing API key")
			return
		}
		key, err := s.keys.Authenticate(r.Context(), token)
		if err != nil {
			if !errors.Is(err, auth.ErrInvalidKey) {
				s.log.Error("authenticate", "err", err)
				writeError(w, http.StatusServiceUnavailable, "temporarily unavailable")
				return
			}
			if !s.authBad.allow(ip, time.Now()) {
				writeError(w, http.StatusTooManyRequests, "too many failed attempts")
				return
			}
			writeError(w, http.StatusUnauthorized, "invalid API key")
			return
		}
		if !key.Allows(scope) {
			writeError(w, http.StatusForbidden, fmt.Sprintf("this key lacks the %q scope", scope))
			return
		}
		if !s.perKey.allow(strconv.FormatInt(key.ID, 10), time.Now()) {
			w.Header().Set("Retry-After", "1")
			writeError(w, http.StatusTooManyRequests, "rate limit exceeded")
			return
		}
		next(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, key)))
	})
}

// clientIP uses the socket address. Put Conveyor behind a proxy you control and
// terminate TLS there; X-Forwarded-For is deliberately not trusted here.
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.store.Pool().Ping(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "database unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) enqueue(w http.ResponseWriter, r *http.Request) {
	var p queue.EnqueueParams
	if !decode(w, r, &p) {
		return
	}
	job, created, err := s.store.Enqueue(r.Context(), p)
	if err != nil {
		s.fail500(w, err)
		return
	}
	status := http.StatusCreated
	if !created {
		status = http.StatusOK // idempotent replay
	}
	writeJSON(w, status, job)
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	job, err := s.store.Get(r.Context(), id)
	if err != nil {
		s.fail500(w, err)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	before, _ := strconv.ParseInt(q.Get("before"), 10, 64)
	jobs, err := s.store.List(r.Context(), queue.ListFilter{
		Queue: q.Get("queue"), Status: queue.Status(q.Get("status")), Limit: limit, BeforeID: before,
	})
	if err != nil {
		s.fail500(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": jobs})
}

func (s *Server) retry(w http.ResponseWriter, r *http.Request)  { s.transition(w, r, s.store.Retry) }
func (s *Server) cancel(w http.ResponseWriter, r *http.Request) { s.transition(w, r, s.store.Cancel) }

func (s *Server) transition(w http.ResponseWriter, r *http.Request, fn func(context.Context, int64) (queue.Job, error)) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	job, err := fn(r.Context(), id)
	if err != nil {
		s.fail500(w, err)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) stats(w http.ResponseWriter, r *http.Request) {
	stats, err := s.store.Stats(r.Context())
	if err != nil {
		s.fail500(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"queues": stats})
}

// metrics renders queue depth in the Prometheus text exposition format.
func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	stats, err := s.store.Stats(r.Context())
	if err != nil {
		s.fail500(w, err)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	var b strings.Builder
	b.WriteString("# HELP conveyor_jobs Number of jobs by queue and status.\n# TYPE conveyor_jobs gauge\n")
	for _, q := range stats {
		for status, n := range q.Counts {
			fmt.Fprintf(&b, "conveyor_jobs{queue=%q,status=%q} %d\n", q.Queue, status, n)
		}
	}
	b.WriteString("# HELP conveyor_oldest_ready_job_seconds Age of the oldest job waiting to run.\n# TYPE conveyor_oldest_ready_job_seconds gauge\n")
	for _, q := range stats {
		fmt.Fprintf(&b, "conveyor_oldest_ready_job_seconds{queue=%q} %.3f\n", q.Queue, q.OldestReadySecond)
	}
	io.WriteString(w, b.String()) //nolint:errcheck
}

type claimRequest struct {
	Limit        int `json:"limit"`
	LeaseSeconds int `json:"lease_seconds"`
}

func (s *Server) claim(w http.ResponseWriter, r *http.Request) {
	req := claimRequest{Limit: 1, LeaseSeconds: 30}
	if r.ContentLength != 0 && !decode(w, r, &req) {
		return
	}
	jobs, err := s.store.Claim(r.Context(), r.PathValue("queue"), req.Limit, time.Duration(req.LeaseSeconds)*time.Second)
	if err != nil {
		s.fail500(w, err)
		return
	}
	if jobs == nil {
		jobs = []queue.ClaimedJob{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": jobs})
}

type leaseRequest struct {
	LeaseToken   string `json:"lease_token"`
	Error        string `json:"error"`
	Retryable    *bool  `json:"retryable"`
	LeaseSeconds int    `json:"lease_seconds"`
}

func (s *Server) complete(w http.ResponseWriter, r *http.Request) {
	id, req, ok := s.leaseCall(w, r)
	if !ok {
		return
	}
	if err := s.store.Complete(r.Context(), id, req.LeaseToken); err != nil {
		s.fail500(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": string(queue.StatusSucceeded)})
}

func (s *Server) fail(w http.ResponseWriter, r *http.Request) {
	id, req, ok := s.leaseCall(w, r)
	if !ok {
		return
	}
	retryable := req.Retryable == nil || *req.Retryable
	status, err := s.store.Fail(r.Context(), id, req.LeaseToken, req.Error, retryable)
	if err != nil {
		s.fail500(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": string(status)})
}

func (s *Server) heartbeat(w http.ResponseWriter, r *http.Request) {
	id, req, ok := s.leaseCall(w, r)
	if !ok {
		return
	}
	lease := time.Duration(req.LeaseSeconds) * time.Second
	if lease < time.Second || lease > 12*time.Hour {
		writeError(w, http.StatusBadRequest, "lease_seconds must be between 1 and 43200")
		return
	}
	if err := s.store.Heartbeat(r.Context(), id, req.LeaseToken, lease); err != nil {
		s.fail500(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": string(queue.StatusRunning)})
}

func (s *Server) leaseCall(w http.ResponseWriter, r *http.Request) (int64, leaseRequest, bool) {
	id, ok := pathID(w, r)
	if !ok {
		return 0, leaseRequest{}, false
	}
	var req leaseRequest
	if !decode(w, r, &req) {
		return 0, leaseRequest{}, false
	}
	if req.LeaseToken == "" {
		writeError(w, http.StatusBadRequest, "lease_token is required")
		return 0, leaseRequest{}, false
	}
	return id, req, true
}

// fail500 maps known errors to client responses and hides everything else.
func (s *Server) fail500(w http.ResponseWriter, err error) {
	var verr *queue.ValidationError
	switch {
	case errors.As(err, &verr):
		writeError(w, http.StatusBadRequest, verr.Msg)
	case errors.Is(err, queue.ErrNotFound):
		writeError(w, http.StatusNotFound, "job not found")
	case errors.Is(err, queue.ErrLeaseLost):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, queue.ErrConflict):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, context.Canceled):
		// client went away
	default:
		s.log.Error("request failed", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

func pathID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeError(w, http.StatusBadRequest, "invalid job id")
		return 0, false
	}
	return id, true
}

// decode reads strict JSON: unknown fields and trailing data are rejected.
func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	if ct := r.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		writeError(w, http.StatusUnsupportedMediaType, "Content-Type must be application/json")
		return false
	}
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
		} else {
			writeError(w, http.StatusBadRequest, "invalid JSON body")
		}
		return false
	}
	if dec.More() {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
