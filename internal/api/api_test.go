package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/manideepvarma1999/conveyor/internal/api"
	"github.com/manideepvarma1999/conveyor/internal/auth"
	"github.com/manideepvarma1999/conveyor/internal/testdb"
	"github.com/manideepvarma1999/conveyor/queue"
)

var pool *pgxpool.Pool

func TestMain(m *testing.M) { os.Exit(testdb.Run(m, &pool)) }

type env struct {
	srv                   *httptest.Server
	keys                  *auth.Keys
	admin, producer, work string
}

func newEnv(t *testing.T) *env {
	t.Helper()
	keys := auth.NewKeys(pool)
	srv := httptest.NewServer(api.New(queue.NewStore(pool), keys, slog.New(slog.NewTextHandler(io.Discard, nil))))
	t.Cleanup(srv.Close)
	mk := func(scopes ...auth.Scope) string {
		_, secret, err := keys.Create(context.Background(), "test", scopes)
		if err != nil {
			t.Fatal(err)
		}
		return secret
	}
	return &env{srv: srv, keys: keys, admin: mk(auth.ScopeAdmin), producer: mk(auth.ScopeEnqueue), work: mk(auth.ScopeWork)}
}

func (e *env) do(t *testing.T, method, path, key string, body any) (*http.Response, map[string]any) {
	t.Helper()
	var reader io.Reader
	switch b := body.(type) {
	case nil:
	case string:
		reader = strings.NewReader(b)
	default:
		raw, _ := json.Marshal(b)
		reader = bytes.NewReader(raw)
	}
	req, _ := http.NewRequest(method, e.srv.URL+path, reader)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	json.NewDecoder(resp.Body).Decode(&out) //nolint:errcheck
	return resp, out
}

func TestAuthentication(t *testing.T) {
	e := newEnv(t)
	cases := []struct {
		name, key string
		want      int
	}{
		{"no key", "", http.StatusUnauthorized},
		{"garbage", "hello", http.StatusUnauthorized},
		{"well-formed but unknown", "cvk_" + strings.Repeat("A", 43), http.StatusUnauthorized},
		{"wrong scope", e.work, http.StatusForbidden},
		{"admin", e.admin, http.StatusOK},
	}
	for _, c := range cases {
		resp, _ := e.do(t, "GET", "/v1/stats", c.key, nil)
		if resp.StatusCode != c.want {
			t.Errorf("%s: status %d, want %d", c.name, resp.StatusCode, c.want)
		}
	}
}

func TestRevokedKeysStopWorking(t *testing.T) {
	e := newEnv(t)
	key, secret, _ := e.keys.Create(context.Background(), "temp", []auth.Scope{auth.ScopeAdmin})
	if resp, _ := e.do(t, "GET", "/v1/stats", secret, nil); resp.StatusCode != 200 {
		t.Fatalf("before revoke: %d", resp.StatusCode)
	}
	e.keys.Revoke(context.Background(), key.ID) //nolint:errcheck
	if resp, _ := e.do(t, "GET", "/v1/stats", secret, nil); resp.StatusCode != 401 {
		t.Fatalf("after revoke: %d", resp.StatusCode)
	}
}

func TestKeysAreStoredHashed(t *testing.T) {
	e := newEnv(t)
	var leaked bool
	err := pool.QueryRow(context.Background(), `select exists (select 1 from conveyor_api_keys where key_hash = $1 or key_hash like '%cvk_%')`, e.admin).Scan(&leaked)
	if err != nil || leaked {
		t.Fatalf("plaintext key found in database (err=%v)", err)
	}
}

func TestEnqueueIsIdempotentAndValidated(t *testing.T) {
	e := newEnv(t)
	q := testdb.QueueName()
	body := map[string]any{"queue": q, "kind": "email.send", "payload": map[string]string{"to": "a@example.com"}, "idempotency_key": "signup-1"}
	resp, first := e.do(t, "POST", "/v1/jobs", e.producer, body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("first enqueue: %d %v", resp.StatusCode, first)
	}
	resp, second := e.do(t, "POST", "/v1/jobs", e.producer, body)
	if resp.StatusCode != http.StatusOK || second["id"] != first["id"] {
		t.Fatalf("replay: %d, ids %v vs %v", resp.StatusCode, second["id"], first["id"])
	}

	if resp, out := e.do(t, "POST", "/v1/jobs", e.producer, map[string]any{"queue": "NOT VALID", "kind": "x"}); resp.StatusCode != 400 {
		t.Fatalf("invalid queue: %d %v", resp.StatusCode, out)
	}
	if resp, _ := e.do(t, "POST", "/v1/jobs", e.producer, `{"queue":"a","kind":"x","surprise":1}`); resp.StatusCode != 400 {
		t.Fatalf("unknown field accepted: %d", resp.StatusCode)
	}
	if resp, _ := e.do(t, "POST", "/v1/jobs", e.producer, `{"queue":"a","kind":"x"} {"again":1}`); resp.StatusCode != 400 {
		t.Fatalf("trailing data accepted: %d", resp.StatusCode)
	}
	huge := `{"queue":"a","kind":"x","payload":"` + strings.Repeat("x", queue.MaxPayloadBytes+20<<10) + `"}`
	if resp, _ := e.do(t, "POST", "/v1/jobs", e.producer, huge); resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized body: %d", resp.StatusCode)
	}
	// Producers can't use admin endpoints.
	if resp, _ := e.do(t, "GET", "/v1/jobs", e.producer, nil); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("producer listed jobs: %d", resp.StatusCode)
	}
}

func TestRequiresJSONContentType(t *testing.T) {
	e := newEnv(t)
	req, _ := http.NewRequest("POST", e.srv.URL+"/v1/jobs", strings.NewReader(`{"queue":"a","kind":"x"}`))
	req.Header.Set("Authorization", "Bearer "+e.producer)
	req.Header.Set("Content-Type", "text/plain")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnsupportedMediaType {
		t.Fatalf("status %d", resp.StatusCode)
	}
}

func TestWorkerProtocolOverHTTP(t *testing.T) {
	e := newEnv(t)
	q := testdb.QueueName()
	e.do(t, "POST", "/v1/jobs", e.producer, map[string]any{"queue": q, "kind": "resize"})

	resp, out := e.do(t, "POST", "/v1/queues/"+q+"/claim", e.work, map[string]any{"limit": 5, "lease_seconds": 60})
	jobs := out["jobs"].([]any)
	if resp.StatusCode != 200 || len(jobs) != 1 {
		t.Fatalf("claim: %d %v", resp.StatusCode, out)
	}
	job := jobs[0].(map[string]any)
	id := int64(job["id"].(float64))
	token := job["lease_token"].(string)
	path := "/v1/jobs/" + jsonNumber(id)

	if resp, _ := e.do(t, "POST", path+"/heartbeat", e.work, map[string]any{"lease_token": token, "lease_seconds": 60}); resp.StatusCode != 200 {
		t.Fatalf("heartbeat: %d", resp.StatusCode)
	}
	if resp, _ := e.do(t, "POST", path+"/complete", e.work, map[string]any{"lease_token": "00000000-0000-0000-0000-000000000000"}); resp.StatusCode != http.StatusConflict {
		t.Fatalf("complete with wrong token: %d", resp.StatusCode)
	}
	if resp, _ := e.do(t, "POST", path+"/complete", e.work, map[string]any{"lease_token": token}); resp.StatusCode != 200 {
		t.Fatalf("complete: %d", resp.StatusCode)
	}

	// Status reads never expose the lease token.
	_, got := e.do(t, "GET", path, e.producer, nil)
	if got["status"] != "succeeded" {
		t.Fatalf("status = %v", got["status"])
	}
	if _, leaked := got["lease_token"]; leaked {
		t.Fatal("lease token exposed on GET")
	}
}

func TestAdminRetryAndMetrics(t *testing.T) {
	e := newEnv(t)
	q := testdb.QueueName()
	e.do(t, "POST", "/v1/jobs", e.producer, map[string]any{"queue": q, "kind": "x", "max_attempts": 1})
	_, out := e.do(t, "POST", "/v1/queues/"+q+"/claim", e.work, map[string]any{})
	job := out["jobs"].([]any)[0].(map[string]any)
	path := "/v1/jobs/" + jsonNumber(int64(job["id"].(float64)))
	_, failed := e.do(t, "POST", path+"/fail", e.work, map[string]any{"lease_token": job["lease_token"], "error": "boom"})
	if failed["status"] != "dead" {
		t.Fatalf("fail: %v", failed)
	}
	if resp, retried := e.do(t, "POST", path+"/retry", e.admin, nil); resp.StatusCode != 200 || retried["status"] != "queued" {
		t.Fatalf("retry: %d %v", resp.StatusCode, retried)
	}
	if resp, _ := e.do(t, "POST", path+"/retry", e.admin, nil); resp.StatusCode != http.StatusConflict {
		t.Fatalf("retrying a queued job: %d", resp.StatusCode)
	}

	req, _ := http.NewRequest("GET", e.srv.URL+"/metrics", nil)
	req.Header.Set("Authorization", "Bearer "+e.admin)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `conveyor_jobs{queue="`+q+`",status="queued"} 1`) {
		t.Fatalf("metrics missing queue:\n%s", body)
	}
}

func TestSecurityHeadersAndDashboard(t *testing.T) {
	e := newEnv(t)
	resp, err := http.Get(e.srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || !strings.Contains(string(body), "<title>Conveyor</title>") {
		t.Fatalf("dashboard: %d", resp.StatusCode)
	}
	for header, want := range map[string]string{
		"Content-Security-Policy": "script-src 'self'",
		"X-Content-Type-Options":  "nosniff",
		"Referrer-Policy":         "no-referrer",
	} {
		if !strings.Contains(resp.Header.Get(header), want) {
			t.Errorf("%s = %q", header, resp.Header.Get(header))
		}
	}
	if !strings.Contains(resp.Header.Get("Content-Security-Policy"), "frame-ancestors 'none'") {
		t.Error("CSP must forbid framing")
	}
}

func TestInvalidIDs(t *testing.T) {
	e := newEnv(t)
	for _, path := range []string{"/v1/jobs/abc", "/v1/jobs/-5", "/v1/jobs/0"} {
		if resp, _ := e.do(t, "GET", path, e.admin, nil); resp.StatusCode != 400 {
			t.Errorf("%s: %d", path, resp.StatusCode)
		}
	}
	if resp, _ := e.do(t, "GET", "/v1/jobs/999999999", e.admin, nil); resp.StatusCode != 404 {
		t.Errorf("missing job: %d", resp.StatusCode)
	}
}

func jsonNumber(n int64) string { b, _ := json.Marshal(n); return string(b) }
