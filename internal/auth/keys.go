// Package auth manages API keys. Keys are random 256-bit secrets shown once at
// creation; only their SHA-256 hash is stored.
package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Scope string

const (
	ScopeEnqueue Scope = "enqueue" // add jobs and read their status
	ScopeWork    Scope = "work"    // claim, heartbeat, complete and fail jobs
	ScopeAdmin   Scope = "admin"   // everything, including retry/cancel, listing and stats
)

const keyPrefix = "cvk_"

var ErrInvalidKey = errors.New("invalid or revoked API key")

type Key struct {
	ID         int64      `json:"id"`
	Name       string     `json:"name"`
	Scopes     []Scope    `json:"scopes"`
	CreatedAt  time.Time  `json:"created_at"`
	LastUsedAt *time.Time `json:"last_used_at,omitempty"`
	RevokedAt  *time.Time `json:"revoked_at,omitempty"`
}

// Allows reports whether the key grants scope (admin grants everything).
func (k Key) Allows(scope Scope) bool {
	return slices.Contains(k.Scopes, scope) || slices.Contains(k.Scopes, ScopeAdmin)
}

func ParseScopes(csv string) ([]Scope, error) {
	var out []Scope
	for _, s := range strings.Split(csv, ",") {
		s = strings.TrimSpace(s)
		switch Scope(s) {
		case ScopeEnqueue, ScopeWork, ScopeAdmin:
			if !slices.Contains(out, Scope(s)) {
				out = append(out, Scope(s))
			}
		case "":
		default:
			return nil, fmt.Errorf("unknown scope %q (use enqueue, work, admin)", s)
		}
	}
	if len(out) == 0 {
		return nil, errors.New("at least one scope is required")
	}
	return out, nil
}

func hash(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

type Keys struct{ pool *pgxpool.Pool }

func NewKeys(pool *pgxpool.Pool) *Keys { return &Keys{pool: pool} }

// Create stores a new key and returns its plaintext, which can't be recovered later.
func (k *Keys) Create(ctx context.Context, name string, scopes []Scope) (Key, string, error) {
	name = strings.TrimSpace(name)
	if name == "" || len(name) > 100 {
		return Key{}, "", errors.New("name must be 1-100 characters")
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return Key{}, "", err
	}
	secret := keyPrefix + base64.RawURLEncoding.EncodeToString(raw)
	var key Key
	err := k.pool.QueryRow(ctx, `
		insert into conveyor_api_keys (name, key_hash, scopes) values ($1, $2, $3)
		returning id, name, scopes, created_at`, name, hash(secret), scopes).
		Scan(&key.ID, &key.Name, &key.Scopes, &key.CreatedAt)
	return key, secret, err
}

// Authenticate resolves a presented key. Lookup is by hash, so the comparison never
// touches the plaintext and timing doesn't reveal partial matches.
func (k *Keys) Authenticate(ctx context.Context, secret string) (Key, error) {
	if !strings.HasPrefix(secret, keyPrefix) || len(secret) != len(keyPrefix)+43 {
		return Key{}, ErrInvalidKey
	}
	var key Key
	err := k.pool.QueryRow(ctx, `
		select id, name, scopes, created_at from conveyor_api_keys
		where key_hash = $1 and revoked_at is null`, hash(secret)).
		Scan(&key.ID, &key.Name, &key.Scopes, &key.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Key{}, ErrInvalidKey
	}
	if err != nil {
		return Key{}, err
	}
	// Record usage at most once a minute so busy workers don't write on every request.
	_, _ = k.pool.Exec(ctx, `update conveyor_api_keys set last_used_at = now()
		where id = $1 and (last_used_at is null or last_used_at < now() - interval '1 minute')`, key.ID)
	return key, nil
}

func (k *Keys) List(ctx context.Context) ([]Key, error) {
	rows, err := k.pool.Query(ctx, `select id, name, scopes, created_at, last_used_at, revoked_at from conveyor_api_keys order by id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Key
	for rows.Next() {
		var key Key
		if err := rows.Scan(&key.ID, &key.Name, &key.Scopes, &key.CreatedAt, &key.LastUsedAt, &key.RevokedAt); err != nil {
			return nil, err
		}
		out = append(out, key)
	}
	return out, rows.Err()
}

func (k *Keys) Revoke(ctx context.Context, id int64) error {
	tag, err := k.pool.Exec(ctx, `update conveyor_api_keys set revoked_at = now() where id = $1 and revoked_at is null`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return errors.New("no active key with that id")
	}
	return nil
}
