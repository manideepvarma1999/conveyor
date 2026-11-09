// Package testdb gives tests a migrated PostgreSQL database.
//
// CI sets CONVEYOR_TEST_DATABASE_URL (a Postgres service container). Locally, an
// embedded Postgres is started in a temp directory, so no Docker or setup is needed.
package testdb

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"os"
	"testing"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/manideepvarma1999/conveyor/queue"
)

// Run starts the database, runs the package's tests, and stops it. Use from TestMain.
func Run(m *testing.M, out **pgxpool.Pool) int {
	url := os.Getenv("CONVEYOR_TEST_DATABASE_URL")
	if url == "" {
		dir, err := os.MkdirTemp("", "conveyor-test-")
		if err != nil {
			panic(err)
		}
		defer os.RemoveAll(dir)
		port := freePort()
		password := randomHex(16)
		pg := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().
			Username("conveyor").Password(password).Database("conveyor_test").
			Port(uint32(port)).RuntimePath(dir).Logger(io.Discard))
		if err := pg.Start(); err != nil {
			panic(fmt.Sprintf("start embedded postgres: %v", err))
		}
		defer pg.Stop() //nolint:errcheck
		url = fmt.Sprintf("postgres://conveyor:%s@localhost:%d/conveyor_test?sslmode=disable", password, port)
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		panic(err)
	}
	defer pool.Close()
	if err := queue.Migrate(ctx, pool); err != nil {
		panic(err)
	}
	*out = pool
	return m.Run()
}

// QueueName returns a unique queue name so tests can run in parallel without interfering.
func QueueName() string { return "test-" + randomHex(6) }

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

func freePort() int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}
