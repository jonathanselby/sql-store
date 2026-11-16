package sqlstore_test

import (
	"context"
	"errors"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	store "github.com/jonathanselby/sql-store"
	"github.com/jonathanselby/sql-store/fixtures"
)

func newPostgresFixture(t *testing.T, ms []store.Migration) *fixtures.Fixture {
	t.Helper()
	dsn := os.Getenv("SQLSTORE_POSTGRES_TEST_URL")
	if dsn == "" {
		t.Skip("set SQLSTORE_POSTGRES_TEST_URL for real PostgreSQL integration tests")
	}
	f, err := fixtures.PostgreSQLFixture(context.Background(), dsn, ms)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.Close(); err != nil {
			t.Error(err)
		}
	})
	return f
}
func TestPostgresFixtureIsolation(t *testing.T) {
	a := newPostgresFixture(t, base)
	b := newPostgresFixture(t, base)
	if _, err := a.DB.Exec("INSERT INTO items VALUES ('only-a','a')"); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := b.DB.QueryRow("SELECT count(*) FROM items").Scan(&n); err != nil || n != 0 {
		t.Fatal("fixture leakage", err)
	}
}
func TestSQLiteFixtureIsolation(t *testing.T) {
	a, err := fixtures.SQLiteFixture(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := fixtures.SQLiteFixture(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	_, err = a.DB.Exec("INSERT INTO items VALUES ('only-a','a')")
	if err != nil {
		t.Fatal(err)
	}
	var n int
	_ = b.DB.QueryRow("SELECT count(*) FROM items").Scan(&n)
	if n != 0 {
		t.Fatal("fixture leakage")
	}
}
func TestResetRefusesUnownedTarget(t *testing.T) {
	if err := fixtures.RefuseReset(nil); !errors.Is(err, fixtures.ErrUnowned) {
		t.Fatal("unowned reset")
	}
}
func TestPostgresConcurrentMigration(t *testing.T) {
	f := newPostgresFixture(t, nil)
	ctx := context.Background()
	// Separate native connection pools against the same isolated schema.
	// PostgreSQL migration advisory lock prevents duplicate DDL.
	var wg sync.WaitGroup
	errorsChan := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errorsChan <- store.Migrate(ctx, f.DB, store.PostgreSQL, base) }()
	}
	wg.Wait()
	close(errorsChan)
	for e := range errorsChan {
		if e != nil {
			t.Fatal(e)
		}
	}
	var n int
	if err := f.DB.QueryRow("SELECT count(*) FROM sqlstore_migrations").Scan(&n); err != nil || n != 1 {
		t.Fatal("concurrent migration mismatch", err)
	}
}
func TestPostgresPoolDeadline(t *testing.T) {
	f := newPostgresFixture(t, nil)
	// A saturated native pool waits only until the caller's deadline.
	f.DB.SetMaxOpenConns(1)
	conn, err := f.DB.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = f.DB.ExecContext(ctx, "SELECT 1")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unbounded pool wait: %v", err)
	}
}
func TestPostgresRollbackAndOutbox(t *testing.T) {
	f := newPostgresFixture(t, base)
	ctx := context.Background()
	ob, err := store.NewOutbox(store.PostgreSQL, store.OutboxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := ob.Ensure(ctx, f.DB); err != nil {
		t.Fatal(err)
	}
	tx, err := f.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO items VALUES ($1,$2)", "x", "a")
	if err != nil {
		t.Fatal(err)
	}
	if err := ob.Enqueue(ctx, tx, makeEvent("postgres")); err != nil {
		t.Fatal(err)
	}
	_ = tx.Rollback()
	n, _ := ob.Pending(ctx, f.DB)
	if n != 0 {
		t.Fatal("Postgres atomicity failure")
	}
	tx, err = f.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO items VALUES ($1,$2)", "x", "a")
	if err != nil {
		t.Fatal(err)
	}
	if err := ob.Enqueue(ctx, tx, makeEvent("postgres")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	claims, err := ob.Claim(ctx, f.DB, 1)
	if err != nil || len(claims) != 1 {
		t.Fatalf("Postgres claim %v %v", claims, err)
	}
	if err := ob.MarkPublished(ctx, f.DB, "postgres", claims[0].ClaimToken, store.PublicationProof{Transport: "kafka", Durable: true, Reference: "kafka:0"}); err != nil {
		t.Fatal(err)
	}
}
func TestPostgresBackupRestore(t *testing.T) {
	// PostgreSQL dump/restore is external deployment tooling, not a Go runtime dependency.
	dsn := os.Getenv("SQLSTORE_POSTGRES_TEST_URL")
	if dsn == "" {
		t.Skip("no PostgreSQL test service")
	}
	if _, err := exec.LookPath("pg_dump"); err != nil {
		t.Skip("pg_dump is not installed")
	}
	if _, err := exec.LookPath("pg_restore"); err != nil {
		t.Skip("pg_restore is not installed")
	}
	f := newPostgresFixture(t, base)
	ctx := context.Background()
	var schema string
	if err := f.DB.QueryRowContext(ctx, "SELECT current_schema()").Scan(&schema); err != nil {
		t.Fatal(err)
	}
	_, err := f.DB.ExecContext(ctx, "INSERT INTO items VALUES ('backup','proof')")
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "backup.dump")
	conn, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	password, _ := conn.User.Password()
	clientEnv := append(os.Environ(),
		"PGHOST="+conn.Hostname(), "PGPORT="+conn.Port(), "PGUSER="+conn.User.Username(),
		"PGPASSWORD="+password, "PGDATABASE="+strings.TrimPrefix(conn.Path, "/"), "PGSSLMODE=disable")
	dump := exec.CommandContext(ctx, "pg_dump", "--format=custom", "--schema="+schema, "--file="+target)
	dump.Env = clientEnv
	if err := dump.Run(); err != nil {
		t.Fatal("pg_dump failed: ", err)
	}
	// Restore into the same DISPOSABLE test schema and verify the data.
	if _, err := f.DB.ExecContext(ctx, "DELETE FROM items WHERE id='backup'"); err != nil {
		t.Fatal(err)
	}
	restore := exec.CommandContext(ctx, "pg_restore", "--clean", "--if-exists", "--dbname="+strings.TrimPrefix(conn.Path, "/"), target)
	restore.Env = clientEnv
	if output, err := restore.CombinedOutput(); err != nil {
		t.Fatalf("pg_restore failed: %v %s", err, string(output))
	}
	var value string
	if err := f.DB.QueryRowContext(ctx, "SELECT value FROM items WHERE id='backup'").Scan(&value); err != nil || value != "proof" {
		t.Fatalf("restored value: %q %v", value, err)
	}
}
func BenchmarkDirectSQLVsSQLStore(b *testing.B) {
	path := filepath.Join(b.TempDir(), "benchmark.db")
	db, err := store.Open(context.Background(), store.Config{Engine: store.SQLite, SQLitePath: path})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	if err := store.Migrate(context.Background(), db, store.SQLite, base); err != nil {
		b.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO items VALUES ('k','v')"); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var s string
		if err := db.QueryRow("SELECT value FROM items WHERE id='k'").Scan(&s); err != nil {
			b.Fatal(err)
		}
	}
}
