package sqlstore_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	store "github.com/jonathanselby/sql-store"
)

var base = []store.Migration{{Version: 1, Name: "items", Statements: []string{
	"CREATE TABLE items (id TEXT PRIMARY KEY, value TEXT NOT NULL)",
}}}

func openSQLite(t *testing.T) *sql.DB {
	t.Helper()
	db, err := store.Open(context.Background(), store.Config{Engine: store.SQLite, SQLitePath: filepath.Join(t.TempDir(), "store.db")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}
func openPostgres(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("SQLSTORE_POSTGRES_TEST_URL")
	if dsn == "" {
		t.Skip("PostgreSQL test service is not configured")
	}
	// Dedicated CI database. Root production databases are never touched.
	db, err := store.Open(context.Background(), store.Config{Engine: store.PostgreSQL, PostgreSQLURL: dsn})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}
func TestSQLiteContract(t *testing.T) {
	db := openSQLite(t)
	ctx := context.Background()
	if err := store.Migrate(ctx, db, store.SQLite, base); err != nil {
		t.Fatal(err)
	}
	if err := store.Ready(ctx, db, 1); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO items(id,value) VALUES(?,?)", "a", "ok"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var value string
	if err := db.QueryRowContext(ctx, "SELECT value FROM items WHERE id=?", "a").Scan(&value); err != nil || value != "ok" {
		t.Fatalf("query: %q %v", value, err)
	}
	if err := store.Health(ctx, db); err != nil {
		t.Fatal(err)
	}
}
func TestPostgresContract(t *testing.T) {
	ctx := context.Background()
	// Postgres fixtures need separate schemas to prevent shared-table collisions.
	f := newPostgresFixture(t, base)
	db := f.DB
	if err := store.Ready(ctx, db, 1); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO items(id,value) VALUES($1,$2)", "a", "ok"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var got string
	if err := db.QueryRowContext(ctx, "SELECT value FROM items WHERE id=$1", "a").Scan(&got); err != nil || got != "ok" {
		t.Fatalf("query: %q %v", got, err)
	}
}
func TestTransactionRollback(t *testing.T) {
	for _, tc := range []struct {
		name   string
		engine store.Engine
		open   func(*testing.T) *sql.DB
	}{
		{"sqlite", store.SQLite, openSQLite},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := tc.open(t)
			if err := store.Migrate(ctx, db, tc.engine, base); err != nil {
				t.Fatal(err)
			}
			tx, _ := db.BeginTx(ctx, nil)
			_, err := tx.ExecContext(ctx, "INSERT INTO items(id,value) VALUES(?,?)", "a", "x")
			if err != nil {
				t.Fatal(err)
			}
			_ = tx.Rollback()
			var count int
			_ = db.QueryRowContext(ctx, "SELECT count(*) FROM items").Scan(&count)
			if count != 0 {
				t.Fatalf("partial state: %d", count)
			}
		})
	}
}
func TestMigrationFailureNotReady(t *testing.T) {
	db := openSQLite(t)
	ms := []store.Migration{{Version: 1, Name: "bad", Statements: []string{"CREATE TABL invalid"}}}
	if err := store.Migrate(context.Background(), db, store.SQLite, ms); err == nil {
		t.Fatal("accepted broken SQL")
	}
	if !errors.Is(store.Ready(context.Background(), db, 1), store.ErrNotReady) {
		t.Fatal("partially migrated store ready")
	}
}
func TestMigrationVersioning(t *testing.T) {
	db := openSQLite(t)
	ctx := context.Background()
	if err := store.Migrate(ctx, db, store.SQLite, base); err != nil {
		t.Fatal(err)
	}
	if err := store.Migrate(ctx, db, store.SQLite, base); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM sqlstore_migrations").Scan(&n); err != nil || n != 1 {
		t.Fatalf("version repeated: %d %v", n, err)
	}
}
func TestConcurrentMigration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "concurrent.db")
	ctx := context.Background()
	d1, err := store.Open(ctx, store.Config{Engine: store.SQLite, SQLitePath: path, SQLiteBusyTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer d1.Close()
	d2, err := store.Open(ctx, store.Config{Engine: store.SQLite, SQLitePath: path, SQLiteBusyTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, db := range []*sql.DB{d1, d2} {
		wg.Add(1)
		go func(db *sql.DB) { defer wg.Done(); errs <- store.Migrate(ctx, db, store.SQLite, base) }(db)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	var n int
	if err := d1.QueryRowContext(ctx, "SELECT count(*) FROM sqlstore_migrations").Scan(&n); err != nil || n != 1 {
		t.Fatalf("double migration %d %v", n, err)
	}
}
func TestNewerSchemaNotReady(t *testing.T) {
	db := openSQLite(t)
	ctx := context.Background()
	if err := store.Migrate(ctx, db, store.SQLite, base); err != nil {
		t.Fatal(err)
	}
	if err := store.Ready(ctx, db, 0); !errors.Is(err, store.ErrNotReady) {
		t.Fatalf("expected refusal: %v", err)
	}
	if err := store.Migrate(ctx, db, store.SQLite, nil); !errors.Is(err, store.ErrMigration) {
		t.Fatalf("downgrade? %v", err)
	}
}
func TestNonTransactionalMigrationFailClosed(t *testing.T) {
	db := openSQLite(t)
	ctx := context.Background()
	broken := []store.Migration{{Version: 1, Name: "bad-nontransactional", NonTransactional: true, Statements: []string{"THIS SQL IS INVALID"}}}
	if err := store.Migrate(ctx, db, store.SQLite, broken); err == nil {
		t.Fatal("expected error")
	}
	if err := store.Ready(ctx, db, 1); !errors.Is(err, store.ErrNotReady) {
		t.Fatal("dirty migration ready")
	}
	if err := store.Migrate(ctx, db, store.SQLite, broken); !errors.Is(err, store.ErrMigration) {
		t.Fatal("dirty migration retried")
	}
}
func TestContextCancellation(t *testing.T) {
	db := openSQLite(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := db.ExecContext(ctx, "SELECT 1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("context missing: %v", err)
	}
	if err := store.Migrate(ctx, db, store.SQLite, base); !errors.Is(err, context.Canceled) {
		t.Fatalf("migration context missing: %v", err)
	}
}
func TestCapabilities(t *testing.T) {
	for _, e := range []store.Engine{store.SQLite, store.PostgreSQL} {
		c, err := store.CapabilityOf(e)
		if err != nil || c.Engine != e {
			t.Fatal(e, err)
		}
	}
	if _, err := store.CapabilityOf("mysql"); err == nil {
		t.Fatal("invalid engine allowed")
	}
}
func TestSQLiteBusyDeadline(t *testing.T) {
	path := filepath.Join(t.TempDir(), "busy.db")
	a, err := store.Open(context.Background(), store.Config{Engine: store.SQLite, SQLitePath: path, SQLiteBusyTimeout: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := store.Open(context.Background(), store.Config{Engine: store.SQLite, SQLitePath: path, SQLiteBusyTimeout: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := store.Migrate(context.Background(), a, store.SQLite, base); err != nil {
		t.Fatal(err)
	}
	tx, _ := a.BeginTx(context.Background(), nil)
	_, err = tx.ExecContext(context.Background(), "INSERT INTO items VALUES('locked','row')")
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = b.ExecContext(ctx, "INSERT INTO items VALUES('other','row')")
	_ = tx.Rollback()
	if err == nil || time.Since(start) > 2*time.Second {
		t.Fatalf("unbounded busy wait: %v", err)
	}
}
func TestCredentialRedaction(t *testing.T) {
	secret := "VERY_SECRET_PROBE"
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := store.Open(ctx, store.Config{Engine: store.PostgreSQL, PostgreSQLURL: "postgres://test:" + secret + "@127.0.0.1:1/nope?sslmode=disable"})
	if err == nil {
		t.Fatal("unexpected connection")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatal("credential leaked")
	}
}
func TestSQLiteBackupRestore(t *testing.T) {
	db := openSQLite(t)
	ctx := context.Background()
	if err := store.Migrate(ctx, db, store.SQLite, base); err != nil {
		t.Fatal(err)
	}
	_, _ = db.ExecContext(ctx, "INSERT INTO items VALUES ('before','snapshot')")
	target := filepath.Join(t.TempDir(), "backup.db")
	if err := store.BackupSQLite(ctx, db, target); err != nil {
		t.Fatal(err)
	}
	restored, err := store.Open(ctx, store.Config{Engine: store.SQLite, SQLitePath: target})
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	var value string
	if err := restored.QueryRowContext(ctx, "SELECT value FROM items WHERE id='before'").Scan(&value); err != nil || value != "snapshot" {
		t.Fatalf("backup: %v", err)
	}
	if err := store.BackupSQLite(ctx, db, target); err == nil {
		t.Fatal("backup overwrite allowed")
	}
}
func TestEngineSpecificContracts(t *testing.T) {
	_, err := store.Open(context.Background(), store.Config{Engine: store.SQLite, SQLitePath: ":memory:"})
	if !errors.Is(err, store.ErrConfiguration) {
		t.Fatal("memory connection requires fixture")
	}
	if p := store.PostgresBackupPlan(); p.Tool != "pg_dump" || !p.CredentialsExternal {
		t.Fatal("Postgres backup ownership")
	}
}
func TestVersion(t *testing.T) {
	bytes, err := os.ReadFile("VERSION")
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(bytes)) != store.Version {
		t.Fatal("version mismatch")
	}
}
