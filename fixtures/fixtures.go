// Package fixtures provides isolated, explicitly test-only database setup.
// It is not a general destructive reset API.
package fixtures

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	sqlstore "github.com/jonathanselby/sql-store"
)

var ErrUnowned = errors.New("sqlstore fixtures: unowned or non-test target")

// Fixture owns a disposable database and its cleanup function.
type Fixture struct {
	DB      *sql.DB
	Engine  sqlstore.Engine
	cleanup func() error
}

func (f *Fixture) Close() error {
	if f == nil || f.cleanup == nil {
		return ErrUnowned
	}
	fn := f.cleanup
	f.cleanup = nil
	return fn()
}

// SQLiteFixture creates one isolated, private temporary database.
func SQLiteFixture(ctx context.Context, migrations []sqlstore.Migration) (*Fixture, error) {
	dir, err := os.MkdirTemp("", "sqlstore-test-")
	if err != nil {
		return nil, err
	}
	db, err := sqlstore.Open(ctx, sqlstore.Config{Engine: sqlstore.SQLite, SQLitePath: filepath.Join(dir, "test.db")})
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}
	clean := func() error { e := db.Close(); _ = os.RemoveAll(dir); return e }
	if err := sqlstore.Migrate(ctx, db, sqlstore.SQLite, migrations); err != nil {
		_ = clean()
		return nil, err
	}
	return &Fixture{DB: db, Engine: sqlstore.SQLite, cleanup: clean}, nil
}

// PostgreSQLFixture creates a uniquely named schema on a LOCAL test database.
// It refuses remote hosts and database names without an explicit test suffix.
// Test credentials are deployment-owned and never persisted or logged.
func PostgreSQLFixture(ctx context.Context, dsn string, migrations []sqlstore.Migration) (*Fixture, error) {
	u, err := url.Parse(dsn)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		return nil, ErrUnowned
	}
	host := u.Hostname()
	ip := net.ParseIP(host)
	if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return nil, ErrUnowned
	}
	if !strings.HasPrefix(strings.Trim(u.Path, "/"), "sqlstore_test") {
		return nil, ErrUnowned
	}
	var bytes [12]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return nil, err
	}
	schema := "sqlstore_test_" + hex.EncodeToString(bytes[:])
	admin, err := sqlstore.Open(ctx, sqlstore.Config{Engine: sqlstore.PostgreSQL, PostgreSQLURL: dsn})
	if err != nil {
		return nil, err
	}
	// Name comes exclusively from cryptographic random hex, never caller SQL.
	if _, err := admin.ExecContext(ctx, `CREATE SCHEMA "`+schema+`"`); err != nil {
		_ = admin.Close()
		return nil, fmt.Errorf("fixture schema creation failed")
	}
	cleanAdmin := func() error {
		_, err := admin.ExecContext(context.Background(), `DROP SCHEMA "`+schema+`" CASCADE`)
		_ = admin.Close()
		return err
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	db, err := sqlstore.Open(ctx, sqlstore.Config{Engine: sqlstore.PostgreSQL, PostgreSQLURL: u.String()})
	if err != nil {
		_ = cleanAdmin()
		return nil, err
	}
	clean := func() error { _ = db.Close(); return cleanAdmin() }
	var current string
	if err := db.QueryRowContext(ctx, "SELECT current_schema()").Scan(&current); err != nil || current != schema {
		_ = clean()
		return nil, ErrUnowned
	}
	if err := sqlstore.Migrate(ctx, db, sqlstore.PostgreSQL, migrations); err != nil {
		_ = clean()
		return nil, err
	}
	return &Fixture{DB: db, Engine: sqlstore.PostgreSQL, cleanup: clean}, nil
}

// RefuseReset is deliberately not a production DB reset operation: only a
// Fixture that owns its disposal resources may be closed/cleaned.
func RefuseReset(db *sql.DB) error { return ErrUnowned }
