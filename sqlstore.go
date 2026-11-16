// Package sqlstore owns database mechanics; applications own schemas and queries.
package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"
)

// Engine describes the deployed database; it does not imply SQL dialect equivalence.
type Engine string

const (
	SQLite     Engine = "sqlite"
	PostgreSQL Engine = "postgresql"
)

var (
	ErrConfiguration     = errors.New("sqlstore: invalid configuration")
	ErrUnavailable       = errors.New("sqlstore: database unavailable")
	ErrNotReady          = errors.New("sqlstore: database not ready")
	ErrMigration         = errors.New("sqlstore: migration refused")
	ErrClaimLost         = errors.New("sqlstore: outbox claim no longer owned")
	ErrNonDurableReceipt = errors.New("sqlstore: publication proof is not durable")
)

// Config contains only deployment mechanics. PostgreSQLURL must come from a secret source.
type Config struct {
	Engine            Engine
	SQLitePath        string
	PostgreSQLURL     string
	SQLiteBusyTimeout time.Duration
	MaxOpenConns      int
	MaxIdleConns      int
	ConnMaxLifetime   time.Duration
}

// Capabilities deliberately avoids a generic database feature matrix.
type Capabilities struct{ Engine Engine }

func CapabilityOf(engine Engine) (Capabilities, error) {
	if engine != SQLite && engine != PostgreSQL {
		return Capabilities{}, ErrConfiguration
	}
	return Capabilities{Engine: engine}, nil
}

// Open validates configuration, establishes connectivity and returns the native pool.
// No operation is inserted into subsequent application queries or transactions.
func Open(ctx context.Context, cfg Config) (*sql.DB, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var driver, dsn string
	switch cfg.Engine {
	case SQLite:
		if cfg.SQLitePath == "" || cfg.PostgreSQLURL != "" || cfg.SQLitePath == ":memory:" {
			return nil, ErrConfiguration
		}
		path, err := filepath.Abs(cfg.SQLitePath)
		if err != nil {
			return nil, ErrConfiguration
		}
		info, err := os.Lstat(path)
		if err == nil && (!info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0) {
			return nil, ErrConfiguration
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, ErrConfiguration
		}
		if err == nil && info.Mode().Perm()&0077 != 0 {
			return nil, ErrConfiguration
		}
		// SQLite creates the database when opened; precreate with private permissions.
		if errors.Is(err, os.ErrNotExist) {
			f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
			if e != nil && !errors.Is(e, os.ErrExist) {
				return nil, ErrConfiguration
			}
			if e == nil {
				_ = f.Close()
			} else {
				actual, statErr := os.Lstat(path)
				if statErr != nil || !actual.Mode().IsRegular() || actual.Mode().Perm()&0077 != 0 {
					return nil, ErrConfiguration
				}
			}
		}
		// Every physical connection executes the DSN PRAGMAs. SQLite WAL
		// supports concurrent readers, but only one writer; do not mistake
		// a single writer for a reason to serialise all local reads.
		if cfg.MaxOpenConns > 32 || cfg.MaxIdleConns > 32 {
			return nil, ErrConfiguration
		}
		busy := cfg.SQLiteBusyTimeout
		if busy == 0 {
			busy = 250 * time.Millisecond
		}
		if busy < 0 || busy > 30*time.Second {
			return nil, ErrConfiguration
		}
		uri := url.URL{Scheme: "file", Path: path}
		q := url.Values{}
		q.Add("_pragma", "foreign_keys(ON)")
		q.Add("_pragma", "journal_mode(WAL)")
		q.Add("_pragma", "synchronous(FULL)")
		q.Add("_pragma", "busy_timeout("+strconv.FormatInt(busy.Milliseconds(), 10)+")")
		uri.RawQuery = q.Encode()
		dsn = uri.String()
		driver = "sqlite"
	case PostgreSQL:
		if cfg.PostgreSQLURL == "" || cfg.SQLitePath != "" {
			return nil, ErrConfiguration
		}
		driver, dsn = "pgx", cfg.PostgreSQLURL
	default:
		return nil, ErrConfiguration
	}
	if cfg.MaxOpenConns < 0 || cfg.MaxIdleConns < 0 || cfg.ConnMaxLifetime < 0 {
		return nil, ErrConfiguration
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, fmt.Errorf("%w: connection configuration rejected", ErrUnavailable)
	}
	if cfg.Engine == SQLite {
		maxOpen := cfg.MaxOpenConns
		if maxOpen == 0 {
			maxOpen = 4
		}
		maxIdle := cfg.MaxIdleConns
		if maxIdle == 0 {
			maxIdle = 2
		}
		if maxIdle > maxOpen {
			_ = db.Close()
			return nil, ErrConfiguration
		}
		db.SetMaxOpenConns(maxOpen)
		db.SetMaxIdleConns(maxIdle)
	} else {
		if cfg.MaxOpenConns > 0 {
			db.SetMaxOpenConns(cfg.MaxOpenConns)
		}
		if cfg.MaxIdleConns > 0 {
			db.SetMaxIdleConns(cfg.MaxIdleConns)
		}
		if cfg.ConnMaxLifetime > 0 {
			db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
		}
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		if e := ctx.Err(); e != nil {
			return nil, e
		}
		return nil, fmt.Errorf("%w: connection failed", ErrUnavailable)
	}
	if cfg.Engine == SQLite {
		var journal string
		// The same DSN applies foreign_keys, WAL, FULL synchronous and
		// busy_timeout to each separately opened physical SQLite connection.
		var fk int
		if err := db.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&fk); err != nil || fk != 1 {
			_ = db.Close()
			return nil, fmt.Errorf("%w: foreign keys disabled", ErrUnavailable)
		}
		if err := db.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journal); err != nil || journal != "wal" {
			_ = db.Close()
			return nil, fmt.Errorf("%w: WAL unavailable", ErrUnavailable)
		}
	}
	return db, nil
}

// Health performs one explicit bounded reachability probe, never a per-query probe.
func Health(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return ErrConfiguration
	}
	bound, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := db.PingContext(bound); err != nil {
		if e := bound.Err(); e != nil {
			return e
		}
		return ErrUnavailable
	}
	return nil
}

// Ready compares the authoritative migration ledger with the required schema version.
func Ready(ctx context.Context, db *sql.DB, expectedVersion int64) error {
	if expectedVersion < 0 {
		return ErrConfiguration
	}
	if err := Health(ctx, db); err != nil {
		return err
	}
	rows, err := db.QueryContext(ctx, "SELECT version, state FROM sqlstore_migrations ORDER BY version")
	if err != nil {
		return ErrNotReady
	}
	defer rows.Close()
	var last int64
	for rows.Next() {
		var v int64
		var state string
		if err := rows.Scan(&v, &state); err != nil || state != "applied" || v <= last {
			return ErrNotReady
		}
		last = v
	}
	if rows.Err() != nil || last != expectedVersion {
		return ErrNotReady
	}
	return nil
}
