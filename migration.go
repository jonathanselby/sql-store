package sqlstore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Migration contains trusted, ordered application-authored SQL statements.
// NonTransactional explicitly opts into fail-closed dirty-state recovery.
type Migration struct {
	Version          int64
	Name             string
	Statements       []string
	NonTransactional bool
}

func signature(m Migration) string {
	sum := sha256.New()
	fmt.Fprintf(sum, "%d\x00%s\x00%t\x00", m.Version, m.Name, m.NonTransactional)
	for _, s := range m.Statements {
		fmt.Fprintf(sum, "%s\x00", s)
	}
	return hex.EncodeToString(sum.Sum(nil))
}

func validateMigrations(ms []Migration) error {
	last := int64(0)
	for _, m := range ms {
		if m.Version <= last || strings.TrimSpace(m.Name) == "" || len(m.Statements) == 0 {
			return ErrMigration
		}
		for _, s := range m.Statements {
			if strings.TrimSpace(s) == "" {
				return ErrMigration
			}
		}
		last = m.Version
	}
	return nil
}

const migrationsDDL = `CREATE TABLE IF NOT EXISTS sqlstore_migrations (
 version BIGINT PRIMARY KEY, checksum TEXT NOT NULL,
 state TEXT NOT NULL CHECK (state IN ('started','applied')),
 applied_at TEXT NOT NULL)`

type rowExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

type migrationTxn struct {
	conn   *sql.Conn
	tx     *sql.Tx
	sqlite bool
	active bool
}

func (m *migrationTxn) ExecContext(ctx context.Context, q string, args ...any) (sql.Result, error) {
	if m.sqlite {
		return m.conn.ExecContext(ctx, q, args...)
	}
	return m.tx.ExecContext(ctx, q, args...)
}
func (m *migrationTxn) QueryContext(ctx context.Context, q string, args ...any) (*sql.Rows, error) {
	if m.sqlite {
		return m.conn.QueryContext(ctx, q, args...)
	}
	return m.tx.QueryContext(ctx, q, args...)
}
func (m *migrationTxn) Commit(ctx context.Context) error {
	if !m.active {
		return ErrMigration
	}
	m.active = false
	if m.sqlite {
		_, e := m.conn.ExecContext(ctx, "COMMIT")
		return e
	}
	return m.tx.Commit()
}
func (m *migrationTxn) Rollback() {
	if !m.active {
		return
	}
	m.active = false
	if m.sqlite {
		_, _ = m.conn.ExecContext(context.Background(), "ROLLBACK")
		return
	}
	_ = m.tx.Rollback()
}
func beginMigration(ctx context.Context, conn *sql.Conn, engine Engine) (*migrationTxn, error) {
	if engine == SQLite {
		_, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE")
		if err != nil {
			return nil, err
		}
		return &migrationTxn{conn: conn, sqlite: true, active: true}, nil
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &migrationTxn{conn: conn, tx: tx, active: true}, nil
}

type appliedMigration struct {
	version         int64
	checksum, state string
}

func history(ctx context.Context, ex rowExecutor) ([]appliedMigration, error) {
	rows, err := ex.QueryContext(ctx, "SELECT version,checksum,state FROM sqlstore_migrations ORDER BY version")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []appliedMigration
	for rows.Next() {
		var a appliedMigration
		if err := rows.Scan(&a.version, &a.checksum, &a.state); err != nil {
			return nil, err
		}
		result = append(result, a)
	}
	return result, rows.Err()
}
func verifyHistory(ms []Migration, applied []appliedMigration) error {
	if len(applied) > len(ms) {
		return ErrMigration
	}
	for i, a := range applied {
		if a.state != "applied" || a.version != ms[i].Version || a.checksum != signature(ms[i]) {
			return ErrMigration
		}
	}
	return nil
}
func placeholder(e Engine, n int) string {
	if e == PostgreSQL {
		return fmt.Sprintf("$%d", n)
	}
	return "?"
}
func migrationInsert(e Engine) string {
	return "INSERT INTO sqlstore_migrations(version,checksum,state,applied_at) VALUES(" +
		strings.Join([]string{placeholder(e, 1), placeholder(e, 2), placeholder(e, 3), placeholder(e, 4)}, ",") + ")"
}

// Migrate serialises startup changes using native database coordination.
// It never intercepts ordinary application database/sql calls.
func Migrate(ctx context.Context, db *sql.DB, engine Engine, ms []Migration) error {
	if db == nil || (engine != SQLite && engine != PostgreSQL) {
		return ErrConfiguration
	}
	if err := validateMigrations(ms); err != nil {
		return err
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return migrationError(ctx, err)
	}
	defer conn.Close()
	if engine == PostgreSQL {
		// Session advisory lock is held across ALL migrations, including
		// explicit PostgreSQL non-transactional statements.
		if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock(hashtextextended(current_database() || '.' || current_schema(), 737170541))"); err != nil {
			return migrationError(ctx, err)
		}
		defer func() {
			bound, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, _ = conn.ExecContext(bound, "SELECT pg_advisory_unlock(hashtextextended(current_database() || '.' || current_schema(), 737170541))")
		}()
	}
	// Recheck after acquiring the lock. SQLite uses BEGIN IMMEDIATE per
	// migration, guaranteeing that two processes cannot write a version twice.
	for {
		tx, err := beginMigration(ctx, conn, engine)
		if err != nil {
			return migrationError(ctx, err)
		}
		if _, err = tx.ExecContext(ctx, migrationsDDL); err != nil {
			tx.Rollback()
			return migrationError(ctx, err)
		}
		applied, err := history(ctx, tx)
		if err != nil {
			tx.Rollback()
			return migrationError(ctx, err)
		}
		if err = verifyHistory(ms, applied); err != nil {
			tx.Rollback()
			return err
		}
		if len(applied) == len(ms) {
			if err = tx.Commit(ctx); err != nil {
				return migrationError(ctx, err)
			}
			return nil
		}
		m := ms[len(applied)]
		if m.NonTransactional {
			// Persistent 'started' record is committed before any non-atomic
			// work. A crash demands explicit operator reconciliation.
			_, err = tx.ExecContext(ctx, migrationInsert(engine), m.Version, signature(m), "started", time.Now().UTC().Format(time.RFC3339Nano))
			if err != nil {
				tx.Rollback()
				return migrationError(ctx, err)
			}
			if err = tx.Commit(ctx); err != nil {
				return migrationError(ctx, err)
			}
			for _, stmt := range m.Statements {
				if _, err = conn.ExecContext(ctx, stmt); err != nil {
					return migrationError(ctx, err)
				}
			}
			finish, err := beginMigration(ctx, conn, engine)
			if err != nil {
				return migrationError(ctx, err)
			}
			q := "UPDATE sqlstore_migrations SET state='applied', applied_at=" + placeholder(engine, 1) + " WHERE version=" + placeholder(engine, 2) + " AND state='started' AND checksum=" + placeholder(engine, 3)
			res, err := finish.ExecContext(ctx, q, time.Now().UTC().Format(time.RFC3339Nano), m.Version, signature(m))
			if err != nil {
				finish.Rollback()
				return migrationError(ctx, err)
			}
			n, err := res.RowsAffected()
			if err != nil || n != 1 {
				finish.Rollback()
				return ErrMigration
			}
			if err = finish.Commit(ctx); err != nil {
				return migrationError(ctx, err)
			}
			// The next iteration verifies the completed ledger. A competing
			// SQLite migrator sees 'started' until completion and fails closed.
			continue
		}
		for _, stmt := range m.Statements {
			if _, err = tx.ExecContext(ctx, stmt); err != nil {
				tx.Rollback()
				return migrationError(ctx, err)
			}
		}
		if _, err = tx.ExecContext(ctx, migrationInsert(engine), m.Version, signature(m), "applied", time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			tx.Rollback()
			return migrationError(ctx, err)
		}
		if err = tx.Commit(ctx); err != nil {
			return migrationError(ctx, err)
		}
	}
}

func migrationError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if e := ctx.Err(); e != nil {
		return e
	}
	if errors.Is(err, ErrMigration) {
		return err
	}
	// Drivers can include secret-bearing URLs in their errors.
	return fmt.Errorf("%w: statement or coordination failed", ErrMigration)
}
