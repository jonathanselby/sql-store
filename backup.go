package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
)

// BackupSQLite invokes SQLite's consistent VACUUM INTO snapshot. It never
// copies a live WAL file. The destination must not exist and remains private.
func BackupSQLite(ctx context.Context, db *sql.DB, destination string) error {
	if db == nil || destination == "" {
		return ErrConfiguration
	}
	dest, err := filepath.Abs(destination)
	if err != nil {
		return ErrConfiguration
	}
	_, err = os.Lstat(dest)
	if err == nil {
		return ErrConfiguration
	}
	if !errors.Is(err, os.ErrNotExist) {
		return ErrConfiguration
	}
	// Pre-create an empty destination with mode 0600. SQLite accepts an empty
	// existing file for VACUUM INTO and writes a transactionally consistent DB.
	f, err := os.OpenFile(dest, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return ErrConfiguration
	}
	_ = f.Close()
	if _, err := db.ExecContext(ctx, "VACUUM INTO ?", dest); err != nil {
		_ = os.Remove(dest)
		if e := ctx.Err(); e != nil {
			return e
		}
		return ErrUnavailable
	}
	return nil
}

// PostgreSQLBackupPlan is documentation/metadata for deployment-owned pg_dump.
// SQL Store never executes external tools or receives backup credentials.
type PostgreSQLBackupPlan struct {
	Tool                string
	Format              string
	CredentialsExternal bool
	ConsistentSnapshot  bool
}

func PostgresBackupPlan() PostgreSQLBackupPlan {
	return PostgreSQLBackupPlan{
		Tool: "pg_dump", Format: "custom", CredentialsExternal: true, ConsistentSnapshot: true,
	}
}
