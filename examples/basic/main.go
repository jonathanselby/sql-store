// Small end-to-end application for SQLite or PostgreSQL. It intentionally
// owns its business schema, SQL dialect and publication decision.
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	store "github.com/jonathanselby/sql-store"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cfg := store.Config{Engine: store.SQLite, SQLitePath: filepath.Join(os.TempDir(), "garage-sqlstore-example.db")}
	if dsn := os.Getenv("SQLSTORE_EXAMPLE_POSTGRES_URL"); dsn != "" {
		cfg = store.Config{Engine: store.PostgreSQL, PostgreSQLURL: dsn}
	}
	if err := run(ctx, cfg); err != nil {
		fmt.Fprintln(os.Stderr, "example failed:", err)
		os.Exit(1)
	}
}
func run(ctx context.Context, cfg store.Config) error {
	db, err := store.Open(ctx, cfg)
	if err != nil {
		return err
	}
	defer db.Close()
	ms := []store.Migration{{Version: 1, Name: "example-business-table", Statements: []string{
		"CREATE TABLE example_items (id TEXT PRIMARY KEY, value TEXT NOT NULL)",
	}}}
	if err := store.Migrate(ctx, db, cfg.Engine, ms); err != nil {
		return err
	}
	ob, err := store.NewOutbox(cfg.Engine, store.OutboxOptions{})
	if err != nil {
		return err
	}
	if err := ob.Ensure(ctx, db); err != nil {
		return err
	}
	if err := store.Ready(ctx, db, 1); err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	q := "INSERT INTO example_items(id,value) VALUES(?,?)"
	if cfg.Engine == store.PostgreSQL {
		q = "INSERT INTO example_items(id,value) VALUES($1,$2)"
	}
	id := fmt.Sprintf("example-%d", time.Now().UnixNano())
	if _, err := tx.ExecContext(ctx, q, id, "business state"); err != nil {
		return err
	}
	event := store.Event{ID: id, Type: "example.created", Version: "1", Source: "garage:example",
		OccurredAt: time.Now().UTC(), ContentType: "application/json", Payload: []byte(`{"example":true}`)}
	if err := ob.Enqueue(ctx, tx, event); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	pending, err := ob.Pending(ctx, db)
	if err != nil {
		return err
	}
	fmt.Printf("engine=%s business_committed=true outbox_pending=%d\n", cfg.Engine, pending)
	// Transport is intentionally NOT simulated as durable. A separate
	// application publisher uses Event Stream and acknowledges only after
	// actual durable broker confirmation.
	return nil
}
