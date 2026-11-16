package sqlstore_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	store "github.com/jonathanselby/sql-store"
)

func makeEvent(id string) store.Event {
	return store.Event{ID: id, Type: "case.approved", Version: "1", Source: "garage:test", Subject: "case/1",
		OccurredAt: time.Now().UTC(), CorrelationID: "c-1", CausationID: "cmd-1",
		ContentType: "application/json", Schema: "schema/1", Key: "case/1", Payload: []byte(`{"ok":true}`)}
}
func setupOutbox(t *testing.T) (*sql.DB, *store.Outbox) {
	t.Helper()
	db := openSQLite(t)
	if err := store.Migrate(context.Background(), db, store.SQLite, base); err != nil {
		t.Fatal(err)
	}
	ob, err := store.NewOutbox(store.SQLite, store.OutboxOptions{LeaseDuration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := ob.Ensure(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	return db, ob
}
func enqueue(t *testing.T, db *sql.DB, ob *store.Outbox, id string, commit bool) {
	t.Helper()
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO items(id,value) VALUES(?,?)", id, "business"); err != nil {
		t.Fatal(err)
	}
	if err := ob.Enqueue(ctx, tx, makeEvent(id)); err != nil {
		t.Fatal(err)
	}
	if commit {
		err = tx.Commit()
	} else {
		err = tx.Rollback()
	}
	if err != nil {
		t.Fatal(err)
	}
}
func TestTransactionalOutboxAtomicity(t *testing.T) {
	db, ob := setupOutbox(t)
	ctx := context.Background()
	enqueue(t, db, ob, "rolled-back", false)
	var n int
	_ = db.QueryRowContext(ctx, "SELECT count(*) FROM items").Scan(&n)
	if n != 0 {
		t.Fatal("partial business state")
	}
	count, err := ob.Pending(ctx, db)
	if err != nil || count != 0 {
		t.Fatalf("partial outbox state: %d %v", count, err)
	}
	enqueue(t, db, ob, "committed", true)
	count, err = ob.Pending(ctx, db)
	if err != nil || count != 1 {
		t.Fatalf("missing committed event: %d %v", count, err)
	}
}
func TestOutboxRecovery(t *testing.T) {
	db, ob := setupOutbox(t)
	ctx := context.Background()
	enqueue(t, db, ob, "recover", true)
	// Simulate transport failure: no acknowledgement; release for fast retry.
	claims, err := ob.Claim(ctx, db, 1)
	if err != nil || len(claims) != 1 {
		t.Fatalf("claim: %v %v", claims, err)
	}
	if err := ob.ReleaseClaim(ctx, db, "recover", claims[0].ClaimToken); err != nil {
		t.Fatal(err)
	}
	claims, err = ob.Claim(ctx, db, 1)
	if err != nil || len(claims) != 1 {
		t.Fatal("not recovered", err)
	}
	if err := ob.MarkPublished(ctx, db, "recover", claims[0].ClaimToken, store.PublicationProof{Transport: "kafka", Durable: true, Reference: "kafka:0:12"}); err != nil {
		t.Fatal(err)
	}
	count, _ := ob.Pending(ctx, db)
	if count != 0 {
		t.Fatal("already published remains pending")
	}
}
func TestOutboxAckCrashWindow(t *testing.T) {
	db, ob := setupOutbox(t)
	ctx := context.Background()
	enqueue(t, db, ob, "crash", true)
	claims, err := ob.Claim(ctx, db, 1)
	if err != nil || len(claims) != 1 {
		t.Fatal(err)
	}
	// Ack happened in Kafka but caller crashed before MarkPublished.
	time.Sleep(1100 * time.Millisecond)
	later, err := ob.Claim(ctx, db, 1)
	if err != nil || len(later) != 1 {
		t.Fatal("not redelivered", err)
	}
	if later[0].Event.ID != "crash" || later[0].Attempts != 2 {
		t.Fatal("lost event identity/recovery")
	}
	if err := ob.MarkPublished(ctx, db, "crash", claims[0].ClaimToken, store.PublicationProof{Transport: "kafka", Durable: true, Reference: "old"}); !errors.Is(err, store.ErrClaimLost) {
		t.Fatal("stale publisher succeeded", err)
	}
}
func TestStaleClaimCannotMarkPublished(t *testing.T) { TestOutboxAckCrashWindow(t) }
func TestOutboxClaimRecovery(t *testing.T)           { TestOutboxAckCrashWindow(t) }
func TestEmbeddedReceiptDoesNotMarkPublished(t *testing.T) {
	db, ob := setupOutbox(t)
	ctx := context.Background()
	enqueue(t, db, ob, "embedded", true)
	got, err := ob.Claim(ctx, db, 1)
	if err != nil || len(got) != 1 {
		t.Fatal(err)
	}
	err = ob.MarkPublished(ctx, db, "embedded", got[0].ClaimToken, store.PublicationProof{Transport: "embedded", Durable: false, Reference: "embedded accepted"})
	if !errors.Is(err, store.ErrNonDurableReceipt) {
		t.Fatalf("memory accepted as durable: %v", err)
	}
	if err := ob.MarkPublished(ctx, db, "embedded", got[0].ClaimToken, store.PublicationProof{Transport: "embedded", Durable: true, Reference: "fake"}); !errors.Is(err, store.ErrNonDurableReceipt) {
		t.Fatalf("embedded receipt bypassed durability check: %v", err)
	}
	published, _ := ob.IsPublished(ctx, db, "embedded")
	if published {
		t.Fatal("memory acknowledgment persisted")
	}
}
func TestOutboxEnvelope(t *testing.T) {
	db, ob := setupOutbox(t)
	ctx := context.Background()
	original := makeEvent("envelope")
	tx, _ := db.BeginTx(ctx, nil)
	if err := ob.Enqueue(ctx, tx, original); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	got, err := ob.Claim(ctx, db, 1)
	if err != nil || len(got) != 1 {
		t.Fatal("claim", err)
	}
	e := got[0].Event
	if e.ID != original.ID || e.Type != original.Type || e.Version != original.Version || e.Source != original.Source ||
		e.Subject != original.Subject || e.CorrelationID != original.CorrelationID || e.CausationID != original.CausationID ||
		e.ContentType != original.ContentType || e.Schema != original.Schema || e.Key != original.Key ||
		string(e.Payload) != string(original.Payload) || !e.OccurredAt.Equal(original.OccurredAt) {
		t.Fatalf("envelope mismatch: %+v", e)
	}
}
func TestOutboxLimits(t *testing.T) {
	db, ob := setupOutbox(t)
	ctx := context.Background()
	if _, err := ob.Claim(ctx, db, 101); !errors.Is(err, store.ErrConfiguration) {
		t.Fatal("unbounded poll")
	}
	tx, _ := db.BeginTx(ctx, nil)
	e := makeEvent("big")
	e.Payload = make([]byte, (1<<20)+1)
	if err := ob.Enqueue(ctx, tx, e); !errors.Is(err, store.ErrConfiguration) {
		t.Fatal("unbounded payload")
	}
	_ = tx.Rollback()
}
