package sqlstore_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	eventstream "github.com/jonathanselby/event-stream"
	"github.com/jonathanselby/event-stream/embedded"
	sqlstore "github.com/jonathanselby/sql-store"
)

// This is real interoperability with the separately released Event Stream
// module (v0.1.0), not a reimplemented/mock transport.
func TestOutboxEventStreamRoundTrip(t *testing.T) {
	db, ob := setupOutbox(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	original := makeEvent("real-eventstream")
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := ob.Enqueue(ctx, tx, original); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	stream, err := embedded.New(embedded.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close(context.Background())
	sub, err := stream.Subscribe("garage-test", 4)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	claimed, err := ob.Claim(ctx, db, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("outbox claim: %v %v", err, claimed)
	}
	e := claimed[0].Event
	delivered := eventstream.Event{ID: e.ID, Type: e.Type, Version: e.Version, Source: e.Source,
		Subject: e.Subject, OccurredAt: e.OccurredAt, CorrelationID: e.CorrelationID,
		CausationID: e.CausationID, ContentType: e.ContentType, Schema: e.Schema, Key: e.Key, Payload: e.Payload}
	receipt, err := stream.Publish(ctx, delivered)
	if err != nil {
		t.Fatal(err)
	}
	got, err := sub.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	actual := got.Event
	if actual.ID != original.ID || actual.Type != original.Type || actual.Version != original.Version ||
		actual.Source != original.Source || actual.ContentType != original.ContentType ||
		actual.Subject != original.Subject || actual.Schema != original.Schema || actual.Key != original.Key ||
		actual.CorrelationID != original.CorrelationID || actual.CausationID != original.CausationID ||
		!actual.OccurredAt.Equal(original.OccurredAt) || !bytes.Equal(actual.Payload, original.Payload) {
		t.Fatalf("Event Stream envelope changed: %+v", actual)
	}
	if receipt.Transport != eventstream.TransportEmbedded {
		t.Fatal("unexpected receipt type")
	}
	// Embedded accepts in memory; even successful delivery and receipt do
	// NOT establish durable Kafka/broker publication.
	err = ob.MarkPublished(ctx, db, e.ID, claimed[0].ClaimToken, sqlstore.PublicationProof{
		Transport: "embedded", Durable: false, Reference: "embedded-accepted"})
	if !errors.Is(err, sqlstore.ErrNonDurableReceipt) {
		t.Fatalf("memory acknowledgement marked durable: %v", err)
	}
	if err := ob.ReleaseClaim(ctx, db, e.ID, claimed[0].ClaimToken); err != nil {
		t.Fatal(err)
	}
	again, err := ob.Claim(ctx, db, 1)
	if err != nil || len(again) != 1 || again[0].Event.ID != original.ID {
		t.Fatal("event not recoverable after in-memory delivery", err)
	}
}
