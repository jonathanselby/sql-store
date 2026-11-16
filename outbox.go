package sqlstore

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

const stampLayout = "2006-01-02T15:04:05.000000000Z07:00"

func stamp(t time.Time) string { return t.UTC().Format(stampLayout) }

// Event mirrors the stable transport-neutral Event Stream envelope. The payload
// is opaque. The production SQL module does not import an event transport.
type Event struct {
	ID            string
	Type          string
	Version       string
	Source        string
	Subject       string
	OccurredAt    time.Time
	CorrelationID string
	CausationID   string
	ContentType   string
	Schema        string
	Key           string
	Payload       []byte
}

func (e Event) valid(max int) bool {
	return strings.TrimSpace(e.ID) != "" && strings.TrimSpace(e.Type) != "" &&
		strings.TrimSpace(e.Version) != "" && strings.TrimSpace(e.Source) != "" &&
		strings.TrimSpace(e.ContentType) != "" && !e.OccurredAt.IsZero() &&
		len(e.Payload) <= max
}

type OutboxOptions struct {
	MaxPayloadBytes int
	MaxBatch        int
	LeaseDuration   time.Duration
}

// Outbox stores events; the calling application owns the publisher and retry policy.
type Outbox struct {
	engine  Engine
	options OutboxOptions
}

func NewOutbox(engine Engine, opts OutboxOptions) (*Outbox, error) {
	if engine != SQLite && engine != PostgreSQL {
		return nil, ErrConfiguration
	}
	if opts.MaxPayloadBytes == 0 {
		opts.MaxPayloadBytes = 1 << 20
	}
	if opts.MaxBatch == 0 {
		opts.MaxBatch = 100
	}
	if opts.LeaseDuration == 0 {
		opts.LeaseDuration = 30 * time.Second
	}
	if opts.MaxPayloadBytes < 1 || opts.MaxPayloadBytes > 64<<20 || opts.MaxBatch < 1 || opts.MaxBatch > 1000 || opts.LeaseDuration < time.Second || opts.LeaseDuration > time.Hour {
		return nil, ErrConfiguration
	}
	return &Outbox{engine: engine, options: opts}, nil
}

// Ensure creates the infrastructure table only. The application must call this
// at startup before accepting writes; it is not run for individual enqueues.
func (o *Outbox) Ensure(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return ErrConfiguration
	}
	payloadType := "BLOB"
	if o.engine == PostgreSQL {
		payloadType = "BYTEA"
	}
	q := `CREATE TABLE IF NOT EXISTS sqlstore_outbox (
        event_id TEXT PRIMARY KEY, event_type TEXT NOT NULL, event_version TEXT NOT NULL,
        source TEXT NOT NULL, subject TEXT NOT NULL, occurred_at TEXT NOT NULL,
        correlation_id TEXT NOT NULL, causation_id TEXT NOT NULL,
        content_type TEXT NOT NULL, schema_ref TEXT NOT NULL, event_key TEXT NOT NULL,
        payload ` + payloadType + ` NOT NULL,
        created_at TEXT NOT NULL, status TEXT NOT NULL CHECK (status IN ('pending','claimed','published')),
        claim_token TEXT, lease_until TEXT, attempts BIGINT NOT NULL DEFAULT 0,
        published_at TEXT)`
	if _, err := db.ExecContext(ctx, q); err != nil {
		if e := ctx.Err(); e != nil {
			return e
		}
		return ErrUnavailable
	}
	_, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS sqlstore_outbox_pending ON sqlstore_outbox(status,lease_until,created_at,event_id)`)
	if err != nil {
		if e := ctx.Err(); e != nil {
			return e
		}
		return ErrUnavailable
	}
	return nil
}

// Enqueue adds exactly one insert to the caller's transaction; it never commits.
func (o *Outbox) Enqueue(ctx context.Context, tx *sql.Tx, e Event) error {
	if tx == nil || !e.valid(o.options.MaxPayloadBytes) {
		return ErrConfiguration
	}
	q := `INSERT INTO sqlstore_outbox(event_id,event_type,event_version,source,subject,
        occurred_at,correlation_id,causation_id,content_type,schema_ref,event_key,payload,
        created_at,status,attempts) VALUES(`
	placeholders := make([]string, 15)
	for i := range placeholders {
		placeholders[i] = placeholder(o.engine, i+1)
	}
	q += strings.Join(placeholders, ",") + ")"
	_, err := tx.ExecContext(ctx, q, e.ID, e.Type, e.Version, e.Source, e.Subject, stamp(e.OccurredAt),
		e.CorrelationID, e.CausationID, e.ContentType, e.Schema, e.Key, e.Payload, stamp(time.Now()), "pending", 0)
	if e := ctx.Err(); e != nil {
		return e
	}
	if err != nil {
		return fmt.Errorf("sqlstore: outbox insert failed")
	}
	return nil
}

// ClaimedEvent must be explicitly acknowledged using its claim token.
type ClaimedEvent struct {
	Event
	ClaimToken string
	Attempts   int64
}

const outboxFields = `event_id,event_type,event_version,source,subject,occurred_at,
 correlation_id,causation_id,content_type,schema_ref,event_key,payload,claim_token,attempts`

func randomToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
func scanClaims(rows *sql.Rows) ([]ClaimedEvent, error) {
	var result []ClaimedEvent
	for rows.Next() {
		var e ClaimedEvent
		var at string
		if err := rows.Scan(&e.ID, &e.Type, &e.Version, &e.Source, &e.Subject, &at,
			&e.CorrelationID, &e.CausationID, &e.ContentType, &e.Schema, &e.Key,
			&e.Payload, &e.ClaimToken, &e.Attempts); err != nil {
			return nil, err
		}
		t, err := time.Parse(stampLayout, at)
		if err != nil {
			return nil, ErrUnavailable
		}
		e.OccurredAt = t
		result = append(result, e)
	}
	return result, rows.Err()
}

// Claim takes a bounded batch. PostgreSQL uses SKIP LOCKED, SQLite one atomic
// UPDATE RETURNING. Publication happens after this method returns, without DB locks.
func (o *Outbox) Claim(ctx context.Context, db *sql.DB, limit int) ([]ClaimedEvent, error) {
	if db == nil || limit < 1 || limit > o.options.MaxBatch {
		return nil, ErrConfiguration
	}
	token, err := randomToken()
	if err != nil {
		return nil, ErrUnavailable
	}
	now, until := stamp(time.Now()), stamp(time.Now().Add(o.options.LeaseDuration))
	var q string
	if o.engine == PostgreSQL {
		q = `WITH eligible AS (
            SELECT event_id FROM sqlstore_outbox WHERE status='pending'
            OR (status='claimed' AND lease_until < $1)
            ORDER BY created_at,event_id LIMIT $2 FOR UPDATE SKIP LOCKED)
            UPDATE sqlstore_outbox SET status='claimed',claim_token=$3,lease_until=$4,
            attempts=attempts+1 WHERE event_id IN (SELECT event_id FROM eligible)
            RETURNING ` + outboxFields
	} else {
		q = `UPDATE sqlstore_outbox SET status='claimed',claim_token=?3,lease_until=?4,
            attempts=attempts+1 WHERE event_id IN (
            SELECT event_id FROM sqlstore_outbox WHERE status='pending'
            OR (status='claimed' AND lease_until < ?1)
            ORDER BY created_at,event_id LIMIT ?2)
            RETURNING ` + outboxFields
	}
	rows, err := db.QueryContext(ctx, q, now, limit, token, until)
	if err != nil {
		if e := ctx.Err(); e != nil {
			return nil, e
		}
		return nil, ErrUnavailable
	}
	defer rows.Close()
	events, err := scanClaims(rows)
	if err != nil {
		return nil, ErrUnavailable
	}
	// RETURNING order is not guaranteed. Sort by stable ID for deterministic
	// local observation; there is no global broker ordering claim.
	return events, nil
}

// PublicationProof is supplied by the application-owned publisher after a
// durable transport acknowledgement. In-memory stream acceptance is not proof.
type PublicationProof struct {
	Transport string
	Durable   bool
	Reference string
}

// MarkPublished is fenced by both lease validity and token; stale publishers
// cannot acknowledge rows reclaimed by another worker.
func (o *Outbox) MarkPublished(ctx context.Context, db *sql.DB, id, claimToken string, proof PublicationProof) error {
	if !proof.Durable || strings.TrimSpace(proof.Transport) == "" ||
		strings.EqualFold(strings.TrimSpace(proof.Transport), "embedded") || strings.TrimSpace(proof.Reference) == "" {
		return ErrNonDurableReceipt
	}
	if db == nil || id == "" || claimToken == "" {
		return ErrConfiguration
	}
	p := func(n int) string { return placeholder(o.engine, n) }
	q := `UPDATE sqlstore_outbox SET status='published',published_at=` + p(1) +
		`,claim_token=NULL,lease_until=NULL WHERE event_id=` + p(2) +
		` AND claim_token=` + p(3) + ` AND status='claimed' AND lease_until > ` + p(4)
	now := stamp(time.Now())
	r, err := db.ExecContext(ctx, q, now, id, claimToken, now)
	if e := ctx.Err(); e != nil {
		return e
	}
	if err != nil {
		return ErrUnavailable
	}
	n, err := r.RowsAffected()
	if err != nil {
		return ErrUnavailable
	}
	if n != 1 {
		return ErrClaimLost
	}
	return nil
}

// ReleaseClaim is optional after a failed publish. A process crash needs no
// explicit action: the lease expires and Claim safely reclaims the event.
func (o *Outbox) ReleaseClaim(ctx context.Context, db *sql.DB, id, token string) error {
	if db == nil || id == "" || token == "" {
		return ErrConfiguration
	}
	p := func(n int) string { return placeholder(o.engine, n) }
	q := `UPDATE sqlstore_outbox SET status='pending',claim_token=NULL,lease_until=NULL
        WHERE event_id=` + p(1) + ` AND claim_token=` + p(2) + ` AND status='claimed' AND lease_until > ` + p(3)
	r, err := db.ExecContext(ctx, q, id, token, stamp(time.Now()))
	if e := ctx.Err(); e != nil {
		return e
	}
	if err != nil {
		return ErrUnavailable
	}
	n, err := r.RowsAffected()
	if err != nil {
		return ErrUnavailable
	}
	if n != 1 {
		return ErrClaimLost
	}
	return nil
}

// Pending counts non-published work for application-owned backlog monitoring.
func (o *Outbox) Pending(ctx context.Context, db *sql.DB) (int64, error) {
	if db == nil {
		return 0, ErrConfiguration
	}
	var count int64
	err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlstore_outbox WHERE status <> 'published'").Scan(&count)
	if err != nil {
		if e := ctx.Err(); e != nil {
			return 0, e
		}
		return 0, ErrUnavailable
	}
	return count, nil
}

func (o *Outbox) IsPublished(ctx context.Context, db *sql.DB, id string) (bool, error) {
	if db == nil || id == "" {
		return false, ErrConfiguration
	}
	q := "SELECT status FROM sqlstore_outbox WHERE event_id=" + placeholder(o.engine, 1)
	var status string
	err := db.QueryRowContext(ctx, q, id).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, ErrUnavailable
	}
	return status == "published", nil
}
