// Package store owns crash-consistent control-plane persistence. SQLite stores
// revisioned materialized snapshots, accepted control records, idempotency,
// policy snapshots, incidents, bundles, exports, and segment indexes. High-rate
// observations remain in the ledger package.
package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"
)

var memoryStoreSequence atomic.Uint64

var (
	ErrIdempotencyConflict = errors.New("idempotency key was already used with different input")
	ErrRevisionConflict    = errors.New("control record revision is not the next aggregate revision")
	ErrIntegrity           = errors.New("control record hash chain is invalid")
	ErrPolicyConflict      = errors.New("policy digest already exists with different bytes")
	ErrObjectConflict      = errors.New("named object already exists with different bytes")
	ErrNotFound            = errors.New("record not found")
)

type FaultInjector interface {
	Hit(context.Context, string) error
}
type noFaults struct{}

func (noFaults) Hit(context.Context, string) error { return nil }

type Options struct {
	Path   string
	Faults FaultInjector
	Now    func() time.Time
}

type Store struct {
	db     *sql.DB
	faults FaultInjector
	now    func() time.Time
}

func Open(ctx context.Context, options Options) (*Store, error) {
	if strings.TrimSpace(options.Path) == "" {
		return nil, fmt.Errorf("store path is required")
	}
	if options.Faults == nil {
		options.Faults = noFaults{}
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.Path != ":memory:" {
		if err := ensureParent(options.Path); err != nil {
			return nil, err
		}
	}
	dsn := sqliteDSN(options.Path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	store := &Store{db: db, faults: options.Faults, now: options.Now}
	if err := store.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := store.Verify(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func ensureParent(path string) error {
	return makeDirectory(filepath.Dir(path))
}

func sqliteDSN(path string) string {
	if path == ":memory:" {
		// A unique shared-cache name keeps all connections owned by this sql.DB
		// on one in-memory database without letting independent Store instances
		// observe each other's control records.
		name := "world-control-" + strconv.FormatUint(memoryStoreSequence.Add(1), 10)
		return "file:" + name + "?mode=memory&cache=shared&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)"
	}
	normalizedPath := filepath.ToSlash(path)
	if filepath.VolumeName(path) != "" && !strings.HasPrefix(normalizedPath, "/") {
		normalizedPath = "/" + normalizedPath
	}
	uri := url.URL{Scheme: "file", Path: normalizedPath}
	query := uri.Query()
	query.Add("_pragma", "busy_timeout(5000)")
	query.Add("_pragma", "foreign_keys(1)")
	query.Add("_pragma", "journal_mode(WAL)")
	query.Add("_pragma", "synchronous(FULL)")
	uri.RawQuery = query.Encode()
	return uri.String()
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

type ControlRecord struct {
	Sequence      int64             `json:"sequence"`
	AggregateKind string            `json:"aggregate_kind"`
	AggregateID   string            `json:"aggregate_id"`
	Revision      uint64            `json:"revision"`
	Kind          string            `json:"kind"`
	Payload       []byte            `json:"payload"`
	AcceptedAt    time.Time         `json:"accepted_at"`
	PreviousHash  [sha256.Size]byte `json:"previous_hash"`
	Hash          [sha256.Size]byte `json:"hash"`
}

type Tx struct {
	tx    *sql.Tx
	store *Store
}

type MutationFunc func(context.Context, *Tx) ([]byte, error)

// RunIdempotent executes handler and its control/snapshot writes in the same
// transaction as the idempotency result. A post-commit fault may return an
// error, but retrying the same key deterministically recovers the committed
// response.
func (s *Store) RunIdempotent(ctx context.Context, namespace, key string, request []byte, handler MutationFunc) (response []byte, replayed bool, err error) {
	if namespace == "" || key == "" || len(request) == 0 || handler == nil {
		return nil, false, fmt.Errorf("namespace, key, request, and handler are required")
	}
	if err := s.faults.Hit(ctx, "store.before_begin"); err != nil {
		return nil, false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	if err = s.faults.Hit(ctx, "store.after_begin"); err != nil {
		return nil, false, err
	}
	requestHash := sha256.Sum256(request)
	var storedHash, storedResponse []byte
	lookupErr := tx.QueryRowContext(ctx, `SELECT request_hash, response FROM idempotency WHERE namespace=? AND key=?`, namespace, key).Scan(&storedHash, &storedResponse)
	if lookupErr == nil {
		if !equalHash(storedHash, requestHash[:]) {
			return nil, false, ErrIdempotencyConflict
		}
		if err = tx.Commit(); err != nil {
			return nil, false, err
		}
		return append([]byte(nil), storedResponse...), true, nil
	}
	if !errors.Is(lookupErr, sql.ErrNoRows) {
		return nil, false, lookupErr
	}
	if err = s.faults.Hit(ctx, "store.before_handler"); err != nil {
		return nil, false, err
	}
	response, err = handler(ctx, &Tx{tx: tx, store: s})
	if err != nil {
		return nil, false, err
	}
	if response == nil {
		return nil, false, fmt.Errorf("idempotent handler returned a nil response")
	}
	if err = s.faults.Hit(ctx, "store.before_idempotency_insert"); err != nil {
		return nil, false, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO idempotency(namespace,key,request_hash,response,created_at) VALUES(?,?,?,?,?)`, namespace, key, requestHash[:], response, s.now().UTC().UnixNano())
	if err != nil {
		return nil, false, err
	}
	if err = s.faults.Hit(ctx, "store.before_commit"); err != nil {
		return nil, false, err
	}
	if err = tx.Commit(); err != nil {
		return nil, false, err
	}
	if postCommitErr := s.faults.Hit(ctx, "store.after_commit"); postCommitErr != nil {
		return nil, false, postCommitErr
	}
	return append([]byte(nil), response...), false, nil
}

func equalHash(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	var difference byte
	for index := range left {
		difference |= left[index] ^ right[index]
	}
	return difference == 0
}

func (t *Tx) AppendControl(ctx context.Context, record ControlRecord) (ControlRecord, error) {
	if record.AggregateKind == "" || record.AggregateID == "" || record.Kind == "" || record.Revision == 0 || len(record.Payload) == 0 {
		return ControlRecord{}, fmt.Errorf("complete control record is required")
	}
	var latest uint64
	err := t.tx.QueryRowContext(ctx, `SELECT revision FROM control_records WHERE aggregate_kind=? AND aggregate_id=? ORDER BY revision DESC LIMIT 1`, record.AggregateKind, record.AggregateID).Scan(&latest)
	if errors.Is(err, sql.ErrNoRows) {
		latest = 0
	} else if err != nil {
		return ControlRecord{}, err
	}
	if record.Revision != latest+1 {
		return ControlRecord{}, fmt.Errorf("%w: got %d, want %d", ErrRevisionConflict, record.Revision, latest+1)
	}
	var previous []byte
	err = t.tx.QueryRowContext(ctx, `SELECT hash FROM control_records ORDER BY sequence DESC LIMIT 1`).Scan(&previous)
	if errors.Is(err, sql.ErrNoRows) {
		previous = make([]byte, sha256.Size)
	} else if err != nil {
		return ControlRecord{}, err
	}
	copy(record.PreviousHash[:], previous)
	if record.AcceptedAt.IsZero() {
		record.AcceptedAt = t.store.now().UTC()
	} else {
		record.AcceptedAt = record.AcceptedAt.UTC()
	}
	record.Hash = hashControl(record)
	result, err := t.tx.ExecContext(ctx, `INSERT INTO control_records(aggregate_kind,aggregate_id,revision,kind,payload,accepted_at,previous_hash,hash) VALUES(?,?,?,?,?,?,?,?)`, record.AggregateKind, record.AggregateID, record.Revision, record.Kind, record.Payload, record.AcceptedAt.UnixNano(), record.PreviousHash[:], record.Hash[:])
	if err != nil {
		return ControlRecord{}, err
	}
	record.Sequence, err = result.LastInsertId()
	if err != nil {
		return ControlRecord{}, err
	}
	record.Payload = append([]byte(nil), record.Payload...)
	return record, nil
}

func hashControl(record ControlRecord) [sha256.Size]byte {
	hash := sha256.New()
	_, _ = hash.Write([]byte("world-control-record-v1\x00"))
	_, _ = hash.Write(record.PreviousHash[:])
	writeHashString(hash, record.AggregateKind)
	writeHashString(hash, record.AggregateID)
	var numeric [16]byte
	binary.BigEndian.PutUint64(numeric[:8], record.Revision)
	binary.BigEndian.PutUint64(numeric[8:], uint64(record.AcceptedAt.UnixNano()))
	_, _ = hash.Write(numeric[:])
	writeHashString(hash, record.Kind)
	writeHashBytes(hash, record.Payload)
	var result [sha256.Size]byte
	copy(result[:], hash.Sum(nil))
	return result
}

type hashWriter interface{ Write([]byte) (int, error) }

func writeHashString(writer hashWriter, value string) { writeHashBytes(writer, []byte(value)) }
func writeHashBytes(writer hashWriter, value []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = writer.Write(length[:])
	_, _ = writer.Write(value)
}

func (t *Tx) PutSnapshot(ctx context.Context, kind, id string, revision uint64, payload []byte) error {
	if kind == "" || id == "" || revision == 0 || len(payload) == 0 {
		return fmt.Errorf("complete snapshot is required")
	}
	_, err := t.tx.ExecContext(ctx, `INSERT INTO snapshots(kind,id,revision,payload,updated_at) VALUES(?,?,?,?,?) ON CONFLICT(kind,id) DO UPDATE SET revision=excluded.revision,payload=excluded.payload,updated_at=excluded.updated_at WHERE snapshots.revision < excluded.revision`, kind, id, revision, payload, t.store.now().UTC().UnixNano())
	return err
}

func (s *Store) Snapshot(ctx context.Context, kind, id string) (revision uint64, payload []byte, err error) {
	err = s.db.QueryRowContext(ctx, `SELECT revision,payload FROM snapshots WHERE kind=? AND id=?`, kind, id).Scan(&revision, &payload)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil, ErrNotFound
	}
	return revision, append([]byte(nil), payload...), err
}

func (s *Store) Records(ctx context.Context, after int64, limit int) ([]ControlRecord, error) {
	if after < 0 || limit <= 0 || limit > 10000 {
		return nil, fmt.Errorf("invalid record window")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT sequence,aggregate_kind,aggregate_id,revision,kind,payload,accepted_at,previous_hash,hash FROM control_records WHERE sequence>? ORDER BY sequence LIMIT ?`, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]ControlRecord, 0)
	for rows.Next() {
		var record ControlRecord
		var accepted int64
		var previous, hash []byte
		if err := rows.Scan(&record.Sequence, &record.AggregateKind, &record.AggregateID, &record.Revision, &record.Kind, &record.Payload, &accepted, &previous, &hash); err != nil {
			return nil, err
		}
		if len(previous) != sha256.Size || len(hash) != sha256.Size {
			return nil, ErrIntegrity
		}
		copy(record.PreviousHash[:], previous)
		copy(record.Hash[:], hash)
		record.AcceptedAt = time.Unix(0, accepted).UTC()
		result = append(result, record)
	}
	return result, rows.Err()
}

func (s *Store) Verify(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `SELECT sequence,aggregate_kind,aggregate_id,revision,kind,payload,accepted_at,previous_hash,hash FROM control_records ORDER BY sequence`)
	if err != nil {
		return err
	}
	defer rows.Close()
	var expectedPrevious [sha256.Size]byte
	for rows.Next() {
		var record ControlRecord
		var accepted int64
		var previous, hash []byte
		if err := rows.Scan(&record.Sequence, &record.AggregateKind, &record.AggregateID, &record.Revision, &record.Kind, &record.Payload, &accepted, &previous, &hash); err != nil {
			return err
		}
		if len(previous) != sha256.Size || len(hash) != sha256.Size {
			return fmt.Errorf("%w at sequence %d: invalid hash length", ErrIntegrity, record.Sequence)
		}
		copy(record.PreviousHash[:], previous)
		copy(record.Hash[:], hash)
		record.AcceptedAt = time.Unix(0, accepted).UTC()
		if record.PreviousHash != expectedPrevious || hashControl(record) != record.Hash {
			return fmt.Errorf("%w at sequence %d", ErrIntegrity, record.Sequence)
		}
		expectedPrevious = record.Hash
	}
	return rows.Err()
}
