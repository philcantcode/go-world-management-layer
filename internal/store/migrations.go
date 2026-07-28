package store

import (
	"context"
	"database/sql"
	"fmt"
)

var migrations = []string{
	`CREATE TABLE IF NOT EXISTS control_records (
 sequence INTEGER PRIMARY KEY AUTOINCREMENT,
 aggregate_kind TEXT NOT NULL,
 aggregate_id TEXT NOT NULL,
 revision INTEGER NOT NULL CHECK(revision > 0),
 kind TEXT NOT NULL,
 payload BLOB NOT NULL,
 accepted_at INTEGER NOT NULL,
 previous_hash BLOB NOT NULL CHECK(length(previous_hash) = 32),
 hash BLOB NOT NULL CHECK(length(hash) = 32),
 UNIQUE(aggregate_kind, aggregate_id, revision)
);
CREATE INDEX IF NOT EXISTS control_records_aggregate ON control_records(aggregate_kind, aggregate_id, revision);
CREATE TABLE IF NOT EXISTS idempotency (
 namespace TEXT NOT NULL,
 key TEXT NOT NULL,
 request_hash BLOB NOT NULL CHECK(length(request_hash) = 32),
 response BLOB NOT NULL,
 created_at INTEGER NOT NULL,
 PRIMARY KEY(namespace, key)
);
CREATE TABLE IF NOT EXISTS snapshots (
 kind TEXT NOT NULL,
 id TEXT NOT NULL,
 revision INTEGER NOT NULL CHECK(revision > 0),
 payload BLOB NOT NULL,
 updated_at INTEGER NOT NULL,
 PRIMARY KEY(kind, id)
);
CREATE TABLE IF NOT EXISTS policies (
 digest TEXT PRIMARY KEY,
 canonical_json BLOB NOT NULL,
 created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS named_objects (
 kind TEXT NOT NULL,
 id TEXT NOT NULL,
 payload BLOB NOT NULL,
 created_at INTEGER NOT NULL,
 PRIMARY KEY(kind, id)
);
CREATE TABLE IF NOT EXISTS segment_indexes (
 segment_id TEXT PRIMARY KEY,
 first_cursor INTEGER NOT NULL,
 last_cursor INTEGER NOT NULL,
 path TEXT NOT NULL,
 digest TEXT NOT NULL,
 finalized INTEGER NOT NULL,
 artifact_ref TEXT NOT NULL DEFAULT ''
);`,
}

func (s *Store) migrate(ctx context.Context) error {
	var version int
	if err := s.db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		return err
	}
	if version > len(migrations) {
		return fmt.Errorf("database schema %d is newer than supported %d", version, len(migrations))
	}
	for index := version; index < len(migrations); index++ {
		tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, migrations[index]); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("migration %d: %w", index+1, err)
		}
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`PRAGMA user_version=%d`, index+1)); err != nil {
			_ = tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
