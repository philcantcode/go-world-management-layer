package store

import (
	"context"
	"database/sql"
	"errors"
)

func (s *Store) PutPolicy(ctx context.Context, digest string, canonicalJSON []byte) error {
	if digest == "" || len(canonicalJSON) == 0 {
		return errors.New("policy digest and document are required")
	}
	result, err := s.db.ExecContext(ctx, `INSERT INTO policies(digest,canonical_json,created_at) VALUES(?,?,?) ON CONFLICT(digest) DO UPDATE SET canonical_json=excluded.canonical_json WHERE policies.canonical_json=excluded.canonical_json`, digest, canonicalJSON, s.now().UTC().UnixNano())
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed == 0 {
		return ErrPolicyConflict
	}
	return nil
}

func (s *Store) Policy(ctx context.Context, digest string) ([]byte, error) {
	var payload []byte
	if err := s.db.QueryRowContext(ctx, `SELECT canonical_json FROM policies WHERE digest=?`, digest).Scan(&payload); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return append([]byte(nil), payload...), nil
}

func (s *Store) PutObject(ctx context.Context, kind, id string, payload []byte) error {
	if kind == "" || id == "" || len(payload) == 0 {
		return errors.New("object kind, id, and payload are required")
	}
	result, err := s.db.ExecContext(ctx, `INSERT INTO named_objects(kind,id,payload,created_at) VALUES(?,?,?,?) ON CONFLICT(kind,id) DO UPDATE SET payload=excluded.payload WHERE named_objects.payload=excluded.payload`, kind, id, payload, s.now().UTC().UnixNano())
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed == 0 {
		return ErrObjectConflict
	}
	return nil
}

func (s *Store) Object(ctx context.Context, kind, id string) ([]byte, error) {
	var payload []byte
	if err := s.db.QueryRowContext(ctx, `SELECT payload FROM named_objects WHERE kind=? AND id=?`, kind, id).Scan(&payload); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return append([]byte(nil), payload...), nil
}

type SegmentIndex struct {
	SegmentID   string
	FirstCursor uint64
	LastCursor  uint64
	Path        string
	Digest      string
	Finalized   bool
	ArtifactRef string
}

func (s *Store) PutSegmentIndex(ctx context.Context, index SegmentIndex) error {
	if index.SegmentID == "" || index.FirstCursor == 0 || index.LastCursor < index.FirstCursor || index.Path == "" || index.Digest == "" {
		return errors.New("invalid segment index")
	}
	finalized := 0
	if index.Finalized {
		finalized = 1
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO segment_indexes(segment_id,first_cursor,last_cursor,path,digest,finalized,artifact_ref) VALUES(?,?,?,?,?,?,?) ON CONFLICT(segment_id) DO UPDATE SET first_cursor=excluded.first_cursor,last_cursor=excluded.last_cursor,path=excluded.path,digest=excluded.digest,finalized=excluded.finalized,artifact_ref=excluded.artifact_ref`, index.SegmentID, index.FirstCursor, index.LastCursor, index.Path, index.Digest, finalized, index.ArtifactRef)
	return err
}
