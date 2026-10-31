package bbolt

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/moby/moby/v2/errdefs"
	storagekv "github.com/moby/moby/v2/extpoints/storage/kv/v0"
	bolt "go.etcd.io/bbolt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// snapshotGenerations hands each storage provider a distinct epoch. Handles
// minted before a provider restart carry an older epoch and are rejected
// instead of resolving against a fresh snapshot registry.
var snapshotGenerations atomic.Uint64

func newStorage() *storage {
	return &storage{
		generation: snapshotGenerations.Add(1),
		snapshots:  make(map[uint64]*snapshot),
	}
}

// snapshotDirectory holds immutable, point-in-time copies of records.db.
const snapshotDirectory = "snapshots"

func snapshotDirPath(dataRoot string) string {
	return filepath.Join(dataRoot, "extensions", snapshotDirectory)
}

// removeStaleSnapshots deletes copies left behind by a crashed provider.
// Only the live provider holds snapshots, so anything present at open time
// belongs to an earlier instance.
func removeStaleSnapshots(dataRoot string) {
	entries, err := os.ReadDir(snapshotDirPath(dataRoot))
	if err != nil {
		return
	}
	for _, entry := range entries {
		if entry.Type().IsRegular() && strings.HasPrefix(entry.Name(), "snapshot-") && strings.HasSuffix(entry.Name(), ".db") {
			_ = os.Remove(filepath.Join(snapshotDirPath(dataRoot), entry.Name()))
		}
	}
}

// snapshot is an immutable, read-only copy of the database opened at one
// point in time. It is a separate bbolt database rather than a long-lived
// transaction of the live one: bbolt must remap the live file when it grows,
// and remapping waits for every open read transaction, so a pinned
// transaction could stall all writes for the snapshot's lifetime.
type snapshot struct {
	mu        sync.Mutex
	db        *bolt.DB
	path      string
	namespace string
	closed    bool
}

// close ends the read-only database and removes its file. It is safe to
// call more than once.
func (snap *snapshot) close() {
	snap.mu.Lock()
	defer snap.mu.Unlock()
	if snap.closed {
		return
	}
	snap.closed = true
	_ = snap.db.Close()
	_ = os.Remove(snap.path)
}

// snapshotHandle is the opaque, self-describing content of a snapshot token.
type snapshotHandle struct {
	Generation uint64 `json:"g"`
	ID         uint64 `json:"i"`
}

const maxSnapshotHandleSize = 128

func encodeSnapshotHandle(generation, id uint64) (string, error) {
	data, err := json.Marshal(snapshotHandle{Generation: generation, ID: id})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(data), nil
}

// decodeSnapshotHandle validates the token shape and provider epoch.
// Malformed tokens are invalid requests; well-formed tokens from another
// epoch are expired snapshots.
func decodeSnapshotHandle(token string, expectedGeneration uint64) (uint64, error) {
	if token == "" || len(token) > maxSnapshotHandleSize {
		return 0, invalidRequest("invalid snapshot handle")
	}
	data, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return 0, invalidRequest("invalid snapshot handle")
	}
	var handle snapshotHandle
	if err := json.Unmarshal(data, &handle); err != nil || handle.Generation == 0 || handle.ID == 0 {
		return 0, invalidRequest("invalid snapshot handle")
	}
	if handle.Generation != expectedGeneration {
		return 0, snapshotInvalid()
	}
	return handle.ID, nil
}

type snapshotInvalidError struct{ error }

func (snapshotInvalidError) FailedPrecondition() {}

func (e snapshotInvalidError) GRPCStatus() *status.Status {
	return status.New(codes.FailedPrecondition, e.Error())
}

func (e snapshotInvalidError) Unwrap() error {
	return e.error
}

func snapshotInvalid() error {
	return snapshotInvalidError{errors.New("storage snapshot is invalid, closed, or predates a provider restart")}
}

// closeSnapshotsLocked ends every open snapshot. The caller must hold s.mu
// (and therefore serialize against opens); entries are removed before their
// files are deleted.
func (s *storage) closeSnapshotsLocked() {
	s.snapshotsMu.Lock()
	active := s.snapshots
	s.snapshots = make(map[uint64]*snapshot)
	s.snapshotsMu.Unlock()
	for _, snap := range active {
		snap.close()
	}
}

func (s *storage) OpenSnapshot(ctx context.Context, req *storagekv.OpenSnapshotRequest) (*storagekv.OpenSnapshotResponse, error) {
	if req == nil {
		return nil, invalidRequest("open snapshot request is required")
	}
	if err := validateNamespace(req.Namespace); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var id uint64
	var snap *snapshot
	// Hold the lifecycle read lock while copying so shutdown cannot close
	// the database mid-backup. The backup transaction itself is short: it
	// only lives while the consistent copy is written.
	s.lifecycle.RLock()
	defer s.lifecycle.RUnlock()
	err := func() error {
		s.mu.Lock()
		defer s.mu.Unlock()
		if err := ctx.Err(); err != nil {
			return err
		}
		if s.closed {
			return errdefs.Unavailable(status.Error(codes.Unavailable, "extension storage is closed"))
		}
		if s.dataRoot == "" {
			return errors.New("extension storage is not initialized")
		}
		if s.db == nil {
			db, err := openDatabase(s.dataRoot)
			if err != nil {
				return databaseUnavailableError{err}
			}
			s.db = db
		}
		dir := snapshotDirPath(s.dataRoot)
		if err := os.Mkdir(dir, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
		s.nextSnapID++
		id = s.nextSnapID
		path := filepath.Join(dir, fmt.Sprintf("snapshot-%d-%d.db", s.generation, id))
		// bbolt streams a consistent view of the whole database from one
		// read transaction. Writes after this point go to new pages and can
		// never alter the captured file.
		err := s.db.View(func(tx *bolt.Tx) error {
			file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
			if err != nil {
				return err
			}
			ok := false
			defer func() {
				if !ok {
					_ = file.Close()
					_ = os.Remove(path)
				}
			}()
			if _, err := tx.WriteTo(file); err != nil {
				return err
			}
			if err := file.Sync(); err != nil {
				return err
			}
			if err := file.Close(); err != nil {
				return err
			}
			ok = true
			return nil
		})
		if err != nil {
			return err
		}
		db, err := bolt.Open(path, 0o600, &bolt.Options{
			ReadOnly: true,
			Timeout:  time.Second,
		})
		if err != nil {
			_ = os.Remove(path)
			return err
		}
		snap = &snapshot{db: db, path: path, namespace: req.Namespace}
		s.snapshotsMu.Lock()
		s.snapshots[id] = snap
		s.snapshotsMu.Unlock()
		return nil
	}()
	if err != nil {
		return nil, err
	}
	token, err := encodeSnapshotHandle(s.generation, id)
	if err != nil {
		s.snapshotsMu.Lock()
		delete(s.snapshots, id)
		s.snapshotsMu.Unlock()
		snap.close()
		return nil, err
	}
	return &storagekv.OpenSnapshotResponse{Snapshot: token}, nil
}

func (s *storage) CloseSnapshot(ctx context.Context, req *storagekv.CloseSnapshotRequest) error {
	if req == nil {
		return invalidRequest("close snapshot request is required")
	}
	if err := validateNamespace(req.Namespace); err != nil {
		return err
	}
	id, err := decodeSnapshotHandle(req.Snapshot, s.generation)
	if err != nil {
		return err
	}
	s.snapshotsMu.Lock()
	snap := s.snapshots[id]
	delete(s.snapshots, id)
	s.snapshotsMu.Unlock()
	if snap == nil || snap.namespace != req.Namespace {
		return snapshotInvalid()
	}
	// Releasing a snapshot is idempotent cleanup that must still happen if
	// the caller's context has been canceled.
	snap.close()
	return nil
}

// useSnapshot resolves a handle and runs fn against its immutable database.
// The entry lock keeps Close from shutting the read-only database while an
// operation is in flight.
func (s *storage) useSnapshot(ctx context.Context, namespace, token string, fn func(*bolt.Tx) error) error {
	if err := validateNamespace(namespace); err != nil {
		return err
	}
	id, err := decodeSnapshotHandle(token, s.generation)
	if err != nil {
		return err
	}
	s.snapshotsMu.Lock()
	snap := s.snapshots[id]
	s.snapshotsMu.Unlock()
	if snap == nil || snap.namespace != namespace {
		return snapshotInvalid()
	}
	snap.mu.Lock()
	defer snap.mu.Unlock()
	if snap.closed {
		return snapshotInvalid()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return snap.db.View(fn)
}

func (s *storage) SnapshotGet(ctx context.Context, req *storagekv.SnapshotGetRequest) (*storagekv.GetResponse, error) {
	if req == nil {
		return nil, invalidRequest("snapshot get request is required")
	}
	if err := validateRecord(&storagekv.RecordRequest{Namespace: req.Namespace, Key: req.Key}); err != nil {
		return nil, err
	}
	resp := &storagekv.GetResponse{}
	err := s.useSnapshot(ctx, req.Namespace, req.Snapshot, func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(req.Namespace))
		if bucket == nil {
			return recordNotFound(req.Key)
		}
		value := bucket.Get([]byte(req.Key))
		if value == nil {
			return recordNotFound(req.Key)
		}
		resp.Value = bytes.Clone(value)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// snapshotListCursor binds an exclusive key position to the snapshot,
// namespace, and prefix it was paged from.
type snapshotListCursor struct {
	Namespace string `json:"namespace"`
	Snapshot  string `json:"snapshot"`
	Prefix    string `json:"prefix"`
	After     string `json:"after"`
}

func (s *storage) SnapshotList(ctx context.Context, req *storagekv.SnapshotListRequest) (*storagekv.ListResponse, error) {
	if req == nil {
		return nil, invalidRequest("snapshot list request is required")
	}
	if err := validateNamespace(req.Namespace); err != nil {
		return nil, err
	}
	if !validKey(req.Prefix, true) {
		return nil, invalidRequest("invalid key prefix")
	}
	if req.Limit > storagekv.MaxPageSize {
		return nil, invalidRequest("page limit exceeds maximum size")
	}
	limit := req.Limit
	if limit == 0 {
		limit = storagekv.DefaultPageSize
	}
	resp := &storagekv.ListResponse{}
	err := s.useSnapshot(ctx, req.Namespace, req.Snapshot, func(tx *bolt.Tx) error {
		var after string
		if req.Cursor != "" {
			// Bound allocation before decoding untrusted cursor data.
			if len(req.Cursor) > 8*(storagekv.MaxNamespaceSize+2*storagekv.MaxKeySize)+2*maxSnapshotHandleSize {
				return invalidRequest("invalid snapshot list cursor")
			}
			data, err := base64.RawURLEncoding.DecodeString(req.Cursor)
			var cursor snapshotListCursor
			if err != nil || json.Unmarshal(data, &cursor) != nil ||
				cursor.Namespace != req.Namespace || cursor.Snapshot != req.Snapshot ||
				cursor.Prefix != req.Prefix ||
				!validKey(cursor.After, false) || !strings.HasPrefix(cursor.After, req.Prefix) {
				return invalidRequest("invalid snapshot list cursor")
			}
			after = cursor.After
		}
		bucket := tx.Bucket([]byte(req.Namespace))
		if bucket == nil {
			return nil
		}
		keys, more := pageKeys(bucket, req.Prefix, after, limit)
		resp.Keys = keys
		if more {
			data, err := json.Marshal(snapshotListCursor{
				Namespace: req.Namespace,
				Snapshot:  req.Snapshot,
				Prefix:    req.Prefix,
				After:     resp.Keys[len(resp.Keys)-1],
			})
			if err != nil {
				return err
			}
			resp.NextCursor = base64.RawURLEncoding.EncodeToString(data)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}
