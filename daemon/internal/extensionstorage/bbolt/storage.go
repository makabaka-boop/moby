// Package bbolt implements persistent record storage using bbolt.
package bbolt

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/moby/extensions"
	"github.com/moby/moby/v2/errdefs"
	daemonconfigv0 "github.com/moby/moby/v2/extpoints/daemonconfig/v0"
	storagekv "github.com/moby/moby/v2/extpoints/storage/kv/v0"
	bolt "go.etcd.io/bbolt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// expiryBucketName is the top-level bucket that records absolute expiries.
// It maps namespace + NUL + key to an 8-byte big-endian UnixNano deadline.
// The leading NUL can never appear in a validated namespace, so it cannot
// alias a record bucket. Records without an entry never expire; this keeps
// records written before TTL existed permanently stored.
const expiryBucketName = "\x00ttl"

// defaultCleanupInterval bounds how long expired records linger before the
// provider removes them without anyone accessing their keys.
const defaultCleanupInterval = time.Minute

// ID identifies the daemon's bbolt storage provider.
const ID extensions.ExtensionID = "org.mobyproject.storage.bbolt.v0"

// Extension is a reusable definition of the bbolt storage provider.
// The database is opened lazily on the first storage operation.
// Neither extension shutdown nor removal deletes persistent data.
var Extension definition

type definition struct{}

func (definition) Declaration() extensions.Declaration {
	s := &storage{}
	return extensions.Declaration{
		ID:           ID,
		Providers:    []extensions.Provider{storagekv.Point.Provide(s)},
		Dependencies: []extensions.Dependency{daemonconfigv0.Point.Dependency()},
		Init:         s.init,
		Shutdown:     s.shutdown,
	}
}

type storage struct {
	lifecycle sync.RWMutex
	mu        sync.Mutex
	dataRoot  string
	db        *bolt.DB
	closed    bool

	// cleanupInterval overrides the expiry sweeper's period. It is read once
	// when the database opens and exists for tests.
	cleanupInterval time.Duration
	janitorStop     chan struct{}
	janitorDone     chan struct{}
}

type databaseUnavailableError struct{ error }

func (databaseUnavailableError) Unavailable() {}

func (e databaseUnavailableError) GRPCStatus() *status.Status {
	return status.New(codes.Unavailable, e.Error())
}

func (e databaseUnavailableError) Unwrap() error {
	return e.error
}

func (s *storage) init(ctx context.Context, _ extensions.Config, resolver extensions.Resolver) error {
	provider, err := daemonconfigv0.Point.Single(resolver)
	if err != nil {
		return fmt.Errorf("resolve daemon config: %w", err)
	}
	response, err := provider.Get(ctx, &daemonconfigv0.GetRequest{})
	if err != nil {
		return fmt.Errorf("get daemon config: %w", err)
	}
	if response == nil || response.RootDir == "" {
		return errors.New("daemon config persistent root is required")
	}
	if !filepath.IsAbs(response.RootDir) {
		return fmt.Errorf("daemon config persistent root %q must be absolute", response.RootDir)
	}
	s.mu.Lock()
	s.dataRoot = response.RootDir
	s.mu.Unlock()
	return nil
}

func (s *storage) shutdown(ctx context.Context) error {
	s.lifecycle.Lock()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		s.lifecycle.Unlock()
		return nil
	}
	s.closed = true
	var db *bolt.DB
	if s.db != nil {
		db = s.db
		s.db = nil
	}
	stop := s.janitorStop
	done := s.janitorDone
	s.mu.Unlock()
	s.lifecycle.Unlock()

	// The lifecycle lock this function held until now guarantees no sweep is
	// running. Signal before closing so a sweep about to start observes the
	// closed database and exits instead of touching it.
	if stop != nil {
		close(stop)
	}
	var err error
	if db != nil {
		err = db.Close()
	}
	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
		}
	}
	return err
}

// startJanitor launches the background expiry sweeper. It runs at most once
// per storage, right after the database's first transaction completed.
func (s *storage) startJanitor() {
	s.mu.Lock()
	stop := make(chan struct{})
	done := make(chan struct{})
	s.janitorStop = stop
	s.janitorDone = done
	interval := s.cleanupInterval
	if interval <= 0 {
		interval = defaultCleanupInterval
	}
	s.mu.Unlock()
	go s.runJanitor(stop, done, interval)
}

func (s *storage) runJanitor(stop <-chan struct{}, done chan<- struct{}, interval time.Duration) {
	defer close(done)
	// Sweep once on startup so expiries elapsed while the provider was down
	// are removed without waiting for the first interval.
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-stop:
			return
		case <-timer.C:
		}
		// A closed or never-opened database is unavailable; shutdown owns the
		// database once that happens, so the sweeper is finished.
		if err := s.purgeExpired(context.Background()); cerrdefs.IsUnavailable(err) {
			return
		}
		timer.Reset(interval)
	}
}

// purgeExpired physically removes records whose persisted deadline passed.
// It scans the expiry index, so keys do not have to be accessed to be
// collected.
func (s *storage) purgeExpired(ctx context.Context) error {
	return s.transaction(ctx, true, func(tx *bolt.Tx) error {
		expiry := tx.Bucket([]byte(expiryBucketName))
		if expiry == nil {
			return nil
		}
		now := time.Now().UnixNano()
		cursor := expiry.Cursor()
		for composite, meta := cursor.First(); composite != nil; composite, meta = cursor.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			expiresAt, ok := decodeExpiry(meta)
			if !ok || expiresAt > now {
				continue
			}
			namespace, key, ok := strings.Cut(string(composite), "\x00")
			if !ok {
				continue
			}
			// Write transactions are serialized and each sees every earlier
			// commit, so a renewal or same-name rebuild committed before this
			// sweep already replaced the deadline above, and none can commit
			// while it runs. Removing the value only while the index still
			// shows this past deadline cannot touch a renewed or rebuilt
			// record.
			if bucket := tx.Bucket([]byte(namespace)); bucket != nil {
				if err := bucket.Delete([]byte(key)); err != nil {
					return err
				}
			}
			if err := cursor.Delete(); err != nil {
				return err
			}
		}
		return nil
	})
}

// transaction holds the lifecycle read lock through transaction completion so
// shutdown cannot close the database between capturing it and beginning work.
func (s *storage) transaction(ctx context.Context, write bool, fn func(*bolt.Tx) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.lifecycle.RLock()
	defer s.lifecycle.RUnlock()

	var db *bolt.DB
	openedNow := false
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
			var err error
			s.db, err = openDatabase(s.dataRoot)
			if err != nil {
				return databaseUnavailableError{err}
			}
			openedNow = true
		}
		db = s.db
		return nil
	}()
	if err != nil {
		return err
	}

	operation := func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(tx); err != nil {
			return err
		}
		return ctx.Err()
	}
	if write {
		err = db.Update(operation)
	} else {
		err = db.View(operation)
	}
	// Start the sweeper only after the opening transaction finished: a bbolt
	// writer holds the metadata lock while it runs, so a sweep starting inside
	// another long-lived transaction would needlessly block new readers. The
	// lifecycle read lock is still held here, which keeps shutdown from
	// interleaving with the start.
	if openedNow {
		s.startJanitor()
	}
	return err
}

func (s *storage) Get(ctx context.Context, req *storagekv.RecordRequest) (*storagekv.GetResponse, error) {
	if err := validateRecord(req); err != nil {
		return nil, err
	}
	resp := &storagekv.GetResponse{}
	err := s.transaction(ctx, false, func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(req.Namespace))
		if bucket == nil {
			return recordNotFound(req.Key)
		}
		value := bucket.Get([]byte(req.Key))
		if value == nil {
			return recordNotFound(req.Key)
		}
		if expiry := tx.Bucket([]byte(expiryBucketName)); expiry != nil && recordExpired(expiry.Get(expiryComposite(req.Namespace, req.Key)), time.Now().UnixNano()) {
			return recordNotFound(req.Key)
		}
		// Bolt values are valid only for the lifetime of the transaction.
		resp.Value = bytes.Clone(value)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *storage) Create(ctx context.Context, req *storagekv.WriteRequest) error {
	return s.write(ctx, req, true)
}

func (s *storage) Update(ctx context.Context, req *storagekv.WriteRequest) error {
	return s.write(ctx, req, false)
}

func (s *storage) write(ctx context.Context, req *storagekv.WriteRequest, create bool) error {
	if req == nil {
		return invalidRequest("write request is required")
	}
	if err := validateRecord(&storagekv.RecordRequest{Namespace: req.Namespace, Key: req.Key}); err != nil {
		return err
	}
	if len(req.Value) > storagekv.MaxValueSize {
		return invalidRequest("record value exceeds maximum size")
	}
	if req.TTL < 0 {
		return invalidRequest("record ttl must not be negative")
	}
	return s.transaction(ctx, true, func(tx *bolt.Tx) error {
		now := time.Now()
		bucket := tx.Bucket([]byte(req.Namespace))
		expiry, err := tx.CreateBucketIfNotExists([]byte(expiryBucketName))
		if err != nil {
			return err
		}
		composite := expiryComposite(req.Namespace, req.Key)
		// An expired record counts as absent: create may rebuild it and
		// update reports not-found, matching every other operation.
		exists := bucket != nil && bucket.Get([]byte(req.Key)) != nil &&
			!recordExpired(expiry.Get(composite), now.UnixNano())
		if create && exists {
			return errdefs.Conflict(status.Errorf(codes.AlreadyExists, "record %q already exists", req.Key))
		}
		if !create && !exists {
			return recordNotFound(req.Key)
		}
		if bucket == nil {
			var err error
			bucket, err = tx.CreateBucket([]byte(req.Namespace))
			if err != nil {
				return err
			}
		}
		if err := bucket.Put([]byte(req.Key), req.Value); err != nil {
			return err
		}
		// Replace the expiry together with the value: a positive TTL stores
		// the new absolute deadline; zero clears a deadline set previously.
		if req.TTL > 0 {
			return expiry.Put(composite, encodeExpiry(expiryAt(now, req.TTL)))
		}
		return expiry.Delete(composite)
	})
}

func (s *storage) Delete(ctx context.Context, req *storagekv.RecordRequest) error {
	if err := validateRecord(req); err != nil {
		return err
	}
	return s.transaction(ctx, true, func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(req.Namespace))
		if bucket == nil {
			return nil
		}
		if err := bucket.Delete([]byte(req.Key)); err != nil {
			return err
		}
		if expiry := tx.Bucket([]byte(expiryBucketName)); expiry != nil {
			return expiry.Delete(expiryComposite(req.Namespace, req.Key))
		}
		return nil
	})
}

func (s *storage) DeletePrefix(ctx context.Context, req *storagekv.DeletePrefixRequest) error {
	if req == nil {
		return invalidRequest("delete prefix request is required")
	}
	if err := validateNamespace(req.Namespace); err != nil {
		return err
	}
	if !validKey(req.Prefix, false) {
		return invalidRequest("invalid key prefix")
	}
	return s.transaction(ctx, true, func(tx *bolt.Tx) error {
		if bucket := tx.Bucket([]byte(req.Namespace)); bucket != nil {
			cursor := bucket.Cursor()
			prefix := []byte(req.Prefix)
			// Deletion shifts the remaining keys; advancing would skip the next key.
			for key, _ := cursor.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, _ = cursor.Seek(prefix) {
				if err := ctx.Err(); err != nil {
					return err
				}
				if err := cursor.Delete(); err != nil {
					return err
				}
			}
		}
		if expiry := tx.Bucket([]byte(expiryBucketName)); expiry != nil {
			metaPrefix := expiryComposite(req.Namespace, req.Prefix)
			cursor := expiry.Cursor()
			for key, _ := cursor.Seek(metaPrefix); key != nil && bytes.HasPrefix(key, metaPrefix); key, _ = cursor.Seek(metaPrefix) {
				if err := ctx.Err(); err != nil {
					return err
				}
				if err := cursor.Delete(); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// listCursor binds an exclusive key position to the original query.
// It carries no authority; callers may already read any namespace.
type listCursor struct {
	Namespace string `json:"namespace"`
	Prefix    string `json:"prefix"`
	After     string `json:"after"`
}

func (s *storage) List(ctx context.Context, req *storagekv.ListRequest) (*storagekv.ListResponse, error) {
	if req == nil {
		return nil, invalidRequest("list request is required")
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
	var after string
	if req.Cursor != "" {
		// Bound allocation before decoding untrusted cursor data.
		if len(req.Cursor) > 8*(storagekv.MaxNamespaceSize+2*storagekv.MaxKeySize) {
			return nil, invalidRequest("invalid list cursor")
		}
		data, err := base64.RawURLEncoding.DecodeString(req.Cursor)
		var cursor listCursor
		if err != nil || json.Unmarshal(data, &cursor) != nil || cursor.Namespace != req.Namespace || cursor.Prefix != req.Prefix || !validKey(cursor.After, false) || !strings.HasPrefix(cursor.After, req.Prefix) {
			return nil, invalidRequest("invalid list cursor")
		}
		after = cursor.After
	}
	resp := &storagekv.ListResponse{}
	err := s.transaction(ctx, false, func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(req.Namespace))
		if bucket == nil {
			return nil
		}
		expiry := tx.Bucket([]byte(expiryBucketName))
		now := time.Now().UnixNano()
		cursor := bucket.Cursor()
		start := req.Prefix
		if after != "" {
			start = after
		}
		key, _ := cursor.Seek([]byte(start))
		if after != "" && string(key) == after {
			key, _ = cursor.Next()
		}
		for ; key != nil && bytes.HasPrefix(key, []byte(req.Prefix)); key, _ = cursor.Next() {
			// Expired keys are invisible and do not fill the page.
			if expiry != nil && recordExpired(expiry.Get(expiryComposite(req.Namespace, string(key))), now) {
				continue
			}
			if uint32(len(resp.Keys)) == limit {
				data, err := json.Marshal(listCursor{Namespace: req.Namespace, Prefix: req.Prefix, After: resp.Keys[len(resp.Keys)-1]})
				if err != nil {
					return err
				}
				resp.NextCursor = base64.RawURLEncoding.EncodeToString(data)
				break
			}
			resp.Keys = append(resp.Keys, string(key))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func validateRecord(req *storagekv.RecordRequest) error {
	if req == nil {
		return invalidRequest("record request is required")
	}
	if err := validateNamespace(req.Namespace); err != nil {
		return err
	}
	if !validKey(req.Key, false) {
		return invalidRequest("invalid record key")
	}
	return nil
}

func validateNamespace(namespace string) error {
	if namespace == "" || len(namespace) > storagekv.MaxNamespaceSize {
		return invalidRequest("invalid storage namespace")
	}
	for i := 0; i < len(namespace); i++ {
		c := namespace[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' {
			continue
		}
		if i > 0 && (c == '.' || c == '_' || c == '-') {
			continue
		}
		return invalidRequest("invalid storage namespace")
	}
	return nil
}

func validKey(key string, allowEmpty bool) bool {
	return (allowEmpty || key != "") && len(key) <= storagekv.MaxKeySize && utf8.ValidString(key) && !strings.ContainsRune(key, 0)
}

func invalidRequest(message string) error {
	return errdefs.InvalidParameter(status.Error(codes.InvalidArgument, message))
}

func recordNotFound(key string) error {
	return errdefs.NotFound(status.Errorf(codes.NotFound, "record %q not found", key))
}

// expiryComposite builds the expiry index key. Neither component can contain
// a NUL byte, so the split is unambiguous.
func expiryComposite(namespace, key string) []byte {
	composite := make([]byte, 0, len(namespace)+1+len(key))
	composite = append(composite, namespace...)
	composite = append(composite, 0)
	composite = append(composite, key...)
	return composite
}

// encodeExpiry stores an absolute UnixNano deadline as eight bytes.
func encodeExpiry(expiresAt int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(expiresAt))
	return b
}

// decodeExpiry reads a deadline written by encodeExpiry.
func decodeExpiry(meta []byte) (int64, bool) {
	if len(meta) != 8 {
		return 0, false
	}
	return int64(binary.BigEndian.Uint64(meta)), true
}

// recordExpired reports whether meta records a deadline no later than now.
// No metadata means the record is permanent.
func recordExpired(meta []byte, now int64) bool {
	expiresAt, ok := decodeExpiry(meta)
	return ok && expiresAt <= now
}

// expiryAt computes the absolute deadline for a TTL, saturating at the
// largest representable UnixNano instead of wrapping to an already-past time.
func expiryAt(now time.Time, ttl int64) int64 {
	nowNano := now.UnixNano()
	if ttl > math.MaxInt64-nowNano {
		return math.MaxInt64
	}
	return nowNano + ttl
}
