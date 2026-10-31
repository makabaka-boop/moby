package bbolt

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/containerd/errdefs/pkg/errgrpc"
	"github.com/moby/extensions"
	daemonconfigv0 "github.com/moby/moby/v2/extpoints/daemonconfig/v0"
	storagekv "github.com/moby/moby/v2/extpoints/storage/kv/v0"
	storagepb "github.com/moby/moby/v2/extpoints/storage/kv/v0/protogen"
	bolt "go.etcd.io/bbolt"
	bolterrors "go.etcd.io/bbolt/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gotest.tools/v3/assert"
	is "gotest.tools/v3/assert/cmp"
	"gotest.tools/v3/poll"
	"gotest.tools/v3/skip"
)

type testResolver map[extensions.PointID]any

func (r testResolver) Provider(point extensions.PointID, _ extensions.ExtensionID) (any, error) {
	p, ok := r[point]
	if !ok {
		return nil, errors.New("no provider")
	}
	return p, nil
}

func (r testResolver) Providers(point extensions.PointID) []extensions.ResolvedProvider {
	if p, ok := r[point]; ok {
		return []extensions.ResolvedProvider{{Impl: p}}
	}
	return nil
}

type testConfig struct{ root string }

type cancelOnErrContext struct {
	context.Context
	cancel   context.CancelFunc
	calls    int
	cancelAt int
}

func (ctx *cancelOnErrContext) Err() error {
	ctx.calls++
	if ctx.calls == ctx.cancelAt {
		ctx.cancel()
	}
	return ctx.Context.Err()
}

func (c testConfig) Get(context.Context, *daemonconfigv0.GetRequest) (*daemonconfigv0.GetResponse, error) {
	return &daemonconfigv0.GetResponse{RootDir: c.root}, nil
}

func newTestStorage(t *testing.T, root string) storagekv.Storage {
	t.Helper()
	d := Extension.Declaration()
	assert.NilError(t, d.Init(t.Context(), nil, testResolver{daemonconfigv0.Point.ID(): testConfig{root: root}}))
	t.Cleanup(func() { assert.NilError(t, d.Shutdown(context.WithoutCancel(t.Context()))) })
	return d.Providers[0].Impl.(storagekv.Storage)
}

func testKV(t *testing.T, provider storagekv.Storage, namespace string) *storagekv.KV {
	t.Helper()
	kv, err := storagekv.GetKV(testResolver{storagekv.Point.ID(): provider}, namespace)
	assert.NilError(t, err)
	return kv
}

func rpcStorage(t *testing.T, provider storagekv.Storage) storagekv.Storage {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NilError(t, err)
	server := grpc.NewServer()
	storagepb.ServerPoint.Register(server, provider)
	t.Cleanup(server.Stop)
	go func() { _ = server.Serve(listener) }()
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	assert.NilError(t, err)
	t.Cleanup(func() { assert.NilError(t, conn.Close()) })
	return storagepb.NewClient(conn)
}

func TestKVContract(t *testing.T) {
	t.Parallel()
	for _, transport := range []string{"in-process", "RPC"} {
		t.Run(transport, func(t *testing.T) {
			t.Parallel()
			provider := newTestStorage(t, t.TempDir())
			if transport == "RPC" {
				provider = rpcStorage(t, provider)
			}
			kv := testKV(t, provider, "org.my.extension")
			other := testKV(t, provider, "org.other.extension")
			shared := testKV(t, provider, "org.my.extension")
			ctx := t.Context()

			_, err := kv.Get(ctx, "jobs/one")
			assert.Check(t, cerrdefs.IsNotFound(err), "%v", err)
			assert.Check(t, cerrdefs.IsNotFound(kv.Update(ctx, "jobs/one", []byte("missing"))))
			assert.NilError(t, kv.Delete(ctx, "jobs/one"))
			assert.NilError(t, kv.Create(ctx, "jobs/one", []byte("original")))
			err = kv.Create(ctx, "jobs/one", []byte("duplicate"))
			assert.Check(t, cerrdefs.IsConflict(err), "%v", err)
			value, err := shared.Get(ctx, "jobs/one")
			assert.NilError(t, err)
			assert.Equal(t, string(value), "original")
			value[0] = 'X'
			value, err = kv.Get(ctx, "jobs/one")
			assert.NilError(t, err)
			assert.Equal(t, string(value), "original")

			_, err = other.Get(ctx, "jobs/one")
			assert.Check(t, cerrdefs.IsNotFound(err), "%v", err)
			assert.NilError(t, other.Create(ctx, "jobs/one", []byte("other")))
			assert.NilError(t, kv.Update(ctx, "jobs/one", []byte("updated")))
			value, err = kv.Get(ctx, "jobs/one")
			assert.NilError(t, err)
			assert.Equal(t, string(value), "updated")

			// Logical keys must never be interpreted as host filesystem paths.
			assert.NilError(t, kv.Create(ctx, "../../outside", nil))
			value, err = kv.Get(ctx, "../../outside")
			assert.NilError(t, err)
			assert.Equal(t, len(value), 0)
			assert.Check(t, cerrdefs.IsConflict(kv.Create(ctx, "../../outside", nil)))
			assert.NilError(t, kv.Update(ctx, "../../outside", []byte("nonempty")))

			assert.NilError(t, kv.Create(ctx, "runs/one/a", []byte("run")))
			assert.NilError(t, kv.Delete(ctx, "jobs/one"))
			assert.NilError(t, kv.Delete(ctx, "jobs/one"))
			_, err = kv.Get(ctx, "jobs/one")
			assert.Check(t, cerrdefs.IsNotFound(err), "%v", err)
			value, err = kv.Get(ctx, "runs/one/a")
			assert.NilError(t, err)
			assert.Equal(t, string(value), "run")
			value, err = other.Get(ctx, "jobs/one")
			assert.NilError(t, err)
			assert.Equal(t, string(value), "other")

			for _, key := range []string{"jobs/a", "jobs/b", "jobs/c", "jobs-old/a", "../../jobs/a"} {
				assert.NilError(t, kv.Create(ctx, key, []byte(key)))
			}
			assert.NilError(t, other.Create(ctx, "jobs/a", []byte("other namespace")))
			assert.NilError(t, kv.DeletePrefix(ctx, "jobs/"))
			assert.NilError(t, kv.DeletePrefix(ctx, "jobs/"))
			for _, key := range []string{"jobs/a", "jobs/b", "jobs/c"} {
				_, err := kv.Get(ctx, key)
				assert.Check(t, cerrdefs.IsNotFound(err), "%v", err)
			}
			for _, key := range []string{"jobs-old/a", "../../jobs/a"} {
				value, err = kv.Get(ctx, key)
				assert.NilError(t, err)
				assert.Equal(t, string(value), key)
			}
			value, err = other.Get(ctx, "jobs/a")
			assert.NilError(t, err)
			assert.Equal(t, string(value), "other namespace")
			assert.NilError(t, testKV(t, provider, "missing.namespace").DeletePrefix(ctx, "jobs/"))
			assert.NilError(t, kv.Create(ctx, "preserved", []byte("value")))
			assert.Check(t, cerrdefs.IsInvalidArgument(kv.DeletePrefix(ctx, "")))
			value, err = kv.Get(ctx, "preserved")
			assert.NilError(t, err)
			assert.Equal(t, string(value), "value")

			assert.Check(t, cerrdefs.IsInvalidArgument(kv.Create(ctx, "", nil)))
			assert.Check(t, cerrdefs.IsInvalidArgument(testKV(t, provider, "../invalid").Create(ctx, "key", nil)))
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			err = kv.Create(cancelled, "cancelled", nil)
			assert.ErrorIs(t, err, context.Canceled)
			_, err = kv.Get(ctx, "cancelled")
			assert.Check(t, cerrdefs.IsNotFound(err), "%v", err)

			page, err := kv.List(ctx, storagekv.ListOptions{Prefix: "runs/"})
			assert.NilError(t, err)
			assert.DeepEqual(t, page.Keys, []string{"runs/one/a"})
			assert.Equal(t, page.NextCursor, "")
		})
	}
}

func TestStoragePersistence(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	first := newTestStorage(t, root)
	kv := testKV(t, first, "jobs")
	assert.NilError(t, kv.Create(t.Context(), "job", []byte("original")))
	assert.NilError(t, kv.Update(t.Context(), "job", []byte("updated")))
	assert.NilError(t, kv.Create(t.Context(), "empty", nil))
	assert.NilError(t, kv.Create(t.Context(), "deleted", []byte("gone")))
	assert.NilError(t, kv.Delete(t.Context(), "deleted"))
	assert.NilError(t, kv.Create(t.Context(), "purge/a", []byte("gone")))
	assert.NilError(t, kv.Create(t.Context(), "purge/b", []byte("gone")))
	assert.NilError(t, kv.DeletePrefix(t.Context(), "purge/"))
	assert.NilError(t, first.(*storage).shutdown(t.Context()))

	second := newTestStorage(t, root)
	kv = testKV(t, second, "jobs")
	value, err := kv.Get(t.Context(), "job")
	assert.NilError(t, err)
	assert.Equal(t, string(value), "updated")
	value, err = kv.Get(t.Context(), "empty")
	assert.NilError(t, err)
	assert.Equal(t, len(value), 0)
	_, err = kv.Get(t.Context(), "deleted")
	assert.Check(t, cerrdefs.IsNotFound(err), "%v", err)
	_, err = kv.Get(t.Context(), "purge/a")
	assert.Check(t, cerrdefs.IsNotFound(err), "%v", err)
	_, err = kv.Get(t.Context(), "purge/b")
	assert.Check(t, cerrdefs.IsNotFound(err), "%v", err)
	_, err = first.Get(t.Context(), &storagekv.RecordRequest{Namespace: "jobs", Key: "job"})
	assert.Check(t, cerrdefs.IsUnavailable(err), "%v", err)
}

func TestStorageListing(t *testing.T) {
	t.Parallel()
	provider := newTestStorage(t, t.TempDir())
	kv := testKV(t, provider, "jobs")
	ctx := t.Context()
	page, err := kv.List(ctx, storagekv.ListOptions{})
	assert.NilError(t, err)
	assert.Equal(t, len(page.Keys), 0)
	for _, key := range []string{"runs/b", "jobs/c", "runs/a", "jobs/a", "jobs/b"} {
		assert.NilError(t, kv.Create(ctx, key, nil))
	}
	page, err = kv.List(ctx, storagekv.ListOptions{Prefix: "jobs/", Limit: 2})
	assert.NilError(t, err)
	assert.DeepEqual(t, page.Keys, []string{"jobs/a", "jobs/b"})
	assert.Assert(t, page.NextCursor != "")
	cursor := page.NextCursor
	// Continuation does not depend on the previous key still existing.
	assert.NilError(t, kv.Delete(ctx, "jobs/b"))
	page, err = kv.List(ctx, storagekv.ListOptions{Prefix: "jobs/", Limit: 1, Cursor: cursor})
	assert.NilError(t, err)
	assert.DeepEqual(t, page.Keys, []string{"jobs/c"})
	assert.Equal(t, page.NextCursor, "")
	_, err = kv.List(ctx, storagekv.ListOptions{Prefix: "runs/", Cursor: cursor})
	assert.Check(t, cerrdefs.IsInvalidArgument(err), "%v", err)
	_, err = testKV(t, provider, "other").List(ctx, storagekv.ListOptions{Prefix: "jobs/", Cursor: cursor})
	assert.Check(t, cerrdefs.IsInvalidArgument(err), "%v", err)
	_, err = kv.List(ctx, storagekv.ListOptions{Cursor: "invalid!"})
	assert.Check(t, cerrdefs.IsInvalidArgument(err), "%v", err)
}

func TestStorageConcurrentCreate(t *testing.T) {
	t.Parallel()
	kv := testKV(t, newTestStorage(t, t.TempDir()), "jobs")
	const writers = 16
	results := make(chan error, writers)
	var wg sync.WaitGroup
	for i := range writers {
		wg.Go(func() { results <- kv.Create(t.Context(), "same", fmt.Appendf(nil, "%d", i)) })
	}
	wg.Wait()
	close(results)
	var successes int
	for err := range results {
		if err == nil {
			successes++
		} else {
			assert.Check(t, cerrdefs.IsConflict(err), "%v", err)
		}
	}
	assert.Equal(t, successes, 1)
}

func TestStorageConcurrentReads(t *testing.T) {
	t.Parallel()
	s := newTestStorage(t, t.TempDir()).(*storage)
	const readers = 2
	entered := make(chan struct{}, readers)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseReaders := func() { releaseOnce.Do(func() { close(release) }) }
	var wg sync.WaitGroup
	results := make(chan error, readers)
	for range readers {
		wg.Go(func() {
			results <- s.transaction(t.Context(), false, func(*bolt.Tx) error {
				entered <- struct{}{}
				<-release
				return nil
			})
		})
	}
	finished := make(chan struct{})
	go func() {
		wg.Wait()
		close(finished)
	}()

	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for range readers {
		select {
		case <-entered:
		case <-timer.C:
			releaseReaders()
			select {
			case <-finished:
			case <-time.After(2 * time.Second):
				t.Fatal("read transactions did not finish after release")
			}
			t.Fatal("read transactions did not overlap")
		}
	}
	releaseReaders()
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("read transactions did not finish")
	}
	for range readers {
		assert.NilError(t, <-results)
	}
}

func TestStorageShutdownWaitsForTransaction(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	provider := newTestStorage(t, root)
	s := provider.(*storage)
	entered := make(chan struct{})
	release := make(chan struct{})
	transactionResult := make(chan error, 1)
	go func() {
		transactionResult <- s.transaction(t.Context(), true, func(tx *bolt.Tx) error {
			bucket, err := tx.CreateBucket([]byte("shutdown"))
			if err != nil {
				return err
			}
			if err := bucket.Put([]byte("key"), []byte("committed")); err != nil {
				return err
			}
			close(entered)
			<-release
			return nil
		})
	}()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("transaction did not reach its synchronization point")
	}

	shutdownResult := make(chan error, 1)
	go func() {
		shutdownResult <- s.shutdown(t.Context())
	}()

	// A pending shutdown writer prevents new readers from acquiring the lock.
	// Wait for that state instead of assuming the goroutine has been scheduled.
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for s.lifecycle.TryRLock() {
		s.lifecycle.RUnlock()
		select {
		case err := <-shutdownResult:
			close(release)
			t.Fatalf("shutdown returned before the active transaction completed: %v", err)
		case <-deadline.C:
			close(release)
			t.Fatal("shutdown did not wait on the active transaction")
		case <-ticker.C:
		}
	}
	close(release)
	select {
	case err := <-transactionResult:
		assert.NilError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("transaction did not finish after release")
	}
	select {
	case err := <-shutdownResult:
		assert.NilError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not finish after the transaction committed")
	}
	assert.Check(t, cerrdefs.IsUnavailable(s.Create(t.Context(), &storagekv.WriteRequest{
		Namespace: "shutdown",
		Key:       "after",
	})))

	reopened := testKV(t, newTestStorage(t, root), "shutdown")
	value, err := reopened.Get(t.Context(), "key")
	assert.NilError(t, err)
	assert.Equal(t, string(value), "committed")
}

func TestStorageOpenFailureUnavailable(t *testing.T) {
	t.Parallel()
	root := filepath.Join(t.TempDir(), "missing")
	provider := newTestStorage(t, root)
	_, err := provider.Get(t.Context(), &storagekv.RecordRequest{Namespace: "jobs", Key: "key"})
	assert.Check(t, cerrdefs.IsUnavailable(err), "%v", err)
	assert.ErrorIs(t, err, os.ErrNotExist)

	err = testKV(t, provider, "jobs").Create(t.Context(), "key", nil)
	assert.Check(t, cerrdefs.IsUnavailable(err), "%v", err)
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestStorageOpenContentionUnavailable(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	first := newTestStorage(t, root)
	second := newTestStorage(t, root)
	firstKV := testKV(t, first, "jobs")
	assert.NilError(t, firstKV.Create(t.Context(), "held", []byte("value")))

	err := second.Create(t.Context(), &storagekv.WriteRequest{
		Namespace: "jobs",
		Key:       "provider",
	})
	assert.Check(t, cerrdefs.IsUnavailable(err), "%v", err)
	assert.ErrorIs(t, err, bolterrors.ErrTimeout)

	secondKV := testKV(t, second, "jobs")
	err = secondKV.Create(t.Context(), "direct-helper", nil)
	assert.Check(t, cerrdefs.IsUnavailable(err), "%v", err)
	assert.ErrorIs(t, err, bolterrors.ErrTimeout)

	rpcKV := testKV(t, rpcStorage(t, second), "jobs")
	err = rpcKV.Create(t.Context(), "rpc-helper", nil)
	assert.Check(t, cerrdefs.IsUnavailable(err), "%v", err)

	assert.NilError(t, first.(*storage).shutdown(t.Context()))
	assert.NilError(t, secondKV.Create(t.Context(), "retry", []byte("available")))
}

func TestStorageValidationBeforeIO(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	provider := newTestStorage(t, root)
	ctx := t.Context()
	for _, namespace := range []string{"", ".", "../escape", "/absolute", "a/b", "a\\b", "a\x00b", strings.Repeat("a", storagekv.MaxNamespaceSize+1)} {
		err := provider.Create(ctx, &storagekv.WriteRequest{Namespace: namespace, Key: "key"})
		assert.Check(t, cerrdefs.IsInvalidArgument(err), "namespace %q: %v", namespace, err)
	}
	for _, key := range []string{"", "a\x00b", "\xff", strings.Repeat("k", storagekv.MaxKeySize+1)} {
		err := provider.Create(ctx, &storagekv.WriteRequest{Namespace: "jobs", Key: key})
		assert.Check(t, cerrdefs.IsInvalidArgument(err), "key %q: %v", key, err)
	}
	assert.Check(t, cerrdefs.IsInvalidArgument(provider.Create(ctx, nil)))
	assert.Check(t, cerrdefs.IsInvalidArgument(provider.Update(ctx, nil)))
	assert.Check(t, cerrdefs.IsInvalidArgument(provider.Delete(ctx, nil)))
	assert.Check(t, cerrdefs.IsInvalidArgument(provider.DeletePrefix(ctx, nil)))
	_, err := provider.Get(ctx, nil)
	assert.Check(t, cerrdefs.IsInvalidArgument(err))
	_, err = provider.List(ctx, nil)
	assert.Check(t, cerrdefs.IsInvalidArgument(err))
	err = provider.Create(ctx, &storagekv.WriteRequest{Namespace: "jobs", Key: "large", Value: make([]byte, storagekv.MaxValueSize+1)})
	assert.Check(t, cerrdefs.IsInvalidArgument(err))
	_, err = provider.List(ctx, &storagekv.ListRequest{Namespace: "jobs", Limit: storagekv.MaxPageSize + 1})
	assert.Check(t, cerrdefs.IsInvalidArgument(err))
	_, err = provider.List(ctx, &storagekv.ListRequest{Namespace: "jobs", Prefix: "\xff"})
	assert.Check(t, cerrdefs.IsInvalidArgument(err))
	for _, namespace := range []string{"", "../invalid"} {
		err := provider.DeletePrefix(ctx, &storagekv.DeletePrefixRequest{Namespace: namespace, Prefix: "key"})
		assert.Check(t, cerrdefs.IsInvalidArgument(err), "namespace %q: %v", namespace, err)
	}
	for _, prefix := range []string{"", "a\x00b", "\xff", strings.Repeat("p", storagekv.MaxKeySize+1)} {
		err := provider.DeletePrefix(ctx, &storagekv.DeletePrefixRequest{Namespace: "jobs", Prefix: prefix})
		assert.Check(t, cerrdefs.IsInvalidArgument(err), "prefix %q: %v", prefix, err)
	}
	_, err = os.Stat(filepath.Join(root, "extensions"))
	assert.Check(t, is.ErrorIs(err, os.ErrNotExist))
}

func TestStorageDeletePrefixCancellationIsAtomic(t *testing.T) {
	t.Parallel()
	provider := newTestStorage(t, t.TempDir())
	kv := testKV(t, provider, "jobs")
	for _, key := range []string{"batch/a", "batch/b", "batch/c"} {
		assert.NilError(t, kv.Create(t.Context(), key, []byte("preserve")))
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cancelled := &cancelOnErrContext{Context: ctx, cancel: cancel, cancelAt: 5}
	err := provider.DeletePrefix(cancelled, &storagekv.DeletePrefixRequest{Namespace: "jobs", Prefix: "batch/"})
	assert.ErrorIs(t, err, context.Canceled)
	for _, key := range []string{"batch/a", "batch/b", "batch/c"} {
		value, err := kv.Get(t.Context(), key)
		assert.NilError(t, err)
		assert.Equal(t, string(value), "preserve")
	}
}

func TestStorageLimits(t *testing.T) {
	t.Parallel()
	kv := testKV(t, rpcStorage(t, newTestStorage(t, t.TempDir())), strings.Repeat("n", storagekv.MaxNamespaceSize))
	key := strings.Repeat("k", storagekv.MaxKeySize)
	value := make([]byte, storagekv.MaxValueSize)
	assert.NilError(t, kv.Create(t.Context(), key, value))
	got, err := kv.Get(t.Context(), key)
	assert.NilError(t, err)
	assert.DeepEqual(t, got, value)
	for i := range storagekv.DefaultPageSize {
		assert.NilError(t, kv.Create(t.Context(), fmt.Sprintf("a%03d", i), nil))
	}
	page, err := kv.List(t.Context(), storagekv.ListOptions{})
	assert.NilError(t, err)
	assert.Equal(t, len(page.Keys), storagekv.DefaultPageSize)
	assert.Assert(t, page.NextCursor != "")
	page, err = kv.List(t.Context(), storagekv.ListOptions{Cursor: page.NextCursor})
	assert.NilError(t, err)
	assert.DeepEqual(t, page.Keys, []string{key})
	assert.Equal(t, page.NextCursor, "")
}

func TestStorageFilesystemProtection(t *testing.T) {
	t.Parallel()
	skip.If(t, runtime.GOOS == "windows", "symlink creation requires privileges on Windows")
	for _, target := range []string{"directory", "database"} {
		t.Run(target, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			outside := t.TempDir()
			file := filepath.Join(outside, "sentinel")
			if target == "directory" {
				assert.NilError(t, os.Symlink(outside, filepath.Join(root, "extensions")))
			} else {
				assert.NilError(t, os.WriteFile(file, []byte("untouched"), 0o644))
				// WriteFile applies the process umask; this check needs an exact mode.
				assert.NilError(t, os.Chmod(file, 0o644))
				assert.NilError(t, os.Mkdir(filepath.Join(root, "extensions"), 0o700))
				assert.NilError(t, os.Symlink(file, filepath.Join(root, "extensions", "records.db")))
			}
			kv := testKV(t, newTestStorage(t, root), "jobs")
			err := kv.Create(t.Context(), "job", nil)
			assert.ErrorContains(t, err, "symlinks are not allowed")
			if target == "directory" {
				_, err := os.Lstat(filepath.Join(outside, "records.db"))
				assert.Check(t, is.ErrorIs(err, os.ErrNotExist))
				return
			}
			data, err := os.ReadFile(file)
			assert.NilError(t, err)
			assert.Equal(t, string(data), "untouched")
			info, err := os.Stat(file)
			assert.NilError(t, err)
			assert.Equal(t, info.Mode().Perm(), os.FileMode(0o644))
		})
	}
}

func TestStoragePrivatePermissions(t *testing.T) {
	t.Parallel()
	skip.If(t, runtime.GOOS == "windows", "Unix permissions")
	root := t.TempDir()
	first := newTestStorage(t, root)
	assert.NilError(t, testKV(t, first, "jobs").Create(t.Context(), "key", nil))
	assert.NilError(t, first.(*storage).shutdown(t.Context()))
	dir := filepath.Join(root, "extensions")
	db := filepath.Join(dir, "records.db")
	assert.NilError(t, os.Chmod(dir, 0o755))
	assert.NilError(t, os.Chmod(db, 0o644))
	second := newTestStorage(t, root)
	_, err := testKV(t, second, "jobs").Get(t.Context(), "key")
	assert.NilError(t, err)
	for path, mode := range map[string]os.FileMode{dir: 0o700, db: 0o600} {
		info, err := os.Stat(path)
		assert.NilError(t, err)
		assert.Equal(t, info.Mode().Perm(), mode)
	}
}

func TestStorageDoesNotReplaceCorruptDatabase(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	dir := filepath.Join(root, "extensions")
	assert.NilError(t, os.Mkdir(dir, 0o700))
	path := filepath.Join(dir, "records.db")
	assert.NilError(t, os.WriteFile(path, []byte("corrupt database"), 0o600))
	kv := testKV(t, newTestStorage(t, root), "jobs")
	_, err := kv.Get(t.Context(), "key")
	assert.Assert(t, err != nil)
	assert.Check(t, !cerrdefs.IsNotFound(err))
	data, err := os.ReadFile(path)
	assert.NilError(t, err)
	assert.Equal(t, string(data), "corrupt database")
}

func TestStorageInitialization(t *testing.T) {
	t.Parallel()
	for _, root := range []string{"", "relative"} {
		d := Extension.Declaration()
		err := d.Init(t.Context(), nil, testResolver{daemonconfigv0.Point.ID(): testConfig{root: root}})
		assert.Assert(t, err != nil)
		assert.NilError(t, d.Shutdown(t.Context()))
	}
	d := Extension.Declaration()
	assert.Assert(t, d.Init(t.Context(), nil, testResolver{}) != nil)
	assert.NilError(t, d.Shutdown(t.Context()))
	_, err := storagekv.GetKV(testResolver{}, "jobs")
	assert.Assert(t, err != nil)
}

func waitForNotFound(t *testing.T, kv *storagekv.KV, key string) {
	t.Helper()
	poll.WaitOn(t, func(poll.LogT) poll.Result {
		_, err := kv.Get(t.Context(), key)
		switch {
		case cerrdefs.IsNotFound(err):
			return poll.Success()
		case err != nil:
			return poll.Error(err)
		default:
			return poll.Continue("record %q is still live", key)
		}
	}, poll.WithDelay(time.Millisecond), poll.WithTimeout(10*time.Second))
}

func TestStorageTTL(t *testing.T) {
	t.Parallel()
	for _, transport := range []string{"in-process", "RPC"} {
		t.Run(transport, func(t *testing.T) {
			t.Parallel()
			provider := newTestStorage(t, t.TempDir())
			if transport == "RPC" {
				provider = rpcStorage(t, provider)
			}
			kv := testKV(t, provider, "jobs")
			ctx := t.Context()
			// Generous deadline: under -race the parallel tests can delay an
			// RPC round trip well past a sub-100ms TTL before a record is read
			// back, which would masquerade as an implementation failure.
			const ttl = time.Second

			// Negative TTL is rejected at the helper and at the contract,
			// so the check survives transport boundaries as well.
			assert.Check(t, cerrdefs.IsInvalidArgument(kv.Create(ctx, "negative", nil, -time.Nanosecond)))
			assert.Check(t, cerrdefs.IsInvalidArgument(kv.Update(ctx, "negative", nil, -time.Nanosecond)))
			// The provider itself rejects negative TTL, which keeps in-process
			// and RPC semantics identical regardless of the client helper.
			err := provider.Create(ctx, &storagekv.WriteRequest{
				Namespace: "jobs",
				Key:       "negative-raw",
				TTL:       -1,
			})
			assert.Check(t, cerrdefs.IsInvalidArgument(errgrpc.ToNative(err)), "%v", err)

			assert.NilError(t, kv.Create(ctx, "volatile", []byte("one"), ttl))
			value, err := kv.Get(ctx, "volatile")
			assert.NilError(t, err)
			assert.Equal(t, string(value), "one")
			assert.Check(t, cerrdefs.IsConflict(kv.Create(ctx, "volatile", []byte("two"), ttl)))

			waitForNotFound(t, kv, "volatile")

			// Expired records are absent for update and create too.
			assert.Check(t, cerrdefs.IsNotFound(kv.Update(ctx, "volatile", []byte("stale"))))
			assert.NilError(t, kv.Create(ctx, "volatile", []byte("rebuilt"), ttl))
			value, err = kv.Get(ctx, "volatile")
			assert.NilError(t, err)
			assert.Equal(t, string(value), "rebuilt")
			waitForNotFound(t, kv, "volatile")

			// Update replaces the expiry: omitting the TTL clears it.
			assert.NilError(t, kv.Create(ctx, "cleared", []byte("first"), ttl))
			assert.NilError(t, kv.Update(ctx, "cleared", []byte("second")))
			time.Sleep(2 * ttl)
			value, err = kv.Get(ctx, "cleared")
			assert.NilError(t, err)
			assert.Equal(t, string(value), "second")

			// A fresh TTL on update restarts the deadline from the update.
			assert.NilError(t, kv.Create(ctx, "renewed", nil, ttl))
			assert.NilError(t, kv.Update(ctx, "renewed", []byte("alive"), time.Hour))
			time.Sleep(2 * ttl)
			value, err = kv.Get(ctx, "renewed")
			assert.NilError(t, err)
			assert.Equal(t, string(value), "alive")

			// Existing calls without a TTL keep permanent storage behavior.
			assert.NilError(t, kv.Create(ctx, "permanent", []byte("keep")))
			time.Sleep(2 * ttl)
			value, err = kv.Get(ctx, "permanent")
			assert.NilError(t, err)
			assert.Equal(t, string(value), "keep")
		})
	}
}

func TestStorageTTLListSkipsExpired(t *testing.T) {
	t.Parallel()
	for _, transport := range []string{"in-process", "RPC"} {
		t.Run(transport, func(t *testing.T) {
			t.Parallel()
			provider := newTestStorage(t, t.TempDir())
			if transport == "RPC" {
				provider = rpcStorage(t, provider)
			}
			kv := testKV(t, provider, "jobs")
			ctx := t.Context()
			for _, key := range []string{"jobs/a", "jobs/c", "jobs/d"} {
				assert.NilError(t, kv.Create(ctx, key, nil))
			}
			assert.NilError(t, kv.Create(ctx, "jobs/b", nil, time.Second))

			waitForNotFound(t, kv, "jobs/b")

			// The expired key is omitted and does not fill either page.
			page, err := kv.List(ctx, storagekv.ListOptions{Prefix: "jobs/", Limit: 2})
			assert.NilError(t, err)
			assert.DeepEqual(t, page.Keys, []string{"jobs/a", "jobs/c"})
			assert.Assert(t, page.NextCursor != "")
			page, err = kv.List(ctx, storagekv.ListOptions{Prefix: "jobs/", Limit: 2, Cursor: page.NextCursor})
			assert.NilError(t, err)
			assert.DeepEqual(t, page.Keys, []string{"jobs/d"})
			assert.Equal(t, page.NextCursor, "")
		})
	}
}

func TestStorageTTLExpiryPersistsAcrossRestart(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	first := newTestStorage(t, root)
	kv := testKV(t, first, "jobs")
	ctx := t.Context()
	const ttl = 5 * time.Second
	assert.NilError(t, kv.Create(ctx, "job", []byte("v"), ttl))
	assert.NilError(t, kv.Create(ctx, "long", []byte("v"), time.Hour))
	assert.NilError(t, kv.Create(ctx, "forever", []byte("v")))

	// Let most, but not all, of the deadline elapse before the restart.
	time.Sleep(2 * time.Second)
	_, err := kv.Get(ctx, "job")
	assert.NilError(t, err)
	assert.NilError(t, first.(*storage).shutdown(ctx))

	second := newTestStorage(t, root)
	kv = testKV(t, second, "jobs")
	// The persisted deadline is absolute: only a few seconds remain instead
	// of a fresh full TTL, so the record expires shortly after the restart.
	poll.WaitOn(t, func(poll.LogT) poll.Result {
		if _, err := kv.Get(ctx, "job"); cerrdefs.IsNotFound(err) {
			return poll.Success()
		}
		return poll.Continue("record still live after restart")
	}, poll.WithDelay(time.Millisecond), poll.WithTimeout(6*time.Second))

	for _, key := range []string{"long", "forever"} {
		value, err := kv.Get(ctx, key)
		assert.NilError(t, err)
		assert.Equal(t, string(value), "v")
	}
}

func TestStorageExpirySweeper(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	provider := newTestStorage(t, root).(*storage)
	// The database opens lazily on the first operation, so the override
	// takes effect when the sweeper starts.
	provider.cleanupInterval = 5 * time.Millisecond
	kv := testKV(t, provider, "jobs")
	ctx := t.Context()

	assert.NilError(t, kv.Create(ctx, "gone", []byte("v"), 100*time.Millisecond))
	// The renewal below happens after a wall-clock sleep that a -race build can
	// stretch, so give the original deadline ample headroom.
	assert.NilError(t, kv.Create(ctx, "renewed", []byte("v1"), 5*time.Second))
	assert.NilError(t, kv.Create(ctx, "rebuilt", []byte("v1"), 100*time.Millisecond))
	assert.NilError(t, kv.Create(ctx, "forever", []byte("v")))

	// Renew before the original deadline while sweeps are already running.
	time.Sleep(40 * time.Millisecond)
	assert.NilError(t, kv.Update(ctx, "renewed", []byte("v2"), time.Hour))

	// Let a record expire, then rebuild it under the same name.
	waitForNotFound(t, kv, "rebuilt")
	assert.NilError(t, kv.Create(ctx, "rebuilt", []byte("v2"), time.Hour))

	// Deleted records take their expiry index entry with them.
	assert.NilError(t, kv.Create(ctx, "deleted", nil, time.Hour))
	assert.NilError(t, kv.Delete(ctx, "deleted"))
	assert.NilError(t, kv.Create(ctx, "batch/a", nil, time.Hour))
	assert.NilError(t, kv.DeletePrefix(ctx, "batch/"))

	// The sweeper physically removes the expired record and its index entry
	// without anyone accessing its key.
	poll.WaitOn(t, func(poll.LogT) poll.Result {
		value, meta := rawRecord(t, provider, "jobs", "gone")
		if value == nil && meta == nil {
			return poll.Success()
		}
		return poll.Continue("expired record not swept yet")
	}, poll.WithDelay(time.Millisecond), poll.WithTimeout(10*time.Second))

	// Repeated sweeps past the original deadlines must not remove the renewed
	// record, the same-name rebuild, or permanent records.
	time.Sleep(300 * time.Millisecond)
	for key, want := range map[string]string{"renewed": "v2", "rebuilt": "v2", "forever": "v"} {
		value, meta := rawRecord(t, provider, "jobs", key)
		assert.Equal(t, string(value), want, key)
		assert.Assert(t, meta != nil || key == "forever", key)
	}
	for _, key := range []string{"deleted", "batch/a", "gone"} {
		value, meta := rawRecord(t, provider, "jobs", key)
		assert.Assert(t, value == nil, key)
		assert.Assert(t, meta == nil, key)
	}
}

// rawRecord reads the database through the provider's own read transaction,
// bypassing the expiry semantics so tests can observe physical deletion.
func rawRecord(t *testing.T, s *storage, namespace, key string) (value, meta []byte) {
	t.Helper()
	assert.NilError(t, s.transaction(t.Context(), false, func(tx *bolt.Tx) error {
		if bucket := tx.Bucket([]byte(namespace)); bucket != nil {
			value = bytes.Clone(bucket.Get([]byte(key)))
		}
		if expiry := tx.Bucket([]byte(expiryBucketName)); expiry != nil {
			meta = bytes.Clone(expiry.Get(expiryComposite(namespace, key)))
		}
		return nil
	}))
	return value, meta
}
