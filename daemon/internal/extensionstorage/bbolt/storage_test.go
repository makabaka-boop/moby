package bbolt

import (
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
	"github.com/moby/extensions"
	daemonconfigv0 "github.com/moby/moby/v2/extpoints/daemonconfig/v0"
	storagekv "github.com/moby/moby/v2/extpoints/storage/kv/v0"
	storagepb "github.com/moby/moby/v2/extpoints/storage/kv/v0/protogen"
	bolt "go.etcd.io/bbolt"
	bolterrors "go.etcd.io/bbolt/errors"
	"google.golang.org/grpc"
	grpcCodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	grpcStatus "google.golang.org/grpc/status"
	"gotest.tools/v3/assert"
	is "gotest.tools/v3/assert/cmp"
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

// isInvalidSnapshot recognizes the failed-precondition category both in
// process (moby errdefs markers) and over gRPC (raw status errors returned
// by raw provider calls that skip the KV helper's error translation).
func isInvalidSnapshot(err error) bool {
	if storagekv.IsInvalidSnapshot(err) {
		return true
	}
	st, ok := grpcStatus.FromError(err)
	return ok && st.Code() == grpcCodes.FailedPrecondition
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
	_, err = provider.OpenSnapshot(ctx, nil)
	assert.Check(t, cerrdefs.IsInvalidArgument(err))
	_, err = provider.OpenSnapshot(ctx, &storagekv.OpenSnapshotRequest{Namespace: "../bad"})
	assert.Check(t, cerrdefs.IsInvalidArgument(err))
	assert.Check(t, cerrdefs.IsInvalidArgument(provider.CloseSnapshot(ctx, nil)))
	assert.Check(t, cerrdefs.IsInvalidArgument(provider.CloseSnapshot(ctx, &storagekv.CloseSnapshotRequest{Namespace: "jobs", Snapshot: "garbage"})))
	_, err = provider.SnapshotGet(ctx, nil)
	assert.Check(t, cerrdefs.IsInvalidArgument(err))
	_, err = provider.SnapshotGet(ctx, &storagekv.SnapshotGetRequest{Namespace: "jobs", Snapshot: "garbage", Key: "key"})
	assert.Check(t, cerrdefs.IsInvalidArgument(err))
	_, err = provider.SnapshotList(ctx, nil)
	assert.Check(t, cerrdefs.IsInvalidArgument(err))
	_, err = provider.SnapshotList(ctx, &storagekv.SnapshotListRequest{Namespace: "jobs", Snapshot: "garbage"})
	assert.Check(t, cerrdefs.IsInvalidArgument(err))
	_, err = provider.SnapshotList(ctx, &storagekv.SnapshotListRequest{Namespace: "jobs", Prefix: "\xff"})
	assert.Check(t, cerrdefs.IsInvalidArgument(err))
	_, err = provider.SnapshotList(ctx, &storagekv.SnapshotListRequest{Namespace: "jobs", Limit: storagekv.MaxPageSize + 1})
	assert.Check(t, cerrdefs.IsInvalidArgument(err))
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

func TestStorageSnapshots(t *testing.T) {
	t.Parallel()
	for _, transport := range []string{"in-process", "RPC"} {
		t.Run(transport, func(t *testing.T) {
			t.Parallel()
			provider := newTestStorage(t, t.TempDir())
			if transport == "RPC" {
				provider = rpcStorage(t, provider)
			}
			kv := testKV(t, provider, "jobs")
			other := testKV(t, provider, "other")
			ctx := t.Context()

			for _, rec := range []struct{ key, value string }{
				{"p/a", "old-a"},
				{"p/b", "old-b"},
				{"p/gone", "old-gone"},
				{"z", "old-z"},
			} {
				assert.NilError(t, kv.Create(ctx, rec.key, []byte(rec.value)))
			}
			assert.NilError(t, other.Create(ctx, "k", []byte("other")))

			snap, err := kv.OpenSnapshot(ctx)
			assert.NilError(t, err)
			// Keep a raw handle to verify namespace binding at the provider level.
			opened, err := provider.OpenSnapshot(ctx, &storagekv.OpenSnapshotRequest{Namespace: "jobs"})
			assert.NilError(t, err)
			rawHandle := opened.Snapshot

			// Mutations after opening must never reach the snapshot.
			assert.NilError(t, kv.Update(ctx, "p/a", []byte("new-a")))
			assert.NilError(t, kv.Create(ctx, "p/new", []byte("new")))
			assert.NilError(t, kv.Delete(ctx, "p/gone"))
			assert.NilError(t, kv.DeletePrefix(ctx, "p/b"))
			assert.NilError(t, other.Update(ctx, "k", []byte("other-new")))

			value, err := snap.Get(ctx, "p/a")
			assert.NilError(t, err)
			assert.Equal(t, string(value), "old-a")
			// Returned bytes are owned by the caller: mutating them is harmless.
			value[0] = 'X'
			value, err = snap.Get(ctx, "p/a")
			assert.NilError(t, err)
			assert.Equal(t, string(value), "old-a")
			value, err = snap.Get(ctx, "p/b")
			assert.NilError(t, err)
			assert.Equal(t, string(value), "old-b")
			value, err = snap.Get(ctx, "p/gone")
			assert.NilError(t, err)
			assert.Equal(t, string(value), "old-gone")
			_, err = snap.Get(ctx, "p/new")
			assert.Check(t, cerrdefs.IsNotFound(err), "%v", err)
			_, err = snap.Get(ctx, "missing")
			assert.Check(t, cerrdefs.IsNotFound(err), "%v", err)

			// The live view follows the writes.
			value, err = kv.Get(ctx, "p/a")
			assert.NilError(t, err)
			assert.Equal(t, string(value), "new-a")
			_, err = kv.Get(ctx, "p/gone")
			assert.Check(t, cerrdefs.IsNotFound(err), "%v", err)
			page, err := kv.List(ctx, storagekv.ListOptions{Prefix: "p/"})
			assert.NilError(t, err)
			assert.DeepEqual(t, page.Keys, []string{"p/a", "p/new"})

			// A full snapshot listing stays at the opened state.
			page, err = snap.List(ctx, storagekv.SnapshotListOptions{Limit: 10})
			assert.NilError(t, err)
			assert.DeepEqual(t, page.Keys, []string{"p/a", "p/b", "p/gone", "z"})
			assert.Equal(t, page.NextCursor, "")

			// Prefix paging follows existing key order and page limits.
			first, err := snap.List(ctx, storagekv.SnapshotListOptions{Prefix: "p/", Limit: 2})
			assert.NilError(t, err)
			assert.DeepEqual(t, first.Keys, []string{"p/a", "p/b"})
			assert.Assert(t, first.NextCursor != "")
			var keys []string
			cursor := first.NextCursor
			for {
				next, err := snap.List(ctx, storagekv.SnapshotListOptions{Prefix: "p/", Limit: 2, Cursor: cursor})
				assert.NilError(t, err)
				keys = append(keys, next.Keys...)
				if next.NextCursor == "" {
					break
				}
				cursor = next.NextCursor
			}
			assert.DeepEqual(t, keys, []string{"p/gone"})

			// Cursors cannot leave their prefix, snapshot, or namespace.
			_, err = snap.List(ctx, storagekv.SnapshotListOptions{Prefix: "z/", Limit: 2, Cursor: first.NextCursor})
			assert.Check(t, cerrdefs.IsInvalidArgument(err), "%v", err)
			_, err = snap.List(ctx, storagekv.SnapshotListOptions{Prefix: "p/", Limit: 2, Cursor: first.NextCursor + "garbage"})
			assert.Check(t, cerrdefs.IsInvalidArgument(err), "%v", err)
			snap2, err := kv.OpenSnapshot(ctx)
			assert.NilError(t, err)
			_, err = snap2.List(ctx, storagekv.SnapshotListOptions{Prefix: "p/", Limit: 2, Cursor: first.NextCursor})
			assert.Check(t, cerrdefs.IsInvalidArgument(err), "%v", err)
			_, err = provider.SnapshotList(ctx, &storagekv.SnapshotListRequest{
				Namespace: "other", Snapshot: rawHandle, Prefix: "p/", Cursor: first.NextCursor,
			})
			assert.Check(t, isInvalidSnapshot(err), "%v", err)
			_, err = provider.SnapshotGet(ctx, &storagekv.SnapshotGetRequest{
				Namespace: "other", Snapshot: rawHandle, Key: "k",
			})
			assert.Check(t, isInvalidSnapshot(err), "%v", err)

			if transport == "in-process" {
				entries, err := os.ReadDir(snapshotDirPath(provider.(*storage).dataRoot))
				assert.NilError(t, err)
				assert.Equal(t, len(entries), 3)
			}

			assert.NilError(t, snap2.Close(ctx))
			assert.NilError(t, snap.Close(ctx))
			assert.NilError(t, provider.CloseSnapshot(ctx, &storagekv.CloseSnapshotRequest{Namespace: "jobs", Snapshot: rawHandle}))

			if transport == "in-process" {
				entries, err := os.ReadDir(snapshotDirPath(provider.(*storage).dataRoot))
				assert.NilError(t, err)
				assert.Equal(t, len(entries), 0)
			}

			// Closed snapshots fail every operation with a recognizable error.
			_, err = snap.Get(ctx, "p/a")
			assert.Check(t, storagekv.IsInvalidSnapshot(err), "%v", err)
			_, err = snap.List(ctx, storagekv.SnapshotListOptions{})
			assert.Check(t, storagekv.IsInvalidSnapshot(err), "%v", err)
			err = snap.Close(ctx)
			assert.Check(t, storagekv.IsInvalidSnapshot(err), "%v", err)
		})
	}
}

func TestStorageSnapshotRestart(t *testing.T) {
	t.Parallel()
	for _, transport := range []string{"in-process", "RPC"} {
		t.Run(transport, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			ctx := t.Context()

			first := newTestStorage(t, root)
			kv := testKV(t, first, "jobs")
			assert.NilError(t, kv.Create(ctx, "job", []byte("v1")))
			opened, err := first.OpenSnapshot(ctx, &storagekv.OpenSnapshotRequest{Namespace: "jobs"})
			assert.NilError(t, err)
			handle := opened.Snapshot
			s := first.(*storage)
			assert.Equal(t, len(s.snapshots), 1)

			// Leave the snapshot deliberately open while shutting down.
			assert.NilError(t, s.shutdown(ctx))
			assert.Equal(t, len(s.snapshots), 0)
			entries, err := os.ReadDir(snapshotDirPath(root))
			assert.NilError(t, err)
			assert.Equal(t, len(entries), 0)
			_, err = first.OpenSnapshot(ctx, &storagekv.OpenSnapshotRequest{Namespace: "jobs"})
			assert.Check(t, cerrdefs.IsUnavailable(err), "%v", err)

			second := newTestStorage(t, root)
			var provider storagekv.Storage = second
			if transport == "RPC" {
				provider = rpcStorage(t, second)
			}
			restored := testKV(t, provider, "jobs")
			value, err := restored.Get(ctx, "job")
			assert.NilError(t, err)
			assert.Equal(t, string(value), "v1")
			assert.NilError(t, restored.Create(ctx, "after", []byte("v2")))

			// Handles from before the restart are stale on the new provider.
			_, err = provider.SnapshotGet(ctx, &storagekv.SnapshotGetRequest{Namespace: "jobs", Snapshot: handle, Key: "job"})
			assert.Check(t, isInvalidSnapshot(err), "%v", err)
			_, err = provider.SnapshotList(ctx, &storagekv.SnapshotListRequest{Namespace: "jobs", Snapshot: handle})
			assert.Check(t, isInvalidSnapshot(err), "%v", err)
			err = provider.CloseSnapshot(ctx, &storagekv.CloseSnapshotRequest{Namespace: "jobs", Snapshot: handle})
			assert.Check(t, isInvalidSnapshot(err), "%v", err)

			// Snapshots on the restarted provider work and read persisted data.
			snap, err := restored.OpenSnapshot(ctx)
			assert.NilError(t, err)
			value, err = snap.Get(ctx, "after")
			assert.NilError(t, err)
			assert.Equal(t, string(value), "v2")
			assert.NilError(t, snap.Close(ctx))
		})
	}
}

func TestStorageOpenSnapshotsDoNotBlock(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	provider := newTestStorage(t, root)
	s := provider.(*storage)
	kv := testKV(t, provider, "jobs")
	ctx := t.Context()
	assert.NilError(t, kv.Create(ctx, "k", []byte("v")))

	snap, err := kv.OpenSnapshot(ctx)
	assert.NilError(t, err)

	// Writes keep succeeding while the snapshot stays open.
	for i := range 100 {
		assert.NilError(t, kv.Update(ctx, "k", fmt.Appendf(nil, "v%d", i)))
		assert.NilError(t, kv.Create(ctx, fmt.Sprintf("k%03d", i), nil))
	}
	value, err := snap.Get(ctx, "k")
	assert.NilError(t, err)
	assert.Equal(t, string(value), "v")

	// Provider shutdown must not wait for the snapshot to be closed.
	done := make(chan error, 1)
	go func() { done <- s.shutdown(context.WithoutCancel(ctx)) }()
	select {
	case err := <-done:
		assert.NilError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown blocked by open snapshot")
	}
	assert.Equal(t, len(s.snapshots), 0)
	entries, err := os.ReadDir(snapshotDirPath(root))
	assert.NilError(t, err)
	assert.Equal(t, len(entries), 0)
	_, err = snap.Get(ctx, "k")
	assert.Check(t, storagekv.IsInvalidSnapshot(err), "%v", err)

	// The provider can reopen against the same data and serve writes.
	next := newTestStorage(t, root)
	assert.NilError(t, testKV(t, next, "jobs").Create(ctx, "again", nil))
}

func TestStorageStaleSnapshotsRemovedOnOpen(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	dir := snapshotDirPath(root)
	assert.NilError(t, os.MkdirAll(dir, 0o700))
	assert.NilError(t, os.WriteFile(filepath.Join(dir, "snapshot-99-99.db"), []byte("not a database"), 0o600))
	assert.NilError(t, os.WriteFile(filepath.Join(dir, "keep.txt"), []byte("unrelated"), 0o600))
	provider := newTestStorage(t, root)
	// The first operation lazily opens the database and sweeps leftovers.
	assert.NilError(t, testKV(t, provider, "jobs").Create(t.Context(), "k", nil))
	_, err := os.Stat(filepath.Join(dir, "snapshot-99-99.db"))
	assert.Check(t, is.ErrorIs(err, os.ErrNotExist))
	_, err = os.Stat(filepath.Join(dir, "keep.txt"))
	assert.NilError(t, err)
}
