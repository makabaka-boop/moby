package storagekvv0

import (
	"context"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/containerd/errdefs/pkg/errgrpc"
	"github.com/moby/extensions"
	"github.com/moby/moby/v2/errdefs"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// KV accesses records in one namespace.
// It owns no resources and does not need to be closed.
type KV struct {
	provider  Storage
	namespace string
}

// GetKV resolves the storage provider and binds it to namespace.
// Call it during initialization after declaring Point.Dependency().
// It performs no storage I/O; namespace validation happens on each operation.
func GetKV(resolver extensions.Resolver, namespace string) (*KV, error) {
	provider, err := Point.Single(resolver)
	if err != nil {
		return nil, err
	}
	return &KV{provider: provider, namespace: namespace}, nil
}

// Get reads a record, or returns a not-found error if it is absent.
func (kv *KV) Get(ctx context.Context, key string) ([]byte, error) {
	resp, err := kv.provider.Get(ctx, &RecordRequest{Namespace: kv.namespace, Key: key})
	if err != nil {
		return nil, recordError(err)
	}
	return resp.Value, nil
}

// Create writes a new record, or returns a conflict if the key exists.
func (kv *KV) Create(ctx context.Context, key string, value []byte) error {
	return recordError(kv.provider.Create(ctx, &WriteRequest{Namespace: kv.namespace, Key: key, Value: value}))
}

// Update replaces a record, or returns a not-found error if it is absent.
func (kv *KV) Update(ctx context.Context, key string, value []byte) error {
	return recordError(kv.provider.Update(ctx, &WriteRequest{Namespace: kv.namespace, Key: key, Value: value}))
}

// Delete removes a record; deleting an absent record succeeds.
func (kv *KV) Delete(ctx context.Context, key string) error {
	return recordError(kv.provider.Delete(ctx, &RecordRequest{Namespace: kv.namespace, Key: key}))
}

// DeletePrefix atomically removes records matching the nonempty key prefix.
// Deleting a prefix with no matching records succeeds.
func (kv *KV) DeletePrefix(ctx context.Context, prefix string) error {
	return recordError(kv.provider.DeletePrefix(ctx, &DeletePrefixRequest{Namespace: kv.namespace, Prefix: prefix}))
}

// ListOptions selects a page of keys in a KV namespace.
type ListOptions struct {
	Prefix string
	Limit  uint32
	Cursor string
}

// List returns matching keys in ascending byte order, not a snapshot.
// Reuse NextCursor with the same prefix to continue listing.
func (kv *KV) List(ctx context.Context, opts ListOptions) (*ListResponse, error) {
	resp, err := kv.provider.List(ctx, &ListRequest{
		Namespace: kv.namespace,
		Prefix:    opts.Prefix,
		Limit:     opts.Limit,
		Cursor:    opts.Cursor,
	})
	return resp, recordError(err)
}

// Snapshot is a read-only, point-in-time view of one namespace.
// Reads observe the state captured when the snapshot opened and are not
// affected by later writes. Close releases the provider's resources; leaving
// a snapshot open does not block writes or provider shutdown, but the handle
// becomes invalid once the provider shuts down or restarts.
type Snapshot struct {
	provider  Storage
	namespace string
	handle    string
}

// OpenSnapshot opens a read-only, point-in-time view of the namespace.
// Call Close on the returned snapshot when recovery or enumeration ends.
func (kv *KV) OpenSnapshot(ctx context.Context) (*Snapshot, error) {
	resp, err := kv.provider.OpenSnapshot(ctx, &OpenSnapshotRequest{Namespace: kv.namespace})
	if err != nil {
		return nil, recordError(err)
	}
	return &Snapshot{provider: kv.provider, namespace: kv.namespace, handle: resp.Snapshot}, nil
}

// Get reads a record from the snapshot, or returns a not-found error if the
// key was absent when the snapshot opened.
func (s *Snapshot) Get(ctx context.Context, key string) ([]byte, error) {
	resp, err := s.provider.SnapshotGet(ctx, &SnapshotGetRequest{
		Namespace: s.namespace,
		Snapshot:  s.handle,
		Key:       key,
	})
	if err != nil {
		return nil, recordError(err)
	}
	return resp.Value, nil
}

// SnapshotListOptions selects a page of keys in a snapshot.
type SnapshotListOptions struct {
	Prefix string
	Limit  uint32
	Cursor string
}

// List returns matching keys in ascending byte order at the snapshot's
// point in time. Reuse NextCursor with the same prefix to continue; a cursor
// belongs to its snapshot and never works on another one.
func (s *Snapshot) List(ctx context.Context, opts SnapshotListOptions) (*ListResponse, error) {
	resp, err := s.provider.SnapshotList(ctx, &SnapshotListRequest{
		Namespace: s.namespace,
		Snapshot:  s.handle,
		Prefix:    opts.Prefix,
		Limit:     opts.Limit,
		Cursor:    opts.Cursor,
	})
	return resp, recordError(err)
}

// Close releases the snapshot's resources.
func (s *Snapshot) Close(ctx context.Context) error {
	return recordError(s.provider.CloseSnapshot(ctx, &CloseSnapshotRequest{
		Namespace: s.namespace,
		Snapshot:  s.handle,
	}))
}

// IsInvalidSnapshot reports whether err marks a snapshot handle that is no
// longer usable: it is unknown to the provider, was already closed, or was
// opened before a provider restart. Malformed handles and cursors instead
// fail as invalid parameters.
func IsInvalidSnapshot(err error) bool {
	return cerrdefs.IsFailedPrecondition(err)
}

// Preserve native errors, including context cancellation and filesystem causes.
// RPC clients need their status errors translated back to Go error categories.
func recordError(err error) error {
	if err == nil || cerrdefs.IsUnavailable(err) {
		return err
	}
	s, ok := status.FromError(err)
	if !ok {
		return err
	}
	native := errgrpc.ToNative(err)
	if s.Code() == codes.AlreadyExists {
		return errdefs.Conflict(native)
	}
	return native
}
