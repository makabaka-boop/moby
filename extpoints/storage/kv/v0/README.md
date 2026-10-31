# Persistent record storage

Store opaque key/value records without depending on host-local paths.
Declare `storagekv.Point.Dependency()` and resolve a namespace during extension initialization:

```go
kv, err := storagekv.GetKV(resolver, "org.my.extension")
if err != nil {
    return err
}
```

Use the namespace-bound helper for record operations:

```go
err = kv.Create(ctx, "jobs/123", data)
data, err = kv.Get(ctx, "jobs/123")
err = kv.Update(ctx, "jobs/123", updatedData)
err = kv.Delete(ctx, "jobs/123")
err = kv.DeletePrefix(ctx, "jobs/")
page, err := kv.List(ctx, storagekv.ListOptions{Prefix: "jobs/"})
```

## Record operations

- Keys are logical strings, not paths; consumers own value encoding and schema versions.
- `Create` requires an absent key; `Update` requires an existing key; `Delete` tolerates absence.
- `DeletePrefix` atomically and durably removes all matching records; a nonempty prefix is required, and no matches (including a missing namespace) succeeds.
- Individual writes are durable and atomic per record.
  `DeletePrefix` is the only multi-record atomic operation; there is no concurrent-update protection.
- Listing is paginated, not a snapshot; continue with `NextCursor` using the same prefix.
- The helper needs no `Close`; records survive provider restarts and extension removal.

## Read-only snapshots

Recovery and enumeration that page through records while other handlers write need one stable view.
Open a snapshot for the namespace, read from it, then close it:

```go
snap, err := kv.OpenSnapshot(ctx)
if err != nil {
    return err
}
defer snap.Close(ctx)

value, err := snap.Get(ctx, "jobs/123")
first, err := snap.List(ctx, storagekv.SnapshotListOptions{Prefix: "jobs/", Limit: 100})
next, err := snap.List(ctx, storagekv.SnapshotListOptions{Prefix: "jobs/", Limit: 100, Cursor: first.NextCursor})
```

- Every read observes the state captured when the snapshot opened; later creates, updates, and deletes never change it, including keys deleted afterwards and keys missing at open time.
- Paging follows the same ascending byte order, prefix rules, `DefaultPageSize`, and `MaxPageSize` as `List`. A cursor belongs to one snapshot, namespace, and prefix; reusing it elsewhere fails as an invalid parameter.
- Malformed handles fail as invalid parameters. Handles that are unknown, already closed, or minted before a provider restart fail with a recognizable error; check it with `storagekv.IsInvalidSnapshot(err)`.
- `Close` releases the snapshot's resources. Leaving snapshots open never blocks ordinary writes or provider shutdown; open handles simply become invalid after shutdown or restart.
- In-process and RPC calls behave identically; non-snapshot operations are unaffected.

Executable consumers register the generated `protogen.ClientPoint` with their SDK client-point registry.
See [storage.go](storage.go) for the full contract and size limits.

## Namespace and trust model

Namespaces partition records for cooperative use; they are not an authorization boundary.
All trusted installed extensions that can reach this provider can access any namespace.
v0 performs no identity-based authorization, and namespace names do not authenticate ownership.
Any future identity enforcement belongs in host-resolved identity and per-caller storage wiring.
Identity enforcement must still allow explicit namespace sharing.

## Persistence and lifecycle

Records are not purged automatically when an extension is absent or uninstalled.
Each extension owns cleanup of its individual records.
v0 stores no namespace ownership metadata and defines no garbage-collection policy; policy is deferred to v1.
Do not delete the shared database to remove one extension's data.
Corruption of the shared database affects all namespaces, and storage provides no automatic repair or replacement.
Storage does not automatically compact the database or reclaim file space.
