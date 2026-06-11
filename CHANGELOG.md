## 0.3.0

### Added

- `close()`, `exists()`, and `listStoredPaths()` on `MultiIsolateKvStoreClient`
- Pure Dart example in `example/dart_only/`
- Export `HashedKvPath` from package entrypoint
- Stale truncate temp file cleanup before new overwrite writes

### Fixed

- Backpressure credit registration race (deadlock on large streams)
- Delete/subscribe hangs when write worker is respawning
- Write queue drain after open failure; `writeAbort` no longer fails queued writes
- Router worker-wait/retry and live subscription re-register on respawn
- Folder ensure deduplication in router

### Changed

- `KvWriteException` carries error type and stack trace when available
- Live subscriber chunks use `TransferableTypedData`
- Single SHA256 digest per `openWrite` via `HashedKvPath.pathsForKey()`
- CI runs on Ubuntu and Windows; OpenSpec validation uses `npx`
- Document append non-atomic semantics and I/O failure queue behavior

## 0.2.0

- Multi-isolate write architecture with per-key serialization and sharded workers
- Atomic truncate writes via temp file + rename
- Direct client-to-worker chunk routing with `TransferableTypedData`
- Write backpressure, configurable flush tuning, and optional best-effort `fsyncOnClose`
- `KvNotFoundException` / `KvWriteException` error types
- Delete commands queued behind active writes; worker respawn after idle purge
- Client stores `rootDirPath`; `readStream(key)` and `pathForKey(key)` convenience APIs

## 0.1.0

- Initial public package version

## 0.0.1

- Initial package scaffold
