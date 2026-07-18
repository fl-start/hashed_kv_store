## 0.4.2

### Added

- Opt-in write-path tracing via `--dart-define=KV_STORE_TRACE=true` (`KvTraceWrite`, `developer.log` under `hashed_kv_store`)
- Slow-path logs (≥5s) for router folder ensure, worker open/write-end, and client open/end acks

## 0.4.1

### Added

- CI now runs `dart test` across Linux and Windows on x64 and ARM64, macOS on Apple Silicon (ARM64), plus on-device integration tests on an Android emulator and an iOS simulator (`example/integration_test`)

### Fixed

- `writeFromStream` aborted before it subscribed (e.g. while awaiting the open ack) now drains the input stream, so a caller-owned `StreamController` can `close()` without hanging
- CI dependency install no longer fails resolving the Flutter `example/` package on the Dart-only runner; `dart pub get --no-example` scopes resolution to the root package
- CI `dart analyze` scoped to `lib`/`test` so it no longer fails on the unresolved `benchmark/` and `example/` sub-packages

## 0.4.0

### Added

- `KvAbortController`, `KvAbortSignal`, and `KvAbortException` for cooperative cancellation
- Optional `signal` parameter on `writeFromStream`, `writeFromStreamDirect`, `readBytes`, `readBytesAll`, and `readStream`
- Router `abortWrite`, `openRead`, and `cancelRead` message handling
- Worker-routed cancellable reads when `signal` is provided on a spawned client

### Changed

- Write aborts route through router `abortWrite` (replaces direct client `writeAbort` on cancellation)
- Aborted truncate writes end live subscriptions and delete partial temp files

## 0.3.3

### Fixed

- `readStream` no longer chains `exists()` + `stat()` + `readAsBytes()`; uses optimistic `openRead()` (one syscall on hit)
- `readBytes(checkExists: true)` uses optimistic read instead of `exists()` + `readAsBytes()`

### Removed

- `smallFileReadThreshold` spawn/openReadOnly option (small-file `readStream` fast path caused a regression on Windows)

## 0.3.2

### Added

- `readBytes()`, `readBytesAll()`, and `readStream(checkExists: false)` for faster small-file reads
- `openReadOnly()` — read-only client without write isolates
- `writeFromStreamDirect()` and `deleteLocal()` for caller-isolate I/O
- LRU path cache (`pathCacheMaxEntries` on spawn/openReadOnly)
- Small-file `readStream` fast path via `readAsBytes` (`smallFileReadThreshold`, default 64 KiB)
- `KvDirectIo` exported for aligned direct disk I/O

### Changed

- `openWrite` registers write backpressure credits in the same round trip (no separate `registerCredits` hop)

## 0.3.1

### Added

- Layout version metadata (`.hashed_kv_meta.json`) written on spawn
- Automatic wipe of incompatible or legacy on-disk data when layout version mismatches
- `wipeOnLayoutMismatch` spawn option (default `true`) and `KvLayoutMismatchException` when disabled

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
