## ADDED Requirements

### Requirement: Cooperative Cancellation API

The system SHALL provide `KvAbortController`, `KvAbortSignal`, and `KvAbortException`. `KvAbortController.abort()` SHALL be idempotent. Read and write APIs SHALL accept an optional `KvAbortSignal? signal` without changing behavior when `signal` is null.

#### Scenario: Abort before write starts

- **WHEN** `signal` is already aborted before `writeFromStream` begins
- **THEN** the future completes with `KvAbortException` and no file is created

#### Scenario: Abort during truncate write

- **WHEN** an in-flight truncate write is aborted via `signal`
- **THEN** upstream stream consumption stops, partial temp files are removed, and prior committed content remains visible

#### Scenario: Abort releases same-key queue

- **WHEN** an active write is aborted and another write is queued for the same `(key, extension)`
- **THEN** the queued write proceeds after cleanup

#### Scenario: Routed read cancellation

- **WHEN** a read is performed with a non-null `signal` on a spawned client and `signal` is aborted during streaming
- **THEN** worker-side file streaming stops and the caller receives `KvAbortException`

## MODIFIED Requirements

### Requirement: Write Abort Queue Semantics

When an active write is aborted (via stream error or `KvAbortSignal`), the system SHALL clean up the aborted write, end live subscriptions for that write, and SHALL allow queued writes for the same key to proceed. Aborted queued `openWrite` requests SHALL receive an abort response without blocking the channel.

#### Scenario: Queued write after abort

- **WHEN** a second write is queued behind an active write and the active write is aborted
- **THEN** the queued write completes successfully

#### Scenario: Queued openWrite aborted before start

- **WHEN** a write is queued waiting for `openWrite` and its `signal` is aborted
- **THEN** the queued request is removed and completes with `KvAbortException`
