# kv-store Specification

## Purpose

Filesystem-backed key/value storage for Dart with SHA256-derived paths, streaming I/O, multi-isolate writes, direct reads, and push-based live write subscriptions.

## Requirements

### Requirement: SHA256 Key Path Mapping

The system SHALL map each string key to a deterministic filesystem path by hashing the key with SHA256, encoding the digest as lowercase Crockford Base32, and using the first 20 Base32 characters for folder and file naming.

#### Scenario: Deterministic path for a key

- **WHEN** the same key and extension are mapped twice with the same hierarchy level
- **THEN** the resulting file path is identical

#### Scenario: Extension separates values

- **WHEN** the same key is stored with different extensions
- **THEN** each extension maps to a separate file path

### Requirement: Configurable Folder Hierarchy

The system SHALL support `folderHierarchyLevels` of 0, 1, or 2 with these layouts:

- Level 0: `<root>/<xxxx-xxxx-xxxx-xxxx>.<ext>`
- Level 1: `<root>/<cc>/<xxxx-xxxx-xxxx-xxxx>.<ext>`
- Level 2: `<root>/<cc>/<cc>/<xxxx-xxxx-xxxx-xxxx>.<ext>`

#### Scenario: Level 1 default layout

- **WHEN** a value is written with `folderHierarchyLevels` set to 1
- **THEN** the file is stored under a single two-character subdirectory of the root

### Requirement: Streaming Write

The system SHALL accept writes as a `Stream<List<int>>` without buffering the entire payload in memory.

#### Scenario: Large payload write

- **WHEN** a caller streams chunks for a key
- **THEN** the stored file contains the concatenation of all chunks in order

### Requirement: Atomic Truncate Writes

The system SHALL publish truncate writes atomically by writing to a temp file and renaming over the final path on completion.

#### Scenario: Old content visible during truncate write

- **WHEN** a truncate write is in progress for an existing key
- **THEN** readers observe the previous file content until the write completes

#### Scenario: New content visible after truncate write

- **WHEN** a truncate write completes successfully
- **THEN** readers observe only the newly written content

### Requirement: Write Error Propagation

The system SHALL propagate folder, worker, and input-stream failures to the `writeFromStream` caller as [KvWriteException].

#### Scenario: Write failure surfaces to caller

- **WHEN** a write cannot be completed due to an I/O or protocol error
- **THEN** the caller's `writeFromStream` future completes with an error

### Requirement: Per-Key Write Serialization

The system SHALL serialize writes for the same `(key, extension)` pair within a write worker.

#### Scenario: Concurrent writes to same key

- **WHEN** two writes for the same key and extension overlap in time
- **THEN** the second write does not begin until the first write completes

### Requirement: Concurrent Writes Across Keys

The system SHALL allow different keys to be written concurrently when routed to different write workers.

#### Scenario: Independent keys complete concurrently

- **WHEN** writes to two different keys are in progress
- **THEN** one key's write may complete before the other without waiting for unrelated keys

### Requirement: Centralized Folder Creation

The system SHALL serialize all directory creation through a single master folder isolate.

#### Scenario: Folder exists before write

- **WHEN** a write is opened for a key
- **THEN** the required parent directories exist before file I/O begins

### Requirement: Direct Read Access

The system SHALL support reads directly from disk without routing through write isolates.

#### Scenario: Read existing value

- **WHEN** a caller reads a key that exists on disk
- **THEN** a stream of file chunks is returned

#### Scenario: Read missing value

- **WHEN** a caller reads a key that does not exist
- **THEN** a `KvNotFoundException` is thrown

### Requirement: Live Write Subscription

The system SHALL provide push-based live subscriptions that emit write chunks as they are persisted, without polling.

#### Scenario: Subscriber receives live chunks

- **WHEN** a subscriber registers before or during an active write
- **THEN** the subscriber receives chunks as they are written and the stream completes when the write ends

### Requirement: Delete With Acknowledgement

The system SHALL delete a key's file if present and complete the caller's `delete` future after the write worker processes the command. Deletes for a key SHALL be queued behind any active or pending writes for that key.

#### Scenario: Delete existing key

- **WHEN** a caller deletes an existing key
- **THEN** subsequent reads throw `KvNotFoundException`

#### Scenario: Delete during active write

- **WHEN** a delete is requested while a write is active for the same key
- **THEN** the delete runs after the active write completes

### Requirement: Optional Best-Effort Fsync On Close

The system SHALL support an optional `fsyncOnClose` spawn parameter (default false) that performs a best-effort file flush before acknowledging completed writes.

#### Scenario: Fsync disabled by default

- **WHEN** a write completes with default spawn settings
- **THEN** the write is acknowledged after sink flush and close without an additional file flush

### Requirement: Write Backpressure

The system SHALL limit in-flight write chunks per session using a configurable credit window (`writeMaxInFlightChunks`, default 8).

#### Scenario: Producer backpressure

- **WHEN** the client has sent the maximum allowed in-flight chunks for a write
- **THEN** it waits for worker credit before sending additional chunks

#### Scenario: Credit registration before chunks

- **WHEN** a write session registers its credit port
- **THEN** the worker acknowledges registration before the client sends write chunks

### Requirement: Client Lifecycle

The system SHALL provide `close()` on [MultiIsolateKvStoreClient] to shut down router, worker, and folder isolates. After close, write, delete, and subscribe operations SHALL fail fast.

#### Scenario: Close shuts down isolates

- **WHEN** a caller invokes `close()` on a store client
- **THEN** subsequent `writeFromStream` and `delete` calls throw `StateError`

### Requirement: Worker Respawn Resilience

The system SHALL wait for write workers to become available during respawn and SHALL re-register live subscriptions after a worker respawns.

#### Scenario: Delete after worker respawn

- **WHEN** a delete is requested after a worker has respawned from idle purge
- **THEN** the delete future completes successfully

#### Scenario: Live subscription after worker respawn

- **WHEN** a live subscription is active across a worker respawn
- **THEN** the subscriber continues to receive chunks from subsequent writes

### Requirement: Write Abort Queue Semantics

When an active write is aborted, the system SHALL clean up the aborted write and SHALL allow queued writes for the same key to proceed.

#### Scenario: Queued write after abort

- **WHEN** a second write is queued behind an active write and the active write is aborted
- **THEN** the queued write completes successfully

### Requirement: Append Write Visibility

Append writes (`truncateExisting: false`) SHALL write directly to the target file and are not atomic. Readers MAY observe partial appended content during an in-progress append.

#### Scenario: Append is non-atomic

- **WHEN** a caller appends to an existing key
- **THEN** the write does not use temp-file rename semantics

### Requirement: Write Channel I/O Failure Semantics

When an active write fails due to chunk or commit I/O for a `(key, extension)` channel, the system SHALL fail queued writes and deletes for that same channel to avoid ambiguous partial state.

#### Scenario: Queued operations fail with active write I/O error

- **WHEN** an active write fails during chunk or commit I/O and other writes are queued for the same key and extension
- **THEN** the queued write futures complete with errors

### Requirement: Open Failure Queue Drain

When a queued write fails to open, the system SHALL attempt to start the next queued write for the same `(key, extension)` channel.

#### Scenario: Queued write after open failure

- **WHEN** a queued write fails during open and another write remains queued for the same key and extension
- **THEN** the next queued write is started

### Requirement: Stale Temp File Cleanup

Before starting a truncate write, the system SHALL delete stale `.<writeId>.tmp` files that belong to the target file path.

#### Scenario: Orphan temp removed before truncate

- **WHEN** a truncate write opens for a key with orphan temp siblings present
- **THEN** those stale temp files are removed before the new temp file is created

### Requirement: Storage Introspection

The system SHALL provide `exists(key)` and `listStoredPaths()` on [MultiIsolateKvStoreClient]. `listStoredPaths` SHALL return hashed storage paths relative to the root and SHALL NOT recover original string keys.

#### Scenario: Exists check

- **WHEN** a caller checks `exists` for a stored key
- **THEN** the result is true

#### Scenario: List stored paths

- **WHEN** a caller invokes `listStoredPaths` after writing values
- **THEN** hashed relative file paths are returned without temp files

### Requirement: Layout Version Metadata

The system SHALL write `.hashed_kv_meta.json` under the storage root containing a `layoutVersion` integer that matches the package's current on-disk layout version.

#### Scenario: Fresh root writes metadata

- **WHEN** `MultiIsolateKvStoreClient.spawn` is called on an empty root directory
- **THEN** layout metadata is written without deleting the root directory

### Requirement: Layout Mismatch Handling

When stored layout metadata is missing and the root contains files, or when stored `layoutVersion` does not match the package layout version, the system SHALL wipe all contents under the root (except the root directory itself) before writing fresh metadata, unless `wipeOnLayoutMismatch` is false.

#### Scenario: Legacy data without metadata

- **WHEN** spawn is called on a root with files but no layout metadata
- **THEN** existing files are removed and fresh metadata is written

#### Scenario: Opt out of automatic wipe

- **WHEN** spawn is called with `wipeOnLayoutMismatch: false` and layout versions mismatch
- **THEN** spawn throws `KvLayoutMismatchException` without modifying stored files

### Requirement: Multi-Isolate Write Architecture

The system SHALL use a router isolate, a master folder isolate, and a configurable pool of write worker isolates sharded by key.

#### Scenario: Spawn store

- **WHEN** `MultiIsolateKvStoreClient.spawn` is called with `numWriteWorkers`
- **THEN** the router and write worker pool are available for write, subscribe, and delete commands
