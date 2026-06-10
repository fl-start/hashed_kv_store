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

### Requirement: Optional Fsync On Close

The system SHALL support an optional `fsyncOnClose` spawn parameter (default false) that fsyncs file data before acknowledging completed writes.

#### Scenario: Fsync disabled by default

- **WHEN** a write completes with default spawn settings
- **THEN** the write is acknowledged after flush and close without fsync

### Requirement: Write Backpressure

The system SHALL limit in-flight write chunks per session using a configurable credit window (`writeMaxInFlightChunks`, default 8).

#### Scenario: Producer backpressure

- **WHEN** the client has sent the maximum allowed in-flight chunks for a write
- **THEN** it waits for worker credit before sending additional chunks

### Requirement: Multi-Isolate Write Architecture

The system SHALL use a router isolate, a master folder isolate, and a configurable pool of write worker isolates sharded by key.

#### Scenario: Spawn store

- **WHEN** `MultiIsolateKvStoreClient.spawn` is called with `numWriteWorkers`
- **THEN** the router and write worker pool are available for write, subscribe, and delete commands
