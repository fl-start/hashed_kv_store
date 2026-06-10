# Project Context

## Purpose
A SHA256-based key/value store package for Dart with streaming IO and multi-isolate support. The package provides efficient storage and retrieval of large files using streaming interfaces, with concurrent read operations and serialized writes per key. Designed for use cases such as downloading large files, log streaming, and any scenario requiring efficient key-value storage with live subscription capabilities.

## Tech Stack
- **Dart**: SDK >=3.0.0 <4.0.0
- **Dart Isolates**: For concurrent processing and multi-isolate architecture
- **crypto**: ^3.0.0 - For SHA256 hashing of keys
- **path**: ^1.8.0 - For cross-platform path manipulation
- **test**: ^1.24.0 - For unit and integration testing
- **flutter_lints**: ^5.0.0 - For code style and linting

## Project Conventions

### Code Style
- Follows Dart style guide and uses `flutter_lints` package for linting rules
- Private classes and members prefixed with underscore (e.g., `_WriteContext`)
- Shared path logic lives in `HashedKvPath` (`lib/hashed_kv_path.dart`)
- Use `typedef` for command types (e.g., `typedef _Cmd = Map<String, dynamic>`)
- Prefer descriptive names that clearly indicate purpose
- Use documentation comments (`///`) for public APIs
- File organization: one main class per file, with related helper classes in the same file when tightly coupled

### Architecture Patterns
- **Multi-Isolate Architecture**: 
  - Router isolate manages write worker pool
  - One master folder isolate handles all directory creation (serialized globally)
  - Multiple write worker isolates for parallelism (sharded by key hash)
  - Write workers maintain per-key write queues for serialization within each worker
  - Reads are fully decoupled and can be performed from any isolate
  - Communication via `SendPort`/`ReceivePort` message passing
- **Per-Key Write Serialization**: Writes for the same (key, extension) pair are serialized within a write worker
- **Folder Centralization**: All directory operations go through a single master folder isolate
- **Concurrent Writes**: Different keys can be written concurrently to different write workers
- **Direct Reads**: Reads bypass the router and can be done from any isolate (main, UI, compute)
- **Streaming-First Design**: All read/write operations use `Stream<List<int>>` for efficient handling of large files without buffering entire files in memory
- **Push-Based Notifications**: Live subscriptions use push notifications (no polling)
- **Storage Layout**: Files stored based on configured hierarchy level (0, 1, or 2):
  - Level 0 (flat): `<rootDir>/<cccc-cccc-cccc-cccc>.<extension>`
  - Level 1 (default): `<rootDir>/<cc>/<cccc-cccc-cccc-cccc>.<extension>`
  - Level 2 (nested): `<rootDir>/<cc>/<cc>/<cccc-cccc-cccc-cccc>.<extension>`
  Where each `c` is a lowercase Crockford Base32 character from SHA256 (only first 20 chars used); remaining ignored

### Testing Strategy
- Use `test` package for all tests
- Tests should cover:
  - Basic read/write operations
  - Error handling (e.g., non-existent keys)
  - Live subscription functionality
  - Write queuing behavior
  - Extension handling (same key, different extensions)
  - Deletion operations
- Use temporary directories for test isolation
- Include cleanup in `tearDown` to remove test artifacts
- Test with realistic delays to verify async behavior

### Git Workflow
- Follow conventional commit messages
- Main branch: `main`
- Create feature branches for new work
- Ensure tests pass before committing

## Domain Context
- **Key-Value Store**: Keys are arbitrary strings, values are binary data stored as files
- **SHA256 Hashing**: All keys are hashed using SHA256 to generate deterministic file paths
- **Extension-Based Separation**: The same key can have different values stored with different extensions (e.g., `key.json` vs `key.bin`)
- **Streaming Operations**: Designed for large files that shouldn't be fully loaded into memory
- **Live Subscriptions**: Similar to `tail -f`, allows real-time monitoring of writes as they happen
- **Concurrent Reads**: Multiple reads can happen simultaneously from any isolate without blocking writes
- **Per-Key Write Serialization**: Writes for the same key within a write worker are serialized; different keys can write concurrently
- **Concurrent Writes Across Keys**: Different keys can be written concurrently via separate write workers
- **Centralized Folder Management**: All directory creation goes through a single master folder isolate
- **Configurable Folder Hierarchy**: Storage structure supports 0, 1, or 2 folder nesting levels (default: 1)
- **Direct Read Access**: Reads bypass the router entirely and can be performed from main thread, UI isolates, or compute isolates
- **Write Worker Sharding**: Keys are sharded across write workers using a stable hash function for load distribution

## Important Constraints
- **No Polling**: Live subscriptions must be push-based, not polling-based
- **Memory Efficiency**: Operations must be fully streaming - no buffering entire files in memory
- **Per-Key Write Serialization**: Writes for the same (key, extension) pair must be serialized within their write worker
- **Centralized Folder Operations**: All directory creation must be serialized through master folder isolate
- **Direct Read API**: Reads must support direct disk access without going through the router
- **Cross-Platform**: Must work on all Dart-supported platforms (uses `dart:io` for filesystem operations)
- **Isolate Communication**: All communication between isolates must use message passing (no shared memory)

## External Dependencies
- **crypto package**: Provides SHA256 hashing functionality
- **path package**: Provides cross-platform path manipulation utilities
- **Dart SDK**: Core language and runtime, including `dart:isolate` for concurrency and `dart:io` for filesystem operations
