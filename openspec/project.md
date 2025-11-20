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
- Private classes and members prefixed with underscore (e.g., `_HashedKvCore`, `_WriteContext`)
- Use `typedef` for command types (e.g., `typedef _Cmd = Map<String, dynamic>`)
- Prefer descriptive names that clearly indicate purpose
- Use documentation comments (`///`) for public APIs
- File organization: one main class per file, with related helper classes in the same file when tightly coupled

### Architecture Patterns
- **Multi-Isolate Architecture**: 
  - Router isolate manages worker pool and routes commands
  - Worker isolates handle actual filesystem IO operations
  - Communication via `SendPort`/`ReceivePort` message passing
- **Per-Key Write Queuing**: Writes for the same (key, extension) pair are serialized to prevent race conditions
- **Streaming-First Design**: All read/write operations use `Stream<List<int>>` for efficient handling of large files without buffering entire files in memory
- **Push-Based Notifications**: Live subscriptions use push notifications (no polling)
- **Storage Layout**: Files stored in two-level directory structure: `<rootDir>/<hh>/<hh>/<sha256-digest>.<extension>` where `hh` are the first 4 hex characters of the SHA256 digest

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
- **Concurrent Reads**: Multiple reads can happen simultaneously across different worker isolates
- **Serialized Writes**: Only one write per (key, extension) pair can be active at a time; subsequent writes are queued
- **Worker Sharding**: Keys are sharded across worker isolates using a simple hash function for load distribution

## Important Constraints
- **No Polling**: Live subscriptions must be push-based, not polling-based
- **Memory Efficiency**: Operations must be fully streaming - no buffering entire files in memory
- **Write Serialization**: Writes for the same key must be queued and processed sequentially
- **Cross-Platform**: Must work on all Dart-supported platforms (uses `dart:io` for filesystem operations)
- **Isolate Communication**: All communication between isolates must use message passing (no shared memory)

## External Dependencies
- **crypto package**: Provides SHA256 hashing functionality
- **path package**: Provides cross-platform path manipulation utilities
- **Dart SDK**: Core language and runtime, including `dart:isolate` for concurrency and `dart:io` for filesystem operations
