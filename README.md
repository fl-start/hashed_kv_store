# hashed_kv_store

A SHA256-based key/value store with streaming IO and multi-isolate support for Dart.

## Features

- **SHA256-based storage**: Keys are hashed using SHA256 and stored under a single folder by default (`<cc>/<cccc-cccc-cccc-cccc>.<ext>`). Optional two-level nesting (`<cc>/<cc>/<...>`) is available via `folderHierarchyLevels: 2`.
- **Streaming interfaces**: Both read and write operations use streams for efficient handling of large files
- **Multi-isolate architecture**: 
  - Router isolate manages multiple write workers
  - One master folder isolate handles all directory creation (serialized)
  - Write workers are sharded by key with per-key write serialization
  - Different keys can be written concurrently to different write workers
  - Reads can be performed directly from any isolate (including main UI) without going through the router
- **Live subscriptions**: Subscribe to writes as they happen (similar to `tail -f`)
- **No polling**: Push-based notifications to subscribers

## Getting started

Add this package to your `pubspec.yaml`:

```yaml
dependencies:
  hashed_kv_store: ^0.1.0
```

## Usage

### Basic write and read

```dart
import 'dart:convert';
import 'package:hashed_kv_store/hashed_kv_store.dart';

void main() async {
  const rootDirPath = './kv_root';
  final store = await MultiIsolateKvStoreClient.spawn(
    rootDirPath: rootDirPath,
    numWriteWorkers: 4,
  );

  const key = 'user:1234:profile';
  const ext = 'json';

  // Write
  final jsonString = jsonEncode({'name': 'Alice', 'age': 30});
  final writeStream = Stream<List<int>>.fromIterable([
    utf8.encode(jsonString),
  ]);
  await store.writeFromStream(key, writeStream, extension: ext);

  // Read
  final readStream = store.readStream(rootDirPath, key, extension: ext);
  final bytes = <int>[];
  await for (final chunk in readStream) {
    bytes.addAll(chunk);
  }
  final readJson = utf8.decode(bytes);
  print('Read back: $readJson');
}
```

### Folder hierarchy (default: 1 level)

By default, files are stored one folder deep under the root (`<cc>/<file>`). To use two nested folders, pass `folderHierarchyLevels: 2` when spawning the store:

```dart
final store = await MultiIsolateKvStoreClient.spawn(
  rootDirPath: './kv_root',
  folderHierarchyLevels: 2, // override default of 1
);
```

Flutter apps can define a constant (see `example/lib/main.dart`) and pass it to `spawn` so the nesting depth is configured in one place.

### Live subscription (tail -f style)

```dart
final store = await MultiIsolateKvStoreClient.spawn(
  rootDirPath: './kv_root',
  numWriteWorkers: 4,
);

const key = 'live:log:stream';
const ext = 'log';

// Subscribe before writing
final liveStream = store.subscribeLive(key, extension: ext);
unawaited(() async {
  await for (final chunk in liveStream) {
    stdout.write(utf8.decode(chunk));
  }
  print('Stream ended (writer finished).');
}());

// Write chunks over time
final controller = StreamController<List<int>>();
unawaited(store.writeFromStream(key, controller.stream, extension: ext));

for (var i = 0; i < 5; i++) {
  final line = 'line $i at ${DateTime.now().toIso8601String()}\n';
  controller.add(utf8.encode(line));
  await Future.delayed(const Duration(seconds: 1));
}

await controller.close();
```

### Download HTTP file with Dio and pipe to KV store

```dart
import 'package:dio/dio.dart';
import 'package:hashed_kv_store/hashed_kv_store.dart';

Future<void> downloadToKvStore() async {
  final dio = Dio();
  final store = await MultiIsolateKvStoreClient.spawn(
    rootDirPath: './kv_root',
    numWriteWorkers: 4,
  );

  const url = 'https://example.com/large-file.eml';
  const key = 'mail:1234';
  const ext = 'eml';

  // Download and pipe to KV store
  final response = await dio.get<ResponseBody>(
    url,
    options: Options(
      responseType: ResponseType.stream,
      followRedirects: true,
    ),
  );

  final httpStream = response.data!.stream;
  await store.writeFromStream(key, httpStream, extension: ext);
}
```

### Download with live subscription

```dart
// Subscribe before starting download
final liveStream = store.subscribeLive(key, extension: ext);
unawaited(() async {
  int totalBytes = 0;
  await for (final chunk in liveStream) {
    totalBytes += chunk.length;
    print('Received ${chunk.length} bytes (total: $totalBytes)');
  }
}());

// Wait for subscription to register
await Future.delayed(const Duration(milliseconds: 100));

// Download and pipe
final response = await dio.get<ResponseBody>(
  url,
  options: Options(responseType: ResponseType.stream),
);
await store.writeFromStream(key, response.data!.stream, extension: ext);
```

## API Reference

### MultiIsolateKvStoreClient

#### `spawn({required String rootDirPath, int numWriteWorkers = 2, int folderHierarchyLevels = 1, Duration writeIdlePurgeDuration = const Duration(seconds: 60)})`

Spawns the router and write workers. Returns a client bound to that router.

- `rootDirPath`: Directory where all KV files are stored
- `numWriteWorkers`: Number of write worker isolates to spawn, sharded by key (default: 2)
- `folderHierarchyLevels`: Folder nesting depth - 0 (root only), 1 (one folder level), or 2 (two folder levels) (default: 1)
- `writeIdlePurgeDuration`: Write worker idle timeout before purge (default: 60 seconds)

#### `writeFromStream(String key, Stream<List<int>> data, {String extension = 'bin', bool truncateExisting = true})`

Streaming write into the KV store.

- Writes for the same (key, extension) are serialized within a write worker
- Different keys may be written concurrently across different write workers
- Folder creation is handled by master folder isolate (centralized)
- `extension`: File extension without leading dot (e.g., 'eml', 'bin')
- `truncateExisting`: If true, overwrites existing file; otherwise appends

#### `readStream(String rootDirPath, String key, {String extension = 'bin'})`

Streaming read for the value stored under [key]/[extension].

- Reads directly from disk without going through isolates
- Returns a `Stream<List<int>>` that emits file chunks
- Throws [KvNotFoundException] if key doesn't exist
- Can be called from any isolate (main UI, compute isolates, etc.)
- `rootDirPath` must be the same directory used when spawning the store

#### `pathForKey(String rootDirPath, String key, {String extension = 'bin'})`

Get the file path for a key on disk.

- Allows any isolate to read files directly using standard File I/O
- Returns the full file path as a string
- Useful for advanced use cases where you need direct file access

#### `subscribeLive(String key, {String extension = 'bin'})`

Subscribe to live writes for a key.

- Returns a `Stream<List<int>>` that emits chunks as they are written to disk
- Stream completes when the writer finishes
- No polling - pure push-based notifications

#### `delete(String key, {String extension = 'bin'})`

Delete the value for a key if it exists. Returns a [Future] that completes when the write worker has processed the delete.

## Architecture

The package uses a multi-isolate architecture:

1. **Router isolate**: Routes commands to appropriate workers
  - Routes writes to write workers (sharded by key hash)
  - Requests folder creation from master folder isolate

2. **Master folder isolate**: Handles all directory creation
  - Serializes all directory operations
  - Caches created folders to minimize syscalls
  - Ensures all folder paths exist before writes

3. **Write worker isolates**: Handle file I/O with per-key serialization
  - Sharded by key hash for parallelism across keys
  - Per-key write queue: writes to same (key, ext) are serialized
  - Notifies live subscribers as data is written
  - May self-purge after configured idle timeout

4. **Direct reads**: Reads can be performed from any isolate
  - Main UI isolate can read directly without blocking
  - Compute isolates can perform concurrent reads
  - Use `pathForKey()` to get file paths for custom I/O operations
  - Use `readStream()` for async streaming reads

5. **Storage layout**: Files are stored based on configured hierarchy level:
   - **Level 0 (flat)**: `<rootDir>/<cccc-cccc-cccc-cccc>.<extension>`
   - **Level 1 (default)**: `<rootDir>/<cc>/<cccc-cccc-cccc-cccc>.<extension>`
   - **Level 2 (nested)**: `<rootDir>/<cc>/<cc>/<cccc-cccc-cccc-cccc>.<extension>`
  
  Where each `c` is a lowercase Crockford Base32 character derived from the
  SHA256 digest. Only the first 20 Crockford Base32 characters are used:
  - first 2 chars -> optional first folder (levels 1-2)
  - next 2 chars -> optional second folder (level 2 only)
  - next 16 chars -> file stem formatted as `xxxx-xxxx-xxxx-xxxx`
  Remaining digest characters are ignored.

## Additional information

- All operations are fully streaming - no buffering of entire files in memory
- Writes for the same (key, extension) are serialized within their write worker
- Different keys can be written concurrently to different write workers
- All folder operations are centralized for simplicity and consistency
- Reads are fully decoupled from the router and can be done from any isolate
- Direct read access allows zero-copy patterns for compute-intensive workloads
- Live subscriptions work across isolates without polling
