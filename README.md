# hashed_kv_store

A SHA256-based key/value store with streaming IO and multi-isolate support for Dart.

## Features

- **SHA256-based storage**: Keys are hashed using SHA256 and stored in a two-level folder structure (`<hh>/<hh>/<digest>.<ext>`)
- **Streaming interfaces**: Both read and write operations use streams for efficient handling of large files
- **Multi-isolate architecture**: 
  - Router isolate manages a pool of worker isolates
  - Concurrent reads and writes across multiple isolates
  - Per-key write queuing (writes for the same key are serialized)
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
  final store = await MultiIsolateKvStoreClient.spawn(
    rootDirPath: './kv_root',
    numWorkers: 4,
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
  final readStream = store.readStream(key, extension: ext);
  final bytes = <int>[];
  await for (final chunk in readStream) {
    bytes.addAll(chunk);
  }
  final readJson = utf8.decode(bytes);
  print('Read back: $readJson');
}
```

### Live subscription (tail -f style)

```dart
final store = await MultiIsolateKvStoreClient.spawn(
  rootDirPath: './kv_root',
  numWorkers: 4,
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
    numWorkers: 4,
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

#### `spawn({required String rootDirPath, int numWorkers = 4})`

Spawns the router and worker pool. Returns a client bound to that router.

- `rootDirPath`: Directory where all KV files are stored
- `numWorkers`: Number of worker isolates to spawn (default: 4)

#### `writeFromStream(String key, Stream<List<int>> data, {String extension = 'bin', bool truncateExisting = true})`

Streaming write into the KV store.

- Writes for the same (key, extension) are enqueued: only one active write at a time
- `extension`: File extension without leading dot (e.g., 'eml', 'bin')
- `truncateExisting`: If true, overwrites existing file; otherwise appends

#### `readStream(String key, {String extension = 'bin'})`

Streaming read from the KV store.

- Returns a `Stream<List<int>>` that emits file chunks
- Throws `StateError` with message 'not_found' if key doesn't exist
- Can be routed to any worker isolate

#### `subscribeLive(String key, {String extension = 'bin'})`

Subscribe to live writes for a key.

- Returns a `Stream<List<int>>` that emits chunks as they are written to disk
- Stream completes when the writer finishes
- No polling - pure push-based notifications

#### `delete(String key, {String extension = 'bin'})`

Delete the value for a key if it exists.

## Architecture

The package uses a multi-isolate architecture:

1. **Router isolate**: Manages worker pool and routes commands
   - Maintains per-key write queues
   - Ensures writes for the same key are serialized
   - Routes reads to any available worker

2. **Worker isolates**: Handle actual filesystem IO
   - Each worker owns a `_HashedKvCore` for path mapping
   - Writes chunks to disk and notifies live subscribers
   - Handles streaming reads

3. **Storage layout**: Files are stored as:
   ```
   <rootDir>/<hh>/<hh>/<sha256-digest>.<extension>
   ```
   Where `hh` are the first 4 hex characters of the SHA256 digest.

## Additional information

- All operations are fully streaming - no buffering of entire files in memory
- Writes for the same key are automatically queued and processed sequentially
- Reads can be served by any worker isolate for better load distribution
- Live subscriptions work across isolates without polling
