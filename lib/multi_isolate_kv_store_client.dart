import 'dart:async';
import 'dart:io';
import 'dart:isolate';

import 'hashed_kv_path.dart';
import 'kv_router_isolate.dart';

/// Thrown when a value for a given key does not exist on disk.
class KvNotFoundException implements Exception {
  final String key;
  final String extension;

  KvNotFoundException(this.key, this.extension);

  @override
  String toString() =>
      'KvNotFoundException: No value found for key="$key" (.$extension)';
}

/// Public client API for the multi-isolate KV store.
///
/// Behind the scenes:
/// - Spawns one router isolate.
/// - Router spawns a pool of write worker isolates, sharded by key.
/// - A master folder isolate serializes all directory creation.
/// - Writes for the same (key, extension) are serialized within a write worker.
/// - Different keys may be written concurrently across different write workers.
/// - Reads bypass isolates and read directly from the shared filesystem.
class MultiIsolateKvStoreClient {
  final SendPort _routerPort;
  final int _folderHierarchyLevels;

  MultiIsolateKvStoreClient._(this._routerPort, this._folderHierarchyLevels);

  /// Spawns the router and write worker pool. Returns a client bound to that router.
  ///
  /// [rootDirPath] is the directory where all KV files live.
  /// [numWriteWorkers] controls how many write worker isolates to spawn, sharded by key (default: 2).
  /// [folderHierarchyLevels] controls the folder nesting depth (0, 1, or 2; default: 1).
  ///   0: files stored directly in root
  ///   1: files stored in one folder level (2 chars)
  ///   2: files stored in two folder levels (2 chars each)
  /// [writeIdlePurgeDuration] controls when idle write workers may be purged (default: 60 seconds).
  static Future<MultiIsolateKvStoreClient> spawn({
    required String rootDirPath,
    int numWriteWorkers = 2,
    int folderHierarchyLevels = 1,
    Duration writeIdlePurgeDuration = const Duration(seconds: 60),
  }) async {
    final init = ReceivePort();
    await Isolate.spawn(
      kvRouterIsolateEntry,
      [
        rootDirPath,
        init.sendPort,
        writeIdlePurgeDuration.inMilliseconds,
        numWriteWorkers,
        folderHierarchyLevels,
      ],
    );
    final routerPort = await init.first as SendPort;
    return MultiIsolateKvStoreClient._(routerPort, folderHierarchyLevels);
  }

  /// Streaming write into the KV store for [key] with [extension].
  ///
  /// Semantics:
  /// - Writes for the same (key, extension) are serialized within a write worker.
  /// - Different keys may be written concurrently across different write workers.
  /// - Folder creation is centralized through a master folder isolate.
  /// - This method:
  ///   1) sends an 'openWrite' request and waits for ACK containing a [writeId].
  ///   2) streams chunks via 'writeChunk' messages.
  ///   3) finishes with a 'writeEnd' message and waits for durability ack.
  ///
  /// [data] is a stream of raw byte chunks.
  /// [extension] should be provided without a leading dot, e.g. 'eml', 'bin'.
  /// If [truncateExisting] is true, the file is overwritten; otherwise appended.
  Future<void> writeFromStream(
    String key,
    Stream<List<int>> data, {
    String extension = 'bin',
    bool truncateExisting = true,
  }) async {
    // Step 1: request a write slot for this key.
    final replyPort = ReceivePort();
    _routerPort.send(<String, dynamic>{
      'type': 'openWrite',
      'key': key,
      'ext': extension,
      'truncate': truncateExisting,
      'replyPort': replyPort.sendPort,
    });

    final ack = await replyPort.first as Map;
    replyPort.close();

    final writeId = ack['writeId'] as int;

    // Step 2: send chunks.
    await for (final chunk in data) {
      _routerPort.send(<String, dynamic>{
        'type': 'writeChunk',
        'key': key,
        'ext': extension,
        'writeId': writeId,
        'chunk': chunk,
      });
    }

    // Step 3: signal end of write.
    final endReply = ReceivePort();
    _routerPort.send(<String, dynamic>{
      'type': 'writeEnd',
      'key': key,
      'ext': extension,
      'writeId': writeId,
      'replyPort': endReply.sendPort,
    });
    await endReply.first;
    endReply.close();
  }

  /// Subscribe to live writes for [key]/[extension].
  ///
  /// - Router forwards this to the worker responsible for that key.
  /// - The returned [Stream] emits chunks as they are written to disk.
  /// - When the writer finishes, the stream completes (null sentinel from worker).
  Stream<List<int>> subscribeLive(
    String key, {
    String extension = 'bin',
  }) {
    final recv = ReceivePort();
    _routerPort.send(<String, dynamic>{
      'type': 'subscribeLive',
      'key': key,
      'ext': extension,
      'subscriberPort': recv.sendPort,
    });

    final controller = StreamController<List<int>>();

    recv.listen((message) {
      if (message == null) {
        // Writer signaled completion.
        recv.close();
        controller.close();
      } else {
        controller.add((message as List).cast<int>());
      }
    });

    return controller.stream;
  }

  /// Get the file path for a key.
  ///
  /// This allows any isolate to read the file directly without going through
  /// the router. The caller is responsible for ensuring the file exists.
  ///
  /// [rootDirPath] must be the same directory used when spawning the store.
  String pathForKey(String rootDirPath, String key, {String extension = 'bin'}) {
    return HashedKvPath.pathForKey(
      rootDirPath,
      key,
      extension,
      _folderHierarchyLevels,
    );
  }

  /// Streaming read for the value stored under [key]/[extension].
  ///
  /// Reads directly from disk without going through worker isolates.
  /// The returned stream emits file chunks and then completes.
  /// Throws [KvNotFoundException] if the key does not exist.
  ///
  /// [rootDirPath] must be the same directory used when spawning the store.
  Stream<List<int>> readStream(
    String rootDirPath,
    String key, {
    String extension = 'bin',
  }) async* {
    final filePath = pathForKey(rootDirPath, key, extension: extension);
    final file = File(filePath);

    if (!await file.exists()) {
      throw KvNotFoundException(key, extension);
    }

    final stream = file.openRead();
    await for (final chunk in stream) {
      yield chunk;
    }
  }

  /// Delete the value for [key]/[extension] if it exists.
  ///
  /// Waits until the write worker has processed the delete command.
  Future<void> delete(String key, {String extension = 'bin'}) async {
    final replyPort = ReceivePort();
    _routerPort.send(<String, dynamic>{
      'type': 'delete',
      'key': key,
      'ext': extension,
      'replyPort': replyPort.sendPort,
    });
    await replyPort.first;
    replyPort.close();
  }
}
