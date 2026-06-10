import 'dart:async';
import 'dart:io';
import 'dart:isolate';
import 'dart:typed_data';

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

/// Thrown when a write operation fails in a worker or folder isolate.
class KvWriteException implements Exception {
  final String message;

  KvWriteException(this.message);

  @override
  String toString() => 'KvWriteException: $message';
}

void _throwIfError(Map<dynamic, dynamic> response) {
  final error = response['error'];
  if (error != null) {
    throw KvWriteException(error.toString());
  }
}

/// Public client API for the multi-isolate KV store.
///
/// Behind the scenes:
/// - Spawns one router isolate.
/// - Router spawns a pool of write worker isolates, sharded by key.
/// - A master folder isolate serializes all directory creation.
/// - Writes for the same (key, extension) are serialized within a write worker.
/// - Truncate writes use a temp file and atomic rename on completion.
/// - Write chunks are sent directly to the responsible worker isolate.
/// - Reads bypass isolates and read directly from the shared filesystem.
class MultiIsolateKvStoreClient {
  final SendPort _routerPort;
  final int _folderHierarchyLevels;

  MultiIsolateKvStoreClient._(this._routerPort, this._folderHierarchyLevels);

  /// Spawns the router and write worker pool. Returns a client bound to that router.
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
  /// Truncate writes are atomically published via temp file + rename.
  /// Chunk data is transferred with [TransferableTypedData] to reduce copies.
  Future<void> writeFromStream(
    String key,
    Stream<List<int>> data, {
    String extension = 'bin',
    bool truncateExisting = true,
  }) async {
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
    _throwIfError(ack);

    final writeId = ack['writeId'] as int;
    final workerPort = ack['workerPort'] as SendPort;

    try {
      await for (final chunk in data) {
        final transferable = TransferableTypedData.fromList([
          Uint8List.fromList(chunk),
        ]);
        workerPort.send(<String, dynamic>{
          'type': 'writeChunk',
          'writeId': writeId,
          'chunk': transferable,
        });
      }
    } catch (e) {
      workerPort.send(<String, dynamic>{
        'type': 'writeAbort',
        'writeId': writeId,
      });
      rethrow;
    }

    final endReply = ReceivePort();
    workerPort.send(<String, dynamic>{
      'type': 'writeEnd',
      'writeId': writeId,
      'replyPort': endReply.sendPort,
    });
    final endAck = await endReply.first as Map;
    endReply.close();
    _throwIfError(endAck);
  }

  /// Subscribe to live writes for [key]/[extension].
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
        recv.close();
        controller.close();
      } else {
        controller.add((message as List).cast<int>());
      }
    });

    return controller.stream;
  }

  String pathForKey(String rootDirPath, String key, {String extension = 'bin'}) {
    return HashedKvPath.pathForKey(
      rootDirPath,
      key,
      extension,
      _folderHierarchyLevels,
    );
  }

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

  /// Waits until the write worker has processed the delete command.
  ///
  /// Deletes are queued behind any active or pending writes for the same key.
  Future<void> delete(String key, {String extension = 'bin'}) async {
    final replyPort = ReceivePort();
    _routerPort.send(<String, dynamic>{
      'type': 'delete',
      'key': key,
      'ext': extension,
      'replyPort': replyPort.sendPort,
    });
    final ack = await replyPort.first as Map;
    replyPort.close();
    _throwIfError(ack);
  }
}
