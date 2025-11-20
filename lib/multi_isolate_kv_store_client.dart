import 'dart:async';
import 'dart:isolate';

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
/// - Router spawns a pool of worker isolates.
/// - Writes for the same (key, extension) are serialized:
///     they are enqueued and processed one after another.
/// - Reads can be handled by any worker (filesystem is shared).
class MultiIsolateKvStoreClient {
  final SendPort _routerPort;

  MultiIsolateKvStoreClient._(this._routerPort);

  /// Spawns the router + worker pool and returns a client
  /// bound to that router.
  ///
  /// [rootDirPath] is the directory where all KV files live.
  /// [numWorkers] controls how many worker isolates to spawn.
  static Future<MultiIsolateKvStoreClient> spawn({
    required String rootDirPath,
    int numWorkers = 4,
  }) async {
    final init = ReceivePort();
    await Isolate.spawn(
      kvRouterIsolateEntry,
      [rootDirPath, numWorkers, init.sendPort],
    );
    final routerPort = await init.first as SendPort;
    return MultiIsolateKvStoreClient._(routerPort);
  }

  /// Streaming write into the KV store for [key] with [extension].
  ///
  /// Semantics:
  /// - Writes for the same (key, extension) are enqueued:
  ///   only one active write at a time; subsequent writes wait.
  /// - This method:
  ///   1) sends an 'openWrite' request and waits for ACK containing a [writeId].
  ///   2) streams chunks via 'writeChunk' messages.
  ///   3) finishes with a 'writeEnd' message.
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
    // workerIndex is also returned but not needed on the client side;
    // router uses it internally for sharding.

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
    _routerPort.send(<String, dynamic>{
      'type': 'writeEnd',
      'key': key,
      'ext': extension,
      'writeId': writeId,
    });
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

  /// Streaming read for the value stored under [key]/[extension].
  ///
  /// - Router may route the read to *any* worker, since all
  ///   see the same filesystem.
  /// - The returned stream emits file chunks and then completes.
  /// - If the key does not exist, the stream will emit a [StateError]
  ///   with message 'not_found' and then close.
  Stream<List<int>> readStream(
    String key, {
    String extension = 'bin',
  }) {
    final recv = ReceivePort();
    _routerPort.send(<String, dynamic>{
      'type': 'readStream',
      'key': key,
      'ext': extension,
      'replyPort': recv.sendPort,
    });

    final controller = StreamController<List<int>>();

    recv.listen((message) {
      if (message == null) {
        // End-of-stream sentinel from worker.
        recv.close();
        controller.close();
      } else if (message is Map && message['error'] == 'not_found') {
        controller.addError(StateError('not_found'));
      } else {
        controller.add((message as List).cast<int>());
      }
    });

    return controller.stream;
  }

  /// Delete the value for [key]/[extension] if it exists.
  ///
  /// This is a fire-and-forget command; you can extend it
  /// to return a result if needed.
  Future<void> delete(String key, {String extension = 'bin'}) async {
    _routerPort.send(<String, dynamic>{
      'type': 'delete',
      'key': key,
      'ext': extension,
    });
  }
}

