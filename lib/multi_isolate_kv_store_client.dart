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
class MultiIsolateKvStoreClient {
  final SendPort _routerPort;
  final String _rootDirPath;
  final int _folderHierarchyLevels;
  final int _writeMaxInFlightChunks;

  MultiIsolateKvStoreClient._(
    this._routerPort,
    this._rootDirPath,
    this._folderHierarchyLevels,
    this._writeMaxInFlightChunks,
  );

  /// Root directory where KV files are stored (from [spawn]).
  String get rootDirPath => _rootDirPath;

  /// Spawns the router and write worker pool. Returns a client bound to that router.
  static Future<MultiIsolateKvStoreClient> spawn({
    required String rootDirPath,
    int numWriteWorkers = 2,
    int folderHierarchyLevels = 1,
    Duration writeIdlePurgeDuration = const Duration(seconds: 60),
    bool fsyncOnClose = false,
    int flushThresholdBytes = 64 * 1024,
    Duration flushInterval = const Duration(milliseconds: 100),
    int writeMaxInFlightChunks = 8,
  }) async {
    if (numWriteWorkers <= 0) {
      throw ArgumentError.value(
        numWriteWorkers,
        'numWriteWorkers',
        'must be greater than zero',
      );
    }
    if (folderHierarchyLevels < 0 || folderHierarchyLevels > 2) {
      throw ArgumentError.value(
        folderHierarchyLevels,
        'folderHierarchyLevels',
        'must be 0, 1, or 2',
      );
    }
    if (flushThresholdBytes <= 0) {
      throw ArgumentError.value(
        flushThresholdBytes,
        'flushThresholdBytes',
        'must be greater than zero',
      );
    }
    if (flushInterval.isNegative) {
      throw ArgumentError.value(
        flushInterval,
        'flushInterval',
        'must not be negative',
      );
    }
    if (writeMaxInFlightChunks < 0) {
      throw ArgumentError.value(
        writeMaxInFlightChunks,
        'writeMaxInFlightChunks',
        'must not be negative',
      );
    }

    final init = ReceivePort();
    await Isolate.spawn(
      kvRouterIsolateEntry,
      [
        rootDirPath,
        init.sendPort,
        writeIdlePurgeDuration.inMilliseconds,
        numWriteWorkers,
        folderHierarchyLevels,
        fsyncOnClose,
        flushThresholdBytes,
        flushInterval.inMilliseconds,
      ],
    );
    final routerPort = await init.first as SendPort;
    return MultiIsolateKvStoreClient._(
      routerPort,
      rootDirPath,
      folderHierarchyLevels,
      writeMaxInFlightChunks,
    );
  }

  /// Streaming write into the KV store for [key] with [extension].
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

    ReceivePort? creditPort;
    StreamIterator<dynamic>? creditIterator;
    var credits = _writeMaxInFlightChunks;
    if (_writeMaxInFlightChunks > 0) {
      creditPort = ReceivePort();
      creditIterator = StreamIterator<dynamic>(creditPort);
      workerPort.send(<String, dynamic>{
        'type': 'registerCredits',
        'writeId': writeId,
        'creditPort': creditPort.sendPort,
      });
    }

    try {
      await for (final chunk in data) {
        if (creditPort != null) {
          while (credits <= 0) {
            final hasCredit = await creditIterator!.moveNext();
            if (!hasCredit) {
              throw StateError('write credit port closed');
            }
            credits++;
          }
          credits--;
        }

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
      await creditIterator?.cancel();
      creditPort?.close();
      rethrow;
    }

    await creditIterator?.cancel();
    creditPort?.close();

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

  /// File path for [key], using this client's [rootDirPath].
  String pathForKey(String key, {String extension = 'bin'}) {
    return HashedKvPath.pathForKey(
      _rootDirPath,
      key,
      extension,
      _folderHierarchyLevels,
    );
  }

  /// Read using this client's [rootDirPath].
  Stream<List<int>> readStream(
    String key, {
    String extension = 'bin',
  }) {
    return _readStreamAt(_rootDirPath, key, extension: extension);
  }

  /// Read using an explicit [rootDirPath] (e.g. when sharing path logic across clients).
  Stream<List<int>> readStreamAt(
    String rootDirPath,
    String key, {
    String extension = 'bin',
  }) {
    return _readStreamAt(rootDirPath, key, extension: extension);
  }

  Stream<List<int>> _readStreamAt(
    String rootDirPath,
    String key, {
    required String extension,
  }) async* {
    final filePath = HashedKvPath.pathForKey(
      rootDirPath,
      key,
      extension,
      _folderHierarchyLevels,
    );
    final file = File(filePath);

    if (!await file.exists()) {
      throw KvNotFoundException(key, extension);
    }

    final stream = file.openRead();
    await for (final chunk in stream) {
      yield chunk;
    }
  }

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
