import 'dart:async';
import 'dart:io';
import 'dart:isolate';
import 'dart:typed_data';

import 'hashed_kv_path.dart';
import 'kv_exceptions.dart';
import 'kv_router_isolate.dart';

export 'kv_exceptions.dart';

/// Public client API for the multi-isolate KV store.
class MultiIsolateKvStoreClient {
  final SendPort _routerPort;
  final String _rootDirPath;
  final int _folderHierarchyLevels;
  final int _writeMaxInFlightChunks;
  bool _closed = false;

  MultiIsolateKvStoreClient._(
    this._routerPort,
    this._rootDirPath,
    this._folderHierarchyLevels,
    this._writeMaxInFlightChunks,
  );

  /// Root directory where KV files are stored (from [spawn]).
  String get rootDirPath => _rootDirPath;

  /// Whether [close] has been called on this client.
  bool get isClosed => _closed;

  void _ensureOpen() {
    if (_closed) {
      throw StateError('KV store is closed');
    }
  }

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
    if (rootDirPath.isEmpty) {
      throw ArgumentError.value(
        rootDirPath,
        'rootDirPath',
        'must not be empty',
      );
    }
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
    if (writeIdlePurgeDuration.isNegative) {
      throw ArgumentError.value(
        writeIdlePurgeDuration,
        'writeIdlePurgeDuration',
        'must not be negative',
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

  /// Shuts down router, worker, and folder isolates. Idempotent.
  ///
  /// After close, write/delete/subscribe operations throw [StateError].
  /// Direct reads via [readStream] remain available.
  Future<void> close() async {
    if (_closed) return;
    _closed = true;

    final replyPort = ReceivePort();
    _routerPort.send(<String, dynamic>{
      'type': 'shutdown',
      'replyPort': replyPort.sendPort,
    });
    await replyPort.first;
    replyPort.close();
  }

  /// Streaming write into the KV store for [key] with [extension].
  Future<void> writeFromStream(
    String key,
    Stream<List<int>> data, {
    String extension = 'bin',
    bool truncateExisting = true,
  }) async {
    _ensureOpen();

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
    kvThrowIfError(ack);

    final writeId = ack['writeId'] as int;
    final workerPort = ack['workerPort'] as SendPort;

    ReceivePort? creditPort;
    StreamIterator<dynamic>? creditIterator;
    var credits = _writeMaxInFlightChunks;
    if (_writeMaxInFlightChunks > 0) {
      creditPort = ReceivePort();
      creditIterator = StreamIterator<dynamic>(creditPort);
      final creditsReply = ReceivePort();
      workerPort.send(<String, dynamic>{
        'type': 'registerCredits',
        'writeId': writeId,
        'creditPort': creditPort.sendPort,
        'replyPort': creditsReply.sendPort,
      });
      final creditsAck = await creditsReply.first as Map;
      creditsReply.close();
      kvThrowIfError(creditsAck);
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

        final bytes = chunk is Uint8List ? chunk : Uint8List.fromList(chunk);
        final transferable = TransferableTypedData.fromList([bytes]);
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
    kvThrowIfError(endAck);
  }

  Stream<List<int>> subscribeLive(
    String key, {
    String extension = 'bin',
  }) {
    _ensureOpen();

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
      } else if (message is TransferableTypedData) {
        controller.add(message.materialize().asUint8List());
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
    _ensureOpen();

    final replyPort = ReceivePort();
    _routerPort.send(<String, dynamic>{
      'type': 'delete',
      'key': key,
      'ext': extension,
      'replyPort': replyPort.sendPort,
    });
    final ack = await replyPort.first as Map;
    replyPort.close();
    kvThrowIfError(ack);
  }
}
