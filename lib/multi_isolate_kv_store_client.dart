import 'dart:async';
import 'dart:io';
import 'dart:isolate';
import 'dart:typed_data';

import 'package:path/path.dart' as p;

import 'hashed_kv_path.dart';
import 'kv_abort.dart';
import 'kv_direct_io.dart';
import 'kv_exceptions.dart';
import 'kv_layout_migration.dart';
import 'kv_path_cache.dart';
import 'kv_router_isolate.dart';

export 'kv_exceptions.dart';

/// Public client API for the multi-isolate KV store.
class MultiIsolateKvStoreClient {
  final SendPort? _routerPort;
  final String _rootDirPath;
  final int _folderHierarchyLevels;
  final int _writeMaxInFlightChunks;
  final KvPathCache _pathCache;
  bool _closed = false;

  static var _nextWriteRequestId = 1;
  static var _nextReadRequestId = 1;

  MultiIsolateKvStoreClient._(
    this._routerPort,
    this._rootDirPath,
    this._folderHierarchyLevels,
    this._writeMaxInFlightChunks,
    this._pathCache,
  );

  /// Root directory where KV files are stored.
  String get rootDirPath => _rootDirPath;

  /// Whether [close] has been called on this client.
  bool get isClosed => _closed;

  /// Whether this client was opened with [openReadOnly] (no write isolates).
  bool get isReadOnly => _routerPort == null;

  void _ensureOpen() {
    if (_closed) {
      throw StateError('KV store is closed');
    }
  }

  void _ensureWriteCapable() {
    _ensureOpen();
    if (_routerPort == null) {
      throw StateError(
        'Write operations require spawn(); use openReadOnly for read-only access',
      );
    }
  }

  String _cacheKey(String key, String extension) => '$key::$extension';

  String _resolvePath(String rootDirPath, String key, String extension) {
    if (rootDirPath != _rootDirPath) {
      return HashedKvPath.pathForKey(
        rootDirPath,
        key,
        extension,
        _folderHierarchyLevels,
      );
    }
    return _pathCache.getOrCompute(
      _cacheKey(key, extension),
      () => HashedKvPath.pathForKey(
        _rootDirPath,
        key,
        extension,
        _folderHierarchyLevels,
      ),
    );
  }

  File _fileFor(String rootDirPath, String key, String extension) {
    return File(_resolvePath(rootDirPath, key, extension));
  }

  /// Opens a read-only client without spawning write isolates.
  ///
  /// Reads, [exists], [listStoredPaths], [deleteLocal], and
  /// [writeFromStreamDirect] work in the caller isolate. Use [spawn] when
  /// writes, [delete], or live subscriptions are needed.
  static Future<MultiIsolateKvStoreClient> openReadOnly({
    required String rootDirPath,
    int folderHierarchyLevels = 1,
    bool wipeOnLayoutMismatch = true,
    int pathCacheMaxEntries = 4096,
  }) async {
    _validateRootDirPath(rootDirPath);
    _validateFolderHierarchyLevels(folderHierarchyLevels);
    if (pathCacheMaxEntries < 0) {
      throw ArgumentError.value(
        pathCacheMaxEntries,
        'pathCacheMaxEntries',
        'must not be negative',
      );
    }

    await ensureKvStoreLayout(
      rootDirPath: rootDirPath,
      wipeOnLayoutMismatch: wipeOnLayoutMismatch,
    );

    return MultiIsolateKvStoreClient._(
      null,
      rootDirPath,
      folderHierarchyLevels,
      0,
      KvPathCache(maxEntries: pathCacheMaxEntries),
    );
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
    bool wipeOnLayoutMismatch = true,
    int pathCacheMaxEntries = 4096,
  }) async {
    _validateRootDirPath(rootDirPath);
    if (numWriteWorkers <= 0) {
      throw ArgumentError.value(
        numWriteWorkers,
        'numWriteWorkers',
        'must be greater than zero',
      );
    }
    _validateFolderHierarchyLevels(folderHierarchyLevels);
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
    if (pathCacheMaxEntries < 0) {
      throw ArgumentError.value(
        pathCacheMaxEntries,
        'pathCacheMaxEntries',
        'must not be negative',
      );
    }

    await ensureKvStoreLayout(
      rootDirPath: rootDirPath,
      wipeOnLayoutMismatch: wipeOnLayoutMismatch,
    );

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
      KvPathCache(maxEntries: pathCacheMaxEntries),
    );
  }

  static void _validateRootDirPath(String rootDirPath) {
    if (rootDirPath.isEmpty) {
      throw ArgumentError.value(
        rootDirPath,
        'rootDirPath',
        'must not be empty',
      );
    }
  }

  static void _validateFolderHierarchyLevels(int folderHierarchyLevels) {
    if (folderHierarchyLevels < 0 || folderHierarchyLevels > 2) {
      throw ArgumentError.value(
        folderHierarchyLevels,
        'folderHierarchyLevels',
        'must be 0, 1, or 2',
      );
    }
  }

  /// Shuts down router, worker, and folder isolates. Idempotent.
  ///
  /// For read-only clients this only marks the client closed. Direct reads
  /// via [readStream] and [readBytes] remain available until [close].
  Future<void> close() async {
    if (_closed) return;
    _closed = true;

    final routerPort = _routerPort;
    if (routerPort == null) return;

    final replyPort = ReceivePort();
    routerPort.send(<String, dynamic>{
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
    KvAbortSignal? signal,
  }) async {
    if (signal?.aborted ?? false) {
      // Abort happened before we subscribed; still drain the caller's stream so
      // a single-subscription source (e.g. StreamController) can close cleanly.
      await _drainAndCancel(data);
      throw KvAbortException(signal!.reason);
    }
    _ensureWriteCapable();
    final routerPort = _routerPort!;

    final writeRequestId = _nextWriteRequestId++;

    ReceivePort? creditPort;
    if (_writeMaxInFlightChunks > 0) {
      creditPort = ReceivePort();
    }

    final replyPort = ReceivePort();
    routerPort.send(<String, dynamic>{
      'type': 'openWrite',
      'key': key,
      'ext': extension,
      'truncate': truncateExisting,
      'replyPort': replyPort.sendPort,
      'writeRequestId': writeRequestId,
      if (creditPort != null) 'creditPort': creditPort.sendPort,
    });

    late Map<dynamic, dynamic> ack;
    try {
      if (signal != null) {
        ack = await raceWithAbort(
          replyPort.first.then((value) => value as Map<dynamic, dynamic>),
          signal,
        );
      } else {
        ack = await replyPort.first as Map<dynamic, dynamic>;
      }
    } on KvAbortException {
      replyPort.close();
      _sendAbortWrite(
        routerPort: routerPort,
        key: key,
        extension: extension,
        writeRequestId: writeRequestId,
      );
      // We aborted while awaiting the open ack and never subscribed to the
      // input; drain it so a caller-owned StreamController can close cleanly.
      await _drainAndCancel(data);
      rethrow;
    }
    replyPort.close();
    kvThrowIfError(ack);

    final writeId = ack['writeId'] as int;
    final workerPort = ack['workerPort'] as SendPort;

    StreamIterator<dynamic>? creditIterator;
    var credits = _writeMaxInFlightChunks;
    if (creditPort != null) {
      creditIterator = StreamIterator<dynamic>(creditPort);
    }

    final input = StreamIterator<List<int>>(data);
    var aborted = false;
    void onAbort() {
      aborted = true;
      input.cancel();
    }

    signal?.onAbort(onAbort);

    try {
      while (true) {
        if (aborted) {
          throw KvAbortException(signal?.reason);
        }
        signal?.throwIfAborted();

        if (!await input.moveNext()) {
          break;
        }
        final chunk = input.current;

        if (creditPort != null) {
          while (credits <= 0) {
            if (aborted) {
              throw KvAbortException(signal?.reason);
            }
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

      if (aborted) {
        throw KvAbortException(signal?.reason);
      }
    } catch (e) {
      _sendAbortWrite(
        routerPort: routerPort,
        key: key,
        extension: extension,
        writeRequestId: writeRequestId,
        writeId: writeId,
      );
      rethrow;
    } finally {
      signal?.removeAbortListener(onAbort);
      await input.cancel();
      await creditIterator?.cancel();
      creditPort?.close();
    }

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

  /// Caller-isolate write bypassing worker isolates.
  ///
  /// Uses the same atomic truncate semantics as worker writes but does not
  /// notify live subscribers. Suitable for bulk ingest when isolation is not
  /// needed.
  Future<void> writeFromStreamDirect(
    String key,
    Stream<List<int>> data, {
    String extension = 'bin',
    bool truncateExisting = true,
    KvAbortSignal? signal,
  }) async {
    _ensureOpen();
    final direct = KvDirectIo(
      rootDirPath: _rootDirPath,
      folderHierarchyLevels: _folderHierarchyLevels,
    );
    await direct.writeFromStream(
      key,
      data,
      extension: extension,
      truncateExisting: truncateExisting,
      signal: signal,
    );
  }

  Stream<List<int>> subscribeLive(
    String key, {
    String extension = 'bin',
  }) {
    _ensureWriteCapable();
    final routerPort = _routerPort!;

    final recv = ReceivePort();
    routerPort.send(<String, dynamic>{
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
    return _resolvePath(_rootDirPath, key, extension);
  }

  /// Whether a value exists for [key] and [extension] on disk.
  Future<bool> exists(String key, {String extension = 'bin'}) async {
    return _fileFor(_rootDirPath, key, extension).exists();
  }

  /// Lists stored file paths relative to [rootDirPath].
  ///
  /// Original string keys cannot be recovered from hashed paths. Each entry is
  /// a relative path such as `ab/ab12-3456-7890-cdef.bin`.
  Future<List<String>> listStoredPaths() async {
    final root = Directory(_rootDirPath);
    if (!await root.exists()) return const [];

    final paths = <String>[];
    await _collectStoredPaths(root, '', paths);
    paths.sort();
    return paths;
  }

  Future<void> _collectStoredPaths(
    Directory dir,
    String relativePrefix,
    List<String> out,
  ) async {
    await for (final entity in dir.list(followLinks: false)) {
      if (entity is Directory) {
        final name = p.basename(entity.path);
        final nextPrefix =
            relativePrefix.isEmpty ? name : p.join(relativePrefix, name);
        await _collectStoredPaths(entity, nextPrefix, out);
      } else if (entity is File && _looksLikeStoredValue(entity.path)) {
        final name = p.basename(entity.path);
        out.add(relativePrefix.isEmpty ? name : p.join(relativePrefix, name));
      }
    }
  }

  bool _looksLikeStoredValue(String filePath) {
    if (filePath.contains('.tmp')) return false;
    final fileName = p.basename(filePath);
    final dot = fileName.lastIndexOf('.');
    final stem = dot == -1 ? fileName : fileName.substring(0, dot);
    return RegExp(
      r'^[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}$',
    ).hasMatch(stem);
  }

  /// Read the full value as bytes in the caller isolate.
  Future<Uint8List> readBytes(
    String key, {
    String extension = 'bin',
    bool checkExists = true,
    KvAbortSignal? signal,
  }) {
    if (signal != null && _routerPort != null) {
      return _readBytesRouted(
        _rootDirPath,
        key,
        extension: extension,
        checkExists: checkExists,
        signal: signal,
      );
    }
    return _readBytesAt(
      _rootDirPath,
      key,
      extension: extension,
      checkExists: checkExists,
      signal: signal,
    );
  }

  Future<Uint8List> _readBytesAt(
    String rootDirPath,
    String key, {
    required String extension,
    required bool checkExists,
    KvAbortSignal? signal,
  }) async {
    final file = _fileFor(rootDirPath, key, extension);

    Future<Uint8List> readFuture() => file.readAsBytes();

    try {
      if (signal != null) {
        return await raceWithAbort(readFuture(), signal);
      }
      return await readFuture();
    } catch (e) {
      if (checkExists && kvIsNotFoundError(e)) {
        throw KvNotFoundException(key, extension);
      }
      rethrow;
    }
  }

  Future<Uint8List> _readBytesRouted(
    String rootDirPath,
    String key, {
    required String extension,
    required bool checkExists,
    required KvAbortSignal signal,
  }) async {
    final bytes = <int>[];
    await for (final chunk in _readStreamRouted(
      rootDirPath,
      key,
      extension: extension,
      checkExists: checkExists,
      signal: signal,
    )) {
      bytes.addAll(chunk);
    }
    return Uint8List.fromList(bytes);
  }

  /// Reads many keys concurrently in the caller isolate.
  Future<Map<String, Uint8List>> readBytesAll(
    Iterable<String> keys, {
    String extension = 'bin',
    bool checkExists = true,
    KvAbortSignal? signal,
  }) async {
    final keyList = keys is List<String> ? keys : keys.toList(growable: false);
    final entries = await Future.wait(
      keyList.map(
        (key) async => MapEntry(
          key,
          await readBytes(
            key,
            extension: extension,
            checkExists: checkExists,
            signal: signal,
          ),
        ),
      ),
    );
    return Map.fromEntries(entries);
  }

  /// Read using this client's [rootDirPath].
  Stream<List<int>> readStream(
    String key, {
    String extension = 'bin',
    bool checkExists = true,
    KvAbortSignal? signal,
  }) {
    if (signal != null && _routerPort != null) {
      return _readStreamRouted(
        _rootDirPath,
        key,
        extension: extension,
        checkExists: checkExists,
        signal: signal,
      );
    }
    return _readStreamAt(
      _rootDirPath,
      key,
      extension: extension,
      checkExists: checkExists,
      signal: signal,
    );
  }

  /// Read using an explicit [rootDirPath] (e.g. when sharing path logic across clients).
  Stream<List<int>> readStreamAt(
    String rootDirPath,
    String key, {
    String extension = 'bin',
    bool checkExists = true,
    KvAbortSignal? signal,
  }) {
    if (signal != null && _routerPort != null) {
      return _readStreamRouted(
        rootDirPath,
        key,
        extension: extension,
        checkExists: checkExists,
        signal: signal,
      );
    }
    return _readStreamAt(
      rootDirPath,
      key,
      extension: extension,
      checkExists: checkExists,
      signal: signal,
    );
  }

  Stream<List<int>> _readStreamAt(
    String rootDirPath,
    String key, {
    required String extension,
    required bool checkExists,
    KvAbortSignal? signal,
  }) async* {
    final file = _fileFor(rootDirPath, key, extension);
    final input = StreamIterator<List<int>>(file.openRead());
    var aborted = false;
    void onAbort() {
      aborted = true;
      input.cancel();
    }

    signal?.onAbort(onAbort);

    try {
      while (true) {
        signal?.throwIfAborted();
        if (aborted) {
          throw KvAbortException(signal?.reason);
        }
        if (!await input.moveNext()) {
          break;
        }
        yield input.current;
      }
    } catch (e) {
      if (aborted && e is! KvAbortException) {
        throw KvAbortException(signal?.reason);
      }
      if (checkExists && kvIsNotFoundError(e)) {
        throw KvNotFoundException(key, extension);
      }
      rethrow;
    } finally {
      signal?.removeAbortListener(onAbort);
      await input.cancel();
    }
  }

  Stream<List<int>> _readStreamRouted(
    String rootDirPath,
    String key, {
    required String extension,
    required bool checkExists,
    required KvAbortSignal signal,
  }) async* {
    signal.throwIfAborted();
    _ensureWriteCapable();
    final routerPort = _routerPort!;
    final readRequestId = _nextReadRequestId++;

    final chunkPort = ReceivePort();
    final replyPort = ReceivePort();
    routerPort.send(<String, dynamic>{
      'type': 'openRead',
      'key': key,
      'ext': extension,
      'readRequestId': readRequestId,
      'chunkPort': chunkPort.sendPort,
      'replyPort': replyPort.sendPort,
    });

    late Map<dynamic, dynamic> ack;
    try {
      ack = await raceWithAbort(
        replyPort.first.then((value) => value as Map<dynamic, dynamic>),
        signal,
      );
    } on KvAbortException {
      replyPort.close();
      chunkPort.close();
      rethrow;
    }
    replyPort.close();
    kvThrowIfError(ack);

    final readId = ack['readId'] as int;
    var aborted = false;
    void onAbort() {
      if (aborted) return;
      aborted = true;
      routerPort.send(<String, dynamic>{
        'type': 'cancelRead',
        'readId': readId,
        'key': key,
      });
      chunkPort.close();
    }

    signal.onAbort(onAbort);

    try {
      await for (final message in chunkPort) {
        if (message is Map) {
          if (message['aborted'] == true) {
            throw KvAbortException(signal.reason);
          }
          kvThrowIfError(message);
        } else if (message is TransferableTypedData) {
          yield message.materialize().asUint8List();
        } else if (message == null) {
          break;
        }
      }
    } finally {
      signal.removeAbortListener(onAbort);
      chunkPort.close();
    }

    if (aborted) {
      throw KvAbortException(signal.reason);
    }
  }

  /// Subscribes to and immediately cancels [data] so a caller-provided
  /// single-subscription stream can complete its own `close()` even when the
  /// write is aborted before the main pump loop subscribes.
  Future<void> _drainAndCancel(Stream<List<int>> data) async {
    try {
      final sub = data.listen(null, cancelOnError: true);
      await sub.cancel();
    } catch (_) {}
  }

  void _sendAbortWrite({
    required SendPort routerPort,
    required String key,
    required String extension,
    required int writeRequestId,
    int? writeId,
  }) {
    routerPort.send(<String, dynamic>{
      'type': 'abortWrite',
      'key': key,
      'ext': extension,
      'writeRequestId': writeRequestId,
      if (writeId != null) 'writeId': writeId,
    });
  }

  /// Deletes a key directly in the caller isolate.
  ///
  /// Does not coordinate with active worker writes for the same key. Prefer
  /// [delete] when a spawned client may have in-flight writes.
  Future<void> deleteLocal(String key, {String extension = 'bin'}) async {
    _ensureOpen();
    final direct = KvDirectIo(
      rootDirPath: _rootDirPath,
      folderHierarchyLevels: _folderHierarchyLevels,
    );
    await direct.delete(key, extension: extension);
  }

  Future<void> delete(String key, {String extension = 'bin'}) async {
    _ensureWriteCapable();
    final routerPort = _routerPort!;

    final replyPort = ReceivePort();
    routerPort.send(<String, dynamic>{
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
