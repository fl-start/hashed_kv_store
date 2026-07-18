import 'dart:async';
import 'dart:isolate';
import 'dart:io';
import 'dart:typed_data';

import 'hashed_kv_path.dart';
import 'kv_exceptions.dart';
import 'kv_trace.dart';

typedef _Cmd = Map<String, dynamic>;

class _WriteContext {
  final IOSink sink;
  final String key;
  final String ext;
  final File targetFile;
  final File? tempFile;
  DateTime lastFlushAt;

  _WriteContext({
    required this.sink,
    required this.key,
    required this.ext,
    required this.targetFile,
    this.tempFile,
    required this.lastFlushAt,
  });
}

class _QueuedWrite {
  final String key;
  final String ext;
  final bool truncate;
  final SendPort replyPort;
  final String? filePath;
  final SendPort? creditPort;
  final int writeRequestId;

  _QueuedWrite({
    required this.key,
    required this.ext,
    required this.truncate,
    required this.replyPort,
    required this.writeRequestId,
    this.filePath,
    this.creditPort,
  });
}

class _ReadContext {
  final int readId;
  final SendPort chunkPort;
  StreamSubscription<List<int>>? subscription;
  var cancelled = false;

  _ReadContext({
    required this.readId,
    required this.chunkPort,
  });
}

class _QueuedDelete {
  final String key;
  final String ext;
  final SendPort replyPort;
  final String? filePath;

  _QueuedDelete({
    required this.key,
    required this.ext,
    required this.replyPort,
    this.filePath,
  });
}

List<int> _chunkFromMessage(Object? raw) {
  if (raw is TransferableTypedData) {
    return raw.materialize().asUint8List();
  }
  return (raw as List).cast<int>();
}

void kvWriteWorkerIsolateEntry(List<dynamic> args) async {
  final String rootDirPath = args[0] as String;
  final SendPort initPort = args[1] as SendPort;
  final int idlePurgeMs = (args.length > 2 ? args[2] as int : 60000);
  final int folderHierarchyLevels = (args.length > 3 ? args[3] as int : 1);
  final int workerIndex = (args.length > 4 ? args[4] as int : 0);
  final SendPort? routerControlPort =
      args.length > 5 ? args[5] as SendPort : null;
  final bool fsyncOnClose = (args.length > 6 ? args[6] as bool : false);
  final int flushThresholdBytes = (args.length > 7 ? args[7] as int : 65536);
  final int flushIntervalMs = (args.length > 8 ? args[8] as int : 100);

  final cmdPort = ReceivePort();
  initPort.send(cmdPort.sendPort);

  final subscribers = <String, List<SendPort>>{};
  final activeWriteIdForKey = <String, int?>{};
  final pendingWritesForKey = <String, List<_QueuedWrite>>{};
  final pendingDeletesForKey = <String, List<_QueuedDelete>>{};
  final writes = <int, _WriteContext>{};
  final creditPortsForWrite = <int, SendPort>{};
  final writeRequestIdForWriteId = <int, int>{};
  final activeReads = <int, _ReadContext>{};

  var nextWriteId = 1;
  var nextReadId = 1;
  var anyActivityRecent = true;
  Timer? idleTimer;

  String chanId(String key, String ext) => '$key::$ext';

  File fileFor(String key, String extension, {String? filePath}) {
    if (filePath != null) return File(filePath);
    return File(
      HashedKvPath.pathForKey(
        rootDirPath,
        key,
        extension,
        folderHierarchyLevels,
      ),
    );
  }

  Future<void> deleteKey(String key, String extension,
      {String? filePath}) async {
    final file = fileFor(key, extension, filePath: filePath);
    if (await file.exists()) {
      await file.delete();
    }
  }

  Future<void> syncFile(File file) async {
    if (!fsyncOnClose) return;
    RandomAccessFile? raf;
    try {
      raf = await file.open(mode: FileMode.read);
      await raf.flush();
    } catch (_) {
      // Best-effort flush; some platforms reject flush on temp/read handles.
    } finally {
      await raf?.close();
    }
  }

  void notifySubscribers(String key, String ext, List<int> chunk) {
    final subs = subscribers[chanId(key, ext)];
    if (subs == null) return;
    final bytes = chunk is Uint8List ? chunk : Uint8List.fromList(chunk);
    final transferable = TransferableTypedData.fromList([bytes]);
    for (final sp in subs) {
      sp.send(transferable);
    }
  }

  void endChannel(String key, String ext) {
    final subs = subscribers.remove(chanId(key, ext));
    if (subs == null) return;
    for (final sp in subs) {
      sp.send(null);
    }
  }

  Future<void> maybeFlush(_WriteContext ctx, int chunkLength) async {
    final now = DateTime.now();
    if (chunkLength >= flushThresholdBytes ||
        now.difference(ctx.lastFlushAt).inMilliseconds >= flushIntervalMs) {
      await ctx.sink.flush();
      ctx.lastFlushAt = now;
    }
  }

  Future<void> cleanupWriteContext(_WriteContext ctx) async {
    try {
      await ctx.sink.flush();
    } catch (_) {}
    try {
      await ctx.sink.close();
    } catch (_) {}
    if (ctx.tempFile != null && await ctx.tempFile!.exists()) {
      try {
        await ctx.tempFile!.delete();
      } catch (_) {}
    }
  }

  Future<void> failChannel(String key, String ext, Object error) async {
    final k = chanId(key, ext);
    activeWriteIdForKey[k] = null;

    final queuedWrites = pendingWritesForKey.remove(k) ?? [];
    for (final queued in queuedWrites) {
      kvSendError(queued.replyPort, error);
    }

    final queuedDeletes = pendingDeletesForKey.remove(k) ?? [];
    for (final queued in queuedDeletes) {
      kvSendError(queued.replyPort, error);
    }

    endChannel(key, ext);
  }

  Future<void> cleanupStaleTempFiles(File targetFile) async {
    final parent = targetFile.parent;
    if (!await parent.exists()) return;

    final prefix = '${targetFile.path}.';
    await for (final entity in parent.list()) {
      if (entity is! File) continue;
      final name = entity.path;
      if (name.startsWith(prefix) && name.endsWith('.tmp')) {
        try {
          await entity.delete();
        } catch (_) {}
      }
    }
  }

  Future<_WriteContext> openWriteContext({
    required String key,
    required String ext,
    required bool truncate,
    required int writeId,
    String? filePath,
  }) async {
    final targetFile = fileFor(key, ext, filePath: filePath);
    final now = DateTime.now();
    if (truncate) {
      await cleanupStaleTempFiles(targetFile);
      final tempFile = File('${targetFile.path}.$writeId.tmp');
      final sink = tempFile.openWrite();
      return _WriteContext(
        sink: sink,
        key: key,
        ext: ext,
        targetFile: targetFile,
        tempFile: tempFile,
        lastFlushAt: now,
      );
    }

    final sink = targetFile.openWrite(mode: FileMode.append);
    return _WriteContext(
      sink: sink,
      key: key,
      ext: ext,
      targetFile: targetFile,
      lastFlushAt: now,
    );
  }

  Future<void> publishTempFile(File tempFile, File targetFile) async {
    FileSystemException? lastError;

    for (var attempt = 0; attempt < 100; attempt++) {
      try {
        await tempFile.rename(targetFile.path);
        return;
      } on FileSystemException catch (e) {
        lastError = e;
      }

      try {
        if (await targetFile.exists()) {
          await targetFile.delete();
        }
        await tempFile.rename(targetFile.path);
        return;
      } on FileSystemException catch (e) {
        lastError = e;
        await Future.delayed(const Duration(milliseconds: 10));
      }
    }

    throw lastError ??
        FileSystemException('Failed to publish temp file', tempFile.path);
  }

  Future<void> completeWriteContext(_WriteContext ctx) async {
    await ctx.sink.flush();
    await ctx.sink.close();

    if (ctx.tempFile != null) {
      await syncFile(ctx.tempFile!);
      await publishTempFile(ctx.tempFile!, ctx.targetFile);
      await syncFile(ctx.targetFile);
    } else {
      await syncFile(ctx.targetFile);
    }
  }

  Future<void> processPendingDeletes(String k) async {
    final queue = pendingDeletesForKey[k];
    if (queue == null || queue.isEmpty) return;

    while (queue.isNotEmpty) {
      final next = queue.removeAt(0);
      try {
        await deleteKey(next.key, next.ext, filePath: next.filePath);
        kvSendOk(next.replyPort);
      } catch (e, st) {
        kvSendError(next.replyPort, e, st);
      }
    }
    pendingDeletesForKey.remove(k);
  }

  void registerWriteCredits(int writeId, SendPort? creditPort) {
    if (creditPort != null) {
      creditPortsForWrite[writeId] = creditPort;
    }
  }

  void sendOpenWriteAck(SendPort replyPort, int writeId) {
    replyPort.send(<String, dynamic>{
      'writeId': writeId,
      'workerPort': cmdPort.sendPort,
    });
  }

  Future<void> startQueuedWrite(String k) async {
    final queue = pendingWritesForKey[k];
    if (queue == null || queue.isEmpty) return;

    final next = queue.removeAt(0);
    if (queue.isEmpty) {
      pendingWritesForKey.remove(k);
    }

    final newWriteId = nextWriteId++;
    activeWriteIdForKey[k] = newWriteId;

    final trace = KvTraceWrite.begin(next.key);
    trace?.event('worker_start_write', {
      'ext': next.ext,
      'writeId': newWriteId,
      'worker': workerIndex,
      'queued': queue.length,
    });
    try {
      final ctx = await openWriteContext(
        key: next.key,
        ext: next.ext,
        truncate: next.truncate,
        writeId: newWriteId,
        filePath: next.filePath,
      );
      writes[newWriteId] = ctx;
      writeRequestIdForWriteId[newWriteId] = next.writeRequestId;
      registerWriteCredits(newWriteId, next.creditPort);
      sendOpenWriteAck(next.replyPort, newWriteId);
      trace?.slow('worker_open_write_ctx', {
        'ext': next.ext,
        'writeId': newWriteId,
        'worker': workerIndex,
      });
    } catch (e, st) {
      activeWriteIdForKey[k] = null;
      kvSendError(next.replyPort, e, st);
      await startQueuedWrite(k);
    }
  }

  void cancelIdleTimer() {
    idleTimer?.cancel();
    idleTimer = null;
  }

  void scheduleIdleCheckIfNeeded() {
    final isActive = activeWriteIdForKey.values.any((id) => id != null) ||
        pendingWritesForKey.values.any((queue) => queue.isNotEmpty) ||
        pendingDeletesForKey.values.any((queue) => queue.isNotEmpty) ||
        activeReads.isNotEmpty;

    if (!isActive && !anyActivityRecent) {
      cancelIdleTimer();
      idleTimer = Timer(Duration(milliseconds: idlePurgeMs), () {
        final stillActive =
            activeWriteIdForKey.values.any((id) => id != null) ||
                pendingWritesForKey.values.any((queue) => queue.isNotEmpty) ||
                pendingDeletesForKey.values.any((queue) => queue.isNotEmpty) ||
                activeReads.isNotEmpty;
        if (!stillActive) {
          routerControlPort?.send(<String, dynamic>{
            'type': 'workerExiting',
            'index': workerIndex,
          });
          Isolate.exit();
        }
      });
    } else {
      cancelIdleTimer();
      anyActivityRecent = false;
    }
  }

  Future<void> abortActiveWrite(int writeId, {SendPort? replyPort}) async {
    creditPortsForWrite.remove(writeId);
    writeRequestIdForWriteId.remove(writeId);
    final ctx = writes.remove(writeId);
    if (ctx == null) {
      kvSendAbort(replyPort);
      return;
    }

    await cleanupWriteContext(ctx);
    endChannel(ctx.key, ctx.ext);
    final k = chanId(ctx.key, ctx.ext);
    activeWriteIdForKey[k] = null;
    await processPendingDeletes(k);
    await startQueuedWrite(k);
    scheduleIdleCheckIfNeeded();
    kvSendAbort(replyPort);
  }

  Future<void> handleAbortWrite(_Cmd cmd) async {
    final key = cmd['key'] as String;
    final ext = cmd['ext'] as String;
    final writeRequestId = cmd['writeRequestId'] as int?;
    final writeId = cmd['writeId'] as int?;
    final replyPort = cmd['replyPort'] as SendPort?;
    final k = chanId(key, ext);

    if (writeId != null && writes.containsKey(writeId)) {
      await abortActiveWrite(writeId, replyPort: replyPort);
      return;
    }

    if (writeRequestId != null) {
      final activeId = activeWriteIdForKey[k];
      if (activeId != null &&
          writeRequestIdForWriteId[activeId] == writeRequestId) {
        await abortActiveWrite(activeId, replyPort: replyPort);
        return;
      }

      final queue = pendingWritesForKey[k];
      if (queue != null) {
        final idx = queue
            .indexWhere((queued) => queued.writeRequestId == writeRequestId);
        if (idx >= 0) {
          final removed = queue.removeAt(idx);
          if (queue.isEmpty) {
            pendingWritesForKey.remove(k);
          }
          kvSendAbort(removed.replyPort);
          if (activeWriteIdForKey[k] == null &&
              (pendingWritesForKey[k]?.isNotEmpty ?? false)) {
            await startQueuedWrite(k);
          }
          kvSendAbort(replyPort);
          return;
        }
      }
    }

    kvSendAbort(replyPort);
  }

  Future<void> cancelRead(int readId, {SendPort? replyPort}) async {
    final ctx = activeReads.remove(readId);
    if (ctx == null) {
      kvSendAbort(replyPort);
      return;
    }

    ctx.cancelled = true;
    await ctx.subscription?.cancel();
    ctx.chunkPort.send(<String, dynamic>{'aborted': true});
    kvSendAbort(replyPort);
  }

  await for (final raw in cmdPort) {
    final cmd = raw as _Cmd;
    final type = cmd['type'] as String;
    anyActivityRecent = true;

    switch (type) {
      case 'shutdown':
        kvSendOk(cmd['replyPort'] as SendPort?);
        Isolate.exit();

      case 'openWrite':
        {
          final key = cmd['key'] as String;
          final ext = cmd['ext'] as String;
          final truncate = cmd['truncate'] as bool? ?? true;
          final replyPort = cmd['replyPort'] as SendPort;
          final filePath = cmd['filePath'] as String?;
          final writeRequestId = cmd['writeRequestId'] as int? ?? 0;
          final k = chanId(key, ext);
          final active = activeWriteIdForKey[k];

          if (active != null || (pendingWritesForKey[k]?.isNotEmpty ?? false)) {
            pendingWritesForKey
                .putIfAbsent(k, () => <_QueuedWrite>[])
                .add(_QueuedWrite(
                  key: key,
                  ext: ext,
                  truncate: truncate,
                  replyPort: replyPort,
                  writeRequestId: writeRequestId,
                  filePath: filePath,
                  creditPort: cmd['creditPort'] as SendPort?,
                ));
            scheduleIdleCheckIfNeeded();
            break;
          }

          final writeId = nextWriteId++;
          activeWriteIdForKey[k] = writeId;

          try {
            final ctx = await openWriteContext(
              key: key,
              ext: ext,
              truncate: truncate,
              writeId: writeId,
              filePath: filePath,
            );
            writes[writeId] = ctx;
            writeRequestIdForWriteId[writeId] = writeRequestId;
            registerWriteCredits(writeId, cmd['creditPort'] as SendPort?);
            sendOpenWriteAck(replyPort, writeId);
          } catch (e, st) {
            activeWriteIdForKey[k] = null;
            kvSendError(replyPort, e, st);
            await startQueuedWrite(k);
          }
          scheduleIdleCheckIfNeeded();
        }
        break;

      case 'registerCredits':
        {
          final writeId = cmd['writeId'] as int;
          creditPortsForWrite[writeId] = cmd['creditPort'] as SendPort;
          final replyPort = cmd['replyPort'] as SendPort?;
          kvSendOk(replyPort);
        }
        break;

      case 'writeChunk':
        {
          final writeId = cmd['writeId'] as int;
          final ctx = writes[writeId];
          // Late chunks after abort are ignored when writeId is unknown.
          if (ctx == null) break;

          try {
            final chunk = _chunkFromMessage(cmd['chunk']);
            ctx.sink.add(chunk);
            await maybeFlush(ctx, chunk.length);
            notifySubscribers(ctx.key, ctx.ext, chunk);
            creditPortsForWrite[writeId]?.send(null);
          } catch (e) {
            creditPortsForWrite.remove(writeId);
            writes.remove(writeId);
            await cleanupWriteContext(ctx);
            await failChannel(ctx.key, ctx.ext, e);
            scheduleIdleCheckIfNeeded();
          }
        }
        break;

      case 'writeEnd':
        {
          final writeId = cmd['writeId'] as int;
          final replyPort = cmd['replyPort'] as SendPort?;
          creditPortsForWrite.remove(writeId);
          writeRequestIdForWriteId.remove(writeId);
          final ctx = writes.remove(writeId);
          // Late writeEnd after abort is ignored when writeId is unknown.
          if (ctx == null) {
            kvSendError(replyPort, StateError('unknown writeId: $writeId'));
            break;
          }

          final trace = KvTraceWrite.begin(ctx.key);
          trace?.event('worker_write_end_start', {
            'ext': ctx.ext,
            'writeId': writeId,
            'worker': workerIndex,
          });
          try {
            await completeWriteContext(ctx);
            endChannel(ctx.key, ctx.ext);
            kvSendOk(replyPort);

            final k = chanId(ctx.key, ctx.ext);
            activeWriteIdForKey[k] = null;
            await processPendingDeletes(k);
            await startQueuedWrite(k);
            trace?.eventWithMs('worker_write_end_done', {
              'ext': ctx.ext,
              'writeId': writeId,
              'worker': workerIndex,
            });
            trace?.slow('worker_write_end', {
              'ext': ctx.ext,
              'writeId': writeId,
              'worker': workerIndex,
            });
          } catch (e, st) {
            await cleanupWriteContext(ctx);
            await failChannel(ctx.key, ctx.ext, e);
            kvSendError(replyPort, e, st);
          }
          scheduleIdleCheckIfNeeded();
        }
        break;

      case 'writeAbort':
        {
          final writeId = cmd['writeId'] as int;
          await abortActiveWrite(writeId);
        }
        break;

      case 'abortWrite':
        await handleAbortWrite(cmd);
        break;

      case 'openRead':
        {
          final key = cmd['key'] as String;
          final ext = cmd['ext'] as String;
          final filePath = cmd['filePath'] as String;
          final chunkPort = cmd['chunkPort'] as SendPort;
          final replyPort = cmd['replyPort'] as SendPort;
          final readId = nextReadId++;
          final file = File(filePath);
          final ctx = _ReadContext(readId: readId, chunkPort: chunkPort);
          activeReads[readId] = ctx;

          try {
            if (!await file.exists()) {
              activeReads.remove(readId);
              kvSendError(replyPort, KvNotFoundException(key, ext));
              break;
            }

            kvSendOk(replyPort, <String, dynamic>{'readId': readId});
            final stream = file.openRead();
            ctx.subscription = stream.listen(
              (chunk) {
                if (ctx.cancelled) return;
                final bytes =
                    chunk is Uint8List ? chunk : Uint8List.fromList(chunk);
                chunkPort.send(TransferableTypedData.fromList([bytes]));
              },
              onError: (Object error, StackTrace stackTrace) {
                activeReads.remove(readId);
                chunkPort.send(<String, dynamic>{
                  'error': error.toString(),
                  'type': error.runtimeType.toString(),
                  'stackTrace': stackTrace.toString(),
                });
              },
              onDone: () {
                activeReads.remove(readId);
                if (!ctx.cancelled) {
                  chunkPort.send(null);
                }
              },
              cancelOnError: true,
            );
          } catch (e, st) {
            activeReads.remove(readId);
            kvSendError(replyPort, e, st);
          }
          scheduleIdleCheckIfNeeded();
        }
        break;

      case 'cancelRead':
        await cancelRead(cmd['readId'] as int,
            replyPort: cmd['replyPort'] as SendPort?);
        break;

      case 'delete':
        {
          final key = cmd['key'] as String;
          final ext = cmd['ext'] as String;
          final replyPort = cmd['replyPort'] as SendPort?;
          final filePath = cmd['filePath'] as String?;
          final k = chanId(key, ext);
          final hasActiveWrite = activeWriteIdForKey[k] != null;
          final hasPendingWrites = pendingWritesForKey[k]?.isNotEmpty ?? false;

          if (hasActiveWrite || hasPendingWrites) {
            pendingDeletesForKey
                .putIfAbsent(k, () => <_QueuedDelete>[])
                .add(_QueuedDelete(
                  key: key,
                  ext: ext,
                  replyPort: replyPort!,
                  filePath: filePath,
                ));
            scheduleIdleCheckIfNeeded();
            break;
          }

          try {
            await deleteKey(key, ext, filePath: filePath);
            kvSendOk(replyPort);
          } catch (e, st) {
            kvSendError(replyPort, e, st);
          }
          scheduleIdleCheckIfNeeded();
        }
        break;

      case 'subscribeLive':
        {
          final key = cmd['key'] as String;
          final ext = cmd['ext'] as String;
          final subPort = cmd['subscriberPort'] as SendPort;
          final list =
              subscribers.putIfAbsent(chanId(key, ext), () => <SendPort>[]);
          list.add(subPort);
        }
        break;

      default:
        break;
    }
  }
}
