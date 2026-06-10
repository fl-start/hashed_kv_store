import 'dart:async';
import 'dart:isolate';
import 'dart:io';

import 'hashed_kv_path.dart';

typedef _Cmd = Map<String, dynamic>;

class _WriteContext {
  final IOSink sink;
  final String key;
  final String ext;
  final File targetFile;
  final File? tempFile;

  _WriteContext({
    required this.sink,
    required this.key,
    required this.ext,
    required this.targetFile,
    this.tempFile,
  });
}

class _QueuedWrite {
  final String key;
  final String ext;
  final bool truncate;
  final SendPort replyPort;

  _QueuedWrite({
    required this.key,
    required this.ext,
    required this.truncate,
    required this.replyPort,
  });
}

class _QueuedDelete {
  final String key;
  final String ext;
  final SendPort replyPort;

  _QueuedDelete({
    required this.key,
    required this.ext,
    required this.replyPort,
  });
}

void _sendError(SendPort? port, Object error) {
  port?.send(<String, dynamic>{'error': error.toString()});
}

void _sendOk(SendPort? port) {
  port?.send(<String, dynamic>{'ok': true});
}

List<int> _chunkFromMessage(Object? raw) {
  if (raw is TransferableTypedData) {
    return raw.materialize().asUint8List();
  }
  return (raw as List).cast<int>();
}

/// Write worker isolate entry function.
///
/// args:
/// - args[0] : String rootDirPath
/// - args[1] : SendPort initPort (router uses this to get our SendPort)
/// - args[2] : int idlePurgeMs
/// - args[3] : int folderHierarchyLevels (default: 1)
/// - args[4] : int workerIndex
/// - args[5] : SendPort routerControlPort (worker lifecycle)
void kvWriteWorkerIsolateEntry(List<dynamic> args) async {
  final String rootDirPath = args[0] as String;
  final SendPort initPort = args[1] as SendPort;
  final int idlePurgeMs = (args.length > 2 ? args[2] as int : 60000);
  final int folderHierarchyLevels = (args.length > 3 ? args[3] as int : 1);
  final int workerIndex = (args.length > 4 ? args[4] as int : 0);
  final SendPort? routerControlPort =
      args.length > 5 ? args[5] as SendPort : null;

  final cmdPort = ReceivePort();
  initPort.send(cmdPort.sendPort);

  final subscribers = <String, List<SendPort>>{};
  final activeWriteIdForKey = <String, int?>{};
  final pendingWritesForKey = <String, List<_QueuedWrite>>{};
  final pendingDeletesForKey = <String, List<_QueuedDelete>>{};
  final writes = <int, _WriteContext>{};

  var nextWriteId = 1;
  var anyActivityRecent = true;
  Timer? idleTimer;

  String chanId(String key, String ext) => '$key::$ext';

  File fileFor(String key, String extension) {
    return File(
      HashedKvPath.pathForKey(
        rootDirPath,
        key,
        extension,
        folderHierarchyLevels,
      ),
    );
  }

  Future<void> deleteKey(String key, String extension) async {
    final file = fileFor(key, extension);
    if (await file.exists()) {
      await file.delete();
    }
  }

  void notifySubscribers(String key, String ext, List<int> chunk) {
    final subs = subscribers[chanId(key, ext)];
    if (subs == null) return;
    for (final sp in subs) {
      sp.send(chunk);
    }
  }

  void endChannel(String key, String ext) {
    final subs = subscribers.remove(chanId(key, ext));
    if (subs == null) return;
    for (final sp in subs) {
      sp.send(null);
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
      _sendError(queued.replyPort, error);
    }

    final queuedDeletes = pendingDeletesForKey.remove(k) ?? [];
    for (final queued in queuedDeletes) {
      _sendError(queued.replyPort, error);
    }

    endChannel(key, ext);
  }

  Future<_WriteContext> openWriteContext({
    required String key,
    required String ext,
    required bool truncate,
    required int writeId,
  }) async {
    final targetFile = fileFor(key, ext);
    if (truncate) {
      final tempFile = File('${targetFile.path}.$writeId.tmp');
      final sink = tempFile.openWrite();
      return _WriteContext(
        sink: sink,
        key: key,
        ext: ext,
        targetFile: targetFile,
        tempFile: tempFile,
      );
    }

    final sink = targetFile.openWrite(mode: FileMode.append);
    return _WriteContext(
      sink: sink,
      key: key,
      ext: ext,
      targetFile: targetFile,
    );
  }

  Future<void> completeWriteContext(_WriteContext ctx) async {
    await ctx.sink.flush();
    await ctx.sink.close();
    if (ctx.tempFile != null) {
      if (await ctx.targetFile.exists()) {
        await ctx.targetFile.delete();
      }
      await ctx.tempFile!.rename(ctx.targetFile.path);
    }
  }

  Future<void> processPendingDeletes(String k) async {
    final queue = pendingDeletesForKey[k];
    if (queue == null || queue.isEmpty) return;

    while (queue.isNotEmpty) {
      final next = queue.removeAt(0);
      try {
        await deleteKey(next.key, next.ext);
        _sendOk(next.replyPort);
      } catch (e) {
        _sendError(next.replyPort, e);
      }
    }
    pendingDeletesForKey.remove(k);
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

    try {
      final ctx = await openWriteContext(
        key: next.key,
        ext: next.ext,
        truncate: next.truncate,
        writeId: newWriteId,
      );
      writes[newWriteId] = ctx;
      next.replyPort.send(<String, dynamic>{
        'writeId': newWriteId,
        'workerPort': cmdPort.sendPort,
      });
    } catch (e) {
      activeWriteIdForKey[k] = null;
      _sendError(next.replyPort, e);
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
        pendingDeletesForKey.values.any((queue) => queue.isNotEmpty);

    if (!isActive && !anyActivityRecent) {
      cancelIdleTimer();
      idleTimer = Timer(Duration(milliseconds: idlePurgeMs), () {
        final stillActive = activeWriteIdForKey.values.any((id) => id != null) ||
            pendingWritesForKey.values.any((queue) => queue.isNotEmpty) ||
            pendingDeletesForKey.values.any((queue) => queue.isNotEmpty);
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

  await for (final raw in cmdPort) {
    final cmd = raw as _Cmd;
    final type = cmd['type'] as String;
    anyActivityRecent = true;

    switch (type) {
      case 'openWrite':
        {
          final key = cmd['key'] as String;
          final ext = cmd['ext'] as String;
          final truncate = cmd['truncate'] as bool? ?? true;
          final replyPort = cmd['replyPort'] as SendPort;
          final k = chanId(key, ext);
          final active = activeWriteIdForKey[k];

          if (active != null ||
              (pendingWritesForKey[k]?.isNotEmpty ?? false)) {
            pendingWritesForKey
                .putIfAbsent(k, () => <_QueuedWrite>[])
                .add(_QueuedWrite(
                  key: key,
                  ext: ext,
                  truncate: truncate,
                  replyPort: replyPort,
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
            );
            writes[writeId] = ctx;
            replyPort.send(<String, dynamic>{
              'writeId': writeId,
              'workerPort': cmdPort.sendPort,
            });
          } catch (e) {
            activeWriteIdForKey[k] = null;
            _sendError(replyPort, e);
          }
          scheduleIdleCheckIfNeeded();
        }
        break;

      case 'writeChunk':
        {
          final writeId = cmd['writeId'] as int;
          final ctx = writes[writeId];
          if (ctx == null) break;

          try {
            final chunk = _chunkFromMessage(cmd['chunk']);
            ctx.sink.add(chunk);
            if (chunk.length > 64 * 1024) {
              await ctx.sink.flush();
            }
            notifySubscribers(ctx.key, ctx.ext, chunk);
          } catch (e) {
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
          final ctx = writes.remove(writeId);
          if (ctx == null) {
            _sendError(replyPort, StateError('unknown writeId: $writeId'));
            break;
          }

          try {
            await completeWriteContext(ctx);
            endChannel(ctx.key, ctx.ext);
            _sendOk(replyPort);

            final k = chanId(ctx.key, ctx.ext);
            activeWriteIdForKey[k] = null;
            await processPendingDeletes(k);
            await startQueuedWrite(k);
          } catch (e) {
            await cleanupWriteContext(ctx);
            await failChannel(ctx.key, ctx.ext, e);
            _sendError(replyPort, e);
          }
          scheduleIdleCheckIfNeeded();
        }
        break;

      case 'writeAbort':
        {
          final writeId = cmd['writeId'] as int;
          final ctx = writes.remove(writeId);
          if (ctx == null) break;

          await cleanupWriteContext(ctx);
          await failChannel(ctx.key, ctx.ext, StateError('write aborted'));
          scheduleIdleCheckIfNeeded();
        }
        break;

      case 'delete':
        {
          final key = cmd['key'] as String;
          final ext = cmd['ext'] as String;
          final replyPort = cmd['replyPort'] as SendPort?;
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
                ));
            scheduleIdleCheckIfNeeded();
            break;
          }

          try {
            await deleteKey(key, ext);
            _sendOk(replyPort);
          } catch (e) {
            _sendError(replyPort, e);
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
