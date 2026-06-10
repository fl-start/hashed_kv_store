import 'dart:async';
import 'dart:isolate';
import 'dart:io';

import 'hashed_kv_path.dart';

typedef _Cmd = Map<String, dynamic>;

class _WriteContext {
  final IOSink sink;
  final String key;
  final String ext;

  _WriteContext(this.sink, this.key, this.ext);
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

/// Write worker isolate entry function.
///
/// Handles writes for a shard of keys with per-key write queuing.
/// Multiple write workers can run in parallel, sharded by key hash.
///
/// args:
/// - args[0] : String rootDirPath
/// - args[1] : SendPort initPort (router uses this to get our SendPort)
/// - args[2] : int idlePurgeMs
/// - args[3] : int folderHierarchyLevels (default: 1)
void kvWriteWorkerIsolateEntry(List<dynamic> args) async {
  final String rootDirPath = args[0] as String;
  final SendPort initPort = args[1] as SendPort;
  final int idlePurgeMs = (args.length > 2 ? args[2] as int : 60000);
  final int folderHierarchyLevels = (args.length > 3 ? args[3] as int : 1);

  final cmdPort = ReceivePort();
  initPort.send(cmdPort.sendPort);

  // Per-key subscribers: (key::ext) -> list of SendPorts
  final subscribers = <String, List<SendPort>>{};

  // Per-key state
  // keyId -> currently active writeId
  final activeWriteIdForKey = <String, int?>{};

  // keyId -> queue of pending writes
  final pendingWritesForKey = <String, List<_QueuedWrite>>{};

  // writeId -> active write context
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
    final id = chanId(key, ext);
    final subs = subscribers[id];
    if (subs == null) return;
    for (final sp in subs) {
      sp.send(chunk);
    }
  }

  void endChannel(String key, String ext) {
    final id = chanId(key, ext);
    final subs = subscribers.remove(id);
    if (subs == null) return;
    for (final sp in subs) {
      sp.send(null);
    }
  }

  void cancelIdleTimer() {
    idleTimer?.cancel();
    idleTimer = null;
  }

  void scheduleIdleCheckIfNeeded() {
    final isActive = activeWriteIdForKey.values.any((id) => id != null) ||
        pendingWritesForKey.values.any((queue) => queue.isNotEmpty);

    if (!isActive && !anyActivityRecent) {
      cancelIdleTimer();
      idleTimer = Timer(Duration(milliseconds: idlePurgeMs), () {
        final stillActive = activeWriteIdForKey.values.any((id) => id != null) ||
            pendingWritesForKey.values.any((queue) => queue.isNotEmpty);
        if (!stillActive) {
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

          if (active == null) {
            // No active write for this key: start immediately
            final writeId = nextWriteId++;
            activeWriteIdForKey[k] = writeId;

            final file = fileFor(key, ext);
            final mode = truncate ? FileMode.write : FileMode.append;
            final sink = file.openWrite(mode: mode);
            writes[writeId] = _WriteContext(sink, key, ext);

            replyPort.send(<String, dynamic>{'writeId': writeId});
          } else {
            // Active write exists: enqueue this one
            final queue =
                pendingWritesForKey.putIfAbsent(k, () => <_QueuedWrite>[]);
            queue.add(_QueuedWrite(
              key: key,
              ext: ext,
              truncate: truncate,
              replyPort: replyPort,
            ));
          }
          scheduleIdleCheckIfNeeded();
        }
        break;

      case 'writeChunk':
        {
          final writeId = cmd['writeId'] as int;
          final chunk = (cmd['chunk'] as List).cast<int>();
          final ctx = writes[writeId];
          if (ctx == null) break;

          ctx.sink.add(chunk);
          if (chunk.length > 64 * 1024) {
            await ctx.sink.flush();
          }
          notifySubscribers(ctx.key, ctx.ext, chunk);
        }
        break;

      case 'writeEnd':
        {
          final writeId = cmd['writeId'] as int;
          final replyPort = cmd['replyPort'] as SendPort?;
          final ctx = writes.remove(writeId);
          if (ctx == null) break;

          await ctx.sink.flush();
          await ctx.sink.close();
          endChannel(ctx.key, ctx.ext);
          replyPort?.send(<String, dynamic>{'ok': true});

          // Free this key and start next queued write if any
          final k = chanId(ctx.key, ctx.ext);
          activeWriteIdForKey[k] = null;

          final queue = pendingWritesForKey[k];
          if (queue != null && queue.isNotEmpty) {
            final next = queue.removeAt(0);
            final newWriteId = nextWriteId++;
            activeWriteIdForKey[k] = newWriteId;

            final file = fileFor(next.key, next.ext);
            final mode = next.truncate ? FileMode.write : FileMode.append;
            final sink = file.openWrite(mode: mode);
            writes[newWriteId] = _WriteContext(sink, next.key, next.ext);

            next.replyPort.send(<String, dynamic>{'writeId': newWriteId});

            if (queue.isEmpty) {
              pendingWritesForKey.remove(k);
            }
          }
          scheduleIdleCheckIfNeeded();
        }
        break;

      case 'delete':
        {
          final key = cmd['key'] as String;
          final ext = cmd['ext'] as String;
          final replyPort = cmd['replyPort'] as SendPort?;
          await deleteKey(key, ext);
          replyPort?.send(<String, dynamic>{'ok': true});
          scheduleIdleCheckIfNeeded();
        }
        break;

      case 'subscribeLive':
        {
          final key = cmd['key'] as String;
          final ext = cmd['ext'] as String;
          final subPort = cmd['subscriberPort'] as SendPort;
          final id = chanId(key, ext);
          final list = subscribers.putIfAbsent(id, () => <SendPort>[]);
          list.add(subPort);
        }
        break;

      default:
        break;
    }
  }
}
