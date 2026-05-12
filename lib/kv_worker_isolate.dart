import 'dart:async';
import 'dart:convert';
import 'dart:isolate';
import 'dart:io';
import 'dart:collection';

import 'package:crypto/crypto.dart';
import 'package:path/path.dart' as p;

typedef _Cmd = Map<String, dynamic>;

/// Internal core for hashed key -> file path mapping.
class _HashedKvCore {
  final Directory rootDir;
  final int folderHierarchyLevels;
  static const String _crockfordBase32Lower =
      '0123456789abcdefghjkmnpqrstvwxyz';

  _HashedKvCore(this.rootDir, this.folderHierarchyLevels);

  String _crockfordBase32ForKey(String key) {
    final digestBytes = sha256.convert(utf8.encode(key)).bytes;
    final out = StringBuffer();

    var bitBuffer = 0;
    var bitCount = 0;
    for (final byte in digestBytes) {
      bitBuffer = (bitBuffer << 8) | byte;
      bitCount += 8;
      while (bitCount >= 5) {
        final index = (bitBuffer >> (bitCount - 5)) & 0x1f;
        out.write(_crockfordBase32Lower[index]);
        bitCount -= 5;
      }
    }

    // Emit the remaining bits (if any) as the final Crockford character.
    if (bitCount > 0) {
      final index = (bitBuffer << (5 - bitCount)) & 0x1f;
      out.write(_crockfordBase32Lower[index]);
    }

    return out.toString();
  }

  String _relativePathForDigest(String digestBase32, String extension) {
    final stem = digestBase32.substring(4, 20);
    final fileStem =
        '${stem.substring(0, 4)}-${stem.substring(4, 8)}-${stem.substring(8, 12)}-${stem.substring(12, 16)}';
    final fileName =
        extension.isEmpty ? fileStem : '$fileStem.$extension'.replaceAll('..', '.');

    if (folderHierarchyLevels == 0) {
      return fileName;
    } else if (folderHierarchyLevels == 1) {
      final level1 = digestBase32.substring(0, 2);
      return p.join(level1, fileName);
    } else {
      // folderHierarchyLevels == 2
      final level1 = digestBase32.substring(0, 2);
      final level2 = digestBase32.substring(2, 4);
      return p.join(level1, level2, fileName);
    }
  }

  File fileFor(String key, String extension) {
    final digestBase32 = _crockfordBase32ForKey(key);
    final rel = _relativePathForDigest(digestBase32, extension);
    return File(p.join(rootDir.path, rel));
  }

  Future<void> delete(String key, String extension) async {
    final file = fileFor(key, extension);
    if (await file.exists()) {
      await file.delete();
    }
  }
}

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

class _QueuedDelete {
  final String key;
  final String ext;

  _QueuedDelete({required this.key, required this.ext});
}

/// Single writer isolate entry function.
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
  final core = _HashedKvCore(Directory(rootDirPath), folderHierarchyLevels);

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

            final file = core.fileFor(key, ext);
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

            final file = core.fileFor(next.key, next.ext);
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
          await core.delete(key, ext);
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

