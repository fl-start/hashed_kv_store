import 'dart:async';
import 'dart:convert';
import 'dart:isolate';
import 'dart:io';

import 'package:crypto/crypto.dart';
import 'package:path/path.dart' as p;

typedef _Cmd = Map<String, dynamic>;

/// Internal core for hashed key -> file path mapping.
class _HashedKvCore {
  final Directory rootDir;

  _HashedKvCore(this.rootDir);

  String _hexForKey(String key) {
    final digest = sha256.convert(utf8.encode(key));
    return digest.toString(); // 64-hex SHA256
  }

  String _relativePathForDigest(String digest, String extension) {
    final level1 = digest.substring(0, 2);
    final level2 = digest.substring(2, 4);
    final fileName =
        extension.isEmpty ? digest : '$digest.$extension'.replaceAll('..', '.');
    return p.join(level1, level2, fileName);
  }

  File fileFor(String key, String extension) {
    final hex = _hexForKey(key);
    final rel = _relativePathForDigest(hex, extension);
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

/// Worker isolate entry function.
///
/// args:
/// - args[0] : String rootDirPath
/// - args[1] : SendPort initPort (router uses this to get our SendPort)
void kvWorkerIsolateEntry(List<dynamic> args) async {
  final String rootDirPath = args[0] as String;
  final SendPort initPort = args[1] as SendPort;
  final core = _HashedKvCore(Directory(rootDirPath));

  final cmdPort = ReceivePort();

  // Let the parent (router) know how to talk to this worker.
  initPort.send(cmdPort.sendPort);

  // Active write contexts: writeId -> context.
  final writes = <int, _WriteContext>{};

  // Live subscribers: (key::ext) -> list of SendPorts.
  final subscribers = <String, List<SendPort>>{};

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
    // Use null as "done" sentinel.
    for (final sp in subs) {
      sp.send(null);
    }
  }

  await for (final raw in cmdPort) {
    final cmd = raw as _Cmd;
    final type = cmd['type'] as String;

    switch (type) {
      // ----------------------------------------------------
      // Write lifecycle
      // ----------------------------------------------------
      case 'writeStart':
        {
          final key = cmd['key'] as String;
          final ext = cmd['ext'] as String;
          final truncate = cmd['truncate'] as bool? ?? true;
          final writeId = cmd['writeId'] as int;
          final file = core.fileFor(key, ext);
          await file.parent.create(recursive: true);
          final mode = truncate ? FileMode.write : FileMode.append;
          final sink = file.openWrite(mode: mode);
          writes[writeId] = _WriteContext(sink, key, ext);
        }
        break;

      case 'writeChunk':
        {
          final writeId = cmd['writeId'] as int;
          final chunk = (cmd['chunk'] as List).cast<int>();
          final ctx = writes[writeId];
          if (ctx == null) break; // unknown or already closed
          ctx.sink.add(chunk);
          // Periodically flush for large files to prevent excessive buffering
          // IOSink auto-flushes, but explicit flush helps with very large files
          if (chunk.length > 64 * 1024) {
            // Flush immediately for large chunks (>64KB)
            await ctx.sink.flush();
          }
          // After writing to disk, notify live subscribers.
          notifySubscribers(ctx.key, ctx.ext, chunk);
        }
        break;

      case 'writeEnd':
        {
          final writeId = cmd['writeId'] as int;
          final ctx = writes.remove(writeId);
          if (ctx != null) {
            // Ensure all buffered data is written before closing
            await ctx.sink.flush();
            await ctx.sink.close();
            endChannel(ctx.key, ctx.ext);
          }
        }
        break;

      // ----------------------------------------------------
      // Live subscription
      // ----------------------------------------------------
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

      // ----------------------------------------------------
      // Delete
      // ----------------------------------------------------
      case 'delete':
        {
          final key = cmd['key'] as String;
          final ext = cmd['ext'] as String;
          await core.delete(key, ext);
        }
        break;

      // ----------------------------------------------------
      // Streaming read
      // ----------------------------------------------------
      case 'readStream':
        {
          final key = cmd['key'] as String;
          final ext = cmd['ext'] as String;
          final replyPort = cmd['replyPort'] as SendPort;
          final file = core.fileFor(key, ext);
          if (!await file.exists()) {
            replyPort.send(<String, dynamic>{'error': 'not_found'});
            replyPort.send(null); // end-of-stream sentinel
            break;
          }

          final stream = file.openRead();
          await for (final chunk in stream) {
            replyPort.send(chunk);
          }
          replyPort.send(null); // end-of-stream sentinel
        }
        break;

      default:
        // Unknown command; ignore or log.
        break;
    }
  }
}

