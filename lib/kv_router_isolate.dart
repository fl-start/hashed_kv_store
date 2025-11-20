import 'dart:isolate';

import 'kv_worker_isolate.dart';

typedef _Cmd = Map<String, dynamic>;

class _PendingWrite {
  final String key;
  final String extension;
  final bool truncate;
  final SendPort replyPort;
  final int workerIndex;

  _PendingWrite({
    required this.key,
    required this.extension,
    required this.truncate,
    required this.replyPort,
    required this.workerIndex,
  });
}

/// Router isolate entry.
///
/// args:
/// - args[0]: String rootDirPath
/// - args[1]: int numWorkers
/// - args[2]: SendPort initPort (for returning router's SendPort)
void kvRouterIsolateEntry(List<dynamic> args) async {
  final String rootDirPath = args[0] as String;
  final int numWorkers = args[1] as int;
  final SendPort initPort = args[2] as SendPort;

  // ----------------- Spawn worker isolates -----------------
  final workers = <SendPort>[];
  for (var i = 0; i < numWorkers; i++) {
    final init = ReceivePort();
    await Isolate.spawn(
      kvWorkerIsolateEntry,
      [rootDirPath, init.sendPort],
    );
    final workerPort = await init.first as SendPort;
    workers.add(workerPort);
  }

  // ----------------- Routing / sharding helpers -----------------
  int workerIndexForKey(String key) {
    // Simple stable hash -> worker index. Replace with better hash if needed.
    final hash = key.codeUnits.fold<int>(0, (a, b) => a + b);
    return hash % workers.length;
  }

  String keyId(String key, String ext) => '$key::$ext';

  // ----------------- Per-key write queue state -----------------
  // keyId -> currently active writeId (or null)
  final activeWriteIdForKey = <String, int?>{};

  // keyId -> queue of pending writes
  final pendingWritesForKey = <String, List<_PendingWrite>>{};

  // Router global writeId generator
  var nextWriteId = 1;

  int allocWriteId() => nextWriteId++;

  // ----------------- Read routing state (round-robin) -----------------
  var readRR = 0;

  final routerPort = ReceivePort();
  initPort.send(routerPort.sendPort);

  await for (final raw in routerPort) {
    final cmd = raw as _Cmd;
    final type = cmd['type'] as String;

    switch (type) {
      // =========================================================
      //  OPEN WRITE (queue per key)
      // =========================================================
      case 'openWrite':
        {
          final key = cmd['key'] as String;
          final ext = cmd['ext'] as String;
          final truncate = cmd['truncate'] as bool? ?? true;
          final replyPort = cmd['replyPort'] as SendPort;
          final workerIdx = workerIndexForKey(key);
          final k = keyId(key, ext);
          final active = activeWriteIdForKey[k];

          if (active == null) {
            // No active write: allocate and start immediately.
            final writeId = allocWriteId();
            activeWriteIdForKey[k] = writeId;

            workers[workerIdx].send(<String, dynamic>{
              'type': 'writeStart',
              'key': key,
              'ext': ext,
              'truncate': truncate,
              'writeId': writeId,
            });

            // ACK client with writeId (and workerIndex if needed).
            replyPort.send(<String, dynamic>{
              'writeId': writeId,
              'workerIndex': workerIdx,
            });
          } else {
            // There is an active write: enqueue this one.
            final queue =
                pendingWritesForKey.putIfAbsent(k, () => <_PendingWrite>[]);
            queue.add(_PendingWrite(
              key: key,
              extension: ext,
              truncate: truncate,
              replyPort: replyPort,
              workerIndex: workerIdx,
            ));
          }
        }
        break;

      // =========================================================
      //  WRITE CHUNK / END (only active write per key)
      // =========================================================
      case 'writeChunk':
        {
          final key = cmd['key'] as String;
          final ext = cmd['ext'] as String;
          final writeId = cmd['writeId'] as int;
          final chunk = cmd['chunk'] as List;
          final k = keyId(key, ext);
          final active = activeWriteIdForKey[k];

          // Ignore non-active writes for this key.
          if (active != writeId) break;

          final workerIdx = workerIndexForKey(key);
          workers[workerIdx].send(<String, dynamic>{
            'type': 'writeChunk',
            'writeId': writeId,
            'chunk': chunk,
          });
        }
        break;

      case 'writeEnd':
        {
          final key = cmd['key'] as String;
          final ext = cmd['ext'] as String;
          final writeId = cmd['writeId'] as int;
          final k = keyId(key, ext);
          final active = activeWriteIdForKey[k];

          if (active != writeId) break;

          final workerIdx = workerIndexForKey(key);
          workers[workerIdx].send(<String, dynamic>{
            'type': 'writeEnd',
            'writeId': writeId,
          });

          // Free this key, then start next queued write if any.
          activeWriteIdForKey[k] = null;

          final queue = pendingWritesForKey[k];
          if (queue != null && queue.isNotEmpty) {
            final next = queue.removeAt(0);
            final newWriteId = allocWriteId();
            activeWriteIdForKey[k] = newWriteId;

            workers[next.workerIndex].send(<String, dynamic>{
              'type': 'writeStart',
              'key': next.key,
              'ext': next.extension,
              'truncate': next.truncate,
              'writeId': newWriteId,
            });

            // ACK next writer so it can now send chunks.
            next.replyPort.send(<String, dynamic>{
              'writeId': newWriteId,
              'workerIndex': next.workerIndex,
            });

            if (queue.isEmpty) {
              pendingWritesForKey.remove(k);
            }
          }
        }
        break;

      // =========================================================
      //  LIVE SUBSCRIBE (sharded by key)
      // =========================================================
      case 'subscribeLive':
        {
          final key = cmd['key'] as String;
          final ext = cmd['ext'] as String;
          final subscriberPort = cmd['subscriberPort'] as SendPort;
          final workerIdx = workerIndexForKey(key);

          workers[workerIdx].send(<String, dynamic>{
            'type': 'subscribeLive',
            'key': key,
            'ext': ext,
            'subscriberPort': subscriberPort,
          });
        }
        break;

      // =========================================================
      //  READ STREAM (can go to any worker)
      // =========================================================
      case 'readStream':
        {
          final key = cmd['key'] as String;
          final ext = cmd['ext'] as String;
          final replyPort = cmd['replyPort'] as SendPort;

          // Choose worker round-robin.
          final idx = readRR % workers.length;
          readRR = (readRR + 1);

          workers[idx].send(<String, dynamic>{
            'type': 'readStream',
            'key': key,
            'ext': ext,
            'replyPort': replyPort,
          });
        }
        break;

      // =========================================================
      //  DELETE (sharded by key, no queueing)
      // =========================================================
      case 'delete':
        {
          final key = cmd['key'] as String;
          final ext = cmd['ext'] as String;
          final workerIdx = workerIndexForKey(key);

          workers[workerIdx].send(<String, dynamic>{
            'type': 'delete',
            'key': key,
            'ext': ext,
          });
        }
        break;

      default:
        // Unknown command; ignore or log.
        break;
    }
  }
}
