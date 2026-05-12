import 'dart:isolate';
import 'dart:io';
import 'dart:convert';

import 'package:crypto/crypto.dart';
import 'package:path/path.dart' as p;

import 'kv_worker_isolate.dart';

typedef _Cmd = Map<String, dynamic>;

class _LiveSubscription {
  final String key;
  final String ext;
  final SendPort subscriberPort;

  _LiveSubscription({
    required this.key,
    required this.ext,
    required this.subscriberPort,
  });
}

/// Router isolate entry.
///
/// args:
/// - args[0]: String rootDirPath
/// - args[1]: SendPort initPort (for returning router's SendPort)
/// - args[2]: int writeIdlePurgeMs
/// - args[3]: int numWriteWorkers
/// - args[4]: int folderHierarchyLevels (default: 1)
void kvRouterIsolateEntry(List<dynamic> args) async {
  final String rootDirPath = args[0] as String;
  final SendPort initPort = args[1] as SendPort;
  final int writeIdlePurgeMs = (args.length > 2 ? args[2] as int : 60000);
  final int numWriteWorkers = (args.length > 3 ? args[3] as int : 2);
  final int folderHierarchyLevels = (args.length > 4 ? args[4] as int : 1);

  // Helper to compute stable worker index from key
  int workerIndexForKey(String key) {
    final hash = key.codeUnits.fold<int>(0, (a, b) => a + b);
    return hash % numWriteWorkers;
  }

  // ----------------- Spawn multiple write workers with per-key queuing -----------------
  final writeWorkers = <SendPort>[];
  for (var i = 0; i < numWriteWorkers; i++) {
    final init = ReceivePort();
    await Isolate.spawn(
      kvWriteWorkerIsolateEntry,
      [rootDirPath, init.sendPort, writeIdlePurgeMs, folderHierarchyLevels],
    );
    final workerPort = await init.first as SendPort;
    writeWorkers.add(workerPort);
  }

  // ----------------- Master folder isolate -----------------
  final folderInit = ReceivePort();
  await Isolate.spawn(
    kvFolderIsolateEntry,
    [rootDirPath, folderInit.sendPort, folderHierarchyLevels],
  );
  final folderPort = await folderInit.first as SendPort;

  // Track live subscriptions for replay on write worker restarts
  final liveSubscriptions = <_LiveSubscription>[];

  final routerPort = ReceivePort();
  initPort.send(routerPort.sendPort);

  await for (final raw in routerPort) {
    final cmd = raw as _Cmd;
    final type = cmd['type'] as String;

    switch (type) {
      // =========================================================
      //  OPEN WRITE (ensure folder, then route to write worker)
      // =========================================================
      case 'openWrite':
        {
          final key = cmd['key'] as String;
          final ext = cmd['ext'] as String;
          final workerIdx = workerIndexForKey(key);

          // Ask folder isolate to ensure directories exist
          final folderReply = ReceivePort();
          folderPort.send(<String, dynamic>{
            'type': 'ensureFolder',
            'key': key,
            'replyPort': folderReply.sendPort,
          });
          await folderReply.first;
          folderReply.close();

          // Route to appropriate write worker
          writeWorkers[workerIdx].send(cmd);
        }
        break;

      case 'writeChunk':
        {
          final key = cmd['key'] as String;
          final workerIdx = workerIndexForKey(key);
          writeWorkers[workerIdx].send(cmd);
        }
        break;

      case 'writeEnd':
        {
          final key = cmd['key'] as String;
          final workerIdx = workerIndexForKey(key);
          writeWorkers[workerIdx].send(cmd);
        }
        break;

      // =========================================================
      //  LIVE SUBSCRIBE (route to appropriate write worker)
      // =========================================================
      case 'subscribeLive':
        {
          final key = cmd['key'] as String;
          final ext = cmd['ext'] as String;
          final subscriberPort = cmd['subscriberPort'] as SendPort;
          final workerIdx = workerIndexForKey(key);

          liveSubscriptions.add(
            _LiveSubscription(
              key: key,
              ext: ext,
              subscriberPort: subscriberPort,
            ),
          );

          writeWorkers[workerIdx].send(cmd);
        }
        break;

      // =========================================================
      //  DELETE (route to appropriate write worker)
      // =========================================================
      case 'delete':
        {
          final key = cmd['key'] as String;
          final workerIdx = workerIndexForKey(key);
          writeWorkers[workerIdx].send(cmd);
        }
        break;

      default:
        break;
    }
  }
}

/// Master folder isolate: handles all directory creation operations.
///
/// args:
/// - args[0]: String rootDirPath
/// - args[1]: SendPort initPort
/// - args[2]: int folderHierarchyLevels (default: 1)
void kvFolderIsolateEntry(List<dynamic> args) async {
  final String rootDirPath = args[0] as String;
  final SendPort initPort = args[1] as SendPort;
  final int folderHierarchyLevels = (args.length > 2 ? args[2] as int : 1);
  final rootDir = Directory(rootDirPath);
  final createdFolders = <String>{};

  final cmdPort = ReceivePort();
  initPort.send(cmdPort.sendPort);

  await for (final raw in cmdPort) {
    final cmd = raw as _Cmd;
    final type = cmd['type'] as String;

    if (type == 'ensureFolder') {
      final key = cmd['key'] as String;
      final replyPort = cmd['replyPort'] as SendPort;

      // Compute folder path from key
      final digestBase32 = _computeBase32ForKey(key);
      String folderPath;

      if (folderHierarchyLevels == 0) {
        folderPath = rootDir.path;
      } else if (folderHierarchyLevels == 1) {
        final level1 = digestBase32.substring(0, 2);
        folderPath = p.join(rootDir.path, level1);
      } else {
        // folderHierarchyLevels == 2
        final level1 = digestBase32.substring(0, 2);
        final level2 = digestBase32.substring(2, 4);
        folderPath = p.join(rootDir.path, level1, level2);
      }

      // Only create if not already created in this isolate session
      if (!createdFolders.contains(folderPath)) {
        await Directory(folderPath).create(recursive: true);
        createdFolders.add(folderPath);
      }

      replyPort.send({'ok': true});
    }
  }
}

String _computeBase32ForKey(String key) {
  const String crockfordBase32Lower = '0123456789abcdefghjkmnpqrstvwxyz';
  final digestBytes = sha256.convert(utf8.encode(key)).bytes;
  final out = StringBuffer();

  var bitBuffer = 0;
  var bitCount = 0;
  for (final byte in digestBytes) {
    bitBuffer = (bitBuffer << 8) | byte;
    bitCount += 8;
    while (bitCount >= 5) {
      final index = (bitBuffer >> (bitCount - 5)) & 0x1f;
      out.write(crockfordBase32Lower[index]);
      bitCount -= 5;
    }
  }

  if (bitCount > 0) {
    final index = (bitBuffer << (5 - bitCount)) & 0x1f;
    out.write(crockfordBase32Lower[index]);
  }

  return out.toString();
}
