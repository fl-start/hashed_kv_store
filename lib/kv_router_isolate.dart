import 'dart:io';
import 'dart:isolate';

import 'hashed_kv_path.dart';

import 'kv_worker_isolate.dart';

typedef _Cmd = Map<String, dynamic>;

void _sendError(SendPort? port, Object error) {
  port?.send(<String, dynamic>{'error': error.toString()});
}

/// Router isolate entry.
void kvRouterIsolateEntry(List<dynamic> args) async {
  final String rootDirPath = args[0] as String;
  final SendPort initPort = args[1] as SendPort;
  final int writeIdlePurgeMs = (args.length > 2 ? args[2] as int : 60000);
  final int numWriteWorkers = (args.length > 3 ? args[3] as int : 2);
  final int folderHierarchyLevels = (args.length > 4 ? args[4] as int : 1);

  int workerIndexForKey(String key) {
    final digest = HashedKvPath.crockfordBase32ForKey(key);
    var hash = 0;
    for (var i = 0; i < digest.length && i < 4; i++) {
      hash = (hash * 31 + digest.codeUnitAt(i)) & 0x7fffffff;
    }
    return hash % numWriteWorkers;
  }

  final writeWorkers = List<SendPort?>.filled(numWriteWorkers, null);

  Future<void> spawnWorker(int index) async {
    final init = ReceivePort();
    final workerControlPort = ReceivePort();
    await Isolate.spawn(
      kvWriteWorkerIsolateEntry,
      [
        rootDirPath,
        init.sendPort,
        writeIdlePurgeMs,
        folderHierarchyLevels,
        index,
        workerControlPort.sendPort,
      ],
    );
    writeWorkers[index] = await init.first as SendPort;

    workerControlPort.listen((raw) async {
      if (raw is! _Cmd) return;
      if (raw['type'] == 'workerExiting' && raw['index'] == index) {
        writeWorkers[index] = null;
        workerControlPort.close();
        await spawnWorker(index);
      }
    });
  }

  for (var i = 0; i < numWriteWorkers; i++) {
    await spawnWorker(i);
  }

  final folderInit = ReceivePort();
  await Isolate.spawn(
    kvFolderIsolateEntry,
    [rootDirPath, folderInit.sendPort, folderHierarchyLevels],
  );
  final folderPort = await folderInit.first as SendPort;

  final routerPort = ReceivePort();
  initPort.send(routerPort.sendPort);

  await for (final raw in routerPort) {
    final cmd = raw as _Cmd;
    final type = cmd['type'] as String;

    switch (type) {
      case 'openWrite':
        {
          final key = cmd['key'] as String;
          final replyPort = cmd['replyPort'] as SendPort;
          final workerIdx = workerIndexForKey(key);
          final workerPort = writeWorkers[workerIdx];
          if (workerPort == null) {
            _sendError(replyPort, StateError('write worker unavailable'));
            break;
          }

          final folderReply = ReceivePort();
          folderPort.send(<String, dynamic>{
            'type': 'ensureFolder',
            'key': key,
            'replyPort': folderReply.sendPort,
          });
          final folderResult = await folderReply.first as Map;
          folderReply.close();

          if (folderResult.containsKey('error')) {
            _sendError(replyPort, folderResult['error'] as Object);
            break;
          }

          workerPort.send(cmd);
        }
        break;

      case 'subscribeLive':
        {
          final key = cmd['key'] as String;
          final workerIdx = workerIndexForKey(key);
          writeWorkers[workerIdx]?.send(cmd);
        }
        break;

      case 'delete':
        {
          final key = cmd['key'] as String;
          final workerIdx = workerIndexForKey(key);
          writeWorkers[workerIdx]?.send(cmd);
        }
        break;

      default:
        break;
    }
  }
}

/// Master folder isolate: handles all directory creation operations.
void kvFolderIsolateEntry(List<dynamic> args) async {
  final String rootDirPath = args[0] as String;
  final SendPort initPort = args[1] as SendPort;
  final int folderHierarchyLevels = (args.length > 2 ? args[2] as int : 1);
  final createdFolders = <String>{};

  final cmdPort = ReceivePort();
  initPort.send(cmdPort.sendPort);

  await for (final raw in cmdPort) {
    final cmd = raw as _Cmd;
    if (cmd['type'] != 'ensureFolder') continue;

    final key = cmd['key'] as String;
    final replyPort = cmd['replyPort'] as SendPort;

    try {
      final folderPath = HashedKvPath.folderPathForKey(
        rootDirPath,
        key,
        folderHierarchyLevels,
      );

      if (!createdFolders.contains(folderPath)) {
        await Directory(folderPath).create(recursive: true);
        createdFolders.add(folderPath);
      }

      replyPort.send(<String, dynamic>{'ok': true});
    } catch (e) {
      replyPort.send(<String, dynamic>{'error': e.toString()});
    }
  }
}
