import 'dart:async';
import 'dart:io';
import 'dart:isolate';

import 'hashed_kv_path.dart';
import 'kv_exceptions.dart';
import 'kv_trace.dart';
import 'kv_worker_isolate.dart';

typedef _Cmd = Map<String, dynamic>;

String _chanId(String key, String ext) => '$key::$ext';

/// Router isolate entry.
void kvRouterIsolateEntry(List<dynamic> args) async {
  final String rootDirPath = args[0] as String;
  final SendPort initPort = args[1] as SendPort;
  final int writeIdlePurgeMs = (args.length > 2 ? args[2] as int : 60000);
  final int numWriteWorkers = (args.length > 3 ? args[3] as int : 2);
  final int folderHierarchyLevels = (args.length > 4 ? args[4] as int : 1);
  final bool fsyncOnClose = (args.length > 5 ? args[5] as bool : false);
  final int flushThresholdBytes = (args.length > 6 ? args[6] as int : 65536);
  final int flushIntervalMs = (args.length > 7 ? args[7] as int : 100);

  int workerIndexForKey(String key) {
    final digest = HashedKvPath.crockfordBase32ForKey(key);
    var hash = 0;
    for (var i = 0; i < digest.length && i < 4; i++) {
      hash = (hash * 31 + digest.codeUnitAt(i)) & 0x7fffffff;
    }
    return hash % numWriteWorkers;
  }

  final writeWorkers = List<SendPort?>.filled(numWriteWorkers, null);
  final workerWaiters =
      List<List<Completer<void>>>.generate(numWriteWorkers, (_) => []);
  final liveSubscriptions = <String, List<SendPort>>{};
  final folderEnsuresInFlight = <String, Future<void>>{};

  SendPort? folderPort;
  var shuttingDown = false;

  Future<SendPort> getWorkerPort(int idx) async {
    final port = writeWorkers[idx];
    if (port != null) return port;

    final completer = Completer<void>();
    workerWaiters[idx].add(completer);
    await completer.future;
    return writeWorkers[idx]!;
  }

  void reregisterSubscriptions(int workerIdx) {
    final port = writeWorkers[workerIdx];
    if (port == null) return;

    for (final entry in liveSubscriptions.entries) {
      final parts = entry.key.split('::');
      if (parts.length != 2) continue;
      final key = parts[0];
      final ext = parts[1];
      if (workerIndexForKey(key) != workerIdx) continue;

      for (final subPort in entry.value) {
        port.send(<String, dynamic>{
          'type': 'subscribeLive',
          'key': key,
          'ext': ext,
          'subscriberPort': subPort,
        });
      }
    }
  }

  void notifyWorkerReady(int idx) {
    for (final waiter in workerWaiters[idx]) {
      if (!waiter.isCompleted) {
        waiter.complete();
      }
    }
    workerWaiters[idx].clear();
    reregisterSubscriptions(idx);
  }

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
        fsyncOnClose,
        flushThresholdBytes,
        flushIntervalMs,
      ],
    );
    writeWorkers[index] = await init.first as SendPort;
    notifyWorkerReady(index);

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
  folderPort = await folderInit.first as SendPort;

  Future<void> ensureFolderOnce(String folderPath) async {
    final folderReply = ReceivePort();
    folderPort!.send(<String, dynamic>{
      'type': 'ensureFolder',
      'folderPath': folderPath,
      'replyPort': folderReply.sendPort,
    });
    final folderResult = await folderReply.first as Map;
    folderReply.close();

    if (folderResult.containsKey('error')) {
      throw StateError(folderResult['error'] as String);
    }
  }

  Future<void> ensureFolder(String folderPath) async {
    final inFlight = folderEnsuresInFlight[folderPath];
    if (inFlight != null) {
      await inFlight;
      return;
    }

    final future = ensureFolderOnce(folderPath);
    folderEnsuresInFlight[folderPath] = future;
    try {
      await future;
    } finally {
      folderEnsuresInFlight.remove(folderPath);
    }
  }

  Future<void> handleOpenWrite(_Cmd cmd) async {
    final key = cmd['key'] as String;
    final ext = cmd['ext'] as String;
    final replyPort = cmd['replyPort'] as SendPort;
    final workerIdx = workerIndexForKey(key);
    final trace = KvTraceWrite.begin(key);
    trace?.event('router_open_write', {'ext': ext, 'worker': workerIdx});

    try {
      final workerPort = await getWorkerPort(workerIdx);
      final paths = HashedKvPath.pathsForKey(
        rootDirPath,
        key,
        ext,
        folderHierarchyLevels,
      );
      await ensureFolder(paths.folderPath);
      trace?.slow('router_ensure_folder', {'ext': ext, 'worker': workerIdx});

      cmd['filePath'] = paths.filePath;
      workerPort.send(cmd);
      trace?.eventWithMs('router_open_write_forwarded', {
        'ext': ext,
        'worker': workerIdx,
      });
    } catch (e, st) {
      trace?.eventWithMs('router_open_write_failed', {
        'ext': ext,
        'worker': workerIdx,
        'error': e.toString(),
      });
      kvSendError(replyPort, e, st);
    }
  }

  Future<void> handleSubscribeLive(_Cmd cmd) async {
    final key = cmd['key'] as String;
    final ext = cmd['ext'] as String;
    final subPort = cmd['subscriberPort'] as SendPort;
    final chanKey = _chanId(key, ext);

    liveSubscriptions.putIfAbsent(chanKey, () => <SendPort>[]).add(subPort);

    final workerIdx = workerIndexForKey(key);
    final workerPort = await getWorkerPort(workerIdx);
    workerPort.send(cmd);
  }

  Future<void> handleAbortWrite(_Cmd cmd) async {
    final key = cmd['key'] as String;
    final workerIdx = workerIndexForKey(key);
    try {
      final workerPort = await getWorkerPort(workerIdx);
      workerPort.send(cmd);
    } catch (e, st) {
      kvSendError(cmd['replyPort'] as SendPort?, e, st);
    }
  }

  Future<void> handleOpenRead(_Cmd cmd) async {
    final key = cmd['key'] as String;
    final ext = cmd['ext'] as String;
    final replyPort = cmd['replyPort'] as SendPort;
    final workerIdx = workerIndexForKey(key);

    try {
      final workerPort = await getWorkerPort(workerIdx);
      cmd['filePath'] = HashedKvPath.pathForKey(
        rootDirPath,
        key,
        ext,
        folderHierarchyLevels,
      );
      workerPort.send(cmd);
    } catch (e, st) {
      kvSendError(replyPort, e, st);
    }
  }

  Future<void> handleCancelRead(_Cmd cmd) async {
    final key = cmd['key'] as String;
    final workerIdx = workerIndexForKey(key);
    try {
      final workerPort = await getWorkerPort(workerIdx);
      workerPort.send(cmd);
    } catch (e, st) {
      kvSendError(cmd['replyPort'] as SendPort?, e, st);
    }
  }

  Future<void> handleDelete(_Cmd cmd) async {
    final key = cmd['key'] as String;
    final ext = cmd['ext'] as String;
    final workerIdx = workerIndexForKey(key);

    cmd['filePath'] = HashedKvPath.pathForKey(
      rootDirPath,
      key,
      ext,
      folderHierarchyLevels,
    );

    final workerPort = await getWorkerPort(workerIdx);
    workerPort.send(cmd);
  }

  Future<void> handleShutdown(SendPort replyPort) async {
    if (shuttingDown) {
      kvSendOk(replyPort);
      return;
    }
    shuttingDown = true;

    final acks = <Future<void>>[];

    for (final port in writeWorkers) {
      if (port == null) continue;
      final reply = ReceivePort();
      port.send(<String, dynamic>{
        'type': 'shutdown',
        'replyPort': reply.sendPort,
      });
      acks.add(reply.first.then((_) => reply.close()));
    }

    final folder = folderPort;
    if (folder != null) {
      final reply = ReceivePort();
      folder.send(<String, dynamic>{
        'type': 'shutdown',
        'replyPort': reply.sendPort,
      });
      acks.add(reply.first.then((_) => reply.close()));
    }

    await Future.wait(acks);
    kvSendOk(replyPort);
    Isolate.exit();
  }

  final routerPort = ReceivePort();
  initPort.send(routerPort.sendPort);

  routerPort.listen((raw) {
    final cmd = raw as _Cmd;
    final type = cmd['type'] as String;

    switch (type) {
      case 'openWrite':
        unawaited(handleOpenWrite(cmd));
        break;
      case 'subscribeLive':
        unawaited(handleSubscribeLive(cmd));
        break;
      case 'delete':
        unawaited(handleDelete(cmd));
        break;
      case 'abortWrite':
        unawaited(handleAbortWrite(cmd));
        break;
      case 'openRead':
        unawaited(handleOpenRead(cmd));
        break;
      case 'cancelRead':
        unawaited(handleCancelRead(cmd));
        break;
      case 'shutdown':
        unawaited(handleShutdown(cmd['replyPort'] as SendPort));
        break;
      default:
        break;
    }
  });
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
    final type = cmd['type'] as String;

    if (type == 'shutdown') {
      final replyPort = cmd['replyPort'] as SendPort?;
      kvSendOk(replyPort);
      Isolate.exit();
    }

    if (type != 'ensureFolder') continue;

    final replyPort = cmd['replyPort'] as SendPort;

    try {
      final folderPath = cmd.containsKey('folderPath')
          ? cmd['folderPath'] as String
          : HashedKvPath.folderPathForKey(
              rootDirPath,
              cmd['key'] as String,
              folderHierarchyLevels,
            );

      if (!createdFolders.contains(folderPath)) {
        await Directory(folderPath).create(recursive: true);
        createdFolders.add(folderPath);
      }

      kvSendOk(replyPort);
    } catch (e, st) {
      kvSendError(replyPort, e, st);
    }
  }
}
