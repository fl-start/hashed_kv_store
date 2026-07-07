import 'dart:async';
import 'dart:io';

import 'package:path/path.dart' as p;

import 'hashed_kv_path.dart';
import 'kv_abort.dart';
import 'kv_exceptions.dart';

/// Caller-isolate disk I/O aligned with production path layout and write semantics.
class KvDirectIo {
  final String rootDirPath;
  final int folderHierarchyLevels;

  var _nextWriteId = 1;

  KvDirectIo({
    required this.rootDirPath,
    this.folderHierarchyLevels = 1,
  });

  String pathForKey(String key, {String extension = 'bin'}) {
    return HashedKvPath.pathForKey(
      rootDirPath,
      key,
      extension,
      folderHierarchyLevels,
    );
  }

  Future<void> writeFromStream(
    String key,
    Stream<List<int>> data, {
    String extension = 'bin',
    bool truncateExisting = true,
    KvAbortSignal? signal,
  }) async {
    signal?.throwIfAborted();

    final targetFile = File(pathForKey(key, extension: extension));
    await Directory(p.dirname(targetFile.path)).create(recursive: true);

    if (truncateExisting) {
      final writeId = _nextWriteId++;
      final tempFile = File('${targetFile.path}.$writeId.tmp');
      final sink = tempFile.openWrite();
      try {
        await _pumpStreamIntoSink(data, sink, signal: signal);
        await sink.flush();
        await sink.close();
        await _publishTempFile(tempFile, targetFile);
      } catch (e) {
        try {
          await sink.flush();
        } catch (_) {}
        try {
          await sink.close();
        } catch (_) {}
        if (await tempFile.exists()) {
          try {
            await tempFile.delete();
          } catch (_) {}
        }
        rethrow;
      }
    } else {
      final sink = targetFile.openWrite(mode: FileMode.append);
      try {
        await _pumpStreamIntoSink(data, sink, signal: signal);
        await sink.flush();
        await sink.close();
      } catch (e) {
        try {
          await sink.flush();
        } catch (_) {}
        try {
          await sink.close();
        } catch (_) {}
        rethrow;
      }
    }
  }

  Future<void> _pumpStreamIntoSink(
    Stream<List<int>> data,
    IOSink sink, {
    KvAbortSignal? signal,
  }) async {
    final input = StreamIterator<List<int>>(data);
    // Cancelling the iterator unblocks a pending moveNext (completes it with
    // false) so an abort mid-stream does not deadlock waiting for the producer.
    void onAbort() {
      unawaited(input.cancel());
    }

    signal?.onAbort(onAbort);
    try {
      while (true) {
        signal?.throwIfAborted();
        final hasNext = await input.moveNext();
        signal?.throwIfAborted();
        if (!hasNext) {
          return;
        }
        sink.add(input.current);
      }
    } finally {
      signal?.removeAbortListener(onAbort);
      await input.cancel();
    }
  }

  Future<void> _publishTempFile(File tempFile, File targetFile) async {
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

  Stream<List<int>> readStream(
    String key, {
    String extension = 'bin',
  }) async* {
    final file = File(pathForKey(key, extension: extension));
    if (!await file.exists()) {
      throw KvNotFoundException(key, extension);
    }
    yield* file.openRead();
  }

  Future<void> delete(String key, {String extension = 'bin'}) async {
    final file = File(pathForKey(key, extension: extension));
    if (await file.exists()) {
      await file.delete();
    }
  }
}
