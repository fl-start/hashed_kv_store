import 'dart:async';
import 'dart:io';

import 'package:path/path.dart' as p;

import 'hashed_kv_path.dart';
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
  }) async {
    final targetFile = File(pathForKey(key, extension: extension));
    await Directory(p.dirname(targetFile.path)).create(recursive: true);

    if (truncateExisting) {
      final writeId = _nextWriteId++;
      final tempFile = File('${targetFile.path}.$writeId.tmp');
      final sink = tempFile.openWrite();
      await for (final chunk in data) {
        sink.add(chunk);
      }
      await sink.flush();
      await sink.close();
      await _publishTempFile(tempFile, targetFile);
    } else {
      final sink = targetFile.openWrite(mode: FileMode.append);
      await for (final chunk in data) {
        sink.add(chunk);
      }
      await sink.flush();
      await sink.close();
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
