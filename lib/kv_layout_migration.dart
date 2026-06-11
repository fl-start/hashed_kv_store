import 'dart:convert';
import 'dart:io';

import 'kv_exceptions.dart';
import 'kv_layout_version.dart';

/// Ensures [rootDirPath] matches [kKvStoreLayoutVersion], wiping stale data when needed.
Future<void> ensureKvStoreLayout({
  required String rootDirPath,
  bool wipeOnLayoutMismatch = true,
}) async {
  if (await File(rootDirPath).exists()) {
    return;
  }

  final root = Directory(rootDirPath);
  if (!await root.exists()) {
    await root.create(recursive: true);
  }

  final metaFile = File(kvStoreMetaFilePath(rootDirPath));
  final storedVersion = await _readStoredLayoutVersion(metaFile);

  if (storedVersion == kKvStoreLayoutVersion) {
    return;
  }

  final needsWipe = storedVersion != null ||
      await _rootHasContentsBesidesMeta(rootDirPath, metaFile);

  if (!needsWipe) {
    await _writeLayoutMeta(metaFile);
    return;
  }

  if (!wipeOnLayoutMismatch) {
    throw KvLayoutMismatchException(
      rootDirPath: rootDirPath,
      storedVersion: storedVersion,
      expectedVersion: kKvStoreLayoutVersion,
    );
  }

  await _wipeRootContents(rootDirPath);
  await _writeLayoutMeta(metaFile);
}

Future<int?> _readStoredLayoutVersion(File metaFile) async {
  if (!await metaFile.exists()) return null;

  try {
    final decoded = jsonDecode(await metaFile.readAsString());
    if (decoded is! Map) return null;
    final version = decoded['layoutVersion'];
    if (version is int) return version;
    if (version is num) return version.toInt();
    return null;
  } catch (_) {
    return null;
  }
}

Future<bool> _rootHasContentsBesidesMeta(
  String rootDirPath,
  File metaFile,
) async {
  final root = Directory(rootDirPath);
  await for (final entity in root.list(followLinks: false)) {
    if (entity.path == metaFile.path) continue;
    return true;
  }
  return false;
}

Future<void> _wipeRootContents(String rootDirPath) async {
  final root = Directory(rootDirPath);
  if (!await root.exists()) return;

  await for (final entity in root.list(followLinks: false)) {
    if (entity is File) {
      await entity.delete();
    } else if (entity is Directory) {
      await entity.delete(recursive: true);
    }
  }
}

Future<void> _writeLayoutMeta(File metaFile) async {
  await metaFile.writeAsString(
    jsonEncode(<String, dynamic>{'layoutVersion': kKvStoreLayoutVersion}),
  );
}
