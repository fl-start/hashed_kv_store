import 'dart:convert';
import 'dart:io';

import 'package:hashed_kv_store/hashed_kv_store.dart';

Future<void> main() async {
  final storageDir =
      await Directory.systemTemp.createTemp('hashed_kv_example_');
  stdout.writeln('Storage directory: ${storageDir.path}');

  final store = await MultiIsolateKvStoreClient.spawn(
    rootDirPath: storageDir.path,
    numWriteWorkers: 2,
  );

  try {
    const key = 'example:user:1';
    const ext = 'json';
    final payload = jsonEncode({'name': 'Ada', 'role': 'engineer'});

    await store.writeFromStream(
      key,
      Stream.value(utf8.encode(payload)),
      extension: ext,
    );

    stdout.writeln('exists: ${await store.exists(key, extension: ext)}');
    stdout.writeln('path: ${store.pathForKey(key, extension: ext)}');
    stdout.writeln('stored paths: ${await store.listStoredPaths()}');

    final bytes = <int>[];
    await for (final chunk in store.readStream(key, extension: ext)) {
      bytes.addAll(chunk);
    }
    stdout.writeln('read back: ${utf8.decode(bytes)}');
  } finally {
    await store.close();
    await storageDir.delete(recursive: true);
  }
}
