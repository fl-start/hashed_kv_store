import 'dart:io';

import 'package:hashed_kv_store/hashed_kv_store.dart';
import 'package:path/path.dart' as p;
import 'package:test/test.dart';

void main() {
  group('path layout via client.pathForKey', () {
    late Directory tempDir;
    late MultiIsolateKvStoreClient store;

    setUp(() async {
      tempDir = await Directory.systemTemp.createTemp('path_test_');
      store = await MultiIsolateKvStoreClient.spawn(
        rootDirPath: tempDir.path,
        numWriteWorkers: 1,
      );
    });

    tearDown(() async {
      await store.close();
      if (await tempDir.exists()) {
        await tempDir.delete(recursive: true);
      }
    });

    test('level 1 relative layout matches Crockford pattern', () {
      const key = 'layout:test:key';
      final path = store.pathForKey(key, extension: 'dat');
      final relative = p.relative(path, from: tempDir.path);
      expect(
        relative,
        matches(
          RegExp(
            r'^[0-9a-z]{2}[/\\][0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}\.dat$',
          ),
        ),
      );
      expect(relative, isNot(matches(RegExp(r'[ilo]'))));
    });

    test('same key and extension produce stable paths', () {
      const key = 'stable:key';
      final a = store.pathForKey(key, extension: 'bin');
      final b = store.pathForKey(key, extension: 'bin');
      expect(a, equals(b));
    });

    test('different extensions produce different paths', () {
      const key = 'same:key';
      final a = store.pathForKey(key, extension: 'a');
      final b = store.pathForKey(key, extension: 'b');
      expect(a, isNot(equals(b)));
    });
  });

  group('HashedKvPath.pathsForKey', () {
    test('folder and file paths share one digest', () {
      const root = '/data';
      const key = 'digest:test';
      final paths = HashedKvPath.pathsForKey(root, key, 'bin', 1);
      expect(paths.filePath, equals(HashedKvPath.pathForKey(root, key, 'bin', 1)));
      expect(
        paths.folderPath,
        equals(HashedKvPath.folderPathForKey(root, key, 1)),
      );
    });
  });

  group('folder hierarchy levels', () {
    test('level 0 places file directly under root', () async {
      final temp = await Directory.systemTemp.createTemp('path_h0_');
      try {
        final store = await MultiIsolateKvStoreClient.spawn(
          rootDirPath: temp.path,
          folderHierarchyLevels: 0,
          numWriteWorkers: 1,
        );
        final path = store.pathForKey('k', extension: 'bin');
        expect(p.dirname(path), equals(temp.path));
      } finally {
        await temp.delete(recursive: true);
      }
    });

    test('level 2 nests two folders', () async {
      final temp = await Directory.systemTemp.createTemp('path_h2_');
      try {
        final store = await MultiIsolateKvStoreClient.spawn(
          rootDirPath: temp.path,
          folderHierarchyLevels: 2,
          numWriteWorkers: 1,
        );
        final path = store.pathForKey('k', extension: 'bin');
        final relative = p.relative(path, from: temp.path);
        expect(
            relative.split(RegExp(r'[/\\]')).length, greaterThanOrEqualTo(3));
      } finally {
        await temp.delete(recursive: true);
      }
    });
  });
}
