import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:hashed_kv_store/hashed_kv_store.dart';
import 'package:path/path.dart' as p;
import 'package:test/test.dart';

void unawaited(Future<void> future) {
  // Helper to avoid unawaited warnings in tests
}

void main() {
  late Directory tempDir;
  late MultiIsolateKvStoreClient store;

  setUp(() async {
    tempDir = await Directory.systemTemp.createTemp('hashed_kv_test_');
    store = await MultiIsolateKvStoreClient.spawn(
      rootDirPath: tempDir.path,
      numWriteWorkers: 2,
    );
  });

  tearDown(() async {
    // Give isolates time to finish
    await Future.delayed(const Duration(milliseconds: 200));
    if (await tempDir.exists()) {
      try {
        await tempDir.delete(recursive: true);
      } catch (e) {
        // Ignore cleanup errors
      }
    }
  });

  test('write and read stream', () async {
    const key = 'test:key:1';
    const ext = 'txt';
    const content = 'Hello, World!';

    // Write
    final writeStream = Stream<List<int>>.fromIterable([
      utf8.encode(content),
    ]);
    await store.writeFromStream(key, writeStream, extension: ext);

    // Read
    final readStream = store.readStream(key, extension: ext);
    final bytes = <int>[];
    await for (final chunk in readStream) {
      bytes.addAll(chunk);
    }

    expect(utf8.decode(bytes), equals(content));
  });

  test('read non-existent key throws error', () async {
    const key = 'non:existent';
    const ext = 'bin';

    final readStream = store.readStream(key, extension: ext);
    expect(
      () async => await readStream.forEach((_) {}),
      throwsA(isA<KvNotFoundException>()),
    );
  });

  test('live subscription receives chunks as written', () async {
    const key = 'live:test';
    const ext = 'log';

    final receivedChunks = <List<int>>[];
    final subscription = store.subscribeLive(key, extension: ext).listen(
      (chunk) {
        receivedChunks.add(chunk);
      },
    );

    // Wait a bit for subscription to register
    await Future.delayed(const Duration(milliseconds: 100));

    // Write chunks
    final controller = StreamController<List<int>>();
    final writeFuture = store.writeFromStream(
      key,
      controller.stream,
      extension: ext,
    );

    controller.add(utf8.encode('chunk1\n'));
    await Future.delayed(const Duration(milliseconds: 50));
    controller.add(utf8.encode('chunk2\n'));
    await Future.delayed(const Duration(milliseconds: 50));
    controller.add(utf8.encode('chunk3\n'));

    await controller.close();
    await writeFuture;

    // Wait for subscription to receive all chunks
    await Future.delayed(const Duration(milliseconds: 200));

    expect(receivedChunks.length, greaterThan(0));
    final allBytes = receivedChunks.expand((chunk) => chunk).toList();
    final content = utf8.decode(allBytes);
    expect(content, contains('chunk1'));
    expect(content, contains('chunk2'));
    expect(content, contains('chunk3'));

    await subscription.cancel();
  });

  test('writes for same key are queued', () async {
    const key = 'queued:test';
    const ext = 'txt';

    final write1Complete = Completer<void>();
    final write2Complete = Completer<void>();

    // Start first write
    final controller1 = StreamController<List<int>>();
    unawaited(
      store.writeFromStream(key, controller1.stream, extension: ext).then(
            (_) => write1Complete.complete(),
          ),
    );

    // Wait a bit for first write to start
    await Future.delayed(const Duration(milliseconds: 50));

    // Start second write (should be queued)
    final controller2 = StreamController<List<int>>();
    unawaited(
      store.writeFromStream(key, controller2.stream, extension: ext).then(
            (_) => write2Complete.complete(),
          ),
    );

    // Send data to first write
    controller1.add(utf8.encode('write1\n'));
    await Future.delayed(const Duration(milliseconds: 100));
    controller1.add(utf8.encode('write1_end\n'));
    await controller1.close();

    // Wait for first write to complete
    await write1Complete.future;
    await Future.delayed(const Duration(milliseconds: 100));

    // Now send data to second write (should now be active)
    controller2.add(utf8.encode('write2\n'));
    await Future.delayed(const Duration(milliseconds: 100));
    controller2.add(utf8.encode('write2_end\n'));
    await controller2.close();

    await write2Complete.future;
    await Future.delayed(const Duration(milliseconds: 100));

    // Read back - second write should have overwritten first (truncateExisting=true by default)
    final readStream = store.readStream(key, extension: ext);
    final bytes = <int>[];
    await for (final chunk in readStream) {
      bytes.addAll(chunk);
    }

    final content = utf8.decode(bytes);
    // Since truncateExisting=true, only write2 should be present
    expect(content, contains('write2'));
    expect(content, isNot(contains('write1')));
  });

  test('delete removes key', () async {
    const key = 'delete:test';
    const ext = 'bin';

    // Write
    await store.writeFromStream(
      key,
      Stream.value([1, 2, 3, 4, 5]),
      extension: ext,
    );

    // Wait for write to complete
    await Future.delayed(const Duration(milliseconds: 100));

    // Delete
    await store.delete(key, extension: ext);

    // Read should fail
    final readStream = store.readStream(key, extension: ext);
    expect(
      () async => await readStream.forEach((_) {}),
      throwsA(isA<KvNotFoundException>()),
    );
  });

  test('different extensions for same key are separate', () async {
    const key = 'same:key';
    const content1 = 'content1';
    const content2 = 'content2';

    await store.writeFromStream(
      key,
      Stream.value(utf8.encode(content1)),
      extension: 'ext1',
    );

    await Future.delayed(const Duration(milliseconds: 100));

    await store.writeFromStream(
      key,
      Stream.value(utf8.encode(content2)),
      extension: 'ext2',
    );

    await Future.delayed(const Duration(milliseconds: 100));

    final read1 = store.readStream(key, extension: 'ext1');
    final bytes1 = <int>[];
    await for (final chunk in read1) {
      bytes1.addAll(chunk);
    }
    expect(utf8.decode(bytes1), equals(content1));

    final read2 = store.readStream(key, extension: 'ext2');
    final bytes2 = <int>[];
    await for (final chunk in read2) {
      bytes2.addAll(chunk);
    }
    expect(utf8.decode(bytes2), equals(content2));
  });

  test('storage path uses lowercase Crockford Base32 layout', () async {
    const key = 'layout:test:key';
    const ext = 'dat';

    await store.writeFromStream(
      key,
      Stream.value(utf8.encode('payload')),
      extension: ext,
    );

    await Future.delayed(const Duration(milliseconds: 150));

    final allEntries = tempDir
        .listSync(recursive: true)
        .whereType<File>()
        .map((f) => f.path)
        .toList();

    expect(allEntries, hasLength(1));

    final relative = p.relative(allEntries.first, from: tempDir.path);
    expect(
      relative,
      matches(
        RegExp(
          r'^[0-9a-z]{2}[/\\][0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}\.dat$',
        ),
      ),
      reason:
          'Path should use default 1-level folder hierarchy with Crockford Base32',
    );
    expect(relative, isNot(matches(RegExp(r'[ilo]'))));
  });

  test('truncate write keeps old content visible until complete', () async {
    const key = 'atomic:replace';
    const ext = 'txt';

    await store.writeFromStream(
      key,
      Stream.value(utf8.encode('version1')),
      extension: ext,
    );

    final controller = StreamController<List<int>>();
    final writeFuture = store.writeFromStream(
      key,
      controller.stream,
      extension: ext,
    );

    await Future.delayed(const Duration(milliseconds: 80));

    final midBytes = <int>[];
    await for (final chunk in store.readStream(key, extension: ext)) {
      midBytes.addAll(chunk);
    }
    expect(utf8.decode(midBytes), equals('version1'));

    controller.add(utf8.encode('version2'));
    await controller.close();
    await writeFuture;

    final finalBytes = <int>[];
    await for (final chunk in store.readStream(key, extension: ext)) {
      finalBytes.addAll(chunk);
    }
    expect(utf8.decode(finalBytes), equals('version2'));
  });

  test('append mode preserves existing content', () async {
    const key = 'append:test';
    const ext = 'txt';

    await store.writeFromStream(
      key,
      Stream.value(utf8.encode('part1')),
      extension: ext,
    );

    await store.writeFromStream(
      key,
      Stream.value(utf8.encode('part2')),
      extension: ext,
      truncateExisting: false,
    );

    final bytes = <int>[];
    await for (final chunk in store.readStream(key, extension: ext)) {
      bytes.addAll(chunk);
    }
    expect(utf8.decode(bytes), equals('part1part2'));
  });

  test('delete waits for active write to finish', () async {
    const key = 'delete:during:write';
    const ext = 'txt';

    await store.writeFromStream(
      key,
      Stream.value(utf8.encode('keep-until-delete')),
      extension: ext,
    );

    final controller = StreamController<List<int>>();
    final writeFuture = store.writeFromStream(
      key,
      controller.stream,
      extension: ext,
    );

    await Future.delayed(const Duration(milliseconds: 50));

    final deleteFuture = store.delete(key, extension: ext);
    controller.add(utf8.encode('should-not-appear'));
    await controller.close();
    await writeFuture;
    await deleteFuture;

    final readStream = store.readStream(key, extension: ext);
    expect(
      () async => await readStream.forEach((_) {}),
      throwsA(isA<KvNotFoundException>()),
    );
  });

  test('different keys can be written concurrently across workers', () async {
    const key1 = 'concurrent:key:alpha';
    const key2 = 'concurrent:key:beta';
    const ext = 'txt';

    final firstController = StreamController<List<int>>();
    final secondController = StreamController<List<int>>();

    var secondWriteCompleted = false;

    final firstWrite =
        store.writeFromStream(key1, firstController.stream, extension: ext);

    await Future.delayed(const Duration(milliseconds: 60));

    final secondWrite =
        store.writeFromStream(key2, secondController.stream, extension: ext);
    secondWrite.whenComplete(() {
      secondWriteCompleted = true;
    });

    secondController.add(utf8.encode('second\n'));
    await secondController.close();
    await secondWrite;

    // Different keys may complete independently while another write is active.
    expect(secondWriteCompleted, isTrue);

    firstController.add(utf8.encode('first\n'));
    await firstController.close();
    await firstWrite;

    final key1Bytes = <int>[];
    await for (final chunk in store.readStream(key1, extension: ext)) {
      key1Bytes.addAll(chunk);
    }
    expect(utf8.decode(key1Bytes), contains('first'));

    final key2Bytes = <int>[];
    await for (final chunk in store.readStream(key2, extension: ext)) {
      key2Bytes.addAll(chunk);
    }
    expect(utf8.decode(key2Bytes), contains('second'));
  });

  test('folder hierarchy level 0 stores files in root', () async {
    // Test with 0 folder levels
    final tempDir0 =
        await Directory.systemTemp.createTemp('hashed_kv_test_h0_');
    try {
      final store0 = await MultiIsolateKvStoreClient.spawn(
        rootDirPath: tempDir0.path,
        numWriteWorkers: 2,
        folderHierarchyLevels: 0,
      );

      const key = 'test:key:hierarchy:0';
      const ext = 'bin';
      const content = 'Hierarchy 0';

      final writeStream = Stream<List<int>>.fromIterable([
        utf8.encode(content),
      ]);
      await store0.writeFromStream(key, writeStream, extension: ext);

      // Verify file is stored directly in root (no subdirectories)
      final files = await tempDir0.list(recursive: false).toList();
      expect(
        files.whereType<File>(),
        isNotEmpty,
        reason: 'Should have files in root directory',
      );

      // Read back to verify content
      final readData = <int>[];
      await for (final chunk in store0.readStream(key, extension: ext)) {
        readData.addAll(chunk);
      }
      expect(utf8.decode(readData), equals(content));

      await tempDir0.delete(recursive: true);
    } catch (e) {
      await tempDir0.delete(recursive: true);
      rethrow;
    }
  });

  test('folder hierarchy level 1 stores files in one folder', () async {
    // Test with 1 folder level
    final tempDir1 =
        await Directory.systemTemp.createTemp('hashed_kv_test_h1_');
    try {
      final store1 = await MultiIsolateKvStoreClient.spawn(
        rootDirPath: tempDir1.path,
        numWriteWorkers: 2,
        folderHierarchyLevels: 1,
      );

      const key = 'test:key:hierarchy:1';
      const ext = 'bin';
      const content = 'Hierarchy 1';

      final writeStream = Stream<List<int>>.fromIterable([
        utf8.encode(content),
      ]);
      await store1.writeFromStream(key, writeStream, extension: ext);

      // Verify file is stored in one folder level
      final subDirs = await tempDir1.list(recursive: false).toList();
      expect(
        subDirs.whereType<Directory>(),
        isNotEmpty,
        reason: 'Should have one folder level',
      );

      // Read back to verify content
      final readData = <int>[];
      await for (final chunk in store1.readStream(key, extension: ext)) {
        readData.addAll(chunk);
      }
      expect(utf8.decode(readData), equals(content));

      await tempDir1.delete(recursive: true);
    } catch (e) {
      await tempDir1.delete(recursive: true);
      rethrow;
    }
  });

  test('folder hierarchy level 2 stores files in two folders', () async {
    // Test with 2 folder levels (opt-in; default is 1)
    final tempDir2 =
        await Directory.systemTemp.createTemp('hashed_kv_test_h2_');
    try {
      final store2 = await MultiIsolateKvStoreClient.spawn(
        rootDirPath: tempDir2.path,
        numWriteWorkers: 2,
        folderHierarchyLevels: 2,
      );

      const key = 'test:key:hierarchy:2';
      const ext = 'bin';
      const content = 'Hierarchy 2';

      final writeStream = Stream<List<int>>.fromIterable([
        utf8.encode(content),
      ]);
      await store2.writeFromStream(key, writeStream, extension: ext);

      // Verify file is stored in two folder levels
      final subDirs = await tempDir2.list(recursive: false).toList();
      expect(
        subDirs.whereType<Directory>(),
        isNotEmpty,
        reason: 'Should have first folder level',
      );

      // Check nested folders
      final nestedDirs = await tempDir2.list(recursive: true).toList();
      final dirCount = nestedDirs.whereType<Directory>().length;
      expect(dirCount, greaterThanOrEqualTo(2),
          reason: 'Should have at least 2 folder levels');

      // Read back to verify content
      final readData = <int>[];
      await for (final chunk in store2.readStream(key, extension: ext)) {
        readData.addAll(chunk);
      }
      expect(utf8.decode(readData), equals(content));

      await tempDir2.delete(recursive: true);
    } catch (e) {
      await tempDir2.delete(recursive: true);
      rethrow;
    }
  });

  test('client exposes rootDirPath and pathForKey', () {
    expect(store.rootDirPath, equals(tempDir.path));
    final path = store.pathForKey('some:key', extension: 'bin');
    expect(path, startsWith(tempDir.path));
  });

  test('readStreamAt reads from explicit root path', () async {
    const key = 'read:at';
    const ext = 'txt';
    await store.writeFromStream(
      key,
      Stream.value(utf8.encode('explicit')),
      extension: ext,
    );

    final bytes = <int>[];
    await for (final chunk
        in store.readStreamAt(tempDir.path, key, extension: ext)) {
      bytes.addAll(chunk);
    }
    expect(utf8.decode(bytes), equals('explicit'));
  });

  test('input stream error aborts write and preserves prior content', () async {
    const key = 'stream:error';
    const ext = 'txt';

    await store.writeFromStream(
      key,
      Stream.value(utf8.encode('preserved')),
      extension: ext,
    );

    final controller = StreamController<List<int>>();
    final writeFuture =
        store.writeFromStream(key, controller.stream, extension: ext);
    controller.add(utf8.encode('partial'));

    final expectation = expectLater(writeFuture, throwsA(isA<StateError>()));
    controller.addError(StateError('stream failed'));
    await controller.close();
    await expectation;

    final bytes = <int>[];
    await for (final chunk in store.readStream(key, extension: ext)) {
      bytes.addAll(chunk);
    }
    expect(utf8.decode(bytes), equals('preserved'));
  });

  test('write to invalid storage root throws KvWriteException', () async {
    final blocker = File('${tempDir.path}/blocker');
    await blocker.writeAsString('not a directory');

    final badStore = await MultiIsolateKvStoreClient.spawn(
      rootDirPath: blocker.path,
      numWriteWorkers: 1,
    );

    await expectLater(
      badStore.writeFromStream(
        'fail:key',
        Stream.value([1, 2, 3]),
      ),
      throwsA(isA<KvWriteException>()),
    );
  });

  test('worker respawns after idle purge and accepts new writes', () async {
    final purgeDir = await Directory.systemTemp.createTemp('hashed_kv_purge_');
    try {
      final purgeStore = await MultiIsolateKvStoreClient.spawn(
        rootDirPath: purgeDir.path,
        numWriteWorkers: 1,
        writeIdlePurgeDuration: const Duration(milliseconds: 100),
      );

      await purgeStore.writeFromStream(
        'warmup',
        Stream.value(utf8.encode('warm')),
        extension: 'txt',
      );

      await Future.delayed(const Duration(milliseconds: 400));

      await purgeStore.writeFromStream(
        'after:purge',
        Stream.value(utf8.encode('ok')),
        extension: 'txt',
      );

      final bytes = <int>[];
      await for (final chunk
          in purgeStore.readStream('after:purge', extension: 'txt')) {
        bytes.addAll(chunk);
      }
      expect(utf8.decode(bytes), equals('ok'));
    } finally {
      if (await purgeDir.exists()) {
        await purgeDir.delete(recursive: true);
      }
    }
  });

  test('backpressure handles more chunks than the in-flight window', () async {
    final smallWindowStore = await MultiIsolateKvStoreClient.spawn(
      rootDirPath: tempDir.path,
      numWriteWorkers: 1,
      writeMaxInFlightChunks: 1,
    );

    const key = 'backpressure:many:chunks';
    const ext = 'txt';
    final chunks = List.generate(32, (i) => utf8.encode('$i\n'));

    await smallWindowStore.writeFromStream(
      key,
      Stream<List<int>>.fromIterable(chunks),
      extension: ext,
    );

    final bytes = <int>[];
    await for (final chunk
        in smallWindowStore.readStream(key, extension: ext)) {
      bytes.addAll(chunk);
    }
    expect(utf8.decode(bytes), equals(List.generate(32, (i) => '$i\n').join()));
  });

  test('truncate commit never exposes missing or partial content', () async {
    const key = 'atomic:stress';
    const ext = 'txt';
    const oldContent = 'old-value';
    const newContent = 'new-value';

    await store.writeFromStream(
      key,
      Stream.value(utf8.encode(oldContent)),
      extension: ext,
    );

    final controller = StreamController<List<int>>();
    final writeFuture = store.writeFromStream(
      key,
      controller.stream,
      extension: ext,
    );

    controller.add(utf8.encode(newContent));
    final closeFuture = controller.close();

    var sawNew = false;
    for (var i = 0; i < 50 && !sawNew; i++) {
      final bytes = <int>[];
      await for (final chunk in store.readStream(key, extension: ext)) {
        bytes.addAll(chunk);
      }
      final content = utf8.decode(bytes);
      expect(content, anyOf(oldContent, newContent));
      sawNew = content == newContent;
      await Future.delayed(const Duration(milliseconds: 5));
    }

    await closeFuture;
    await writeFuture;
  });

  test('spawn validates public parameters', () async {
    await expectLater(
      () => MultiIsolateKvStoreClient.spawn(
        rootDirPath: tempDir.path,
        numWriteWorkers: 0,
      ),
      throwsArgumentError,
    );
    await expectLater(
      () => MultiIsolateKvStoreClient.spawn(
        rootDirPath: tempDir.path,
        folderHierarchyLevels: 3,
      ),
      throwsArgumentError,
    );
    await expectLater(
      () => MultiIsolateKvStoreClient.spawn(
        rootDirPath: tempDir.path,
        flushThresholdBytes: 0,
      ),
      throwsArgumentError,
    );
    await expectLater(
      () => MultiIsolateKvStoreClient.spawn(
        rootDirPath: tempDir.path,
        flushInterval: const Duration(milliseconds: -1),
      ),
      throwsArgumentError,
    );
    await expectLater(
      () => MultiIsolateKvStoreClient.spawn(
        rootDirPath: tempDir.path,
        writeMaxInFlightChunks: -1,
      ),
      throwsArgumentError,
    );
  });
}
