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
    await store.close();
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
        .where((f) => !p.basename(f.path).startsWith('.'))
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
    await badStore.close();
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
      await purgeStore.close();
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
    await smallWindowStore.close();
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

    final midBytes = <int>[];
    await for (final chunk in store.readStream(key, extension: ext)) {
      midBytes.addAll(chunk);
    }
    expect(utf8.decode(midBytes), equals(oldContent));

    controller.add(utf8.encode(newContent));
    await controller.close();
    await writeFuture;

    final finalBytes = <int>[];
    await for (final chunk in store.readStream(key, extension: ext)) {
      finalBytes.addAll(chunk);
    }
    expect(utf8.decode(finalBytes), equals(newContent));
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
    await expectLater(
      () => MultiIsolateKvStoreClient.spawn(
        rootDirPath: tempDir.path,
        writeIdlePurgeDuration: const Duration(milliseconds: -1),
      ),
      throwsArgumentError,
    );
    await expectLater(
      () => MultiIsolateKvStoreClient.spawn(
        rootDirPath: '',
      ),
      throwsArgumentError,
    );
  });

  test('credit registration handles default window with many chunks', () async {
    final creditStore = await MultiIsolateKvStoreClient.spawn(
      rootDirPath: tempDir.path,
      numWriteWorkers: 1,
      writeMaxInFlightChunks: 8,
    );

    const key = 'credit:race';
    const ext = 'bin';
    final chunks = List.generate(32, (i) => utf8.encode('c$i'));

    await creditStore.writeFromStream(
      key,
      Stream<List<int>>.fromIterable(chunks),
      extension: ext,
    );

    final bytes = <int>[];
    await for (final chunk in creditStore.readStream(key, extension: ext)) {
      bytes.addAll(chunk);
    }
    expect(utf8.decode(bytes), equals(chunks.map(utf8.decode).join()));
    await creditStore.close();
  });

  test('delete completes after worker idle purge respawn', () async {
    final purgeDir = await Directory.systemTemp.createTemp('delete_purge_');
    try {
      final purgeStore = await MultiIsolateKvStoreClient.spawn(
        rootDirPath: purgeDir.path,
        numWriteWorkers: 1,
        writeIdlePurgeDuration: const Duration(milliseconds: 100),
      );

      await purgeStore.writeFromStream(
        'del:key',
        Stream.value(utf8.encode('x')),
        extension: 'txt',
      );
      await Future.delayed(const Duration(milliseconds: 400));

      await purgeStore.delete('del:key', extension: 'txt');

      expect(
        () async =>
            await purgeStore.readStream('del:key', extension: 'txt').drain(),
        throwsA(isA<KvNotFoundException>()),
      );
      await purgeStore.close();
    } finally {
      if (await purgeDir.exists()) {
        await purgeDir.delete(recursive: true);
      }
    }
  });

  test('writeAbort allows queued write to proceed', () async {
    const key = 'abort:queue';
    const ext = 'txt';

    final controllerA = StreamController<List<int>>();
    final writeA = store.writeFromStream(key, controllerA.stream, extension: ext);
    controllerA.add(utf8.encode('partial'));

    final writeB = store.writeFromStream(
      key,
      Stream.value(utf8.encode('queued-ok')),
      extension: ext,
    );

    final abortExpect = expectLater(writeA, throwsA(isA<StateError>()));
    controllerA.addError(StateError('abort'));
    await controllerA.close();
    await abortExpect;

    await writeB;

    final bytes = <int>[];
    await for (final chunk in store.readStream(key, extension: ext)) {
      bytes.addAll(chunk);
    }
    expect(utf8.decode(bytes), equals('queued-ok'));
  });

  test('close prevents further write and delete operations', () async {
    await store.close();

    await expectLater(
      store.writeFromStream('k', Stream.value([1])),
      throwsA(isA<StateError>()),
    );
    await expectLater(
      store.delete('k'),
      throwsA(isA<StateError>()),
    );
    expect(store.isClosed, isTrue);
  });

  test('fsyncOnClose accepts writes', () async {
    final fsyncStore = await MultiIsolateKvStoreClient.spawn(
      rootDirPath: tempDir.path,
      numWriteWorkers: 1,
      fsyncOnClose: true,
    );

    await fsyncStore.writeFromStream(
      'fsync:key',
      Stream.value(utf8.encode('synced')),
      extension: 'txt',
    );

    final bytes = <int>[];
    await for (final chunk in fsyncStore.readStream('fsync:key', extension: 'txt')) {
      bytes.addAll(chunk);
    }
    expect(utf8.decode(bytes), equals('synced'));
    await fsyncStore.close();
  });

  test('live subscription survives worker respawn', () async {
    final purgeDir = await Directory.systemTemp.createTemp('live_purge_');
    try {
      final purgeStore = await MultiIsolateKvStoreClient.spawn(
        rootDirPath: purgeDir.path,
        numWriteWorkers: 1,
        writeIdlePurgeDuration: const Duration(milliseconds: 100),
      );

      final received = <List<int>>[];
      final sub = purgeStore
          .subscribeLive('live:respawn', extension: 'txt')
          .listen(received.add);

      await Future.delayed(const Duration(milliseconds: 50));

      await purgeStore.writeFromStream(
        'warmup',
        Stream.value(utf8.encode('warm')),
        extension: 'txt',
      );
      await Future.delayed(const Duration(milliseconds: 400));

      await purgeStore.writeFromStream(
        'live:respawn',
        Stream.value(utf8.encode('after-respawn')),
        extension: 'txt',
      );
      await Future.delayed(const Duration(milliseconds: 100));

      expect(received, isNotEmpty);
      final content = utf8.decode(received.expand((c) => c).toList());
      expect(content, contains('after-respawn'));

      await sub.cancel();
      await purgeStore.close();
    } finally {
      if (await purgeDir.exists()) {
        await purgeDir.delete(recursive: true);
      }
    }
  });

  test('stale truncate temp files are removed on new truncate write', () async {
    const key = 'temp:cleanup';
    const ext = 'txt';
    final path = store.pathForKey(key, extension: ext);

    await store.writeFromStream(
      key,
      Stream.value(utf8.encode('old')),
      extension: ext,
    );

    final orphan = File('$path.999.tmp');
    await orphan.writeAsString('orphan');

    await store.writeFromStream(
      key,
      Stream.value(utf8.encode('new')),
      extension: ext,
    );

    expect(await orphan.exists(), isFalse);
    final bytes = <int>[];
    await for (final chunk in store.readStream(key, extension: ext)) {
      bytes.addAll(chunk);
    }
    expect(utf8.decode(bytes), equals('new'));
  });

  test('exists and listStoredPaths report stored values', () async {
    await store.writeFromStream('alpha', Stream.value([1]), extension: 'bin');
    await store.writeFromStream('beta', Stream.value([2]), extension: 'txt');

    expect(await store.exists('alpha', extension: 'bin'), isTrue);
    expect(await store.exists('beta', extension: 'txt'), isTrue);
    expect(await store.exists('gamma', extension: 'bin'), isFalse);

    final paths = await store.listStoredPaths();
    expect(paths.length, equals(2));
    expect(paths.every((path) => !path.contains('.tmp')), isTrue);
    expect(
      paths.every(
        (path) => RegExp(
          r'(^|[/\\])[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}\.[0-9a-z]+$',
        ).hasMatch(path),
      ),
      isTrue,
    );
  });

  group('read optimizations', () {
    test('readBytes returns full value', () async {
      const key = 'bytes:test';
      const content = 'read-bytes-payload';
      await store.writeFromStream(
        key,
        Stream.value(utf8.encode(content)),
        extension: 'txt',
      );

      final bytes = await store.readBytes(key, extension: 'txt');
      expect(utf8.decode(bytes), equals(content));
    });

    test('readBytes with checkExists true on missing key throws KvNotFoundException',
        () async {
      await expectLater(
        store.readBytes('missing:bytes'),
        throwsA(isA<KvNotFoundException>()),
      );
    });

    test('readBytes with checkExists false on missing key throws on read', () async {
      await expectLater(
        store.readBytes('missing:bytes', checkExists: false),
        throwsA(isA<FileSystemException>()),
      );
    });

    test('readStream with checkExists false skips exists probe', () async {
      const key = 'nocheck:stream';
      await store.writeFromStream(
        key,
        Stream.value([9, 8, 7]),
        extension: 'bin',
      );

      final bytes = <int>[];
      await for (final chunk
          in store.readStream(key, checkExists: false)) {
        bytes.addAll(chunk);
      }
      expect(bytes, equals([9, 8, 7]));
    });

    test('readBytesAll reads many keys concurrently', () async {
      await store.writeFromStream('batch:a', Stream.value([1]));
      await store.writeFromStream('batch:b', Stream.value([2, 3]));

      final all = await store.readBytesAll(['batch:a', 'batch:b']);
      expect(all['batch:a'], equals([1]));
      expect(all['batch:b'], equals([2, 3]));
    });

    test('path cache returns consistent paths', () async {
      final first = store.pathForKey('cache:key', extension: 'bin');
      final second = store.pathForKey('cache:key', extension: 'bin');
      expect(second, equals(first));
    });
  });

  group('read-only client', () {
    late Directory readOnlyDir;
    late MultiIsolateKvStoreClient readOnly;

    setUp(() async {
      readOnlyDir = await Directory.systemTemp.createTemp('kv_readonly_');
      readOnly = await MultiIsolateKvStoreClient.openReadOnly(
        rootDirPath: readOnlyDir.path,
      );
    });

    tearDown(() async {
      await readOnly.close();
      if (await readOnlyDir.exists()) {
        await readOnlyDir.delete(recursive: true);
      }
    });

    test('isReadOnly and write operations throw', () async {
      expect(readOnly.isReadOnly, isTrue);
      expect(
        () => readOnly.writeFromStream('k', Stream.value([1])),
        throwsA(isA<StateError>()),
      );
      expect(
        () => readOnly.delete('k'),
        throwsA(isA<StateError>()),
      );
      expect(
        () => readOnly.subscribeLive('k'),
        throwsA(isA<StateError>()),
      );
    });

    test('writeFromStreamDirect and read work without isolates', () async {
      const key = 'ro:direct';
      await readOnly.writeFromStreamDirect(
        key,
        Stream.value(utf8.encode('direct')),
        extension: 'txt',
      );
      expect(await readOnly.exists(key, extension: 'txt'), isTrue);
      final bytes = await readOnly.readBytes(key, extension: 'txt');
      expect(utf8.decode(bytes), equals('direct'));
    });

    test('deleteLocal removes file', () async {
      const key = 'ro:delete';
      await readOnly.writeFromStreamDirect(key, Stream.value([5]));
      expect(await readOnly.exists(key), isTrue);
      await readOnly.deleteLocal(key);
      expect(await readOnly.exists(key), isFalse);
    });
  });

  test('queued write drains after open failure in worker queue', () async {
    const key = 'open:fail:drain';
    const ext = 'txt';

    await store.writeFromStream(
      key,
      Stream.value(utf8.encode('seed')),
      extension: ext,
    );

    final path = store.pathForKey(key, extension: ext);
    final hold = StreamController<List<int>>();
    var w1Done = false;
    final w1 = store
        .writeFromStream(key, hold.stream, extension: ext)
        .whenComplete(() => w1Done = true);
    hold.add(utf8.encode('one'));

    final w2 = store.writeFromStream(
      key,
      Stream.value(utf8.encode('two')),
      extension: ext,
      truncateExisting: false,
    );
    final w3 = store.writeFromStream(
      key,
      Stream.value(utf8.encode('three')),
      extension: ext,
    );

    unawaited(() async {
      while (!w1Done) {
        await Future.delayed(const Duration(microseconds: 100));
      }
      for (var i = 0; i < 300; i++) {
        if (await File(path).exists()) {
          try {
            await File(path).delete();
            await Directory(path).create();
            return;
          } catch (_) {}
        }
        await Future.delayed(const Duration(microseconds: 100));
      }
    }());

    hold.close();
    await w1;

    await expectLater(w2, throwsA(isA<KvWriteException>()));

    if (await Directory(path).exists()) {
      await Directory(path).delete(recursive: true);
    }

    await w3;

    final bytes = <int>[];
    await for (final chunk in store.readStream(key, extension: ext)) {
      bytes.addAll(chunk);
    }
    expect(utf8.decode(bytes), equals('three'));
  }, onPlatform: {
    'windows': Skip('Timing-sensitive queue drain test'),
  });

  group('layout version migration', () {
    test('fresh root writes metadata and preserves data across respawn', () async {
      final dir = await Directory.systemTemp.createTemp('kv_layout_fresh_');
      try {
        final first = await MultiIsolateKvStoreClient.spawn(
          rootDirPath: dir.path,
          numWriteWorkers: 1,
        );
        await first.writeFromStream(
          'persist',
          Stream.value(utf8.encode('ok')),
          extension: 'txt',
        );
        await first.close();

        final meta = File(kvStoreMetaFilePath(dir.path));
        expect(await meta.exists(), isTrue);
        expect(
          jsonDecode(await meta.readAsString()),
          equals(<String, dynamic>{'layoutVersion': kKvStoreLayoutVersion}),
        );

        final second = await MultiIsolateKvStoreClient.spawn(
          rootDirPath: dir.path,
          numWriteWorkers: 1,
        );
        final bytes = <int>[];
        await for (final chunk in second.readStream('persist', extension: 'txt')) {
          bytes.addAll(chunk);
        }
        expect(utf8.decode(bytes), equals('ok'));
        await second.close();
      } finally {
        await dir.delete(recursive: true);
      }
    });

    test('stale metadata triggers wipe on spawn', () async {
      final dir = await Directory.systemTemp.createTemp('kv_layout_stale_');
      try {
        await dir.create(recursive: true);
        final staleKeyPath = p.join(dir.path, 'stale-marker.txt');
        await File(staleKeyPath).writeAsString('old');

        await File(kvStoreMetaFilePath(dir.path)).writeAsString(
          jsonEncode(<String, dynamic>{'layoutVersion': 0}),
        );

        final client = await MultiIsolateKvStoreClient.spawn(
          rootDirPath: dir.path,
          numWriteWorkers: 1,
        );
        expect(await File(staleKeyPath).exists(), isFalse);
        await client.writeFromStream(
          'new',
          Stream.value(utf8.encode('fresh')),
          extension: 'txt',
        );
        await client.close();

        final meta = jsonDecode(
          await File(kvStoreMetaFilePath(dir.path)).readAsString(),
        ) as Map<String, dynamic>;
        expect(meta['layoutVersion'], equals(kKvStoreLayoutVersion));
      } finally {
        await dir.delete(recursive: true);
      }
    });

    test('legacy files without metadata are wiped on spawn', () async {
      final dir = await Directory.systemTemp.createTemp('kv_layout_legacy_');
      try {
        final legacyFile = File(
          p.join(
            dir.path,
            'ab',
            'cd',
            '${'a' * 64}.txt',
          ),
        );
        await legacyFile.parent.create(recursive: true);
        await legacyFile.writeAsString('legacy');

        final client = await MultiIsolateKvStoreClient.spawn(
          rootDirPath: dir.path,
          numWriteWorkers: 1,
        );
        expect(await legacyFile.exists(), isFalse);
        expect(await dir.exists(), isTrue);
        await client.close();
      } finally {
        await dir.delete(recursive: true);
      }
    });

    test('wipeOnLayoutMismatch false throws on mismatch', () async {
      final dir = await Directory.systemTemp.createTemp('kv_layout_optout_');
      try {
        await File(kvStoreMetaFilePath(dir.path)).writeAsString(
          jsonEncode(<String, dynamic>{'layoutVersion': 0}),
        );

        await expectLater(
          () => MultiIsolateKvStoreClient.spawn(
            rootDirPath: dir.path,
            numWriteWorkers: 1,
            wipeOnLayoutMismatch: false,
          ),
          throwsA(isA<KvLayoutMismatchException>()),
        );
      } finally {
        await dir.delete(recursive: true);
      }
    });
  });
}
