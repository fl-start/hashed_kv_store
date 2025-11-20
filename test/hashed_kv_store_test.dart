import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:hashed_kv_store/hashed_kv_store.dart';
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
      numWorkers: 2,
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
      throwsA(isA<StateError>()),
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

    // Wait for delete to complete
    await Future.delayed(const Duration(milliseconds: 100));

    // Read should fail
    final readStream = store.readStream(key, extension: ext);
    expect(
      () async => await readStream.forEach((_) {}),
      throwsA(isA<StateError>()),
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
}
