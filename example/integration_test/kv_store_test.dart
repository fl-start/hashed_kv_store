import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:flutter_test/flutter_test.dart';
import 'package:hashed_kv_store/hashed_kv_store.dart';
import 'package:integration_test/integration_test.dart';
import 'package:path_provider/path_provider.dart';

/// On-device integration tests that exercise the multi-isolate store on real
/// mobile/desktop platforms (Android, iOS, macOS, etc.), covering the isolate
/// spawn path, streaming file IO, batch reads, deletion, and cancellation.
void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  late Directory rootDir;
  late MultiIsolateKvStoreClient store;

  setUp(() async {
    final base = await getApplicationDocumentsDirectory();
    rootDir = await Directory(
      '${base.path}/kv_it_${DateTime.now().microsecondsSinceEpoch}',
    ).create(recursive: true);
    store = await MultiIsolateKvStoreClient.spawn(
      rootDirPath: rootDir.path,
      numWriteWorkers: 2,
    );
  });

  tearDown(() async {
    await store.close();
    if (await rootDir.exists()) {
      try {
        await rootDir.delete(recursive: true);
      } catch (_) {}
    }
  });

  testWidgets('write then read round-trips through worker isolates',
      (tester) async {
    const key = 'it:roundtrip';
    await store.writeFromStream(
      key,
      Stream.value(utf8.encode('hello device')),
      extension: 'txt',
    );

    expect(await store.exists(key, extension: 'txt'), isTrue);

    final bytes = <int>[];
    await for (final chunk in store.readStream(key, extension: 'txt')) {
      bytes.addAll(chunk);
    }
    expect(utf8.decode(bytes), equals('hello device'));
  });

  testWidgets('readBytes returns the full value', (tester) async {
    const key = 'it:bytes';
    final payload = Uint8List.fromList(List.filled(128 * 1024, 42));
    await store.writeFromStream(key, Stream.value(payload));

    final read = await store.readBytes(key);
    expect(read.length, equals(payload.length));
    expect(read.first, equals(42));
    expect(read.last, equals(42));
  });

  testWidgets('readBytesAll reads multiple keys concurrently', (tester) async {
    for (var i = 0; i < 5; i++) {
      await store.writeFromStream('it:batch:$i', Stream.value([i, i, i]));
    }

    final results = await store.readBytesAll([
      for (var i = 0; i < 5; i++) 'it:batch:$i',
    ]);
    expect(results.length, equals(5));
    for (var i = 0; i < 5; i++) {
      expect(results['it:batch:$i'], equals([i, i, i]));
    }
  });

  testWidgets('delete removes a stored value', (tester) async {
    const key = 'it:delete';
    await store.writeFromStream(key, Stream.value([1, 2, 3]));
    expect(await store.exists(key), isTrue);

    await store.delete(key);
    expect(await store.exists(key), isFalse);
  });

  testWidgets('abort cancels an in-flight write and preserves prior content',
      (tester) async {
    const key = 'it:abort';
    const ext = 'txt';
    await store.writeFromStream(
      key,
      Stream.value(utf8.encode('original')),
      extension: ext,
    );

    final controller = KvAbortController();
    final hold = StreamController<List<int>>();
    final writeFuture = store.writeFromStream(
      key,
      hold.stream,
      extension: ext,
      signal: controller.signal,
    );
    hold.add(utf8.encode('partial'));
    await Future.delayed(const Duration(milliseconds: 50));
    controller.abort();

    await expectLater(writeFuture, throwsA(isA<KvAbortException>()));
    await hold.close();

    final bytes = await store.readBytes(key, extension: ext);
    expect(utf8.decode(bytes), equals('original'));
  });

  testWidgets('concurrent writes to different keys all succeed',
      (tester) async {
    await Future.wait([
      for (var i = 0; i < 10; i++)
        store.writeFromStream('it:concurrent:$i', Stream.value([i])),
    ]);

    for (var i = 0; i < 10; i++) {
      expect(await store.readBytes('it:concurrent:$i'), equals([i]));
    }
  });
}
