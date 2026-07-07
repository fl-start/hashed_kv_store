import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:hashed_kv_store/hashed_kv_store.dart';
import 'package:test/test.dart';

/// Ignores the returned future to avoid unawaited_futures lints in tests.
void _ignore(Future<void> future) {
  future.catchError((_) {});
}

Uint8List _payload(int size, [int fill = 7]) =>
    Uint8List.fromList(List.filled(size, fill));

void main() {
  group('abort integration', () {
    late Directory tempDir;
    late MultiIsolateKvStoreClient store;

    setUp(() async {
      tempDir = await Directory.systemTemp.createTemp('kv_abort_it_');
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
        } catch (_) {}
      }
    });

    group('KvAbortSignal API', () {
      test('onAbort fires once with reason available', () {
        final controller = KvAbortController();
        var fired = 0;
        controller.signal.onAbort(() => fired++);

        expect(controller.signal.aborted, isFalse);
        controller.abort('stop');

        expect(fired, equals(1));
        expect(controller.signal.aborted, isTrue);
        expect(controller.signal.reason, equals('stop'));
      });

      test('onAbort on already-aborted signal fires immediately', () {
        final controller = KvAbortController();
        controller.abort();

        var fired = false;
        controller.signal.onAbort(() => fired = true);
        expect(fired, isTrue);
      });

      test('removeAbortListener prevents firing', () {
        final controller = KvAbortController();
        var fired = false;
        void listener() => fired = true;

        controller.signal.onAbort(listener);
        controller.signal.removeAbortListener(listener);
        controller.abort();

        expect(fired, isFalse);
      });

      test('throwIfAborted throws only after abort', () {
        final controller = KvAbortController();
        expect(controller.signal.throwIfAborted, returnsNormally);

        controller.abort('boom');
        expect(
          controller.signal.throwIfAborted,
          throwsA(
            isA<KvAbortException>().having(
              (e) => e.reason,
              'reason',
              'boom',
            ),
          ),
        );
      });

      test('multiple abort calls keep first reason (idempotent)', () {
        final controller = KvAbortController();
        controller.abort('first');
        controller.abort('second');
        controller.abort();

        expect(controller.signal.reason, equals('first'));
        expect(controller.signal.aborted, isTrue);
      });
    });

    group('write abort', () {
      test('abort before write starts writes nothing', () async {
        final controller = KvAbortController();
        controller.abort();

        await expectLater(
          store.writeFromStream(
            'w:before',
            Stream.value(utf8.encode('data')),
            signal: controller.signal,
          ),
          throwsA(isA<KvAbortException>()),
        );

        expect(await store.exists('w:before'), isFalse);
      });

      test('abort mid-stream under backpressure leaves prior content intact',
          () async {
        final backpressureDir =
            await Directory.systemTemp.createTemp('kv_abort_bp_');
        final bpStore = await MultiIsolateKvStoreClient.spawn(
          rootDirPath: backpressureDir.path,
          numWriteWorkers: 1,
          writeMaxInFlightChunks: 1,
        );
        addTearDown(() async {
          await bpStore.close();
          if (await backpressureDir.exists()) {
            await backpressureDir.delete(recursive: true);
          }
        });

        const key = 'w:backpressure';
        await bpStore.writeFromStream(key, Stream.value(utf8.encode('seed')));

        final controller = KvAbortController();
        final hold = StreamController<List<int>>();
        final writeFuture = bpStore.writeFromStream(
          key,
          hold.stream,
          signal: controller.signal,
        );

        for (var i = 0; i < 50; i++) {
          hold.add(_payload(4096, i % 256));
        }
        await Future.delayed(const Duration(milliseconds: 50));
        controller.abort('cancel-bp');
        await expectLater(writeFuture, throwsA(isA<KvAbortException>()));
        await hold.close();

        expect(
          utf8.decode(await bpStore.readBytes(key)),
          equals('seed'),
        );
      });

      test('aborted write releases queue and next write proceeds', () async {
        const key = 'w:queue';
        final hold = StreamController<List<int>>();
        final controller = KvAbortController();

        final aborted = store.writeFromStream(
          key,
          hold.stream,
          signal: controller.signal,
        );
        hold.add(utf8.encode('aborted-partial'));
        await Future.delayed(const Duration(milliseconds: 50));

        final queued =
            store.writeFromStream(key, Stream.value(utf8.encode('winner')));

        controller.abort();
        await expectLater(aborted, throwsA(isA<KvAbortException>()));
        await queued;
        await hold.close();

        expect(utf8.decode(await store.readBytes(key)), equals('winner'));
      });

      test(
          're-writing same key after abort yields new content (no late-chunk corruption)',
          () async {
        const key = 'w:late-chunk';
        final controller = KvAbortController();
        final hold = StreamController<List<int>>();

        final aborted = store.writeFromStream(
          key,
          hold.stream,
          signal: controller.signal,
        );
        hold.add(_payload(8192, 1));
        await Future.delayed(const Duration(milliseconds: 30));
        controller.abort();

        // Keep pushing chunks after abort; these must be ignored, not crash.
        for (var i = 0; i < 10; i++) {
          hold.add(_payload(8192, 2));
        }
        await expectLater(aborted, throwsA(isA<KvAbortException>()));
        await hold.close();

        await store.writeFromStream(key, Stream.value(utf8.encode('fresh')));
        expect(utf8.decode(await store.readBytes(key)), equals('fresh'));
      });

      test('abort while queued (behind active write) removes the queued write',
          () async {
        const key = 'w:queued-abort';
        final blocker = StreamController<List<int>>();
        final blockerFuture = store.writeFromStream(key, blocker.stream);
        blocker.add(utf8.encode('block'));
        await Future.delayed(const Duration(milliseconds: 30));

        final controller = KvAbortController();
        final queued = store.writeFromStream(
          key,
          Stream.value(utf8.encode('never')),
          signal: controller.signal,
        );
        await Future.delayed(const Duration(milliseconds: 30));
        controller.abort();
        await expectLater(queued, throwsA(isA<KvAbortException>()));

        await blocker.close();
        await blockerFuture;

        expect(utf8.decode(await store.readBytes(key)), equals('block'));
      });

      test('aborting one key does not affect concurrent writes to other keys',
          () async {
        final controller = KvAbortController();
        final hold = StreamController<List<int>>();

        final abortedWrite = store.writeFromStream(
          'w:concurrent:aborted',
          hold.stream,
          signal: controller.signal,
        );
        hold.add(_payload(4096));

        final others = <Future<void>>[];
        for (var i = 0; i < 5; i++) {
          others.add(
            store.writeFromStream(
              'w:concurrent:$i',
              Stream.value(utf8.encode('ok-$i')),
            ),
          );
        }

        await Future.delayed(const Duration(milliseconds: 50));
        controller.abort();
        await expectLater(abortedWrite, throwsA(isA<KvAbortException>()));
        await Future.wait(others);
        await hold.close();

        for (var i = 0; i < 5; i++) {
          expect(
            utf8.decode(await store.readBytes('w:concurrent:$i')),
            equals('ok-$i'),
          );
        }
        expect(await store.exists('w:concurrent:aborted'), isFalse);
      });

      test('early abort still drains caller stream so close() completes',
          () async {
        // Regression: aborting before writeFromStream subscribes to the input
        // must still consume the caller-owned StreamController, otherwise its
        // close() blocks forever waiting for a listener.
        final controller = KvAbortController();
        controller.abort();

        final hold = StreamController<List<int>>();
        final writeFuture = store.writeFromStream(
          'w:early-drain',
          hold.stream,
          signal: controller.signal,
        );
        hold.add(_payload(4096));

        await expectLater(writeFuture, throwsA(isA<KvAbortException>()));
        await expectLater(
          hold.close().timeout(const Duration(seconds: 5)),
          completes,
        );
        expect(await store.exists('w:early-drain'), isFalse);
      });

      test('write completes normally when signal is never aborted', () async {
        const key = 'w:not-aborted';
        final controller = KvAbortController();
        await store.writeFromStream(
          key,
          Stream.fromIterable([
            utf8.encode('hello '),
            utf8.encode('world'),
          ]),
          signal: controller.signal,
        );
        expect(utf8.decode(await store.readBytes(key)), equals('hello world'));
      });
    });

    group('writeFromStreamDirect abort', () {
      test('truncate abort cleans temp and preserves prior content', () async {
        const key = 'd:truncate';
        await store.writeFromStreamDirect(
          key,
          Stream.value(utf8.encode('prior')),
        );

        final controller = KvAbortController();
        final hold = StreamController<List<int>>();
        final writeFuture = store.writeFromStreamDirect(
          key,
          hold.stream,
          signal: controller.signal,
        );
        hold.add(_payload(8192));
        await Future.delayed(const Duration(milliseconds: 30));
        controller.abort();
        await expectLater(writeFuture, throwsA(isA<KvAbortException>()));
        await hold.close();

        expect(utf8.decode(await store.readBytes(key)), equals('prior'));
      });

      test('abort before direct write starts writes nothing', () async {
        final controller = KvAbortController();
        controller.abort();
        await expectLater(
          store.writeFromStreamDirect(
            'd:before',
            Stream.value(utf8.encode('x')),
            signal: controller.signal,
          ),
          throwsA(isA<KvAbortException>()),
        );
        expect(await store.exists('d:before'), isFalse);
      });

      test('direct write works on read-only client with signal', () async {
        final roDir = await Directory.systemTemp.createTemp('kv_abort_ro_');
        final ro = await MultiIsolateKvStoreClient.openReadOnly(
          rootDirPath: roDir.path,
        );
        addTearDown(() async {
          await ro.close();
          if (await roDir.exists()) {
            await roDir.delete(recursive: true);
          }
        });

        const key = 'd:ro';
        final controller = KvAbortController();
        await ro.writeFromStreamDirect(
          key,
          Stream.value(utf8.encode('ro-data')),
          signal: controller.signal,
        );
        expect(utf8.decode(await ro.readBytes(key)), equals('ro-data'));
      });
    });

    group('read abort', () {
      test('pre-aborted signal throws before streaming (routed)', () async {
        const key = 'r:pre-abort';
        await store.writeFromStream(key, Stream.value(_payload(256 * 1024)));

        final controller = KvAbortController();
        controller.abort('read-cancel');

        await expectLater(
          store.readStream(key, signal: controller.signal).drain<void>(),
          throwsA(isA<KvAbortException>()),
        );
      });

      test('mid-stream abort stops routed readStream', () async {
        const key = 'r:mid-stream';
        await store.writeFromStream(key, Stream.value(_payload(1024 * 1024)));

        final controller = KvAbortController();
        var received = 0;
        final readFuture = () async {
          await for (final chunk
              in store.readStream(key, signal: controller.signal)) {
            received += chunk.length;
            if (received >= 64 * 1024) {
              controller.abort();
            }
          }
        }();

        await expectLater(readFuture, throwsA(isA<KvAbortException>()));
        expect(received, lessThan(1024 * 1024));
      });

      test('routed readStream returns full content when not aborted', () async {
        const key = 'r:full';
        final payload = _payload(200 * 1024, 3);
        await store.writeFromStream(key, Stream.value(payload));

        final controller = KvAbortController();
        final bytes = <int>[];
        await for (final chunk
            in store.readStream(key, signal: controller.signal)) {
          bytes.addAll(chunk);
        }
        expect(bytes.length, equals(payload.length));
        expect(bytes.first, equals(3));
        expect(bytes.last, equals(3));
      });

      test('readBytes with pre-aborted signal throws (routed)', () async {
        const key = 'r:bytes-pre';
        await store.writeFromStream(key, Stream.value(_payload(128 * 1024)));

        final controller = KvAbortController();
        controller.abort();

        await expectLater(
          store.readBytes(key, signal: controller.signal),
          throwsA(isA<KvAbortException>()),
        );
      });

      test('readBytes returns full content when not aborted (routed)',
          () async {
        const key = 'r:bytes-full';
        final payload = _payload(150 * 1024, 9);
        await store.writeFromStream(key, Stream.value(payload));

        final controller = KvAbortController();
        final bytes = await store.readBytes(key, signal: controller.signal);
        expect(bytes.length, equals(payload.length));
      });

      test('readBytesAll with pre-aborted signal throws (routed)', () async {
        await store.writeFromStream('r:all:a', Stream.value(_payload(4096)));
        await store.writeFromStream('r:all:b', Stream.value(_payload(4096)));

        final controller = KvAbortController();
        controller.abort();

        await expectLater(
          store.readBytesAll(
            ['r:all:a', 'r:all:b'],
            signal: controller.signal,
          ),
          throwsA(isA<KvAbortException>()),
        );
      });

      test('readStreamAt honors signal', () async {
        const key = 'r:at';
        await store.writeFromStream(key, Stream.value(_payload(256 * 1024)));

        final controller = KvAbortController();
        controller.abort();

        await expectLater(
          store
              .readStreamAt(tempDir.path, key, signal: controller.signal)
              .drain<void>(),
          throwsA(isA<KvAbortException>()),
        );
      });

      test('read without signal is unaffected', () async {
        const key = 'r:no-signal';
        final payload = _payload(100 * 1024, 5);
        await store.writeFromStream(key, Stream.value(payload));

        final bytes = await store.readBytes(key);
        expect(bytes.length, equals(payload.length));
      });
    });

    group('resilience after aborts', () {
      test('store stays healthy after many write aborts', () async {
        for (var i = 0; i < 20; i++) {
          final controller = KvAbortController();
          final hold = StreamController<List<int>>();
          final future = store.writeFromStream(
            'stress:$i',
            hold.stream,
            signal: controller.signal,
          );
          hold.add(_payload(4096));
          await Future.delayed(const Duration(milliseconds: 5));
          controller.abort();
          await expectLater(future, throwsA(isA<KvAbortException>()));
          await hold.close();
        }

        // Store must still accept and serve writes/reads normally.
        await store.writeFromStream(
          'stress:final',
          Stream.value(utf8.encode('healthy')),
        );
        expect(
          utf8.decode(await store.readBytes('stress:final')),
          equals('healthy'),
        );
      });

      test('store stays healthy after many read aborts', () async {
        const key = 'read:stress';
        await store.writeFromStream(key, Stream.value(_payload(512 * 1024)));

        for (var i = 0; i < 20; i++) {
          final controller = KvAbortController();
          controller.abort();
          _ignore(
            store.readStream(key, signal: controller.signal).drain<void>(),
          );
        }

        // A subsequent full read must still succeed.
        final bytes = await store.readBytes(key);
        expect(bytes.length, equals(512 * 1024));
      });
    });
  });
}
