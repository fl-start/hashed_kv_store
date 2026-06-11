import 'dart:convert';
import 'dart:io';

import 'package:hashed_kv_store/hashed_kv_store.dart';
import 'package:hashed_kv_store_benchmark/direct_kv_io.dart';
import 'package:hashed_kv_store_benchmark/stats.dart';

/// Sequential read of many small files — typical read-heavy KV workload.
///
/// Usage: dart run bin/many_small_reads_benchmark.dart [fileCount] [fileSizeBytes]
Future<void> main(List<String> args) async {
  final fileCount = args.isNotEmpty ? int.parse(args[0]) : 1000;
  final fileSize = args.length > 1 ? int.parse(args[1]) : 512;
  const extension = 'bin';
  const warmupPasses = 2;
  const measurePasses = 5;

  final runRoot = await Directory.systemTemp.createTemp('kv_many_reads_');
  final dataRoot = Directory('${runRoot.path}/data');
  await dataRoot.create(recursive: true);

  stdout.writeln('Many small files — read workload benchmark');
  stdout.writeln('Platform: ${Platform.operatingSystem}');
  stdout.writeln('Files: $fileCount  Size: $fileSize B each');
  stdout.writeln('Data root: ${dataRoot.path}');
  stdout.writeln('');

  final keys = List.generate(fileCount, (i) => 'small:read:$i');
  final payload = payloadOfSize(fileSize);

  // Spawn first so layout metadata exists; seeding after avoids auto-wipe on spawn.
  final store = await MultiIsolateKvStoreClient.spawn(
    rootDirPath: dataRoot.path,
    numWriteWorkers: 2,
    folderHierarchyLevels: 1,
    writeIdlePurgeDuration: const Duration(days: 1),
  );

  final direct = KvDirectIo(rootDirPath: dataRoot.path);

  stdout.writeln('Seeding $fileCount files (not timed)...');
  for (final key in keys) {
    await direct.writeFromStream(
      key,
      Stream.value(payload),
      extension: extension,
    );
  }

  final paths = {
    for (final key in keys) key: store.pathForKey(key, extension: extension),
  };

  final readOnly = await MultiIsolateKvStoreClient.openReadOnly(
    rootDirPath: dataRoot.path,
    folderHierarchyLevels: 1,
  );

  final variants = <String, Future<int> Function(String key)>{
    'isolate_readBytes': (key) async {
      return (await store.readBytes(key, extension: extension)).length;
    },
    'isolate_readStream': (key) async {
      var total = 0;
      await for (final chunk in store.readStream(key, extension: extension)) {
        total += chunk.length;
      }
      return total;
    },
    'isolate_readStream_noExists': (key) async {
      var total = 0;
      await for (final chunk
          in store.readStream(key, extension: extension, checkExists: false)) {
        total += chunk.length;
      }
      return total;
    },
    'readonly_readBytes': (key) async {
      return (await readOnly.readBytes(key, extension: extension)).length;
    },
    'direct_readStream': (key) async {
      var total = 0;
      await for (final chunk in direct.readStream(key, extension: extension)) {
        total += chunk.length;
      }
      return total;
    },
    'raw_openRead': (key) async {
      var total = 0;
      await for (final chunk in File(paths[key]!).openRead()) {
        total += chunk.length;
      }
      return total;
    },
    'raw_readAsBytes': (key) async {
      return (await File(paths[key]!).readAsBytes()).length;
    },
  };

  final results = <Map<String, dynamic>>[];

  for (final entry in variants.entries) {
    final passTotalsUs = <int>[];
    final perFileUs = <int>[];

    for (var pass = 0; pass < warmupPasses + measurePasses; pass++) {
      final sw = Stopwatch()..start();
      for (final key in keys) {
        final fileSw = Stopwatch()..start();
        final bytes = await entry.value(key);
        fileSw.stop();
        if (bytes != fileSize) {
          throw StateError('Expected $fileSize bytes for $key, got $bytes');
        }
        if (pass >= warmupPasses) {
          perFileUs.add(fileSw.elapsedMicroseconds);
        }
      }
      sw.stop();
      if (pass >= warmupPasses) {
        passTotalsUs.add(sw.elapsedMicroseconds);
      }
    }

    final totalStats = LatencyStats('${entry.key}_batch', passTotalsUs);
    final fileStats = LatencyStats('${entry.key}_per_file', perFileUs);

    results.add({
      'variant': entry.key,
      'batch': totalStats.toJson(),
      'perFile': fileStats.toJson(),
      'filesPerSec': fileCount / (totalStats.meanUs / 1e6),
    });

    stdout.writeln(entry.key);
    stdout.writeln(
      '  batch  mean=${totalStats.meanUs.round()}us '
      'p50=${totalStats.p50Us}us (${(fileCount / (totalStats.meanUs / 1e6)).toStringAsFixed(0)} files/s)',
    );
    stdout.writeln(
      '  /file  mean=${fileStats.meanUs.toStringAsFixed(1)}us '
      'p50=${fileStats.p50Us}us p95=${fileStats.p95Us}us',
    );
    stdout.writeln('');
  }

  await store.close();
  await readOnly.close();

  final ranked = List<Map<String, dynamic>>.from(results)
    ..sort(
      (a, b) => (a['batch']['meanUs'] as double)
          .compareTo(b['batch']['meanUs'] as double),
    );

  stdout.writeln('Ranking (fastest batch mean first):');
  for (var i = 0; i < ranked.length; i++) {
    final r = ranked[i];
    final fastest = ranked.first['batch']['meanUs'] as double;
    final ratio = (r['batch']['meanUs'] as double) / fastest;
    stdout.writeln(
      '  ${i + 1}. ${r['variant']} '
      '${(r['batch']['meanUs'] as double).round()}us '
      '(${ratio.toStringAsFixed(2)}x vs fastest)',
    );
  }

  final resultsDir = Directory('results');
  if (!await resultsDir.exists()) {
    await resultsDir.create(recursive: true);
  }
  await File('results/many_small_reads.json').writeAsString(
    const JsonEncoder.withIndent('  ').convert({
      'timestamp': DateTime.now().toUtc().toIso8601String(),
      'platform': Platform.operatingSystem,
      'fileCount': fileCount,
      'fileSizeBytes': fileSize,
      'variants': results,
      'ranking': ranked.map((r) => r['variant']).toList(),
    }),
  );

  stdout.writeln('');
  stdout.writeln('Wrote results/many_small_reads.json');
  await runRoot.delete(recursive: true);
}
