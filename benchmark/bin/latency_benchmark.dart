import 'dart:convert';
import 'dart:io';

import 'package:hashed_kv_store/hashed_kv_store.dart';
import 'package:hashed_kv_store_benchmark/direct_kv_io.dart';
import 'package:hashed_kv_store_benchmark/stats.dart';

const _warmupIterations = 5;
const _measureIterations = 50;
const _extension = 'bin';

Future<void> main() async {
  final runRoot = await Directory.systemTemp.createTemp('kv_bench_run_');
  final isolateRoot = Directory('${runRoot.path}/isolate');
  final directRoot = Directory('${runRoot.path}/direct');
  await isolateRoot.create(recursive: true);
  await directRoot.create(recursive: true);

  stdout.writeln('Hashed KV Store — isolate vs direct disk I/O latency');
  stdout.writeln('Run root: ${runRoot.path}');
  stdout.writeln('Warmup: $_warmupIterations  Measure: $_measureIterations');
  stdout.writeln('');

  final store = await MultiIsolateKvStoreClient.spawn(
    rootDirPath: isolateRoot.path,
    numWriteWorkers: 2,
    folderHierarchyLevels: 1,
    writeMaxInFlightChunks: 8,
    writeIdlePurgeDuration: const Duration(days: 1),
  );

  final direct = KvDirectIo(
    rootDirPath: directRoot.path,
    folderHierarchyLevels: 1,
  );

  final scenarios = <_Scenario>[
    _Scenario(
      id: 'W1',
      name: 'write_256b',
      payload: [payloadOfSize(256)],
    ),
    _Scenario(
      id: 'W2',
      name: 'write_4k',
      payload: [payloadOfSize(4 * 1024)],
    ),
    _Scenario(
      id: 'W3',
      name: 'write_64k',
      payload: [payloadOfSize(64 * 1024)],
    ),
    _Scenario(
      id: 'W4',
      name: 'write_1m_16chunks',
      payload: chunkedPayload(16, 64 * 1024),
    ),
  ];

  final results = <Map<String, dynamic>>[];

  for (final scenario in scenarios) {
    final isolateStats = await _benchmarkWrites(
      label: 'isolate:${scenario.name}',
      iterations: _warmupIterations + _measureIterations,
      warmup: _warmupIterations,
      run: (i) => store.writeFromStream(
        'bench:${scenario.name}:$i',
        Stream<List<int>>.fromIterable(scenario.payload),
        extension: _extension,
      ),
    );

    final directStats = await _benchmarkWrites(
      label: 'direct:${scenario.name}',
      iterations: _warmupIterations + _measureIterations,
      warmup: _warmupIterations,
      run: (i) => direct.writeFromStream(
        'bench:${scenario.name}:$i',
        Stream<List<int>>.fromIterable(scenario.payload),
        extension: _extension,
      ),
    );

    results.add(_resultRow(scenario, isolateStats, directStats));
    _printRow(scenario, isolateStats, directStats);
  }

  // R1: read 4 KiB (isolate readStream is already caller-isolate I/O)
  const readKey = 'bench:read:seed';
  await store.writeFromStream(
    readKey,
    Stream.value(payloadOfSize(4 * 1024)),
    extension: _extension,
  );
  await direct.writeFromStream(
    readKey,
    Stream.value(payloadOfSize(4 * 1024)),
    extension: _extension,
  );

  final isolateRead = await _benchmarkReads(
    label: 'isolate:read_4k',
    iterations: _warmupIterations + _measureIterations,
    warmup: _warmupIterations,
    run: () async {
      final bytes = <int>[];
      await for (final chunk in store.readStream(readKey, extension: _extension)) {
        bytes.addAll(chunk);
      }
    },
  );

  final directRead = await _benchmarkReads(
    label: 'direct:read_4k',
    iterations: _warmupIterations + _measureIterations,
    warmup: _warmupIterations,
    run: () async {
      final bytes = <int>[];
      await for (final chunk in direct.readStream(readKey, extension: _extension)) {
        bytes.addAll(chunk);
      }
    },
  );

  final readScenario = _Scenario(id: 'R1', name: 'read_4k', payload: const []);
  results.add(_resultRow(readScenario, isolateRead, directRead));
  _printRow(readScenario, isolateRead, directRead);

  // D1: delete after 4 KiB write
  final isolateDelete = await _benchmarkDeletes(
    label: 'isolate:delete_4k',
    iterations: _warmupIterations + _measureIterations,
    warmup: _warmupIterations,
    prepare: (i) => store.writeFromStream(
      'bench:delete:$i',
      Stream.value(payloadOfSize(4 * 1024)),
      extension: _extension,
    ),
    run: (i) => store.delete('bench:delete:$i', extension: _extension),
  );

  final directDelete = await _benchmarkDeletes(
    label: 'direct:delete_4k',
    iterations: _warmupIterations + _measureIterations,
    warmup: _warmupIterations,
    prepare: (i) => direct.writeFromStream(
      'bench:delete:$i',
      Stream.value(payloadOfSize(4 * 1024)),
      extension: _extension,
    ),
    run: (i) => direct.delete('bench:delete:$i', extension: _extension),
  );

  final deleteScenario = _Scenario(id: 'D1', name: 'delete_4k', payload: const []);
  results.add(_resultRow(deleteScenario, isolateDelete, directDelete));
  _printRow(deleteScenario, isolateDelete, directDelete);

  await store.close();

  final resultsDir = Directory('results');
  if (!await resultsDir.exists()) {
    await resultsDir.create(recursive: true);
  }
  final output = {
    'timestamp': DateTime.now().toUtc().toIso8601String(),
    'platform': Platform.operatingSystem,
    'warmupIterations': _warmupIterations,
    'measureIterations': _measureIterations,
    'scenarios': results,
  };
  await File('results/latest.json').writeAsString(
    const JsonEncoder.withIndent('  ').convert(output),
  );

  stdout.writeln('');
  stdout.writeln('Wrote results/latest.json');

  await runRoot.delete(recursive: true);
}

class _Scenario {
  final String id;
  final String name;
  final List<List<int>> payload;

  const _Scenario({
    required this.id,
    required this.name,
    required this.payload,
  });
}

Future<LatencyStats> _benchmarkWrites({
  required String label,
  required int iterations,
  required int warmup,
  required Future<void> Function(int index) run,
}) async {
  final samples = <int>[];
  for (var i = 0; i < iterations; i++) {
    final sw = Stopwatch()..start();
    await run(i);
    sw.stop();
    if (i >= warmup) {
      samples.add(sw.elapsedMicroseconds);
    }
  }
  return LatencyStats(label, samples);
}

Future<LatencyStats> _benchmarkReads({
  required String label,
  required int iterations,
  required int warmup,
  required Future<void> Function() run,
}) async {
  final samples = <int>[];
  for (var i = 0; i < iterations; i++) {
    final sw = Stopwatch()..start();
    await run();
    sw.stop();
    if (i >= warmup) {
      samples.add(sw.elapsedMicroseconds);
    }
  }
  return LatencyStats(label, samples);
}

Future<LatencyStats> _benchmarkDeletes({
  required String label,
  required int iterations,
  required int warmup,
  required Future<void> Function(int index) prepare,
  required Future<void> Function(int index) run,
}) async {
  final samples = <int>[];
  for (var i = 0; i < iterations; i++) {
    await prepare(i);
    final sw = Stopwatch()..start();
    await run(i);
    sw.stop();
    if (i >= warmup) {
      samples.add(sw.elapsedMicroseconds);
    }
  }
  return LatencyStats(label, samples);
}

Map<String, dynamic> _resultRow(
  _Scenario scenario,
  LatencyStats isolate,
  LatencyStats direct,
) {
  final overhead = direct.meanUs > 0 ? isolate.meanUs / direct.meanUs : 0;
  return {
    'id': scenario.id,
    'name': scenario.name,
    'isolate': isolate.toJson(),
    'direct': direct.toJson(),
    'overheadRatio': overhead,
  };
}

void _printRow(_Scenario scenario, LatencyStats isolate, LatencyStats direct) {
  final overhead = direct.meanUs > 0 ? isolate.meanUs / direct.meanUs : 0;
  stdout.writeln(
    '${scenario.id} ${scenario.name.padRight(20)} '
    'isolate p50=${isolate.p50Us}us direct p50=${direct.p50Us}us '
    'overhead=${overhead.toStringAsFixed(2)}x',
  );
}
