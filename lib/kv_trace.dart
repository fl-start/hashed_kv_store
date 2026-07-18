import 'dart:developer' as developer;

/// Opt-in KV store tracing for write-path diagnosis.
///
/// Enable with `--dart-define=KV_STORE_TRACE=true`.
const bool kKvStoreTrace =
    bool.fromEnvironment('KV_STORE_TRACE', defaultValue: false);

const int kKvStoreSlowMs = 5000;

/// Short, log-safe key label (never log full mailbox keys in traces).
String kvTraceKeyLabel(String key, {int tail = 12}) {
  if (key.length <= tail) return key;
  return '…${key.substring(key.length - tail)}';
}

void kvTrace(String event, [Map<String, Object?> data = const {}]) {
  if (!kKvStoreTrace) return;
  if (data.isEmpty) {
    developer.log(event, name: 'hashed_kv_store');
    return;
  }
  developer.log(event, name: 'hashed_kv_store', error: data);
}

void kvTraceSlow(
  String event,
  int ms, [
  Map<String, Object?> data = const {},
]) {
  if (!kKvStoreTrace) return;
  if (ms < kKvStoreSlowMs) return;
  kvTrace('SLOW_$event', {...data, 'ms': ms});
}

/// Per-write trace context. Returns null when [kKvStoreTrace] is false so hot
/// paths avoid Stopwatch / string work.
class KvTraceWrite {
  KvTraceWrite._(this.keyLabel, this._sw);

  final String keyLabel;
  final Stopwatch _sw;

  int get ms => _sw.elapsedMilliseconds;

  static KvTraceWrite? begin(String key) {
    if (!kKvStoreTrace) return null;
    return KvTraceWrite._(kvTraceKeyLabel(key), Stopwatch()..start());
  }

  void event(String name, Map<String, Object?> data) {
    kvTrace(name, {...data, 'key': keyLabel});
  }

  void slow(String name, Map<String, Object?> data) {
    kvTraceSlow(name, ms, {...data, 'key': keyLabel});
  }

  void eventWithMs(String name, Map<String, Object?> data) {
    kvTrace(name, {...data, 'key': keyLabel, 'ms': ms});
  }
}
