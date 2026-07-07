import 'dart:async';

import 'kv_exceptions.dart';

/// Cooperative cancellation signal, similar to web [AbortSignal].
class KvAbortSignal {
  bool _aborted = false;
  Object? _reason;
  final _listeners = <void Function()>[];

  /// Whether [KvAbortController.abort] has been called.
  bool get aborted => _aborted;

  /// Optional reason passed to [KvAbortController.abort].
  Object? get reason => _reason;

  /// Registers a listener invoked synchronously when this signal is aborted.
  void onAbort(void Function() listener) {
    if (_aborted) {
      listener();
      return;
    }
    _listeners.add(listener);
  }

  /// Removes a listener previously added with [onAbort].
  void removeAbortListener(void Function() listener) {
    _listeners.remove(listener);
  }

  void _abort([Object? reason]) {
    if (_aborted) return;
    _aborted = true;
    _reason = reason;
    final listeners = List<void Function()>.from(_listeners);
    _listeners.clear();
    for (final listener in listeners) {
      listener();
    }
  }

  /// Throws [KvAbortException] when [aborted] is true.
  void throwIfAborted() {
    if (_aborted) {
      throw KvAbortException(_reason);
    }
  }
}

/// Creates and controls a [KvAbortSignal].
class KvAbortController {
  final KvAbortSignal signal = KvAbortSignal();

  /// Aborts [signal]. Idempotent; later calls are ignored.
  void abort([Object? reason]) {
    signal._abort(reason);
  }
}

/// Races [operation] against [signal] abort.
Future<T> raceWithAbort<T>(Future<T> operation, KvAbortSignal signal) {
  if (signal.aborted) {
    // Swallow the orphaned operation's eventual result/error so it does not
    // surface as an unhandled async error after the caller has moved on.
    operation.ignore();
    return Future.error(KvAbortException(signal.reason));
  }

  final completer = Completer<T>();
  late void Function() listener;

  listener = () {
    if (!completer.isCompleted) {
      completer.completeError(KvAbortException(signal.reason));
    }
  };

  signal.onAbort(listener);
  operation.then(
    (value) {
      if (!completer.isCompleted) {
        completer.complete(value);
      }
    },
    onError: (Object error, StackTrace stackTrace) {
      if (!completer.isCompleted) {
        completer.completeError(error, stackTrace);
      }
    },
  );

  return completer.future.whenComplete(() {
    signal.removeAbortListener(listener);
  });
}
