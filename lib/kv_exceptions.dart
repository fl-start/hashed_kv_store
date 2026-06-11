import 'dart:io';
import 'dart:isolate';

/// Thrown when a value for a given key does not exist on disk.
class KvNotFoundException implements Exception {
  final String key;
  final String extension;

  KvNotFoundException(this.key, this.extension);

  @override
  String toString() =>
      'KvNotFoundException: No value found for key="$key" (.$extension)';
}

/// Thrown when on-disk layout metadata does not match the package layout version.
class KvLayoutMismatchException implements Exception {
  final String rootDirPath;
  final int? storedVersion;
  final int expectedVersion;

  KvLayoutMismatchException({
    required this.rootDirPath,
    required this.storedVersion,
    required this.expectedVersion,
  });

  @override
  String toString() =>
      'KvLayoutMismatchException: layout version mismatch at "$rootDirPath" '
      '(stored: $storedVersion, expected: $expectedVersion)';
}

/// Thrown when a write operation fails in a worker or folder isolate.
class KvWriteException implements Exception {
  final String message;
  final String? type;
  final String? stackTrace;

  KvWriteException(
    this.message, {
    this.type,
    this.stackTrace,
  });

  @override
  String toString() {
    final buffer = StringBuffer('KvWriteException: $message');
    if (type != null) {
      buffer.write(' ($type)');
    }
    if (stackTrace != null) {
      buffer.write('\n$stackTrace');
    }
    return buffer.toString();
  }
}

void kvSendError(SendPort? port, Object error, [StackTrace? stackTrace]) {
  port?.send(<String, dynamic>{
    'error': error.toString(),
    'type': error.runtimeType.toString(),
    if (stackTrace != null) 'stackTrace': stackTrace.toString(),
  });
}

void kvSendOk(SendPort? port, [Map<String, dynamic>? extra]) {
  port?.send(<String, dynamic>{'ok': true, ...?extra});
}

/// Whether [error] indicates the target path does not exist.
bool kvIsPathNotFound(FileSystemException error) {
  final code = error.osError?.errorCode;
  if (code == 2 || code == 3) {
    return true;
  }
  final message = error.message.toLowerCase();
  return message.contains('no such file') ||
      message.contains('not found') ||
      message.contains('cannot find');
}

/// Whether a read/open error indicates the target path does not exist.
bool kvIsNotFoundError(Object error) {
  if (error is PathNotFoundException) {
    return true;
  }
  return error is FileSystemException && kvIsPathNotFound(error);
}

void kvThrowIfError(Map<dynamic, dynamic> response) {
  final error = response['error'];
  if (error != null) {
    throw KvWriteException(
      error.toString(),
      type: response['type'] as String?,
      stackTrace: response['stackTrace'] as String?,
    );
  }
}
