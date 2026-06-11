import 'dart:typed_data';

export 'package:hashed_kv_store/kv_direct_io.dart';

/// Build a single-chunk payload of [size] bytes.
Uint8List payloadOfSize(int size) {
  return Uint8List.fromList(List.generate(size, (i) => i & 0xff));
}

/// Build [chunkCount] chunks of [chunkSize] bytes each.
List<Uint8List> chunkedPayload(int chunkCount, int chunkSize) {
  return List.generate(chunkCount, (_) => payloadOfSize(chunkSize));
}
