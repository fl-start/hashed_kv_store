import 'package:path/path.dart' as p;

/// On-disk layout version. Bump only when storage format changes incompatibly.
const int kKvStoreLayoutVersion = 1;

const String kKvStoreMetaFileName = '.hashed_kv_meta.json';

/// Absolute path to the layout metadata file under [rootDirPath].
String kvStoreMetaFilePath(String rootDirPath) {
  return p.join(rootDirPath, kKvStoreMetaFileName);
}
