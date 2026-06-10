import 'dart:convert';

import 'package:crypto/crypto.dart';
import 'package:path/path.dart' as p;

/// Shared SHA256 → Crockford Base32 path mapping for the hashed KV store.
class HashedKvPath {
  static const String crockfordBase32Lower = '0123456789abcdefghjkmnpqrstvwxyz';

  /// Crockford Base32 encoding of the SHA256 digest for [key].
  static String crockfordBase32ForKey(String key) {
    final digestBytes = sha256.convert(utf8.encode(key)).bytes;
    final out = StringBuffer();

    var bitBuffer = 0;
    var bitCount = 0;
    for (final byte in digestBytes) {
      bitBuffer = (bitBuffer << 8) | byte;
      bitCount += 8;
      while (bitCount >= 5) {
        final index = (bitBuffer >> (bitCount - 5)) & 0x1f;
        out.write(crockfordBase32Lower[index]);
        bitCount -= 5;
      }
    }

    if (bitCount > 0) {
      final index = (bitBuffer << (5 - bitCount)) & 0x1f;
      out.write(crockfordBase32Lower[index]);
    }

    return out.toString();
  }

  /// Relative path from [rootDirPath] for [key] with [extension].
  ///
  /// [hierarchyLevels]: 0, 1, or 2
  ///   0: `<cccc-cccc-cccc-cccc>.<ext>`
  ///   1: `<cc>/<cccc-cccc-cccc-cccc>.<ext>`
  ///   2: `<cc>/<cc>/<cccc-cccc-cccc-cccc>.<ext>`
  static String relativePathForKey(
    String key,
    String extension,
    int hierarchyLevels,
  ) {
    return _relativePathFromDigest(
      crockfordBase32ForKey(key),
      extension,
      hierarchyLevels,
    );
  }

  /// Absolute file path for [key] under [rootDirPath].
  static String pathForKey(
    String rootDirPath,
    String key,
    String extension,
    int hierarchyLevels,
  ) {
    return pathsForKey(
      rootDirPath,
      key,
      extension,
      hierarchyLevels,
    ).filePath;
  }

  /// Folder path that must exist before writing [key].
  static String folderPathForKey(
    String rootDirPath,
    String key,
    int hierarchyLevels,
  ) {
    return pathsForKey(
      rootDirPath,
      key,
      '',
      hierarchyLevels,
    ).folderPath;
  }

  /// Folder and file paths for [key], computed from a single digest.
  static ({String folderPath, String filePath}) pathsForKey(
    String rootDirPath,
    String key,
    String extension,
    int hierarchyLevels,
  ) {
    final digestBase32 = crockfordBase32ForKey(key);
    final relative = _relativePathFromDigest(
      digestBase32,
      extension,
      hierarchyLevels,
    );
    return (
      folderPath: _folderPathFromDigest(
        rootDirPath,
        digestBase32,
        hierarchyLevels,
      ),
      filePath: p.join(rootDirPath, relative),
    );
  }

  static String _relativePathFromDigest(
    String digestBase32,
    String extension,
    int hierarchyLevels,
  ) {
    final stem = digestBase32.substring(4, 20);
    final fileStem =
        '${stem.substring(0, 4)}-${stem.substring(4, 8)}-${stem.substring(8, 12)}-${stem.substring(12, 16)}';
    final fileName = extension.isEmpty
        ? fileStem
        : '$fileStem.$extension'.replaceAll('..', '.');

    if (hierarchyLevels == 0) {
      return fileName;
    } else if (hierarchyLevels == 1) {
      final level1 = digestBase32.substring(0, 2);
      return p.join(level1, fileName);
    } else {
      final level1 = digestBase32.substring(0, 2);
      final level2 = digestBase32.substring(2, 4);
      return p.join(level1, level2, fileName);
    }
  }

  static String _folderPathFromDigest(
    String rootDirPath,
    String digestBase32,
    int hierarchyLevels,
  ) {
    if (hierarchyLevels == 0) {
      return rootDirPath;
    }
    if (hierarchyLevels == 1) {
      final level1 = digestBase32.substring(0, 2);
      return p.join(rootDirPath, level1);
    }
    final level1 = digestBase32.substring(0, 2);
    final level2 = digestBase32.substring(2, 4);
    return p.join(rootDirPath, level1, level2);
  }
}
