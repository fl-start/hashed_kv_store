/// LRU cache for resolved on-disk paths keyed by `key::extension`.
class KvPathCache {
  final int maxEntries;
  final _cache = <String, String>{};

  KvPathCache({this.maxEntries = 4096});

  String getOrCompute(String cacheKey, String Function() compute) {
    final hit = _cache[cacheKey];
    if (hit != null) {
      _cache.remove(cacheKey);
      _cache[cacheKey] = hit;
      return hit;
    }

    final path = compute();
    if (_cache.length >= maxEntries) {
      _cache.remove(_cache.keys.first);
    }
    _cache[cacheKey] = path;
    return path;
  }

  void clear() => _cache.clear();
}
