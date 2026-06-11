/// Aggregated latency statistics in microseconds.
class LatencyStats {
  final String label;
  final List<int> samplesUs;

  LatencyStats(this.label, this.samplesUs);

  int get count => samplesUs.length;

  int get minUs => samplesUs.reduce((a, b) => a < b ? a : b);

  int get maxUs => samplesUs.reduce((a, b) => a > b ? a : b);

  double get meanUs =>
      samplesUs.isEmpty ? 0 : samplesUs.reduce((a, b) => a + b) / count;

  int get p50Us => _percentile(50);

  int get p95Us => _percentile(95);

  double get opsPerSec => meanUs <= 0 ? 0 : 1e6 / meanUs;

  int _percentile(int p) {
    if (samplesUs.isEmpty) return 0;
    final sorted = List<int>.from(samplesUs)..sort();
    final index = ((p / 100) * (sorted.length - 1)).round();
    return sorted[index.clamp(0, sorted.length - 1)];
  }

  Map<String, dynamic> toJson() => {
        'label': label,
        'count': count,
        'meanUs': meanUs,
        'minUs': minUs,
        'maxUs': maxUs,
        'p50Us': p50Us,
        'p95Us': p95Us,
        'opsPerSec': opsPerSec,
      };
}
