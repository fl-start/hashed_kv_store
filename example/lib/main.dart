import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:dio/dio.dart';
import 'package:flutter/material.dart';
import 'package:hashed_kv_store/hashed_kv_store.dart';
import 'package:path_provider/path_provider.dart';

/// Folder nesting for stored files when spawning [MultiIsolateKvStoreClient].
///
/// Use `1` for the package default (one subdirectory under the root).
/// Set to `2` for two nested folder levels when your app needs deeper sharding.
const int kFolderHierarchyLevels = 1;

void main() {
  runApp(const MyApp());
}

class MyApp extends StatelessWidget {
  const MyApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'Hashed KV Store Demo',
      theme: ThemeData(
        colorScheme: ColorScheme.fromSeed(seedColor: Colors.deepPurple),
        useMaterial3: true,
      ),
      home: const DownloadScreen(),
    );
  }
}

class DownloadScreen extends StatefulWidget {
  const DownloadScreen({super.key});

  @override
  State<DownloadScreen> createState() => _DownloadScreenState();
}

class _DownloadScreenState extends State<DownloadScreen> {
  MultiIsolateKvStoreClient? _store;
  final Dio _dio = Dio();
  bool _initialized = false;
  String? _error;
  String? _storagePath;

  // Download state
  String? _currentDownloadKey;
  int _downloadedBytes = 0;
  int _totalBytes = 0;
  bool _isDownloading = false;
  String? _downloadStatus;
  final List<String> _liveChunks = [];
  StreamSubscription<List<int>>? _liveSubscription;

  // Test download URLs (real files)
  final List<DownloadItem> _downloadItems = [
    DownloadItem(
      name: 'Sample JSON File',
      url: 'https://jsonplaceholder.typicode.com/posts/1',
      key: 'json:post:1',
      extension: 'json',
    ),
    DownloadItem(
      name: 'Sample Text File',
      url:
          'https://www.w3.org/WAI/ER/tests/xhtml/testfiles/resources/pdf/dummy.pdf',
      key: 'test:pdf',
      extension: 'pdf',
    ),
    DownloadItem(
      name: 'Sample Image (Small)',
      url: 'https://picsum.photos/200/300',
      key: 'image:small',
      extension: 'jpg',
    ),
    DownloadItem(
      name: 'Sample Image (Large)',
      url: 'https://picsum.photos/1024/768',
      key: 'image:large',
      extension: 'jpg',
    ),
    DownloadItem(
      name: 'Sample Text Data',
      url:
          'https://www.learningcontainer.com/wp-content/uploads/2020/04/sample-text-file.txt',
      key: 'text:sample',
      extension: 'txt',
    ),
  ];

  @override
  void initState() {
    super.initState();
    _initializeStore();
  }

  Future<void> _initializeStore() async {
    try {
      final directory = await getApplicationDocumentsDirectory();
      final storageDir = Directory('${directory.path}/hashed_kv_store');
      await storageDir.create(recursive: true);

      _store = await MultiIsolateKvStoreClient.spawn(
        rootDirPath: storageDir.path,
        numWriteWorkers: 4,
        folderHierarchyLevels: kFolderHierarchyLevels,
      );

      setState(() {
        _initialized = true;
        _storagePath = storageDir.path;
        _error = null;
      });
    } catch (e) {
      setState(() {
        _error = 'Failed to initialize store: $e';
        _initialized = false;
      });
    }
  }

  Future<void> _downloadFile(DownloadItem item) async {
    if (_store == null || _isDownloading) return;

    setState(() {
      _isDownloading = true;
      _currentDownloadKey = item.key;
      _downloadedBytes = 0;
      _totalBytes = 0;
      _downloadStatus = 'Starting download...';
      _liveChunks.clear();
    });

    try {
      // Subscribe to live updates before starting download
      final liveStream = _store!.subscribeLive(
        item.key,
        extension: item.extension,
      );
      _liveSubscription?.cancel();
      _liveSubscription = liveStream.listen(
        (chunk) {
          setState(() {
            _downloadedBytes += chunk.length;
            _liveChunks.add(
              'Chunk: ${chunk.length} bytes at ${DateTime.now().toString().substring(11, 19)}',
            );
            if (_liveChunks.length > 10) {
              _liveChunks.removeAt(0);
            }
          });
        },
        onDone: () {
          setState(() {
            _downloadStatus = 'Download complete!';
          });
        },
      );

      // Wait for subscription to register
      await Future.delayed(const Duration(milliseconds: 100));

      setState(() {
        _downloadStatus = 'Connecting...';
      });

      // Download file
      final response = await _dio.get<ResponseBody>(
        item.url,
        options: Options(
          responseType: ResponseType.stream,
          followRedirects: true,
          receiveTimeout: const Duration(seconds: 30),
        ),
        onReceiveProgress: (received, total) {
          if (total != -1) {
            setState(() {
              _totalBytes = total;
            });
          }
        },
      );

      setState(() {
        _downloadStatus = 'Downloading...';
      });

      // Pipe HTTP stream to KV store
      await _store!.writeFromStream(
        item.key,
        response.data!.stream,
        extension: item.extension,
      );

      setState(() {
        _downloadStatus = 'Saved to store!';
        _isDownloading = false;
      });

      // Wait a bit for live subscription to finish
      await Future.delayed(const Duration(milliseconds: 500));
    } catch (e) {
      setState(() {
        _error = 'Download failed: $e';
        _isDownloading = false;
        _downloadStatus = 'Error: $e';
      });
      _liveSubscription?.cancel();
    }
  }

  Future<void> _readFile(String key, String extension) async {
    if (_store == null) return;

    try {
      setState(() {
        _downloadStatus = 'Reading file...';
      });

      final readStream = _store!.readStream(key, extension: extension);
      final bytes = <int>[];
      await for (final chunk in readStream) {
        bytes.addAll(chunk);
      }

      setState(() {
        _downloadStatus = 'File size: ${bytes.length} bytes';
        _totalBytes = bytes.length;
      });

      // Show file content preview (first 500 chars for text files)
      if (extension == 'json' || extension == 'txt') {
        final content = utf8.decode(bytes);
        final preview = content.length > 500
            ? '${content.substring(0, 500)}...'
            : content;
        _showFilePreview(key, preview);
      } else {
        _showFilePreview(key, 'Binary file (${bytes.length} bytes)');
      }
    } catch (e) {
      setState(() {
        _error = 'Read failed: $e';
      });
    }
  }

  void _showFilePreview(String key, String content) {
    showDialog(
      context: context,
      builder: (context) => AlertDialog(
        title: Text('File: $key'),
        content: SingleChildScrollView(child: Text(content)),
        actions: [
          TextButton(
            onPressed: () => Navigator.pop(context),
            child: const Text('Close'),
          ),
        ],
      ),
    );
  }

  Future<void> _deleteFile(String key, String extension) async {
    if (_store == null) return;

    try {
      await _store!.delete(key, extension: extension);
      setState(() {
        _downloadStatus = 'File deleted';
      });
    } catch (e) {
      setState(() {
        _error = 'Delete failed: $e';
      });
    }
  }

  @override
  void dispose() {
    _liveSubscription?.cancel();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        backgroundColor: Theme.of(context).colorScheme.inversePrimary,
        title: const Text('Hashed KV Store Demo'),
      ),
      body: _initialized
          ? _buildContent()
          : Center(
              child: _error != null
                  ? Column(
                      mainAxisAlignment: MainAxisAlignment.center,
                      children: [
                        const Icon(Icons.error, size: 48, color: Colors.red),
                        const SizedBox(height: 16),
                        Text(_error!),
                        const SizedBox(height: 16),
                        ElevatedButton(
                          onPressed: _initializeStore,
                          child: const Text('Retry'),
                        ),
                      ],
                    )
                  : const Column(
                      mainAxisAlignment: MainAxisAlignment.center,
                      children: [
                        CircularProgressIndicator(),
                        SizedBox(height: 16),
                        Text('Initializing store...'),
                      ],
                    ),
            ),
    );
  }

  Widget _buildContent() {
    return SingleChildScrollView(
      padding: const EdgeInsets.all(16),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.stretch,
        children: [
          // Storage info
          Card(
            child: Padding(
              padding: const EdgeInsets.all(16),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  const Text(
                    'Storage Info',
                    style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold),
                  ),
                  const SizedBox(height: 8),
                  Text('Path: $_storagePath'),
                  Text('Folder hierarchy levels: $kFolderHierarchyLevels'),
                  const SizedBox(height: 8),
                  if (_downloadStatus != null)
                    Text(
                      'Status: $_downloadStatus',
                      style: const TextStyle(fontWeight: FontWeight.w500),
                    ),
                ],
              ),
            ),
          ),
          const SizedBox(height: 16),

          // Download progress
          if (_isDownloading || _downloadedBytes > 0)
            Card(
              color: Colors.blue.shade50,
              child: Padding(
                padding: const EdgeInsets.all(16),
                child: Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    const Text(
                      'Download Progress',
                      style: TextStyle(
                        fontSize: 18,
                        fontWeight: FontWeight.bold,
                      ),
                    ),
                    const SizedBox(height: 8),
                    if (_totalBytes > 0)
                      LinearProgressIndicator(
                        value: _downloadedBytes / _totalBytes,
                      ),
                    const SizedBox(height: 8),
                    Text('Downloaded: ${_formatBytes(_downloadedBytes)}'),
                    if (_totalBytes > 0)
                      Text('Total: ${_formatBytes(_totalBytes)}'),
                    if (_liveChunks.isNotEmpty) ...[
                      const SizedBox(height: 16),
                      const Text(
                        'Live Chunks:',
                        style: TextStyle(fontWeight: FontWeight.bold),
                      ),
                      const SizedBox(height: 4),
                      ...(_liveChunks
                          .take(5)
                          .map(
                            (chunk) => Padding(
                              padding: const EdgeInsets.only(bottom: 2),
                              child: Text(
                                chunk,
                                style: const TextStyle(fontSize: 12),
                              ),
                            ),
                          )),
                    ],
                  ],
                ),
              ),
            ),
          if (_isDownloading || _downloadedBytes > 0)
            const SizedBox(height: 16),

          // Error display
          if (_error != null)
            Card(
              color: Colors.red.shade50,
              child: Padding(
                padding: const EdgeInsets.all(16),
                child: Row(
                  children: [
                    const Icon(Icons.error, color: Colors.red),
                    const SizedBox(width: 8),
                    Expanded(child: Text(_error!)),
                    IconButton(
                      icon: const Icon(Icons.close),
                      onPressed: () => setState(() => _error = null),
                    ),
                  ],
                ),
              ),
            ),
          if (_error != null) const SizedBox(height: 16),

          // Download buttons
          const Text(
            'Download Files',
            style: TextStyle(fontSize: 20, fontWeight: FontWeight.bold),
          ),
          const SizedBox(height: 8),
          ..._downloadItems.map((item) => _buildDownloadCard(item)),
        ],
      ),
    );
  }

  Widget _buildDownloadCard(DownloadItem item) {
    final isCurrentDownload = _currentDownloadKey == item.key;
    return Card(
      margin: const EdgeInsets.only(bottom: 12),
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Row(
              children: [
                Expanded(
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      Text(
                        item.name,
                        style: const TextStyle(
                          fontSize: 16,
                          fontWeight: FontWeight.bold,
                        ),
                      ),
                      const SizedBox(height: 4),
                      Text(
                        item.url,
                        style: TextStyle(fontSize: 12, color: Colors.grey[600]),
                        maxLines: 1,
                        overflow: TextOverflow.ellipsis,
                      ),
                      Text(
                        'Key: ${item.key}.${item.extension}',
                        style: TextStyle(fontSize: 12, color: Colors.grey[600]),
                      ),
                    ],
                  ),
                ),
              ],
            ),
            const SizedBox(height: 12),
            Wrap(
              spacing: 8,
              runSpacing: 8,
              children: [
                ElevatedButton.icon(
                  onPressed: _isDownloading && !isCurrentDownload
                      ? null
                      : () => _downloadFile(item),
                  icon: const Icon(Icons.download),
                  label: Text(
                    isCurrentDownload && _isDownloading
                        ? 'Downloading...'
                        : 'Download',
                  ),
                ),
                OutlinedButton.icon(
                  onPressed: _isDownloading
                      ? null
                      : () => _readFile(item.key, item.extension),
                  icon: const Icon(Icons.read_more),
                  label: const Text('Read'),
                ),
                OutlinedButton.icon(
                  onPressed: _isDownloading
                      ? null
                      : () => _deleteFile(item.key, item.extension),
                  icon: const Icon(Icons.delete),
                  label: const Text('Delete'),
                  style: OutlinedButton.styleFrom(foregroundColor: Colors.red),
                ),
              ],
            ),
          ],
        ),
      ),
    );
  }

  String _formatBytes(int bytes) {
    if (bytes < 1024) return '$bytes B';
    if (bytes < 1024 * 1024) return '${(bytes / 1024).toStringAsFixed(2)} KB';
    return '${(bytes / (1024 * 1024)).toStringAsFixed(2)} MB';
  }
}

class DownloadItem {
  final String name;
  final String url;
  final String key;
  final String extension;

  DownloadItem({
    required this.name,
    required this.url,
    required this.key,
    required this.extension,
  });
}
