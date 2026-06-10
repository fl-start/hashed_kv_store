## 0.1.0

- Multi-isolate write architecture with per-key serialization and sharded workers
- Atomic truncate writes via temp file + rename
- Direct client-to-worker chunk routing with `TransferableTypedData`
- Write backpressure, configurable flush tuning, and optional `fsyncOnClose`
- `KvNotFoundException` / `KvWriteException` error types
- Delete commands queued behind active writes; worker respawn after idle purge
- Client stores `rootDirPath`; `readStream(key)` and `pathForKey(key)` convenience APIs

## 0.0.1

- Initial package scaffold
