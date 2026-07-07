# Change: Add abort/cancellation support

## Why

Callers need to cancel in-flight reads and writes (e.g. HTTP download cancellation, user navigation) without leaving partial truncate files or blocking same-key write queues.

## What Changes

- `KvAbortController`, `KvAbortSignal`, `KvAbortException`
- Optional `signal` on read/write APIs
- Router `abortWrite`, `openRead`, `cancelRead` message types
- Worker write abort cleanup and routed read streaming

## Impact

- Affected specs: `kv-store`
- Affected code: `lib/kv_abort.dart`, `lib/multi_isolate_kv_store_client.dart`, `lib/kv_router_isolate.dart`, `lib/kv_worker_isolate.dart`, `lib/kv_direct_io.dart`
