# Hashed KV Store Demo

Flutter example that downloads files over HTTP (via Dio) and streams them into
`hashed_kv_store`, with live write progress via `subscribeLive`.

## Run

```bash
cd example
flutter pub get
flutter run
```

## Configuration

- `kFolderHierarchyLevels` in `lib/main.dart` — `1` (default) or `2` for nested folders
- Downloads are stored under the app documents directory in `hashed_kv_store/`
