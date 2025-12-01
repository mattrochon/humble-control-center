## Current State (2025-11-27)

- **UI pages**: Home, Library, Item detail, Admin/Control Room, Settings. Home shows a Bundles section (opens Library filtered by bundle) and downloaded-only category highlights.
- **Library**: filters by bundle/category/platform/ext/search/sort, toggle to include non-downloaded, shows tags/status. Item detail shows activation keys when present (no bad download link).
- **Admin**: stats/logs, sync/download controls, AI reclassify, wizard, debug panels for purchases/orders/bundle JSON.

- **Collection/metadata**:
  - LibraryIndexer handles orders without subproducts, collects activation-key-only products, and gathers all platform downloads. Images prefer purchase icon/image/tile (filtered to avoid torrent/zip URLs); game/bundle API used as fallback. Force metadata scans up to 500 assets.
  - Categories: heuristics + OpenWebUI; AI descriptions add tags; category tag backfill.
  - Highlights: downloaded-only, validated against disk.

- **Downloads**: ThreadPoolExecutor with tuned HTTP pool; skips mark downloaded; download failures recorded to `download_error`.

- **DB**: `assets` includes `activation_key`, `download_error`, `image_url`, `description`, `category`; `asset_tags` for tags; `assets.db` in `data/`.

- **Auth/AI**: `_simpleauth_sess` required; OpenWebUI (URL/model/API key) needed for AI; no Ollama fallback.

- **Endpoints**: `/api/highlights`, `/api/bundles`, `/api/assets`, `/api/assets/{id}`, `/api/assets/{id}/file`, `/api/reclassify`, `/api/sync`, `/api/download`, `/api/debug/purchases`, `/api/debug/orders`, websocket `/ws/updates`.

## Outstanding / Issues

- Activation-key items still need download/instruction text surfaced alongside the key.
- Image quality still limited to purchase icons/game/bundle lookups; some items may lack high-res covers.
- Categories can skew to `archive`; prompt/label tuning may be needed.

## How to Force Metadata Refresh

```bash
curl -X POST http://127.0.0.1:8000/api/sync \
  -H "Content-Type: application/json" \
  -d '{"update": true}'
```
