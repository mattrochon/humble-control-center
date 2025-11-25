## Current State (2025-11-24)

- **UI pages**: Home (`/`), Library (`/library`), Item detail (`/item?id=...`), Admin/Control Room (`/admin`).
- **Home** shows downloaded-only category highlights, uses asset `image_url` if present; “See all” links to Library with category preset.
- **Library** lists assets with thumbnail (image_url), rows clickable to detail; columns: Product, Bundle, File, Category, Platform, Ext; filter by category/platform/ext/search/sort.
- **Item detail** shows cover image, description, metadata, download link if on disk, and tags (from `asset_tags`).
- **Admin** has Back-to-Home button, Index/Force re-sync/Download buttons; shows stats, logs, and search table.

- **Metadata flow**:
  - LibraryIndexer collects assets; extracts `image_url` from Humble order/trove payload fields (`tile_image`, icon/image/cover/logo/tile/thumbnail/thumb/visuals) and descriptions from description-like fields.
  - Categories: heuristic map + OpenWebUI classification (categories include game, ebook, comic, audio, video, software, android, archive, key, other, sounds, art, 3d, rpg, rpg maker, unity, unreal).
  - Metadata worker runs every ~2 minutes. Force re-sync (`/api/sync` with `update:true`) triggers immediate metadata pass and overwrites image/description/category as available.
  - AI descriptions via OpenWebUI; category and description writes also add tags (`<category>` and `ai-described`).
  - Highlights are downloaded-only and re-validated against disk.

- **Download fixes**: Standard ThreadPoolExecutor; enlarged HTTP pool; skip marks downloaded if file exists; cache skip requires actual file present; on-shutdown signals handled.

- **DB**: `assets` table includes `image_url`, `description`, `category`; `asset_tags` supports tags; `assets.db` lives in `data/`. Reconciliation marks downloads based on disk paths (bundle/product and fallback).

- **Env/AI**: Uses `OPENWEBUI_URL`, `OPENWEBUI_MODEL`, `OPENWEBUI_API_KEY`. No Ollama fallback. `.env` expected.

- **Endpoints**: `/api/highlights`, `/api/assets`, `/api/assets/{id}`, `/api/assets/{id}/file`, `/api/reclassify`, `/api/sync`, `/api/download`, websocket `/ws/updates`.

## Observed Issues / Next Steps

- Categories still skew to `archive`; need better AI prompting/label mapping and perhaps parsing of Humble product types to tags.
- Many assets lack AI tags despite category set; confirm tag adds and maybe log AI responses.
- Disk locks occurred when app running; copy DB if querying while server is live.
- UI: Home tiles/Library working; ensure Back to Home works (added `goHome()`).
- Metadata refresh now overwrites existing image/description on force re-sync.

## How to Force Metadata Refresh

```bash
# Force sync + metadata refresh
curl -X POST http://127.0.0.1:8000/api/sync \
  -H "Content-Type: application/json" \
  -d '{"update": true}'
```

## Debugging AI/Tags

- Check logs for “Top categories after sync” and “Metadata pass complete...” summaries.
- Item detail shows tags; `ai-described` is added when AI writes a description; category tag equals the AI/heuristic category.
- If tags stay `archive`, inspect AI prompt or add logging of AI responses in `_fill_descriptions_ai` / categorizer.
