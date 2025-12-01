import asyncio
import json
import logging
import os
import signal
import queue
import threading
import time
from pathlib import Path
from typing import List, Optional
from contextlib import suppress

import requests
import parsel
import uvicorn
from fastapi import FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse, JSONResponse
from pydantic import BaseModel

from .asset_db import AssetDB
from .download_library import DownloadLibrary
from .library_index import LibraryIndexer, AssetCategorizer, _clean_name
from .state import UIState, default_data_dir

logger = logging.getLogger(__name__)


class SessionPayload(BaseModel):
    cookie: str


class ConfigPayload(BaseModel):
    library_path: str
    include: List[str] = []
    exclude: List[str] = []
    platforms: List[str] = []
    trove: bool = False


class SyncPayload(BaseModel):
    update: bool = False  # when true, force metadata refresh
    trove: Optional[bool] = None


class TagPayload(BaseModel):
    tags: List[str]


class ReclassifyPayload(BaseModel):
    asset_ids: Optional[List[int]] = None


class SettingsPayload(BaseModel):
    session_cookie: Optional[str] = None
    library_path: Optional[str] = None
    include: Optional[List[str]] = None
    exclude: Optional[List[str]] = None
    platforms: Optional[List[str]] = None
    trove: Optional[bool] = None
    openwebui_url: Optional[str] = None
    openwebui_model: Optional[str] = None
    openwebui_api_key: Optional[str] = None
    auth_header_name: Optional[str] = None
    auth_header_value: Optional[str] = None


class EventBus:
    def __init__(self):
        self._subscribers: List[queue.Queue] = []
        self._async_subscribers: List[tuple[asyncio.Queue, asyncio.AbstractEventLoop]] = []
        self._lock = threading.Lock()

    def subscribe(self) -> queue.Queue:
        q: queue.Queue = queue.Queue()
        with self._lock:
            self._subscribers.append(q)
        return q

    def unsubscribe(self, q: queue.Queue):
        with self._lock:
            if q in self._subscribers:
                self._subscribers.remove(q)

    def subscribe_async(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue()
        loop = asyncio.get_running_loop()
        with self._lock:
            self._async_subscribers.append((q, loop))
        return q

    def unsubscribe_async(self, q: asyncio.Queue):
        with self._lock:
            self._async_subscribers = [
                pair for pair in self._async_subscribers if pair[0] is not q
            ]

    def publish(self, event: dict):
        with self._lock:
            targets = list(self._subscribers)
            async_targets = list(self._async_subscribers)
        for q in targets:
            q.put(event)
        for aq, loop in async_targets:
            if loop.is_closed():
                continue
            try:
                loop.call_soon_threadsafe(aq.put_nowait, event)
            except RuntimeError:
                # Loop likely closing.
                pass

    def stop_all(self):
        # Push a sentinel to all queues so listeners exit promptly.
        with self._lock:
            targets = list(self._subscribers)
            async_targets = list(self._async_subscribers)
        for q in targets:
            q.put({"type": "__shutdown__"})
        for aq, loop in async_targets:
            if loop.is_closed():
                continue
            try:
                loop.call_soon_threadsafe(aq.put_nowait, {"type": "__shutdown__"})
            except RuntimeError:
                pass


class Coordinator:
    def __init__(self, state: UIState, db: AssetDB, events: EventBus):
        self.state = state
        self.db = db
        self.events = events
        self.syncing = False
        self.downloading = False
        self.last_sync: float | None = None
        self.last_download: float | None = None
        self.log_lines: List[str] = []
        self.download_total = 0
        self.download_done = 0
        self.download_skipped = 0
        self._lock = threading.Lock()
        self._threads: List[threading.Thread] = []
        self.stop_event = threading.Event()
        self._download_failures = 0
        self._metadata_thread: threading.Thread | None = None
        self._start_metadata_worker()

    def _session(self) -> requests.Session:
        if not self.state.ready():
            raise RuntimeError("Session cookie and library path required.")
        session = requests.Session()
        session.headers.update(
            {"cookie": f"_simpleauth_sess={self.state.data['session_cookie']}"}
        )
        return session

    def _append_log(self, line: str):
        with self._lock:
            self.log_lines.append(line)
            self.log_lines = self.log_lines[-200:]
        self.events.publish({"type": "log", "line": line, "ts": time.time()})

    def _start_metadata_worker(self):
        def _worker():
            while not self.stop_event.is_set():
                try:
                    self._metadata_pass(force=False)
                    time.sleep(120)
                except Exception:
                    logger.exception("Metadata worker failed")
                    time.sleep(120)

        t = threading.Thread(target=_worker, daemon=True, name="metadata-worker")
        t.start()
        self._metadata_thread = t

    def _metadata_pass(self, force: bool = False):
        if not self.state.ready():
            return
        session = self._session()
        indexer = LibraryIndexer(
            session=session,
            library_path=self.state.data["library_path"],
            ext_include=self.state.data.get("include"),
            ext_exclude=self.state.data.get("exclude"),
            platforms=self.state.data.get("platforms"),
            purchase_keys=None,
            trove=self.state.data.get("trove"),
        )
        # Categories
        missing_cat = self.db.get_assets_missing_category(limit=25 if not force else 100)
        if force and not missing_cat:
            # Re-run on some already-classified assets for refresh
            missing_cat = self.db.get_assets_for_reclassify()[:50]
        if missing_cat:
            categorizer = AssetCategorizer(
                openwebui_url=os.environ.get("OPENWEBUI_URL"),
                openwebui_model=os.environ.get("OPENWEBUI_MODEL"),
            )
            for asset in missing_cat:
                if self.stop_event.is_set():
                    break
                category, extra_tags = categorizer.categorize_with_tags(
                    file_name=asset.get("file_name", ""),
                    platform=asset.get("platform", ""),
                    bundle_title=asset.get("bundle_title", ""),
                    product_title=asset.get("product_title", ""),
                )
                if category:
                    self.db.set_category(asset["id"], category)
                    try:
                        self.db.add_tags(asset["id"], [category, *extra_tags])
                    except Exception:
                        logger.exception("Failed to tag asset %s", asset.get("id"))
                    self._append_log(
                        f"AI category set for {asset.get('file_name','')}: {category}"
                    )
        self._backfill_category_tags()
        # Images/descriptions via Humble order metadata
        self._fill_meta_from_orders(indexer, force=force)
        # Descriptions via OpenWebUI for remaining
        self._fill_descriptions_ai(force=force)
        cats = self.db.category_counts(limit=10)
        cat_summary = ", ".join([f"{c.get('category') or 'unknown'}:{c.get('cnt')}" for c in cats])
        self._append_log(f"Metadata pass complete. Top categories now: {cat_summary}")

    def _fill_meta_from_orders(self, indexer: LibraryIndexer, force: bool = False):
        if force:
            targets = self.db.get_assets_for_orders(limit=500)
        else:
            targets = self.db.get_assets_missing_image(limit=30) + self.db.get_assets_missing_description(limit=30)
        needed_by_order: dict[str, list[int]] = {}
        for asset in targets:
            order_id = asset.get("order_id")
            if not order_id or order_id == "trove":
                continue
            needed_by_order.setdefault(order_id, []).append(asset["id"])
        for order_id, asset_ids in needed_by_order.items():
            if self.stop_event.is_set():
                break
            order = indexer._fetch_order(order_id)
            if not order:
                continue
            meta_map = indexer.product_meta_from_order(order)
            for asset_id in asset_ids:
                asset = self.db.get_asset(asset_id)
                if not asset:
                    continue
                prod = (asset.get("product_title") or "").strip()
                prod_key = prod or (asset.get("file_name") or "")
                entry = meta_map.get(prod) or meta_map.get(prod_key)
                if not entry:
                    continue
                if entry.get("image_url"):
                    self.db.set_image_url(asset_id, entry["image_url"])
                    self._append_log(f"Set image for {prod or asset_id}")
                if entry.get("description"):
                    self.db.set_description(asset_id, entry["description"])
                    self._append_log(f"Set description for {prod or asset_id}")
    # Also backfill download URLs for this order where missing.
            func = globals().get("_backfill_download_urls")
            if func:
                func(order_id, order)

    def _fill_descriptions_ai(self, force: bool = False):
        if not (os.environ.get("OPENWEBUI_URL") and os.environ.get("OPENWEBUI_MODEL")):
            return
        targets = self.db.get_assets_missing_description(limit=10 if not force else 40)
        if force and not targets:
            targets = self.db.get_assets_for_reclassify(None)[:20]
        for asset in targets:
            if self.stop_event.is_set():
                break
            prompt = (
                f"Write a brief, neutral 1-2 sentence description of this Humble item.\n"
                f"Bundle: {asset.get('bundle_title')}\n"
                f"Product: {asset.get('product_title')}\n"
                f"Filename: {asset.get('file_name')}\n"
            )
            desc = self._openwebui_generate(prompt)
            if desc:
                self.db.set_description(asset["id"], desc)
                try:
                    self.db.add_tags(asset["id"], ["ai-described"])
                except Exception:
                    logger.exception("Failed adding ai-described tag")
                preview = (desc[:120] + "...") if len(desc) > 120 else desc
                self._append_log(f"AI description added for {asset.get('product_title','')}: {preview}")
            else:
                self._append_log("AI description generation returned nothing")

    def _backfill_category_tags(self):
        missing_tags = self.db.get_assets_missing_category_tag(limit=250)
        if not missing_tags:
            return
        for asset in missing_tags:
            try:
                self.db.add_tags(asset["id"], [asset["category"]])
            except Exception:
                logger.exception("Failed adding category tag for %s", asset.get("id"))
        self._append_log(f"Added missing category tags to {len(missing_tags)} assets")

    def _openwebui_generate(self, prompt: str) -> Optional[str]:
        url = os.environ.get("OPENWEBUI_URL")
        model = os.environ.get("OPENWEBUI_MODEL")
        if not url or not model:
            return None
        base = url.rstrip("/")
        if not base.endswith("/chat/completions"):
            base = base + "/api/v1/chat/completions"
        headers = {"Content-Type": "application/json"}
        api_key = os.environ.get("OPENWEBUI_API_KEY")
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": "You write concise, neutral blurbs about Humble Bundle items."},
                {"role": "user", "content": prompt},
            ],
            "max_tokens": 120,
            "temperature": 0.3,
        }
        try:
            r = requests.post(base, json=payload, headers=headers, timeout=8)
            if not r.ok:
                return None
            data = r.json()
            text = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            return text.strip()
        except Exception:
            logger.exception("OpenWebUI description generation failed")
            return None

    def sync_assets(self, trove: Optional[bool] = None, force_meta: bool = False):
        if self.syncing:
            return
        self.syncing = True
        event: dict | None = None
        try:
            session = self._session()
            known_orders = db.distinct_order_ids(include_trove=trove if trove is not None else False)
            indexer = LibraryIndexer(
                session=session,
                library_path=self.state.data["library_path"],
                ext_include=self.state.data.get("include"),
                ext_exclude=self.state.data.get("exclude"),
                platforms=self.state.data.get("platforms"),
                purchase_keys=known_orders or None,
                trove=trove if trove is not None else self.state.data.get("trove"),
            )
            assets = indexer.collect()
            self.db.upsert_assets(assets)
            self.last_sync = time.time()
            self._append_log(f"Indexed {len(assets)} assets.")
            cats = self.db.category_counts(limit=10)
            cat_summary = ", ".join([f"{c.get('category') or 'unknown'}:{c.get('cnt')}" for c in cats])
            self._append_log(f"Top categories after sync: {cat_summary}")
            if force_meta:
                self._metadata_pass(force=True)
            event = {"type": "sync-complete", "ts": self.last_sync}
        except Exception as exc:
            logger.exception("Index failed")
            self._append_log(f"Index failed: {exc}")
            event = {"type": "sync-failed"}
        finally:
            self.syncing = False
            if event:
                self.events.publish(event)

    def start_sync(self, trove: Optional[bool] = None, force_meta: bool = False):
        if self.syncing:
            raise RuntimeError("Sync already in progress")
        self.stop_event.clear()
        thread = threading.Thread(
            target=self.sync_assets, args=(trove, force_meta), daemon=True, name="sync-thread"
        )
        thread.start()
        self._threads.append(thread)

    def _cache_key_for_asset(self, asset: dict) -> str:
        if asset.get("trove"):
            return f"trove:{asset['file_name']}"
        return f"{asset['order_id']}:{asset['file_name']}"

    def start_download(self, update: bool = False, trove: Optional[bool] = None):
        if self.downloading:
            raise RuntimeError("Download already running")
        if not self.state.ready():
            raise RuntimeError("Set session cookie and library path first")
        self.stop_event.clear()
        thread = threading.Thread(
            target=self._download_thread, args=(update, trove), daemon=True
        )
        thread.start()
        self._threads.append(thread)

    def _download_thread(self, update: bool, trove: Optional[bool]):
        self.downloading = True
        self.download_total = 0
        self.download_done = 0
        self._download_failures = 0
        self.download_skipped = 0
        event: dict | None = None
        try:
            library_path = self.state.data.get("library_path", "")
            assets = db.get_assets_pending_download(library_path, limit=None)
            self.download_total = len(assets)
            self._append_log(f"Download pass: {self.download_total} assets with URLs need download.")
            self.events.publish(
                {
                    "type": "download-start",
                    "done": self.download_done,
                    "total": self.download_total,
                }
            )
            if self.download_total == 0:
                self._append_log("No assets with URLs are pending download.")
                event = {
                    "type": "download-complete",
                    "ts": time.time(),
                    "done": 0,
                    "total": 0,
                    "failures": 0,
                    "skipped": 0,
                }
                return
            self._download_direct_from_urls(assets)
            self.last_download = time.time()
            self._append_log(
                f"Download finished: {self.download_done}/{self.download_total} items. Failures: {self._download_failures} Skipped: {self.download_skipped}"
            )
            event = {
                "type": "download-complete",
                "ts": self.last_download,
                "done": self.download_done,
                "total": self.download_total,
                "failures": self._download_failures,
                "skipped": self.download_skipped,
            }
        except Exception as exc:
            logger.exception("Download failed")
            self._append_log(f"Download failed: {exc}")
            event = {"type": "download-failed"}
        finally:
            self.downloading = False
            if event:
                self.events.publish(event)

    def _download_direct_from_urls(self, assets: list[dict]):
        session = self._session()
        for asset in assets:
            if self.stop_event.is_set():
                break
            urls = _parse_download_urls(asset.get("download_urls"))
            if not urls:
                self._append_log(f"No URLs for {asset.get('file_name','')}")
                continue
            url = urls[0]
            local_path = asset.get("download_path") or os.path.join(
                self.state.data.get("library_path", ""),
                asset.get("bundle_title", ""),
                asset.get("product_title", ""),
                asset.get("file_name", ""),
            )
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            try:
                resp = session.get(url, stream=True, timeout=(5, 60))
                if not resp.ok:
                    self._append_log(f"Download failed for {asset.get('file_name','')}: status {resp.status_code}")
                    self._download_failures += 1
                    continue
                with open(local_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        if self.stop_event.is_set():
                            break
                        if chunk:
                            f.write(chunk)
                if self.stop_event.is_set():
                    break
                self.db.mark_downloaded(url, local_path)
                self.download_done += 1
                self.events.publish(
                    {
                        "type": "download-progress",
                        "done": self.download_done,
                        "total": self.download_total,
                        "file": os.path.basename(local_path),
                        "failures": self._download_failures,
                        "skipped": self.download_skipped,
                    }
                )
            except Exception as exc:
                self._append_log(f"Download failed for {asset.get('file_name','')}: {exc}")
                self._download_failures += 1


state = UIState()
db = AssetDB(str(default_data_dir() / "assets.db"))
event_bus = EventBus()


def _reclassify_category(from_cat: str):
    rows = db.assets_by_category([from_cat])
    if not rows:
        return
    updated = 0
    for row in rows:
        new_cat, extra_tags = categorizer.categorize_with_tags(
            file_name=row.get("file_name", ""),
            platform=row.get("platform", ""),
            bundle_title=row.get("bundle_title", ""),
            product_title=row.get("product_title", ""),
        )
        if new_cat and new_cat != from_cat:
            db.set_category(row["id"], new_cat)
            if extra_tags:
                db.add_tags(row["id"], extra_tags + [new_cat])
            updated += 1
    if updated:
        logger.info("Reclassified %s assets from %s", updated, from_cat)


def _load_settings_from_db():
    settings = db.get_settings()
    if settings.get("session_cookie"):
        state.set_cookie(settings["session_cookie"])
    if settings.get("library_path"):
        state.set_library_path(settings["library_path"])
    if any(k in settings for k in ("include", "exclude", "platforms", "trove")):
        state.set_filters(
            include=json.loads(settings.get("include", "[]")) if settings.get("include") else state.data.get("include"),
            exclude=json.loads(settings.get("exclude", "[]")) if settings.get("exclude") else state.data.get("exclude"),
            platforms=json.loads(settings.get("platforms", "[]")) if settings.get("platforms") else state.data.get("platforms"),
            trove=json.loads(settings.get("trove", "false")) if settings.get("trove") else state.data.get("trove"),
        )
    if any(k in settings for k in ("openwebui_url", "openwebui_model", "openwebui_api_key")):
        state.set_openwebui(
            url=settings.get("openwebui_url", state.data.get("openwebui_url")),
            model=settings.get("openwebui_model", state.data.get("openwebui_model")),
            api_key=settings.get("openwebui_api_key", state.data.get("openwebui_api_key")),
        )
        os.environ["OPENWEBUI_URL"] = state.data.get("openwebui_url", "") or os.environ.get("OPENWEBUI_URL", "")
        os.environ["OPENWEBUI_MODEL"] = state.data.get("openwebui_model", "") or os.environ.get("OPENWEBUI_MODEL", "")
        if state.data.get("openwebui_api_key"):
            os.environ["OPENWEBUI_API_KEY"] = state.data["openwebui_api_key"]
    if any(k in settings for k in ("auth_header_name", "auth_header_value")):
        state.set_auth_header(
            name=settings.get("auth_header_name", state.data.get("auth_header_name")),
            value=settings.get("auth_header_value", state.data.get("auth_header_value")),
        )


_load_settings_from_db()
coordinator = Coordinator(state, db, event_bus)
categorizer = LibraryIndexer(
    session=requests.Session(),
    library_path="",
).categorizer
try:
    _reclassify_category("video")
except Exception:
    logger.exception("Failed to reclassify legacy 'video' items")
shutdown_flag = threading.Event()
_order_name_cache: dict[str, dict] = {}

# Ensure application logs are visible; default to DEBUG for download tracing.
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")

app = FastAPI()

static_dir = Path(__file__).resolve().parent / "web" / "static"
app.mount("/static", StaticFiles(directory=static_dir), name="static")
# React dist (SPA)
react_dist = static_dir.parent / "react-dist"
if react_dist.exists():
    app.mount("/app", StaticFiles(directory=react_dist, html=True), name="react-app")
    if (react_dist / "assets").exists():
        app.mount("/assets", StaticFiles(directory=react_dist / "assets"), name="react-assets")


@app.get("/")
def home():
    if react_dist.exists() and (react_dist / "index.html").exists():
        return FileResponse(react_dist / "index.html")
    if not state.ready():
        return RedirectResponse(url="/settings")
    return FileResponse(static_dir / "home.html")


@app.get("/library")
def library():
    if react_dist.exists() and (react_dist / "index.html").exists():
        return FileResponse(react_dist / "index.html")
    if not state.ready():
        return RedirectResponse(url="/settings")
    return FileResponse(static_dir / "library.html")


@app.get("/item")
def item_page():
    if react_dist.exists() and (react_dist / "index.html").exists():
        return FileResponse(react_dist / "index.html")
    if not state.ready():
        return RedirectResponse(url="/settings")
    return FileResponse(static_dir / "item.html")


@app.get("/bundle")
def bundle_page():
    if not state.ready():
        return RedirectResponse(url="/settings")
    return FileResponse(static_dir / "bundle.html")


@app.get("/admin")
def admin():
    if react_dist.exists() and (react_dist / "index.html").exists():
        return FileResponse(react_dist / "index.html")
    if not state.ready():
        return RedirectResponse(url="/settings")
    return FileResponse(static_dir / "index.html")


@app.get("/purchases")
def purchases_page():
    if not state.ready():
        return RedirectResponse(url="/settings")
    return FileResponse(static_dir / "purchases.html")


@app.get("/settings")
def settings_page():
    if react_dist.exists() and (react_dist / "index.html").exists():
        return FileResponse(react_dist / "index.html")
    return FileResponse(static_dir / "settings.html")


@app.middleware("http")
async def trusted_header_guard(request: Request, call_next):
    name = (state.data.get("auth_header_name") or "").strip()
    if name:
        user_val = request.headers.get(name)
        if not user_val:
            return JSONResponse({"detail": "Unauthorized"}, status_code=401)
        request.state.auth_user = user_val
    response = await call_next(request)
    return response


@app.get("/api/me")
def get_me(request: Request):
    name = (state.data.get("auth_header_name") or "").strip()
    if not name:
        return {"user": ""}
    return {"user": request.headers.get(name, "")}


def _fetch_library_json(session: requests.Session) -> dict:
    """Fetch purchase keys/library metadata from the user order API (cookie-auth)."""
    try:
        api_r = session.get("https://www.humblebundle.com/api/v1/user/order", timeout=(5, 15))
        if api_r.ok:
            return api_r.json()
        raise RuntimeError(f"status {api_r.status_code}")
    except Exception as exc:
        logger.exception("Failed to fetch library JSON")
        raise HTTPException(status_code=502, detail=f"Failed to fetch library JSON: {exc}")


def _fetch_order_json(session: requests.Session, order_id: str) -> dict:
    try:
        order_r = session.get(
            f"https://www.humblebundle.com/api/v1/order/{order_id}?all_tpkds=true",
            headers={
                "content-type": "application/json",
                "content-encoding": "gzip",
            },
            timeout=(5, 20),
        )
        if not order_r.ok:
            raise RuntimeError(f"status {order_r.status_code}")
        return order_r.json()
    except Exception as exc:
        logger.exception("Failed to fetch order %s", order_id)
        raise HTTPException(status_code=502, detail=f"Failed to fetch order {order_id}: {exc}")


def _extract_downloads_from_order(order: dict) -> dict:
    """Map product title -> list of downloads (web+bt, filename, platform) for backfill."""
    downloads_by_product: dict[str, list[dict]] = {}
    for prod in order.get("subproducts") or []:
        title = _clean_name(prod.get("human_name", ""))
        entries: list[dict] = []
        for d in prod.get("downloads", []) or []:
            for file_type in d.get("download_struct", []) or []:
                url_obj = file_type.get("url") or {}
                web = url_obj.get("web")
                if not web:
                    continue
                urls = [web]
                bt = url_obj.get("bittorrent")
                if bt:
                    urls.append(bt)
                filename = web.split("?", 1)[0].split("/")[-1]
                platform = (file_type.get("platform") or d.get("platform") or "").lower()
                entries.append({"filename": filename, "urls": urls, "platform": platform})
        if entries:
            downloads_by_product[title] = entries
    return downloads_by_product


def _graceful_signal(signum, frame):
    shutdown_flag.set()
    coordinator.stop_event.set()


try:
    signal.signal(signal.SIGINT, _graceful_signal)
    signal.signal(signal.SIGTERM, _graceful_signal)
except Exception:
    # Some platforms (e.g., Windows) may not support all signals.
    pass


@app.get("/")
def index():
    stats = db.stats()
    if not state.ready() or stats.get("total", 0) == 0:
        return FileResponse(static_dir / "index.html")
    return FileResponse(static_dir / "home.html")


@app.get("/admin")
def admin():
    return FileResponse(static_dir / "index.html")


@app.on_event("shutdown")
def on_shutdown():
    shutdown_flag.set()
    coordinator.stop_event.set()
    event_bus.stop_all()
    # Try to join worker threads briefly
    for thread in list(coordinator._threads):
        thread.join(timeout=2)


@app.get("/api/status")
def status():
    library_path = state.data.get("library_path")
    # Reconcile downloaded flags based on files on disk (best-effort).
    with suppress(Exception):
        if library_path:
            db.reconcile_downloaded(library_path)
    stats = db.stats(library_path=library_path)
    return {
        "ready": state.ready(),
        "library_path": library_path,
        "filters": {
            "include": state.data.get("include", []),
            "exclude": state.data.get("exclude", []),
            "platforms": state.data.get("platforms", []),
            "trove": state.data.get("trove", False),
        },
        "syncing": coordinator.syncing,
        "downloading": coordinator.downloading,
        "download_progress": {
            "done": coordinator.download_done,
            "total": coordinator.download_total,
        },
        "last_sync": coordinator.last_sync,
        "last_download": coordinator.last_download,
        "stats": stats,
        "ai_configured": bool(
            os.environ.get("OPENWEBUI_URL") and os.environ.get("OPENWEBUI_MODEL")
        ),
        "session_valid": _session_valid(),
    }


@app.get("/library")
def library():
    return FileResponse(static_dir / "library.html")


@app.get("/item")
def item():
    return FileResponse(static_dir / "item.html")


@app.get("/api/assets/{asset_id}")
def get_asset(asset_id: int):
    asset = db.get_asset(asset_id)
    if not asset:
        raise HTTPException(status_code=404, detail="Not found")
    path = asset.get("download_path")
    if path and os.path.exists(path):
        try:
            size = os.path.getsize(path)
            asset["exists"] = size > 0
            asset["size_bytes"] = size
        except OSError:
            asset["exists"] = False
    else:
        asset["exists"] = False
    return asset


@app.get("/api/assets/{asset_id}/file")
def get_asset_file(asset_id: int):
    asset = db.get_asset(asset_id)
    if not asset:
        raise HTTPException(status_code=404, detail="Not found")
    path = asset.get("download_path")
    if not path or not os.path.exists(path):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(path, filename=os.path.basename(path))


@app.get("/api/highlights")
def highlights(limit_per_category: int = 12, max_categories: int = 6):
    return db.category_highlights(
        limit_per_category=limit_per_category,
        max_categories=max_categories,
        library_path=state.data.get("library_path"),
    )


@app.get("/api/bundles")
def list_bundles(limit: int = 500):
    return db.bundle_summaries(limit=limit)


@app.get("/api/purchases")
def list_purchases(limit: int = 500):
    return db.purchase_summaries(limit=limit)


@app.get("/api/facets")
def get_facets(downloaded: Optional[int] = None):
    return db.get_facets(downloaded_only=bool(downloaded))


@app.get("/api/order/{order_id}")
def get_order(order_id: str):
    if not state.ready():
        raise HTTPException(status_code=400, detail="Set session cookie first")
    session = coordinator._session()
    return _fetch_order_json(session, order_id)


@app.post("/api/session")
def set_session(payload: SessionPayload):
    if not payload.cookie:
        raise HTTPException(status_code=400, detail="Cookie value is required")
    state.set_cookie(payload.cookie)
    return {"ok": True}


@app.post("/api/config")
def set_config(payload: ConfigPayload):
    state.set_library_path(payload.library_path)
    state.set_filters(
        include=[e.lower() for e in payload.include],
        exclude=[e.lower() for e in payload.exclude],
        platforms=[p.lower() for p in payload.platforms],
        trove=payload.trove,
    )
    return {"ok": True, "state": state.data}


@app.post("/api/sync")
def sync(payload: SyncPayload):
    try:
        coordinator.start_sync(payload.trove, force_meta=payload.update)
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return {"started": True}


@app.post("/api/download")
def download(payload: SyncPayload):
    if coordinator.downloading:
        raise HTTPException(status_code=409, detail="Download already running")
    try:
        coordinator.start_download(update=payload.update, trove=payload.trove)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {"started": True}


@app.get("/api/assets")
def list_assets(
    q: Optional[str] = None,
    order_id: Optional[str] = None,
    platform: Optional[str] = None,
    bundle: Optional[str] = None,
    product: Optional[str] = None,
    ext: Optional[str] = None,
    category: Optional[str] = None,
    trove: Optional[bool] = Query(None),
    downloaded: Optional[bool] = Query(None),
    sort: str = "recent",
    limit: int = 50,
    offset: int = 0,
):
    result = db.search_assets(
        query=q,
        order_id=order_id,
        platform=platform,
        bundle=bundle,
        product=product,
        ext=ext,
        category=category,
        trove=trove,
        downloaded=downloaded,
        sort=sort,
        limit=limit,
        offset=offset,
    )
    items = result.get("items", [])
    for asset in items:
        path = asset.get("download_path")
        if path and os.path.exists(path):
            try:
                size = os.path.getsize(path)
                asset["exists"] = size > 0
                asset["size_bytes"] = size
            except OSError:
                asset["exists"] = False
        else:
            asset["exists"] = False
    return result


@app.post("/api/assets/{asset_id}/tags")
def update_tags(asset_id: int, payload: TagPayload):
    db.set_tags(asset_id, payload.tags)
    return {"ok": True}


@app.get("/api/logs")
def get_logs():
    return {"lines": coordinator.log_lines[-100:]}


@app.get("/api/debug/purchases")
def debug_purchases():
    if not state.ready():
        raise HTTPException(status_code=400, detail="Set session cookie first")
    session = coordinator._session()
    data = _fetch_library_json(session)
    return data


@app.get("/api/debug/orders")
def debug_orders(limit: Optional[int] = Query(None, ge=1)):
    if not state.ready():
        raise HTTPException(status_code=400, detail="Set session cookie first")
    session = coordinator._session()
    data = _fetch_library_json(session)
    keys = []
    if isinstance(data, dict):
        keys = data.get("gamekeys") or data.get("gamekeys_json") or []
    elif isinstance(data, list):
        keys = [item.get("gamekey") for item in data if isinstance(item, dict) and item.get("gamekey")]
    if limit:
        keys = keys[:limit]
    orders = []
    bundle_infos = []
    for key in keys:
        order = _fetch_order_json(session, key)
        orders.append(order)
        bundle_data = {"order_id": key}
        try:
            info = session.get(
                f"https://www.humblebundle.com/api/v1/bundle/{key}",
                timeout=(5, 15),
            )
            if info.ok:
                bundle_data["bundle_info"] = info.json()
            else:
                bundle_data["error"] = f"status {info.status_code}"
        except Exception as exc:
            bundle_data["error"] = str(exc)
        bundle_infos.append(bundle_data)
    return {"orders": orders, "count": len(orders), "bundles": bundle_infos}


@app.get("/api/settings")
def get_settings():
    settings = db.get_settings()
    # Merge state values to reflect current runtime defaults.
    merged = {
        "session_cookie": settings.get("session_cookie") or state.data.get("session_cookie", ""),
        "library_path": settings.get("library_path") or state.data.get("library_path", ""),
        "include": json.loads(settings.get("include", "[]")) if settings.get("include") else state.data.get("include", []),
        "exclude": json.loads(settings.get("exclude", "[]")) if settings.get("exclude") else state.data.get("exclude", []),
        "platforms": json.loads(settings.get("platforms", "[]")) if settings.get("platforms") else state.data.get("platforms", []),
        "trove": json.loads(settings.get("trove", "false")) if settings.get("trove") else state.data.get("trove", False),
        "openwebui_url": settings.get("openwebui_url") or state.data.get("openwebui_url", ""),
        "openwebui_model": settings.get("openwebui_model") or state.data.get("openwebui_model", ""),
        "openwebui_api_key": settings.get("openwebui_api_key") or state.data.get("openwebui_api_key", ""),
        "auth_header_name": settings.get("auth_header_name") or state.data.get("auth_header_name", ""),
        "auth_header_value": settings.get("auth_header_value") or state.data.get("auth_header_value", ""),
    }
    return merged


@app.post("/api/settings")
def update_settings(payload: SettingsPayload):
    updates: dict[str, str] = {}
    if payload.session_cookie is not None:
        state.set_cookie(payload.session_cookie)
        updates["session_cookie"] = payload.session_cookie
    if payload.library_path is not None:
        state.set_library_path(payload.library_path)
        updates["library_path"] = payload.library_path
    if any(v is not None for v in (payload.include, payload.exclude, payload.platforms, payload.trove)):
        state.set_filters(
            include=payload.include if payload.include is not None else state.data.get("include"),
            exclude=payload.exclude if payload.exclude is not None else state.data.get("exclude"),
            platforms=payload.platforms if payload.platforms is not None else state.data.get("platforms"),
            trove=payload.trove if payload.trove is not None else state.data.get("trove"),
        )
        if payload.include is not None:
            updates["include"] = json.dumps(payload.include)
        if payload.exclude is not None:
            updates["exclude"] = json.dumps(payload.exclude)
        if payload.platforms is not None:
            updates["platforms"] = json.dumps(payload.platforms)
        if payload.trove is not None:
            updates["trove"] = json.dumps(payload.trove)
    if any(v is not None for v in (payload.openwebui_url, payload.openwebui_model, payload.openwebui_api_key)):
        state.set_openwebui(
            url=payload.openwebui_url if payload.openwebui_url is not None else state.data.get("openwebui_url"),
            model=payload.openwebui_model if payload.openwebui_model is not None else state.data.get("openwebui_model"),
            api_key=payload.openwebui_api_key if payload.openwebui_api_key is not None else state.data.get("openwebui_api_key"),
        )
        if payload.openwebui_url is not None:
            updates["openwebui_url"] = payload.openwebui_url
            os.environ["OPENWEBUI_URL"] = payload.openwebui_url
        if payload.openwebui_model is not None:
            updates["openwebui_model"] = payload.openwebui_model
            os.environ["OPENWEBUI_MODEL"] = payload.openwebui_model
        if payload.openwebui_api_key is not None:
            updates["openwebui_api_key"] = payload.openwebui_api_key
            os.environ["OPENWEBUI_API_KEY"] = payload.openwebui_api_key
    if any(v is not None for v in (payload.auth_header_name, payload.auth_header_value)):
        state.set_auth_header(
            name=payload.auth_header_name if payload.auth_header_name is not None else state.data.get("auth_header_name"),
            value=payload.auth_header_value if payload.auth_header_value is not None else state.data.get("auth_header_value"),
        )
        if payload.auth_header_name is not None:
            updates["auth_header_name"] = payload.auth_header_name
        if payload.auth_header_value is not None:
            updates["auth_header_value"] = payload.auth_header_value
    if updates:
        db.set_settings(updates)
    return {"ok": True}


def _backfill_download_urls(order_id: str, order: dict):
    if not order_id or not order:
        return
    downloads_map = _extract_downloads_from_order(order)
    if not downloads_map:
        return
    missing = db.get_assets_missing_download_urls(limit=200)
    targets = [m for m in missing if m.get("order_id") == order_id]
    for asset in targets:
        title = (asset.get("product_title") or "").strip()
        filename = (asset.get("file_name") or "").strip()
        platform = (asset.get("platform") or "").lower()
        dl_entries = downloads_map.get(title) or []
        best = None
        if dl_entries:
            # Prefer matching filename and platform.
            for entry in dl_entries:
                if entry.get("filename") == filename and (not platform or entry.get("platform") == platform):
                    best = entry
                    break
            # Fallback: match filename only.
            if not best:
                for entry in dl_entries:
                    if entry.get("filename") == filename:
                        best = entry
                        break
        if not best:
            # Try any product: match filename (and platform if possible).
            for entries in downloads_map.values():
                for entry in entries:
                    if entry.get("filename") == filename and (not platform or entry.get("platform") == platform):
                        best = entry
                        break
                if best:
                    break
            if not best:
                for entries in downloads_map.values():
                    for entry in entries:
                        if entry.get("filename") == filename:
                            best = entry
                            break
                    if best:
                        break
        if not best:
            continue
        urls = best.get("urls") or []
        if urls:
            db.set_download_urls(asset["id"], urls)


def _session_valid() -> bool:
    try:
        session = coordinator._session()
        r = session.get("https://www.humblebundle.com/api/v1/user/order", timeout=(5, 10))
        return r.ok
    except Exception:
        return False


def _reclassify_assets(asset_ids: Optional[List[int]] = None) -> dict:
    assets = db.get_assets_for_reclassify(asset_ids)
    updated = 0
    skipped = 0
    for asset in assets:
        category = categorizer.categorize(
            file_name=asset.get("file_name", ""),
            platform=asset.get("platform", ""),
            bundle_title=asset.get("bundle_title", ""),
            product_title=asset.get("product_title", ""),
        )
        if category:
            db.set_category(asset["id"], category)
            updated += 1
        else:
            skipped += 1
    return {"updated": updated, "skipped": skipped, "total": len(assets)}


def _filename_only(url: str) -> str:
    if not url:
        return ""
    base = url.split("?", 1)[0]
    return base.split("/")[-1]


def _parse_download_urls(val) -> list[str]:
    if not val:
        return []
    if isinstance(val, list):
        return [str(v).strip() for v in val if str(v).strip()]
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return []
        try:
            if s.startswith("["):
                parsed = json.loads(s)
                if isinstance(parsed, list):
                    return [str(v).strip() for v in parsed if str(v).strip()]
            return [part.strip() for part in s.split(",") if part.strip()]
        except Exception:
            return [part.strip() for part in s.split(",") if part.strip()]
    return []


@app.post("/api/reclassify")
def reclassify(payload: ReclassifyPayload):
    if not (os.environ.get("OPENWEBUI_URL") and os.environ.get("OPENWEBUI_MODEL")):
        raise HTTPException(
            status_code=400, detail="OpenWebUI is not configured; cannot run AI classification."
        )
    return _reclassify_assets(payload.asset_ids)


@app.get("/api/updates")
async def updates():
    return {"detail": "Use websocket /ws/updates"}


@app.websocket("/ws/updates")
async def ws_updates(websocket: WebSocket):
    await websocket.accept()
    q = event_bus.subscribe_async()
    try:
        while not shutdown_flag.is_set():
            try:
                event = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                await websocket.send_text(json.dumps({"type": "keepalive"}))
                continue
            if isinstance(event, dict) and event.get("type") == "__shutdown__":
                await websocket.send_text(json.dumps({"type": "shutdown"}))
                break
            await websocket.send_text(json.dumps(event))
    except WebSocketDisconnect:
        pass
    except asyncio.CancelledError:
        pass
    finally:
        event_bus.unsubscribe_async(q)
        with suppress(Exception):
            await websocket.close()


def run():
    config = uvicorn.Config(
        "humblebundle_downloader.ui_server:app",
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 8000)),
        reload=False,
        timeout_graceful_shutdown=10,
        timeout_keep_alive=1,
        log_level="info",
    )
    server = uvicorn.Server(config)
    server.install_signal_handlers = False

    def _handle_signal(signum, frame):
        shutdown_flag.set()
        coordinator.stop_event.set()
        server.should_exit = True

    signal.signal(signal.SIGINT, _handle_signal)
    try:
        signal.signal(signal.SIGTERM, _handle_signal)
    except Exception:
        pass

    server.run()


if __name__ == "__main__":
    run()
