from __future__ import annotations

import json
import logging
import os
import re
import time
import threading
from typing import Dict, Iterable, List, Optional
from pathlib import Path

import parsel
import requests

from .download_library import _clean_name

logger = logging.getLogger(__name__)


class AssetCategorizer:
    CATEGORIES = {
        "ebook",
        "comic",
        "music",
        "sfx",
        "audio",
        "tutorial",
        "software",
        "android",
        "archive",
        "key",
        "other",
        "art",
        "tileset",
        "sprites",
        "characters",
        "ui",
        "3d",
        "rpg",
        "rpg maker",
        "unity",
        "unreal",
        "source",
        "tool",
    }

    EXT_MAP = {
        # Books / comics
        "pdf": "ebook",
        "epub": "ebook",
        "mobi": "ebook",
        "azw3": "ebook",
        "cbz": "comic",
        "cbr": "comic",
        # Audio / music / sfx
        "mp3": "music",
        "flac": "music",
        "aac": "music",
        "ogg": "music",
        "wav": "sfx",
        # Video / courses
        "mp4": "tutorial",
        "mkv": "tutorial",
        "avi": "tutorial",
        "mov": "tutorial",
        # Engines / 3d
        "unitypackage": "unity",
        "fbx": "3d",
        "obj": "3d",
        "blend": "3d",
        "uasset": "unreal",
        "uproject": "unreal",
        "psd": "art",
        "png": "art",
        "jpg": "art",
        "jpeg": "art",
        # Games / software
        "exe": "software",
        "msi": "software",
        "dmg": "software",
        "pkg": "software",
        "deb": "software",
        "rpm": "software",
        "sh": "software",
        "appimage": "software",
        "apk": "android",
        "iso": "software",
        "rom": "software",
        "img": "software",
    }

    ARCHIVE_EXT = {"zip", "tar", "gz", "bz2", "7z", "rar"}

    PLATFORM_MAP = {
        "audio": "audio",
        "android": "android",
        "linux": "software",
        "mac": "software",
        "macos": "software",
        "osx": "software",
        "windows": "software",
        "steam": "game",
        "origin": "game",
        "uplay": "game",
        "video": "video",
        "ebook": "ebook",
        "ebook_apk": "android",
        "ebook_pdf": "ebook",
        "ebook_epub": "ebook",
        "ebook_mobi": "ebook",
    }

    def __init__(
        self,
        ollama_url: Optional[str] = None,
        ollama_model: Optional[str] = None,
        openwebui_url: Optional[str] = None,
        openwebui_model: Optional[str] = None,
    ):
        self.ollama_url = ollama_url or os.environ.get("OLLAMA_URL")
        self.ollama_model = ollama_model or os.environ.get("OLLAMA_MODEL")
        self.openwebui_url = openwebui_url or os.environ.get("OPENWEBUI_URL")
        model_env = openwebui_model or os.environ.get("OPENWEBUI_MODEL")
        classify_models_env = os.environ.get("OPENWEBUI_MODELS_CLASSIFY") or os.environ.get(
            "OPENWEBUI_MODEL_CLASSIFY"
        )
        # Prefer dedicated classify models if provided; fall back to default.
        models = classify_models_env or model_env or ""
        self.openwebui_models: List[str] = [m.strip() for m in models.split(",") if m.strip()]
        self.openwebui_api_key = os.environ.get("OPENWEBUI_API_KEY")
        self._warned_missing_ai = False

    def categorize(self, *, file_name: str, platform: str, bundle_title: str, product_title: str) -> str:
        primary, _ = self.categorize_with_tags(
            file_name=file_name,
            platform=platform,
            bundle_title=bundle_title,
            product_title=product_title,
        )
        return primary

    def categorize_with_tags(
        self, *, file_name: str, platform: str, bundle_title: str, product_title: str
    ) -> tuple[str, List[str]]:
        ext = file_name.split(".")[-1].lower() if "." in file_name else ""
        platform = (platform or "").lower()
        combined = f"{bundle_title} {product_title} {file_name}".lower()

        primary: Optional[str] = None
        tags: List[str] = []
        archive_hint = ext in self.ARCHIVE_EXT

        def add_tag(cat: Optional[str]):
            if cat and cat not in tags:
                tags.append(cat)

        # High-confidence extension hits.
        if ext in self.EXT_MAP and self.EXT_MAP[ext] != "archive":
            primary = self.EXT_MAP[ext]
            add_tag(primary)

        # Platform hints (treat archive-like platforms as lower confidence).
        if platform in self.PLATFORM_MAP:
            platform_guess = self.PLATFORM_MAP[platform]
            if platform_guess != "archive":
                if not primary:
                    primary = platform_guess
                add_tag(platform_guess)

        text_hit = self._text_rules(combined)
        if text_hit:
            if not primary:
                primary = text_hit
            add_tag(text_hit)

        ai_guesses = self._ai_guess(
            file_name, platform, bundle_title, product_title, ext=ext, text=combined
        )
        for g in ai_guesses or []:
            if not primary:
                primary = g
            add_tag(g)

        if not primary and archive_hint:
            primary = "archive"
            add_tag("archive")
        if not primary and ext in self.EXT_MAP:
            primary = self.EXT_MAP[ext]
            add_tag(primary)
        if not primary:
            primary = "other"
            add_tag("other")

        extras = [t for t in tags if t != primary]
        return primary, extras

    def _text_rules(self, text: str) -> Optional[str]:
        if re.search(r"(comic|manga|graphic novel|cbz|cbr)", text):
            return "comic"
        if re.search(r"(ebook|book|novel|guide|pdf|epub|mobi)", text):
            return "ebook"
        if re.search(r"(soundtrack|ost|music|score|flac|mp3)", text):
            return "music"
        if re.search(r"(sfx|sound effect|fx pack|foley|sound pack|soundfx)", text):
            return "sfx"
        if re.search(r"(video|tutorial|course|webinar|lesson|masterclass|recording)", text):
            return "tutorial"
        if re.search(r"(dlc|key|activation)", text):
            return "key"
        if re.search(r"(unitypackage|unity\s)", text):
            return "unity"
        if re.search(r"(unreal|ue4|ue5|uasset|uproject)", text):
            return "unreal"
        if re.search(r"(3d model|3d pack|low poly|fbx|obj|blend|poly)", text):
            return "3d"
        if re.search(r"(rpg maker|rmmv|rm2k|rpgmaker|rmxp|rmvx|rmz)", text):
            return "rpg maker"
        if re.search(r"(rpg\b|role[- ]?playing)", text):
            return "rpg"
        if re.search(r"(tile(set)?|tileset|grid map)", text):
            return "tileset"
        if re.search(r"(sprite|spritesheet|pixel art|icon pack|ui pack|art pack|texture|background|asset pack|game dev assets)", text):
            return "sprites"
        if re.search(r"(character|npc|enemy pack|portraits?|busts?)", text):
            return "characters"
        if re.search(r"(ui kit|interface|hud|menus?)", text):
            return "ui"
        if re.search(r"(linux|windows|mac|appimage|installer|exe|client|tool)", text):
            return "software"
        if re.search(r"(source code|sourcecode|unity project|unreal project|godot|plugin|addon)", text):
            return "source"
        return None

    def _ai_guess(
        self,
        file_name: str,
        platform: str,
        bundle_title: str,
        product_title: str,
        ext: str,
        text: str,
    ) -> Optional[List[str]]:
        prompt = (
            "Classify this Humble download. Choose one or two categories from: ebook, comic, music, sfx, audio, tutorial, software, android, archive, key, other, art, tileset, sprites, characters, ui, 3d, rpg, rpg maker, unity, unreal, source, tool.\n"
            "Prefer the end content (e.g., a .zip containing a tileset or course should be tileset/tutorial, not 'archive'). Only answer 'archive' when the content is truly mixed/unknown.\n"
            f"Bundle: {bundle_title}\nProduct: {product_title}\nFilename: {file_name}\nExtension: {ext or 'none'}\nPlatform hint: {platform or 'unknown'}\n"
            f"Text hints: {text[:500]}\n"
            "Answer with one or two category words from the list (comma-separated if two)."
        )
        guess = None
        if self.openwebui_url and self.openwebui_models:
            guess = self._guess_openwebui(prompt)
        else:
            if not self._warned_missing_ai:
                logger.warning("OpenWebUI not configured; skipping AI classification.")
                self._warned_missing_ai = True
        return guess

    def _allowed(self, guess: str) -> Optional[str]:
        allowed = self.CATEGORIES
        guess = guess.strip().lower()
        aliases = {
            "soundtrack": "music",
            "sound": "sfx",
            "sfx": "sfx",
            "book": "ebook",
            "novel": "ebook",
            "course": "tutorial",
            "tutorial": "tutorial",
            "video": "tutorial",
            "video course": "tutorial",
            "app": "software",
            "application": "software",
            "sprite": "sprites",
            "sprite pack": "sprites",
            "tile": "tileset",
            "tiles": "tileset",
            "tilesets": "tileset",
            "character pack": "characters",
            "character": "characters",
            "icons": "ui",
            "hud": "ui",
            "interface": "ui",
            "tooling": "tool",
            "utility": "tool",
            "code": "source",
            "sourcecode": "source",
            "project": "source",
            "audio": "audio",
            "music": "music",
        }
        guess = aliases.get(guess, guess)
        return guess if guess in allowed else None

    def _extract_allowed_list(self, text: str) -> List[str]:
        cleaned = (text or "").strip().lower()
        if not cleaned:
            return []
        normalized = re.sub(r"[\\|/;]", ",", cleaned)
        normalized = normalized.replace("\n", ",")
        parts = [p.strip() for p in normalized.split(",") if p.strip()]
        if not parts:
            parts = cleaned.split()
        found: List[str] = []
        for part in parts:
            allowed = self._allowed(part)
            if allowed and allowed not in found:
                found.append(allowed)
        # As a fallback, scan the full string for known categories in order of appearance.
        if not found:
            for cat in self.CATEGORIES:
                if cat in cleaned:
                    found.append(cat)
        return found

    def _guess_openwebui(self, prompt: str) -> Optional[List[str]]:
        # Try multiple models if provided, merging unique allowed categories.
        url = self.openwebui_url.rstrip("/")
        if not url.endswith("/chat/completions"):
            if "/api/v1" in url:
                url = url + "/chat/completions"
            else:
                url = url + "/api/v1/chat/completions"
        headers = {"Content-Type": "application/json"}
        if self.openwebui_api_key:
            headers["Authorization"] = f"Bearer {self.openwebui_api_key}"

        merged: List[str] = []

        for model in self.openwebui_models:
            payload = {
                "model": model,
                "messages": [
                    {
                        "role": "system",
                        "content": (
                            "You classify Humble Bundle items into up to two categories. "
                            "Choose one or two from: ebook, comic, music, sfx, audio, tutorial, software, android, archive, key, other, art, tileset, sprites, characters, ui, 3d, rpg, rpg maker, unity, unreal, source, tool. "
                            "If the download is a packaged .zip/.7z/etc but clearly for tilesets, sprites, characters, or a course, choose that content category instead of archive."
                        ),
                    },
                    {"role": "user", "content": prompt},
                ],
                "max_tokens": 8,
                "temperature": 0,
            }
            try:
                r = requests.post(url, json=payload, headers=headers, timeout=8)
                if not r.ok:
                    continue
                data = r.json()
                text = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                guesses = self._extract_allowed_list(text)
                for g in guesses:
                    if g not in merged:
                        merged.append(g)
                if merged:
                    # If a model gave valid categories, keep them; still let later models add new ones.
                    continue
                if not guesses and text:
                    logger.debug("OpenWebUI guess not allowed (model %s): %s", model, text)
            except Exception:
                continue

        return merged or None

    def _guess_ollama(self, prompt: str) -> Optional[str]:
        return None


class LibraryIndexer:
    def __init__(
        self,
        session: requests.Session,
        library_path: str,
        ext_include: Optional[Iterable[str]] = None,
        ext_exclude: Optional[Iterable[str]] = None,
        platforms: Optional[Iterable[str]] = None,
        purchase_keys: Optional[List[str]] = None,
        trove: bool = False,
        stop_event=None,
    ):
        self.session = session
        self.timeout = (5, 15)
        self.library_path = library_path
        self.ext_include = [] if ext_include is None else list(map(str.lower, ext_include))
        self.ext_exclude = [] if ext_exclude is None else list(map(str.lower, ext_exclude))
        self.platforms = [] if platforms is None else list(map(str.lower, platforms))
        self.purchase_keys = purchase_keys
        self.trove = trove
        self.categorizer = AssetCategorizer()
        self.stop_event = stop_event
        self._image_missing_logs = 0
        self._debug_dumped = False
        self._game_image_cache: dict[str, Optional[str]] = {}

    def collect(self) -> List[Dict]:
        assets: List[Dict] = []
        if self.trove:
            for product in self._get_trove_products():
                if self.stop_event and self.stop_event.is_set():
                    break
                title = _clean_name(product["human-name"])
                assets.extend(self._collect_trove_assets(title, product))
            return assets

        purchase_keys = self.purchase_keys or self._get_purchase_keys()
        for order_id in purchase_keys:
            if self.stop_event and self.stop_event.is_set():
                break
            order = self._fetch_order(order_id)
            if not order:
                logger.warning("Order fetch failed or empty for %s (check session cookie)", order_id)
                continue
            order_product = order.get("product", {}) or {}
            bundle_title = _clean_name(order_product.get("human_name", ""))
            order_category = (order_product.get("category") or "").lower()
            products = order.get("subproducts") or []
        if not products:
            # Handle single-product orders without subproducts.
            products = [
                {
                    "human_name": bundle_title or order.get("product", {}).get("human_name", ""),
                    "downloads": order.get("downloads", []),
                    "tpkd_dict": order.get("tpkd_dict"),
                    "icon": order.get("product", {}).get("image"),
                    "category": order_category,
                }
            ]
            for product in products:
                if self.stop_event and self.stop_event.is_set():
                    break
                assets.extend(
                    self._collect_bundle_assets(order_id, bundle_title, product)
                )
        return assets

    def product_meta_from_order(self, order: Dict) -> Dict[str, Dict]:
        """Extract image/description per product title from a raw order response."""
        if not order or "subproducts" not in order or "product" not in order:
            return {}
        meta = {}
        bundle_title = _clean_name(order["product"].get("human_name", ""))
        bundle_fallback_image = self._extract_image(order.get("product", {})) if isinstance(order.get("product"), dict) else None
        # Diagnostic: dump the first order payload we see so we can locate image fields reliably.
        if not self._debug_dumped:
            try:
                data_dir = Path(__file__).resolve().parent.parent / "data"
                data_dir.mkdir(parents=True, exist_ok=True)
                sample_path = data_dir / f"order_sample_{order.get('gamekey','unknown')}.json"
                with sample_path.open("w", encoding="utf-8") as f:
                    json.dump(order, f, indent=2)
                logger.info("Wrote image debug sample to %s", sample_path)
                self._debug_dumped = True
            except Exception:
                logger.exception("Failed to write image debug sample")
        for product in order["subproducts"]:
            product_title = _clean_name(product.get("human_name", ""))
            if not product_title:
                continue
            image = self._extract_image(product)
            if not image and bundle_fallback_image:
                image = bundle_fallback_image
            desc = self._extract_description(product)
            if not image and self._image_missing_logs < 20:
                self._image_missing_logs += 1
                image_like_keys = [
                    k for k, v in product.items() if isinstance(v, str) and ("http" in v or "//" in v)
                ]
                # Provide a small hint about why image is missing without dumping the payload.
                logger.info(
                    "No image found for product '%s' (bundle '%s'); url-like keys: %s",
                    product_title,
                    bundle_title,
                    ", ".join(image_like_keys) or "none",
                )
            meta[product_title] = {
                "bundle_title": bundle_title,
                "product_title": product_title,
                "image_url": image,
                "description": desc,
            }
        return meta

    def _get_purchase_keys(self) -> List[str]:
        # Prefer user order API; fall back to page scrape if needed.
        try:
            api_r = self.session.get(
                "https://www.humblebundle.com/api/v1/user/order", timeout=self.timeout
            )
            if api_r.ok:
                data = api_r.json()
                if isinstance(data, list):
                    keys = [item.get("gamekey") for item in data if isinstance(item, dict) and item.get("gamekey")]
                    if keys:
                        logger.info("Fetched %d purchase keys via user/order API", len(keys))
                        return keys
                else:
                    logger.debug("Unexpected user/order payload type: %s", type(data))
        except Exception:
            logger.debug("user/order API failed", exc_info=True)
        library_r = self.session.get(
            "https://www.humblebundle.com/home/library", timeout=self.timeout
        )
        library_page = parsel.Selector(text=library_r.text)
        user_data = (
            library_page.css("#user-home-json-data").xpath("string()").extract_first()
        )
        if user_data is None:
            raise Exception("Unable to download user-data, cookies missing?")
        orders_json = json.loads(user_data)
        logger.info("Fetched %d purchase keys via library page", len(orders_json.get('gamekeys', [])))
        return orders_json["gamekeys"]

    def _fetch_order(self, order_id: str):
        try:
            order_r = self.session.get(
                f"https://www.humblebundle.com/api/v1/order/{order_id}?all_tpkds=true",
                headers={
                    "content-type": "application/json",
                    "content-encoding": "gzip",
                },
                timeout=self.timeout,
            )
            return order_r.json()
        except Exception:
            logger.exception("Failed to fetch order %s", order_id)
            return None

    def _collect_bundle_assets(
        self, order_id: str, bundle_title: str, product: Dict
    ) -> List[Dict]:
        collected = []
        product_title = _clean_name(product["human_name"])
        image_url = self._extract_image(product)
        description = self._extract_description(product)
        downloads = product.get("downloads", []) or []
        # Some items expose keys under tpkd_dict/all_tpks or all_tpkds without download_struct; capture them as assets.
        tpkd = product.get("tpkd_dict") or {}
        all_tpks = tpkd.get("all_tpks") or []
        tpkd_entries = product.get("all_tpkds") or []
        for tk in all_tpks:
            key_val = tk.get("key") or tk.get("machine_name") or tk.get("gamekey")
            if not key_val:
                continue
            collected.append(
                self._as_asset(
                    order_id=order_id,
                    bundle_title=bundle_title,
                    product_title=product_title,
                    platform="key",
                    category="key",
                    file_name=key_val,
                    url="",
                    md5=None,
                    uploaded_at=tk.get("timestamp"),
                    image_url=image_url,
                    description=description or tk.get("instructions"),
                    tags=["key"],
                    activation_key=key_val,
                    order_name=bundle_title,
                )
            )
        for entry in tpkd_entries:
            platform_hint = (entry.get("platform") or "").lower()
            if entry.get("key") or entry.get("tpkd_dict"):
                key_val = entry.get("key") or entry.get("tpkd_dict", {}).get("machine_name") or entry.get("tpkd_dict", {}).get("gamekey")
                if key_val:
                    collected.append(
                        self._as_asset(
                            order_id=order_id,
                            bundle_title=bundle_title,
                            product_title=product_title,
                            platform=platform_hint or "key",
                            category="key",
                            file_name=key_val,
                            url="",
                            md5=None,
                            uploaded_at=entry.get("timestamp"),
                            image_url=image_url,
                            description=description or entry.get("instructions"),
                            tags=["key"],
                            activation_key=key_val,
                            order_name=bundle_title,
                        )
                    )
                continue
            url_obj = entry.get("url") if isinstance(entry, dict) else None
            if isinstance(url_obj, dict) and "web" in url_obj:
                url = url_obj["web"]
                filename = self._canonical_url(url).split("/")[-1]
                if not self._should_download_file(filename):
                    continue
                category, extra_tags = self.categorizer.categorize_with_tags(
                    file_name=filename,
                    platform=platform_hint,
                    bundle_title=bundle_title,
                    product_title=product_title,
                )
                collected.append(
                    self._as_asset(
                        order_id=order_id,
                        bundle_title=bundle_title,
                        product_title=product_title,
                        platform=platform_hint or "other",
                        category=category,
                        file_name=filename,
                        url=self._canonical_url(url),
                        md5=entry.get("md5"),
                        uploaded_at=entry.get("timestamp"),
                        image_url=image_url,
                        description=description,
                        tags=[category, *extra_tags],
                        download_urls=[url],
                        order_name=bundle_title,
                    )
                )

        if not downloads and not all_tpks and not tpkd_entries:
            # Create a stub asset so the purchase appears even without downloads/keys.
            stub_url = f"stub:{order_id}:{product_title}"
            stub_category = (product.get("category") or bundle_title or "other").lower() or "other"
            collected.append(
                self._as_asset(
                    order_id=order_id,
                    bundle_title=bundle_title,
                    product_title=product_title,
                    platform="other",
                    category=stub_category,
                    file_name=f"{product_title}.stub",
                    url=stub_url,
                    md5=None,
                    uploaded_at=None,
                    image_url=image_url,
                    description=description or product.get("instructions"),
                    tags=[stub_category, "stub"],
                    order_name=bundle_title,
                )
            )
            return collected

        for download_type in downloads:
            if self.stop_event and self.stop_event.is_set():
                break
            platform_hint = download_type.get("platform", "").lower()
            for file_type in download_type.get("download_struct", []):
                if self.stop_event and self.stop_event.is_set():
                    break
                file_platform = (file_type.get("platform") or platform_hint).lower()
                if not self._should_download_platform(file_platform):
                    continue
                # Activation key only entries
                if file_type.get("key") or file_type.get("tpkd_dict"):
                    key_val = file_type.get("key") or file_type.get("tpkd_dict", {}).get("machine_name") or file_type.get("tpkd_dict", {}).get("gamekey")
                    if key_val:
                        collected.append(
                            self._as_asset(
                                order_id=order_id,
                                bundle_title=bundle_title,
                                product_title=product_title,
                                platform="key",
                                category="key",
                                file_name=key_val,
                                 url="",
                                 md5=None,
                                 uploaded_at=file_type.get("timestamp")
                                 or file_type.get("uploaded_at"),
                                 image_url=image_url,
                                 description=description,
                                 tags=["key"],
                                 activation_key=key_val,
                                 order_name=bundle_title,
                             )
                        )
                    continue
                if "url" in file_type and "web" in file_type["url"]:
                    url = file_type["url"]["web"]
                    filename = self._canonical_url(url).split("/")[-1]
                    if not self._should_download_file(filename):
                        continue
                    url_list = [url]
                    if isinstance(file_type.get("url"), dict):
                        bt = file_type["url"].get("bittorrent")
                        if bt:
                            url_list.append(bt)
                    category, extra_tags = self.categorizer.categorize_with_tags(
                        file_name=filename,
                        platform=file_platform,
                        bundle_title=bundle_title,
                        product_title=product_title,
                    )
                    collected.append(
                        self._as_asset(
                            order_id=order_id,
                            bundle_title=bundle_title,
                            product_title=product_title,
                            platform=file_platform,
                            category=category,
                            file_name=filename,
                            url=self._canonical_url(url),
                            md5=file_type.get("md5"),
                            uploaded_at=file_type.get("timestamp")
                            or file_type.get("uploaded_at"),
                            image_url=image_url,
                            description=description,
                            tags=[category, *extra_tags],
                            download_urls=url_list,
                            order_name=bundle_title,
                        )
                    )
        return collected

    def _collect_trove_assets(self, title: str, product: Dict) -> List[Dict]:
        collected = []
        image_url = self._extract_image(product)
        description = self._extract_description(product)
        for platform, download in product["downloads"].items():
            if self.stop_event and self.stop_event.is_set():
                break
            if not self._should_download_platform(platform):
                continue
            url = download["url"]["web"]
            filename = self._canonical_url(url).split("/")[-1]
            if not self._should_download_file(filename):
                continue
            category, extra_tags = self.categorizer.categorize_with_tags(
                file_name=filename,
                platform=platform,
                bundle_title="Humble Trove",
                product_title=title,
            )
            collected.append(
                self._as_asset(
                    order_id="trove",
                    bundle_title="Humble Trove",
                    product_title=title,
                    platform=platform,
                    category=category,
                    file_name=filename,
                    url=self._canonical_url(url),
                    md5=download.get("md5"),
                    uploaded_at=download.get("uploaded_at")
                    or download.get("timestamp")
                    or product.get("date_added"),
                    trove=True,
                    image_url=image_url,
                    description=description,
                    tags=[category, *extra_tags],
                )
            )
        return collected

    def _get_trove_products(self) -> List[Dict]:
        trove_products = []
        idx = 0
        trove_base_url = "https://www.humblebundle.com/client/catalog?index={idx}"
        while True:
            trove_page_url = trove_base_url.format(idx=idx)
            trove_r = self.session.get(trove_page_url, timeout=self.timeout)
            page_content = trove_r.json()
            if len(page_content) == 0:
                break
            trove_products.extend(page_content)
            idx += 1
        return trove_products

    def _as_asset(
        self,
        order_id: str,
        bundle_title: str,
        product_title: str,
        platform: str,
        category: str,
        file_name: str,
        url: str,
        md5: Optional[str],
        uploaded_at: Optional[str],
        trove: bool = False,
        image_url: Optional[str] = None,
        description: Optional[str] = None,
        tags: Optional[List[str]] = None,
        activation_key: Optional[str] = None,
        order_name: Optional[str] = None,
        download_urls: Optional[List[str]] = None,
    ) -> Dict:
        ext = file_name.split(".")[-1].lower() if "." in file_name else ""
        local_path = os.path.join(bundle_title, product_title, file_name)
        if trove:
            local_path = os.path.join("Humble Trove", product_title, file_name)
        category = (category or "other").lower()
        return {
            "order_id": order_id,
            "bundle_title": bundle_title,
            "product_title": product_title,
            "platform": platform,
            "category": category,
            "file_name": file_name,
            "url": url,
            "ext": ext,
            "uploaded_at": uploaded_at,
            "md5": md5,
            "trove": trove,
            "download_path": os.path.join(self.library_path, local_path),
            "added_ts": int(time.time()),
            "image_url": image_url,
            "description": description,
            "tags": tags or [],
            "order_name": order_name or bundle_title,
            "activation_key": activation_key,
            "download_urls": download_urls or [],
        }

    def _extract_image(self, product: Dict) -> Optional[str]:
        if not isinstance(product, dict):
            return None
        # Prefer any explicit image/icon/tile field first.
        url_fields = [
            "tile_image",  # common in order subproducts
            "icon",
            "image",
            "cover",
            "logo",
            "tile",
            "thumbnail",
            "thumb",
        ]
        candidates: List[str] = []
        for key in url_fields:
            val = product.get(key)
            candidates.extend(self._extract_url_candidates(val))
        visuals = product.get("visuals")
        candidates.extend(self._extract_url_candidates(visuals))
        # Broader scan across the product payload; filtering will drop torrent/zip links.
        if not candidates:
            for val in product.values():
                candidates.extend(self._extract_url_candidates(val))
        # Fallback: try the game info API for a larger image.
        if not candidates:
            fetched = self._fetch_game_image(product)
            if fetched:
                candidates.append(fetched)
        # Fallback: try bundle-level image if available in the order payload.
        if not candidates:
            bundle_image = self._extract_bundle_image_from_order(product)
            if bundle_image:
                candidates.append(bundle_image)
        if not candidates:
            return None
        return self._pick_best_image_url(candidates)

    def _extract_url_candidates(self, val) -> List[str]:
        found: List[str] = []
        if isinstance(val, str):
            # Pull any http/https URLs embedded in the string to avoid missing inline links.
            for match in re.findall(r"https?://[^\s\"'>]+", val):
                normalized = self._normalize_image_url(match)
                if normalized and self._is_plausible_image_url(normalized):
                    found.append(normalized)
            # Also handle protocol-relative or scheme-less CDN URLs.
            normalized = self._normalize_image_url(val)
            if normalized and self._is_plausible_image_url(normalized):
                found.append(normalized)
        elif isinstance(val, dict):
            for subval in val.values():
                found.extend(self._extract_url_candidates(subval))
        elif isinstance(val, list):
            for item in val:
                found.extend(self._extract_url_candidates(item))
        return found

    def _normalize_image_url(self, url: str) -> Optional[str]:
        """Accept http/https or protocol-relative URLs; ignore obvious non-URLs."""
        raw = url.strip()
        if not raw:
            return None
        if raw.startswith("//"):
            return "https:" + raw
        if raw.startswith("http://") or raw.startswith("https://"):
            return raw
        # Humble sometimes returns domain-only without scheme; patch in https.
        if ("humblebundle" in raw or "cloudfront" in raw or "digitaloceanspaces" in raw) and "/" in raw:
            return "https://" + raw
        return None

    def _is_plausible_image_url(self, url: str) -> bool:
        """Filter out torrent/download links and prefer typical image assets."""
        lowered = url.lower()
        base = lowered.split("?", 1)[0]
        if ".torrent" in lowered:
            return False
        if any(ext in lowered for ext in (".zip", ".rar", ".7z", ".tar", ".gz")):
            return False
        image_exts = (".jpg", ".jpeg", ".png", ".webp", ".gif", ".svg")
        if base.endswith(image_exts):
            return True
        # Heuristic: allow URLs containing common image path hints while avoiding obvious downloads.
        if any(token in lowered for token in ("image", "images", "cover", "tile", "thumb", "thumbnail", "artwork", "banner")):
            if not any(token in lowered for token in ("download", "dl", "payload", "torrent", "manifest", ".zip", ".rar", ".7z")):
                return True
        return False

    def _pick_best_image_url(self, urls: List[str]) -> str:
        """Pick the highest-resolution-looking URL from a set of candidates."""
        def score(url: str) -> int:
            area_score = 0
            for match in re.findall(r"(\d{2,4})x(\d{2,4})", url):
                try:
                    w, h = int(match[0]), int(match[1])
                    area_score = max(area_score, w * h)
                except Exception:
                    continue
            # Prefer hints of original/large/hires.
            bonus = 0
            lowered = url.lower()
            if "original" in lowered or "hires" in lowered or "large" in lowered:
                bonus += 500000
            # Fallback: longer URLs often carry size tokens we couldn't parse.
            return area_score + bonus + len(url)

        best = max(urls, key=score)
        return best

    def _fetch_game_image(self, product: Dict) -> Optional[str]:
        """Query the game info API for a higher-res image using machine/game id."""
        game_id = (
            product.get("machine_name")
            or product.get("machine-name")
            or product.get("game_id")
            or product.get("game-id")
        )
        if not game_id:
            return None
        key = str(game_id)
        if key in self._game_image_cache:
            return self._game_image_cache[key]
        url = f"https://www.humblebundle.com/api/v1/game/{game_id}"
        try:
            r = self.session.get(url, timeout=self.timeout)
            if not r.ok:
                self._game_image_cache[key] = None
                return None
            data = r.json()
            # Look for common image fields.
            candidates = []
            for field in (
                "image",
                "large_image",
                "tile_image",
                "logo",
                "header_image",
                "background_image",
                "featured_image",
                "featured_small_image",
            ):
                candidates.extend(self._extract_url_candidates(data.get(field)))
            visuals = data.get("visuals")
            candidates.extend(self._extract_url_candidates(visuals))
            if candidates:
                best = self._pick_best_image_url(candidates)
                self._game_image_cache[key] = best
                return best
        except Exception:
            logger.debug("Game info fetch failed for %s", game_id, exc_info=True)
        self._game_image_cache[key] = None
        return None

    def _extract_bundle_image_from_order(self, product: Dict) -> Optional[str]:
        """Try to pull a bundle-level image from the parent order structure."""
        # The parent order isn't passed here, so rely on known product keys that may carry bundle visuals.
        bundle_keys = ("bundle_tile_image", "bundle_icon", "bundle_logo", "bundle_image")
        for key in bundle_keys:
            if key in product:
                imgs = self._extract_url_candidates(product.get(key))
                if imgs:
                    return imgs[0]
        return None

    def _extract_description(self, product: Dict) -> Optional[str]:
        if not isinstance(product, dict):
            return None
        for key in ("description", "body", "blurb"):
            if key in product and isinstance(product[key], str):
                text = product[key].strip()
                if text:
                    return text
        # Generic scan for description-like fields
        for key, val in product.items():
            if isinstance(val, str) and ("desc" in key or "description" in key):
                text = val.strip()
                if text:
                    return text
        return None

    def _should_download_platform(self, platform: str) -> bool:
        # Always collect all platform variants so multi-platform products are indexed.
        return True

    def _should_download_file(self, filename: str) -> bool:
        ext = filename.split(".")[-1].lower()
        if self.ext_include:
            return ext in self.ext_include
        if self.ext_exclude:
            return ext not in self.ext_exclude
        return True

    def _canonical_url(self, url: str) -> str:
        return url.split("?", 1)[0]
