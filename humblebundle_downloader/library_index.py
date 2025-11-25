from __future__ import annotations

import json
import logging
import os
import re
import time
import threading
from typing import Dict, Iterable, List, Optional

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
                continue
            bundle_title = _clean_name(order["product"]["human_name"])
            for product in order["subproducts"]:
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
        for product in order["subproducts"]:
            product_title = _clean_name(product.get("human_name", ""))
            if not product_title:
                continue
            image = self._extract_image(product)
            desc = self._extract_description(product)
            meta[product_title] = {
                "bundle_title": bundle_title,
                "product_title": product_title,
                "image_url": image,
                "description": desc,
            }
        return meta

    def _get_purchase_keys(self) -> List[str]:
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
        for download_type in product.get("downloads", []):
            if self.stop_event and self.stop_event.is_set():
                break
            platform_hint = download_type.get("platform", "").lower()
            if not self._should_download_platform(platform_hint):
                continue
            for file_type in download_type.get("download_struct", []):
                if self.stop_event and self.stop_event.is_set():
                    break
                file_platform = (file_type.get("platform") or platform_hint).lower()
                if not self._should_download_platform(file_platform):
                    continue
                if "url" in file_type and "web" in file_type["url"]:
                    url = file_type["url"]["web"]
                    filename = self._canonical_url(url).split("/")[-1]
                    if not self._should_download_file(filename):
                        continue
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
        }

    def _extract_image(self, product: Dict) -> Optional[str]:
        if not isinstance(product, dict):
            return None
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
        for key in url_fields:
            val = product.get(key)
            if isinstance(val, str) and val.startswith("http"):
                return val
            if isinstance(val, dict):
                # Pick the largest-looking URL entry
                for subval in sorted(val.values(), reverse=True):
                    if isinstance(subval, str) and subval.startswith("http"):
                        return subval
        visuals = product.get("visuals")
        if isinstance(visuals, dict):
            for val in visuals.values():
                if isinstance(val, str) and val.startswith("http"):
                    return val
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
        platform = platform.lower()
        if self.platforms and platform not in self.platforms:
            return False
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
