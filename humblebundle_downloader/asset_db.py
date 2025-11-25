import os
import sqlite3
import time
from typing import Dict, Iterable, List, Optional, Tuple


class AssetDB:
    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._init_db()

    def _connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        return conn

    def _init_db(self):
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS assets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id TEXT,
                    bundle_title TEXT,
                    product_title TEXT,
                    platform TEXT,
                    file_name TEXT,
                    url TEXT UNIQUE,
                    ext TEXT,
                    uploaded_at TEXT,
                    md5 TEXT,
                    trove INTEGER DEFAULT 0,
                    size_bytes INTEGER,
                    category TEXT,
                    image_url TEXT,
                    description TEXT,
                    added_ts INTEGER,
                    downloaded INTEGER DEFAULT 0,
                    download_path TEXT
                );
                """
            )
            has_category = conn.execute(
                "SELECT 1 FROM pragma_table_info('assets') WHERE name='category';"
            ).fetchone()
            if not has_category:
                conn.execute("ALTER TABLE assets ADD COLUMN category TEXT;")
            has_image = conn.execute(
                "SELECT 1 FROM pragma_table_info('assets') WHERE name='image_url';"
            ).fetchone()
            if not has_image:
                conn.execute("ALTER TABLE assets ADD COLUMN image_url TEXT;")
            has_desc = conn.execute(
                "SELECT 1 FROM pragma_table_info('assets') WHERE name='description';"
            ).fetchone()
            if not has_desc:
                conn.execute("ALTER TABLE assets ADD COLUMN description TEXT;")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS asset_tags (
                    asset_id INTEGER,
                    tag TEXT,
                    UNIQUE(asset_id, tag),
                    FOREIGN KEY(asset_id) REFERENCES assets(id) ON DELETE CASCADE
                );
                """
            )
            conn.execute(
                """
                CREATE VIRTUAL TABLE IF NOT EXISTS assets_fts
                USING fts5(file_name, product_title, bundle_title, content='assets', content_rowid='id');
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_assets_platform ON assets(platform);"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_assets_bundle ON assets(bundle_title);"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_assets_product ON assets(product_title);"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                );
                """
            )

    def upsert_assets(self, assets: Iterable[Dict]):
        now = int(time.time())
        with self._connect() as conn:
            for asset in assets:
                data = {
                    "order_id": asset.get("order_id"),
                    "bundle_title": asset.get("bundle_title"),
                    "product_title": asset.get("product_title"),
                    "platform": asset.get("platform"),
                    "category": asset.get("category"),
                    "image_url": asset.get("image_url"),
                    "description": asset.get("description"),
                    "file_name": asset.get("file_name"),
                    "url": asset.get("url"),
                    "ext": asset.get("ext"),
                    "uploaded_at": asset.get("uploaded_at"),
                    "md5": asset.get("md5"),
                    "trove": int(asset.get("trove", False)),
                    "size_bytes": asset.get("size_bytes"),
                    "download_path": asset.get("download_path"),
                }
                data["added_ts"] = asset.get("added_ts", now)
                cur = conn.execute(
                    """
                    INSERT INTO assets (
                        order_id, bundle_title, product_title, platform,
                        category, file_name, url, ext, uploaded_at, md5, trove,
                        size_bytes, added_ts, download_path, image_url, description
                    )
                    VALUES (
                        :order_id, :bundle_title, :product_title, :platform,
                        :category, :file_name, :url, :ext, :uploaded_at, :md5, :trove,
                        :size_bytes, :added_ts, :download_path, :image_url, :description
                    )
                    ON CONFLICT(url) DO UPDATE SET
                        order_id=excluded.order_id,
                        bundle_title=excluded.bundle_title,
                        product_title=excluded.product_title,
                        platform=excluded.platform,
                        category=COALESCE(excluded.category, assets.category),
                        file_name=excluded.file_name,
                        ext=excluded.ext,
                        uploaded_at=excluded.uploaded_at,
                        md5=excluded.md5,
                        trove=excluded.trove,
                        size_bytes=excluded.size_bytes,
                        download_path=COALESCE(excluded.download_path, assets.download_path),
                        image_url=COALESCE(excluded.image_url, assets.image_url),
                        description=COALESCE(excluded.description, assets.description);
                    """,
                    {**data, "added_ts": data.get("added_ts", now)},
                )
                asset_id = cur.lastrowid or conn.execute(
                    "SELECT id FROM assets WHERE url=?", (data["url"],)
                ).fetchone()[0]
                conn.execute(
                    """
                    INSERT OR REPLACE INTO assets_fts(rowid, file_name, product_title, bundle_title)
                    VALUES (?, ?, ?, ?);
                    """,
                    (
                        asset_id,
                        data.get("file_name"),
                        data.get("product_title"),
                        data.get("bundle_title"),
                    ),
                )
                extra_tags = asset.get("tags") or []
                for tag in extra_tags:
                    if not tag:
                        continue
                    conn.execute(
                        "INSERT OR IGNORE INTO asset_tags(asset_id, tag) VALUES (?, ?);",
                        (asset_id, str(tag).strip().lower()),
                    )

    def mark_downloaded(self, url: str, download_path: str):
        if not url:
            return
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE assets
                SET downloaded=1, download_path=?
                WHERE url=?;
                """,
                (download_path, url),
            )

    def _candidate_paths(self, asset: sqlite3.Row, library_path: str) -> List[str]:
        paths: List[str] = []
        download_path = asset.get("download_path") if isinstance(asset, dict) else asset["download_path"]
        if download_path:
            paths.append(download_path)
        file_name = asset.get("file_name") if isinstance(asset, dict) else asset["file_name"]
        bundle = asset.get("bundle_title") if isinstance(asset, dict) else asset["bundle_title"]
        product = asset.get("product_title") if isinstance(asset, dict) else asset["product_title"]
        if library_path and file_name:
            if asset.get("trove") if isinstance(asset, dict) else asset["trove"]:
                paths.append(os.path.join(library_path, "Humble Trove", product or "", file_name))
            paths.append(os.path.join(library_path, bundle or "", product or "", file_name))
            paths.append(os.path.join(library_path, file_name))
        return [p for p in paths if p]

    def reconcile_downloaded(self, library_path: str) -> int:
        """Mark assets as downloaded when a candidate file path exists with nonzero size."""
        found = 0
        if not library_path:
            return found
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT id, url, download_path, file_name, bundle_title, product_title, trove FROM assets;"
            ).fetchall()
            for row in rows:
                for path in self._candidate_paths(row, library_path):
                    if os.path.exists(path) and os.path.getsize(path) > 0:
                        conn.execute(
                            "UPDATE assets SET downloaded=1, download_path=? WHERE id=?;",
                            (path, row["id"]),
                        )
                        found += 1
                        break
        return found

    def search_assets(
        self,
        query: Optional[str] = None,
        platform: Optional[str] = None,
        bundle: Optional[str] = None,
        product: Optional[str] = None,
        ext: Optional[str] = None,
        category: Optional[str] = None,
        trove: Optional[bool] = None,
        downloaded: Optional[bool] = None,
        sort: str = "recent",
        limit: int = 50,
        offset: int = 0,
    ) -> Dict:
        with self._connect() as conn:
            where = []
            params: List = []
            join_fts = ""
            if query:
                join_fts = "JOIN assets_fts f ON f.rowid = a.id"
                where.append("f MATCH ?")
                params.append(self._fts_query(query))
            if platform:
                where.append("a.platform = ?")
                params.append(platform)
            if bundle:
                where.append("a.bundle_title = ?")
                params.append(bundle)
            if product:
                where.append("a.product_title = ?")
                params.append(product)
            if ext:
                where.append("a.ext = ?")
                params.append(ext.lower())
            if category:
                where.append(
                    "(a.category = ? OR EXISTS (SELECT 1 FROM asset_tags t WHERE t.asset_id = a.id AND t.tag = ?))"
                )
                cat = category.lower()
                params.extend([cat, cat])
            if trove is not None:
                where.append("a.trove = ?")
                params.append(int(trove))
            if downloaded is not None:
                where.append("a.downloaded = ?")
                params.append(int(downloaded))

            sort_sql = self._sort_clause(sort)
            where_sql = f"WHERE {' AND '.join(where)}" if where else ""

            rows = conn.execute(
                f"""
                SELECT
                    a.*,
                    (
                        SELECT GROUP_CONCAT(tag, ',')
                        FROM asset_tags t
                        WHERE t.asset_id = a.id
                    ) AS tags
                FROM assets a
                {join_fts}
                {where_sql}
                {sort_sql}
                LIMIT ? OFFSET ?;
                """,
                (*params, limit, offset),
            ).fetchall()

            total = conn.execute(
                f"SELECT COUNT(*) FROM assets a {join_fts} {where_sql};",
                params,
            ).fetchone()[0]

        return {
            "items": [dict(r) for r in rows],
            "total": total,
        }

    def _fts_query(self, query: str) -> str:
        terms = query.strip().replace('"', "").split()
        return " AND ".join(terms)

    def _sort_clause(self, sort: str) -> str:
        if sort == "alpha":
            return "ORDER BY a.product_title COLLATE NOCASE ASC, a.file_name COLLATE NOCASE ASC"
        if sort == "bundle":
            return "ORDER BY a.bundle_title COLLATE NOCASE ASC, a.product_title COLLATE NOCASE ASC"
        return "ORDER BY COALESCE(a.uploaded_at, a.added_ts) DESC"

    def set_tags(self, asset_id: int, tags: List[str]):
        clean_tags = [t.strip() for t in tags if t.strip()]
        with self._connect() as conn:
            conn.execute("DELETE FROM asset_tags WHERE asset_id = ?", (asset_id,))
            for tag in clean_tags:
                conn.execute(
                    "INSERT OR IGNORE INTO asset_tags(asset_id, tag) VALUES (?, ?);",
                    (asset_id, tag),
                )

    def add_tags(self, asset_id: int, tags: List[str]):
        clean_tags = [t.strip() for t in tags if t.strip()]
        if not clean_tags:
            return
        with self._connect() as conn:
            for tag in clean_tags:
                conn.execute(
                    "INSERT OR IGNORE INTO asset_tags(asset_id, tag) VALUES (?, ?);",
                    (asset_id, tag),
                )

    def stats(self, library_path: Optional[str] = None) -> Dict:
        with self._connect() as conn:
            total = conn.execute("SELECT COUNT(*) FROM assets;").fetchone()[0]
            downloaded = conn.execute(
                "SELECT COUNT(*) FROM assets WHERE downloaded = 1;"
            ).fetchone()[0]
            bundles = conn.execute(
                "SELECT COUNT(DISTINCT bundle_title) FROM assets;"
            ).fetchone()[0]
            products = conn.execute(
                "SELECT COUNT(DISTINCT product_title) FROM assets;"
            ).fetchone()[0]
        on_disk = None
        if library_path:
            # Best-effort scan to count files that truly exist, independent of DB flag.
            on_disk = self.count_downloaded_on_disk(library_path)
        return {
            "total": total,
            "downloaded": downloaded,
            "downloaded_on_disk": on_disk,
            "bundles": bundles,
            "products": products,
        }

    def count_downloaded_on_disk(self, library_path: str) -> int:
        if not library_path:
            return 0
        found = 0
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT id, url, download_path, file_name, bundle_title, product_title, trove FROM assets;"
            ).fetchall()
            for row in rows:
                for path in self._candidate_paths(row, library_path):
                    if os.path.exists(path) and os.path.getsize(path) > 0:
                        found += 1
                        break
        return found

    def category_highlights(
        self,
        limit_per_category: int = 12,
        max_categories: int = 6,
        library_path: Optional[str] = None,
    ):
        def _exists(row) -> bool:
            if row.get("download_path") and os.path.exists(row["download_path"]):
                return os.path.getsize(row["download_path"]) > 0
            if library_path:
                for path in self._candidate_paths(row, library_path):
                    if os.path.exists(path) and os.path.getsize(path) > 0:
                        return True
            return False

        with self._connect() as conn:
            cats = conn.execute(
                """
                SELECT category, COUNT(*) as cnt
                FROM assets
                WHERE category IS NOT NULL AND category <> '' AND downloaded = 1
                GROUP BY category
                ORDER BY cnt DESC
                LIMIT ?;
                """,
                (max_categories,),
            ).fetchall()
            highlights = []
            for cat in cats:
                rows = conn.execute(
                    """
                    SELECT *
                    FROM assets
                    WHERE category = ? AND downloaded = 1
                    ORDER BY COALESCE(uploaded_at, added_ts) DESC
                    LIMIT ?;
                    """,
                    (cat["category"], limit_per_category * 2),
                ).fetchall()
                filtered = []
                for r in rows:
                    row_dict = dict(r)
                    if _exists(row_dict):
                        filtered.append(row_dict)
                    if len(filtered) >= limit_per_category:
                        break
                highlights.append(
                    {
                        "category": cat["category"],
                        "count": cat["cnt"],
                        "items": filtered,
                    }
                )
        return highlights

    def get_assets_for_reclassify(
        self, asset_ids: Optional[List[int]] = None
    ) -> List[Dict]:
        with self._connect() as conn:
            if asset_ids:
                placeholders = ",".join("?" for _ in asset_ids)
                rows = conn.execute(
                    f"""
                    SELECT id, bundle_title, product_title, file_name, platform
                    FROM assets
                    WHERE id IN ({placeholders});
                    """,
                    tuple(asset_ids),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT id, bundle_title, product_title, file_name, platform
                    FROM assets;
                    """
                ).fetchall()
        return [dict(r) for r in rows]

    def set_category(self, asset_id: int, category: str):
        with self._connect() as conn:
            conn.execute(
                "UPDATE assets SET category=? WHERE id=?;", (category.lower(), asset_id)
            )

    def get_assets_missing_category_tag(self, limit: int = 200) -> List[Dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, category
                FROM assets a
                WHERE category IS NOT NULL AND category != ''
                  AND NOT EXISTS (
                    SELECT 1 FROM asset_tags t
                    WHERE t.asset_id = a.id AND t.tag = a.category
                  )
                LIMIT ?;
                """,
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_assets_missing_category(self, limit: int = 50) -> List[Dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, bundle_title, product_title, file_name, platform
                FROM assets
                WHERE (category IS NULL OR category = '')
                LIMIT ?;
                """,
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_asset(self, asset_id: int) -> Optional[Dict]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT a.*,
                       (
                         SELECT GROUP_CONCAT(tag, ',')
                         FROM asset_tags t
                         WHERE t.asset_id = a.id
                       ) AS tags
                FROM assets a
                WHERE a.id=?;
                """,
                (asset_id,),
            ).fetchone()
            return dict(row) if row else None

    def get_assets_missing_description(self, limit: int = 20) -> List[Dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, bundle_title, product_title, file_name, platform, description
                FROM assets
                WHERE (description IS NULL OR description = '')
                LIMIT ?;
                """,
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def set_description(self, asset_id: int, description: str):
        with self._connect() as conn:
            conn.execute(
                "UPDATE assets SET description=? WHERE id=?;",
                (description.strip(), asset_id),
            )

    def set_image_url(self, asset_id: int, image_url: str):
        with self._connect() as conn:
            conn.execute(
                "UPDATE assets SET image_url=? WHERE id=?;", (image_url, asset_id)
            )

    def get_assets_missing_image(self, limit: int = 20) -> List[Dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, bundle_title, product_title, order_id
                FROM assets
                WHERE (image_url IS NULL OR image_url = '')
                LIMIT ?;
                """,
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_assets_for_orders(self, limit: int = 50) -> List[Dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, bundle_title, product_title, order_id, file_name, image_url, description
                FROM assets
                WHERE order_id IS NOT NULL AND order_id != 'trove'
                LIMIT ?;
                """,
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def category_counts(self, limit: int = 20) -> List[Dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT category, COUNT(*) AS cnt,
                       SUM(CASE WHEN downloaded=1 THEN 1 ELSE 0 END) AS downloaded_cnt
                FROM assets
                GROUP BY category
                ORDER BY cnt DESC
                LIMIT ?;
                """,
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    # --- Settings helpers ---
    def get_settings(self) -> Dict[str, str]:
        with self._connect() as conn:
            rows = conn.execute("SELECT key, value FROM settings;").fetchall()
        return {r["key"]: r["value"] for r in rows}

    def set_settings(self, values: Dict[str, str]):
        if not values:
            return
        with self._connect() as conn:
            for k, v in values.items():
                conn.execute(
                    "INSERT INTO settings(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value;",
                    (k, v),
                )

    def clear_settings(self):
        with self._connect() as conn:
            conn.execute("DELETE FROM settings;")
