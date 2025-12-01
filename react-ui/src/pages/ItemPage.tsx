import { useEffect, useState } from "react";
import { useParams } from "react-router-dom";
import { Asset, fetchAsset } from "../api";

function parseUrls(val: string[] | string | undefined): string[] {
  if (!val) return [];
  if (Array.isArray(val)) return val;
  try {
    if (val.trim().startsWith("[")) {
      const parsed = JSON.parse(val);
      if (Array.isArray(parsed)) return parsed;
    }
  } catch {
    /* ignore */
  }
  return val.split(",").map((s) => s.trim()).filter(Boolean);
}

export default function ItemPage() {
  const { id } = useParams();
  const [item, setItem] = useState<Asset | null>(null);
  const [variants, setVariants] = useState<Asset[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!id) return;
    setLoading(true);
    fetchAsset(id)
      .then((a) => setItem(a))
      .catch(() => setItem(null))
      .finally(() => setLoading(false));
  }, [id]);

  useEffect(() => {
    if (!item) return;
    const params = new URLSearchParams();
    params.set("product", item.product_title || "");
    params.set("order_id", item.order_id || "");
    params.set("limit", "200");
    fetch(`/api/assets?${params.toString()}`)
      .then((r) => r.json())
      .then((d) => setVariants(d.items || []))
      .catch(() => setVariants([]));
  }, [item]);

  if (!item) return <div className="panel">{loading ? "Loading..." : "Not found"}</div>;

  const cover = item.image_url;
  const title = item.product_title || item.file_name || "Item";
  const urlList = parseUrls(item.download_urls);
  const sizeMb = item.size_bytes ? `${Math.round(item.size_bytes / 1024 / 1024)} MB` : "";

  return (
    <div className="panel">
      <p className="muted"><a href="/">Home</a> · <a href="/library">Library</a></p>
      <div className="hero">
        <img src={cover} alt="cover" style={{ maxWidth: 180, borderRadius: 12 }} />
        <div>
          <h1>{title}</h1>
          <p className="muted">{item.bundle_title}</p>
          <p className="muted">{item.description}</p>
        </div>
      </div>
      <div className="meta" style={{ marginTop: 12 }}>
        <div><strong>Category</strong><br />{item.category || "-"}</div>
        <div><strong>Platform</strong><br />{item.platform || "-"}</div>
        <div><strong>Ext</strong><br />{item.ext || "-"}</div>
      </div>
      <div style={{ marginTop: 16 }}>
        <h3>Downloads</h3>
        <div className="variant">
          <div>
            <div><strong>{item.file_name}</strong></div>
            <div className="muted">
              {item.platform || "n/a"} · {item.ext || ""} · {sizeMb || "?"} · {item.downloaded ? "Downloaded" : "Not downloaded"}
            </div>
          </div>
          <div style={{ display: "flex", gap: 8, justifyContent: "flex-end" }}>
            {item.downloaded ? <a className="btn" href={`/api/assets/${item.id}/file`}>Download cached</a> : <span className="btn" style={{ opacity: 0.5, pointerEvents: "none" }}>Download cached</span>}
            {urlList.map((u, idx) => (
              <a key={u} className="btn secondary" href={u}>{idx === 0 ? "Download from Humble Bundle" : "Torrent from Humble Bundle"}</a>
            ))}
          </div>
        </div>
      </div>
      <div style={{ marginTop: 16 }}>
        <h3>Other variants</h3>
        <div className="variant-list">
          {variants.map((v) => {
            const urls = parseUrls(v.download_urls);
            const variantSizeMb = v.size_bytes ? `${Math.round(v.size_bytes / 1024 / 1024)} MB` : "";
            return (
              <div className="variant" key={v.id}>
                <div>
                  <div><strong>{v.product_title || v.file_name}</strong></div>
                  <div className="muted">
                    {v.platform || "n/a"} · {v.ext || ""} · {variantSizeMb || "?"} · {v.downloaded ? "Downloaded" : "Not downloaded"}
                  </div>
                </div>
                <div style={{ display: "flex", gap: 8, justifyContent: "flex-end" }}>
                  {v.downloaded ? <a className="btn" href={`/api/assets/${v.id}/file`}>Download cached</a> : <span className="btn" style={{ opacity: 0.5, pointerEvents: "none" }}>Download cached</span>}
                  {urls.map((u, idx) => (
                    <a key={u} className="btn secondary" href={u}>{idx === 0 ? "Download from Humble Bundle" : "Torrent from Humble Bundle"}</a>
                  ))}
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
