import { useEffect, useState } from "react";
import { fetchBundles, fetchStatus, Asset } from "../api";
import { useNavigate } from "react-router-dom";
import AssetCard from "../components/AssetCard";

type Bundle = {
  bundle_title: string;
  label?: string;
  order_id?: string;
  total: number;
  downloaded: number;
  image_url?: string;
};

type Highlight = {
  category: string;
  count: number;
  items: Asset[];
};

export default function Home() {
  const [bundles, setBundles] = useState<Bundle[]>([]);
  const [highlights, setHighlights] = useState<Highlight[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [ready, setReady] = useState(false);
  const nav = useNavigate();

  useEffect(() => {
    const load = async () => {
      setLoading(true);
      setError(null);
      try {
        const status = await fetchStatus();
        setReady(status.ready);
        const b = await fetchBundles();
        setBundles(b);
        const hiRes = await fetch("/api/highlights");
        const hiData = await hiRes.json();
        if (!Array.isArray(hiData)) throw new Error("Invalid highlights payload");
        setHighlights(hiData);
      } catch (err: any) {
        console.error(err);
        setError(err?.message || "Failed to load data");
        setBundles([]);
        setHighlights([]);
      } finally {
        setLoading(false);
      }
    };
    load();
  }, []);

  if (!ready) {
    return <div className="panel">Loading…</div>;
  }

  return (
    <div className="panel">
      <div className="section-header">
        <div>
          <p className="pill">Home</p>
          <h1>Your collection</h1>
          <p className="muted">Bundles and downloaded highlights. Click any tile to jump to the Library with filters pre-set.</p>
        </div>
        <button className="btn" onClick={() => nav("/admin")}>Open Control Room</button>
      </div>
      {error && <div className="pill" style={{ background: "rgba(255,179,71,0.15)", borderColor: "rgba(255,179,71,0.4)", color: "#ffb347" }}>{error}</div>}

      <h3 style={{ marginTop: 12 }}>Bundles</h3>
      <div className="bundle-grid bundle-grid-compact">
        {loading && bundles.length === 0 ? <div className="muted">Loading bundles…</div> : null}
        {bundles.map((b) => (
          <div
            key={b.order_id || b.bundle_title}
            className="bundle-card"
            onClick={() =>
              nav("/library", {
                state: {
                  bundle: b.bundle_title || b.label,
                  order_id: b.order_id,
                  downloadedOnly: true,
                },
              })
            }
          >
            {b.image_url ? <div className="asset-bg" style={{ backgroundImage: `url(${b.image_url})` }} /> : <div className="asset-monogram">{(b.label || b.bundle_title || "?").slice(0,1).toUpperCase()}</div>}
            <div className="asset-content">
              <div className="asset-title">{b.label || b.bundle_title}</div>
              <div className="asset-meta">{b.total} items · {b.downloaded} downloaded</div>
            </div>
          </div>
        ))}
        {!loading && bundles.length === 0 && <div className="muted">No bundles indexed.</div>}
      </div>

      <h3 style={{ marginTop: 20 }}>Highlights by category</h3>
      {loading && highlights.length === 0 ? <div className="muted">Loading highlights…</div> : null}
      {highlights.map((h) => (
        <div key={h.category} style={{ marginBottom: 18 }}>
          <div className="section-header" style={{ padding: 0, alignItems: "center" }}>
            <div>
              <h3 style={{ margin: 0 }}>{h.category}</h3>
              <p className="muted" style={{ marginTop: 4 }}>{h.count} downloaded</p>
            </div>
            <button
              className="btn secondary"
              onClick={() => nav("/library", { state: { category: h.category, downloadedOnly: true } })}
            >
              View in Library
            </button>
          </div>
          <div className="asset-grid">
            {h.items.slice(0, 8).map((item) => (
              <div key={item.id} onClick={() => nav(`/item/${item.id}`)}>
                <AssetCard
                  asset={{
                    ...item,
                    product_title: item.product_title || item.file_name,
                    image_url: item.image_url,
                  }}
                />
              </div>
            ))}
          </div>
        </div>
      ))}
      {!loading && highlights.length === 0 && <div className="muted">No downloaded items yet.</div>}

    </div>
  );
}
