import { useEffect, useState } from "react";
import { fetchPurchases } from "../api";
import { useNavigate } from "react-router-dom";

type Purchase = {
  order_id: string;
  name?: string;
  total: number;
  downloaded: number;
  image_url?: string;
};

export default function Purchases() {
  const [items, setItems] = useState<Purchase[]>([]);
  const [loading, setLoading] = useState(true);
  const nav = useNavigate();
  useEffect(() => {
    setLoading(true);
    fetchPurchases()
      .then(setItems)
      .catch(() => setItems([]))
      .finally(() => setLoading(false));
  }, []);
  return (
    <div className="panel">
      <div className="section-header">
        <div>
          <p className="pill">Purchases</p>
          <h1>Your purchases</h1>
        </div>
      </div>
      <div className="bundle-grid">
        {loading && <div className="muted">Loading purchases…</div>}
        {items.map((p) => (
          <div key={p.order_id} className="bundle-card" onClick={() => nav(`/bundle/${p.order_id}`)}>
            {p.image_url ? (
              <div className="asset-bg" style={{ backgroundImage: `url(${p.image_url})` }} />
            ) : (
              <div className="asset-monogram">{(p.name || p.order_id).slice(0, 1).toUpperCase()}</div>
            )}
            <div className="asset-content">
              <div className="asset-title">{p.name || "Purchase"}</div>
              <div className="asset-meta">{p.total} items · {p.downloaded || 0} downloaded</div>
              <div className="asset-meta">Order: {p.order_id}</div>
              <div className="controls" style={{ marginTop: 8 }}>
                <button className="btn small secondary" onClick={(e) => { e.stopPropagation(); nav("/library", { state: { bundle: p.name, downloadedOnly: true } }); }}>View in Library</button>
              </div>
            </div>
          </div>
        ))}
        {!loading && items.length === 0 && <div className="muted">No purchases found.</div>}
      </div>
    </div>
  );
}
