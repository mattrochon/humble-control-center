import { useEffect, useState } from "react";
import { useParams } from "react-router-dom";
import { fetchAssets, Asset } from "../api";

export default function Bundle() {
  const { orderId } = useParams();
  const [assets, setAssets] = useState<Asset[]>([]);

  useEffect(() => {
    if (!orderId) return;
    fetchAssets({ order_id: orderId, limit: 200 }).then(setAssets).catch(() => setAssets([]));
  }, [orderId]);

  return (
    <div className="panel">
      <p className="pill">Bundle</p>
      <h1>Order {orderId}</h1>
      <div className="asset-grid">
        {assets.map((a) => (
          <div key={a.id} className="bundle-card" style={{ minHeight: 140 }}>
            {a.image_url ? <div className="asset-bg" style={{ backgroundImage: `url(${a.image_url})` }} /> : null}
            <div className="asset-content">
              <div className="asset-title">{a.product_title || a.file_name}</div>
              <div className="asset-meta">{a.platform} · {a.ext}</div>
              <div className="asset-meta">{a.category}</div>
              <a className="btn secondary" href={`/item/${a.id}`}>Open</a>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
