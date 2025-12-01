import { Asset } from "../api";
import { Link } from "react-router-dom";

type Props = {
  asset: Asset;
};

export default function AssetCard({ asset }: Props) {
  const title = asset.product_title || asset.file_name || "Item";
  const img = asset.image_url;
  const status = asset.downloaded ? "Downloaded" : "Not downloaded";
  return (
    <div className="asset-card">
      {img ? <div className="asset-bg" style={{ backgroundImage: `url(${img})` }} /> : <div className="asset-monogram">{title.slice(0, 1).toUpperCase()}</div>}
      <div className="asset-content">
        <div className="asset-meta">{asset.bundle_title}</div>
        <div className="asset-title">{title}</div>
        <div className="asset-tags">{asset.category || asset.ext}</div>
        <div className="asset-status">{status}</div>
        <Link to={`/item/${asset.id}`} className="btn small">Open</Link>
      </div>
    </div>
  );
}
