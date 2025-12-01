import { useEffect, useMemo, useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { Asset, fetchAssets, fetchFacets } from "../api";
type LocationState = {
  bundle?: string;
  order_id?: string;
  category?: string;
  downloadedOnly?: boolean;
};

export default function Library() {
  const location = useLocation();
  const nav = useNavigate();
  const state = (location.state as LocationState) || {};
  const [bundle, setBundle] = useState(state.bundle || "");
  const [orderId, setOrderId] = useState(state.order_id || "");
  const [category, setCategory] = useState(state.category || "");
  const [platform, setPlatform] = useState("");
  const [ext, setExt] = useState("");
  const [q, setQ] = useState("");
  const [downloadedOnly, setDownloadedOnly] = useState(state.downloadedOnly !== false);
  const [facetOptions, setFacetOptions] = useState<{ categories: string[]; platforms: string[]; exts: string[]; bundles: string[] }>({
    categories: [],
    platforms: [],
    exts: [],
    bundles: [],
  });
  const [items, setItems] = useState<Asset[]>([]);
  const [loading, setLoading] = useState(true);

  const facets = useMemo(() => {
    return {
      categories: facetOptions.categories,
      platforms: facetOptions.platforms,
      exts: facetOptions.exts,
    };
  }, [facetOptions]);

  const load = () => {
    setLoading(true);
    const params: Record<string, string> = {};
    if (bundle) params.bundle = bundle;
    if (category) params.category = category;
    if (platform) params.platform = platform;
    if (ext) params.ext = ext;
    if (q) params.q = q;
    if (orderId) params.order_id = orderId;
    if (downloadedOnly) params.downloaded = "1";
    fetchAssets(params)
      .then(setItems)
      .catch(() => setItems([]))
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    load();
    fetchFacets(downloadedOnly)
      .then(setFacetOptions)
      .catch(() => setFacetOptions({ categories: [], platforms: [], exts: [], bundles: [] }));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [downloadedOnly]);

  useEffect(() => {
    const s = (location.state as LocationState) || {};
    if (s.bundle !== undefined) setBundle(s.bundle || "");
    if (s.order_id !== undefined) setOrderId(s.order_id || "");
    if (s.category !== undefined) setCategory(s.category || "");
    if (s.downloadedOnly !== undefined) setDownloadedOnly(s.downloadedOnly);
    // Re-load when navigation state changes
    load();
    fetchFacets(s.downloadedOnly ?? downloadedOnly)
      .then(setFacetOptions)
      .catch(() => setFacetOptions({ categories: [], platforms: [], exts: [], bundles: [] }));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [location.state]);

  return (
    <div className="panel">
      <div className="section-header">
        <div>
          <p className="pill">Library</p>
          <h1>Browse assets</h1>
        </div>
        <button className="btn" onClick={load}>Refresh</button>
      </div>
      <div className="controls" style={{ flexWrap: "wrap" }}>
        <input className="input" placeholder="Search" value={q} onChange={(e) => setQ(e.target.value)} />
        <input className="input" placeholder="Bundle" value={bundle} onChange={(e) => setBundle(e.target.value)} list="bundle-options" />
        <datalist id="bundle-options">
          {facetOptions.bundles.map((b) => <option key={b} value={b} />)}
        </datalist>
        <input className="input" placeholder="Order ID" value={orderId} onChange={(e) => setOrderId(e.target.value)} />
        <select className="input" value={category} onChange={(e) => setCategory(e.target.value)}>
          <option value="">All categories</option>
          {facets.categories.map((c) => <option key={c} value={c}>{c}</option>)}
        </select>
        <select className="input" value={platform} onChange={(e) => setPlatform(e.target.value)}>
          <option value="">All platforms</option>
          {facets.platforms.map((p) => <option key={p} value={p}>{p}</option>)}
        </select>
        <select className="input" value={ext} onChange={(e) => setExt(e.target.value)}>
          <option value="">All extensions</option>
          {facets.exts.map((x) => <option key={x} value={x}>{x}</option>)}
        </select>
        <label className="pill" style={{ cursor: "pointer" }}>
          <input
            type="checkbox"
            checked={downloadedOnly}
            onChange={(e) => setDownloadedOnly(e.target.checked)}
            style={{ marginRight: 6 }}
          />
          Downloaded only
        </label>
        <button className="btn secondary" onClick={load}>Apply</button>
        <button className="btn secondary" onClick={() => { setBundle(""); setCategory(""); setPlatform(""); setExt(""); setQ(""); setDownloadedOnly(true); load(); }}>Clear</button>
      </div>

      <div style={{ marginTop: 18 }}>
        <h3>List</h3>
        <div className="table">
          <div className="thead">
            <div>Product</div>
            <div>Bundle</div>
            <div>File</div>
            <div>Category</div>
            <div>Platform</div>
            <div>Ext</div>
            <div>Status</div>
          </div>
          {loading && <div className="muted" style={{ padding: 10 }}>Loading…</div>}
          {!loading && items.map((a) => (
            <div key={`row-${a.id}`} className="trow" onClick={() => nav(`/item/${a.id}`)}>
              <div className="cell primary" style={{ display: "grid", gridTemplateColumns: "60px 1fr", gap: 8, alignItems: "center" }}>
                <div>
                  {a.image_url ? (
                    <div className="thumb" style={{ backgroundImage: `url(${a.image_url})` }} />
                  ) : (
                    <div className="thumb thumb-empty">{(a.product_title || a.file_name || "?").slice(0, 1).toUpperCase()}</div>
                  )}
                </div>
                <div>
                  <div className="title">{a.product_title || a.file_name}</div>
                  <div className="muted small">{a.order_id}</div>
                </div>
              </div>
              <div className="cell">{a.bundle_title}</div>
              <div className="cell">{a.file_name}</div>
              <div className="cell">{a.category}</div>
              <div className="cell">{a.platform}</div>
              <div className="cell">{a.ext}</div>
              <div className="cell">{a.downloaded ? "Downloaded" : "Not downloaded"}</div>
            </div>
          ))}
          {items.length === 0 && <div className="muted" style={{ padding: 10 }}>No assets found.</div>}
        </div>
      </div>
    </div>
  );
}
