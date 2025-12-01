import { useEffect, useState } from "react";
import { fetchStatus } from "../api";

export default function Admin() {
  const [status, setStatus] = useState<any>({});
  const [logs, setLogs] = useState<string[]>([]);

  useEffect(() => {
    fetchStatus().then(setStatus);
    loadLogs();
  }, []);

  const loadLogs = async () => {
    const res = await fetch("/api/logs");
    const data = await res.json();
    setLogs(data.lines || []);
  };

  const post = (path: string, body: any = {}) => fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });

  const hint = (text: string) => (
    <span className="hint" title={text}>?</span>
  );

  return (
    <div className="panel">
      <div className="section-header">
        <div>
          <p className="pill">Control Room</p>
          <h1>Admin</h1>
        </div>
        <div className="controls" style={{ maxWidth: 640 }}>
          <button className="btn" onClick={() => post("/api/sync").then(loadLogs)}>Index {hint("Fetch orders and update DB entries (no forced metadata overwrite).")}</button>
          <button className="btn secondary" onClick={() => post("/api/sync", { update: true }).then(loadLogs)}>Force re-sync {hint("Index + overwrite metadata (images/descriptions/categories) from Humble/AI).")}</button>
          <button className="btn secondary" onClick={() => post("/api/download").then(loadLogs)}>Download all {hint("Download all assets with URLs that are not on disk.")}</button>
          <button className="btn secondary" onClick={() => location.assign("/settings")}>Settings</button>
        </div>
      </div>
      <div className="meta">
        <div><strong>Total</strong><br />{status.stats?.total || 0}</div>
        <div><strong>Downloaded</strong><br />{status.stats?.downloaded || 0}</div>
        <div><strong>Session</strong><br />{status.session_valid ? "Valid" : "Invalid"}</div>
      </div>
      <div style={{ marginTop: 16 }}>
        <h3>Logs</h3>
        <pre className="logs">{logs.join("\n")}</pre>
      </div>
    </div>
  );
}
