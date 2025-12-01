import { useEffect, useState } from "react";
import { fetchStatus } from "../api";

export default function SessionBadge() {
  const [label, setLabel] = useState("Session: unknown");
  const [ok, setOk] = useState<boolean | null>(null);
  const [worker, setWorker] = useState<string | null>(null);
  useEffect(() => {
    fetchStatus()
      .then((s) => {
        setOk(s.session_valid ?? s.ready);
        setLabel(s.session_valid ? "Session: valid" : s.ready ? "Session: ready" : "Session: missing");
        if ((s as any).worker_status) {
          setWorker((s as any).worker_status);
        }
      })
      .catch(() => {
        setOk(false);
        setLabel("Session: error");
      });
    const id = setInterval(() => {
      fetchStatus()
        .then((s) => {
          setOk(s.session_valid ?? s.ready);
          if ((s as any).worker_status) {
            setWorker((s as any).worker_status);
          } else {
            setWorker(null);
          }
        })
        .catch(() => {
          setWorker(null);
        });
    }, 5000);
    return () => clearInterval(id);
  }, []);
  return (
    <span className={`badge ${ok ? "good" : "warn"}`} style={{ display: "inline-flex", gap: 8, alignItems: "center" }}>
      {label}
      {worker && <span className="pill" style={{ background: "rgba(255,255,255,0.08)", borderColor: "rgba(255,255,255,0.2)" }}>{worker}</span>}
    </span>
  );
}
