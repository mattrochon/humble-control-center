import { useEffect, useState } from "react";
import { fetchStatus } from "../api";

export default function SessionBadge() {
  const [label, setLabel] = useState("Session: unknown");
  const [ok, setOk] = useState<boolean | null>(null);
  useEffect(() => {
    fetchStatus()
      .then((s) => {
        setOk(s.session_valid ?? s.ready);
        setLabel(s.session_valid ? "Session: valid" : s.ready ? "Session: ready" : "Session: missing");
      })
      .catch(() => {
        setOk(false);
        setLabel("Session: error");
      });
  }, []);
  return (
    <span className={`badge ${ok ? "good" : "warn"}`}>
      {label}
    </span>
  );
}
