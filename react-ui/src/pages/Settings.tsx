import { FormEvent, useEffect, useState } from "react";
import { fetchSettings, updateSettings, Settings } from "../api";
import { useNavigate } from "react-router-dom";

const asList = (value: string) =>
  value
    .split(",")
    .map((v) => v.trim())
    .filter(Boolean);

export default function SettingsPage() {
  const [form, setForm] = useState<Settings | null>(null);
  const [saving, setSaving] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const nav = useNavigate();

  useEffect(() => {
    fetchSettings().then(setForm);
  }, []);

  const updateField = (key: keyof Settings, value: any) => {
    if (!form) return;
    setForm({ ...form, [key]: value });
  };

  const onSubmit = async (e: FormEvent) => {
    e.preventDefault();
    if (!form) return;
    setSaving(true);
    setMessage(null);
    try {
      await updateSettings({
        ...form,
        include: asList(form.include.join(",")),
        exclude: asList(form.exclude.join(",")),
        platforms: asList(form.platforms.join(",")),
      });
      setMessage("Saved");
      setTimeout(() => setMessage(null), 1500);
    } finally {
      setSaving(false);
    }
  };

  if (!form) {
    return <div className="panel">Loading settings…</div>;
  }

  return (
    <div className="panel">
      <div className="section-header">
        <div>
          <p className="pill">Control Room</p>
          <h1>Settings</h1>
        </div>
        <button className="btn secondary" onClick={() => nav(-1)}>Back</button>
      </div>
      <form className="settings-form" onSubmit={onSubmit}>
        <div className="grid two">
          <label className="field">
            <span>Session cookie</span>
            <input
              type="text"
              value={form.session_cookie}
              onChange={(e) => updateField("session_cookie", e.target.value)}
              placeholder="_simpleauth_sess"
            />
          </label>
          <label className="field">
            <span>Library path</span>
            <input
              type="text"
              value={form.library_path}
              onChange={(e) => updateField("library_path", e.target.value)}
              placeholder="G:\\workspace\\humble\\data\\library"
            />
          </label>
          <label className="field">
            <span>Include filters (comma)</span>
            <input
              type="text"
              value={form.include.join(", ")}
              onChange={(e) => updateField("include", asList(e.target.value))}
            />
          </label>
          <label className="field">
            <span>Exclude filters (comma)</span>
            <input
              type="text"
              value={form.exclude.join(", ")}
              onChange={(e) => updateField("exclude", asList(e.target.value))}
            />
          </label>
          <label className="field">
            <span>Platforms (comma)</span>
            <input
              type="text"
              value={form.platforms.join(", ")}
              onChange={(e) => updateField("platforms", asList(e.target.value))}
            />
          </label>
          <label className="field checkbox">
            <input
              type="checkbox"
              checked={form.trove}
              onChange={(e) => updateField("trove", e.target.checked)}
            />
            <span>Include Trove</span>
          </label>
        </div>

        <h3 style={{ marginTop: 24 }}>AI (OpenWebUI)</h3>
        <div className="grid three">
          <label className="field">
            <span>URL</span>
            <input
              type="text"
              value={form.openwebui_url}
              onChange={(e) => updateField("openwebui_url", e.target.value)}
              placeholder="http://127.0.0.1:3000"
            />
          </label>
          <label className="field">
            <span>Model</span>
            <input
              type="text"
              value={form.openwebui_model}
              onChange={(e) => updateField("openwebui_model", e.target.value)}
              placeholder="gpt-4o-mini"
            />
          </label>
          <label className="field">
            <span>API key</span>
            <input
              type="text"
              value={form.openwebui_api_key}
              onChange={(e) => updateField("openwebui_api_key", e.target.value)}
              placeholder="sk-..."
            />
          </label>
        </div>

        <h3 style={{ marginTop: 24 }}>Custom Auth Header</h3>
        <div className="grid two">
          <label className="field">
            <span>Header name</span>
            <input
              type="text"
              value={form.auth_header_name}
              onChange={(e) => updateField("auth_header_name", e.target.value)}
              placeholder="Authorization"
            />
          </label>
          <label className="field">
            <span>Header value</span>
            <input
              type="text"
              value={form.auth_header_value}
              onChange={(e) => updateField("auth_header_value", e.target.value)}
              placeholder="Bearer ..."
            />
          </label>
        </div>

        <div className="actions" style={{ marginTop: 24 }}>
          <button className="btn" type="submit" disabled={saving}>
            {saving ? "Saving…" : "Save settings"}
          </button>
          {message && <span className="pill success" style={{ marginLeft: 12 }}>{message}</span>}
        </div>
      </form>
    </div>
  );
}
