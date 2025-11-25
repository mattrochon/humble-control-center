import json
import os
from pathlib import Path
from typing import Any, Dict


def default_data_dir() -> Path:
    root = Path(__file__).resolve().parent.parent
    return root / "data"


class UIState:
    def __init__(self, path: Path | None = None):
        self.data_dir = path or default_data_dir()
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.state_file = self.data_dir / "ui_state.json"
        self.data = self._load()

    def _load(self) -> Dict[str, Any]:
        if self.state_file.exists():
            with self.state_file.open() as f:
                return json.load(f)
        return {
            "session_cookie": "",
            "library_path": str(self.data_dir / "library"),
            "include": [],
            "exclude": [],
            "platforms": [],
            "trove": False,
            "openwebui_url": "",
            "openwebui_model": "",
            "openwebui_api_key": "",
            "auth_header_name": "",
            "auth_header_value": "",
        }

    def save(self):
        with self.state_file.open("w") as f:
            json.dump(self.data, f, indent=2)

    def set_cookie(self, cookie_value: str):
        self.data["session_cookie"] = cookie_value.strip()
        self.save()

    def set_library_path(self, library_path: str):
        self.data["library_path"] = os.path.expanduser(library_path)
        self.save()

    def set_filters(self, include=None, exclude=None, platforms=None, trove=None):
        if include is not None:
            self.data["include"] = include
        if exclude is not None:
            self.data["exclude"] = exclude
        if platforms is not None:
            self.data["platforms"] = platforms
        if trove is not None:
            self.data["trove"] = bool(trove)
        self.save()

    def set_openwebui(self, url: str | None = None, model: str | None = None, api_key: str | None = None):
        if url is not None:
            self.data["openwebui_url"] = url.strip()
        if model is not None:
            self.data["openwebui_model"] = model.strip()
        if api_key is not None:
            self.data["openwebui_api_key"] = api_key.strip()
        self.save()

    def set_auth_header(self, name: str | None = None, value: str | None = None):
        if name is not None:
            self.data["auth_header_name"] = name.strip()
        if value is not None:
            self.data["auth_header_value"] = value.strip()
        self.save()

    def ready(self) -> bool:
        return bool(self.data.get("session_cookie")) and bool(
            self.data.get("library_path")
        )
