# backend/storage.py
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
STATE_PATH = DATA_DIR / "state.json"
HISTORY_PATH = DATA_DIR / "history.jsonl"

def _ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

def load_state() -> dict[str, Any]:
    _ensure_data_dir()
    if not STATE_PATH.exists():
        return {"best": {}}
    with open(STATE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def save_state(state: dict[str, Any]) -> None:
    _ensure_data_dir()
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def append_history(run_id: str, profile: dict, results: list[dict]) -> None:
    _ensure_data_dir()
    record = {"run_id": run_id, "profile": profile, "results": results}
    with open(HISTORY_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
