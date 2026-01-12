from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


# Pasta onde os históricos serão salvos
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)


@dataclass
class HistoryEvent:
    ts_utc: str
    type: str
    payload: Dict[str, Any]


class HistoryStore:
    def __init__(self, name: str = "default"):
        self.name = name
        self.path = DATA_DIR / f"{name}.jsonl"

    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def append(self, event_type: str, payload: Dict[str, Any]) -> None:
        event = HistoryEvent(
            ts_utc=self._now(),
            type=event_type,
            payload=payload,
        )
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(asdict(event), ensure_ascii=False) + "\n")

    def all(self) -> List[Dict[str, Any]]:
        if not self.path.exists():
            return []

        rows = []
        with self.path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        return rows

    def filter_by_type(self, event_type: str) -> List[Dict[str, Any]]:
        return [e for e in self.all() if e.get("type") == event_type]

    def last(self, n: int = 10) -> List[Dict[str, Any]]:
        return self.all()[-n:]

    def clear(self) -> None:
        if self.path.exists():
            self.path.unlink()
