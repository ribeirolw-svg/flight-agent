# history_store.py
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

HISTORY_DIR = Path("data")
HISTORY_DIR.mkdir(exist_ok=True)
DEFAULT_HISTORY_JSONL = HISTORY_DIR / "history.jsonl"
DEFAULT_HISTORY_JSON = HISTORY_DIR / "history.json"  # legado


def utc_now_iso() -> str:
    # ISO UTC "Z"
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class SearchRecord:
    ts_utc: str
    origin: str
    destination: str
    departure_date: str
    return_date: Optional[str]
    adults: int
    children: int
    cabin: str
    currency: str
    direct_only: bool

    # oferta (melhor)
    best_price: Optional[float] = None
    best_airline: Optional[str] = None
    best_stops: Optional[int] = None

    # agregados úteis
    offers_count: Optional[int] = None
    provider: str = "amadeus"
    run_id: Optional[str] = None

    # payload auxiliar (se quiser guardar coisas extras)
    extra: Optional[Dict[str, Any]] = None


def ensure_jsonl(history_jsonl_path: Path = DEFAULT_HISTORY_JSONL,
                 legacy_json_path: Path = DEFAULT_HISTORY_JSON) -> Path:
    """
    Garante que existe um JSONL.
    Se existir history.json (lista) e não existir jsonl, migra.
    """
    if history_jsonl_path.exists():
        return history_jsonl_path

    # Migra legado (lista JSON) -> JSONL
    if legacy_json_path.exists():
        try:
            raw = json.loads(legacy_json_path.read_text(encoding="utf-8"))
            if isinstance(raw, list):
                with history_jsonl_path.open("w", encoding="utf-8") as f:
                    for item in raw:
                        f.write(json.dumps(item, ensure_ascii=False) + "\n")
                return history_jsonl_path
        except Exception:
            # se der erro, cria vazio e segue
            pass

    # cria vazio
    history_jsonl_path.write_text("", encoding="utf-8")
    return history_jsonl_path


def append_record(record: Dict[str, Any], history_jsonl_path: Path = DEFAULT_HISTORY_JSONL) -> None:
    """
    Append atômico-ish: escreve uma linha por registro.
    """
    ensure_jsonl(history_jsonl_path=history_jsonl_path)
    with history_jsonl_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def append_search_record(sr: SearchRecord, history_jsonl_path: Path = DEFAULT_HISTORY_JSONL) -> None:
    append_record(asdict(sr), history_jsonl_path=history_jsonl_path)


def iter_records(history_jsonl_path: Path = DEFAULT_HISTORY_JSONL) -> Iterable[Dict[str, Any]]:
    ensure_jsonl(history_jsonl_path=history_jsonl_path)
    with history_jsonl_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue


def load_records(history_jsonl_path: Path = DEFAULT_HISTORY_JSONL, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Carrega tudo (ou os últimos N, se limit informado).
    Para arquivos muito grandes, o ideal é usar "tail" (poderíamos implementar depois).
    """
    records = list(iter_records(history_jsonl_path))
    if limit is not None and limit > 0:
        return records[-limit:]
    return records
