import os
import uuid
import yaml
import pandas as pd
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple

from search import search_flights  # usa o search.py que ajustamos

# ------------------------------------------------------------
# Configs
# ------------------------------------------------------------
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
ROUTES_FILE = os.path.join(PROJECT_DIR, "routes.yaml")

DATA_DIR = os.path.join(PROJECT_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

OUTPUT_XLSX = os.path.join(DATA_DIR, "flights.xlsx")
SHEET_NAME = "history"

# ------------------------------------------------------------
# Util: carregar YAML
# ------------------------------------------------------------
def load_routes_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

# ------------------------------------------------------------
# Util: gerar pares de datas
# - Se você já tem date_rules.generate_date_pairs, ele usa.
# - Se não tiver, usa um fallback simples.
# ------------------------------------------------------------
def generate_date_list_fallback(
    start_date: str,
    days: int,
    step: int = 7,
) -> List[str]:
    """
    start_date: 'YYYY-MM-DD'
    days: quantidade de dias para frente
    step: intervalo entre buscas (ex.: 7 = semanal)
    """
    from datetime import date, timedelta

    y, m, d = [int(x) for x in start_date.split("-")]
    start = date(y, m, d)
    out = []
    for offset in range(0, days + 1, step):
        out.append((start + timedelta(days=offset)).isoformat())
    return out


def get_departure_dates(cfg: Dict[str, Any]) -> List[str]:
    """
    Espera algo no routes.yaml como:
      dates:
        start: "2026-03-10"
        horizon_days: 120
        step_days: 7

    OU, se você já usa date_rules.py:
      dates:
        mode: "pairs"
        ... (o que você já tinha)
    """
    dates_cfg = (cfg.get("dates") or {})
    # tenta usar sua função existente, se disponível
    try:
        from date_rules import generate_date_pairs  # type: ignore

        # Se você já tinha um schema específico, adapte aqui.
        # Como não tenho seu date_rules, vou suportar um modo simples:
        # se existir 'date_pairs' no YAML, usamos direto.
        if "date_pairs" in dates_cfg:
            # Ex.: date_pairs: [["2026-03-10","2026-03-17"], ...]
            pairs = dates_cfg["date_pairs"]
            # aqui vamos usar só a ida (primeiro elemento)
            return [p[0] for p in pairs if p and len(p) >= 1]

        # Se você quer realmente usar generate_date_pairs:
        # deixe no YAML os parâmetros que seu generate_date_pairs espera.
        # Ex.: dates_cfg["pairs_params"] = {...}
        if "pairs_params" in dates_cfg:
            pairs = generate_date_pairs(**dates_cfg["pairs_params"])
            return [p[0] for p in pairs if p and len(p) >= 1]

    except Exception:
        pass

    # fallback
    start = dates_cfg.get("start")
    horizon_days = int(dates_cfg.get("horizon_days", 120))
    step_days = int(dates_cfg.get("step_days", 7))
    if not start:
        # fallback padrão: começa daqui 30 dias
        from datetime import date, timedelta
        start = (date.today() + timedelta(days=30)).isoformat()

    return generate_date_list_fallback(start_date=start, days=horizon_days, step=step_days)

# ------------------------------------------------------------
# Parsing de resposta do Amadeus
# ------------------------------------------------------------
def normalize_offers(
    raw: Dict[str, Any],
    origin: str,
    destination: str,
    departu
