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
    departure_date: str,
    run_id: str,
    queried_at_utc: str,
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    offers = raw.get("data", []) or []

    for offer in offers:
        price = (offer.get("price") or {}).get("grandTotal")
        currency = (offer.get("price") or {}).get("currency")
        last_ticketing_date = offer.get("lastTicketingDate")

        # tenta pegar 1 companhia do itinerário (nem sempre existe do jeito esperado)
        validating_airline = offer.get("validatingAirlineCodes")
        validating_airline = validating_airline[0] if isinstance(validating_airline, list) and validating_airline else None

        rows.append(
            {
                "run_id": run_id,
                "queried_at_utc": queried_at_utc,
                "origin": origin,
                "destination": destination,
                "departure_date": departure_date,
                "price_grand_total": price,
                "currency": currency,
                "validating_airline": validating_airline,
                "last_ticketing_date": last_ticketing_date,
                "offer_id": offer.get("id"),
                "raw_offer_count": len(offers),
            }
        )

    if not rows:
        rows.append(
            {
                "run_id": run_id,
                "queried_at_utc": queried_at_utc,
                "origin": origin,
                "destination": destination,
                "departure_date": departure_date,
                "price_grand_total": None,
                "currency": None,
                "validating_airline": None,
                "last_ticketing_date": None,
                "offer_id": None,
                "raw_offer_count": 0,
            }
        )

    return pd.DataFrame(rows)

# ------------------------------------------------------------
# Excel append (histórico)
# ------------------------------------------------------------
def append_to_excel(path: str, sheet_name: str, df_new: pd.DataFrame) -> None:
    if not os.path.exists(path):
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            df_new.to_excel(writer, sheet_name=sheet_name, index=False)
        return

    # lê existente
    try:
        df_old = pd.read_excel(path, sheet_name=sheet_name)
    except Exception:
        df_old = pd.DataFrame()

    df_all = pd.concat([df_old, df_new], ignore_index=True)

    with pd.ExcelWriter(path, engine="openpyxl", mode="w") as writer:
        df_all.to_excel(writer, sheet_name=sheet_name, index=False)

# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
def main() -> None:
    if not os.path.exists(ROUTES_FILE):
        raise FileNotFoundError(f"Não achei routes.yaml em: {ROUTES_FILE}")

    cfg = load_routes_config(ROUTES_FILE)

    # routes.yaml pode ter:
    # routes:
    #   - origin: GRU
    #     destination: FCO
    #     nonstop: true
    routes = cfg.get("routes") or []
    if not routes:
        # fallback: um default
        routes = [{"origin": "GRU", "destination": "FCO", "nonstop": True}]

    departure_dates = get_departure_dates(cfg)

    run_id = str(uuid.uuid4())[:8]
    queried_at_utc = datetime.now(timezone.utc).isoformat()

    print("=======================================")
    print("SCHEDULER RUN_ID:", run_id)
    print("queried_at_utc:", queried_at_utc)
    print("routes_count:", len(routes))
    print("dates_count:", len(departure_dates))
    print("output:", OUTPUT_XLSX)
    print("=======================================")

    all_frames: List[pd.DataFrame] = []

    for r in routes:
        origin = (r.get("origin") or "GRU").strip().upper()
        destination = (r.get("destination") or "FCO").strip().upper()
        nonstop = bool(r.get("nonstop", True))

        for dep in departure_dates:
            try:
                raw = search_flights(
                    origin=origin,
                    destination=destination,
                    departure_date=dep,
                    adults=int(cfg.get("adults", 1)),
                    currency=str(cfg.get("currency", "BRL")),
                    nonstop=nonstop,
                    max_results=int(cfg.get("max_results", 5)),
                )
                df = normalize_offers(
                    raw=raw,
                    origin=origin,
                    destination=destination,
                    departure_date=dep,
                    run_id=run_id,
                    queried_at_utc=queried_at_utc,
                )
                all_frames.append(df)
                print(f"[OK] {origin}->{destination} {dep} | offers={df['raw_offer_count'].iloc[0]}")
            except Exception as e:
                print(f"[ERRO] {origin}->{destination} {dep} | {e}")
                # registra erro também no histórico
                df_err = pd.DataFrame([{
                    "run_id": run_id,
                    "queried_at_utc": queried_at_utc,
                    "origin": origin,
                    "destination": destination,
                    "departure_date": dep,
                    "price_grand_total": None,
                    "currency": None,
                    "validating_airline": None,
                    "last_ticketing_date": None,
                    "offer_id": None,
                    "raw_offer_count": None,
                    "error": str(e),
                }])
                all_frames.append(df_err)

    df_all = pd.concat(all_frames, ignore_index=True) if all_frames else pd.DataFrame()
    append_to_excel(OUTPUT_XLSX, SHEET_NAME, df_all)

    print("=======================================")
    print("FINALIZADO")
    print("linhas_gravadas_neste_run:", len(df_all))
    print("arquivo:", OUTPUT_XLSX)
    print("=======================================")

if __name__ == "__main__":
    main()
