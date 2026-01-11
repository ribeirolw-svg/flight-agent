import os
import uuid
import yaml
import pandas as pd
from datetime import datetime, timezone, date, timedelta
from typing import Dict, Any, List, Tuple, Optional

from search import search_flights

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(PROJECT_DIR, "routes.yaml")  # mantém o nome que você já usa
DATA_DIR = os.getenv("DATA_DIR", os.path.join(PROJECT_DIR, "data"))

os.makedirs(DATA_DIR, exist_ok=True)

OUTPUT_XLSX = os.path.join(DATA_DIR, "flights.xlsx")
SHEET_NAME = "history"

def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def parse_yyyy_mm_dd(s: str) -> date:
    y, m, d = [int(x) for x in s.split("-")]
    return date(y, m, d)

def daterange(start: date, end: date) -> List[date]:
    # inclusive
    out = []
    cur = start
    while cur <= end:
        out.append(cur)
        cur += timedelta(days=1)
    return out

def build_trip_pairs(date_rule: Dict[str, Any]) -> List[Tuple[str, str]]:
    """
    Gera pares (departure_date, return_date) conforme:
      depart_start, depart_end, trip_length_days, return_deadline
    Regras:
      return = depart + trip_length_days
      se return > return_deadline -> descarta
    """
    depart_start = parse_yyyy_mm_dd(date_rule["depart_start"])
    depart_end = parse_yyyy_mm_dd(date_rule["depart_end"])
    trip_len = int(date_rule["trip_length_days"])
    return_deadline = parse_yyyy_mm_dd(date_rule["return_deadline"])

    pairs = []
    for dep in daterange(depart_start, depart_end):
        ret = dep + timedelta(days=trip_len)
        if ret <= return_deadline:
            pairs.append((dep.isoformat(), ret.isoformat()))
    return pairs

def normalize_offers(
    raw: Dict[str, Any],
    origin: str,
    destination: str,
    departure_date: str,
    return_date: Optional[str],
    run_id: str,
    queried_at_utc: str,
    source: str,
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    offers = raw.get("data", []) or []

    for offer in offers:
        price_obj = offer.get("price") or {}
        price = price_obj.get("grandTotal")
        currency = price_obj.get("currency")
        last_ticketing_date = offer.get("lastTicketingDate")

        validating = offer.get("validatingAirlineCodes")
        validating_airline = validating[0] if isinstance(validating, list) and validating else None

        rows.append({
            "run_id": run_id,
            "queried_at_utc": queried_at_utc,
            "source": source,
            "origin": origin,
            "destination": destination,
            "departure_date": departure_date,
            "return_date": return_date,
            "price_grand_total": price,
            "currency": currency,
            "validating_airline": validating_airline,
            "last_ticketing_date": last_ticketing_date,
            "offer_id": offer.get("id"),
            "raw_offer_count": len(offers),
        })

    if not rows:
        rows.append({
            "run_id": run_id,
            "queried_at_utc": queried_at_utc,
            "source": source,
            "origin": origin,
            "destination": destination,
            "departure_date": departure_date,
            "return_date": return_date,
            "price_grand_total": None,
            "currency": None,
            "validating_airline": None,
            "last_ticketing_date": None,
            "offer_id": None,
            "raw_offer_count": 0,
        })

    return pd.DataFrame(rows)

def append_to_excel(path: str, sheet_name: str, df_new: pd.DataFrame) -> None:
    if not os.path.exists(path):
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            df_new.to_excel(writer, sheet_name=sheet_name, index=False)
        return

    try:
        df_old = pd.read_excel(path, sheet_name=sheet_name)
    except Exception:
        df_old = pd.DataFrame()

    df_all = pd.concat([df_old, df_new], ignore_index=True)

    with pd.ExcelWriter(path, engine="openpyxl", mode="w") as writer:
        df_all.to_excel(writer, sheet_name=sheet_name, index=False)

def main() -> None:
    if not os.path.exists(CONFIG_FILE):
        raise FileNotFoundError(f"Não achei routes.yaml em: {CONFIG_FILE}")

    cfg = load_config(CONFIG_FILE)

    route = cfg.get("route") or {}
    date_rule = cfg.get("date_rule") or {}
    sources = cfg.get("sources") or ["amadeus"]

    # Só vamos executar Amadeus aqui; Kiwi fica para outro adapter/arquivo
    use_amadeus = "amadeus" in [s.lower() for s in sources]

    origin = (route.get("origin") or "GRU").strip().upper()
    destination = (route.get("destination") or "FCO").strip().upper()
    direct_only = bool(route.get("direct_only", True))
    adults = int(route.get("adults", 1))
    children = int(route.get("children", 0))
    cabin = (route.get("cabin") or "ECONOMY").strip().upper()

    # children_ages está no YAML mas o endpoint do Amadeus não aceita idades diretamente neste método
    # (ele aceita a contagem de children). Então vamos ignorar idades por ora.
    # children_ages = route.get("children_ages", [])

    pairs = build_trip_pairs(date_rule)

    run_id = str(uuid.uuid4())[:8]
    queried_at_utc = datetime.now(timezone.utc).isoformat()

    print("=======================================")
    print("SCHEDULER RUN_ID:", run_id)
    print("queried_at_utc:", queried_at_utc)
    print("origin/destination:", origin, destination)
    print("direct_only:", direct_only, "| adults:", adults, "| children:", children, "| cabin:", cabin)
    print("pairs_count:", len(pairs))
    print("sources:", sources)
    print("output:", OUTPUT_XLSX)
    print("=======================================")

    frames: List[pd.DataFrame] = []

    if not use_amadeus:
        print("[INFO] 'amadeus' não está em sources. Nada a fazer.")
        return

    max_results = int(os.getenv("AMADEUS_MAX_RESULTS", "5"))

    for dep, ret in pairs:
        try:
            raw = search_flights(
                origin=origin,
                destination=destination,
                departure_date=dep,
                return_date=ret,
                adults=adults,
                children=children,
                travel_class=cabin,
                currency="BRL",
                nonstop=direct_only,
                max_results=max_results,
            )
            df = normalize_offers(
                raw=raw,
                origin=origin,
                destination=destination,
                departure_date=dep,
                return_date=ret,
                run_id=run_id,
                queried_at_utc=queried_at_utc,
                source="amadeus",
            )
            frames.append(df)
            offers_n = int(df["raw_offer_count"].iloc[0]) if "raw_offer_count" in df.columns else 0
            print(f"[OK] {origin}->{destination} {dep}->{ret} | offers={offers_n}")
        except Exception as e:
            print(f"[ERRO] {origin}->{destination} {dep}->{ret} | {e}")
            frames.append(pd.DataFrame([{
                "run_id": run_id,
                "queried_at_utc": queried_at_utc,
                "source": "amadeus",
                "origin": origin,
                "destination": destination,
                "departure_date": dep,
                "return_date": ret,
                "price_grand_total": None,
                "currency": None,
                "validating_airline": None,
                "last_ticketing_date": None,
                "offer_id": None,
                "raw_offer_count": None,
                "error": str(e),
            }]))

    df_all = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    append_to_excel(OUTPUT_XLSX, SHEET_NAME, df_all)

    print("=======================================")
    print("FINALIZADO")
    print("linhas_gravadas_neste_run:", len(df_all))
    print("arquivo:", OUTPUT_XLSX)
    print("=======================================")

if __name__ == "__main__":
    main()
