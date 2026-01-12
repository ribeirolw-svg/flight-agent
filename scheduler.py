from __future__ import annotations

import os
import sys
from pathlib import Path

# -------------------------------------------------------------------
# PATH FIX: garante que o Python encontre /app e seus módulos
# -------------------------------------------------------------------
ROOT_DIR = Path(__file__).resolve().parent
APP_DIR = ROOT_DIR / "app"
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

# Agora esses imports funcionam no GitHub Actions e local
from search import run_search_and_store  # search.py está em /app
# (se o seu search.py estiver fora de /app, me avisa; mas pelo seu log, é assim que está no streamlit)

def main() -> None:
    client_id = os.environ.get("AMADEUS_CLIENT_ID", "").strip()
    client_secret = os.environ.get("AMADEUS_CLIENT_SECRET", "").strip()
    if not client_id or not client_secret:
        raise RuntimeError("Missing AMADEUS_CLIENT_ID / AMADEUS_CLIENT_SECRET env vars")

    store_name = os.environ.get("STORE_NAME", "default").strip() or "default"
    origin = os.environ.get("ORIGIN", "CGH").strip().upper()
    destination = os.environ.get("DESTINATION", "CWB").strip().upper()
    departure_date = os.environ.get("DEPARTURE_DATE", "2026-01-30").strip()
    return_date = os.environ.get("RETURN_DATE", "").strip() or None

    adults = int(os.environ.get("ADULTS", "2"))
    children = int(os.environ.get("CHILDREN", "1"))
    cabin = os.environ.get("CABIN", "ECONOMY").strip().upper()
    currency = os.environ.get("CURRENCY", "BRL").strip().upper()
    direct_only = os.environ.get("DIRECT_ONLY", "true").strip().lower() == "true"

    result = run_search_and_store(
        store_name=store_name,
        client_id=client_id,
        client_secret=client_secret,
        origin=origin,
        destination=destination,
        departure_date=departure_date,
        return_date=return_date,
        adults=adults,
        children=children,
        cabin=cabin,
        currency=currency,
        direct_only=direct_only,
    )

    print("Search stored:", result)


if __name__ == "__main__":
    main()
