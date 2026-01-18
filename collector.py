import os
import json
import yaml
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path
from date_rules import generate_date_pairs

# ----------------------------
# Amadeus endpoints (TEST)
# If you move to production later, change BASE_URL to:
#   https://api.amadeus.com
# ----------------------------
BASE_URL = "https://test.api.amadeus.com"
TOKEN_URL = f"{BASE_URL}/v1/security/oauth2/token"
FLIGHT_OFFERS = f"{BASE_URL}/v2/shopping/flight-offers"
AIRLINE_LOOKUP = f"{BASE_URL}/v1/reference-data/airlines"

# ----------------------------
# Debug storage (repo root/data/debug)
# This assumes this file is at repo root: flight-agent/collector.py
# ----------------------------
ROOT_DIR = Path(__file__).resolve().parent
DEBUG_DIR = ROOT_DIR / "data" / "debug"
DEBUG_DIR.mkdir(parents=True, exist_ok=True)

DEBUG_SAVE_RAW = True  # set False later if you want

# Cache for airline name lookups
AIRLINE_NAME_CACHE = {}

# OPTIONAL: make UI faster (limit how many date pairs to query)
# Set to None to query all valid pairs.
MAX_DATE_PAIRS = 3  # <-- keep 3 for faster manual runs; set None for full scan


def load_config():
    with open(ROOT_DIR / "routes.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def amadeus_get_token(client_id: str, client_secret: str) -> str:
    resp = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def amadeus_search_offers(
    token: str,
    origin: str,
    destination: str,
    depart: str,
    ret: str,
    adults: int,
    children: int,
    max_results: int = 50,
) -> dict:
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "originLocationCode": origin,
        "destinationLocationCode": destination,
        "departureDate": depart,
        "returnDate": ret,
        "adults": adults,
        "children": children,
        # IMPORTANT: do NOT use nonStop here; we'll filter direct ourselves (more reliable)
        "max": str(max_results_
