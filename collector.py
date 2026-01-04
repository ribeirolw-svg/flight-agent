import os
import yaml
import requests
import pandas as pd
from datetime import datetime
from date_rules import generate_date_pairs

# Se quiser ir para produção depois: "https://api.amadeus.com"
BASE_URL = "https://test.api.amadeus.com"
TOKEN_URL = f"{BASE_URL}/v1/security/oauth2/token"
FLIGHT_OFFERS = f"{BASE_URL}/v2/shopping/flight-offers"
AIRLINE_LOOKUP = f"{BASE_URL}/v1/reference-data/airlines"

# Cache simples para nomes de companhias (evita chamadas repetidas)
AIRLINE_NAME_CACHE = {}

def load_config():
    with open("routes.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def amadeus_get_token(client_id, client_secret):
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

def amadeus_search_offers(token, origin, destination, depart, ret, adults, children, max_results=50):
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "originLocationCode": origin,
        "destinationLocationCode": destination,
        "departureDate": depart,
        "returnDate": ret,
        "adults": adults,
        "children": children,
        # NÃO usar nonStop aqui; filtramos "direto" via segmentos
        "max": str(max_results),
        # moeda "como vier" -> não definir currencyCode
    }
    resp = requests.get(FLIGHT_OFFERS, headers=headers, params=params, timeout=30)
    if resp.status_code >= 400:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:300]}")
    return resp.json()

def is_roundtrip_direct(offer):
    """Direto = 1 segmento na ida + 1 segmento na volta."""
    itins = offer.get("itineraries", [])
    if len(itins) < 2:
        return False
    out_segs = itins[0].get("segments", [])
    in_segs = itins[1].get("segments", [])
    return (len(out_segs) == 1) and (len(in_segs) == 1)

def lookup_airline_name(token, airline_code):
    code = (airline_code or "").strip().upper()
    if not code:
        return None

    if code in AIRLINE_NAME_CACHE:
        return AIRLINE_NAME_CACHE[code]

    headers = {"Authorization": f"Bearer {token}"}
    params = {"airlineCodes": code}
    resp = requests.get(AIRLINE_LOOKUP, headers=headers, params=params, timeout=30)

    if resp.status_code >= 400:
        AIRLINE_NAME_CACHE[code] = None
        return None

    data = resp.json().get("data", [])
    name = None
    if isinstance(data, list) and len(data) > 0:
        item = data[0]
        name = item.get("commonName") or item.get("businessName")

    AIRLINE_NAME_CACHE[code] = name
    return name

def normalize_direct_offers(data_json, base_row, token):
    rows = []
    offers = data_json.get("data", [])
    direct_offers = [o for o in offers if is_roundtrip_direct(o)]

    for offer in direct_offers:
        price = offer.get("price", {})
        grand_total = price.get("grandTotal")
        currency = price.get("currency")

        validating = offer.get("validatingAirlineCodes", [])
        airline_code = validating[0] if validating else None
        airline_name = lookup_airline_name(token, airline_code) if airline_code else None

        rows.append({
            **base_row,
            "fonte": "amadeus",
            "companhia": airline_code,
            "companhia_nome": airline_name,
            "preco_total": grand_total,
            "moeda": currency,
            "observacoes": "Direto (filtrado por segmentos)",
        })

    if not rows:
        rows.append({
            **base_row,
            "fonte": "amadeus",
            "companhia": None,
            "companhia_nome": None,
            "preco_total": None,
            "moeda": None,
            "observacoes": "Nenhuma oferta DIRETA retornada para esta data (filtrado por segmentos).",
        })

    return rows

def collect():
    cfg = load_config()
    route = cfg["route"]
    dates = generate_date_pairs(
        cfg["date_rule"]["depart_start"],
        cfg["date_rule"]["depart_end"],
        cfg["date_rule"]["trip_length_days"],
        cfg["date_rule"]["return_deadline"],
    )

    client_id = os.environ.get("AMADEUS_CLIENT_ID")
    client_secret = os.environ.get("AMADEUS_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise RuntimeError("Secrets do Amadeus não configurados (AMADEUS_CLIENT_ID / AMADEUS_CLIENT_SECRET).")

    token = amadeus_get_token(client_id, client_secret)

    rows = []
    for depart, ret in dates:
        base_row = {
            "data_coleta": datetime.utcnow().isoformat(timespec="seconds"),
            "origem": route["origin"],
            "destino": route["destination"],
            "ida": depart,
            "volta": ret,
            "duracao_dias": cfg["date_rule"]["trip_length_days"],
            "adultos": route["adults"],
            "criancas": route["children"],
            "direto": "S",
        }

        try:
            json_data = amadeus_search_offers(
                token=token,
                origin=route["origin"],
                destination=route["destination"],
                depart=depart,
                ret=ret,
                adults=route["adults"],
                children=route["children"],
                max_results=50,
            )
            rows.extend(normalize_direct_offers(json_data, base_row, token))
        except Exception as e:
            rows.append({
                **base_row,
                "fonte": "amadeus",
                "companhia": None,
                "companhia_nome": None,
                "preco_total": None,
                "moeda": None,
                "observacoes": f"Erro na consulta: {str(e)[:160]}",
            })

    df = pd.DataFrame(rows)

    # Ordena por menor preço quando houver
    if "preco_total" in df.columns:
        df["_preco_num"] = pd.to_numeric(df["preco_total"], errors="coerce")
        df = df.sort_values(["_preco_num", "ida"], ascending=[True, True]).drop(columns=["_preco_num"])

    return df
