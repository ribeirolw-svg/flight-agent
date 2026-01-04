import os
import yaml
import requests
import pandas as pd
from datetime import datetime
from date_rules import generate_date_pairs

AMADEUS_TOKEN_URL = "https://test.api.amadeus.com/v1/security/oauth2/token"
AMADEUS_FLIGHT_OFFERS = "https://test.api.amadeus.com/v2/shopping/flight-offers"

def load_config():
    with open("routes.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def amadeus_get_token(client_id: str, client_secret: str) -> str:
    resp = requests.post(
        AMADEUS_TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]

def amadeus_search_offers(token: str, origin: str, destination: str, depart: str, ret: str,
                          adults: int, children: int, non_stop: bool = True, max_results: int = 25):
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "originLocationCode": origin,
        "destinationLocationCode": destination,
        "departureDate": depart,
        "returnDate": ret,
        "adults": adults,
        "children": children,
        "nonStop": "true" if non_stop else "false",
        "currencyCode": "BRL",  # se preferir "como vier", posso tirar isso; mas BRL facilita leitura
        "max": str(max_results),
    }
    resp = requests.get(AMADEUS_FLIGHT_OFFERS, headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()

def normalize_offers(data_json, base_row):
    rows = []
    data = data_json.get("data", [])
    for offer in data:
        price = offer.get("price", {})
        grand_total = price.get("grandTotal")
        currency = price.get("currency")

        # tentar puxar companhias (nem sempre vem completo)
        validating = offer.get("validatingAirlineCodes", [])
        airline = validating[0] if validating else None

        rows.append({
            **base_row,
            "fonte": "amadeus",
            "companhia": airline,
            "preco_total": grand_total,
            "moeda": currency,
            "observacoes": None,
        })
    if not rows:
        rows.append({
            **base_row,
            "fonte": "amadeus",
            "companhia": None,
            "preco_total": None,
            "moeda": None,
            "observacoes": "Sem oferta retornada (nonstop pode estar restritivo ou sem disponibilidade).",
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
        raise RuntimeError("Secrets do Amadeus não configurados no Streamlit (AMADEUS_CLIENT_ID / AMADEUS_CLIENT_SECRET).")

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
            "direto": "S" if route.get("direct_only", True) else "N",
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
                non_stop=route.get("direct_only", True),
                max_results=10,
            )
            rows.extend(normalize_offers(json_data, base_row))
        except Exception as e:
            rows.append({
                **base_row,
                "fonte": "amadeus",
                "companhia": None,
                "preco_total": None,
                "moeda": None,
                "observacoes": f"Erro na consulta: {type(e).__name__}",
            })

    df = pd.DataFrame(rows)

    # ordena: menor preço primeiro (quando existir)
    if "preco_total" in df.columns:
        df["preco_total_num"] = pd.to_numeric(df["preco_total"], errors="coerce")
        df = df.sort_values(["preco_total_num", "ida"], ascending=[True, True]).drop(columns=["preco_total_num"])

    return df
