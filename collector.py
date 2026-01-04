import yaml
import pandas as pd
from datetime import datetime
from date_rules import generate_date_pairs

def load_config():
    with open("routes.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def collect():
    cfg = load_config()
    route = cfg["route"]

    dates = generate_date_pairs(
        cfg["date_rule"]["depart_start"],
        cfg["date_rule"]["depart_end"],
        cfg["date_rule"]["trip_length_days"],
        cfg["date_rule"]["return_deadline"],
    )

    rows = []
    for depart, ret in dates:
        rows.append({
            "data_coleta": datetime.utcnow().isoformat(timespec="seconds"),
            "origem": route["origin"],
            "destino": route["destination"],
            "ida": depart,
            "volta": ret,
            "duracao_dias": 15,
            "adultos": route["adults"],
            "criancas": route["children"],
            "direto": "S",
            "fonte": "placeholder",
            "companhia": None,
            "preco_total": None,
            "moeda": None,
            "observacoes": "APIs conectadas na próxima etapa"
        })

    return pd.DataFrame(rows)
