# app.py
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

# Opcional: ler YAML (routes.yaml)
try:
    import yaml  # type: ignore
except Exception:
    yaml = None

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # py<3.9 (não deve ser o caso)


DATA_DIR = Path("data")
ROUTES_FILE = Path("routes.yaml")
TZ_NAME = "America/Sao_Paulo"


# -------------------------
# Utilidades básicas
# -------------------------
def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def fmt_money(v: Any, currency: str = "BRL") -> str:
    try:
        x = float(v)
        return f"{currency} {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return str(v)


def local_today() -> date:
    if ZoneInfo is None:
        return datetime.utcnow().date()
    return datetime.now(ZoneInfo(TZ_NAME)).date()


def dow_name(d: date) -> str:
    # 0=Mon..6=Sun
    names = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"]
    return names[d.weekday()]


def route_key(r: Dict[str, Any]) -> str:
    return f'{r.get("origin","")}-{r.get("destination","")}:{r.get("departure_date","")}:{r.get("return_date","")}:{r.get("cabin","")}'


# -------------------------
# Regras de Ouro (expansão)
# -------------------------
def generate_rome_pairs(
    year: int,
    start_mm_dd: Tuple[int, int] = (9, 1),
    latest_return_mm_dd: Tuple[int, int] = (10, 5),
    trip_days: int = 15,
) -> List[Tuple[date, date]]:
    """
    Roma:
      - ida a partir de 01/09 (inclusive)
      - sempre 15 dias
      - retorno até 05/10 (inclusive)
    """
    start = date(year, start_mm_dd[0], start_mm_dd[1])
    latest_return = date(year, latest_return_mm_dd[0], latest_return_mm_dd[1])

    latest_depart = latest_return - timedelta(days=trip_days)
    pairs: List[Tuple[date, date]] = []

    d = start
    while d <= latest_depart:
        r = d + timedelta(days=trip_days)
        if r <= latest_return:
            pairs.append((d, r))
        d += timedelta(days=1)
    return pairs


def generate_weekend_30d_pairs(
    base: date,
    horizon_days: int = 30,
    depart_dows: Tuple[int, ...] = (4, 5),  # Sex(4) ou Sáb(5)
    return_dows: Tuple[int, ...] = (6, 0),  # Dom(6) ou Seg(0)
    max_trip_len_days: int = 4,
) -> List[Tuple[date, date]]:
    """
    Curitiba / Navegantes:
      - olhar sempre 30 dias pra frente
      - ida na sexta ou sábado
      - volta no domingo ou segunda
    """
    end = base + timedelta(days=horizon_days)
    pairs: List[Tuple[date, date]] = []

    d = base
    while d <= end:
        if d.weekday() in depart_dows:
            # possíveis retornos nos próximos dias (1..max_trip_len_days)
            for k in range(1, max_trip_len_days + 1):
                r = d + timedelta(days=k)
                if r <= end and r.weekday() in return_dows:
                    pairs.append((d, r))
        d += timedelta(days=1)

    # remover duplicatas e ordenar
    pairs = sorted(list(set(pairs)))
    return pairs


def expand_routes_from_rules(routes_yaml: Dict[str, Any], today: date) -> List[Dict[str, Any]]:
    """
    Entende duas formas:
    1) Rotas "fixas": tem departure_date/return_date
    2) Rotas com rule: "ROME_15D_WINDOW" ou "WEEKEND_30D"
    """
    routes = routes_yaml.get("routes") or []
    expanded: List[Dict[str, Any]] = []

    for r in routes:
        rule = (r.get("rule") or "").strip().upper()

        # Se tem datas fixas, mantém como 1 rota
        if r.get("departure_date") and r.get("return_date") and not rule:
            expanded.append(r)
            continue

        if rule == "ROME_15D_WINDOW":
            # por padrão usa o ano do "today"
            pairs = generate_rome_pairs(
                year=today.year,
                trip_days=int((r.get("rule_params") or {}).get("trip_days", 15)),
            )
            # se hoje já passou de 05/10 do ano corrente, mostra para o ano seguinte
            # (isso evita "vazio" fora da janela)
            if not pairs:
                pairs = generate_rome_pairs(year=today.year + 1)

            for dep, ret in pairs:
                rr = dict(r)
                rr["departure_date"] = dep.isoformat()
                rr["return_date"] = ret.isoformat()
                expanded.append(rr)

        elif rule == "WEEKEND_30D":
            params = r.get("rule_params") or {}
            horizon = int(params.get("horizon_days", 30))
            pairs = generate_weekend_30d_pairs(today, horizon_days=horizon)

            for dep, ret in pairs:
                rr = dict(r)
                rr["departure_date"] = dep.isoformat()
                rr["return_date"] = ret.isoformat()
                expanded.append(rr)

        else:
            # fallback: se não tem datas e não tem rule reconhecida, ignora com aviso no app
            rr = dict(r)
            rr["_invalid"] = True
            expanded.append(rr)

    return expanded


# -------------------------
# UI
# -------------------------
st.set_page_config(page_title="Flight Agent Dashboard", layout="wide")

st.title("✈️ Flight Agent — Dashboard")

colA, colB, colC = st.columns(3)
with colA:
    st.metric("Hoje (local)", str(local_today()))
with colB:
    st.caption("Pasta de dados")
    st.code(str(DATA_DIR), language="text")
with colC:
    st.caption("Arquivos esperados")
    st.code("data/best_offers.json\n"
            "data/alerts.json\n"
            "data/summary.md\n"
            "data/state.json\n"
            "data/history.jsonl", language="text")

best = load_json(DATA_DIR / "best_offers.json", {"by_route": {}})
alerts = load_json(DATA_DIR / "alerts.json", {"alerts": []})
summary_md = (DATA_DIR / "summary.md").read_text(encoding="utf-8") if (DATA_DIR / "summary.md").exists() else None
state = load_json(DATA_DIR / "state.json", {})

# ---- Top: status do último run
st.subheader("🧾 Último run")
c1, c2, c3, c4 = st.columns(4)
with c1:
    st.metric("run_id", str(state.get("run_id") or best.get("run_id") or alerts.get("run_id") or "—"))
with c2:
    st.metric("finished_utc", str(state.get("finished_utc") or best.get("updated_utc") or alerts.get("updated_utc") or "—"))
with c3:
    st.metric("offers_saved", str(state.get("offers_saved") or "—"))
with c4:
    st.metric("amadeus_env", str(state.get("amadeus_env") or "—"))

if summary_md:
    with st.expander("Ver summary.md"):
        st.markdown(summary_md)

st.divider()

# ---- Best Offer
st.subheader("🏆 Best Offer (sempre aparece)")
rows_best: List[Dict[str, Any]] = []
by_route = best.get("by_route") or {}

for rk, b in by_route.items():
    # b pode ser: {"origin":..,"destination":..,"price_total":..} ou outro shape
    if not isinstance(b, dict):
        continue
    price = b.get("price_total")
    if price is None:
        continue
    rows_best.append({
        "Rota": f'{b.get("origin","?")}→{b.get("destination","?")}',
        "Ida": b.get("departure_date"),
        "Volta": b.get("return_date"),
        "Cia": b.get("carrier"),
        "Stops": b.get("stops"),
        "Preço (Total)": float(price),
    })

if rows_best:
    dfb = pd.DataFrame(rows_best).sort_values("Preço (Total)", ascending=True)
    dfb["Preço (Total)"] = dfb["Preço (Total)"].apply(lambda x: fmt_money(x, "BRL"))
    st.dataframe(dfb, use_container_width=True, hide_index=True)
else:
    st.info("Ainda não há best_offers.json com conteúdo (ou não há offers válidas).")

st.divider()

# ---- Alerts
st.subheader("🚨 Alertas de preço")
als = alerts.get("alerts") or []
if not als:
    st.success("Nenhuma oferta no trigger identificada ✅")
else:
    dfa = pd.DataFrame(als)

    # Ordenação inteligente
    if "delta_pct" in dfa.columns:
        dfa = dfa.sort_values("delta_pct", ascending=False)

    # Formatar números
    if "current_price" in dfa.columns:
        dfa["current_price"] = dfa["current_price"].apply(lambda x: fmt_money(x, "BRL"))
    if "prev_best_price" in dfa.columns:
        dfa["prev_best_price"] = dfa["prev_best_price"].apply(lambda x: fmt_money(x, "BRL"))
    if "target_price" in dfa.columns:
        dfa["target_price"] = dfa["target_price"].apply(lambda x: fmt_money(x, "BRL"))
    if "delta_pct" in dfa.columns:
        dfa["delta_pct"] = dfa["delta_pct"].apply(lambda x: f"{float(x):.1f}%")

    st.dataframe(dfa, use_container_width=True, hide_index=True)

st.divider()

# ---- Planejador / Regras de Ouro
st.subheader("📅 Planejador de Rotas (regras de ouro)")

if yaml is None:
    st.warning("PyYAML não está instalado no ambiente do Streamlit. Instale para o planejador ler routes.yaml.")
else:
    if not ROUTES_FILE.exists():
        st.warning("routes.yaml não encontrado na raiz do projeto. (O scheduler também depende dele.)")
    else:
        cfg = yaml.safe_load(ROUTES_FILE.read_text(encoding="utf-8")) or {}
        today = local_today()

        expanded = expand_routes_from_rules(cfg, today)

        # Separar inválidas
        invalid = [r for r in expanded if r.get("_invalid")]
        ok = [r for r in expanded if not r.get("_invalid")]

        # Mostrar rotas “como o scheduler deveria enxergar” depois de expandir regra
        rows_plan: List[Dict[str, Any]] = []
        for r in ok:
            dep = r.get("departure_date")
            ret = r.get("return_date")
            if not dep or not ret:
                continue
            dep_d = date.fromisoformat(dep)
            ret_d = date.fromisoformat(ret)

            rows_plan.append({
                "Origem": r.get("origin"),
                "Destino": r.get("destination"),
                "Ida": dep,
                "DOW Ida": dow_name(dep_d),
                "Volta": ret,
                "DOW Volta": dow_name(ret_d),
                "Rule": (r.get("rule") or "").strip() or "FIXA",
            })

        dfp = pd.DataFrame(rows_plan)

        # Filtros
        c1, c2, c3 = st.columns(3)
        with c1:
            dest_filter = st.selectbox(
                "Filtrar destino",
                options=["(todos)"] + sorted([d for d in dfp["Destino"].unique()]) if not dfp.empty else ["(todos)"],
            )
        with c2:
            max_rows = st.slider("Máximo de linhas", 10, 300, 60, step=10)
        with c3:
            show_only_next = st.checkbox("Mostrar só as próximas 20 por destino", value=True)

        if not dfp.empty:
            if dest_filter != "(todos)":
                dfp = dfp[dfp["Destino"] == dest_filter]

            # ordenar por destino e ida
            dfp = dfp.sort_values(["Destino", "Ida"], ascending=True)

            if show_only_next:
                dfp = dfp.groupby("Destino", as_index=False).head(20)

            st.dataframe(dfp.head(max_rows), use_container_width=True, hide_index=True)
        else:
            st.info("Nenhuma rota planejada gerada a partir do routes.yaml.")

        if invalid:
            with st.expander("Rotas inválidas (sem datas e sem rule reconhecida)"):
                st.json(invalid)

st.caption(
    "Dica: se seu scheduler ainda está cravando datas, mude o routes.yaml para usar 'rule' em vez de departure_date/return_date fixos."
)
