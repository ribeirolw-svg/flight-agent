# analytics.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Optional, Tuple

import pandas as pd


def to_dataframe(records: list[dict]) -> pd.DataFrame:
    """
    Converte a lista de dicts (histórico) em DataFrame tipado e com colunas derivadas.
    Compatível com JSONL (1 dict por linha) e com legado (lista JSON).

    Espera colunas parecidas com:
      ts_utc, origin, destination, departure_date, return_date,
      adults, children, cabin, currency, direct_only,
      best_price, best_airline, best_stops, offers_count, provider, run_id, extra
    """
    if not records:
        return pd.DataFrame()

    df = pd.DataFrame.from_records(records)

    # Garantir colunas básicas (evita KeyError quando algum registro antigo não tiver)
    for col in [
        "ts_utc", "origin", "destination", "departure_date", "return_date",
        "adults", "children", "cabin", "currency", "direct_only",
        "best_price", "best_airline", "best_stops", "offers_count",
        "provider", "run_id", "extra"
    ]:
        if col not in df.columns:
            df[col] = pd.NA

    # Tipos
    df["ts_utc"] = pd.to_datetime(df["ts_utc"], errors="coerce", utc=True)

    # datas (date) — se vierem como string "YYYY-MM-DD"
    df["departure_date"] = pd.to_datetime(df["departure_date"], errors="coerce").dt.date
    df["return_date"] = pd.to_datetime(df["return_date"], errors="coerce").dt.date

    # numéricos
    for col in ["adults", "children", "offers_count", "best_stops"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    df["best_price"] = pd.to_numeric(df["best_price"], errors="coerce")

    # boolean
    # (aceita True/False, "true"/"false", 0/1)
    def _to_bool(v):
        if pd.isna(v):
            return pd.NA
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, float)):
            return bool(v)
        if isinstance(v, str):
            s = v.strip().lower()
            if s in ("true", "t", "1", "yes", "y", "sim"):
                return True
            if s in ("false", "f", "0", "no", "n", "não", "nao"):
                return False
        return pd.NA

    df["direct_only"] = df["direct_only"].map(_to_bool).astype("boolean")

    # derivados
    df["route"] = df["origin"].astype(str).fillna("") + "→" + df["destination"].astype(str).fillna("")
    df.loc[df["origin"].isna() | df["destination"].isna(), "route"] = pd.NA

    return df


def apply_filters(
    df: pd.DataFrame,
    route: Optional[str] = None,
    airline: Optional[str] = None,
    direct_only: Optional[bool] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    dep_date_from: Optional[datetime] = None,
    dep_date_to: Optional[datetime] = None,
) -> pd.DataFrame:
    """
    Aplica filtros no DataFrame.

    - date_from/date_to: filtra pela coluna ts_utc (timestamp de quando consultou)
    - dep_date_from/dep_date_to: filtra pela coluna departure_date (data da viagem)
    """
    if df is None or df.empty:
        return df

    out = df.copy()

    if route and "route" in out.columns:
        out = out[out["route"] == route]

    if airline and "best_airline" in out.columns:
        out = out[out["best_airline"] == airline]

    if direct_only is not None and "direct_only" in out.columns:
        out = out[out["direct_only"] == direct_only]

    # ts_utc window
    if "ts_utc" in out.columns:
        if date_from is not None:
            dfrom = pd.to_datetime(date_from, utc=True, errors="coerce")
            if not pd.isna(dfrom):
                out = out[out["ts_utc"] >= dfrom]
        if date_to is not None:
            dto = pd.to_datetime(date_to, utc=True, errors="coerce")
            if not pd.isna(dto):
                out = out[out["ts_utc"] <= dto]

    # departure_date window (date)
    if "departure_date" in out.columns and (dep_date_from is not None or dep_date_to is not None):
        # converte datetime -> date
        if dep_date_from is not None:
            out = out[out["departure_date"] >= pd.to_datetime(dep_date_from, errors="coerce").date()]
        if dep_date_to is not None:
            out = out[out["departure_date"] <= pd.to_datetime(dep_date_to, errors="coerce").date()]

    return out


def summary_metrics(df: pd.DataFrame) -> Dict[str, object]:
    """
    Métricas calculadas do filtro atual.
    trend_pct: tendência simples (%), comparando média dos primeiros 20% vs últimos 20% registros no período.
    """
    if df is None or df.empty:
        return {
            "rows": 0,
            "best_price_min": None,
            "best_price_avg": None,
            "best_price_max": None,
            "last_seen": None,
            "trend_pct": None,
        }

    rows = int(len(df))

    price_series = df["best_price"].dropna() if "best_price" in df.columns else pd.Series(dtype=float)
    best_min = float(price_series.min()) if len(price_series) else None
    best_avg = float(price_series.mean()) if len(price_series) else None
    best_max = float(price_series.max()) if len(price_series) else None

    last_seen = None
    if "ts_utc" in df.columns:
        mx = df["ts_utc"].max()
        if not pd.isna(mx):
            # devolve datetime aware (UTC)
            last_seen = mx.to_pydatetime()

    # tendência simples
    trend_pct = None
    if {"ts_utc", "best_price"}.issubset(df.columns):
        dfx = df.dropna(subset=["ts_utc", "best_price"]).sort_values("ts_utc")
        if len(dfx) >= 10:
            n = max(2, int(len(dfx) * 0.2))
            early = float(dfx["best_price"].head(n).mean())
            late = float(dfx["best_price"].tail(n).mean())
            if early != 0:
                trend_pct = (late - early) / early * 100.0

    return {
        "rows": rows,
        "best_price_min": best_min,
        "best_price_avg": best_avg,
        "best_price_max": best_max,
        "last_seen": last_seen,
        "trend_pct": float(trend_pct) if trend_pct is not None else None,
    }


def group_views(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Retorna duas visões agregadas:
    - by_route: estatísticas por rota
    - by_airline: estatísticas por cia
    """
    if df is None or df.empty:
        return pd.DataFrame(), pd.DataFrame()

    by_route = pd.DataFrame()
    if {"route", "best_price"}.issubset(df.columns):
        tmp = df.dropna(subset=["route", "best_price"])
        if not tmp.empty:
            by_route = (
                tmp.groupby("route", as_index=False)
                   .agg(
                       consultas=("best_price", "count"),
                       min_preco=("best_price", "min"),
                       media_preco=("best_price", "mean"),
                       max_preco=("best_price", "max"),
                   )
                   .sort_values(["min_preco", "consultas"], ascending=[True, False])
            )

    by_airline = pd.DataFrame()
    if {"best_airline", "best_price"}.issubset(df.columns):
        tmp = df.dropna(subset=["best_airline", "best_price"])
        if not tmp.empty:
            by_airline = (
                tmp.groupby("best_airline", as_index=False)
                   .agg(
                       consultas=("best_price", "count"),
                       min_preco=("best_price", "min"),
                       media_preco=("best_price", "mean"),
                       max_preco=("best_price", "max"),
                   )
                   .sort_values(["min_preco", "consultas"], ascending=[True, False])
            )

    return by_route, by_airline


def price_timeseries(df: pd.DataFrame, freq: str = "D") -> pd.DataFrame:
    """
    Série temporal do preço (min/média) no tempo, para plot ou análise.
    freq padrão: 'D' (diário). Outros: 'H', 'W', 'M' etc.

    Retorna DataFrame com colunas:
      bucket, min_price, avg_price, count
    """
    if df is None or df.empty:
        return pd.DataFrame()

    if not {"ts_utc", "best_price"}.issubset(df.columns):
        return pd.DataFrame()

    dfx = df.dropna(subset=["ts_utc", "best_price"]).copy()
    if dfx.empty:
        return pd.DataFrame()

    dfx = dfx.sort_values("ts_utc")
    dfx["bucket"] = dfx["ts_utc"].dt.floor(freq)

    out = (
        dfx.groupby("bucket", as_index=False)
           .agg(
               min_price=("best_price", "min"),
               avg_price=("best_price", "mean"),
               count=("best_price", "count"),
           )
           .sort_values("bucket")
    )
    return out


def best_deals(df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    """
    Retorna os melhores registros (menor preço), útil pra card/lista.
    """
    if df is None or df.empty or "best_price" not in df.columns:
        return pd.DataFrame()

    dfx = df.dropna(subset=["best_price"]).copy()
    if "ts_utc" in dfx.columns:
        dfx = dfx.sort_values(["best_price", "ts_utc"], ascending=[True, False])
    else:
        dfx = dfx.sort_values(["best_price"], ascending=[True])

    cols_pref = [
        "ts_utc", "route", "origin", "destination",
        "departure_date", "return_date",
        "best_airline", "best_stops", "best_price",
        "offers_count", "direct_only", "cabin", "currency", "provider"
    ]
    cols = [c for c in cols_pref if c in dfx.columns]
    return dfx[cols].head(int(top_n))
