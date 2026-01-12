#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import yaml

# =========================
# Config / Env
# =========================
DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
ROUTES_FILE = os.getenv("ROUTES_FILE", "routes.yaml")
ALERTS_FILE = os.getenv("ALERTS_FILE", "alerts.yaml")  # mantido p/ compatibilidade
STORE_NAME = os.getenv("STORE_NAME", "default")
MAX_RESULTS = int(os.getenv("AMADEUS_MAX_RESULTS", "10"))
AMADEUS_ENV = os.getenv("AMADEUS_ENV", "").strip().lower() or "test"  # evita vazio

# =========================
# Guardrails (regras imutáveis)
# =========================
# Origens e destinos obrigatórios que NÃO PODEM SUMIR sem você pedir.
REQUIRED_ORIGINS: Set[str] = {"GRU", "CGH"}
REQUIRED_DESTS: Set[str] = {"FCO", "CWB", "NVT"}

# =========================
# Paths (artefatos)
# =========================
HISTORY_PATH = DATA_DIR / "history.jsonl"
STATE_PATH = DATA_DIR / "state.json"
ALERTS_PATH = DATA_DIR / "alerts.json"
SUMMARY_MD_PATH = DATA_DIR / "summary.md"
RUNS_DIR = DATA_DIR / "runs"


# =========================
# Helpers
# =========================
def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def run_id_from_dt(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    RUNS_DIR.mkdir(parents=True, exist_ok=True)


def load_yaml_file(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def append_jsonl(path: Path, obj: Dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def write_text(path: Path, txt: str) -> None:
    path.write_text(txt, encoding="utf-8")


def safe_str(e: BaseException) -> str:
    return f"{type(e).__name__}: {e}"


def extract_error_code(message: str) -> str:
    m = message.lower()
    if "401" in m or "unauthorized" in m or "invalid_client" in m:
        return "HTTP_401"
    if "403" in m or "forbidden" in m:
        return "HTTP_403"
    if "404" in m:
        return "HTTP_404"
    if "408" in m or "timeout" in m:
        return "TIMEOUT"
    if "429" in m or "too many requests" in m:
        return "HTTP_429"
    if "500" in m:
        return "HTTP_500"
    if "502" in m:
        return "HTTP_502"
    if "503" in m:
        return "HTTP_503"
    if "504" in m:
        return "HTTP_504"
    if "no offers" in m or "no_offers" in m:
        return "NO_OFFERS"
    return "ERR_UNKNOWN"


def summarize_offer_for_log(offer: Dict[str, Any]) -> str:
    price = ""
    currency = ""
    carrier = ""
    stops = ""

    try:
        price = str(offer.get("price", {}).get("total") or offer.get("price", {}).get("grandTotal") or "")
    except Exception:
        price = ""

    try:
        currency = str(offer.get("price", {}).get("currency") or "")
    except Exception:
        currency = ""

    try:
        vac = offer.get("validatingAirlineCodes")
        if isinstance(vac, list) and vac:
            carrier = str(vac[0])
    except Exception:
        carrier = ""

    try:
        itins = offer.get("itineraries")
        if isinstance(itins, list) and itins:
            segs = itins[0].get("segments", [])
            if isinstance(segs, list) and len(segs) > 0:
                stops = f"{max(len(segs) - 1, 0)} stop(s)"
    except Exception:
        stops = ""

    bits = [b for b in [carrier, (f"{currency} {price}".strip()), stops] if b]
    return " | ".join(bits) if bits else "offer"


def offer_signature(route_meta: Dict[str, Any], offer: Dict[str, Any]) -> str:
    carrier = ""
    try:
        vac = offer.get("validatingAirlineCodes")
        if isinstance(vac, list) and vac:
            carrier = str(vac[0])
    except Exception:
        carrier = ""

    price = ""
    try:
        price = str(offer.get("price", {}).get("total") or offer.get("price", {}).get("grandTotal") or "")
    except Exception:
        price = ""

    return (
        f"{route_meta.get('origin')}->{route_meta.get('destination')}|"
        f"{route_meta.get('departure_date')}|{route_meta.get('return_date')}|{carrier}|{price}"
    )


# =========================
# Core Search Integration
# =========================
def import_search_callable():
    """
    Espera existir search.py com a função:
      search_offers_for_route(route: dict, *, max_results: int, env: str) -> list[dict]
    """
    try:
        import search  # type: ignore
    except Exception as e:
        raise RuntimeError(
            "Não consegui importar 'search.py'. Garanta que existe search.py na raiz."
        ) from e

    fn = getattr(search, "search_offers_for_route", None)
    if not callable(fn):
        raise RuntimeError(
            "Em search.py, esperava uma função 'search_offers_for_route(route, *, max_results, env)'. "
            "Se seu search usa outro nome/assinatura, cola aqui que eu ajusto."
        )
    return fn


# =========================
# Scheduler
# =========================
@dataclass
class CallResult:
    ok: bool
    offers_count: int = 0
    err_code: Optional[str] = None
    err_msg: Optional[str] = None


def validate_immutable_rules(routes: List[Dict[str, Any]]) -> None:
    """
    Guardrail: se origens/destinos obrigatórios sumirem do routes.yaml, falha imediatamente.
    """
    origins_in_yaml = {r.get("origin") for r in routes if isinstance(r, dict)}
    dests_in_yaml = {r.get("destination") for r in routes if isinstance(r, dict)}

    origins_clean = {o for o in origins_in_yaml if isinstance(o, str) and o.strip()}
    dests_clean = {d for d in dests_in_yaml if isinstance(d, str) and d.strip()}

    missing_o = REQUIRED_ORIGINS - origins_clean
    missing_d = REQUIRED_DESTS - dests_clean

    if missing_o or missing_d:
        print("[FATAL] routes.yaml violou regras imutáveis.")
        if missing_o:
            print(f"[FATAL] Origens faltando: {sorted(missing_o)}")
        if missing_d:
            print(f"[FATAL] Destinos faltando: {sorted(missing_d)}")
        sys.exit(1)


def main() -> None:
    ensure_dirs()

    started_dt = utc_now()
    started_utc = iso_z(started_dt)
    rid = run_id_from_dt(started_dt)

    total_calls = 0
    ok_calls = 0
    err_calls = 0
    offers_saved = 0
    errors: List[Dict[str, Any]] = []
    sample_offers: List[str] = []
    sample_seen: Set[str] = set()

    # load routes
    try:
        routes_cfg = load_yaml_file(ROUTES_FILE)
    except Exception as e:
        print(f"[FATAL] Falha ao ler {ROUTES_FILE}: {safe_str(e)}")
        sys.exit(1)

    routes = routes_cfg.get("routes") if isinstance(routes_cfg, dict) else None
    if not isinstance(routes, list) or not routes:
        print(f"[FATAL] Nenhuma rota encontrada em {ROUTES_FILE}. Esperava chave 'routes:'.")
        sys.exit(1)

    # Guardrail IMEDIATO (antes de gastar chamadas)
    validate_immutable_rules(routes)

    # import search callable
    try:
        search_fn = import_search_callable()
    except Exception as e:
        print(f"[FATAL] {safe_str(e)}")
        sys.exit(1)

    print(f"[INFO] Run: {rid}")
    print(f"[INFO] Store: {STORE_NAME} | Env: {AMADEUS_ENV} | Max results: {MAX_RESULTS}")
    print(f"[INFO] Routes: {len(routes)} | Routes file: {ROUTES_FILE}")

    for i, route in enumerate(routes, start=1):
        total_calls += 1

        route_meta = {
            "idx": i,
            "origin": route.get("origin"),
            "destination": route.get("destination"),
            "departure_date": route.get("departure_date"),
            "return_date": route.get("return_date"),
            "adults": route.get("adults"),
            "children": route.get("children"),
            "cabin": route.get("cabin"),
            "currency": route.get("currency"),
            "direct_only": route.get("direct_only"),
        }

        try:
            offers = search_fn(route, max_results=MAX_RESULTS, env=AMADEUS_ENV)  # type: ignore
            if not isinstance(offers, list):
                raise RuntimeError(f"search_fn retornou tipo inválido: {type(offers)}")

            ok_calls += 1
            offers_count = len(offers)

            for off in offers:
                if not isinstance(off, dict):
                    continue
                record = {
                    "run_id": rid,
                    "ts_utc": started_utc,
                    **route_meta,
                    "offer": off,
                }
                append_jsonl(HISTORY_PATH, record)
                offers_saved += 1

                # sample únicos (até 3)
                if len(sample_offers) < 3:
                    sig = offer_signature(route_meta, off)
                    if sig not in sample_seen:
                        sample_seen.add(sig)
                        sample_offers.append(
                            f"{route_meta.get('origin')}->{route_meta.get('destination')} "
                            f"{route_meta.get('departure_date')}/{route_meta.get('return_date')} | "
                            f"{summarize_offer_for_log(off)}"
                        )

            print(
                f"[OK] ({i}/{len(routes)}) {route_meta.get('origin')}->{route_meta.get('destination')} "
                f"{route_meta.get('departure_date')}/{route_meta.get('return_date')} | offers: {offers_count}"
            )

        except Exception as e:
            err_calls += 1
            msg = safe_str(e)
            code = extract_error_code(msg)

            errors.append({
                "run_id": rid,
                "route": route_meta,
                "code": code,
                "message": msg,
            })

            print(
                f"[ERR] ({i}/{len(routes)}) {route_meta.get('origin')}->{route_meta.get('destination')} "
                f"{route_meta.get('departure_date')}/{route_meta.get('return_date')} | {code} | {msg}"
            )
            continue

    finished_dt = utc_now()
    finished_utc = iso_z(finished_dt)
    duration_sec = int((finished_dt - started_dt).total_seconds())
    success_rate = (ok_calls / total_calls) if total_calls else 0.0

    top_errors = Counter([e.get("code", "ERR_UNKNOWN") for e in errors])

    # state.json (compacto)
    state = {
        "store": STORE_NAME,
        "env": AMADEUS_ENV,
        "max_results": MAX_RESULTS,
        "routes_file": ROUTES_FILE,
        "immutable_required_origins": sorted(REQUIRED_ORIGINS),
        "immutable_required_dests": sorted(REQUIRED_DESTS),
        "last_run": {
            "run_id": rid,
            "started_utc": started_utc,
            "finished_utc": finished_utc,
            "duration_sec": duration_sec,
            "total_calls": total_calls,
            "ok_calls": ok_calls,
            "err_calls": err_calls,
            "success_rate": round(success_rate, 4),
            "offers_saved": offers_saved,
        },
    }
    write_json(STATE_PATH, state)

    # alerts.json (placeholder)
    if not ALERTS_PATH.exists():
        write_json(ALERTS_PATH, {"alerts": [], "generated_utc": finished_utc})

    # run log detalhado
    run_log = {
        "run_id": rid,
        "started_utc": started_utc,
        "finished_utc": finished_utc,
        "duration_sec": duration_sec,
        "store": STORE_NAME,
        "env": AMADEUS_ENV,
        "max_results": MAX_RESULTS,
        "routes_file": ROUTES_FILE,
        "immutable_required_origins": sorted(REQUIRED_ORIGINS),
        "immutable_required_dests": sorted(REQUIRED_DESTS),
        "total_calls": total_calls,
        "ok_calls": ok_calls,
        "err_calls": err_calls,
        "success_rate": success_rate,
        "offers_saved": offers_saved,
        "routes_count": len(routes),
        "top_errors": dict(top_errors),
        "errors": errors,
        "samples": sample_offers,
    }
    write_json(RUNS_DIR / f"{rid}.json", run_log)

    # summary.md
    summary_lines = [
        "# Flight Agent — Update Summary",
        "",
        f"- started_utc: `{started_utc}`",
        f"- finished_utc: `{finished_utc}`",
        f"- duration_sec: `{duration_sec}`",
        f"- total_calls: `{total_calls}`",
        f"- ok_calls: `{ok_calls}`",
        f"- err_calls: `{err_calls}`",
        f"- success_rate: `{success_rate:.3f}`",
        f"- offers_saved: `{offers_saved}`",
        f"- store: `{STORE_NAME}`",
        f"- max_results: `{MAX_RESULTS}`",
        f"- amadeus_env: `{AMADEUS_ENV}`",
        f"- immutable_required_origins: `{sorted(REQUIRED_ORIGINS)}`",
        f"- immutable_required_dests: `{sorted(REQUIRED_DESTS)}`",
        "",
    ]

    if top_errors:
        summary_lines.append("## Top errors")
        for code, n in top_errors.most_common(10):
            summary_lines.append(f"- {code}: {n}")
        summary_lines.append("")

    if sample_offers:
        summary_lines.append("## Sample offers (preview)")
        for s in sample_offers[:3]:
            summary_lines.append(f"- {s}")
        summary_lines.append("")

    write_text(SUMMARY_MD_PATH, "\n".join(summary_lines).rstrip() + "\n")

    # stdout summary
    print("\n" + "\n".join(summary_lines).rstrip())

    # Anti "GREEN mentiroso"
    if err_calls > 0 or ok_calls == 0 or offers_saved == 0:
        print("\n[FAIL] Run completed with issues (green mentiroso guard enabled).")
        print(f"[FAIL] err_calls={err_calls}, ok_calls={ok_calls}, offers_saved={offers_saved}")
        sys.exit(2)

    print("\n[OK] Run completed successfully.")
    sys.exit(0)


if __name__ == "__main__":
    main()
