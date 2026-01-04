# cleanup_state.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Set

STATE_PATH = Path("data/state.json")
HISTORY_PATH = Path("data/history.jsonl")


def _last_history_keys() -> Set[str]:
    if not HISTORY_PATH.exists():
        return set()

    lines = HISTORY_PATH.read_text(encoding="utf-8").splitlines()
    # pega a última linha válida (último run)
    for line in reversed(lines):
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
            results = rec.get("results", []) or []
            keys = {str(r.get("key")) for r in results if r.get("key")}
            return keys
        except Exception:
            continue
    return set()


def main() -> int:
    if not STATE_PATH.exists():
        print("state.json not found, nothing to clean.")
        return 0

    state: Dict[str, Any] = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    best = state.get("best", {})

    if not isinstance(best, dict):
        print("state.json has no valid 'best' map.")
        return 0

    keep_keys = _last_history_keys()
    if not keep_keys:
        print("No keys found in history.jsonl last run; nothing to clean.")
        return 0

    original_keys = set(best.keys())
    cleaned_best = {k: v for k, v in best.items() if k in keep_keys}
    removed = original_keys - set(cleaned_best.keys())

    state["best"] = cleaned_best
    STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

    print("Cleanup completed.")
    print(f"Kept {len(cleaned_best)} entries from last run.")
    if removed:
        print("Removed keys:")
        for k in sorted(removed):
            print(f" - {k}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
