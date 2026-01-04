# cleanup_state.py
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Dict, Any

STATE_PATH = Path("data/state.json")

# Mantemos SOMENTE Roma (FCO ou CIA) com return limit (RL2026-10-05)
KEEP_REGEX = re.compile(
    r"^GRU-(FCO|CIA)-.*-RL2026-10-05-.*$"
)


def main() -> int:
    if not STATE_PATH.exists():
        print("state.json not found, nothing to clean.")
        return 0

    state: Dict[str, Any] = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    best = state.get("best", {})

    if not isinstance(best, dict):
        print("state.json has no valid 'best' map.")
        return 0

    original_keys = set(best.keys())

    cleaned_best = {
        k: v for k, v in best.items()
        if KEEP_REGEX.match(k)
    }

    removed = original_keys - set(cleaned_best.keys())

    state["best"] = cleaned_best
    STATE_PATH.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    print("Cleanup completed.")
    print(f"Kept {len(cleaned_best)} entries.")
    if removed:
        print("Removed keys:")
        for k in sorted(removed):
            print(f" - {k}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
