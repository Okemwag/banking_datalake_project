from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lakehouse.gold.mart_builder import build_gold_marts
from lakehouse.silver.processor import process_transactions_silver


def main() -> None:
    run_id = "manual-reprocess"
    process_transactions_silver(run_id=run_id)
    build_gold_marts(run_id=run_id)
    print({"status": "reprocessed", "run_id": run_id})


if __name__ == "__main__":
    main()
