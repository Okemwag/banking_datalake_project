from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lakehouse.runtime import build_spark_session, ensure_databases


def main() -> None:
    spark = build_spark_session("create-tables")
    ensure_databases(spark)
    print("Created or verified Bronze, Silver, Gold, and Ops databases.")


if __name__ == "__main__":
    main()
