from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class LocalSchemaRegistry:
    """Minimal registry wrapper for local Avro schema files."""

    def __init__(self, schema_dir: str = "ingestion/kafka/schemas") -> None:
        self.schema_dir = Path(schema_dir)

    def load(self, schema_name: str) -> dict[str, Any]:
        path = self.schema_dir / schema_name
        return json.loads(path.read_text())

