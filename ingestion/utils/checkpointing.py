from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class FileCheckpointStore:
    """Simple local bookmark store for local development and tests."""

    def __init__(self, path: str = "tmp/checkpoints.json") -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def read(self) -> dict[str, Any]:
        if not self.path.exists():
            return {}
        return json.loads(self.path.read_text())

    def write(self, state: dict[str, Any]) -> None:
        self.path.write_text(json.dumps(state, indent=2, sort_keys=True))

    def set(self, key: str, value: Any) -> None:
        state = self.read()
        state[key] = value
        self.write(state)

    def get(self, key: str, default: Any = None) -> Any:
        return self.read().get(key, default)

