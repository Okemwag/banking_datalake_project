from __future__ import annotations

import requests


def post_lineage_event(base_url: str, payload: dict[str, object]) -> None:
    response = requests.post(base_url.rstrip("/") + "/api/v1/lineage", json=payload, timeout=15)
    response.raise_for_status()

