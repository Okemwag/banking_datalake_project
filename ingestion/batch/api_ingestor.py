from __future__ import annotations

from typing import Any

import requests


def paginate_json_api(base_url: str, page_param: str = "page", start_page: int = 1) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    page = start_page
    while True:
        response = requests.get(base_url, params={page_param: page}, timeout=30)
        response.raise_for_status()
        payload = response.json()
        page_records = payload.get("data", [])
        if not page_records:
            break
        records.extend(page_records)
        page += 1
    return records

