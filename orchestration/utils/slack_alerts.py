from __future__ import annotations

import os

import requests


def send_slack_alert(message: str) -> None:
    webhook = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook:
        return
    requests.post(webhook, json={"text": message}, timeout=10).raise_for_status()

