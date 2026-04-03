#!/usr/bin/env bash
set -euo pipefail

cp -n .env.example .env || true
make dev
python3 scripts/create_tables.py

