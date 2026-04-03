from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class JdbcExtractionConfig:
    jdbc_url: str
    query: str
    username: str
    password: str
    driver: str = "org.postgresql.Driver"


def build_jdbc_reader_options(config: JdbcExtractionConfig) -> dict[str, str]:
    return {
        "url": config.jdbc_url,
        "query": config.query,
        "user": config.username,
        "password": config.password,
        "driver": config.driver,
    }

