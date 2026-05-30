"""Thin Memgraph Bolt client."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime
from typing import Any

from neo4j import GraphDatabase, Query
from neo4j.time import Date, DateTime, Duration, Time

from memgraph_ingester_mcp.config import MemgraphConfig


class MemgraphError(RuntimeError):
    """Raised when an MCP operation cannot be executed safely."""


def _to_jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _to_jsonable(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_to_jsonable(item) for item in value]
    if isinstance(value, DateTime | Date | Time | Duration | datetime | date):
        return str(value)
    return value


class MemgraphClient:
    """Execute parameterized Cypher against Memgraph over Bolt."""

    def __init__(self, config: MemgraphConfig, driver: Any | None = None) -> None:
        self.config = config
        if driver is None:
            auth = None
            if config.username is not None:
                auth = (config.username, config.password or "")
            driver = GraphDatabase.driver(config.bolt_uri, auth=auth)
        self._driver = driver

    def close(self) -> None:
        close = getattr(self._driver, "close", None)
        if close is not None:
            close()

    def run(
        self,
        query: str,
        parameters: Mapping[str, Any] | None = None,
        *,
        write: bool = False,
    ) -> list[dict[str, Any]]:
        if write and self.config.read_only:
            raise MemgraphError("Write tools are disabled by MEMGRAPH_INGESTER_MCP_READ_ONLY.")

        session_kwargs: dict[str, Any] = {}
        if self.config.database is not None:
            session_kwargs["database"] = self.config.database

        with self._driver.session(**session_kwargs) as session:
            result = session.run(
                Query(query, timeout=self.config.query_timeout_seconds),
                dict(parameters or {}),
            )
            return [_to_jsonable(dict(record)) for record in result]
