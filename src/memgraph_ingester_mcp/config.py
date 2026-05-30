"""Runtime configuration for the MCP server."""

from __future__ import annotations

from dataclasses import dataclass
from os import getenv


def _optional_env(name: str) -> str | None:
    value = getenv(name)
    if value is None or value == "":
        return None
    return value


def _bool_env(name: str, default: bool) -> bool:
    value = getenv(name)
    if value is None or value == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _float_env(name: str, default: float) -> float:
    value = getenv(name)
    if value is None or value == "":
        return default
    return float(value)


def _int_env(name: str, default: int) -> int:
    value = getenv(name)
    if value is None or value == "":
        return default
    return int(value)


@dataclass(frozen=True)
class MemgraphConfig:
    """Namespaced Memgraph settings used by the stdio MCP process."""

    bolt_uri: str = "bolt://127.0.0.1:7687"
    username: str | None = None
    password: str | None = None
    database: str | None = None
    default_project: str | None = None
    query_timeout_seconds: float = 30.0
    read_only: bool = False
    embedding_model_name: str = "default"
    embedding_dimensions: int = 384

    @classmethod
    def from_environment(cls) -> MemgraphConfig:
        """Load only MCP-specific environment variables to avoid generic name collisions."""

        return cls(
            bolt_uri=getenv("MEMGRAPH_INGESTER_MCP_BOLT_URI", cls.bolt_uri),
            username=_optional_env("MEMGRAPH_INGESTER_MCP_USERNAME"),
            password=_optional_env("MEMGRAPH_INGESTER_MCP_PASSWORD"),
            database=_optional_env("MEMGRAPH_INGESTER_MCP_DATABASE"),
            default_project=_optional_env("MEMGRAPH_INGESTER_MCP_PROJECT"),
            query_timeout_seconds=_float_env(
                "MEMGRAPH_INGESTER_MCP_QUERY_TIMEOUT_SECONDS",
                cls.query_timeout_seconds,
            ),
            read_only=_bool_env("MEMGRAPH_INGESTER_MCP_READ_ONLY", cls.read_only),
            embedding_model_name=getenv(
                "MEMGRAPH_INGESTER_MCP_EMBEDDING_MODEL",
                cls.embedding_model_name,
            ),
            embedding_dimensions=_int_env(
                "MEMGRAPH_INGESTER_MCP_EMBEDDING_DIMENSIONS",
                cls.embedding_dimensions,
            ),
        )
