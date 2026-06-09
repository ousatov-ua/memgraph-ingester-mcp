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
    try:
        return float(value)
    except ValueError:
        raise ValueError(
            f"Environment variable {name!r} has an invalid float value: {value!r}."
        ) from None


def _int_env(name: str, default: int) -> int:
    value = getenv(name)
    if value is None or value == "":
        return default
    try:
        return int(value)
    except ValueError:
        raise ValueError(
            f"Environment variable {name!r} has an invalid integer value: {value!r}."
        ) from None


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
    code_embedding_index_name: str = "code_chunk_embedding_v2"
    memory_embedding_index_name: str = "memory_chunk_embedding_v2"
    embedding_model_name: str = "default"
    embedding_dimensions: int = 384
    compression_enabled: bool = False
    compression_provider: str = "none"
    compression_model_name: str = ""
    compression_device: str = "cpu"
    compression_rate: float = 0.5
    compression_min_chars: int = 800
    compression_fail_open: bool = True

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
            code_embedding_index_name=getenv(
                "MEMGRAPH_INGESTER_MCP_CODE_EMBEDDING_INDEX",
                cls.code_embedding_index_name,
            ),
            memory_embedding_index_name=getenv(
                "MEMGRAPH_INGESTER_MCP_MEMORY_EMBEDDING_INDEX",
                cls.memory_embedding_index_name,
            ),
            embedding_model_name=getenv(
                "MEMGRAPH_INGESTER_MCP_EMBEDDING_MODEL",
                cls.embedding_model_name,
            ),
            embedding_dimensions=_int_env(
                "MEMGRAPH_INGESTER_MCP_EMBEDDING_DIMENSIONS",
                cls.embedding_dimensions,
            ),
            compression_enabled=_bool_env(
                "MEMGRAPH_INGESTER_MCP_COMPRESSION_ENABLED",
                cls.compression_enabled,
            ),
            compression_provider=getenv(
                "MEMGRAPH_INGESTER_MCP_COMPRESSION_PROVIDER",
                cls.compression_provider,
            ),
            compression_model_name=getenv(
                "MEMGRAPH_INGESTER_MCP_COMPRESSION_MODEL",
                cls.compression_model_name,
            ),
            compression_device=getenv(
                "MEMGRAPH_INGESTER_MCP_COMPRESSION_DEVICE",
                cls.compression_device,
            ),
            compression_rate=_float_env(
                "MEMGRAPH_INGESTER_MCP_COMPRESSION_RATE",
                cls.compression_rate,
            ),
            compression_min_chars=_int_env(
                "MEMGRAPH_INGESTER_MCP_COMPRESSION_MIN_CHARS",
                cls.compression_min_chars,
            ),
            compression_fail_open=_bool_env(
                "MEMGRAPH_INGESTER_MCP_COMPRESSION_FAIL_OPEN",
                cls.compression_fail_open,
            ),
        )
