"""Compatibility no-op response compression hooks."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from memgraph_ingester_mcp.config import MemgraphConfig


@dataclass(frozen=True)
class TextCompression:
    value: str
    origin_tokens: int | None = None
    compressed_tokens: int | None = None
    ratio: str | None = None
    rate: str | None = None


class ResponseCompressor:
    """Keep the compression interface stable while returning original responses."""

    def __init__(
        self,
        config: MemgraphConfig,
        *,
        factory: Callable[[], Any] | None = None,
    ) -> None:
        self.config = config
        self._factory = factory

    def compress_response(self, response: dict[str, Any]) -> dict[str, Any]:
        return response

    def _compress_value(
        self,
        value: Any,
        path: tuple[str | int, ...],
        meta: dict[str, Any],
    ) -> None:
        return None

    def _is_compressible_field(self, key: str, value: Any) -> bool:
        return False

    def _compress_text(self, text: str) -> TextCompression:
        return TextCompression(value=text)

    @staticmethod
    def _add_token_stats(meta: dict[str, Any], compressed: TextCompression) -> None:
        return None
