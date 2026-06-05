"""Optional lossy response compression for bulky MCP text fields."""

from __future__ import annotations

from collections.abc import Callable, MutableMapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from memgraph_ingester_mcp.config import MemgraphConfig
from memgraph_ingester_mcp.db import MemgraphError

COMPRESSIBLE_TEXT_FIELDS = frozenset(
    {
        "answer",
        "consequences",
        "content",
        "context",
        "decision",
        "description",
        "evidence",
        "notes",
        "rationale",
        "source",
        "summary",
        "text",
        "why",
        "mitigation",
    }
)

FORCE_TOKENS = [
    "\n",
    ".",
    ",",
    ":",
    ";",
    "(",
    ")",
    "[",
    "]",
    "{",
    "}",
    "/",
    "\\",
    "_",
    "-",
    "#",
    "@",
    "$",
]


@dataclass(frozen=True)
class TextCompression:
    value: str
    origin_tokens: int | None = None
    compressed_tokens: int | None = None
    ratio: str | None = None
    rate: str | None = None


def _path_text(path: Sequence[str | int]) -> str:
    rendered = ""
    for part in path:
        if isinstance(part, int):
            rendered += f"[{part}]"
        else:
            rendered += f".{part}" if rendered else part
    return rendered


class ResponseCompressor:
    """Lazily apply LLMLingua only to long, free-text response fields."""

    def __init__(
        self,
        config: MemgraphConfig,
        *,
        factory: Callable[[], Any] | None = None,
    ) -> None:
        self.config = config
        self._factory = factory
        self._compressor: Any | None = None

    def compress_response(self, response: dict[str, Any]) -> dict[str, Any]:
        if not self.config.compression_enabled:
            return response

        original_response = response
        response = deepcopy(response)
        meta = response.setdefault("meta", {})
        if not isinstance(meta, MutableMapping):
            meta = {"originalMeta": meta}
            response["meta"] = meta

        compression_meta: dict[str, Any] = {
            "enabled": True,
            "provider": self.config.compression_provider,
            "algorithm": "llmlingua2",
            "model": self.config.compression_model_name,
            "lossy": True,
            "minChars": self.config.compression_min_chars,
            "rate": self.config.compression_rate,
            "compressedPaths": [],
        }

        if self.config.compression_provider != "llmlingua":
            compression_meta["status"] = "disabled"
            compression_meta["reason"] = (
                f"Unsupported provider {self.config.compression_provider!r}."
            )
            meta["compression"] = compression_meta
            return response

        try:
            self._compress_value(response, (), compression_meta)
        except Exception as exc:
            if self.config.compression_fail_open:
                original_response = dict(original_response)
                original_meta = original_response.setdefault("meta", {})
                if not isinstance(original_meta, MutableMapping):
                    original_meta = {"originalMeta": original_meta}
                    original_response["meta"] = original_meta
                compression_meta["status"] = "failed"
                compression_meta["error"] = str(exc)
                original_meta["compression"] = compression_meta
                return original_response
            raise MemgraphError(f"LLMLingua response compression failed: {exc}") from exc

        compression_meta["status"] = (
            "applied" if compression_meta["compressedPaths"] else "no_eligible_fields"
        )
        meta["compression"] = compression_meta
        return response

    def _compress_value(
        self,
        value: Any,
        path: tuple[str | int, ...],
        meta: dict[str, Any],
    ) -> None:
        if isinstance(value, MutableMapping):
            for key, item in list(value.items()):
                key_text = str(key)
                next_path = (*path, key_text)
                if self._is_compressible_field(key_text, item):
                    compressed = self._compress_text(item)
                    if compressed.value != item:
                        value[key] = compressed.value
                        meta["compressedPaths"].append(_path_text(next_path))
                        self._add_token_stats(meta, compressed)
                    continue
                self._compress_value(item, next_path, meta)
            return

        if isinstance(value, list):
            for index, item in enumerate(value):
                self._compress_value(item, (*path, index), meta)

    def _is_compressible_field(self, key: str, value: Any) -> bool:
        return (
            key in COMPRESSIBLE_TEXT_FIELDS
            and isinstance(value, str)
            and len(value) >= self.config.compression_min_chars
        )

    def _compress_text(self, text: str) -> TextCompression:
        compressor = self._llmlingua()
        result = compressor.compress_prompt(
            text,
            rate=self.config.compression_rate,
            force_tokens=FORCE_TOKENS,
        )
        compressed = str(result.get("compressed_prompt", text)).strip()
        if not compressed or len(compressed) >= len(text):
            return TextCompression(value=text)
        return TextCompression(
            value=compressed,
            origin_tokens=_optional_int(result.get("origin_tokens")),
            compressed_tokens=_optional_int(result.get("compressed_tokens")),
            ratio=_optional_str(result.get("ratio")),
            rate=_optional_str(result.get("rate")),
        )

    def _llmlingua(self) -> Any:
        if self._compressor is not None:
            return self._compressor

        if self._factory is not None:
            self._compressor = self._factory()
            return self._compressor

        try:
            from llmlingua import PromptCompressor
        except ImportError as exc:  # pragma: no cover - exercised without optional extra.
            raise MemgraphError(
                "LLMLingua compression is enabled but the 'llmlingua' package is not installed. "
                "Install memgraph-ingester-mcp[compression] or disable "
                "MEMGRAPH_INGESTER_MCP_COMPRESSION_ENABLED."
            ) from exc

        self._compressor = PromptCompressor(
            model_name=self.config.compression_model_name,
            device_map=self.config.compression_device,
            use_llmlingua2=True,
        )
        return self._compressor

    @staticmethod
    def _add_token_stats(meta: dict[str, Any], compressed: TextCompression) -> None:
        fields = {
            "originTokens": compressed.origin_tokens,
            "compressedTokens": compressed.compressed_tokens,
            "ratio": compressed.ratio,
            "actualRate": compressed.rate,
        }
        stats = meta.setdefault("stats", {})
        if not isinstance(stats, MutableMapping):
            return
        for key, value in fields.items():
            if value is None:
                continue
            if key in {"originTokens", "compressedTokens"}:
                stats[key] = int(stats.get(key, 0)) + int(value)
            else:
                stats.setdefault(key, value)


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _optional_str(value: Any) -> str | None:
    return str(value) if value is not None else None
