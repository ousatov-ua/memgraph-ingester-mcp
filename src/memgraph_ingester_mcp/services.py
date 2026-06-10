"""High-level Memgraph Ingester operations exposed as MCP tools."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from hashlib import sha256
from typing import Any

from memgraph_ingester_mcp import queries as Q
from memgraph_ingester_mcp.code_context import CodeContextMixin
from memgraph_ingester_mcp.compression import ResponseCompressor
from memgraph_ingester_mcp.config import MemgraphConfig
from memgraph_ingester_mcp.db import MemgraphClient, MemgraphError
from memgraph_ingester_mcp.schema import (
    MEMORY_SPECS,
    READ_ONLY_PREFIXES,
    TARGET_TYPES,
    WRITE_KEYWORDS,
)

TOKEN_RE = re.compile(r"\b[A-Za-z_][A-Za-z0-9_]*\b")
STRING_RE = re.compile(r"'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\"")
CAMEL_BOUNDARY_RE = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|[^A-Za-z0-9]+")
CYPHER_IDENTIFIER_RE = re.compile(r"^[A-Za-z_]\w*$")
RESOURCE_SCAN_EXTENSIONS = (
    ".cypher",
    ".sql",
    ".graphql",
    ".gql",
    ".yaml",
    ".yml",
    ".json",
    ".toml",
    ".xml",
    ".properties",
)
PROJECT_TOKEN_SLUG_LIMIT = 48
PROJECT_TOKEN_HASH_LENGTH = 12
UNBOUNDED_GRAPH_TRAVERSAL_RE = re.compile(r"\[[^\]]*\*\s*\d*\.\.[^\d\]]*\]")
ROOT_MATCH_RE = re.compile(r"\b(?:OPTIONAL\s+)?MATCH\s*\([^)]*\{[^}]+}[^)]*\)", re.IGNORECASE)
SQL_WRITE_WITHOUT_WHERE_RE = re.compile(
    r"\b(?:DELETE\s+FROM|UPDATE)\b(?:(?!\bWHERE\b).)*$",
    re.IGNORECASE,
)

CONTROLLED_VALUES: dict[tuple[str, str], frozenset[str]] = {
    ("Rule", "severity"): frozenset({"hard", "soft", "recommendation"}),
    ("Finding", "type"): frozenset({"bug", "perf", "constraint", "security"}),
    ("Finding", "status"): frozenset({"open", "resolved", "obsolete"}),
    ("Task", "priority"): frozenset({"0", "1", "2", "3", "4"}),
    ("Task", "status"): frozenset({"todo", "doing", "done", "blocked", "cancelled"}),
    ("Risk", "severity"): frozenset({"low", "medium", "high", "critical"}),
    ("Risk", "status"): frozenset({"open", "mitigated", "accepted", "obsolete"}),
    ("Question", "status"): frozenset({"open", "answered", "obsolete"}),
    ("Decision", "status"): frozenset({"proposed", "accepted", "rejected", "superseded"}),
    ("ADR", "status"): frozenset({"draft", "proposed", "accepted", "rejected", "superseded"}),
    ("Idea", "status"): frozenset({"proposed", "accepted", "rejected", "obsolete"}),
}

OUTPUT_FORMATS = frozenset({"json", "table_json"})
HOT_PATH_SECTIONS = frozenset({"largestTypes", "longestMethods", "fanIn", "fanOut"})
DEFAULT_RAG_ROLES = ("primary", "file")
AUTO_QUERY_STOPWORDS = frozenset(
    {
        "all",
        "any",
        "are",
        "can",
        "class",
        "does",
        "file",
        "files",
        "from",
        "get",
        "has",
        "how",
        "not",
        "last",
        "line",
        "method",
        "node",
        "nodes",
        "path",
        "project",
        "set",
        "source",
        "state",
        "test",
        "tests",
        "that",
        "the",
        "this",
        "time",
        "type",
        "value",
        "what",
        "when",
        "where",
        "which",
        "with",
    }
)
DISCOVERY_LIMIT = 5
LOOKUP_LIMIT = 10
CALL_GRAPH_LIMIT = 10
MEMBER_LIMIT = 25
RRF_K = 60
# Down-weight the lexical leg so file-role chunks with large Words vocabularies don't override
# vector hits for concept queries. 0.4 gives lexical a meaningful boost for exact-term matches
# while keeping vector-only hits competitive.
LEXICAL_RRF_WEIGHT = 0.4
# Mirrors MEMORY_CHUNK_METADATA_PROPERTIES in the ingester's EmbeddingSettings so MCP-side
# MemoryChunk refreshes produce embeddings consistent with ingester-side refreshes.
MEMORY_CHUNK_EXCLUDED_PROPERTIES = (
    "id",
    "project",
    "sourceLabel",
    "sourceId",
    "textHash",
    "embedding",
    "embeddingModel",
    "embeddingDimensions",
    "createdAt",
    "updatedAt",
)
DEFAULT_OPERATION_SINKS = frozenset(
    {
        "batch",
        "commit",
        "delete",
        "execute",
        "flush",
        "insert",
        "query",
        "read",
        "resolve",
        "run",
        "save",
        "update",
        "upsert",
        "write",
    }
)


def _bounded_limit(limit: int, *, default: int, maximum: int) -> int:
    if limit <= 0:
        return default
    return min(limit, maximum)


def _bounded_skip(skip: int) -> int:
    return max(skip, 0)


def _compact_text(value: str | None, limit: int = 600) -> str | None:
    if value is None or len(value) <= limit:
        return value
    return f"{value[:limit].rstrip()}..."


def _bounded_text_limit(limit: int) -> int:
    if limit <= 0:
        return 0
    return min(limit, 2_000)


def _project_vector_index_name(base_index_name: str, project: str) -> str:
    if not CYPHER_IDENTIFIER_RE.fullmatch(base_index_name):
        raise MemgraphError("Embedding vector index base name must be a Cypher identifier.")
    normalized = project.strip()
    if not normalized:
        raise MemgraphError("Project is required for project-scoped vector index lookup.")
    return f"{base_index_name}_{_project_index_token(normalized)}"


def _project_index_token(project: str) -> str:
    slug = _project_index_slug(project)
    digest = sha256(project.encode()).hexdigest()[:PROJECT_TOKEN_HASH_LENGTH]
    return f"p_{slug}_{digest}"


def _project_index_slug(project: str) -> str:
    slug = []
    pending_underscore = False
    for ch in project:
        if ch.isascii() and ch.isalnum():
            if pending_underscore and slug:
                slug.append("_")
            slug.append(ch.lower())
            pending_underscore = False
        elif slug:
            pending_underscore = True
        if len(slug) >= PROJECT_TOKEN_SLUG_LIMIT:
            break
    return "".join(slug) or "project"


def _select_vector_index_name(
    base_index_name: str,
    project: str,
    available_index_names: set[str],
) -> str:
    project_index_name = _project_vector_index_name(base_index_name, project)
    if project_index_name in available_index_names:
        return project_index_name
    if base_index_name in available_index_names:
        return base_index_name
    return project_index_name


def _vector_index_names(rows: Sequence[Mapping[str, Any]]) -> set[str]:
    return {str(row.get("index_name")) for row in rows if row.get("index_name") is not None}


def _first(value: Any) -> Any:
    if isinstance(value, Sequence) and not isinstance(value, str):
        return value[0] if value else None
    return value


def _method_name(signature: str | None) -> str | None:
    if not signature:
        return None
    return signature.rsplit(".", 1)[-1].split("(", 1)[0]


def _compact_owner(owner: str | None, name: str | None) -> str | None:
    if not owner or not name:
        return owner
    if owner == name or owner.endswith(f".{name}") or owner.endswith(f"#{name}"):
        return None
    return owner


def _package_name(owner_fqn: str | None) -> str | None:
    if not owner_fqn or "." not in owner_fqn:
        return None
    return owner_fqn.rsplit(".", 1)[0]


def _is_test_path(path: str | None) -> bool:
    return bool(
        path
        and (
            path.startswith(("src/test/", "test/", "tests/"))
            or "/test/" in path
            or "/tests/" in path
        )
    )


def _bounded_depth(depth: int) -> int:
    if depth <= 1:
        return 1
    return min(depth, 2)


def _normalize_string_list(value: Sequence[str] | str | None) -> list[str]:
    if value is None:
        return []
    raw = value.split(",") if isinstance(value, str) else list(value)
    return [item.strip() for item in raw if item and item.strip()]


def _normalize_lower_list(value: Sequence[str] | str | None) -> list[str]:
    return [item.lower() for item in _normalize_string_list(value)]


def _normalize_extensions(value: Sequence[str] | str | None) -> list[str]:
    raw = _normalize_lower_list(value)
    if not raw:
        return list(RESOURCE_SCAN_EXTENSIONS)
    return [item if item.startswith(".") else f".{item}" for item in raw]


def _bounded_symbol_limit(limit: int) -> int:
    return _bounded_limit(limit, default=8, maximum=50)


def _source_excerpt(value: str | None) -> str:
    if not value:
        return ""
    marker = "Source excerpt:\n"
    if marker not in value:
        return value
    return value.split(marker, 1)[1]


def _resource_risk_rows(path: str, language: str | None, text: str) -> list[dict[str, Any]]:
    excerpt = _source_excerpt(text)
    lines = excerpt.splitlines()
    lowered = excerpt.lower()
    rows: list[dict[str, Any]] = []

    def add(
        *,
        risk: str,
        score: int,
        pattern: str,
        line: int | None,
        evidence: str,
        why: str,
    ) -> None:
        rows.append(
            {
                "path": path,
                "language": language,
                "risk": risk,
                "score": score,
                "pattern": pattern,
                "line": line,
                "evidence": _compact_text(evidence.strip(), 180),
                "why": why,
                "occurrences": 1,
            }
        )

    for index, line in enumerate(lines, start=1):
        stripped = line.strip()
        if UNBOUNDED_GRAPH_TRAVERSAL_RE.search(stripped):
            add(
                risk="high",
                score=90,
                pattern="unbounded-variable-length-traversal",
                line=index,
                evidence=stripped,
                why=(
                    "Variable-length graph traversal has no upper bound; "
                    "cost can grow with hierarchy depth."
                ),
            )
        if " like '%" in stripped.lower():
            add(
                risk="medium",
                score=45,
                pattern="leading-wildcard-like",
                line=index,
                evidence=stripped,
                why="Leading-wildcard LIKE predicates usually cannot use normal indexes.",
            )
        if SQL_WRITE_WITHOUT_WHERE_RE.search(stripped):
            add(
                risk="high",
                score=85,
                pattern="write-without-where",
                line=index,
                evidence=stripped,
                why="UPDATE/DELETE without a WHERE clause can touch every row.",
            )

    unwind_count = lowered.count("unwind ")
    traversal_count = len(UNBOUNDED_GRAPH_TRAVERSAL_RE.findall(excerpt))
    optional_match_count = lowered.count("optional match")
    merge_count = lowered.count("merge ")
    call_block_count = lowered.count("call {")
    root_matches = ROOT_MATCH_RE.findall(excerpt)
    repeated_root_matches = len(root_matches) - len(set(root_matches))

    if unwind_count and traversal_count:
        add(
            risk="high",
            score=95 + min(20, traversal_count * 3),
            pattern="per-row-unbounded-traversal",
            line=None,
            evidence=f"UNWIND x{unwind_count}, unbounded traversals x{traversal_count}",
            why="An UNWIND-driven query can repeat unbounded graph traversals once per input row.",
        )
    if unwind_count and optional_match_count >= 3:
        add(
            risk="medium",
            score=60 + min(20, optional_match_count * 2),
            pattern="per-row-many-optional-matches",
            line=None,
            evidence=f"UNWIND x{unwind_count}, OPTIONAL MATCH x{optional_match_count}",
            why="Many OPTIONAL MATCH clauses under an UNWIND can multiply per-row query work.",
        )
    if unwind_count and merge_count >= 3:
        add(
            risk="medium",
            score=55 + min(20, merge_count * 2),
            pattern="per-row-many-merges",
            line=None,
            evidence=f"UNWIND x{unwind_count}, MERGE x{merge_count}",
            why="Many MERGE operations under an UNWIND can create repeated index lookups/writes.",
        )
    if call_block_count >= 3:
        add(
            risk="medium",
            score=55 + min(25, call_block_count * 3),
            pattern="many-subquery-blocks",
            line=None,
            evidence=f"CALL {{ blocks x{call_block_count}",
            why="Many sequential subquery blocks can repeatedly rematch the same roots.",
        )
    if repeated_root_matches > 0:
        add(
            risk="medium",
            score=50 + min(30, repeated_root_matches * 5),
            pattern="repeated-root-rematch",
            line=None,
            evidence=f"Repeated root MATCH patterns x{repeated_root_matches}",
            why="Repeatedly matching the same keyed root in one resource is often avoidable.",
        )
    if "foreach" in lowered and any(token in lowered for token in (" set ", " remove ", " merge ")):
        add(
            risk="medium",
            score=50,
            pattern="write-inside-foreach",
            line=None,
            evidence="FOREACH with write clauses",
            why=(
                "FOREACH with write clauses. Verify this is not a per-row write loop; "
                "single-item conditional FOREACH (CASE THEN [1] ELSE []) is idiomatic and safe."
            ),
        )

    return rows


def _aggregate_resource_risks(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        key = (row.get("path") or "", row.get("pattern") or "")
        current = grouped.get(key)
        if current is None:
            grouped[key] = dict(row)
            continue
        current["occurrences"] = (current.get("occurrences") or 1) + 1
        current["score"] = max(current.get("score") or 0, row.get("score") or 0) + min(
            15,
            current["occurrences"],
        )
        if current.get("line") is None or (
            row.get("line") is not None and row["line"] < current["line"]
        ):
            current["line"] = row.get("line")
            current["evidence"] = row.get("evidence")
    return list(grouped.values())


def _identifier_terms(value: str | None) -> list[str]:
    if not value:
        return []
    terms = [term.lower() for term in CAMEL_BOUNDARY_RE.split(value) if len(term) >= 3]
    seen: set[str] = set()
    deduped: list[str] = []
    for term in terms:
        if term in seen:
            continue
        seen.add(term)
        deduped.append(term)
    return deduped


def _lexical_query_terms(value: str | None, *, min_length: int = 3) -> list[str]:
    return [
        term
        for term in _identifier_terms(value)
        if len(term) >= min_length and term not in AUTO_QUERY_STOPWORDS
    ]


def _query_variants(query: str) -> list[str]:
    """Return the raw query plus a keyword variant when it adds embedding signal.

    The keyword variant lowercases and camel-splits the query so identifier-style
    queries also match the split-word vocabulary embedded in chunk texts.
    """
    variants = [query]
    keyword_variant = " ".join(_lexical_query_terms(query))
    if keyword_variant and keyword_variant != query.strip().lower():
        variants.append(keyword_variant)
    return variants


def _passes_chunk_filters(
    row: Mapping[str, Any],
    *,
    kind_filter: frozenset[str] | set[str],
    path_prefix_filter: Sequence[str],
    path_contains_filter: str,
    owner_filter: str,
    min_score: float,
) -> bool:
    """Apply code_search post-filters; min_score only gates rows carrying a vector score."""
    if kind_filter and row.get("kind") not in kind_filter:
        return False
    if not _starts_with_any(row.get("path"), path_prefix_filter):
        return False
    if path_contains_filter and path_contains_filter not in (row.get("path") or ""):
        return False
    if owner_filter and not _contains_any(row.get("owner"), [owner_filter]):
        return False
    # Lexical-only rows carry no vector score; drop them when min_score is active.
    return not (min_score > 0 and ("score" not in row or float(row.get("score") or 0) < min_score))


def _dedupe_chunk_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Keep the first (best-ranked) row per (kind, sourceId)."""
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        key = (row.get("kind") or "", row.get("sourceId") or "")
        if key in seen:
            continue
        seen.add(key)
        deduped.append(dict(row))
    return deduped


def _rrf_fuse(
    vector_rows: Sequence[Mapping[str, Any]],
    lexical_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Merge two ranked candidate lists with reciprocal rank fusion.

    Rank-based fusion is immune to the uncalibrated absolute similarity scores; vector
    rows win ties via their score and contribute the row payload, lexical rows attach
    termMatches when they agree on the same chunk.
    """
    fused: dict[tuple[str, str], dict[str, Any]] = {}
    scores: dict[tuple[str, str], float] = {}
    for rank, row in enumerate(vector_rows):
        key = (row.get("kind") or "", row.get("sourceId") or "")
        fused.setdefault(key, dict(row))
        scores[key] = scores.get(key, 0.0) + 1.0 / (RRF_K + rank + 1)
    for rank, row in enumerate(lexical_rows):
        key = (row.get("kind") or "", row.get("sourceId") or "")
        if key in fused:
            if row.get("termMatches") is not None:
                fused[key]["termMatches"] = row.get("termMatches")
        else:
            fused[key] = dict(row)
        scores[key] = scores.get(key, 0.0) + LEXICAL_RRF_WEIGHT / (RRF_K + rank + 1)
    ordered = sorted(
        fused.items(),
        key=lambda item: (
            -scores[item[0]],
            -float(item[1].get("score") or 0.0),
            item[1].get("path") or "",
        ),
    )
    return [row for _key, row in ordered]


def _test_fragment_parts(value: str | None) -> tuple[str, str, list[str], int]:
    fragment = (value or "").strip()
    if "." not in fragment:
        terms = _identifier_terms(fragment)
        return "", fragment, terms, 1 if len(terms) <= 2 else 2

    owner_fragment, method_fragment = fragment.rsplit(".", 1)
    terms = _identifier_terms(method_fragment) or _identifier_terms(fragment)
    method_terms = [term for term in terms if len(term) >= 5]
    if method_terms:
        terms = method_terms
    min_matches = min(3, max(1, len(terms) - 1))
    return owner_fragment, method_fragment, terms, min_matches


def _contains_any(value: str | None, needles: Sequence[str]) -> bool:
    if not needles:
        return True
    haystack = (value or "").lower()
    return any(needle.lower() in haystack for needle in needles)


def _starts_with_any(value: str | None, prefixes: Sequence[str]) -> bool:
    if not prefixes:
        return True
    haystack = value or ""
    return any(haystack.startswith(prefix) for prefix in prefixes)


def _normalize_output_format(output_format: str | None) -> str:
    if output_format is None:
        return "json"
    normalized = output_format.strip()
    if normalized not in OUTPUT_FORMATS:
        allowed = ", ".join(sorted(OUTPUT_FORMATS))
        raise MemgraphError(f"Unsupported format {output_format!r}. Allowed: {allowed}.")
    return normalized


def _strip_nones(obj: Any) -> Any:
    """Recursively remove None-valued keys from dicts. Absent keys signal null/empty to callers."""
    if isinstance(obj, Mapping):
        return {k: _strip_nones(v) for k, v in obj.items() if v is not None}
    if isinstance(obj, list):
        return [_strip_nones(item) for item in obj]
    return obj


def _to_table_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _to_table_json(item) for key, item in value.items()}
    if isinstance(value, list) and value and all(isinstance(item, Mapping) for item in value):
        columns: list[str] = []
        for row in value:
            for key in row:
                column = str(key)
                if column not in columns:
                    columns.append(column)
        # Drop columns that are None in every row — callers should treat absent as null.
        live_columns = [col for col in columns if any(row.get(col) is not None for row in value)]
        return {
            "cols": live_columns,
            "rows": [[_to_table_json(row.get(col)) for col in live_columns] for row in value],
        }
    return value


def _format_response(
    response: dict[str, Any],
    output_format: str | None = "json",
) -> dict[str, Any]:
    normalized = _normalize_output_format(output_format)
    if normalized == "json":
        return _strip_nones(response)

    formatted = _to_table_json(response)
    if not isinstance(formatted, dict):  # pragma: no cover - response is always a dict today.
        raise MemgraphError("Formatted response must be an object.")

    return formatted


def _with_result_meta(
    response: dict[str, Any],
    rows: Sequence[Any],
    *,
    skip: int = 0,
    limit: int | None = None,
    total_count: int | None = None,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    returned_count = len(rows)
    total = returned_count if total_count is None else total_count
    next_skip = skip + returned_count
    has_more = next_skip < total
    meta: dict[str, Any] = {"hasMore": True} if has_more else {}
    if has_more:
        meta["nextSkip"] = next_skip
    if total_count is not None and total > returned_count:
        meta["totalCount"] = total
    if extra:
        meta.update(extra)
    response["meta"] = meta
    return response


def _overfetch_limit(limit_value: int, include_count: bool) -> int:
    return limit_value if include_count else limit_value + 1


def _trim_overfetch(
    rows: list[dict[str, Any]],
    *,
    skip: int,
    limit: int,
    include_count: bool,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    if include_count or len(rows) <= limit:
        return rows, None
    trimmed = rows[:limit]
    return trimmed, {"hasMore": True, "nextSkip": skip + len(trimmed)}


def _normalize_sections(
    sections: Sequence[str] | str | None,
    *,
    allowed: frozenset[str],
    default: frozenset[str],
) -> frozenset[str]:
    if sections is None:
        return default
    raw = sections.split(",") if isinstance(sections, str) else list(sections)
    requested = frozenset(section.strip() for section in raw if section and section.strip())
    unknown = sorted(requested - allowed)
    if unknown:
        raise MemgraphError(
            f"Unknown section(s): {', '.join(unknown)}. Allowed: {', '.join(sorted(allowed))}."
        )
    return requested or default


def _memory_spec(memory_type: str):
    spec = MEMORY_SPECS.get(memory_type)
    if spec is None:
        allowed = ", ".join(sorted(MEMORY_SPECS))
        raise MemgraphError(f"Unsupported memory_type {memory_type!r}. Allowed: {allowed}.")
    return spec


def _memory_schema_entry(memory_type: str) -> dict[str, Any]:
    spec = _memory_spec(memory_type)
    return {
        "label": spec.label,
        "relation": spec.relation,
        "fields": sorted(spec.fields),
        "controlledValues": {
            field: sorted(values)
            for (type_name, field), values in sorted(CONTROLLED_VALUES.items())
            if type_name == memory_type
        },
    }


def _memory_label_predicate(variable: str = "memory") -> str:
    return " OR ".join(f"{variable}:{spec.label}" for spec in MEMORY_SPECS.values())


def _validate_target_type(target_type: str) -> None:
    if target_type not in TARGET_TYPES:
        allowed = ", ".join(sorted(TARGET_TYPES))
        raise MemgraphError(f"Unsupported target_type {target_type!r}. Allowed: {allowed}.")


def _target_match(target_type: str) -> tuple[str, str]:
    _validate_target_type(target_type)
    match target_type:
        case "Code":
            return "Code", "target.language = $target_key"
        case "Package":
            return (
                "Package",
                "(target.name = $target_key OR target.language + ':' + target.name = $target_key)",
            )
        case "File":
            return "File", "target.path = $target_key"
        case "Method":
            return "Method", "target.signature = $target_key"
        case "Field":
            return "Field", "target.fqn = $target_key"
        case "Class" | "Interface" | "Annotation":
            return target_type, "target.fqn = $target_key"
    raise MemgraphError(f"Unsupported target_type {target_type!r}.")


def _strip_strings(query: str) -> str:
    return STRING_RE.sub("''", query)


def _ensure_read_only_query(query: str) -> None:
    stripped = query.strip()
    if not stripped:
        raise MemgraphError("Cypher query cannot be empty.")

    lowered = stripped.lower()
    if not lowered.startswith(READ_ONLY_PREFIXES):
        raise MemgraphError("Only read-oriented Cypher is allowed.")

    tokens = {token.lower() for token in TOKEN_RE.findall(_strip_strings(lowered))}
    used_write_keywords = sorted(tokens & WRITE_KEYWORDS)
    if used_write_keywords:
        joined = ", ".join(used_write_keywords)
        raise MemgraphError(f"Raw read query contains write keyword(s): {joined}.")

    write_procedures = ("embeddings.node_sentence", "node2vec.set_embeddings")
    if any(proc in lowered for proc in write_procedures):
        raise MemgraphError("Writeable procedures are not allowed through raw_read_cypher.")


def _ensure_project_scoped(query: str) -> None:
    lowered = query.lower()
    metadata_query = lowered.startswith("show vector index info") or "call mg.procedures" in lowered
    if metadata_query:
        return
    if "$project" not in query and "project:" not in lowered and "project =" not in lowered:
        raise MemgraphError(
            "Raw read queries must include a project filter such as project: $project."
        )


class MemgraphIngesterTools(CodeContextMixin):
    """Safe operations that cover the generated Memgraph instruction templates."""

    def __init__(self, client: MemgraphClient, config: MemgraphConfig) -> None:
        self.client = client
        self.config = config
        self._response_compressor = ResponseCompressor(config)

    def resolve_project(self, project: str | None) -> str:
        resolved = project or self.config.default_project
        if resolved is None or resolved.strip() == "":
            raise MemgraphError(
                "Project is required. Pass project or set MEMGRAPH_INGESTER_MCP_PROJECT."
            )
        return resolved

    def _finalize_response(
        self,
        response: dict[str, Any],
        output_format: str | None = "json",
    ) -> dict[str, Any]:
        result = _format_response(
            self._response_compressor.compress_response(response),
            output_format,
        )
        result.pop("project", None)
        return result

    def _select_vector_index_name(self, base_index_name: str, project: str) -> str:
        return _select_vector_index_name(
            base_index_name,
            project,
            _vector_index_names(self.client.run("SHOW VECTOR INDEX INFO")),
        )

    def _embedding_text_config(self) -> dict[str, Any]:
        """Config for embeddings.text: pin the model when one is explicitly configured.

        Keeps query embeddings on the same model as document embeddings instead of
        relying implicitly on the module default.
        """
        model_name = (self.config.embedding_model_name or "").strip()
        if not model_name or model_name == "default":
            return {}
        return {"model_name": model_name}

    def _node_sentence_config(self) -> dict[str, Any]:
        """Config for embeddings.node_sentence on MemoryChunk nodes.

        Excludes metadata properties exactly like the ingester does so both refresh
        paths produce embeddings from the same text.
        """
        config = self._embedding_text_config()
        config["embedding_property"] = "embedding"
        config["excluded_properties"] = list(MEMORY_CHUNK_EXCLUDED_PROPERTIES)
        return config

    def server_status(self, project: str | None = None) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        vector_index_rows = self.client.run("SHOW VECTOR INDEX INFO")
        available_index_names = _vector_index_names(vector_index_rows)
        vector_index_names = {
            _select_vector_index_name(
                self.config.code_embedding_index_name,
                project_name,
                available_index_names,
            ),
            _select_vector_index_name(
                self.config.memory_embedding_index_name,
                project_name,
                available_index_names,
            ),
        }
        languages = self.client.run(
            Q.SERVER_STATUS_LANGUAGES,
            {"project": project_name},
        )
        inventory = self.client.run(
            Q.SERVER_STATUS_INVENTORY,
            {"project": project_name},
        )
        memories = self.client.run(
            Q.SERVER_STATUS_MEMORIES,
            {"project": project_name},
        )
        indexes = [row for row in vector_index_rows if row.get("index_name") in vector_index_names]
        return {
            "project": project_name,
            "languages": languages,
            "inventory": inventory[0] if inventory else {},
            "memoryCounts": memories,
            "vectorIndexes": indexes,
        }

    def code_orientation(
        self,
        project: str | None = None,
        limit: int = 30,
        sections: Sequence[str] | str | None = None,
    ) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        bounded_limit = _bounded_limit(limit, default=30, maximum=100)
        allowed_sections = frozenset({"languages", "packages", "largestTypes", "crossOwnerCalls"})
        requested = _normalize_sections(
            sections,
            allowed=allowed_sections,
            default=allowed_sections,
        )
        response: dict[str, Any] = {"project": project_name, "sections": sorted(requested)}
        if "languages" in requested:
            response["languages"] = self.client.run(
                Q.CODE_ORIENTATION_LANGUAGES,
                {"project": project_name},
            )
        if "packages" in requested:
            response["packages"] = self.client.run(
                Q.CODE_ORIENTATION_PACKAGES,
                {"project": project_name, "limit": bounded_limit},
            )
        if "largestTypes" in requested:
            response["largestTypes"] = self.client.run(
                Q.CODE_ORIENTATION_LARGEST_TYPES,
                {"project": project_name, "limit": bounded_limit},
            )
        if "crossOwnerCalls" in requested:
            response["crossOwnerCalls"] = self.client.run(
                Q.CODE_ORIENTATION_CROSS_OWNER_CALLS,
                {"project": project_name, "limit": bounded_limit},
            )
        return response

    def code_search(
        self,
        query: str,
        project: str | None = None,
        limit: int = DISCOVERY_LIMIT,
        include_tests: bool = False,
        include_text: bool = False,
        text_limit: int = 160,
        dedupe_by_source: bool = True,
        kinds: Sequence[str] | str | None = None,
        path_prefixes: Sequence[str] | str | None = None,
        path_contains: str | None = None,
        owner_fragment: str | None = None,
        min_score: float = 0.0,
        include_secondary: bool = False,
        rag_roles: Sequence[str] | str | None = None,
        include_keys: bool = False,
        output_format: str = "json",
    ) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        index_name = self._select_vector_index_name(
            self.config.code_embedding_index_name,
            project_name,
        )
        bounded_limit = _bounded_limit(limit, default=DISCOVERY_LIMIT, maximum=25)
        bounded_text_limit = _bounded_text_limit(text_limit)
        role_filter = _normalize_string_list(rag_roles)
        if not role_filter and not include_secondary:
            role_filter = list(DEFAULT_RAG_ROLES)
        kind_filter = frozenset(_normalize_string_list(kinds))
        path_prefix_filter = _normalize_string_list(path_prefixes)
        path_contains_filter = (path_contains or "").strip()
        owner_filter = (owner_fragment or "").strip()
        filter_active = bool(
            kind_filter
            or path_prefix_filter
            or path_contains_filter
            or owner_filter
            or min_score > 0
        )
        fetch_multiplier = 10 if filter_active else (6 if role_filter else 3)
        fetch_limit = (
            min(bounded_limit * fetch_multiplier, 250) if dedupe_by_source else bounded_limit
        )
        role_projection = "effectiveRole AS ragRole," if include_keys else ""
        return_projection = (
            f"""
                   kind,
                   sourceId,
                   owner,
                   name,
                   path,
                   {role_projection}
                   startLine, endLine,
                   round(similarity * 10000) / 10000 AS score,
                   chunk.text AS text
            """
            if include_text
            else f"""
                   kind,
                   sourceId,
                   owner,
                   name,
                   path,
                   {role_projection}
                   startLine, endLine,
                   round(similarity * 10000) / 10000 AS score
            """
        )
        search_query = Q.CODE_SEARCH.replace("__RETURN_PROJECTION__", return_projection.strip())
        variants = _query_variants(query)
        lexical_terms = _lexical_query_terms(query) if dedupe_by_source else []
        vector_unavailable = False
        raw_rows: list[dict[str, Any]] = []
        try:
            raw_rows = self.client.run(
                search_query,
                {
                    "index": index_name,
                    "project": project_name,
                    "queries": variants,
                    "embed_config": self._embedding_text_config(),
                    "limit": fetch_limit,
                    "include_tests": include_tests,
                    "rag_roles": role_filter,
                    "kinds": list(kind_filter),
                    "path_prefixes": path_prefix_filter,
                    "path_contains": path_contains_filter,
                    "owner_fragment": owner_filter,
                    "min_score": min_score,
                },
            )
        except MemgraphError:
            if not lexical_terms:
                raise
            vector_unavailable = True
        raw_lexical_rows: list[dict[str, Any]] = []
        if lexical_terms:
            raw_lexical_rows = self._lexical_chunk_rows(
                project_name,
                required_terms=[],
                optional_terms=lexical_terms,
                limit=fetch_limit,
                include_tests=include_tests,
                include_text=include_text,
                kind_filter=list(kind_filter),
                role_filter=role_filter,
                path_contains_filter=path_contains_filter,
            )

        def passes(row: Mapping[str, Any]) -> bool:
            return _passes_chunk_filters(
                row,
                kind_filter=kind_filter,
                path_prefix_filter=path_prefix_filter,
                path_contains_filter=path_contains_filter,
                owner_filter=owner_filter,
                min_score=min_score,
            )

        filtered_vector = [row for row in raw_rows if passes(row)]
        if dedupe_by_source:
            rows = _rrf_fuse(
                _dedupe_chunk_rows(filtered_vector),
                _dedupe_chunk_rows([row for row in raw_lexical_rows if passes(row)]),
            )[:bounded_limit]
        else:
            rows = filtered_vector[:bounded_limit]
        for row in rows:
            if include_text:
                row["text"] = _compact_text(row.get("text"), bounded_text_limit)
            else:
                row.pop("text", None)
            if not include_keys:
                row.pop("sourceId", None)
                row.pop("ragRole", None)
            row["owner"] = _compact_owner(row.get("owner"), row.get("name"))
        saturated = len(raw_rows) >= fetch_limit
        extra: dict[str, Any] = {}
        if saturated:
            extra["candidateLimitReached"] = True
        if vector_unavailable:
            extra["vectorUnavailable"] = True
        return self._finalize_response(
            _with_result_meta(
                {"project": project_name, "hits": rows},
                rows,
                limit=bounded_limit,
                extra=extra or None,
            ),
            output_format,
        )

    def _lexical_chunk_rows(
        self,
        project_name: str,
        *,
        required_terms: Sequence[str],
        optional_terms: Sequence[str],
        limit: int,
        include_tests: bool,
        include_text: bool,
        kind_filter: Sequence[str],
        role_filter: Sequence[str],
        path_contains_filter: str,
    ) -> list[dict[str, Any]]:
        """Run the shared lexical chunk query; rows keep sourceId/ragRole for callers."""
        search_terms = list(required_terms) + list(optional_terms)
        text_projection = ", chunk.text AS text" if include_text else ""
        return self.client.run(
            Q.LEXICAL_CHUNK_ROWS.replace("__TEXT_PROJECTION__", text_projection),
            {
                "project": project_name,
                "all_terms": list(required_terms),
                "any_terms": list(optional_terms),
                "search_terms": search_terms,
                "kinds": list(kind_filter),
                "rag_roles": list(role_filter),
                "path_contains": path_contains_filter,
                "include_tests": include_tests,
                "limit": limit,
            },
        )

    def code_text_search(
        self,
        query: str | None = None,
        project: str | None = None,
        all_terms: Sequence[str] | str | None = None,
        any_terms: Sequence[str] | str | None = None,
        limit: int = DISCOVERY_LIMIT,
        include_tests: bool = False,
        include_text: bool = False,
        text_limit: int = 160,
        kinds: Sequence[str] | str | None = None,
        include_secondary: bool = False,
        rag_roles: Sequence[str] | str | None = None,
        path_contains: str | None = None,
        output_format: str = "json",
    ) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        bounded_limit = _bounded_limit(limit, default=DISCOVERY_LIMIT, maximum=50)
        bounded_text_limit = _bounded_text_limit(text_limit)
        required_terms = _normalize_lower_list(all_terms)
        optional_terms = _normalize_lower_list(any_terms)
        if query and not required_terms and not optional_terms:
            optional_terms = _lexical_query_terms(query)
        if not required_terms and not optional_terms:
            raise MemgraphError("Provide query, all_terms, or any_terms.")
        kind_filter = _normalize_string_list(kinds)
        role_filter = _normalize_string_list(rag_roles)
        if not role_filter and not include_secondary:
            role_filter = list(DEFAULT_RAG_ROLES)
        path_contains_filter = (path_contains or "").strip()
        rows = self._lexical_chunk_rows(
            project_name,
            required_terms=required_terms,
            optional_terms=optional_terms,
            limit=bounded_limit,
            include_tests=include_tests,
            include_text=include_text,
            kind_filter=kind_filter,
            role_filter=role_filter,
            path_contains_filter=path_contains_filter,
        )
        for row in rows:
            if include_text:
                row["text"] = _compact_text(row.get("text"), bounded_text_limit)
            else:
                row.pop("text", None)
            row.pop("ragRole", None)
            row.pop("sourceId", None)
            row["owner"] = _compact_owner(row.get("owner"), row.get("name"))
        return self._finalize_response(
            _with_result_meta(
                {"project": project_name, "hits": rows},
                rows,
                limit=bounded_limit,
            ),
            output_format,
        )

    def code_discovery_context(
        self,
        query: str,
        project: str | None = None,
        limit: int = 3,
        include_tests: bool = False,
        neighbor_limit: int = 3,
        output_format: str = "json",
    ) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        bounded_limit = _bounded_limit(limit, default=3, maximum=8)
        bounded_neighbor_limit = _bounded_limit(neighbor_limit, default=3, maximum=10)
        search = self.code_search(
            query=query,
            project=project_name,
            limit=bounded_limit,
            include_tests=include_tests,
            include_text=False,
            include_keys=True,
            output_format="json",
        )
        anchors = search["hits"]
        contexts: list[dict[str, Any]] = []
        for anchor in anchors[:bounded_limit]:
            kind = anchor.get("kind")
            source_id = anchor.get("sourceId")
            context: dict[str, Any] = {"anchor": anchor}
            if kind == "Method" and source_id:
                method_context = self.code_method_context(
                    source_id,
                    project_name,
                    method_limit=1,
                    neighbor_limit=bounded_neighbor_limit,
                    include_tests=include_tests,
                    compact=True,
                    output_format="json",
                )
                context["methods"] = method_context.get("methods", [])
                context["callers"] = method_context.get("callers", [])
                context["callees"] = method_context.get("callees", [])
            elif kind in {"Class", "Interface", "Annotation"} and source_id:
                type_context = self.code_lookup_type(
                    project=project_name,
                    fqn=source_id,
                    include_tests=include_tests,
                    include_members=False,
                    member_summary=True,
                    limit=1,
                    compact=True,
                    output_format="json",
                )
                context["types"] = type_context.get("types", [])
            elif anchor.get("path"):
                file_context = self.code_lookup_file(
                    anchor["path"],
                    project_name,
                    limit=1,
                    include_tests=include_tests,
                    compact=True,
                    output_format="json",
                )
                context["files"] = file_context.get("files", [])
            contexts.append(context)
        return self._finalize_response(
            _with_result_meta(
                {
                    "project": project_name,
                    "contexts": contexts,
                },
                contexts,
                limit=bounded_limit,
            ),
            output_format,
        )

    def code_lookup_type(
        self,
        project: str | None = None,
        type_name: str | None = None,
        fqn: str | None = None,
        include_members: bool = False,
        include_tests: bool = False,
        member_limit: int = MEMBER_LIMIT,
        member_summary: bool = False,
        limit: int = LOOKUP_LIMIT,
        include_count: bool = False,
        compact: bool = True,
        output_format: str = "json",
    ) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        if not type_name and not fqn:
            raise MemgraphError("Provide either type_name or fqn.")
        bounded_limit = _bounded_limit(limit, default=LOOKUP_LIMIT, maximum=100)
        bounded_member_limit = _bounded_limit(member_limit, default=MEMBER_LIMIT, maximum=200)
        predicate = "t.fqn = $fqn" if fqn else "t.name = $type_name"
        member_count_cypher = (
            """
            OPTIONAL MATCH (t)-[:DECLARES]->(m_cnt:Method {project: $project})
            WITH t, files, count(m_cnt) AS methodCount
            OPTIONAL MATCH (t)-[:DECLARES]->(f_cnt:Field {project: $project})
            WITH t, files, methodCount, count(f_cnt) AS fieldCount
            """
            if (member_summary and not include_members)
            else ""
        )
        member_count_cols = (
            ", methodCount, fieldCount" if (member_summary and not include_members) else ""
        )
        extra_type_cols = (
            ""
            if compact
            else (
                "t.visibility AS visibility, t.isExternal AS isExternal, "
                "t.language AS language, t.framework AS framework, "
                "t.modulePath AS modulePath, "
            )
        )
        types = self.client.run(
            Q.CODE_LOOKUP_TYPE
            .replace("__PREDICATE__", predicate)
            .replace("__MEMBER_COUNT_CYPHER__", member_count_cypher)
            .replace("__EXTRA_TYPE_COLS__", extra_type_cols)
            .replace("__MEMBER_COUNT_COLS__", member_count_cols),
            {
                "project": project_name,
                "type_name": type_name,
                "fqn": fqn,
                "limit": _overfetch_limit(bounded_limit, include_count),
                "include_tests": include_tests,
            },
        )
        types, page_extra = _trim_overfetch(
            types,
            skip=0,
            limit=bounded_limit,
            include_count=include_count,
        )
        total_count = None
        if include_count:
            count_rows = self.client.run(
                Q.CODE_LOOKUP_TYPE_COUNT.replace("__PREDICATE__", predicate),
                {
                    "project": project_name,
                    "type_name": type_name,
                    "fqn": fqn,
                    "include_tests": include_tests,
                },
            )
            total_count = count_rows[0].get("count", 0) if count_rows else len(types)
        if member_summary and not include_members:
            for item in types:
                item["memberCounts"] = {
                    "methods": item.pop("methodCount", 0),
                    "fields": item.pop("fieldCount", 0),
                }
        if include_members:
            for item in types:
                item_fqn = item.get("fqn")
                if not item_fqn:
                    continue
                method_projection = (
                    """
                    m.signature AS signature, m.name AS name, m.startLine AS startLine,
                    m.endLine AS endLine
                    """
                    if compact
                    else """
                    m.signature AS signature, m.name AS name, m.startLine AS startLine,
                    m.endLine AS endLine, m.returnType AS returnType,
                    m.visibility AS visibility, m.isStatic AS isStatic,
                    m.isSynthetic AS isSynthetic
                    """
                )
                item["methods"] = self.client.run(
                    Q.CODE_LOOKUP_TYPE_MEMBERS_METHODS.replace(
                        "__METHOD_PROJECTION__", method_projection.strip()
                    ),
                    {"project": project_name, "fqn": item_fqn, "limit": bounded_member_limit},
                )
                field_projection = (
                    "field.fqn AS fqn, field.name AS name"
                    if compact
                    else """
                    field.fqn AS fqn, field.name AS name, field.type AS type,
                    field.visibility AS visibility, field.isStatic AS isStatic,
                    field.kind AS kind
                    """
                )
                item["fields"] = self.client.run(
                    Q.CODE_LOOKUP_TYPE_MEMBERS_FIELDS.replace(
                        "__FIELD_PROJECTION__", field_projection.strip()
                    ),
                    {"project": project_name, "fqn": item_fqn, "limit": bounded_member_limit},
                )
        return self._finalize_response(
            _with_result_meta(
                {"project": project_name, "types": types},
                types,
                limit=bounded_limit,
                total_count=total_count,
                extra=page_extra,
            ),
            output_format,
        )

    def code_lookup_methods(
        self,
        signature_fragment: str,
        project: str | None = None,
        skip: int = 0,
        limit: int = LOOKUP_LIMIT,
        include_tests: bool = False,
        compact: bool = True,
        include_count: bool = False,
        output_format: str = "json",
    ) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        skip_value = _bounded_skip(skip)
        limit_value = _bounded_limit(limit, default=LOOKUP_LIMIT, maximum=200)
        # Split on whitespace for AND semantics: "GraphWriter upsertFile" matches methods whose
        # signature contains both terms rather than the exact joined string.
        fragment_terms = [t.strip().lower() for t
                          in (signature_fragment or "").split() if t.strip()]
        return_projection = (
            """
                   method.name AS name, method.ownerDisplayName AS ownerDisplayName,
                   method.startLine AS startLine, method.endLine AS endLine,
                   files, method.signature AS sortSignature
            """
            if compact
            else """
                   method.signature AS signature, method.name AS name,
                   method.ownerFqn AS ownerFqn, method.ownerDisplayName AS ownerDisplayName,
                   method.returnType AS returnType, method.visibility AS visibility,
                   method.startLine AS startLine, method.endLine AS endLine,
                   method.isStatic AS isStatic, method.isSynthetic AS isSynthetic, files
            """
        )
        # Rank methods whose owner exactly matches a fragment term first, so a query like
        # "ChunkEmbeddingRefresher" answers "methods of this class" on page 1 instead of
        # mixing in alphabetically earlier signatures that merely reference the class.
        owner_rank = "CASE WHEN toLower(ownerDisplayName) IN $fragment_terms THEN 0 ELSE 1 END"
        rows = self.client.run(
            Q.CODE_LOOKUP_METHODS
            .replace("__RETURN_PROJECTION__", return_projection.strip())
            .replace(
                "__ORDER_BY__",
                f"{owner_rank}, " + ("sortSignature" if compact else "signature"),
            ),
            {
                "project": project_name,
                "fragment_terms": fragment_terms,
                "skip": skip_value,
                "limit": _overfetch_limit(limit_value, include_count),
                "include_tests": include_tests,
            },
        )
        raw_rows = rows
        rows, page_extra = _trim_overfetch(
            rows,
            skip=skip_value,
            limit=limit_value,
            include_count=include_count,
        )
        # Owner-exact rows sort first, so once a non-exact row appears no exact row follows.
        # Tell paginating callers when the owner-exact rows are exhausted, so a query like
        # "ChunkEmbeddingRefresher" stops at page 1 instead of walking reference matches.
        if fragment_terms and page_extra:
            exact_flags = [
                (row.get("ownerDisplayName") or "").lower() in fragment_terms
                for row in raw_rows
            ]
            kept_exact = sum(exact_flags[: len(rows)])
            boundary_seen = not all(exact_flags)
            note = None
            if kept_exact and boundary_seen:
                note = (
                    "owner-exact matches end on this page; further rows only reference the "
                    "fragment — enumerate members via code_lookup_type(include_members=true)"
                )
            elif not kept_exact and skip_value:
                note = "no owner-exact matches on this page; rows only reference the fragment"
            if note:
                page_extra = {**page_extra, "note": note}
        if compact:
            rows = [
                {
                    "owner": row.get("ownerDisplayName"),
                    "name": row.get("name") or _method_name(row.get("signature")),
                    "path": _first(row.get("files")),
                    "startLine": row.get("startLine"),
                    "endLine": row.get("endLine"),
                }
                for row in rows
            ]
        total_count = None
        if include_count:
            count_rows = self.client.run(
                Q.CODE_LOOKUP_METHODS_COUNT,
                {
                    "project": project_name,
                    "fragment_terms": fragment_terms,
                    "include_tests": include_tests,
                },
            )
            total_count = count_rows[0].get("count", 0) if count_rows else len(rows)
        return self._finalize_response(
            _with_result_meta(
                {"project": project_name, "methods": rows},
                rows,
                skip=skip_value,
                limit=limit_value,
                total_count=total_count,
                extra=page_extra,
            ),
            output_format,
        )

    def code_lookup_field(
        self,
        field_fragment: str,
        project: str | None = None,
        skip: int = 0,
        limit: int = LOOKUP_LIMIT,
        include_tests: bool = False,
        compact: bool = True,
        include_count: bool = False,
        output_format: str = "json",
    ) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        skip_value = _bounded_skip(skip)
        limit_value = _bounded_limit(limit, default=LOOKUP_LIMIT, maximum=200)
        projection = (
            """
                   field.fqn AS fqn, field.name AS name,
                   coalesce(owner.ownerDisplayName, owner.name, owner.fqn) AS owner,
                   field.startLine AS startLine, field.endLine AS endLine,
                   files, field.fqn AS sortKey
            """
            if compact
            else """
                   field.fqn AS fqn, field.name AS name, field.type AS type,
                   field.visibility AS visibility, field.isStatic AS isStatic,
                   field.kind AS kind, field.language AS language,
                   owner.fqn AS ownerFqn,
                   coalesce(owner.ownerDisplayName, owner.name, owner.fqn) AS ownerDisplayName,
                   field.startLine AS startLine, field.endLine AS endLine, files
            """
        )
        rows = self.client.run(
            Q.CODE_LOOKUP_FIELD
            .replace("__PROJECTION__", projection.strip())
            .replace("__ORDER_BY__", "sortKey" if compact else "fqn"),
            {
                "project": project_name,
                "fragment": field_fragment,
                "skip": skip_value,
                "limit": _overfetch_limit(limit_value, include_count),
                "include_tests": include_tests,
            },
        )
        rows, page_extra = _trim_overfetch(
            rows,
            skip=skip_value,
            limit=limit_value,
            include_count=include_count,
        )
        if compact:
            rows = [
                {
                    "owner": row.get("owner"),
                    "name": row.get("name"),
                    "fqn": row.get("fqn"),
                    "path": _first(row.get("files")),
                    "startLine": row.get("startLine"),
                    "endLine": row.get("endLine"),
                }
                for row in rows
            ]
        total_count = None
        if include_count:
            count_rows = self.client.run(
                Q.CODE_LOOKUP_FIELD_COUNT,
                {
                    "project": project_name,
                    "fragment": field_fragment,
                    "include_tests": include_tests,
                },
            )
            total_count = count_rows[0].get("count", 0) if count_rows else len(rows)
        return self._finalize_response(
            _with_result_meta(
                {"project": project_name, "fields": rows},
                rows,
                skip=skip_value,
                limit=limit_value,
                total_count=total_count,
                extra=page_extra,
            ),
            output_format,
        )

    def code_lookup_file(
        self,
        path_fragment: str,
        project: str | None = None,
        skip: int = 0,
        limit: int = LOOKUP_LIMIT,
        include_tests: bool = False,
        compact: bool = True,
        include_count: bool = False,
        output_format: str = "json",
    ) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        skip_value = _bounded_skip(skip)
        limit_value = _bounded_limit(limit, default=LOOKUP_LIMIT, maximum=200)
        projection = (
            """
                   file.path AS path, file.language AS language,
                   definitionCount, chunkCount
            """
            if compact
            else """
                   file.path AS path, file.language AS language,
                   file.lastModified AS lastModified,
                   file.retainedSourceToken AS retainedSourceToken,
                   definitionCount, chunkCount
            """
        )
        rows = self.client.run(
            Q.CODE_LOOKUP_FILE.replace("__PROJECTION__", projection.strip()),
            {
                "project": project_name,
                "fragment": path_fragment,
                "skip": skip_value,
                "limit": _overfetch_limit(limit_value, include_count),
                "include_tests": include_tests,
            },
        )
        rows, page_extra = _trim_overfetch(
            rows,
            skip=skip_value,
            limit=limit_value,
            include_count=include_count,
        )
        total_count = None
        if include_count:
            count_rows = self.client.run(
                Q.CODE_LOOKUP_FILE_COUNT,
                {
                    "project": project_name,
                    "fragment": path_fragment,
                    "include_tests": include_tests,
                },
            )
            total_count = count_rows[0].get("count", 0) if count_rows else len(rows)
        return self._finalize_response(
            _with_result_meta(
                {"project": project_name, "files": rows},
                rows,
                skip=skip_value,
                limit=limit_value,
                total_count=total_count,
                extra=page_extra,
            ),
            output_format,
        )

    def code_impact(
        self,
        signature_fragment: str,
        project: str | None = None,
        skip: int = 0,
        limit: int = CALL_GRAPH_LIMIT,
        depth: int = 2,
        include_tests: bool = True,
        compact: bool = True,
        view: str = "callers",
        output_format: str = "json",
    ) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        if view not in {"callers", "files"}:
            raise MemgraphError("code_impact view must be 'callers' or 'files'.")
        skip_value = _bounded_skip(skip)
        limit_value = _bounded_limit(limit, default=CALL_GRAPH_LIMIT, maximum=200)
        depth_value = _bounded_depth(depth)
        params = {
            "project": project_name,
            "fragment": signature_fragment,
            "skip": skip_value,
            "limit": limit_value,
            "impact_limit": limit_value + 1,
            "depth": depth_value,
            "include_tests": include_tests,
        }
        target_rows = self.client.run(Q.CODE_IMPACT_TARGETS, params)
        impact_rows = self.client.run(Q.CODE_IMPACT_CALLERS, params)
        impact_rows, result_meta_extra = _trim_overfetch(
            impact_rows,
            skip=skip_value,
            limit=limit_value,
            include_count=False,
        )
        if not impact_rows and target_rows:
            fallback_rows = self._code_impact_text_reference_rows(
                params,
                target_rows,
                skip=skip_value,
                limit=limit_value,
            )
            impact_rows, fallback_page_extra = _trim_overfetch(
                fallback_rows,
                skip=skip_value,
                limit=limit_value,
                include_count=False,
            )
            if impact_rows:
                result_meta_extra = {"inference": "textReference"}
                if fallback_page_extra:
                    result_meta_extra.update(fallback_page_extra)

        targets = [
            {
                "owner": row.get("owner"),
                "name": row.get("name") or _method_name(row.get("signature")),
                "signature": row.get("signature"),
                "path": _first(row.get("files")),
                "startLine": row.get("startLine"),
                "endLine": row.get("endLine"),
            }
            if compact
            else row
            for row in target_rows
        ]
        impacts = [self._format_impact_row(row, compact) for row in impact_rows]
        if view == "files":
            file_rows = self._impact_file_rows(targets, impacts)
            file_meta_extra = (
                {"inference": result_meta_extra["inference"]}
                if result_meta_extra and "inference" in result_meta_extra
                else None
            )
            return self._finalize_response(
                _with_result_meta(
                    {
                        "project": project_name,
                        "targetMethods": targets,
                        "files": file_rows,
                    },
                    file_rows,
                    skip=0,
                    limit=limit_value,
                    total_count=len(file_rows),
                    extra=file_meta_extra,
                ),
                output_format,
            )
        return self._finalize_response(
            _with_result_meta(
                {
                    "project": project_name,
                    "targetMethods": targets,
                    "impacts": impacts,
                },
                impacts,
                skip=skip_value,
                limit=limit_value,
                extra=result_meta_extra,
            ),
            output_format,
        )

    def _code_impact_text_reference_rows(
        self,
        params: Mapping[str, Any],
        target_rows: Sequence[Mapping[str, Any]],
        *,
        skip: int,
        limit: int,
    ) -> list[dict[str, Any]]:
        target_names = sorted(
            {
                name
                for row in target_rows
                if (name := (row.get("name") or _method_name(row.get("signature"))))
                and len(name) >= 4
            }
        )
        target_signatures = sorted(
            {signature for row in target_rows if (signature := row.get("signature"))}
        )
        if not target_names or not target_signatures:
            return []
        target_terms = [term for name in target_names for term in (f"{name}(", f"{name} (")]
        return self.client.run(
            Q.CODE_IMPACT_TEXT_REFERENCE,
            {
                **params,
                "target_signatures": target_signatures,
                "target_terms": target_terms,
                "skip": skip,
                "fallback_limit": limit + 1,
            },
        )

    def _impact_file_rows(
        self,
        targets: Sequence[dict[str, Any]],
        impacts: Sequence[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        by_path: dict[str, dict[str, Any]] = {}
        for target in targets:
            path = target.get("path")
            if not path:
                continue
            by_path[path] = {
                "path": path,
                "role": "target",
                "minDepth": 0,
                "callerCount": 0,
                "testCallerCount": 0,
                "crossPackageCount": 0,
                "risk": "high",
            }
        for impact in impacts:
            path = impact.get("path")
            if not path:
                continue
            row = by_path.setdefault(
                path,
                {
                    "path": path,
                    "role": "caller",
                    "minDepth": impact.get("depth"),
                    "callerCount": 0,
                    "testCallerCount": 0,
                    "crossPackageCount": 0,
                    "risk": "low",
                },
            )
            row["minDepth"] = min(row.get("minDepth") or impact.get("depth"), impact.get("depth"))
            row["callerCount"] += 1
            if impact.get("isTest"):
                row["testCallerCount"] += 1
            if impact.get("crossesPackageBoundary"):
                row["crossPackageCount"] += 1
            if row["role"] != "target":
                if row["minDepth"] == 1 and not impact.get("isTest"):
                    row["risk"] = "high"
                elif row["risk"] != "high" and (row["minDepth"] == 1 or impact.get("isTest")):
                    row["risk"] = "medium"
        return sorted(
            by_path.values(),
            key=lambda row: (
                {"high": 0, "medium": 1, "low": 2}.get(row.get("risk"), 3),
                row.get("minDepth") or 99,
                row.get("path") or "",
            ),
        )

    def _format_impact_row(self, row: dict[str, Any], compact: bool) -> dict[str, Any]:
        caller_path = row.get("callerPath")
        caller_package = _package_name(row.get("callerOwnerFqn"))
        target_package = _package_name(row.get("targetOwnerFqn"))
        enriched = dict(row)
        enriched["isTest"] = _is_test_path(caller_path)
        crosses_pkg = (
            caller_package != target_package if caller_package and target_package else None
        )
        enriched["crossesPackageBoundary"] = crosses_pkg
        if not compact:
            return enriched
        return {
            "depth": enriched.get("depth"),
            "owner": enriched.get("callerOwner"),
            "name": enriched.get("callerName") or _method_name(enriched.get("callerSignature")),
            "path": caller_path,
            "startLine": enriched.get("callerStartLine"),
            "endLine": enriched.get("callerEndLine"),
            "viaOwner": enriched.get("viaOwner"),
            "viaName": enriched.get("viaName") or _method_name(enriched.get("viaSignature")),
            "targetOwner": enriched.get("targetOwner"),
            "targetName": enriched.get("targetName")
            or _method_name(enriched.get("targetSignature")),
            "isTest": enriched.get("isTest"),
            "crossesPackageBoundary": crosses_pkg,
            "inferred": enriched.get("inferred"),
            "evidence": enriched.get("evidence"),
        }

    def code_callers(
        self,
        callee_fragment: str,
        project: str | None = None,
        skip: int = 0,
        limit: int = CALL_GRAPH_LIMIT,
        include_tests: bool = False,
        compact: bool = True,
        include_count: bool = False,
        output_format: str = "json",
    ) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        skip_value = _bounded_skip(skip)
        limit_value = _bounded_limit(limit, default=CALL_GRAPH_LIMIT, maximum=100)
        rows = self.client.run(
            Q.CODE_CALLERS,
            {
                "project": project_name,
                "fragment": callee_fragment,
                "skip": skip_value,
                "limit": _overfetch_limit(limit_value, include_count),
                "include_tests": include_tests,
            },
        )
        rows, page_extra = _trim_overfetch(
            rows,
            skip=skip_value,
            limit=limit_value,
            include_count=include_count,
        )
        total_count = None
        if include_count:
            count_rows = self.client.run(
                Q.CODE_CALLERS_COUNT,
                {
                    "project": project_name,
                    "fragment": callee_fragment,
                    "include_tests": include_tests,
                },
            )
            total_count = count_rows[0].get("count", 0) if count_rows else len(rows)
        if compact:
            rows = [
                {
                    "owner": row.get("callerOwner"),
                    "name": row.get("callerName") or _method_name(row.get("callerSignature")),
                    "path": row.get("callerPath"),
                    "startLine": row.get("callerStartLine"),
                    "endLine": row.get("callerEndLine"),
                    "calleeOwner": row.get("calleeOwner"),
                    "calleeName": row.get("calleeName") or _method_name(row.get("calleeSignature")),
                }
                for row in rows
            ]
        return self._finalize_response(
            _with_result_meta(
                {"project": project_name, "callers": rows},
                rows,
                skip=skip_value,
                limit=limit_value,
                total_count=total_count,
                extra=page_extra,
            ),
            output_format,
        )

    def code_method_context(
        self,
        signature_fragment: str,
        project: str | None = None,
        method_limit: int = DISCOVERY_LIMIT,
        neighbor_limit: int = DISCOVERY_LIMIT,
        include_tests: bool = False,
        compact: bool = True,
        output_format: str = "json",
    ) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        methods = self.code_lookup_methods(
            signature_fragment,
            project_name,
            skip=0,
            limit=method_limit,
            include_tests=include_tests,
            compact=compact,
            output_format="json",
        )
        callers = self.code_callers(
            signature_fragment,
            project_name,
            skip=0,
            limit=neighbor_limit,
            include_tests=include_tests,
            compact=compact,
            output_format="json",
        )
        callees = self.code_callees(
            signature_fragment,
            project_name,
            skip=0,
            limit=neighbor_limit,
            include_tests=include_tests,
            compact=compact,
            output_format="json",
        )
        return self._finalize_response(
            {
                "project": project_name,
                "methods": methods["methods"],
                "callers": callers["callers"],
                "callees": callees["callees"],
                "meta": {
                    "methods": methods["meta"],
                    "callers": callers["meta"],
                    "callees": callees["meta"],
                },
            },
            output_format,
        )

    def code_callees(
        self,
        caller_fragment: str,
        project: str | None = None,
        skip: int = 0,
        limit: int = CALL_GRAPH_LIMIT,
        include_tests: bool = False,
        compact: bool = True,
        include_count: bool = False,
        output_format: str = "json",
    ) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        skip_value = _bounded_skip(skip)
        limit_value = _bounded_limit(limit, default=CALL_GRAPH_LIMIT, maximum=100)
        rows = self.client.run(
            Q.CODE_CALLEES,
            {
                "project": project_name,
                "fragment": caller_fragment,
                "skip": skip_value,
                "limit": _overfetch_limit(limit_value, include_count),
                "include_tests": include_tests,
            },
        )
        rows, page_extra = _trim_overfetch(
            rows,
            skip=skip_value,
            limit=limit_value,
            include_count=include_count,
        )
        total_count = None
        if include_count:
            count_rows = self.client.run(
                Q.CODE_CALLEES_COUNT,
                {
                    "project": project_name,
                    "fragment": caller_fragment,
                    "include_tests": include_tests,
                },
            )
            total_count = count_rows[0].get("count", 0) if count_rows else len(rows)
        if compact:
            rows = [
                {
                    "callerOwner": row.get("callerOwner"),
                    "callerName": row.get("callerName") or _method_name(row.get("callerSignature")),
                    "owner": row.get("calleeOwner"),
                    "name": row.get("calleeName") or _method_name(row.get("calleeSignature")),
                    "path": row.get("calleePath"),
                    "startLine": row.get("calleeStartLine"),
                    "endLine": row.get("calleeEndLine"),
                }
                for row in rows
            ]
        return self._finalize_response(
            _with_result_meta(
                {"project": project_name, "callees": rows},
                rows,
                skip=skip_value,
                limit=limit_value,
                total_count=total_count,
                extra=page_extra,
            ),
            output_format,
        )

    def code_hot_paths(
        self,
        project: str | None = None,
        limit: int = DISCOVERY_LIMIT,
        include_tests: bool = False,
        include_evidence: bool = False,
        sections: Sequence[str] | str | None = None,
        output_format: str = "json",
    ) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        bounded_limit = _bounded_limit(limit, default=DISCOVERY_LIMIT, maximum=50)
        requested_sections = _normalize_sections(
            sections,
            allowed=HOT_PATH_SECTIONS,
            default=frozenset({"fanIn", "longestMethods", "fanOut"}),
        )
        params = {
            "project": project_name,
            "limit": bounded_limit,
            "include_tests": include_tests,
        }
        largest_types = (
            self.client.run(Q.CODE_HOT_PATHS_LARGEST_TYPES, params)
            if "largestTypes" in requested_sections
            else []
        )
        longest_methods = (
            self.client.run(Q.CODE_HOT_PATHS_LONGEST_METHODS, params)
            if "longestMethods" in requested_sections
            else []
        )
        fan_in = (
            self.client.run(Q.CODE_HOT_PATHS_FAN_IN, params)
            if "fanIn" in requested_sections
            else []
        )
        fan_out = (
            self.client.run(Q.CODE_HOT_PATHS_FAN_OUT, params)
            if "fanOut" in requested_sections
            else []
        )
        rows: list[dict[str, Any]] = []
        for section, section_rows in (
            ("largestTypes", largest_types),
            ("longestMethods", longest_methods),
            ("fanIn", fan_in),
            ("fanOut", fan_out),
        ):
            for row in section_rows:
                row["section"] = section
                row.pop("sortKey", None)
                if not include_evidence:
                    row.pop("path", None)
                    row.pop("startLine", None)
                    row.pop("endLine", None)
                rows.append(row)
        return self._finalize_response(
            _with_result_meta(
                {"project": project_name, "hotPaths": rows},
                rows,
                limit=bounded_limit,
            ),
            output_format,
        )

    def code_operation_hot_paths(
        self,
        project: str | None = None,
        sink_fragments: Sequence[str] | str | None = None,
        owner_fragment: str | None = None,
        path_contains: str | None = None,
        limit: int = DISCOVERY_LIMIT,
        include_tests: bool = False,
        output_format: str = "json",
    ) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        bounded_limit = _bounded_limit(limit, default=DISCOVERY_LIMIT, maximum=50)
        fragments = _normalize_lower_list(sink_fragments)
        custom_fragments = bool(fragments)
        fragments = fragments or sorted(DEFAULT_OPERATION_SINKS)
        owner_filter = (owner_fragment or "").strip().lower()
        path_filter = (path_contains or "").strip()
        rows = self.client.run(
            Q.CODE_OPERATION_HOT_PATHS,
            {
                "project": project_name,
                "fragments": fragments,
                "owner_fragment": owner_filter,
                "path_contains": path_filter,
                "include_tests": include_tests,
                "custom_fragments": custom_fragments,
                "limit": bounded_limit,
            },
        )
        for row in rows:
            row.pop("signature", None)
            row.pop("score", None)
            row.pop("lines", None)
            row["riskHints"] = [
                hint
                for hint, active in (
                    ("many-sink-calls", (row.get("sinkCallEdges") or 0) >= 5),
                    ("large-method", (row.get("endLine") or 0) - (row.get("startLine") or 0) >= 49),
                    ("multi-sink", (row.get("distinctSinks") or 0) >= 3),
                )
                if active
            ]
        extra: dict[str, Any] | None = None
        if owner_filter or path_filter:
            extra = {
                k: v
                for k, v in (
                    ("ownerFragment", owner_filter),
                    ("pathContains", path_filter),
                )
                if v
            }
        return self._finalize_response(
            _with_result_meta(
                {"project": project_name, "operationHotPaths": rows},
                rows,
                limit=bounded_limit,
                extra=extra,
            ),
            output_format,
        )

    def code_resource_risk_scan(
        self,
        project: str | None = None,
        path_contains: str | None = None,
        extensions: Sequence[str] | str | None = None,
        limit: int = DISCOVERY_LIMIT,
        include_tests: bool = False,
        output_format: str = "json",
    ) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        bounded_limit = _bounded_limit(limit, default=DISCOVERY_LIMIT, maximum=50)
        extension_filter = _normalize_extensions(extensions)
        path_filter = (path_contains or "").strip()
        candidate_limit = min(max(bounded_limit * 20, 100), 500)
        candidates = self.client.run(
            Q.CODE_RESOURCE_RISK_SCAN,
            {
                "project": project_name,
                "path_contains": path_filter,
                "include_tests": include_tests,
                "limit": candidate_limit,
            },
        )
        rows: list[dict[str, Any]] = []
        scanned_files = 0
        for candidate in candidates:
            path = candidate.get("path") or ""
            if extension_filter and not any(path.lower().endswith(ext) for ext in extension_filter):
                continue
            scanned_files += 1
            rows.extend(
                _resource_risk_rows(
                    path,
                    candidate.get("language"),
                    candidate.get("text") or "",
                )
            )
        rows = _aggregate_resource_risks(rows)
        rows.sort(
            key=lambda row: (
                -(row.get("score") or 0),
                row.get("path") or "",
                row.get("line") or 0,
                row.get("pattern") or "",
            )
        )
        rows = rows[:bounded_limit]
        return self._finalize_response(
            _with_result_meta(
                {"project": project_name, "resourceRisks": rows},
                rows,
                limit=bounded_limit,
            ),
            output_format,
        )

    def code_quality_stats(
        self,
        project: str | None = None,
        include_tests: bool = False,
        limit: int = DISCOVERY_LIMIT,
        output_format: str = "json",
    ) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        bounded_limit = _bounded_limit(limit, default=DISCOVERY_LIMIT, maximum=50)
        params = {
            "project": project_name,
            "limit": bounded_limit,
            "include_tests": include_tests,
        }
        rows = self.client.run(Q.CODE_QUALITY_STATS, params)
        stats = rows[0] if rows else {}
        response = {
            "project": project_name,
            "inventory": stats.get("inventory", []),
            "methodLengths": stats.get("methodLengths", {}),
            "fanOut": stats.get("fanOut", {}),
            "fanIn": stats.get("fanIn", {}),
            "typeSizes": stats.get("typeSizes", {}),
            "chunksByLabel": stats.get("chunksByLabel", []),
            "filesByMethods": stats.get("filesByMethods", []),
        }
        response["meta"] = {"limit": bounded_limit}
        return self._finalize_response(response, output_format)

    def code_hierarchy(
        self,
        fqn: str,
        project: str | None = None,
        output_format: str = "json",
    ) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        class_hierarchy = self.client.run(
            Q.CODE_HIERARCHY_CLASS,
            {"project": project_name, "fqn": fqn},
        )
        ancestors = self.client.run(
            Q.CODE_HIERARCHY_ANCESTORS,
            {"project": project_name, "fqn": fqn},
        )
        implementors = self.client.run(
            Q.CODE_HIERARCHY_IMPLEMENTORS,
            {"project": project_name, "fqn": fqn},
        )
        return self._finalize_response(
            {
                "project": project_name,
                "classHierarchy": class_hierarchy,
                "ancestors": ancestors,
                "interfaceImplementors": implementors,
            },
            output_format,
        )

    def code_test_context(
        self,
        test_fragment: str,
        project: str | None = None,
        limit: int = DISCOVERY_LIMIT,
        production_limit: int = DISCOVERY_LIMIT,
        output_format: str = "json",
    ) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        bounded_limit = _bounded_limit(limit, default=DISCOVERY_LIMIT, maximum=25)
        bounded_production_limit = _bounded_limit(
            production_limit,
            default=DISCOVERY_LIMIT,
            maximum=50,
        )
        owner_fragment, _method_fragment, terms, min_term_matches = _test_fragment_parts(
            test_fragment,
        )
        rows = self.client.run(
            Q.CODE_TEST_METHODS,
            {
                "project": project_name,
                "fragment": test_fragment,
                "owner_fragment": owner_fragment,
                "terms": terms,
                "min_term_matches": min_term_matches,
                "limit": bounded_limit,
            },
        )
        file_rows = self.client.run(
            Q.CODE_TEST_FILES,
            {
                "project": project_name,
                "fragment": test_fragment,
                "owner_fragment": owner_fragment,
                "terms": terms,
                "min_term_matches": min_term_matches,
                "limit": bounded_limit,
            },
        )
        exact_matches = sum(1 for row in rows if row.get("exactish"))
        fuzzy_match_count = len(rows) - exact_matches
        rows = [row for row in rows if row.get("exactish")]
        production_rows = []
        if exact_matches > 0:
            production_rows = self.client.run(
                Q.CODE_TEST_PRODUCTION_CALLEES,
                {
                    "project": project_name,
                    "fragment": test_fragment,
                    "limit": bounded_production_limit,
                },
            )
        for row in rows:
            row.pop("signature", None)
            row.pop("exactish", None)
            row.pop("termMatches", None)
        production_rows = [r for r in production_rows if r.get("name") != "<init>"]
        for row in production_rows:
            row.pop("signature", None)
        meta: dict[str, Any] = {"exactMatches": exact_matches}
        if fuzzy_match_count:
            meta["fuzzyMatchesSuppressed"] = True
            meta["fuzzyMatchCount"] = fuzzy_match_count
        return self._finalize_response(
            {
                "project": project_name,
                "tests": rows,
                "productionCallees": production_rows,
                "testFiles": file_rows,
                "meta": meta,
            },
            output_format,
        )

    def memory_orientation(
        self,
        project: str | None = None,
        compact: bool = False,
    ) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        rule_projection = (
            "rule.id AS id, rule.severity AS severity, rule.title AS title"
            if compact
            else """
                       rule.id AS id, rule.severity AS severity, rule.title AS title,
                       rule.description AS description
            """.strip()
        )
        finding_projection = (
            "finding.id AS id, finding.type AS type, finding.title AS title"
            if compact
            else """
                       finding.id AS id, finding.type AS type, finding.title AS title,
                       finding.summary AS summary
            """.strip()
        )
        task_projection = (
            """
                       task.id AS id, task.title AS title, task.status AS status,
                       task.priority AS priority
            """.strip()
            if compact
            else """
                       task.id AS id, task.title AS title, task.status AS status,
                       task.priority AS priority, task.description AS description
            """.strip()
        )
        risk_projection = (
            "risk.id AS id, risk.title AS title, risk.severity AS severity"
            if compact
            else """
                       risk.id AS id, risk.title AS title, risk.severity AS severity,
                       risk.mitigation AS mitigation
            """.strip()
        )
        return self._finalize_response(
            {
                "project": project_name,
                "rules": self.client.run(
                    Q.MEMORY_ORIENTATION_RULES.replace("__RETURN_PROJECTION__", rule_projection),
                    {"project": project_name},
                ),
                "openFindings": self.client.run(
                    Q.MEMORY_ORIENTATION_OPEN_FINDINGS.replace(
                        "__RETURN_PROJECTION__", finding_projection
                    ),
                    {"project": project_name},
                ),
                "activeTasks": self.client.run(
                    Q.MEMORY_ORIENTATION_ACTIVE_TASKS.replace(
                        "__RETURN_PROJECTION__", task_projection
                    ),
                    {"project": project_name},
                ),
                "openQuestions": self.client.run(
                    Q.MEMORY_ORIENTATION_OPEN_QUESTIONS,
                    {"project": project_name},
                ),
                "openRisks": self.client.run(
                    Q.MEMORY_ORIENTATION_OPEN_RISKS.replace(
                        "__RETURN_PROJECTION__", risk_projection
                    ),
                    {"project": project_name},
                ),
            }
        )

    def memory_schema(self, memory_type: str | None = None) -> dict[str, Any]:
        memory_types = [memory_type] if memory_type is not None else sorted(MEMORY_SPECS)
        schemas = {type_name: _memory_schema_entry(type_name) for type_name in memory_types}
        return {
            "memoryTypes": schemas,
            "targetTypes": sorted(TARGET_TYPES),
        }

    def memory_search(
        self,
        query: str,
        project: str | None = None,
        limit: int = 5,
    ) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        index_name = self._select_vector_index_name(
            self.config.memory_embedding_index_name,
            project_name,
        )
        rows = self.client.run(
            Q.MEMORY_SEARCH,
            {
                "index": index_name,
                "project": project_name,
                "query": query,
                "embed_config": self._embedding_text_config(),
                "limit": _bounded_limit(limit, default=5, maximum=20),
            },
        )
        return self._finalize_response({"project": project_name, "query": query, "hits": rows})

    def memory_get(
        self,
        memory_id: str,
        project: str | None = None,
        *,
        finalize: bool = True,
    ) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        rows = self.client.run(
            Q.MEMORY_GET.replace("__MEMORY_LABEL_PREDICATE__", _memory_label_predicate("memory")),
            {"project": project_name, "memory_id": memory_id},
        )
        response = {"project": project_name, "memory": rows[0] if rows else None}
        return self._finalize_response(response) if finalize else response

    def delete_memory(self, memory_id: str, project: str | None = None) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        rows = self.client.run(
            Q.MEMORY_DELETE_READ.replace(
                "__MEMORY_LABEL_PREDICATE__", _memory_label_predicate("memory")
            ),
            {"project": project_name, "memory_id": memory_id},
        )
        if not rows:
            return self._finalize_response(
                {
                    "project": project_name,
                    "deleted": False,
                    "memory": None,
                    "chunkIds": [],
                    "orphanCodeRefsDeleted": 0,
                }
            )

        memory = rows[0]
        code_refs = memory.get("codeRefs", [])
        self.client.run(
            Q.MEMORY_DELETE_WRITE.replace(
                "__MEMORY_LABEL_PREDICATE__", _memory_label_predicate("memory")
            ),
            {"project": project_name, "memory_id": memory_id},
            write=True,
        )

        orphan_deleted = 0
        if code_refs:
            orphan_rows = self.client.run(
                Q.MEMORY_DELETE_ORPHAN_REFS,
                {"project": project_name, "code_refs": code_refs},
                write=True,
            )
            orphan_deleted = orphan_rows[0].get("deleted", 0) if orphan_rows else 0

        return self._finalize_response(
            {
                "project": project_name,
                "deleted": True,
                "memory": {
                    "labels": memory.get("labels", []),
                    "properties": memory.get("properties", {}),
                },
                "chunkIds": memory.get("chunkIds", []),
                "codeRefs": code_refs,
                "orphanCodeRefsDeleted": orphan_deleted,
            }
        )

    def memory_upsert(
        self,
        memory_type: str,
        memory_id: str,
        fields: Mapping[str, Any],
        project: str | None = None,
        code_ref: Mapping[str, str] | None = None,
        refresh_chunk: bool = True,
        embed: bool = True,
    ) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        spec = _memory_spec(memory_type)
        properties = self._validated_memory_fields(memory_type, fields)
        rows = self.client.run(
            Q.MEMORY_UPSERT.replace("__LABEL__", spec.label).replace(
                "__RELATION__", spec.relation
            ),
            {"project": project_name, "memory_id": memory_id, "properties": properties},
            write=True,
        )
        link_result = None
        if code_ref is not None:
            link_result = self.memory_link_code_ref(
                memory_type,
                memory_id,
                code_ref.get("target_type") or code_ref.get("targetType") or "",
                code_ref.get("key") or "",
                project_name,
                refresh_chunk=False,
            )
        chunk_result = None
        if refresh_chunk:
            chunk_result = self.memory_refresh_chunk(
                memory_type,
                memory_id,
                project_name,
                embed=embed,
            )
        return self._finalize_response(
            {
                "project": project_name,
                "memory": rows[0] if rows else None,
                "codeRef": link_result,
                "chunk": chunk_result,
            }
        )

    def memory_update_status(
        self,
        memory_type: str,
        memory_id: str,
        status: str,
        project: str | None = None,
        refresh_chunk: bool = True,
        embed: bool = True,
    ) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        spec = _memory_spec(memory_type)
        if "status" not in spec.fields:
            raise MemgraphError(f"{memory_type} does not have a status field.")
        self._validate_controlled_value(memory_type, "status", status)
        rows = self.client.run(
            Q.MEMORY_UPDATE_STATUS.replace("__LABEL__", spec.label),
            {"project": project_name, "memory_id": memory_id, "status": status},
            write=True,
        )
        chunk_result = None
        if refresh_chunk:
            chunk_result = self.memory_refresh_chunk(
                memory_type,
                memory_id,
                project_name,
                embed=embed,
            )
        return self._finalize_response(
            {
                "project": project_name,
                "memory": rows[0] if rows else None,
                "chunk": chunk_result,
            }
        )

    def memory_link_code_ref(
        self,
        memory_type: str,
        memory_id: str,
        target_type: str,
        key: str,
        project: str | None = None,
        *,
        refresh_chunk: bool = True,
        embed: bool = True,
    ) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        spec = _memory_spec(memory_type)
        target_label, predicate = _target_match(target_type)
        rows = self.client.run(
            Q.MEMORY_LINK_CODE_REF.replace("__LABEL__", spec.label)
            .replace("__TARGET_LABEL__", target_label)
            .replace("__TARGET_PREDICATE__", predicate),
            {
                "project": project_name,
                "memory_id": memory_id,
                "target_type": target_type,
                "target_key": key,
            },
            write=True,
        )
        chunk_result = None
        if rows and refresh_chunk:
            chunk_result = self.memory_refresh_chunk(
                memory_type,
                memory_id,
                project_name,
                embed=embed,
            )
        return self._finalize_response(
            {
                "project": project_name,
                "resolved": bool(rows),
                "links": rows,
                "chunk": chunk_result,
            }
        )

    def memory_refresh_chunk(
        self,
        memory_type: str,
        memory_id: str,
        project: str | None = None,
        *,
        embed: bool = True,
        finalize: bool = True,
    ) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        spec = _memory_spec(memory_type)
        memory = self.memory_get(memory_id, project_name, finalize=False).get("memory")
        if memory is None:
            raise MemgraphError(f"Memory node {memory_id!r} was not found.")

        text = self._memory_chunk_text(memory_type, memory)
        text_hash = sha256(text.encode("utf-8")).hexdigest()
        chunk_id = f"MCH-{memory_id}"
        rows = self.client.run(
            Q.MEMORY_REFRESH_CHUNK.replace("__LABEL__", spec.label),
            {
                "project": project_name,
                "memory_id": memory_id,
                "chunk_id": chunk_id,
                "memory_type": memory_type,
                "text": text,
                "text_hash": text_hash,
            },
            write=True,
        )
        embedding_result = None
        if embed:
            embedding_result = self.memory_refresh_embeddings(
                [chunk_id], project_name, finalize=False
            )
            if rows and chunk_id in set(embedding_result.get("embedded", [])):
                rows[0]["dirty"] = False
        response = {
            "project": project_name,
            "chunk": rows[0] if rows else None,
            "embedding": embedding_result,
        }
        return self._finalize_response(response) if finalize else response

    def memory_refresh_embeddings(
        self,
        chunk_ids: Sequence[str],
        project: str | None = None,
        *,
        finalize: bool = True,
    ) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        ids = [chunk_id for chunk_id in dict.fromkeys(chunk_ids) if chunk_id]
        if not ids:
            response = {"project": project_name, "embedded": []}
            return self._finalize_response(response) if finalize else response

        pending = self.client.run(
            Q.MEMORY_REFRESH_EMBEDDINGS_PENDING,
            {
                "project": project_name,
                "ids": ids,
                "model_name": self.config.embedding_model_name,
                "dimension": self.config.embedding_dimensions,
            },
        )
        pending_ids = [row["id"] for row in pending]
        if not pending_ids:
            response = {"project": project_name, "embedded": []}
            return self._finalize_response(response) if finalize else response

        result = self.client.run(
            Q.MEMORY_REFRESH_EMBEDDINGS_EMBED,
            {
                "project": project_name,
                "ids": pending_ids,
                "embed_config": self._node_sentence_config(),
            },
            write=True,
        )
        embed_success = result[0].get("success", False) if result else False
        if not embed_success:
            response = {"project": project_name, "embedded": [], "result": result}
            return self._finalize_response(response) if finalize else response

        dimension = result[0].get("dimension") if result else self.config.embedding_dimensions
        self.client.run(
            Q.MEMORY_REFRESH_EMBEDDINGS_MARK,
            {
                "project": project_name,
                "ids": pending_ids,
                "model_name": self.config.embedding_model_name,
                "dimension": dimension,
            },
            write=True,
        )
        response = {"project": project_name, "embedded": pending_ids, "result": result}
        return self._finalize_response(response) if finalize else response

    def raw_read_cypher(
        self,
        query: str,
        project: str | None = None,
        parameters: Mapping[str, Any] | None = None,
        limit: int = 200,
        output_format: str = "json",
    ) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        _ensure_read_only_query(query)
        _ensure_project_scoped(query)
        params = dict(parameters or {})
        bounded_limit = _bounded_limit(limit, default=200, maximum=500)
        params.setdefault("project", project_name)
        params.setdefault("limit", bounded_limit)
        rows = self.client.run(query, params)
        return self._finalize_response(
            _with_result_meta(
                {"project": project_name, "rows": rows},
                rows,
                limit=bounded_limit,
            ),
            output_format,
        )

    def _validated_memory_fields(
        self,
        memory_type: str,
        fields: Mapping[str, Any],
    ) -> dict[str, Any]:
        if not isinstance(fields, Mapping):
            raise MemgraphError("fields must be an object.")

        spec = _memory_spec(memory_type)
        unknown = sorted(set(fields) - spec.fields)
        if unknown:
            allowed = ", ".join(sorted(spec.fields))
            raise MemgraphError(
                f"Unsupported {memory_type} field(s): {', '.join(unknown)}. Allowed: {allowed}."
            )

        properties: dict[str, Any] = {}
        for key, value in fields.items():
            if value is None:
                continue
            normalized = str(value) if (memory_type, key) in CONTROLLED_VALUES else value
            self._validate_controlled_value(memory_type, key, normalized)
            properties[key] = normalized
        return properties

    def _validate_controlled_value(self, memory_type: str, field: str, value: Any) -> None:
        allowed = CONTROLLED_VALUES.get((memory_type, field))
        if allowed is not None and value not in allowed:
            joined = ", ".join(sorted(allowed))
            raise MemgraphError(f"{memory_type}.{field} must be one of: {joined}.")

    def _memory_chunk_text(self, memory_type: str, memory: Mapping[str, Any]) -> str:
        properties = memory.get("properties") or {}
        refs = memory.get("codeRefs") or []
        spec = _memory_spec(memory_type)
        field_parts = []
        for key in sorted(spec.fields):
            value = properties.get(key)
            if value not in (None, ""):
                field_parts.append(f"{key}: {value}")
        ref_parts = [f"{ref.get('targetType')} {ref.get('key')}" for ref in refs]
        refs_text = ", ".join(ref_parts) if ref_parts else "none"
        return (
            f"{memory_type}: {properties.get('id')}. "
            f"{'; '.join(field_parts)}. "
            f"CodeRefs: {refs_text}."
        )
