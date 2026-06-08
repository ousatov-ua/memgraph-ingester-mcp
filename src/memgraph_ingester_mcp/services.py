"""High-level Memgraph Ingester operations exposed as MCP tools."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from hashlib import sha256
from typing import Any

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
        "class",
        "file",
        "files",
        "from",
        "get",
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
        "this",
        "time",
        "type",
        "value",
        "with",
    }
)
DISCOVERY_LIMIT = 5
LOOKUP_LIMIT = 10
CALL_GRAPH_LIMIT = 10
MEMBER_LIMIT = 25
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
    return {
        str(row.get("index_name"))
        for row in rows
        if row.get("index_name") is not None
    }


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
    return bool(path and (path.startswith("src/test/") or "/test/" in path))


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
            "rows": [
                [_to_table_json(row.get(col)) for col in live_columns] for row in value
            ],
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
            """
            MATCH (c:Code {project: $project})
            RETURN c.language AS language, c.lastIngested AS lastIngested
            ORDER BY language
            """,
            {"project": project_name},
        )
        inventory = self.client.run(
            """
            MATCH (code:Code {project: $project})
            OPTIONAL MATCH (file:File {project: $project})
            WITH collect(DISTINCT code.language) AS languages, count(DISTINCT file) AS files
            OPTIONAL MATCH (type {project: $project})
            WHERE type:Class OR type:Interface OR type:Annotation
            WITH languages, files, count(DISTINCT type) AS types
            OPTIONAL MATCH (method:Method {project: $project})
            RETURN size(languages) AS languageCount, files AS fileCount,
                   types AS typeCount, count(DISTINCT method) AS methodCount
            """,
            {"project": project_name},
        )
        memories = self.client.run(
            """
            MATCH (node {project: $project})
            WHERE node:Decision OR node:ADR OR node:Rule OR node:Context
               OR node:Finding OR node:Task OR node:Risk OR node:Question OR node:Idea
            RETURN labels(node)[0] AS type, count(node) AS count
            ORDER BY type
            """,
            {"project": project_name},
        )
        indexes = [
            row
            for row in vector_index_rows
            if row.get("index_name") in vector_index_names
        ]
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
                """
                MATCH (l:Language {project: $project})-[:CONTAINS]->(c:Code)
                RETURN l.name AS languageName, l.graphName AS graphName, c.language AS language,
                       c.lastIngested AS lastIngested
                ORDER BY languageName
                """,
                {"project": project_name},
            )
        if "packages" in requested:
            response["packages"] = self.client.run(
                """
                MATCH (p:Package {project: $project})
                OPTIONAL MATCH (p)-[:CONTAINS]->(c:Class {project: $project})
                WITH p, count(DISTINCT c) AS classes
                RETURN p.language AS language, p.name AS package, classes
                ORDER BY language, package
                LIMIT $limit
                """,
                {"project": project_name, "limit": bounded_limit},
            )
        if "largestTypes" in requested:
            response["largestTypes"] = self.client.run(
                """
                MATCH (t {project: $project})-[:DECLARES]->(m:Method {project: $project})
                WHERE (t:Class OR t:Interface OR t:Annotation)
                  AND coalesce(t.isExternal, false) = false
                  AND coalesce(m.isSynthetic, false) = false
                WITH t.fqn AS type, labels(t)[0] AS label, count(m) AS methodCount
                RETURN type, label, methodCount
                ORDER BY methodCount DESC, type
                LIMIT $limit
                """,
                {"project": project_name, "limit": bounded_limit},
            )
        if "crossOwnerCalls" in requested:
            response["crossOwnerCalls"] = self.client.run(
                """
                MATCH (caller:Method {project: $project})
                  -[:CALLS]->(callee:Method {project: $project})
                WHERE caller.ownerFqn IS NOT NULL AND callee.ownerFqn IS NOT NULL
                  AND caller.ownerFqn <> callee.ownerFqn
                WITH caller.ownerDisplayName + ' -> ' + callee.ownerDisplayName AS edge,
                     COUNT(*) AS calls
                RETURN edge, calls
                ORDER BY calls DESC, edge
                LIMIT $limit
                """,
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
        search_query = """
            CALL embeddings.text([$query], {}) YIELD embeddings
            WITH embeddings[0] AS queryVector
            CALL vector_search.search($index, $limit, queryVector)
            YIELD node AS chunk, similarity
            WITH chunk, similarity
            WHERE chunk.project = $project
              AND ($include_tests OR chunk.path IS NULL OR NOT chunk.path STARTS WITH 'src/test/')
            MATCH (source {project: $project})-[:HAS_RAG_CHUNK]->(chunk)
            WITH chunk, source, similarity,
                 CASE
                   WHEN chunk.sourceLabel = 'Method'
                     AND coalesce(source.startLine, 0) <= 0 THEN 'synthetic'
                   WHEN chunk.sourceLabel = 'Class'
                     AND coalesce(chunk.kind, source.kind, '') = 'module' THEN 'synthetic'
                   WHEN chunk.sourceLabel = 'Method'
                     AND coalesce(chunk.kind, '') = 'constructor' THEN 'secondary'
                   WHEN chunk.sourceLabel = 'Field' THEN 'secondary'
                   WHEN chunk.sourceLabel = 'File' THEN 'file'
                   ELSE coalesce(chunk.ragRole, 'primary')
                 END AS effectiveRole
            WHERE size($rag_roles) = 0 OR effectiveRole IN $rag_roles
            WITH chunk, source, similarity, effectiveRole,
                 coalesce(chunk.sourceLabel, labels(source)[0]) AS kind,
                 chunk.sourceId AS sourceId,
                 coalesce(source.ownerDisplayName, source.ownerFqn, chunk.ownerFqn) AS owner,
                 coalesce(source.name, chunk.signature, chunk.sourceId) AS name,
                 chunk.path AS path,
                 source.startLine AS startLine,
                 source.endLine AS endLine
            WHERE (size($kinds) = 0 OR kind IN $kinds)
              AND (size($path_prefixes) = 0
                   OR any(prefix IN $path_prefixes WHERE path STARTS WITH prefix))
              AND ($path_contains = '' OR path CONTAINS $path_contains)
              AND ($owner_fragment = '' OR coalesce(owner, '') CONTAINS $owner_fragment)
              AND ($min_score <= 0 OR similarity >= $min_score)
            RETURN __RETURN_PROJECTION__
            ORDER BY similarity DESC
            """.replace("__RETURN_PROJECTION__", return_projection.strip())
        raw_rows = self.client.run(
            search_query,
            {
                "index": index_name,
                "project": project_name,
                "query": query,
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
        filtered_rows: list[dict[str, Any]] = []
        for row in raw_rows:
            if kind_filter and row.get("kind") not in kind_filter:
                continue
            if not _starts_with_any(row.get("path"), path_prefix_filter):
                continue
            if path_contains_filter and path_contains_filter not in (row.get("path") or ""):
                continue
            if owner_filter and not _contains_any(row.get("owner"), [owner_filter]):
                continue
            if min_score > 0 and float(row.get("score") or 0) < min_score:
                continue
            filtered_rows.append(row)
        rows = filtered_rows
        if dedupe_by_source:
            deduped: list[dict[str, Any]] = []
            seen: set[tuple[str, str]] = set()
            for row in rows:
                key = (row.get("kind") or "", row.get("sourceId") or "")
                if key in seen:
                    continue
                seen.add(key)
                deduped.append(row)
                if len(deduped) >= bounded_limit:
                    break
            rows = deduped
        else:
            rows = rows[:bounded_limit]
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
        return self._finalize_response(
            _with_result_meta(
                {"project": project_name, "hits": rows},
                rows,
                limit=bounded_limit,
                extra={"candidateLimitReached": True} if saturated else None,
            ),
            output_format,
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
        search_terms = required_terms + optional_terms
        kind_filter = _normalize_string_list(kinds)
        role_filter = _normalize_string_list(rag_roles)
        if not role_filter and not include_secondary:
            role_filter = list(DEFAULT_RAG_ROLES)
        path_contains_filter = (path_contains or "").strip()
        text_projection = ", chunk.text AS text" if include_text else ""
        rows = self.client.run(
            f"""
            MATCH (source {{project: $project}})
              -[:HAS_RAG_CHUNK]->(chunk:CodeChunk {{project: $project}})
            WITH source, chunk,
                 toLower(coalesce(chunk.text, '') + ' ' + coalesce(chunk.path, '') + ' '
                         + coalesce(chunk.sourceId, '')) AS haystack,
                 CASE
                   WHEN chunk.sourceLabel = 'Method'
                     AND coalesce(source.startLine, 0) <= 0 THEN 'synthetic'
                   WHEN chunk.sourceLabel = 'Class'
                     AND coalesce(chunk.kind, source.kind, '') = 'module' THEN 'synthetic'
                   WHEN chunk.sourceLabel = 'Method'
                     AND coalesce(chunk.kind, '') = 'constructor' THEN 'secondary'
                   WHEN chunk.sourceLabel = 'Field' THEN 'secondary'
                   WHEN chunk.sourceLabel = 'File' THEN 'file'
                   ELSE coalesce(chunk.ragRole, 'primary')
                 END AS effectiveRole
            WITH source, chunk, haystack, effectiveRole,
                 [term IN $search_terms WHERE haystack CONTAINS term] AS matchedTerms
            WHERE ($include_tests OR chunk.path IS NULL OR NOT chunk.path STARTS WITH 'src/test/')
              AND (size($all_terms) = 0 OR all(term IN $all_terms WHERE haystack CONTAINS term))
              AND (size($any_terms) = 0 OR any(term IN $any_terms WHERE haystack CONTAINS term))
              AND (size($kinds) = 0 OR coalesce(chunk.sourceLabel, labels(source)[0]) IN $kinds)
              AND (size($rag_roles) = 0 OR effectiveRole IN $rag_roles)
              AND ($path_contains = '' OR chunk.path CONTAINS $path_contains)
            RETURN coalesce(chunk.sourceLabel, labels(source)[0]) AS kind,
                   chunk.sourceId AS sourceId,
                   coalesce(source.ownerDisplayName, source.ownerFqn, chunk.ownerFqn) AS owner,
                   coalesce(source.name, chunk.signature, chunk.sourceId) AS name,
                   chunk.path AS path,
                   effectiveRole AS ragRole,
                   source.startLine AS startLine,
                   source.endLine AS endLine,
                   size(matchedTerms) AS termMatches{text_projection}
            ORDER BY termMatches DESC, chunk.path, source.startLine, sourceId
            LIMIT $limit
            """,
            {
                "project": project_name,
                "all_terms": required_terms,
                "any_terms": optional_terms,
                "search_terms": search_terms,
                "kinds": kind_filter,
                "rag_roles": role_filter,
                "path_contains": path_contains_filter,
                "include_tests": include_tests,
                "limit": bounded_limit,
            },
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
            f"""
            MATCH (t {{project: $project}})
            WHERE (t:Class OR t:Interface OR t:Annotation) AND {predicate}
            OPTIONAL MATCH (file:File {{project: $project}})-[:DEFINES]->(t)
            WITH t, file
            WHERE $include_tests OR file.path IS NULL OR NOT file.path STARTS WITH 'src/test/'
            WITH t, collect(DISTINCT file.path) AS files
            ORDER BY t.fqn
            LIMIT $limit
            {member_count_cypher}
            RETURN labels(t) AS labels, t.fqn AS fqn, t.name AS name, t.kind AS kind,
                   {extra_type_cols}files{member_count_cols}
            """,
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
                f"""
                MATCH (t {{project: $project}})
                WHERE (t:Class OR t:Interface OR t:Annotation) AND {predicate}
                OPTIONAL MATCH (file:File {{project: $project}})-[:DEFINES]->(t)
                WITH t, file
                WHERE $include_tests OR file.path IS NULL OR NOT file.path STARTS WITH 'src/test/'
                RETURN count(DISTINCT t) AS count
                """,
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
                    f"""
                    MATCH (t {{project: $project, fqn: $fqn}})-[:DECLARES]->(m:Method)
                    WHERE (t:Class OR t:Interface OR t:Annotation)
                    RETURN {method_projection.strip()}
                    ORDER BY m.name, m.signature
                    LIMIT $limit
                    """,
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
                    f"""
                    MATCH (t {{project: $project, fqn: $fqn}})-[:DECLARES]->(field:Field)
                    WHERE (t:Class OR t:Interface OR t:Annotation)
                    RETURN {field_projection.strip()}
                    ORDER BY field.name
                    LIMIT $limit
                    """,
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
        rows = self.client.run(
            """
            MATCH (method:Method {project: $project})
            WHERE method.signature CONTAINS $fragment
            OPTIONAL MATCH (file:File {project: $project})-[:DEFINES]->(method)
            WITH method, file
            WHERE $include_tests OR file.path IS NULL OR NOT file.path STARTS WITH 'src/test/'
            WITH method, collect(DISTINCT file.path) AS files
            RETURN __RETURN_PROJECTION__
            ORDER BY __ORDER_BY__
            SKIP $skip
            LIMIT $limit
            """.replace("__RETURN_PROJECTION__", return_projection.strip()).replace(
                "__ORDER_BY__", "sortSignature" if compact else "signature"
            ),
            {
                "project": project_name,
                "fragment": signature_fragment,
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
                """
                MATCH (method:Method {project: $project})
                WHERE method.signature CONTAINS $fragment
                OPTIONAL MATCH (file:File {project: $project})-[:DEFINES]->(method)
                WITH method, file
                WHERE $include_tests OR file.path IS NULL OR NOT file.path STARTS WITH 'src/test/'
                RETURN count(DISTINCT method) AS count
                """,
                {
                    "project": project_name,
                    "fragment": signature_fragment,
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
            f"""
            MATCH (field:Field {{project: $project}})
            WHERE field.fqn CONTAINS $fragment OR field.name = $fragment
            OPTIONAL MATCH (owner {{project: $project}})-[:DECLARES]->(field)
            WHERE owner:Class OR owner:Interface OR owner:Annotation
            OPTIONAL MATCH (file:File {{project: $project}})-[:DEFINES]->(field)
            WITH field, owner, file
            WHERE $include_tests OR file.path IS NULL OR NOT file.path STARTS WITH 'src/test/'
            WITH field, owner, collect(DISTINCT file.path) AS files
            RETURN {projection.strip()}
            ORDER BY __ORDER_BY__
            SKIP $skip
            LIMIT $limit
            """.replace("__ORDER_BY__", "sortKey" if compact else "fqn"),
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
                """
                MATCH (field:Field {project: $project})
                WHERE field.fqn CONTAINS $fragment OR field.name = $fragment
                OPTIONAL MATCH (file:File {project: $project})-[:DEFINES]->(field)
                WITH field, file
                WHERE $include_tests OR file.path IS NULL OR NOT file.path STARTS WITH 'src/test/'
                RETURN count(DISTINCT field) AS count
                """,
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
            f"""
            MATCH (file:File {{project: $project}})
            WHERE file.path CONTAINS $fragment
              AND ($include_tests OR NOT file.path STARTS WITH 'src/test/')
            OPTIONAL MATCH (file)-[:DEFINES]->(definition {{project: $project}})
            WITH file, count(DISTINCT definition) AS definitionCount
            OPTIONAL MATCH (chunk:CodeChunk {{project: $project}})
            WHERE chunk.path = file.path
            WITH file, definitionCount, count(DISTINCT chunk) AS chunkCount
            RETURN {projection.strip()}
            ORDER BY file.path
            SKIP $skip
            LIMIT $limit
            """,
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
                """
                MATCH (file:File {project: $project})
                WHERE file.path CONTAINS $fragment
                  AND ($include_tests OR NOT file.path STARTS WITH 'src/test/')
                RETURN count(DISTINCT file) AS count
                """,
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
            "depth": depth_value,
            "include_tests": include_tests,
        }
        target_rows = self.client.run(
            """
            MATCH (target:Method {project: $project})
            WHERE target.signature CONTAINS $fragment
            OPTIONAL MATCH (file:File {project: $project})-[:DEFINES]->(target)
            WITH target, collect(DISTINCT file.path) AS files
            WHERE $include_tests
               OR files = []
               OR any(path IN files WHERE NOT path STARTS WITH 'src/test/')
            RETURN target.signature AS signature,
                   target.ownerDisplayName AS owner,
                   target.ownerFqn AS ownerFqn,
                   target.name AS name,
                   target.startLine AS startLine,
                   target.endLine AS endLine,
                   files
            ORDER BY signature
            LIMIT $limit
            """,
            params,
        )
        impact_rows = self.client.run(
            """
            MATCH (caller:Method {project: $project})-[:CALLS]->(target:Method {project: $project})
            WHERE target.signature CONTAINS $fragment
            OPTIONAL MATCH (callerFile:File {project: $project})-[:DEFINES]->(caller)
            OPTIONAL MATCH (targetFile:File {project: $project})-[:DEFINES]->(target)
            WITH caller, target, callerFile, targetFile
            WHERE $include_tests
               OR ((callerFile.path IS NULL OR NOT callerFile.path STARTS WITH 'src/test/')
               AND (targetFile.path IS NULL OR NOT targetFile.path STARTS WITH 'src/test/'))
            RETURN DISTINCT 1 AS depth,
                   caller.signature AS callerSignature,
                   caller.ownerDisplayName AS callerOwner,
                   caller.ownerFqn AS callerOwnerFqn,
                   caller.name AS callerName,
                   caller.startLine AS callerStartLine,
                   caller.endLine AS callerEndLine,
                   callerFile.path AS callerPath,
                   null AS viaSignature,
                   null AS viaOwner,
                   null AS viaOwnerFqn,
                   null AS viaName,
                   null AS viaPath,
                   target.signature AS targetSignature,
                   target.ownerDisplayName AS targetOwner,
                   target.ownerFqn AS targetOwnerFqn,
                   target.name AS targetName,
                   targetFile.path AS targetPath
            UNION ALL
            MATCH (caller:Method {project: $project})
              -[:CALLS]->(via:Method {project: $project})
              -[:CALLS]->(target:Method {project: $project})
            WHERE $depth >= 2 AND target.signature CONTAINS $fragment
            OPTIONAL MATCH (callerFile:File {project: $project})-[:DEFINES]->(caller)
            OPTIONAL MATCH (viaFile:File {project: $project})-[:DEFINES]->(via)
            OPTIONAL MATCH (targetFile:File {project: $project})-[:DEFINES]->(target)
            WITH caller, via, target, callerFile, viaFile, targetFile
            WHERE $include_tests
               OR ((callerFile.path IS NULL OR NOT callerFile.path STARTS WITH 'src/test/')
               AND (viaFile.path IS NULL OR NOT viaFile.path STARTS WITH 'src/test/')
               AND (targetFile.path IS NULL OR NOT targetFile.path STARTS WITH 'src/test/'))
            RETURN DISTINCT 2 AS depth,
                   caller.signature AS callerSignature,
                   caller.ownerDisplayName AS callerOwner,
                   caller.ownerFqn AS callerOwnerFqn,
                   caller.name AS callerName,
                   caller.startLine AS callerStartLine,
                   caller.endLine AS callerEndLine,
                   callerFile.path AS callerPath,
                   via.signature AS viaSignature,
                   via.ownerDisplayName AS viaOwner,
                   via.ownerFqn AS viaOwnerFqn,
                   via.name AS viaName,
                   viaFile.path AS viaPath,
                   target.signature AS targetSignature,
                   target.ownerDisplayName AS targetOwner,
                   target.ownerFqn AS targetOwnerFqn,
                   target.name AS targetName,
                   targetFile.path AS targetPath
            ORDER BY depth, callerSignature, viaSignature, targetSignature
            SKIP $skip
            LIMIT $limit
            """,
            params,
        )
        count_rows = self.client.run(
            """
            CALL {
              MATCH (caller:Method {project: $project})
                -[:CALLS]->(target:Method {project: $project})
              WHERE target.signature CONTAINS $fragment
              OPTIONAL MATCH (callerFile:File {project: $project})-[:DEFINES]->(caller)
              OPTIONAL MATCH (targetFile:File {project: $project})-[:DEFINES]->(target)
              WITH callerFile, targetFile
              WHERE $include_tests
                 OR ((callerFile.path IS NULL OR NOT callerFile.path STARTS WITH 'src/test/')
                 AND (targetFile.path IS NULL OR NOT targetFile.path STARTS WITH 'src/test/'))
              RETURN 1 AS hit
              UNION ALL
              MATCH (caller:Method {project: $project})
                -[:CALLS]->(via:Method {project: $project})
                -[:CALLS]->(target:Method {project: $project})
              WHERE $depth >= 2 AND target.signature CONTAINS $fragment
              OPTIONAL MATCH (callerFile:File {project: $project})-[:DEFINES]->(caller)
              OPTIONAL MATCH (viaFile:File {project: $project})-[:DEFINES]->(via)
              OPTIONAL MATCH (targetFile:File {project: $project})-[:DEFINES]->(target)
              WITH callerFile, viaFile, targetFile
              WHERE $include_tests
                 OR ((callerFile.path IS NULL OR NOT callerFile.path STARTS WITH 'src/test/')
                 AND (viaFile.path IS NULL OR NOT viaFile.path STARTS WITH 'src/test/')
                 AND (targetFile.path IS NULL OR NOT targetFile.path STARTS WITH 'src/test/'))
              RETURN 1 AS hit
            }
            RETURN count(hit) AS count
            """,
            params,
        )
        total_count = count_rows[0].get("count", 0) if count_rows else len(impact_rows)

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
                total_count=total_count,
            ),
            output_format,
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
            """
            MATCH (caller:Method {project: $project})-[:CALLS]->(callee:Method {project: $project})
            WHERE callee.signature CONTAINS $fragment
            OPTIONAL MATCH (callerFile:File {project: $project})-[:DEFINES]->(caller)
            OPTIONAL MATCH (calleeFile:File {project: $project})-[:DEFINES]->(callee)
            WITH caller, callee, callerFile, calleeFile
            WHERE $include_tests
               OR ((callerFile.path IS NULL OR NOT callerFile.path STARTS WITH 'src/test/')
               AND (calleeFile.path IS NULL OR NOT calleeFile.path STARTS WITH 'src/test/'))
            RETURN caller.signature AS callerSignature,
                   caller.name AS callerName,
                   caller.ownerDisplayName AS callerOwner,
                   caller.startLine AS callerStartLine,
                   caller.endLine AS callerEndLine,
                   callee.signature AS calleeSignature,
                   callee.name AS calleeName,
                   callee.ownerDisplayName AS calleeOwner,
                   callerFile.path AS callerPath
            ORDER BY caller.signature, callee.signature
            SKIP $skip
            LIMIT $limit
            """,
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
                """
                MATCH (caller:Method {project: $project})
                  -[:CALLS]->(callee:Method {project: $project})
                WHERE callee.signature CONTAINS $fragment
                OPTIONAL MATCH (callerFile:File {project: $project})-[:DEFINES]->(caller)
                OPTIONAL MATCH (calleeFile:File {project: $project})-[:DEFINES]->(callee)
                WITH caller, callee, callerFile, calleeFile
                WHERE $include_tests
                   OR ((callerFile.path IS NULL OR NOT callerFile.path STARTS WITH 'src/test/')
                   AND (calleeFile.path IS NULL OR NOT calleeFile.path STARTS WITH 'src/test/'))
                RETURN count(*) AS count
                """,
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
            """
            MATCH (caller:Method {project: $project})-[:CALLS]->(callee:Method {project: $project})
            WHERE caller.signature CONTAINS $fragment
            OPTIONAL MATCH (calleeFile:File {project: $project})-[:DEFINES]->(callee)
            OPTIONAL MATCH (callerFile:File {project: $project})-[:DEFINES]->(caller)
            WITH caller, callee, callerFile, calleeFile
            WHERE $include_tests
               OR ((callerFile.path IS NULL OR NOT callerFile.path STARTS WITH 'src/test/')
               AND (calleeFile.path IS NULL OR NOT calleeFile.path STARTS WITH 'src/test/'))
            RETURN caller.signature AS callerSignature,
                   caller.name AS callerName,
                   caller.ownerDisplayName AS callerOwner,
                   callee.signature AS calleeSignature,
                   callee.name AS calleeName,
                   callee.ownerDisplayName AS calleeOwner,
                   callee.startLine AS calleeStartLine,
                   callee.endLine AS calleeEndLine,
                   calleeFile.path AS calleePath
            ORDER BY caller.signature, callee.signature
            SKIP $skip
            LIMIT $limit
            """,
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
                """
                MATCH (caller:Method {project: $project})
                  -[:CALLS]->(callee:Method {project: $project})
                WHERE caller.signature CONTAINS $fragment
                OPTIONAL MATCH (callerFile:File {project: $project})-[:DEFINES]->(caller)
                OPTIONAL MATCH (calleeFile:File {project: $project})-[:DEFINES]->(callee)
                WITH caller, callee, callerFile, calleeFile
                WHERE $include_tests
                   OR ((callerFile.path IS NULL OR NOT callerFile.path STARTS WITH 'src/test/')
                   AND (calleeFile.path IS NULL OR NOT calleeFile.path STARTS WITH 'src/test/'))
                RETURN count(*) AS count
                """,
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
            self.client.run(
                """
                MATCH (file:File {project: $project})-[:DEFINES]->(type {project: $project})
                WHERE ($include_tests OR NOT file.path STARTS WITH 'src/test/')
                  AND (type:Class OR type:Interface OR type:Annotation)
                OPTIONAL MATCH (type)-[:DECLARES]->(method:Method {project: $project})
                WITH file, type, count(DISTINCT method) AS methods
                RETURN 'type' AS kind, labels(type)[0] AS owner, type.name AS name,
                       methods AS score, file.path AS path, null AS startLine,
                       null AS endLine, type.fqn AS sortKey
                ORDER BY score DESC, sortKey
                LIMIT $limit
                """,
                params,
            )
            if "largestTypes" in requested_sections
            else []
        )
        longest_methods = (
            self.client.run(
                """
                MATCH (file:File {project: $project})
                  -[:DEFINES]->(method:Method {project: $project})
                WHERE ($include_tests OR NOT file.path STARTS WITH 'src/test/')
                  AND method.startLine IS NOT NULL AND method.endLine IS NOT NULL
                  AND coalesce(method.isSynthetic, false) = false
                WITH file, method, method.endLine - method.startLine + 1 AS lines
                RETURN 'method' AS kind, method.ownerDisplayName AS owner, method.name AS name,
                       lines AS score, file.path AS path,
                       method.startLine AS startLine, method.endLine AS endLine,
                       method.signature AS sortKey
                ORDER BY score DESC, sortKey
                LIMIT $limit
                """,
                params,
            )
            if "longestMethods" in requested_sections
            else []
        )
        fan_in = (
            self.client.run(
                """
                MATCH (caller:Method {project: $project})
                  -[call:CALLS]->(method:Method {project: $project})
                OPTIONAL MATCH (file:File {project: $project})-[:DEFINES]->(method)
                WITH method, file, count(call) AS callers
                WHERE $include_tests OR file.path IS NULL OR NOT file.path STARTS WITH 'src/test/'
                RETURN 'fanIn' AS kind, method.ownerDisplayName AS owner, method.name AS name,
                       callers AS score, file.path AS path,
                       method.startLine AS startLine, method.endLine AS endLine,
                       method.signature AS sortKey
                ORDER BY score DESC, sortKey
                LIMIT $limit
                """,
                params,
            )
            if "fanIn" in requested_sections
            else []
        )
        fan_out = (
            self.client.run(
                """
                MATCH (method:Method {project: $project})
                  -[call:CALLS]->(:Method {project: $project})
                OPTIONAL MATCH (file:File {project: $project})-[:DEFINES]->(method)
                WITH method, file, count(call) AS callees
                WHERE $include_tests OR file.path IS NULL OR NOT file.path STARTS WITH 'src/test/'
                RETURN 'fanOut' AS kind, method.ownerDisplayName AS owner, method.name AS name,
                       callees AS score, file.path AS path,
                       method.startLine AS startLine, method.endLine AS endLine,
                       method.signature AS sortKey
                ORDER BY score DESC, sortKey
                LIMIT $limit
                """,
                params,
            )
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
        fragments = _normalize_lower_list(sink_fragments) or sorted(DEFAULT_OPERATION_SINKS)
        owner_filter = (owner_fragment or "").strip().lower()
        path_filter = (path_contains or "").strip()
        rows = self.client.run(
            """
            MATCH (caller:Method {project: $project})
              -[call:CALLS]->(sink:Method {project: $project})
            OPTIONAL MATCH (file:File {project: $project})-[:DEFINES]->(caller)
            WITH caller, sink, file, call,
                 toLower(coalesce(sink.name, '') + ' ' + coalesce(sink.signature, '')) AS sinkText,
                 toLower(coalesce(caller.ownerDisplayName, '') + ' '
                         + coalesce(caller.ownerFqn, '') + ' '
                         + coalesce(caller.signature, '')) AS callerText
            WHERE ($include_tests OR file.path IS NULL OR NOT file.path STARTS WITH 'src/test/')
              AND any(fragment IN $fragments WHERE sinkText CONTAINS fragment)
              AND ($owner_fragment = '' OR callerText CONTAINS $owner_fragment)
              AND ($path_contains = '' OR file.path CONTAINS $path_contains)
            WITH caller, file,
                 count(call) AS sinkCallEdges,
                 count(DISTINCT sink) AS distinctSinks,
                 collect(DISTINCT coalesce(sink.ownerDisplayName, sink.ownerFqn, '')
                                  + '.' + coalesce(sink.name, ''))[..6] AS sinks,
                 CASE
                   WHEN caller.startLine IS NOT NULL AND caller.endLine IS NOT NULL
                   THEN caller.endLine - caller.startLine + 1
                   ELSE 0
                 END AS lines
            RETURN caller.ownerDisplayName AS owner,
                   caller.name AS name,
                   caller.signature AS signature,
                   file.path AS path,
                   caller.startLine AS startLine,
                   caller.endLine AS endLine,
                   lines,
                   sinkCallEdges,
                   distinctSinks,
                   sinks,
                   (sinkCallEdges * 1000 + lines) AS score
            ORDER BY score DESC, signature
            LIMIT $limit
            """,
            {
                "project": project_name,
                "fragments": fragments,
                "owner_fragment": owner_filter,
                "path_contains": path_filter,
                "include_tests": include_tests,
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
            extra = {k: v for k, v in (
                ("ownerFragment", owner_filter),
                ("pathContains", path_filter),
            ) if v}
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
            """
            MATCH (chunk:CodeChunk {project: $project})
            WHERE chunk.sourceLabel = 'File'
              AND coalesce(chunk.ragRole, 'file') = 'file'
              AND ($include_tests OR chunk.path IS NULL OR NOT chunk.path STARTS WITH 'src/test/')
              AND ($path_contains = '' OR chunk.path CONTAINS $path_contains)
            RETURN chunk.path AS path,
                   chunk.language AS language,
                   chunk.text AS text
            ORDER BY chunk.path
            LIMIT $limit
            """,
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
        rows = self.client.run(
            """
            CALL {
              MATCH (n {project: $project})
              WITH labels(n) AS labels, count(n) AS count
              ORDER BY count DESC
              RETURN collect({labels: labels, count: count}) AS inventory
            }
            CALL {
              MATCH (file:File {project: $project})
                -[:DEFINES]->(method:Method {project: $project})
              WHERE ($include_tests OR NOT file.path STARTS WITH 'src/test/')
                AND method.startLine IS NOT NULL AND method.endLine IS NOT NULL
                AND coalesce(method.isSynthetic, false) = false
              WITH method.endLine - method.startLine + 1 AS lines
              RETURN {
                methods: count(lines),
                avgLines: round(avg(lines) * 100) / 100,
                maxLines: max(lines),
                methods50Plus: sum(CASE WHEN lines >= 50 THEN 1 ELSE 0 END),
                methods100Plus: sum(CASE WHEN lines >= 100 THEN 1 ELSE 0 END)
              } AS methodLengths
            }
            CALL {
              MATCH (method:Method {project: $project})
              OPTIONAL MATCH (method)-[call:CALLS]->(:Method {project: $project})
              WITH method, count(call) AS degree
              RETURN {
                methods: count(method),
                avgOut: round(avg(degree) * 100) / 100,
                maxOut: max(degree),
                methodsOut10Plus: sum(CASE WHEN degree >= 10 THEN 1 ELSE 0 END),
                methodsOut0: sum(CASE WHEN degree = 0 THEN 1 ELSE 0 END)
              } AS fanOut
            }
            CALL {
              MATCH (method:Method {project: $project})
              OPTIONAL MATCH (:Method {project: $project})-[call:CALLS]->(method)
              WITH method, count(call) AS degree
              RETURN {
                methods: count(method),
                avgIn: round(avg(degree) * 100) / 100,
                maxIn: max(degree),
                methodsIn10Plus: sum(CASE WHEN degree >= 10 THEN 1 ELSE 0 END),
                methodsIn0: sum(CASE WHEN degree = 0 THEN 1 ELSE 0 END)
              } AS fanIn
            }
            CALL {
              MATCH (type {project: $project})
              WHERE type:Class OR type:Interface OR type:Annotation
              OPTIONAL MATCH (type)-[:DECLARES]->(method:Method {project: $project})
              WITH type, count(method) AS methods
              RETURN {
                types: count(type),
                avgMethodsPerType: round(avg(methods) * 100) / 100,
                maxMethodsPerType: max(methods),
                types25MethodsPlus: sum(CASE WHEN methods >= 25 THEN 1 ELSE 0 END),
                types50MethodsPlus: sum(CASE WHEN methods >= 50 THEN 1 ELSE 0 END)
              } AS typeSizes
            }
            CALL {
              MATCH (chunk:CodeChunk {project: $project})
              WITH chunk.sourceLabel AS sourceLabel, count(chunk) AS chunks
              ORDER BY chunks DESC, sourceLabel
              RETURN collect({sourceLabel: sourceLabel, chunks: chunks}) AS chunksByLabel
            }
            CALL {
              MATCH (file:File {project: $project})
                -[:DEFINES]->(method:Method {project: $project})
              WHERE $include_tests OR NOT file.path STARTS WITH 'src/test/'
              WITH file.path AS path, count(method) AS methods
              ORDER BY methods DESC, path
              LIMIT $limit
              RETURN collect({path: path, methods: methods}) AS filesByMethods
            }
            RETURN inventory, methodLengths, fanOut, fanIn, typeSizes,
                   chunksByLabel, filesByMethods
            """,
            params,
        )
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

    def code_hierarchy(self, fqn: str, project: str | None = None) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        class_hierarchy = self.client.run(
            """
            MATCH (c:Class {fqn: $fqn, project: $project})
            OPTIONAL MATCH (c)-[:EXTENDS]->(parent:Class {project: $project})
            OPTIONAL MATCH (c)-[:IMPLEMENTS]->(iface:Interface {project: $project})
            OPTIONAL MATCH (child:Class {project: $project})-[:EXTENDS]->(c)
            WITH c.fqn AS classFqn, collect(DISTINCT parent.fqn) AS parents,
                 collect(DISTINCT iface.fqn) AS interfaces, collect(DISTINCT child.fqn) AS children
            RETURN classFqn, parents, interfaces, children
            """,
            {"project": project_name, "fqn": fqn},
        )
        ancestors = self.client.run(
            """
            MATCH path =
              (c:Class {fqn: $fqn, project: $project})
              -[:EXTENDS*]->(a:Class {project: $project})
            RETURN [node IN nodes(path) | node.fqn] AS ancestors
            ORDER BY size(ancestors)
            """,
            {"project": project_name, "fqn": fqn},
        )
        implementors = self.client.run(
            """
            MATCH (impl:Class {project: $project})-[:EXTENDS*0..]->(:Class {project: $project})
                  -[:IMPLEMENTS]->(:Interface {project: $project})
                  -[:EXTENDS*0..]->(i:Interface {fqn: $fqn, project: $project})
            RETURN DISTINCT impl.fqn AS implementor
            ORDER BY implementor
            """,
            {"project": project_name, "fqn": fqn},
        )
        return {
            "project": project_name,
            "classHierarchy": class_hierarchy,
            "ancestors": ancestors,
            "interfaceImplementors": implementors,
        }

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
            """
            MATCH (test:Method {project: $project})
            OPTIONAL MATCH (file:File {project: $project})-[:DEFINES]->(test)
            WITH test, file,
                 toLower(coalesce(test.signature, '') + ' ' + coalesce(test.name, '') + ' '
                         + coalesce(file.path, '')) AS haystack
            WITH test, file, haystack,
                 size([term IN $terms WHERE haystack CONTAINS term]) AS termMatches
            WHERE file.path STARTS WITH 'src/test/'
              AND (test.signature CONTAINS $fragment
                OR test.name CONTAINS $fragment
                OR file.path CONTAINS $fragment
                OR ($owner_fragment <> ''
                    AND (test.ownerDisplayName CONTAINS $owner_fragment
                      OR test.signature CONTAINS $owner_fragment
                      OR file.path CONTAINS $owner_fragment))
                OR termMatches >= $min_term_matches)
            RETURN test.ownerDisplayName AS owner,
                   test.name AS name,
                   file.path AS path,
                   test.startLine AS startLine,
                   test.endLine AS endLine,
                   CASE WHEN test.signature CONTAINS $fragment OR test.name = $fragment
                        THEN true ELSE false END AS exactish,
                   termMatches
            ORDER BY exactish DESC, termMatches DESC, path, startLine, name
            LIMIT $limit
            """,
            {
                "project": project_name,
                "fragment": test_fragment,
                "owner_fragment": owner_fragment,
                "terms": terms,
                "min_term_matches": min_term_matches,
                "limit": bounded_limit,
            },
        )
        production_rows = self.client.run(
            """
            MATCH (test:Method {project: $project})-[:CALLS]->(callee:Method {project: $project})
            OPTIONAL MATCH (testFile:File {project: $project})-[:DEFINES]->(test)
            OPTIONAL MATCH (calleeFile:File {project: $project})-[:DEFINES]->(callee)
            WITH test, callee, testFile, calleeFile,
                 toLower(coalesce(test.signature, '') + ' ' + coalesce(test.name, '') + ' '
                         + coalesce(testFile.path, '')) AS haystack
            WITH test, callee, testFile, calleeFile, haystack,
                 size([term IN $terms WHERE haystack CONTAINS term]) AS termMatches
            WHERE testFile.path STARTS WITH 'src/test/'
              AND NOT calleeFile.path STARTS WITH 'src/test/'
              AND (test.signature CONTAINS $fragment
                OR test.name CONTAINS $fragment
                OR testFile.path CONTAINS $fragment
                OR ($owner_fragment <> ''
                    AND (test.ownerDisplayName CONTAINS $owner_fragment
                      OR test.signature CONTAINS $owner_fragment
                      OR testFile.path CONTAINS $owner_fragment))
                OR termMatches >= $min_term_matches)
            RETURN DISTINCT callee.ownerDisplayName AS owner,
                   callee.name AS name,
                   calleeFile.path AS path,
                   callee.startLine AS startLine,
                   callee.endLine AS endLine,
                   test.ownerDisplayName AS testOwner,
                   test.name AS testName
            ORDER BY path, startLine, name
            LIMIT $limit
            """,
            {
                "project": project_name,
                "fragment": test_fragment,
                "owner_fragment": owner_fragment,
                "terms": terms,
                "min_term_matches": min_term_matches,
                "limit": bounded_production_limit,
            },
        )
        file_rows = self.client.run(
            """
            MATCH (file:File {project: $project})
            WITH file, toLower(file.path) AS haystack
            WHERE file.path STARTS WITH 'src/test/'
              AND (file.path CONTAINS $fragment
                OR ($owner_fragment <> '' AND file.path CONTAINS $owner_fragment)
                OR size([term IN $terms WHERE haystack CONTAINS term]) >= $min_term_matches)
            RETURN file.path AS path, file.language AS language
            ORDER BY file.path
            LIMIT $limit
            """,
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
        for row in rows:
            row.pop("signature", None)
            row.pop("exactish", None)
            row.pop("termMatches", None)
        production_rows = [r for r in production_rows if r.get("name") != "<init>"]
        for row in production_rows:
            row.pop("signature", None)
        return self._finalize_response(
            {
                "project": project_name,
                "tests": rows,
                "productionCallees": production_rows,
                "testFiles": file_rows,
                "meta": {"exactMatches": exact_matches},
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
                    """
                MATCH (m:Memory {project: $project})-[:HAS_RULE]->(rule:Rule)
                RETURN __RETURN_PROJECTION__
                ORDER BY rule.severity, rule.id
                """.replace("__RETURN_PROJECTION__", rule_projection),
                    {"project": project_name},
                ),
                "openFindings": self.client.run(
                    """
                MATCH (m:Memory {project: $project})-[:HAS_FINDING]->(finding:Finding)
                WHERE finding.status = 'open'
                RETURN __RETURN_PROJECTION__
                ORDER BY finding.id
                """.replace("__RETURN_PROJECTION__", finding_projection),
                    {"project": project_name},
                ),
                "activeTasks": self.client.run(
                    """
                MATCH (m:Memory {project: $project})-[:HAS_TASK]->(task:Task)
                WHERE task.status IN ['todo', 'doing', 'blocked']
                RETURN __RETURN_PROJECTION__
                ORDER BY task.priority, task.status, task.id
                """.replace("__RETURN_PROJECTION__", task_projection),
                    {"project": project_name},
                ),
                "openQuestions": self.client.run(
                    """
                MATCH (m:Memory {project: $project})-[:HAS_QUESTION]->(question:Question)
                WHERE question.status = 'open'
                RETURN question.id AS id, question.title AS title
                ORDER BY question.id
                """,
                    {"project": project_name},
                ),
                "openRisks": self.client.run(
                    """
                MATCH (m:Memory {project: $project})-[:HAS_RISK]->(risk:Risk)
                WHERE risk.status = 'open'
                RETURN __RETURN_PROJECTION__
                ORDER BY risk.severity, risk.id
                """.replace("__RETURN_PROJECTION__", risk_projection),
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
            """
            CALL embeddings.text([$query], {}) YIELD embeddings
            WITH embeddings[0] AS queryVector
            CALL vector_search.search($index, $limit, queryVector)
            YIELD node AS chunk, similarity
            WITH chunk, similarity
            WHERE chunk.project = $project
            MATCH (memory {project: $project})-[:HAS_RAG_CHUNK]->(chunk)
            RETURN labels(memory) AS type, memory.id AS id, memory.title AS title,
                   memory.status AS status, chunk.sourceLabel AS sourceLabel,
                   chunk.sourceId AS sourceId, similarity
            ORDER BY similarity DESC
            """,
            {
                "index": index_name,
                "project": project_name,
                "query": query,
                "limit": _bounded_limit(limit, default=5, maximum=20),
            },
        )
        return {"project": project_name, "query": query, "hits": rows}

    def memory_get(
        self,
        memory_id: str,
        project: str | None = None,
        *,
        finalize: bool = True,
    ) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        rows = self.client.run(
            f"""
            MATCH (memory {{project: $project, id: $memory_id}})
            WHERE {_memory_label_predicate("memory")}
            OPTIONAL MATCH (memory)-[:REFERS_TO]->(ref:CodeRef)-[:RESOLVES_TO]->(target)
            WITH memory, collect(
                CASE WHEN ref IS NULL THEN NULL ELSE {{
                    targetType: ref.targetType,
                    key: ref.key,
                    targetLabels: labels(target)
                }} END
            ) AS refs
            RETURN labels(memory) AS labels, properties(memory) AS properties,
                   [ref IN refs WHERE ref IS NOT NULL] AS codeRefs
            """,
            {"project": project_name, "memory_id": memory_id},
        )
        response = {"project": project_name, "memory": rows[0] if rows else None}
        return self._finalize_response(response) if finalize else response

    def delete_memory(self, memory_id: str, project: str | None = None) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        rows = self.client.run(
            f"""
            MATCH (memory {{project: $project, id: $memory_id}})
            WHERE {_memory_label_predicate("memory")}
            OPTIONAL MATCH (memory)-[:HAS_RAG_CHUNK]->(chunk:MemoryChunk {{project: $project}})
            OPTIONAL MATCH (memory)-[:REFERS_TO]->(ref:CodeRef {{project: $project}})
            RETURN labels(memory) AS labels, properties(memory) AS properties,
                   [id IN collect(DISTINCT chunk.id) WHERE id IS NOT NULL] AS chunkIds,
                   [codeRef IN collect(DISTINCT CASE WHEN ref IS NULL THEN NULL ELSE {{
                       targetType: ref.targetType,
                       key: ref.key
                   }} END) WHERE codeRef IS NOT NULL] AS codeRefs
            """,
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
            f"""
            MATCH (memory {{project: $project, id: $memory_id}})
            WHERE {_memory_label_predicate("memory")}
            OPTIONAL MATCH (memory)-[:HAS_RAG_CHUNK]->(chunk:MemoryChunk {{project: $project}})
            WITH memory, [chunk IN collect(DISTINCT chunk) WHERE chunk IS NOT NULL] AS chunks
            FOREACH (chunk IN chunks | DETACH DELETE chunk)
            DETACH DELETE memory
            RETURN true AS deleted
            """,
            {"project": project_name, "memory_id": memory_id},
            write=True,
        )

        orphan_deleted = 0
        if code_refs:
            orphan_rows = self.client.run(
                """
                MATCH (ref:CodeRef {project: $project})
                WHERE any(codeRef IN $code_refs
                          WHERE codeRef.targetType = ref.targetType AND codeRef.key = ref.key)
                  AND NOT (()-[:REFERS_TO]->(ref))
                WITH collect(ref) AS refs
                FOREACH (ref IN refs | DETACH DELETE ref)
                RETURN size(refs) AS deleted
                """,
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
            f"""
            MERGE (root:Memory {{project: $project}})
            MERGE (node:{spec.label} {{id: $memory_id, project: $project}})
            SET node += $properties,
                node.createdAt = coalesce(node.createdAt, datetime()),
                node.updatedAt = datetime()
            MERGE (root)-[:{spec.relation}]->(node)
            RETURN labels(node) AS labels, properties(node) AS properties
            """,
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
            f"""
            MATCH (node:{spec.label} {{id: $memory_id, project: $project}})
            SET node.status = $status, node.updatedAt = datetime()
            RETURN labels(node) AS labels, properties(node) AS properties
            """,
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
            f"""
            MATCH (node:{spec.label} {{id: $memory_id, project: $project}})
            MATCH (target:{target_label} {{project: $project}})
            WHERE {predicate}
            MERGE (ref:CodeRef {{project: $project, targetType: $target_type, key: $target_key}})
            MERGE (node)-[:REFERS_TO]->(ref)
            MERGE (ref)-[:RESOLVES_TO]->(target)
            RETURN node.id AS memoryId, ref.targetType AS targetType, ref.key AS key,
                   labels(target) AS targetLabels
            """,
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
        return {
            "project": project_name,
            "resolved": bool(rows),
            "links": rows,
            "chunk": chunk_result,
        }

    def memory_refresh_chunk(
        self,
        memory_type: str,
        memory_id: str,
        project: str | None = None,
        *,
        embed: bool = True,
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
            f"""
            MATCH (node:{spec.label} {{id: $memory_id, project: $project}})
            MERGE (chunk:MemoryChunk {{id: $chunk_id, project: $project}})
            SET chunk.sourceLabel = $memory_type,
                chunk.sourceId = node.id,
                chunk.text = $text,
                chunk.textHash = $text_hash,
                chunk.createdAt = coalesce(chunk.createdAt, datetime()),
                chunk.updatedAt = datetime(),
                chunk.embeddingDirty = true
            REMOVE chunk.embedding, chunk.embeddingModel, chunk.embeddingDimensions
            MERGE (node)-[:HAS_RAG_CHUNK]->(chunk)
            RETURN chunk.id AS id, chunk.textHash AS textHash, chunk.embeddingDirty AS dirty
            """,
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
            embedding_result = self.memory_refresh_embeddings([chunk_id], project_name)
            if rows and chunk_id in set(embedding_result.get("embedded", [])):
                rows[0]["dirty"] = False
        return {
            "project": project_name,
            "chunk": rows[0] if rows else None,
            "embedding": embedding_result,
        }

    def memory_refresh_embeddings(
        self,
        chunk_ids: Sequence[str],
        project: str | None = None,
    ) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        ids = [chunk_id for chunk_id in dict.fromkeys(chunk_ids) if chunk_id]
        if not ids:
            return {"project": project_name, "embedded": []}

        pending = self.client.run(
            """
            MATCH (chunk:MemoryChunk {project: $project})
            WHERE chunk.id IN $ids
              AND chunk.text IS NOT NULL
              AND (chunk.embedding IS NULL
                OR chunk.embeddingModel IS NULL
                OR chunk.embeddingModel <> $model_name
                OR chunk.embeddingDimensions IS NULL
                OR chunk.embeddingDimensions <> $dimension
                OR coalesce(chunk.embeddingDirty, false) = true)
            RETURN chunk.id AS id
            ORDER BY chunk.id
            """,
            {
                "project": project_name,
                "ids": ids,
                "model_name": self.config.embedding_model_name,
                "dimension": self.config.embedding_dimensions,
            },
        )
        pending_ids = [row["id"] for row in pending]
        if not pending_ids:
            return {"project": project_name, "embedded": []}

        result = self.client.run(
            """
            MATCH (chunk:MemoryChunk {project: $project})
            WHERE chunk.id IN $ids
            WITH chunk
            ORDER BY chunk.id
            WITH collect(chunk) AS chunks
            WITH chunks, [chunk IN chunks | chunk.id] AS embeddedIds
            CALL embeddings.node_sentence(chunks, {})
            YIELD success, dimension
            RETURN success AS success, dimension AS dimension, embeddedIds AS ids
            """,
            {"project": project_name, "ids": pending_ids},
            write=True,
        )
        dimension = result[0].get("dimension") if result else self.config.embedding_dimensions
        self.client.run(
            """
            MATCH (chunk:MemoryChunk {project: $project})
            WHERE chunk.id IN $ids
            SET chunk.embeddingModel = $model_name,
                chunk.embeddingDimensions = $dimension,
                chunk.embeddingDirty = false,
                chunk.updatedAt = datetime()
            RETURN chunk.id AS id
            """,
            {
                "project": project_name,
                "ids": pending_ids,
                "model_name": self.config.embedding_model_name,
                "dimension": dimension,
            },
            write=True,
        )
        return {"project": project_name, "embedded": pending_ids, "result": result}

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
