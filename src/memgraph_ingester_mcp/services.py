"""High-level Memgraph Ingester operations exposed as MCP tools."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from hashlib import sha256
from json import dumps
from typing import Any

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


def _json_size(value: Mapping[str, Any]) -> int:
    return len(dumps(value, default=str, separators=(",", ":"), sort_keys=True))


def _normalize_output_format(output_format: str | None) -> str:
    if output_format is None:
        return "json"
    normalized = output_format.strip()
    if normalized not in OUTPUT_FORMATS:
        allowed = ", ".join(sorted(OUTPUT_FORMATS))
        raise MemgraphError(f"Unsupported format {output_format!r}. Allowed: {allowed}.")
    return normalized


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
        return {
            "cols": columns,
            "rows": [[_to_table_json(row.get(column)) for column in columns] for row in value],
        }
    return value


def _format_response(
    response: dict[str, Any],
    output_format: str | None = "json",
) -> dict[str, Any]:
    normalized = _normalize_output_format(output_format)
    if normalized == "json":
        return response

    formatted = _to_table_json(response)
    if not isinstance(formatted, dict):  # pragma: no cover - response is always a dict today.
        raise MemgraphError("Formatted response must be an object.")

    meta = formatted.setdefault("meta", {})
    if isinstance(meta, dict):
        meta["format"] = normalized
        meta.pop("resultChars", None)
        meta["resultChars"] = _json_size(formatted)
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
    meta: dict[str, Any] = {
        "totalCount": total,
        "returnedCount": returned_count,
        "skip": skip,
        "limit": limit,
        "hasMore": next_skip < total,
        "nextSkip": next_skip if next_skip < total else None,
    }
    if extra:
        meta.update(extra)
    response["meta"] = meta
    meta["resultChars"] = _json_size(response)
    return response


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


class MemgraphIngesterTools:
    """Safe operations that cover the generated Memgraph instruction templates."""

    def __init__(self, client: MemgraphClient, config: MemgraphConfig) -> None:
        self.client = client
        self.config = config

    def resolve_project(self, project: str | None) -> str:
        resolved = project or self.config.default_project
        if resolved is None or resolved.strip() == "":
            raise MemgraphError(
                "Project is required. Pass project or set MEMGRAPH_INGESTER_MCP_PROJECT."
            )
        return resolved

    def server_status(self, project: str | None = None) -> dict[str, Any]:
        project_name = self.resolve_project(project)
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
        indexes = self.client.run("SHOW VECTOR INDEX INFO")
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
        response["meta"] = {"resultChars": _json_size(response)}
        return response

    def code_search(
        self,
        query: str,
        project: str | None = None,
        limit: int = 10,
        include_text: bool = False,
        text_limit: int = 160,
        dedupe_by_source: bool = True,
        output_format: str = "json",
    ) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        bounded_limit = _bounded_limit(limit, default=10, maximum=25)
        bounded_text_limit = _bounded_text_limit(text_limit)
        fetch_limit = min(bounded_limit * 3, 75) if dedupe_by_source else bounded_limit
        return_projection = (
            """
                   labels(source) AS sourceType, chunk.sourceId AS sourceId,
                   chunk.path AS path, chunk.ownerFqn AS ownerFqn, chunk.signature AS signature,
                   similarity, chunk.text AS text
            """
            if include_text
            else """
                   labels(source) AS sourceType, chunk.sourceId AS sourceId,
                   chunk.path AS path, chunk.ownerFqn AS ownerFqn, chunk.signature AS signature,
                   similarity
            """
        )
        search_query = """
            CALL embeddings.text([$query], {}) YIELD embeddings
            WITH embeddings[0] AS queryVector
            CALL vector_search.search('code_chunk_embedding_v1', $limit, queryVector)
            YIELD node AS chunk, similarity
            WITH chunk, similarity
            WHERE chunk.project = $project
            MATCH (source {project: $project})-[:HAS_RAG_CHUNK]->(chunk)
            RETURN __RETURN_PROJECTION__
            ORDER BY similarity DESC
            """.replace("__RETURN_PROJECTION__", return_projection.strip())
        rows = self.client.run(
            search_query,
            {"project": project_name, "query": query, "limit": fetch_limit},
        )
        if dedupe_by_source:
            deduped: list[dict[str, Any]] = []
            seen: set[tuple[str, str]] = set()
            for row in rows:
                source_type = ",".join(row.get("sourceType") or [])
                key = (source_type, row.get("sourceId") or "")
                if key in seen:
                    continue
                seen.add(key)
                deduped.append(row)
                if len(deduped) >= bounded_limit:
                    break
            rows = deduped
        for row in rows:
            if include_text:
                row["text"] = _compact_text(row.get("text"), bounded_text_limit)
            else:
                row.pop("text", None)
        return _format_response(
            _with_result_meta(
                {"project": project_name, "query": query, "hits": rows},
                rows,
                limit=bounded_limit,
                extra={
                    "includeText": include_text,
                    "textLimit": bounded_text_limit,
                    "dedupeBySource": dedupe_by_source,
                },
            ),
            output_format,
        )

    def code_lookup_type(
        self,
        project: str | None = None,
        type_name: str | None = None,
        fqn: str | None = None,
        include_members: bool = False,
        member_limit: int = 50,
        member_summary: bool = True,
        limit: int = 20,
        compact: bool = False,
        output_format: str = "json",
    ) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        if not type_name and not fqn:
            raise MemgraphError("Provide either type_name or fqn.")
        bounded_limit = _bounded_limit(limit, default=20, maximum=100)
        bounded_member_limit = _bounded_limit(member_limit, default=50, maximum=200)
        predicate = "t.fqn = $fqn" if fqn else "t.name = $type_name"
        count_rows = self.client.run(
            f"""
            MATCH (t {{project: $project}})
            WHERE (t:Class OR t:Interface OR t:Annotation) AND {predicate}
            RETURN count(t) AS count
            """,
            {"project": project_name, "type_name": type_name, "fqn": fqn},
        )
        total_count = count_rows[0].get("count", 0) if count_rows else 0
        type_projection = (
            """
                   labels(t) AS labels, t.fqn AS fqn, t.name AS name, t.kind AS kind,
                   collect(DISTINCT file.path) AS files
            """
            if compact
            else """
                   labels(t) AS labels, t.fqn AS fqn, t.name AS name, t.kind AS kind,
                   t.visibility AS visibility, t.isExternal AS isExternal,
                   t.language AS language, t.framework AS framework,
                   t.modulePath AS modulePath, collect(DISTINCT file.path) AS files
            """
        )
        types = self.client.run(
            f"""
            MATCH (t {{project: $project}})
            WHERE (t:Class OR t:Interface OR t:Annotation) AND {predicate}
            OPTIONAL MATCH (file:File {{project: $project}})-[:DEFINES]->(t)
            RETURN {type_projection.strip()}
            ORDER BY fqn
            LIMIT $limit
            """,
            {
                "project": project_name,
                "type_name": type_name,
                "fqn": fqn,
                "limit": bounded_limit,
            },
        )
        if member_summary or include_members:
            for item in types:
                item_fqn = item.get("fqn")
                if member_summary and item_fqn:
                    summary = self.client.run(
                        """
                        MATCH (t {project: $project, fqn: $fqn})
                        WHERE t:Class OR t:Interface OR t:Annotation
                        OPTIONAL MATCH (t)-[:DECLARES]->(m:Method {project: $project})
                        WITH t, count(DISTINCT m) AS methods
                        OPTIONAL MATCH (t)-[:DECLARES]->(field:Field {project: $project})
                        RETURN methods AS methods, count(DISTINCT field) AS fields
                        """,
                        {"project": project_name, "fqn": item_fqn},
                    )
                    item["memberCounts"] = summary[0] if summary else {"methods": 0, "fields": 0}
                if not include_members or not item_fqn:
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
        return _format_response(
            _with_result_meta(
                {"project": project_name, "types": types},
                types,
                limit=bounded_limit,
                total_count=total_count,
                extra={
                    "includeMembers": include_members,
                    "memberLimit": bounded_member_limit if include_members else None,
                    "memberSummary": member_summary,
                    "compact": compact,
                },
            ),
            output_format,
        )

    def code_lookup_methods(
        self,
        signature_fragment: str,
        project: str | None = None,
        skip: int = 0,
        limit: int = 50,
        compact: bool = False,
        output_format: str = "json",
    ) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        return_projection = (
            """
                   method.signature AS signature, method.name AS name,
                   method.ownerFqn AS ownerFqn, method.ownerDisplayName AS ownerDisplayName,
                   method.startLine AS startLine, method.endLine AS endLine,
                   collect(DISTINCT file.path) AS files
            """
            if compact
            else """
                   method.signature AS signature, method.name AS name,
                   method.ownerFqn AS ownerFqn, method.ownerDisplayName AS ownerDisplayName,
                   method.returnType AS returnType, method.visibility AS visibility,
                   method.startLine AS startLine, method.endLine AS endLine,
                   method.isStatic AS isStatic, method.isSynthetic AS isSynthetic,
                   collect(DISTINCT file.path) AS files
            """
        )
        rows = self.client.run(
            """
            MATCH (method:Method {project: $project})
            WHERE method.signature CONTAINS $fragment
            OPTIONAL MATCH (file:File {project: $project})-[:DEFINES]->(method)
            RETURN __RETURN_PROJECTION__
            ORDER BY signature
            SKIP $skip
            LIMIT $limit
            """.replace("__RETURN_PROJECTION__", return_projection.strip()),
            {
                "project": project_name,
                "fragment": signature_fragment,
                "skip": _bounded_skip(skip),
                "limit": _bounded_limit(limit, default=50, maximum=200),
            },
        )
        count_rows = self.client.run(
            """
            MATCH (method:Method {project: $project})
            WHERE method.signature CONTAINS $fragment
            RETURN count(method) AS count
            """,
            {"project": project_name, "fragment": signature_fragment},
        )
        total_count = count_rows[0].get("count", 0) if count_rows else len(rows)
        skip_value = _bounded_skip(skip)
        limit_value = _bounded_limit(limit, default=50, maximum=200)
        return _format_response(
            _with_result_meta(
                {"project": project_name, "methods": rows},
                rows,
                skip=skip_value,
                limit=limit_value,
                total_count=total_count,
                extra={"compact": compact},
            ),
            output_format,
        )

    def code_callers(
        self,
        callee_fragment: str,
        project: str | None = None,
        skip: int = 0,
        limit: int = 25,
        compact: bool = True,
        output_format: str = "json",
    ) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        skip_value = _bounded_skip(skip)
        limit_value = _bounded_limit(limit, default=25, maximum=100)
        rows = self.client.run(
            """
            MATCH (caller:Method {project: $project})-[:CALLS]->(callee:Method {project: $project})
            WHERE callee.signature CONTAINS $fragment
            RETURN caller.signature AS callerSignature,
                   caller.ownerDisplayName AS callerOwner,
                   caller.startLine AS callerStartLine,
                   caller.endLine AS callerEndLine,
                   callee.signature AS calleeSignature,
                   callee.ownerDisplayName AS calleeOwner
            ORDER BY caller.signature, callee.signature
            SKIP $skip
            LIMIT $limit
            """,
            {
                "project": project_name,
                "fragment": callee_fragment,
                "skip": skip_value,
                "limit": limit_value,
            },
        )
        count_rows = self.client.run(
            """
            MATCH (caller:Method {project: $project})-[:CALLS]->(callee:Method {project: $project})
            WHERE callee.signature CONTAINS $fragment
            RETURN count(*) AS count
            """,
            {"project": project_name, "fragment": callee_fragment},
        )
        total_count = count_rows[0].get("count", 0) if count_rows else len(rows)
        if compact:
            rows = [
                {
                    "caller": row.get("callerSignature"),
                    "owner": row.get("callerOwner"),
                    "startLine": row.get("callerStartLine"),
                    "endLine": row.get("callerEndLine"),
                    "calleeOwner": row.get("calleeOwner"),
                }
                for row in rows
            ]
        return _format_response(
            _with_result_meta(
                {"project": project_name, "callers": rows},
                rows,
                skip=skip_value,
                limit=limit_value,
                total_count=total_count,
                extra={"compact": compact},
            ),
            output_format,
        )

    def code_callees(
        self,
        caller_fragment: str,
        project: str | None = None,
        skip: int = 0,
        limit: int = 25,
        compact: bool = True,
        output_format: str = "json",
    ) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        skip_value = _bounded_skip(skip)
        limit_value = _bounded_limit(limit, default=25, maximum=100)
        rows = self.client.run(
            """
            MATCH (caller:Method {project: $project})-[:CALLS]->(callee:Method {project: $project})
            WHERE caller.signature CONTAINS $fragment
            RETURN caller.signature AS callerSignature,
                   caller.ownerDisplayName AS callerOwner,
                   callee.signature AS calleeSignature,
                   callee.ownerDisplayName AS calleeOwner
            ORDER BY caller.signature, callee.signature
            SKIP $skip
            LIMIT $limit
            """,
            {
                "project": project_name,
                "fragment": caller_fragment,
                "skip": skip_value,
                "limit": limit_value,
            },
        )
        count_rows = self.client.run(
            """
            MATCH (caller:Method {project: $project})-[:CALLS]->(callee:Method {project: $project})
            WHERE caller.signature CONTAINS $fragment
            RETURN count(*) AS count
            """,
            {"project": project_name, "fragment": caller_fragment},
        )
        total_count = count_rows[0].get("count", 0) if count_rows else len(rows)
        if compact:
            rows = [
                {
                    "callerOwner": row.get("callerOwner"),
                    "callee": row.get("calleeSignature"),
                    "owner": row.get("calleeOwner"),
                }
                for row in rows
            ]
        return _format_response(
            _with_result_meta(
                {"project": project_name, "callees": rows},
                rows,
                skip=skip_value,
                limit=limit_value,
                total_count=total_count,
                extra={"compact": compact},
            ),
            output_format,
        )

    def code_hot_paths(
        self,
        project: str | None = None,
        limit: int = 20,
        include_tests: bool = False,
        include_evidence: bool = True,
        sections: Sequence[str] | str | None = None,
        output_format: str = "json",
    ) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        bounded_limit = _bounded_limit(limit, default=20, maximum=50)
        requested_sections = _normalize_sections(
            sections,
            allowed=HOT_PATH_SECTIONS,
            default=HOT_PATH_SECTIONS,
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
                RETURN 'type' AS kind, type.fqn AS id, labels(type)[0] AS label,
                       methods AS score, file.path AS path, null AS startLine, null AS endLine
                ORDER BY score DESC, id
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
                RETURN 'method' AS kind, method.signature AS id, method.ownerDisplayName AS label,
                       lines AS score, file.path AS path,
                       method.startLine AS startLine, method.endLine AS endLine
                ORDER BY score DESC, id
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
                RETURN 'fanIn' AS kind, method.signature AS id, method.ownerDisplayName AS label,
                       callers AS score, file.path AS path,
                       method.startLine AS startLine, method.endLine AS endLine
                ORDER BY score DESC, id
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
                RETURN 'fanOut' AS kind, method.signature AS id, method.ownerDisplayName AS label,
                       callees AS score, file.path AS path,
                       method.startLine AS startLine, method.endLine AS endLine
                ORDER BY score DESC, id
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
                if not include_evidence:
                    row.pop("path", None)
                    row.pop("startLine", None)
                    row.pop("endLine", None)
                rows.append(row)
        return _format_response(
            _with_result_meta(
                {
                    "project": project_name,
                    "includeTests": include_tests,
                    "hotPaths": rows,
                },
                rows,
                limit=bounded_limit,
                extra={
                    "includeEvidence": include_evidence,
                    "sections": [
                        section
                        for section in ("largestTypes", "longestMethods", "fanIn", "fanOut")
                        if section in requested_sections
                    ],
                },
            ),
            output_format,
        )

    def code_quality_stats(
        self,
        project: str | None = None,
        include_tests: bool = True,
        limit: int = 20,
        output_format: str = "json",
    ) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        bounded_limit = _bounded_limit(limit, default=20, maximum=50)
        params = {
            "project": project_name,
            "limit": bounded_limit,
            "include_tests": include_tests,
        }
        inventory = self.client.run(
            """
            MATCH (n {project: $project})
            RETURN labels(n) AS labels, count(n) AS count
            ORDER BY count DESC
            """,
            {"project": project_name},
        )
        method_lengths = self.client.run(
            """
            MATCH (file:File {project: $project})-[:DEFINES]->(method:Method {project: $project})
            WHERE ($include_tests OR NOT file.path STARTS WITH 'src/test/')
              AND method.startLine IS NOT NULL AND method.endLine IS NOT NULL
              AND coalesce(method.isSynthetic, false) = false
            WITH method.endLine - method.startLine + 1 AS lines
            RETURN count(lines) AS methods,
                   round(avg(lines) * 100) / 100 AS avgLines,
                   max(lines) AS maxLines,
                   sum(CASE WHEN lines >= 50 THEN 1 ELSE 0 END) AS methods50Plus,
                   sum(CASE WHEN lines >= 100 THEN 1 ELSE 0 END) AS methods100Plus
            """,
            params,
        )
        fan_out = self.client.run(
            """
            MATCH (method:Method {project: $project})
            OPTIONAL MATCH (method)-[call:CALLS]->(:Method {project: $project})
            WITH method, count(call) AS degree
            RETURN count(method) AS methods,
                   round(avg(degree) * 100) / 100 AS avgOut,
                   max(degree) AS maxOut,
                   sum(CASE WHEN degree >= 10 THEN 1 ELSE 0 END) AS methodsOut10Plus,
                   sum(CASE WHEN degree = 0 THEN 1 ELSE 0 END) AS methodsOut0
            """,
            {"project": project_name},
        )
        fan_in = self.client.run(
            """
            MATCH (method:Method {project: $project})
            OPTIONAL MATCH (:Method {project: $project})-[call:CALLS]->(method)
            WITH method, count(call) AS degree
            RETURN count(method) AS methods,
                   round(avg(degree) * 100) / 100 AS avgIn,
                   max(degree) AS maxIn,
                   sum(CASE WHEN degree >= 10 THEN 1 ELSE 0 END) AS methodsIn10Plus,
                   sum(CASE WHEN degree = 0 THEN 1 ELSE 0 END) AS methodsIn0
            """,
            {"project": project_name},
        )
        type_sizes = self.client.run(
            """
            MATCH (type {project: $project})
            WHERE type:Class OR type:Interface OR type:Annotation
            OPTIONAL MATCH (type)-[:DECLARES]->(method:Method {project: $project})
            WITH type, count(method) AS methods
            RETURN count(type) AS types,
                   round(avg(methods) * 100) / 100 AS avgMethodsPerType,
                   max(methods) AS maxMethodsPerType,
                   sum(CASE WHEN methods >= 25 THEN 1 ELSE 0 END) AS types25MethodsPlus,
                   sum(CASE WHEN methods >= 50 THEN 1 ELSE 0 END) AS types50MethodsPlus
            """,
            {"project": project_name},
        )
        chunks_by_label = self.client.run(
            """
            MATCH (chunk:CodeChunk {project: $project})
            RETURN chunk.sourceLabel AS sourceLabel, count(chunk) AS chunks
            ORDER BY chunks DESC, sourceLabel
            """,
            {"project": project_name},
        )
        files_by_methods = self.client.run(
            """
            MATCH (file:File {project: $project})-[:DEFINES]->(method:Method {project: $project})
            WHERE $include_tests OR NOT file.path STARTS WITH 'src/test/'
            RETURN file.path AS path, count(method) AS methods
            ORDER BY methods DESC, path
            LIMIT $limit
            """,
            params,
        )
        response = {
            "project": project_name,
            "includeTests": include_tests,
            "inventory": inventory,
            "methodLengths": method_lengths[0] if method_lengths else {},
            "fanOut": fan_out[0] if fan_out else {},
            "fanIn": fan_in[0] if fan_in else {},
            "typeSizes": type_sizes[0] if type_sizes else {},
            "chunksByLabel": chunks_by_label,
            "filesByMethods": files_by_methods,
        }
        response["meta"] = {"limit": bounded_limit, "resultChars": _json_size(response)}
        return _format_response(response, output_format)

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
        return {
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
        rows = self.client.run(
            """
            CALL embeddings.text([$query], {}) YIELD embeddings
            WITH embeddings[0] AS queryVector
            CALL vector_search.search('memory_chunk_embedding_v1', $limit, queryVector)
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
                "project": project_name,
                "query": query,
                "limit": _bounded_limit(limit, default=5, maximum=20),
            },
        )
        return {"project": project_name, "query": query, "hits": rows}

    def memory_get(self, memory_id: str, project: str | None = None) -> dict[str, Any]:
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
        return {"project": project_name, "memory": rows[0] if rows else None}

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
            return {
                "project": project_name,
                "deleted": False,
                "memory": None,
                "chunkIds": [],
                "orphanCodeRefsDeleted": 0,
            }

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

        return {
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
        return {
            "project": project_name,
            "memory": rows[0] if rows else None,
            "codeRef": link_result,
            "chunk": chunk_result,
        }

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
        return {
            "project": project_name,
            "memory": rows[0] if rows else None,
            "chunk": chunk_result,
        }

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
        memory = self.memory_get(memory_id, project_name).get("memory")
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
        return _format_response(
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
