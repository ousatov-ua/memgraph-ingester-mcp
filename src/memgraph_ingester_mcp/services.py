"""High-level Memgraph Ingester operations exposed as MCP tools."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from hashlib import sha256
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


def _memory_spec(memory_type: str):
    spec = MEMORY_SPECS.get(memory_type)
    if spec is None:
        allowed = ", ".join(sorted(MEMORY_SPECS))
        raise MemgraphError(f"Unsupported memory_type {memory_type!r}. Allowed: {allowed}.")
    return spec


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

    def code_orientation(self, project: str | None = None, limit: int = 30) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        bounded_limit = _bounded_limit(limit, default=30, maximum=100)
        languages = self.client.run(
            """
            MATCH (l:Language {project: $project})-[:CONTAINS]->(c:Code)
            RETURN l.name AS languageName, l.graphName AS graphName, c.language AS language,
                   c.lastIngested AS lastIngested
            ORDER BY languageName
            """,
            {"project": project_name},
        )
        packages = self.client.run(
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
        large_types = self.client.run(
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
        cross_owner_calls = self.client.run(
            """
            MATCH (caller:Method {project: $project})-[:CALLS]->(callee:Method {project: $project})
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
        return {
            "project": project_name,
            "languages": languages,
            "packages": packages,
            "largestTypes": large_types,
            "crossOwnerCalls": cross_owner_calls,
        }

    def code_search(
        self,
        query: str,
        project: str | None = None,
        limit: int = 10,
    ) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        bounded_limit = _bounded_limit(limit, default=10, maximum=25)
        rows = self.client.run(
            """
            CALL embeddings.text([$query], {}) YIELD embeddings
            WITH embeddings[0] AS queryVector
            CALL vector_search.search('code_chunk_embedding_v1', $limit, queryVector)
            YIELD node AS chunk, similarity
            WITH chunk, similarity
            WHERE chunk.project = $project
            MATCH (source {project: $project})-[:HAS_RAG_CHUNK]->(chunk)
            RETURN labels(source) AS sourceType, chunk.sourceId AS sourceId,
                   chunk.path AS path, chunk.ownerFqn AS ownerFqn, chunk.signature AS signature,
                   chunk.text AS text, similarity
            ORDER BY similarity DESC
            """,
            {"project": project_name, "query": query, "limit": bounded_limit},
        )
        for row in rows:
            row["text"] = _compact_text(row.get("text"))
        return {"project": project_name, "query": query, "hits": rows}

    def code_lookup_type(
        self,
        project: str | None = None,
        type_name: str | None = None,
        fqn: str | None = None,
        include_members: bool = True,
        limit: int = 20,
    ) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        if not type_name and not fqn:
            raise MemgraphError("Provide either type_name or fqn.")
        bounded_limit = _bounded_limit(limit, default=20, maximum=100)
        predicate = "t.fqn = $fqn" if fqn else "t.name = $type_name"
        types = self.client.run(
            f"""
            MATCH (t {{project: $project}})
            WHERE (t:Class OR t:Interface OR t:Annotation) AND {predicate}
            OPTIONAL MATCH (file:File {{project: $project}})-[:DEFINES]->(t)
            RETURN labels(t) AS labels, t.fqn AS fqn, t.name AS name, t.kind AS kind,
                   t.visibility AS visibility, t.isExternal AS isExternal,
                   t.language AS language, t.framework AS framework,
                   t.modulePath AS modulePath, collect(DISTINCT file.path) AS files
            ORDER BY t.fqn
            LIMIT $limit
            """,
            {
                "project": project_name,
                "type_name": type_name,
                "fqn": fqn,
                "limit": bounded_limit,
            },
        )
        if include_members:
            for item in types:
                item["methods"] = self.client.run(
                    """
                    MATCH (t {project: $project, fqn: $fqn})-[:DECLARES]->(m:Method)
                    WHERE (t:Class OR t:Interface OR t:Annotation)
                    RETURN m.signature AS signature, m.name AS name, m.startLine AS startLine,
                           m.endLine AS endLine, m.returnType AS returnType,
                           m.visibility AS visibility, m.isStatic AS isStatic,
                           m.isSynthetic AS isSynthetic
                    ORDER BY m.name, m.signature
                    LIMIT $limit
                    """,
                    {"project": project_name, "fqn": item["fqn"], "limit": 200},
                )
                item["fields"] = self.client.run(
                    """
                    MATCH (t {project: $project, fqn: $fqn})-[:DECLARES]->(field:Field)
                    WHERE (t:Class OR t:Interface OR t:Annotation)
                    RETURN field.fqn AS fqn, field.name AS name, field.type AS type,
                           field.visibility AS visibility, field.isStatic AS isStatic,
                           field.kind AS kind
                    ORDER BY field.name
                    LIMIT $limit
                    """,
                    {"project": project_name, "fqn": item["fqn"], "limit": 200},
                )
        return {"project": project_name, "types": types}

    def code_lookup_methods(
        self,
        signature_fragment: str,
        project: str | None = None,
        skip: int = 0,
        limit: int = 50,
    ) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        rows = self.client.run(
            """
            MATCH (method:Method {project: $project})
            WHERE method.signature CONTAINS $fragment
            OPTIONAL MATCH (file:File {project: $project})-[:DEFINES]->(method)
            RETURN method.signature AS signature, method.name AS name,
                   method.ownerFqn AS ownerFqn, method.ownerDisplayName AS ownerDisplayName,
                   method.returnType AS returnType, method.visibility AS visibility,
                   method.startLine AS startLine, method.endLine AS endLine,
                   method.isStatic AS isStatic, method.isSynthetic AS isSynthetic,
                   collect(DISTINCT file.path) AS files
            ORDER BY method.signature
            SKIP $skip
            LIMIT $limit
            """,
            {
                "project": project_name,
                "fragment": signature_fragment,
                "skip": _bounded_skip(skip),
                "limit": _bounded_limit(limit, default=50, maximum=200),
            },
        )
        return {"project": project_name, "methods": rows}

    def code_callers(
        self,
        callee_fragment: str,
        project: str | None = None,
        skip: int = 0,
        limit: int = 100,
    ) -> dict[str, Any]:
        project_name = self.resolve_project(project)
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
                "skip": _bounded_skip(skip),
                "limit": _bounded_limit(limit, default=100, maximum=300),
            },
        )
        return {"project": project_name, "callers": rows}

    def code_callees(
        self,
        caller_fragment: str,
        project: str | None = None,
        skip: int = 0,
        limit: int = 100,
    ) -> dict[str, Any]:
        project_name = self.resolve_project(project)
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
                "skip": _bounded_skip(skip),
                "limit": _bounded_limit(limit, default=100, maximum=300),
            },
        )
        return {"project": project_name, "callees": rows}

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

    def memory_orientation(self, project: str | None = None) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        return {
            "project": project_name,
            "rules": self.client.run(
                """
                MATCH (m:Memory {project: $project})-[:HAS_RULE]->(rule:Rule)
                RETURN rule.id AS id, rule.severity AS severity, rule.title AS title,
                       rule.description AS description
                ORDER BY rule.severity, rule.id
                """,
                {"project": project_name},
            ),
            "openFindings": self.client.run(
                """
                MATCH (m:Memory {project: $project})-[:HAS_FINDING]->(finding:Finding)
                WHERE finding.status = 'open'
                RETURN finding.id AS id, finding.type AS type, finding.title AS title,
                       finding.summary AS summary
                ORDER BY finding.id
                """,
                {"project": project_name},
            ),
            "activeTasks": self.client.run(
                """
                MATCH (m:Memory {project: $project})-[:HAS_TASK]->(task:Task)
                WHERE task.status IN ['todo', 'doing', 'blocked']
                RETURN task.id AS id, task.title AS title, task.status AS status,
                       task.priority AS priority, task.description AS description
                ORDER BY task.priority, task.status, task.id
                """,
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
                RETURN risk.id AS id, risk.title AS title, risk.severity AS severity,
                       risk.mitigation AS mitigation
                ORDER BY risk.severity, risk.id
                """,
                {"project": project_name},
            ),
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
            """
            MATCH (memory {project: $project, id: $memory_id})
            WHERE memory:Decision OR memory:ADR OR memory:Rule OR memory:Context
               OR memory:Finding OR memory:Task OR memory:Risk OR memory:Question OR memory:Idea
            OPTIONAL MATCH (memory)-[:REFERS_TO]->(ref:CodeRef)-[:RESOLVES_TO]->(target)
            WITH memory, collect(
                CASE WHEN ref IS NULL THEN NULL ELSE {
                    targetType: ref.targetType,
                    key: ref.key,
                    targetLabels: labels(target)
                } END
            ) AS refs
            RETURN labels(memory) AS labels, properties(memory) AS properties,
                   [ref IN refs WHERE ref IS NOT NULL] AS codeRefs
            """,
            {"project": project_name, "memory_id": memory_id},
        )
        return {"project": project_name, "memory": rows[0] if rows else None}

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
        return {"project": project_name, "resolved": bool(rows), "links": rows}

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
    ) -> dict[str, Any]:
        project_name = self.resolve_project(project)
        _ensure_read_only_query(query)
        _ensure_project_scoped(query)
        params = dict(parameters or {})
        params.setdefault("project", project_name)
        params.setdefault("limit", _bounded_limit(limit, default=200, maximum=500))
        rows = self.client.run(query, params)
        return {"project": project_name, "rows": rows}

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
