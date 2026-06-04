"""Compact code-context composition tools."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from memgraph_ingester_mcp.db import MemgraphError


def _services():
    from memgraph_ingester_mcp import services

    return services


def _group_limited(
    rows: Sequence[Mapping[str, Any]],
    *,
    key: str = "path",
    limit: int,
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        group_key = row.get(key)
        if not isinstance(group_key, str) or not group_key:
            continue
        bucket = grouped.setdefault(group_key, [])
        if len(bucket) < limit:
            item = dict(row)
            item.pop(key, None)
            bucket.append(item)
    return grouped


def _fragment_rank(path: str | None, fragments: Sequence[str]) -> tuple[int, str]:
    if not path:
        return (len(fragments), "")
    for index, fragment in enumerate(fragments):
        if fragment in path:
            return (index, path)
    return (len(fragments), path)


class CodeContextMixin:
    """Composable, low-token context bundles for code workflow discovery."""

    def code_file_context(
        self,
        path_fragments: Sequence[str] | str | None,
        project: str | None = None,
        limit_files: int = 5,
        symbol_limit: int = 8,
        include_tests: bool = False,
        output_format: str = "json",
    ) -> dict[str, Any]:
        services = _services()
        project_name = self.resolve_project(project)
        fragments = services._normalize_string_list(path_fragments)
        if not fragments:
            raise MemgraphError("Provide at least one path fragment.")

        bounded_file_limit = services._bounded_limit(limit_files, default=5, maximum=25)
        bounded_symbol_limit = services._bounded_symbol_limit(symbol_limit)
        file_rows = self.client.run(
            """
            MATCH (file:File {project: $project})
            WHERE any(fragment IN $fragments WHERE file.path CONTAINS fragment)
              AND ($include_tests OR NOT file.path STARTS WITH 'src/test/')
            OPTIONAL MATCH (file)-[:DEFINES]->(definition {project: $project})
            WITH file, count(DISTINCT definition) AS definitionCount
            OPTIONAL MATCH (chunk:CodeChunk {project: $project})
            WHERE chunk.path = file.path
            WITH file, definitionCount, count(DISTINCT chunk) AS chunkCount
            RETURN file.path AS path,
                   file.language AS language,
                   definitionCount,
                   chunkCount
            ORDER BY file.path
            LIMIT $limit
            """,
            {
                "project": project_name,
                "fragments": fragments,
                "include_tests": include_tests,
                "limit": bounded_file_limit,
            },
        )
        file_rows = sorted(
            file_rows,
            key=lambda row: _fragment_rank(row.get("path"), fragments),
        )
        paths = [row["path"] for row in file_rows if row.get("path")]
        if not paths:
            return services._format_response(
                services._with_result_meta(
                    {
                        "project": project_name,
                        "pathFragments": fragments,
                        "files": [],
                    },
                    [],
                    limit=bounded_file_limit,
                    extra={
                        "symbolLimit": bounded_symbol_limit,
                        "includeTests": include_tests,
                    },
                ),
                output_format,
            )

        type_rows = self.client.run(
            """
            MATCH (file:File {project: $project})-[:DEFINES]->(node {project: $project})
            WHERE file.path IN $paths AND (node:Class OR node:Interface OR node:Annotation)
            RETURN file.path AS path,
                   labels(node)[0] AS label,
                   node.name AS name,
                   node.fqn AS fqn,
                   node.kind AS kind,
                   node.startLine AS startLine,
                   node.endLine AS endLine
            ORDER BY file.path, node.startLine, node.fqn
            """,
            {"project": project_name, "paths": paths},
        )
        method_rows = self.client.run(
            """
            MATCH (file:File {project: $project})-[:DEFINES]->(method:Method {project: $project})
            WHERE file.path IN $paths
            RETURN file.path AS path,
                   method.ownerDisplayName AS owner,
                   method.name AS name,
                   method.signature AS signature,
                   method.startLine AS startLine,
                   method.endLine AS endLine
            ORDER BY file.path, method.startLine, method.signature
            """,
            {"project": project_name, "paths": paths},
        )
        field_rows = self.client.run(
            """
            MATCH (file:File {project: $project})-[:DEFINES]->(field:Field {project: $project})
            WHERE file.path IN $paths
            RETURN file.path AS path,
                   coalesce(field.ownerDisplayName, field.ownerFqn) AS owner,
                   field.name AS name,
                   field.fqn AS fqn,
                   field.startLine AS startLine,
                   field.endLine AS endLine
            ORDER BY file.path, field.startLine, field.fqn
            """,
            {"project": project_name, "paths": paths},
        )
        role_rows = self.client.run(
            """
            MATCH (chunk:CodeChunk {project: $project})
            WHERE chunk.path IN $paths
            WITH chunk.path AS path,
                 coalesce(chunk.ragRole, chunk.sourceLabel, 'unknown') AS ragRole,
                 count(*) AS count
            RETURN path, ragRole, count
            ORDER BY path, ragRole
            """,
            {"project": project_name, "paths": paths},
        )

        types_by_path = _group_limited(type_rows, limit=bounded_symbol_limit)
        methods_by_path = _group_limited(method_rows, limit=bounded_symbol_limit)
        fields_by_path = _group_limited(field_rows, limit=bounded_symbol_limit)
        roles_by_path = _group_limited(role_rows, limit=bounded_symbol_limit)

        files = []
        for row in file_rows:
            path = row.get("path")
            files.append(
                {
                    "path": path,
                    "language": row.get("language"),
                    "definitionCount": row.get("definitionCount"),
                    "chunkCount": row.get("chunkCount"),
                    "chunkRoles": roles_by_path.get(path, []),
                    "types": types_by_path.get(path, []),
                    "methods": methods_by_path.get(path, []),
                    "fields": fields_by_path.get(path, []),
                }
            )

        return services._format_response(
            services._with_result_meta(
                {
                    "project": project_name,
                    "pathFragments": fragments,
                    "files": files,
                },
                files,
                limit=bounded_file_limit,
                extra={
                    "symbolLimit": bounded_symbol_limit,
                    "includeTests": include_tests,
                },
            ),
            output_format,
        )

    def code_flow_context(
        self,
        query: str,
        project: str | None = None,
        limit_files: int = 5,
        anchor_limit: int = 8,
        symbol_limit: int = 8,
        include_tests: bool = False,
        output_format: str = "json",
    ) -> dict[str, Any]:
        services = _services()
        project_name = self.resolve_project(project)
        bounded_file_limit = services._bounded_limit(limit_files, default=5, maximum=12)
        bounded_anchor_limit = services._bounded_limit(anchor_limit, default=8, maximum=25)
        bounded_symbol_limit = services._bounded_symbol_limit(symbol_limit)

        semantic = self.code_search(
            query=query,
            project=project_name,
            limit=bounded_anchor_limit,
            include_tests=include_tests,
            include_text=False,
            include_keys=True,
            output_format="json",
        )
        semantic_rows = list(semantic.get("hits", []))

        lexical_rows: list[dict[str, Any]] = []
        lexical_terms = [
            term
            for term in services._identifier_terms(query)
            if len(term) >= 4 and term.lower() not in {"with", "from", "into", "that", "this"}
        ][:8]
        if lexical_terms:
            lexical = self.code_text_search(
                project=project_name,
                any_terms=lexical_terms,
                limit=bounded_anchor_limit,
                include_tests=include_tests,
                include_text=False,
                output_format="json",
            )
            lexical_rows = list(lexical.get("hits", []))

        path_scores: dict[str, float] = {}
        for index, row in enumerate(semantic_rows):
            path = row.get("path")
            if path:
                path_scores[path] = path_scores.get(path, 0.0) + 100.0 - index
        for index, row in enumerate(lexical_rows):
            path = row.get("path")
            if path:
                path_scores[path] = path_scores.get(path, 0.0) + 25.0 - (index / 2)

        selected_paths = [
            path
            for path, _score in sorted(path_scores.items(), key=lambda item: (-item[1], item[0]))[
                :bounded_file_limit
            ]
        ]
        file_context = (
            self.code_file_context(
                selected_paths,
                project=project_name,
                limit_files=bounded_file_limit,
                symbol_limit=bounded_symbol_limit,
                include_tests=include_tests,
                output_format="json",
            )
            if selected_paths
            else {"files": []}
        )
        files = file_context["files"]

        flow_edges = []
        if selected_paths:
            flow_edges = self.client.run(
                """
                MATCH (callerFile:File {project: $project})
                  -[:DEFINES]->(caller:Method {project: $project})
                  -[:CALLS]->(callee:Method {project: $project})
                  <-[:DEFINES]-(calleeFile:File {project: $project})
                WHERE callerFile.path IN $paths OR calleeFile.path IN $paths
                RETURN callerFile.path AS callerPath,
                       caller.ownerDisplayName AS callerOwner,
                       caller.name AS caller,
                       caller.startLine AS callerStartLine,
                       calleeFile.path AS calleePath,
                       callee.ownerDisplayName AS calleeOwner,
                       callee.name AS callee,
                       callee.startLine AS calleeStartLine
                ORDER BY callerFile.path, caller.startLine, calleeFile.path, callee.startLine
                LIMIT $limit
                """,
                {
                    "project": project_name,
                    "paths": selected_paths,
                    "limit": bounded_file_limit * bounded_symbol_limit,
                },
            )

        rows_for_meta = semantic_rows + lexical_rows + flow_edges
        return services._format_response(
            services._with_result_meta(
                {
                    "project": project_name,
                    "query": query,
                    "anchors": semantic_rows,
                    "lexicalAnchors": lexical_rows,
                    "files": files,
                    "flowEdges": flow_edges,
                },
                rows_for_meta,
                limit=bounded_anchor_limit,
                extra={
                    "selectedPaths": selected_paths,
                    "lexicalTerms": lexical_terms,
                    "limitFiles": bounded_file_limit,
                    "symbolLimit": bounded_symbol_limit,
                    "includeTests": include_tests,
                },
            ),
            output_format,
        )
