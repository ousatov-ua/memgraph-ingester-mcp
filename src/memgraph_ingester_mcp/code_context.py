"""Compact code-context composition tools."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from memgraph_ingester_mcp import queries as Q
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
            Q.CODE_FILE_CONTEXT,
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
        if not file_rows:
            return self._finalize_response(
                services._with_result_meta(
                    {
                        "project": project_name,
                        "files": [],
                    },
                    [],
                    limit=bounded_file_limit,
                ),
                output_format,
            )

        def bounded_items(
            value: Any,
            *,
            role_rows: bool = False,
        ) -> list[dict[str, Any]]:
            if not isinstance(value, Sequence) or isinstance(value, str):
                return []
            rows = [dict(item) for item in value if isinstance(item, Mapping)]
            if role_rows:
                rows = [row for row in rows if row.get("count")]
                rows.sort(key=lambda row: (-(row.get("count") or 0), row.get("ragRole") or ""))
            else:
                rows.sort(key=lambda row: (row.get("startLine") or 0, row.get("name") or ""))
            return rows[:bounded_symbol_limit]

        def item_count(value: Any) -> int:
            if not isinstance(value, Sequence) or isinstance(value, str):
                return 0
            return sum(1 for item in value if isinstance(item, Mapping))

        files = []
        for row in file_rows:
            types = bounded_items(row.get("types"))
            methods = bounded_items(row.get("methods"))
            fields = bounded_items(row.get("fields"))
            files.append(
                {
                    "path": row.get("path"),
                    "language": row.get("language"),
                    "definitionCount": (
                        item_count(row.get("types"))
                        + item_count(row.get("methods"))
                        + item_count(row.get("fields"))
                    ),
                    "chunkCount": row.get("chunkCount"),
                    "chunkRoles": bounded_items(row.get("chunkRoles"), role_rows=True),
                    "types": types,
                    "methods": methods,
                    "fields": fields,
                }
            )

        return self._finalize_response(
            services._with_result_meta(
                {
                    "project": project_name,
                    "files": files,
                },
                files,
                limit=bounded_file_limit,
            ),
            output_format,
        )

    def code_flow_context(
        self,
        query: str,
        project: str | None = None,
        limit_files: int = 3,
        anchor_limit: int = 5,
        symbol_limit: int = 3,
        include_tests: bool = False,
        detail: str = "compact",
        output_format: str = "json",
    ) -> dict[str, Any]:
        services = _services()
        project_name = self.resolve_project(project)
        bounded_file_limit = services._bounded_limit(limit_files, default=3, maximum=12)
        bounded_anchor_limit = services._bounded_limit(anchor_limit, default=5, maximum=25)
        bounded_symbol_limit = services._bounded_limit(symbol_limit, default=3, maximum=50)
        normalized_detail = (detail or "compact").strip().lower()
        if normalized_detail not in {"compact", "full"}:
            raise MemgraphError("detail must be 'compact' or 'full'.")
        flow_symbol_limit = (
            bounded_symbol_limit if normalized_detail == "full" else min(bounded_symbol_limit, 3)
        )
        related_file_limit = 2 if normalized_detail == "full" else 1
        edge_limit = (
            bounded_file_limit * bounded_symbol_limit
            if normalized_detail == "full"
            else min(bounded_file_limit * flow_symbol_limit, 12)
        )

        semantic = self.code_search(
            query=query,
            project=project_name,
            limit=bounded_anchor_limit,
            include_tests=include_tests,
            include_text=False,
            output_format="json",
        )
        semantic_rows = list(semantic.get("hits", []))

        path_scores: dict[str, float] = {}
        for index, row in enumerate(semantic_rows):
            path = row.get("path")
            if path:
                score = float(row.get("score") or 0.0)
                path_scores[path] = (
                    path_scores.get(path, 0.0)
                    + (score * 50.0)
                    + ((bounded_anchor_limit - index) * 2.0)
                )

        lexical_rows: list[dict[str, Any]] = []
        # Always fuse lexical evidence: weak-but-diverse vector hits would otherwise lock in
        # wrong paths and the whole flow expansion would be spent on them.
        lexical_terms = services._lexical_query_terms(query, min_length=4)[:16]
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

        for index, row in enumerate(lexical_rows):
            path = row.get("path")
            # Exclude file-role chunks: their Words line aggregates all method names and
            # matches almost any query, drowning out the vector-ranked primary chunks.
            if path and row.get("kind") != "File":
                term_matches = int(row.get("termMatches") or 1)
                path_scores[path] = (
                    path_scores.get(path, 0.0)
                    + (term_matches * 4.0)
                    + (bounded_anchor_limit - index)
                )

        selected_paths = [
            path
            for path, _score in sorted(path_scores.items(), key=lambda item: (-item[1], item[0]))[
                :bounded_file_limit
            ]
        ]

        files = []
        flow_edges = []
        related_files = []
        related_paths: list[str] = []
        if selected_paths:
            flow_edges = self.client.run(
                Q.CODE_FLOW_CONTEXT_EDGES,
                {
                    "project": project_name,
                    "paths": selected_paths,
                    "limit": edge_limit,
                    "include_tests": include_tests,
                },
            )
            edge_path_counts: dict[str, int] = {}
            related_candidates: list[str] = []

            def add_related_candidate(path: Any) -> None:
                if (
                    isinstance(path, str)
                    and path
                    and path not in selected_paths
                    and path not in related_candidates
                ):
                    related_candidates.append(path)

            for selected_path in selected_paths:
                for edge in flow_edges:
                    if edge.get("callerPath") == selected_path:
                        add_related_candidate(edge.get("calleePath"))
                    if edge.get("calleePath") == selected_path:
                        add_related_candidate(edge.get("callerPath"))
            for edge in flow_edges:
                for key in ("callerPath", "calleePath"):
                    path = edge.get(key)
                    if isinstance(path, str) and path and path not in selected_paths:
                        edge_path_counts[path] = edge_path_counts.get(path, 0) + 1
            for path, _count in sorted(
                edge_path_counts.items(), key=lambda item: (-item[1], item[0])
            ):
                add_related_candidate(path)
            related_paths = related_candidates[:2]

        outlined_related_paths = related_paths[:related_file_limit]
        outline_paths = [*selected_paths, *outlined_related_paths]
        if outline_paths:
            file_context = self.code_file_context(
                outline_paths,
                project=project_name,
                limit_files=len(outline_paths),
                symbol_limit=flow_symbol_limit,
                include_tests=include_tests,
                output_format="json",
            )
            all_files = file_context["files"]
            selected_path_set = set(selected_paths)
            related_path_set = set(outlined_related_paths)
            files = [row for row in all_files if row.get("path") in selected_path_set]
            related_files = [row for row in all_files if row.get("path") in related_path_set]

        result: dict[str, Any] = {
            "project": project_name,
            "anchors": semantic_rows,
            "files": files,
            "relatedFiles": related_files,
            "flowEdges": flow_edges,
        }
        if normalized_detail == "full":
            result["lexicalAnchors"] = lexical_rows
        rows_for_meta = semantic_rows + flow_edges + related_files
        if normalized_detail == "full":
            rows_for_meta = rows_for_meta + lexical_rows
        return self._finalize_response(
            services._with_result_meta(
                result,
                rows_for_meta,
                limit=bounded_anchor_limit,
            ),
            output_format,
        )
