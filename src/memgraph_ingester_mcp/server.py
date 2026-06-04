"""FastMCP registration for Memgraph Ingester tools."""

from __future__ import annotations

from typing import Any

from mcp.server.fastmcp import FastMCP

from memgraph_ingester_mcp.config import MemgraphConfig
from memgraph_ingester_mcp.db import MemgraphClient
from memgraph_ingester_mcp.services import (
    CALL_GRAPH_LIMIT,
    DISCOVERY_LIMIT,
    LOOKUP_LIMIT,
    MEMBER_LIMIT,
    MemgraphIngesterTools,
)


def create_server(
    config: MemgraphConfig | None = None,
    client: MemgraphClient | None = None,
) -> FastMCP:
    """Create a FastMCP server with high-level Memgraph Ingester tools."""

    resolved_config = config or MemgraphConfig.from_environment()
    tools = MemgraphIngesterTools(client or MemgraphClient(resolved_config), resolved_config)
    mcp = FastMCP("memgraph-ingester")

    @mcp.tool()
    def server_status(project: str | None = None) -> dict[str, Any]:
        """Summarize graph inventory, memory counts, and vector indexes for a project."""

        return tools.server_status(project)

    @mcp.tool()
    def code_orientation(
        project: str | None = None,
        limit: int = 30,
        sections: list[str] | None = None,
    ) -> dict[str, Any]:
        """Return a compact code graph orientation for an indexed project."""

        return tools.code_orientation(project, limit, sections)

    @mcp.tool()
    def code_search(
        query: str,
        project: str | None = None,
        limit: int = DISCOVERY_LIMIT,
        include_tests: bool = False,
        include_text: bool = False,
        text_limit: int = 160,
        dedupe_by_source: bool = True,
        format: str = "table_json",
    ) -> dict[str, Any]:
        """Search CodeChunk embeddings and return source-linked discovery hits."""

        return tools.code_search(
            query=query,
            project=project,
            limit=limit,
            include_tests=include_tests,
            include_text=include_text,
            text_limit=text_limit,
            dedupe_by_source=dedupe_by_source,
            output_format=format,
        )

    @mcp.tool()
    def code_lookup_type(
        project: str | None = None,
        type_name: str | None = None,
        fqn: str | None = None,
        include_members: bool = False,
        include_tests: bool = False,
        member_limit: int = MEMBER_LIMIT,
        member_summary: bool = False,
        limit: int = LOOKUP_LIMIT,
        compact: bool = True,
        format: str = "table_json",
    ) -> dict[str, Any]:
        """Look up classes, interfaces, or annotations by simple name or FQN."""

        return tools.code_lookup_type(
            project=project,
            type_name=type_name,
            fqn=fqn,
            include_members=include_members,
            include_tests=include_tests,
            member_limit=member_limit,
            member_summary=member_summary,
            limit=limit,
            compact=compact,
            output_format=format,
        )

    @mcp.tool()
    def code_lookup_methods(
        signature_fragment: str,
        project: str | None = None,
        skip: int = 0,
        limit: int = LOOKUP_LIMIT,
        include_tests: bool = False,
        compact: bool = True,
        format: str = "table_json",
    ) -> dict[str, Any]:
        """Find methods by signature fragment and return exact source ranges."""

        return tools.code_lookup_methods(
            signature_fragment=signature_fragment,
            project=project,
            skip=skip,
            limit=limit,
            include_tests=include_tests,
            compact=compact,
            output_format=format,
        )

    @mcp.tool()
    def code_lookup_field(
        field_fragment: str,
        project: str | None = None,
        skip: int = 0,
        limit: int = LOOKUP_LIMIT,
        include_tests: bool = False,
        compact: bool = True,
        format: str = "table_json",
    ) -> dict[str, Any]:
        """Find fields by FQN/name fragment and return source-linked rows."""

        return tools.code_lookup_field(
            field_fragment=field_fragment,
            project=project,
            skip=skip,
            limit=limit,
            include_tests=include_tests,
            compact=compact,
            output_format=format,
        )

    @mcp.tool()
    def code_lookup_file(
        path_fragment: str,
        project: str | None = None,
        skip: int = 0,
        limit: int = LOOKUP_LIMIT,
        include_tests: bool = False,
        compact: bool = True,
        format: str = "table_json",
    ) -> dict[str, Any]:
        """Find indexed files by path fragment with definition and chunk counts."""

        return tools.code_lookup_file(
            path_fragment=path_fragment,
            project=project,
            skip=skip,
            limit=limit,
            include_tests=include_tests,
            compact=compact,
            output_format=format,
        )

    @mcp.tool()
    def code_impact(
        signature_fragment: str,
        project: str | None = None,
        skip: int = 0,
        limit: int = CALL_GRAPH_LIMIT,
        depth: int = 2,
        include_tests: bool = True,
        compact: bool = True,
        format: str = "table_json",
    ) -> dict[str, Any]:
        """Map refactor impact for matching methods through direct and one-level callers."""

        return tools.code_impact(
            signature_fragment=signature_fragment,
            project=project,
            skip=skip,
            limit=limit,
            depth=depth,
            include_tests=include_tests,
            compact=compact,
            output_format=format,
        )

    @mcp.tool()
    def code_callers(
        callee_fragment: str,
        project: str | None = None,
        skip: int = 0,
        limit: int = CALL_GRAPH_LIMIT,
        include_tests: bool = False,
        compact: bool = True,
        format: str = "table_json",
    ) -> dict[str, Any]:
        """List methods that call matching callee signatures."""

        return tools.code_callers(
            callee_fragment=callee_fragment,
            project=project,
            skip=skip,
            limit=limit,
            include_tests=include_tests,
            compact=compact,
            output_format=format,
        )

    @mcp.tool()
    def code_method_context(
        signature_fragment: str,
        project: str | None = None,
        method_limit: int = DISCOVERY_LIMIT,
        neighbor_limit: int = DISCOVERY_LIMIT,
        include_tests: bool = False,
        compact: bool = True,
        format: str = "table_json",
    ) -> dict[str, Any]:
        """Return matching methods plus compact caller and callee context."""

        return tools.code_method_context(
            signature_fragment=signature_fragment,
            project=project,
            method_limit=method_limit,
            neighbor_limit=neighbor_limit,
            include_tests=include_tests,
            compact=compact,
            output_format=format,
        )

    @mcp.tool()
    def code_callees(
        caller_fragment: str,
        project: str | None = None,
        skip: int = 0,
        limit: int = CALL_GRAPH_LIMIT,
        include_tests: bool = False,
        compact: bool = True,
        format: str = "table_json",
    ) -> dict[str, Any]:
        """List callees invoked by matching caller signatures."""

        return tools.code_callees(
            caller_fragment=caller_fragment,
            project=project,
            skip=skip,
            limit=limit,
            include_tests=include_tests,
            compact=compact,
            output_format=format,
        )

    @mcp.tool()
    def code_hot_paths(
        project: str | None = None,
        limit: int = DISCOVERY_LIMIT,
        include_tests: bool = False,
        include_evidence: bool = False,
        sections: list[str] | None = None,
        format: str = "table_json",
    ) -> dict[str, Any]:
        """Return compact hot-path candidates from type size, method size, fan-in, and fan-out."""

        return tools.code_hot_paths(
            project,
            limit,
            include_tests,
            include_evidence,
            sections,
            format,
        )

    @mcp.tool()
    def code_quality_stats(
        project: str | None = None,
        include_tests: bool = False,
        limit: int = DISCOVERY_LIMIT,
        format: str = "table_json",
    ) -> dict[str, Any]:
        """Return compact graph-wide code quality and quantity metrics."""

        return tools.code_quality_stats(project, include_tests, limit, format)

    @mcp.tool()
    def code_hierarchy(fqn: str, project: str | None = None) -> dict[str, Any]:
        """Return class ancestry, children, interfaces, and interface implementors."""

        return tools.code_hierarchy(fqn, project)

    @mcp.tool()
    def memory_orientation(project: str | None = None, compact: bool = False) -> dict[str, Any]:
        """Return rules plus open findings, tasks, questions, and risks."""

        return tools.memory_orientation(project, compact)

    @mcp.tool()
    def memory_schema(memory_type: str | None = None) -> dict[str, Any]:
        """Return allowed memory types, fields, controlled values, and CodeRef targets."""

        return tools.memory_schema(memory_type)

    @mcp.tool()
    def memory_search(query: str, project: str | None = None, limit: int = 5) -> dict[str, Any]:
        """Search MemoryChunk embeddings and return index-only memory hits."""

        return tools.memory_search(query, project, limit)

    @mcp.tool()
    def memory_get(memory_id: str, project: str | None = None) -> dict[str, Any]:
        """Fetch one canonical memory node with resolved CodeRef targets."""

        return tools.memory_get(memory_id, project)

    @mcp.tool()
    def delete_memory(memory_id: str, project: str | None = None) -> dict[str, Any]:
        """Delete one Memory node plus its derived chunk and orphan CodeRefs."""

        return tools.delete_memory(memory_id, project)

    @mcp.tool()
    def memory_upsert(
        memory_type: str,
        memory_id: str,
        fields: dict[str, Any],
        project: str | None = None,
        code_ref: dict[str, str] | None = None,
        refresh_chunk: bool = True,
        embed: bool = True,
    ) -> dict[str, Any]:
        """Create or update an allowed Memory node and optionally link code."""

        return tools.memory_upsert(
            memory_type,
            memory_id,
            fields,
            project,
            code_ref,
            refresh_chunk,
            embed,
        )

    @mcp.tool()
    def memory_update_status(
        memory_type: str,
        memory_id: str,
        status: str,
        project: str | None = None,
        refresh_chunk: bool = True,
        embed: bool = True,
    ) -> dict[str, Any]:
        """Update a lifecycle status and refresh its MemoryChunk."""

        return tools.memory_update_status(
            memory_type,
            memory_id,
            status,
            project,
            refresh_chunk,
            embed,
        )

    @mcp.tool()
    def memory_link_code_ref(
        memory_type: str,
        memory_id: str,
        target_type: str,
        key: str,
        project: str | None = None,
        refresh_chunk: bool = True,
        embed: bool = True,
    ) -> dict[str, Any]:
        """Resolve and link a Memory node to a CodeRef target."""

        return tools.memory_link_code_ref(
            memory_type,
            memory_id,
            target_type,
            key,
            project,
            refresh_chunk=refresh_chunk,
            embed=embed,
        )

    @mcp.tool()
    def memory_refresh_chunk(
        memory_type: str,
        memory_id: str,
        project: str | None = None,
        embed: bool = True,
    ) -> dict[str, Any]:
        """Rebuild one derived MemoryChunk and optionally refresh its embedding."""

        return tools.memory_refresh_chunk(memory_type, memory_id, project, embed=embed)

    @mcp.tool()
    def memory_refresh_embeddings(
        chunk_ids: list[str],
        project: str | None = None,
    ) -> dict[str, Any]:
        """Refresh embeddings for selected MemoryChunk ids and stamp metadata."""

        return tools.memory_refresh_embeddings(chunk_ids, project)

    @mcp.tool()
    def raw_read_cypher(
        query: str,
        project: str | None = None,
        parameters: dict[str, Any] | None = None,
        limit: int = 200,
        format: str = "table_json",
    ) -> dict[str, Any]:
        """Run a project-scoped read-only Cypher query as a last-resort escape hatch."""

        return tools.raw_read_cypher(query, project, parameters, limit, format)

    return mcp


def main() -> None:
    """Start the stdio MCP server."""

    create_server().run()


__all__ = ["create_server", "main"]
