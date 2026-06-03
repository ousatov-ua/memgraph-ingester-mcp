"""FastMCP registration for Memgraph Ingester tools."""

from __future__ import annotations

from typing import Any

from mcp.server.fastmcp import FastMCP

from memgraph_ingester_mcp.config import MemgraphConfig
from memgraph_ingester_mcp.db import MemgraphClient
from memgraph_ingester_mcp.services import MemgraphIngesterTools


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
        limit: int = 10,
        include_text: bool = False,
        text_limit: int = 160,
        dedupe_by_source: bool = True,
    ) -> dict[str, Any]:
        """Search CodeChunk embeddings and return source-linked discovery hits."""

        return tools.code_search(query, project, limit, include_text, text_limit, dedupe_by_source)

    @mcp.tool()
    def code_lookup_type(
        project: str | None = None,
        type_name: str | None = None,
        fqn: str | None = None,
        include_members: bool = False,
        member_limit: int = 50,
        member_summary: bool = True,
        limit: int = 20,
    ) -> dict[str, Any]:
        """Look up classes, interfaces, or annotations by simple name or FQN."""

        return tools.code_lookup_type(
            project,
            type_name,
            fqn,
            include_members,
            member_limit,
            member_summary,
            limit,
        )

    @mcp.tool()
    def code_lookup_methods(
        signature_fragment: str,
        project: str | None = None,
        skip: int = 0,
        limit: int = 50,
        compact: bool = False,
    ) -> dict[str, Any]:
        """Find methods by signature fragment and return exact source ranges."""

        return tools.code_lookup_methods(signature_fragment, project, skip, limit, compact)

    @mcp.tool()
    def code_callers(
        callee_fragment: str,
        project: str | None = None,
        skip: int = 0,
        limit: int = 25,
        compact: bool = True,
    ) -> dict[str, Any]:
        """List methods that call matching callee signatures."""

        return tools.code_callers(callee_fragment, project, skip, limit, compact)

    @mcp.tool()
    def code_callees(
        caller_fragment: str,
        project: str | None = None,
        skip: int = 0,
        limit: int = 25,
        compact: bool = True,
    ) -> dict[str, Any]:
        """List callees invoked by matching caller signatures."""

        return tools.code_callees(caller_fragment, project, skip, limit, compact)

    @mcp.tool()
    def code_hot_paths(
        project: str | None = None,
        limit: int = 20,
        include_tests: bool = False,
        include_evidence: bool = True,
    ) -> dict[str, Any]:
        """Return compact hot-path candidates from type size, method size, fan-in, and fan-out."""

        return tools.code_hot_paths(project, limit, include_tests, include_evidence)

    @mcp.tool()
    def code_quality_stats(
        project: str | None = None,
        include_tests: bool = True,
        limit: int = 20,
    ) -> dict[str, Any]:
        """Return compact graph-wide code quality and quantity metrics."""

        return tools.code_quality_stats(project, include_tests, limit)

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
    ) -> dict[str, Any]:
        """Run a project-scoped read-only Cypher query as a last-resort escape hatch."""

        return tools.raw_read_cypher(query, project, parameters, limit)

    return mcp


def main() -> None:
    """Start the stdio MCP server."""

    create_server().run()


__all__ = ["create_server", "main"]
