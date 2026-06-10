"""FastMCP registration for Memgraph Ingester tools."""

from __future__ import annotations

import inspect
import json
from collections.abc import Callable
from functools import wraps
from typing import Annotated, Any

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations
from memgraph_ingester_tool import MemgraphTools, ToolConfig, ToolError
from memgraph_ingester_tool.tools import (
    CALL_GRAPH_LIMIT,
    DISCOVERY_LIMIT,
    LOOKUP_LIMIT,
    MEMBER_LIMIT,
)
from pydantic import Field


def _compact_json_response(response: Any) -> str:
    return json.dumps(response, ensure_ascii=False, separators=(",", ":"), default=str)


_CONNECTIVITY_MARKERS = ("ServiceUnavailable", "Connection", "Auth", "Socket", "Timeout")
_CONNECTION_HINT = (
    "Check the Memgraph connection: MEMGRAPH_TOOLS_BOLT_URI (or legacy "
    "MEMGRAPH_INGESTER_MCP_BOLT_URI), credentials, and that the database is running."
)


def _shape_unexpected_error(exc: Exception) -> ToolError:
    """Rewrap a non-ToolError failure with a compact, actionable message."""

    name = type(exc).__name__
    detail = str(exc).strip() or name
    connectivity = isinstance(exc, OSError) or any(m in name for m in _CONNECTIVITY_MARKERS)
    if connectivity:
        return ToolError(f"Memgraph is unreachable ({name}): {detail}. {_CONNECTION_HINT}")
    return ToolError(
        f"Unexpected {name} while executing the tool: {detail}. "
        "Verify the arguments; if the failure persists, check the Memgraph server."
    )


READ_ONLY_TOOL = ToolAnnotations(
    readOnlyHint=True,
    destructiveHint=False,
    idempotentHint=True,
    openWorldHint=False,
)
WRITE_TOOL = ToolAnnotations(
    readOnlyHint=False,
    destructiveHint=False,
    idempotentHint=True,
    openWorldHint=False,
)
DESTRUCTIVE_TOOL = ToolAnnotations(
    readOnlyHint=False,
    destructiveHint=True,
    idempotentHint=True,
    openWorldHint=False,
)


ProjectParam = Annotated[
    str | None,
    Field(description="Indexed project name; omit to use the server's configured default project."),
]
FormatParam = Annotated[
    str,
    Field(description="'table_json' returns cols+rows (most compact); 'json' returns row objects."),
]
SkipParam = Annotated[
    int,
    Field(
        ge=0,
        description="Rows to skip for pagination; pass meta.nextSkip from the previous page.",
    ),
]
LimitParam = Annotated[
    int,
    Field(
        ge=1,
        description=(
            "Maximum rows on the first page; keep the compact default and paginate via "
            "meta.nextSkip when meta.hasMore is true."
        ),
    ),
]
IncludeTestsParam = Annotated[
    bool,
    Field(
        description=(
            "Include test-path code (src/test/, tests/, ...); keep false except for test "
            "coverage or test repair work."
        )
    ),
]
IncludeCountParam = Annotated[
    bool,
    Field(
        description=(
            "Add exact meta.totalCount via an extra count query; request only when the full "
            "count is needed."
        )
    ),
]
CompactRowsParam = Annotated[
    bool,
    Field(
        description=(
            "Keep rows to identifiers and source ranges; false adds full signatures, "
            "modifiers, and visibility (larger responses)."
        )
    ),
]
PathContainsParam = Annotated[
    str | None,
    Field(description="Only rows whose file path contains this substring."),
]
MemoryTypeParam = Annotated[
    str,
    Field(
        description=("One of: Decision, ADR, Rule, Context, Finding, Task, Risk, Question, Idea.")
    ),
]
MemoryIdParam = Annotated[
    str,
    Field(description="Stable descriptive memory id, e.g. 'TASK-<topic>-<name>'."),
]
RefreshChunkParam = Annotated[
    bool,
    Field(description="Rebuild the derived MemoryChunk after the write."),
]
EmbedParam = Annotated[
    bool,
    Field(description=("Refresh the chunk embedding; pass false for temporary in-flight updates.")),
]


SERVER_INSTRUCTIONS = """\
Code knowledge-graph tools for projects indexed by memgraph-ingester. Usage discipline:
- Use these tools for structure, relationships, and discovery; once target files and line
  ranges are identified, switch to reading source. Do not interleave graph lookups into an
  edit-compile-test loop — the exceptions are code_impact when a method signature changes,
  code_test_context when a test fails, and code_hierarchy before declaration changes.
- Compact defaults suffice. Paginate with meta.nextSkip only when meta.hasMore is true and
  the extra rows are needed; when a response carries meta.note, follow it — it flags
  exhausted matches and cheaper alternatives. Pass compact=false or include_count=true
  only when those fields are required for the answer.
- If the client defers tool schemas, load every code_* tool you plan to use in one batch
  (one ToolSearch/select call), not one tool per call.
- code_search / code_text_search hits are discovery anchors, not evidence — verify with an
  exact lookup or source. If two probes overlap or return nothing, stop probing and switch
  to exact lookups or source.
- To enumerate one class's methods use code_lookup_type(include_members=true) or
  code_file_context, not paginated code_lookup_methods.
- Refactor blast radius: code_impact, with view="files" for a risk-ranked file list.
  Performance audits: code_hot_paths + code_operation_hot_paths; results are pre-ranked,
  so source-verify only the top suspects.
- Responses are compact JSON (table_json: cols + rows); absent keys mean null or empty,
  never an error.
- Minimize total tool-call turns: batch independent calls in one message, and prefer one
  full source read over repeated small ranged reads of the same file. Each new turn
  re-reads the full accumulated cached context — turn count x context size dominates cost
  more than individual response sizes. For implementation tasks: (1) issue all MCP discovery
  calls in one batched turn, (2) read all needed source files in a second batched turn,
  (3) then edit. This two-phase discipline caps the cache multiplier.
- Type family enumeration: use code_hierarchy over separate code_lookup_type calls per
  class — one call returns all implementors or subclasses instead of N calls.
"""


def create_server(
    config: ToolConfig | None = None,
    client: Any | None = None,
) -> FastMCP:
    """Create a FastMCP server with high-level Memgraph Ingester tools."""

    resolved_config = config or ToolConfig.from_environment()
    tools = MemgraphTools(resolved_config, client=client)
    mcp = FastMCP("memgraph-ingester", instructions=SERVER_INSTRUCTIONS)

    def compact_tool(
        annotations: ToolAnnotations,
    ) -> Callable[[Callable[..., dict[str, Any]]], Callable[..., str]]:
        def register(fn: Callable[..., dict[str, Any]]) -> Callable[..., str]:
            # eval_str resolves the postponed (string) annotations so pydantic can see
            # the Annotated[..., Field(...)] parameter metadata on the wrapper.
            signature = inspect.signature(fn, eval_str=True)

            @wraps(fn)
            def wrapper(*args: Any, **kwargs: Any) -> str:
                try:
                    return _compact_json_response(fn(*args, **kwargs))
                except ToolError:
                    raise
                except Exception as exc:
                    raise _shape_unexpected_error(exc) from exc

            wrapper.__signature__ = signature.replace(return_annotation=str)  # type: ignore[attr-defined]
            mcp.tool(structured_output=False, annotations=annotations)(wrapper)
            return wrapper

        return register

    @compact_tool(READ_ONLY_TOOL)
    def server_status(project: ProjectParam = None) -> dict[str, Any]:
        """Summarize graph inventory, memory counts, and vector indexes for a project."""

        return tools.server_status(project)

    @compact_tool(READ_ONLY_TOOL)
    def code_orientation(
        project: ProjectParam = None,
        limit: Annotated[int, Field(ge=1, description="Maximum rows per section.")] = 30,
        sections: Annotated[
            list[str] | None,
            Field(
                description=(
                    "Subset of ['languages', 'packages', 'largestTypes', 'crossOwnerCalls']; "
                    "omit for all sections."
                )
            ),
        ] = None,
    ) -> dict[str, Any]:
        """Return a compact code graph orientation for an indexed project."""

        return tools.code_orientation(project, limit, sections)

    @compact_tool(READ_ONLY_TOOL)
    def code_search(
        query: Annotated[
            str,
            Field(
                description=(
                    "Concept, identifier, or natural-language query; vector and lexical "
                    "hits are fused by reciprocal rank."
                )
            ),
        ],
        project: ProjectParam = None,
        limit: LimitParam = DISCOVERY_LIMIT,
        include_tests: IncludeTestsParam = False,
        include_text: Annotated[
            bool,
            Field(
                description=(
                    "Include a chunk text preview of up to text_limit characters per row "
                    "(inflates response size)."
                )
            ),
        ] = False,
        text_limit: Annotated[
            int,
            Field(ge=1, description="Maximum preview characters per row when include_text=true."),
        ] = 160,
        dedupe_by_source: Annotated[
            bool,
            Field(
                description=(
                    "Collapse multiple chunks of the same source symbol into the best-scoring hit."
                )
            ),
        ] = True,
        kinds: Annotated[
            list[str] | None,
            Field(
                description=(
                    "Only chunks of these definition kinds, e.g. "
                    "['class', 'method', 'field', 'file']."
                )
            ),
        ] = None,
        path_prefixes: Annotated[
            list[str] | None,
            Field(description="Only chunks whose file path starts with one of these prefixes."),
        ] = None,
        path_contains: PathContainsParam = None,
        owner_fragment: Annotated[
            str | None,
            Field(description="Only chunks whose owning type name or FQN contains this fragment."),
        ] = None,
        min_score: Annotated[
            float,
            Field(
                ge=0.0,
                description="Drop hits with a fused score below this threshold; 0.0 disables.",
            ),
        ] = 0.0,
        include_secondary: Annotated[
            bool,
            Field(
                description=(
                    "Also search secondary-role chunks: constants, accessors, record "
                    "components, synthetic/module chunks."
                )
            ),
        ] = False,
        rag_roles: Annotated[
            list[str] | None,
            Field(description="Explicit chunk roles to search; default ['primary', 'file']."),
        ] = None,
        include_keys: Annotated[
            bool,
            Field(description="Include chunk/source id and RAG role columns (debugging aid)."),
        ] = False,
        format: FormatParam = "table_json",
    ) -> dict[str, Any]:
        """Hybrid CodeChunk search: vector + lexical signals fused by reciprocal rank.
        Hits are discovery anchors, not evidence — verify with an exact lookup or source.
        If two probes overlap or return nothing, switch to exact lookups or source instead
        of reformulating a third time."""

        return tools.code_search(
            query=query,
            project=project,
            limit=limit,
            include_tests=include_tests,
            include_text=include_text,
            text_limit=text_limit,
            dedupe_by_source=dedupe_by_source,
            kinds=kinds,
            path_prefixes=path_prefixes,
            path_contains=path_contains,
            owner_fragment=owner_fragment,
            min_score=min_score,
            include_secondary=include_secondary,
            rag_roles=rag_roles,
            include_keys=include_keys,
            output_format=format,
        )

    @compact_tool(READ_ONLY_TOOL)
    def code_text_search(
        query: Annotated[
            str | None,
            Field(
                description="Free-text query; split into lexical terms and ranked by termMatches."
            ),
        ] = None,
        project: ProjectParam = None,
        all_terms: Annotated[
            list[str] | None,
            Field(description="Terms that must all be present in a chunk."),
        ] = None,
        any_terms: Annotated[
            list[str] | None,
            Field(description="Terms of which at least one must be present."),
        ] = None,
        limit: LimitParam = DISCOVERY_LIMIT,
        include_tests: IncludeTestsParam = False,
        include_text: Annotated[
            bool,
            Field(
                description=(
                    "Include a chunk text preview of up to text_limit characters per row "
                    "(inflates response size)."
                )
            ),
        ] = False,
        text_limit: Annotated[
            int,
            Field(ge=1, description="Maximum preview characters per row when include_text=true."),
        ] = 160,
        kinds: Annotated[
            list[str] | None,
            Field(
                description=(
                    "Only chunks of these definition kinds, e.g. "
                    "['class', 'method', 'field', 'file']."
                )
            ),
        ] = None,
        include_secondary: Annotated[
            bool,
            Field(
                description=(
                    "Also search secondary-role chunks: constants, accessors, record "
                    "components, synthetic/module chunks."
                )
            ),
        ] = False,
        rag_roles: Annotated[
            list[str] | None,
            Field(description="Explicit chunk roles to search; default ['primary', 'file']."),
        ] = None,
        path_contains: PathContainsParam = None,
        format: FormatParam = "table_json",
    ) -> dict[str, Any]:
        """Search indexed chunk text lexically with compact source-linked rows.
        Hits are discovery anchors, not evidence — verify with an exact lookup or source.
        If two probes overlap or return nothing, switch to exact lookups or source instead
        of reformulating a third time."""

        return tools.code_text_search(
            query=query,
            project=project,
            all_terms=all_terms,
            any_terms=any_terms,
            limit=limit,
            include_tests=include_tests,
            include_text=include_text,
            text_limit=text_limit,
            kinds=kinds,
            include_secondary=include_secondary,
            rag_roles=rag_roles,
            path_contains=path_contains,
            output_format=format,
        )

    @compact_tool(READ_ONLY_TOOL)
    def code_discovery_context(
        query: Annotated[
            str,
            Field(
                description=(
                    "Concept query; top semantic anchors are expanded with bounded exact context."
                )
            ),
        ],
        project: ProjectParam = None,
        limit: Annotated[int, Field(ge=1, description="Maximum semantic anchors to expand.")] = 3,
        include_tests: IncludeTestsParam = False,
        neighbor_limit: Annotated[
            int,
            Field(ge=1, description="Maximum caller/callee/file context rows per anchor."),
        ] = 3,
        format: FormatParam = "table_json",
    ) -> dict[str, Any]:
        """Return top semantic anchors plus bounded exact/call/file context."""

        return tools.code_discovery_context(
            query=query,
            project=project,
            limit=limit,
            include_tests=include_tests,
            neighbor_limit=neighbor_limit,
            output_format=format,
        )

    @compact_tool(READ_ONLY_TOOL)
    def code_file_context(
        path_fragments: Annotated[
            list[str] | None,
            Field(description="File path fragments to outline, e.g. ['tools/_guards.py']."),
        ] = None,
        project: ProjectParam = None,
        limit_files: Annotated[
            int, Field(ge=1, description="Maximum matched files to outline.")
        ] = 5,
        symbol_limit: Annotated[
            int, Field(ge=1, description="Maximum top types/methods/fields per file.")
        ] = 8,
        include_tests: IncludeTestsParam = False,
        format: FormatParam = "table_json",
    ) -> dict[str, Any]:
        """Return compact file outlines with chunk roles and top symbols."""

        return tools.code_file_context(
            path_fragments=path_fragments,
            project=project,
            limit_files=limit_files,
            symbol_limit=symbol_limit,
            include_tests=include_tests,
            output_format=format,
        )

    @compact_tool(READ_ONLY_TOOL)
    def code_flow_context(
        query: Annotated[
            str,
            Field(
                description=(
                    "Workflow or feature-path question; one response returns anchors, file "
                    "outlines, and call edges identifying the likely hot files."
                )
            ),
        ],
        project: ProjectParam = None,
        limit_files: Annotated[int, Field(ge=1, description="Maximum files to outline.")] = 3,
        anchor_limit: Annotated[
            int, Field(ge=1, description="Maximum semantic and lexical anchors.")
        ] = 3,
        symbol_limit: Annotated[
            int, Field(ge=1, description="Maximum symbols per file outline.")
        ] = 3,
        include_tests: IncludeTestsParam = False,
        detail: Annotated[str, Field(description="'compact' or 'full' file outlines.")] = "compact",
        format: FormatParam = "table_json",
    ) -> dict[str, Any]:
        """Return semantic and lexical anchors, file outlines, and nearby call edges."""

        return tools.code_flow_context(
            query=query,
            project=project,
            limit_files=limit_files,
            anchor_limit=anchor_limit,
            symbol_limit=symbol_limit,
            include_tests=include_tests,
            detail=detail,
            output_format=format,
        )

    @compact_tool(READ_ONLY_TOOL)
    def code_lookup_type(
        project: ProjectParam = None,
        type_name: Annotated[
            str | None,
            Field(description="Simple type name to match, e.g. 'MemgraphTools'."),
        ] = None,
        fqn: Annotated[
            str | None,
            Field(description="Fully qualified name; more precise than type_name when known."),
        ] = None,
        include_members: Annotated[
            bool,
            Field(
                description=(
                    "Enumerate the type's methods and fields in one call (preferred over "
                    "paginating code_lookup_methods)."
                )
            ),
        ] = False,
        include_tests: IncludeTestsParam = False,
        member_limit: Annotated[
            int,
            Field(ge=1, description="Maximum member rows per type when include_members=true."),
        ] = MEMBER_LIMIT,
        member_summary: Annotated[
            bool,
            Field(description="Add per-kind member counts instead of full member rows."),
        ] = False,
        limit: LimitParam = LOOKUP_LIMIT,
        include_count: IncludeCountParam = False,
        compact: CompactRowsParam = True,
        format: FormatParam = "table_json",
    ) -> dict[str, Any]:
        """Look up classes, interfaces, or annotations by simple name or FQN.
        include_members=true enumerates one class's members in a single call (prefer over
        paginating code_lookup_methods). compact=true (default) returns type metadata and
        member identifiers — sufficient for almost all lookups. compact=false adds
        modifiers, visibility, and annotations; use it only when those specific fields are
        required, as it inflates response size and downstream cache cost."""

        return tools.code_lookup_type(
            project=project,
            type_name=type_name,
            fqn=fqn,
            include_members=include_members,
            include_tests=include_tests,
            member_limit=member_limit,
            member_summary=member_summary,
            limit=limit,
            include_count=include_count,
            compact=compact,
            output_format=format,
        )

    @compact_tool(READ_ONLY_TOOL)
    def code_lookup_methods(
        signature_fragment: Annotated[
            str,
            Field(
                description=(
                    "Method name or signature fragment, e.g. 'ClassName.method'; rows whose "
                    "owner exactly matches a fragment term rank first."
                )
            ),
        ],
        project: ProjectParam = None,
        skip: SkipParam = 0,
        limit: LimitParam = LOOKUP_LIMIT,
        include_tests: IncludeTestsParam = False,
        compact: CompactRowsParam = True,
        include_count: IncludeCountParam = False,
        format: FormatParam = "table_json",
    ) -> dict[str, Any]:
        """Find methods by signature fragment and return exact source ranges.
        Compact rows (owner, name, path, startLine, endLine) suffice for range reads;
        compact=false only when full signatures/modifiers are needed. Methods whose owner
        exactly matches a fragment term rank first. To enumerate all methods of one class,
        prefer code_lookup_type(include_members=true) or code_file_context instead of
        paginating here; paginate only when meta.hasMore is true and the rows are needed.
        Stop paginating when meta.note reports owner-exact matches are exhausted —
        later pages only contain signatures that reference the fragment."""

        return tools.code_lookup_methods(
            signature_fragment=signature_fragment,
            project=project,
            skip=skip,
            limit=limit,
            include_tests=include_tests,
            compact=compact,
            include_count=include_count,
            output_format=format,
        )

    @compact_tool(READ_ONLY_TOOL)
    def code_lookup_field(
        field_fragment: Annotated[
            str,
            Field(description="Field or constant name/FQN fragment."),
        ],
        project: ProjectParam = None,
        skip: SkipParam = 0,
        limit: LimitParam = LOOKUP_LIMIT,
        include_tests: IncludeTestsParam = False,
        compact: CompactRowsParam = True,
        include_count: IncludeCountParam = False,
        format: FormatParam = "table_json",
    ) -> dict[str, Any]:
        """Find fields by FQN/name fragment and return source-linked rows."""

        return tools.code_lookup_field(
            field_fragment=field_fragment,
            project=project,
            skip=skip,
            limit=limit,
            include_tests=include_tests,
            compact=compact,
            include_count=include_count,
            output_format=format,
        )

    @compact_tool(READ_ONLY_TOOL)
    def code_lookup_file(
        path_fragment: Annotated[
            str,
            Field(description="File path fragment, e.g. 'queries/code'."),
        ],
        project: ProjectParam = None,
        skip: SkipParam = 0,
        limit: LimitParam = LOOKUP_LIMIT,
        include_tests: IncludeTestsParam = False,
        compact: CompactRowsParam = True,
        include_count: IncludeCountParam = False,
        format: FormatParam = "table_json",
    ) -> dict[str, Any]:
        """Find indexed files by path fragment with definition and chunk counts."""

        return tools.code_lookup_file(
            path_fragment=path_fragment,
            project=project,
            skip=skip,
            limit=limit,
            include_tests=include_tests,
            compact=compact,
            include_count=include_count,
            output_format=format,
        )

    @compact_tool(READ_ONLY_TOOL)
    def code_impact(
        signature_fragment: Annotated[
            str,
            Field(
                description=(
                    "Signature fragment of the method(s) being changed; multiple "
                    "targetMethods in the response means the fragment is ambiguous."
                )
            ),
        ],
        project: ProjectParam = None,
        skip: SkipParam = 0,
        limit: LimitParam = CALL_GRAPH_LIMIT,
        depth: Annotated[
            int,
            Field(ge=1, description="Caller depth: 1 = direct callers only, 2 adds their callers."),
        ] = 2,
        include_tests: Annotated[
            bool,
            Field(description="Include test callers in the blast radius (recommended)."),
        ] = True,
        compact: CompactRowsParam = True,
        view: Annotated[
            str,
            Field(
                description=(
                    "'callers' for caller rows; 'files' for a pre-ranked deduplicated file "
                    "list with risk flags."
                )
            ),
        ] = "callers",
        format: FormatParam = "table_json",
    ) -> dict[str, Any]:
        """Map refactor impact for matching methods through direct and one-level callers.
        Use view='files' to get a pre-ranked deduplicated file list (role, depth,
        testCallerCount, crossPackageCount, risk) — the direct answer for blast-radius tasks."""

        return tools.code_impact(
            signature_fragment=signature_fragment,
            project=project,
            skip=skip,
            limit=limit,
            depth=depth,
            include_tests=include_tests,
            compact=compact,
            view=view,
            output_format=format,
        )

    @compact_tool(READ_ONLY_TOOL)
    def code_callers(
        callee_fragment: Annotated[
            str,
            Field(description="Signature fragment of the callee whose callers to list."),
        ],
        project: ProjectParam = None,
        skip: SkipParam = 0,
        limit: LimitParam = CALL_GRAPH_LIMIT,
        include_tests: IncludeTestsParam = False,
        compact: CompactRowsParam = True,
        include_count: IncludeCountParam = False,
        format: FormatParam = "table_json",
    ) -> dict[str, Any]:
        """List methods that call matching callee signatures."""

        return tools.code_callers(
            callee_fragment=callee_fragment,
            project=project,
            skip=skip,
            limit=limit,
            include_tests=include_tests,
            compact=compact,
            include_count=include_count,
            output_format=format,
        )

    @compact_tool(READ_ONLY_TOOL)
    def code_method_context(
        signature_fragment: Annotated[
            str,
            Field(description="Method name or signature fragment to trace."),
        ],
        project: ProjectParam = None,
        method_limit: Annotated[
            int, Field(ge=1, description="Maximum matching methods.")
        ] = DISCOVERY_LIMIT,
        neighbor_limit: Annotated[
            int, Field(ge=1, description="Maximum callers and callees per method.")
        ] = DISCOVERY_LIMIT,
        include_tests: IncludeTestsParam = False,
        compact: CompactRowsParam = True,
        format: FormatParam = "table_json",
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

    @compact_tool(READ_ONLY_TOOL)
    def code_callees(
        caller_fragment: Annotated[
            str,
            Field(description="Signature fragment of the caller whose callees to list."),
        ],
        project: ProjectParam = None,
        skip: SkipParam = 0,
        limit: LimitParam = CALL_GRAPH_LIMIT,
        include_tests: IncludeTestsParam = False,
        compact: CompactRowsParam = True,
        include_count: IncludeCountParam = False,
        format: FormatParam = "table_json",
    ) -> dict[str, Any]:
        """List callees invoked by matching caller signatures."""

        return tools.code_callees(
            caller_fragment=caller_fragment,
            project=project,
            skip=skip,
            limit=limit,
            include_tests=include_tests,
            compact=compact,
            include_count=include_count,
            output_format=format,
        )

    @compact_tool(READ_ONLY_TOOL)
    def code_hot_paths(
        project: ProjectParam = None,
        limit: LimitParam = DISCOVERY_LIMIT,
        include_tests: IncludeTestsParam = False,
        include_evidence: Annotated[
            bool,
            Field(
                description=("Add path/startLine/endLine per row so no follow-up lookup is needed.")
            ),
        ] = False,
        sections: Annotated[
            list[str] | None,
            Field(
                description=(
                    "Subset of ['largestTypes', 'longestMethods', 'fanIn', 'fanOut']; "
                    "default ['fanIn', 'longestMethods', 'fanOut']."
                )
            ),
        ] = None,
        format: FormatParam = "table_json",
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

    @compact_tool(READ_ONLY_TOOL)
    def code_operation_hot_paths(
        project: ProjectParam = None,
        sink_fragments: Annotated[
            list[str] | None,
            Field(
                description=(
                    "Operation-like sink name fragments to count; defaults cover "
                    "run/query/write/read/save/delete and similar."
                )
            ),
        ] = None,
        owner_fragment: Annotated[
            str | None,
            Field(
                description=(
                    "Only methods whose owning type matches this fragment (subsystem narrowing)."
                )
            ),
        ] = None,
        path_contains: PathContainsParam = None,
        limit: LimitParam = DISCOVERY_LIMIT,
        include_tests: IncludeTestsParam = False,
        format: FormatParam = "table_json",
    ) -> dict[str, Any]:
        """Return methods with many calls to operation-like sinks such as run/query/write."""

        return tools.code_operation_hot_paths(
            project=project,
            sink_fragments=sink_fragments,
            owner_fragment=owner_fragment,
            path_contains=path_contains,
            limit=limit,
            include_tests=include_tests,
            output_format=format,
        )

    @compact_tool(READ_ONLY_TOOL)
    def code_resource_risk_scan(
        project: ProjectParam = None,
        path_contains: PathContainsParam = None,
        extensions: Annotated[
            list[str] | None,
            Field(description="Resource file extensions to scan, e.g. ['cypher', 'sql', 'yaml']."),
        ] = None,
        limit: LimitParam = DISCOVERY_LIMIT,
        include_tests: IncludeTestsParam = False,
        format: FormatParam = "table_json",
    ) -> dict[str, Any]:
        """Return compact heuristic risks in query/config/resource files."""

        return tools.code_resource_risk_scan(
            project=project,
            path_contains=path_contains,
            extensions=extensions,
            limit=limit,
            include_tests=include_tests,
            output_format=format,
        )

    @compact_tool(READ_ONLY_TOOL)
    def code_quality_stats(
        project: ProjectParam = None,
        include_tests: IncludeTestsParam = False,
        limit: Annotated[
            int, Field(ge=1, description="Maximum rows per metric section.")
        ] = DISCOVERY_LIMIT,
        format: FormatParam = "table_json",
    ) -> dict[str, Any]:
        """Return compact graph-wide code quality and quantity metrics."""

        return tools.code_quality_stats(project, include_tests, limit, format)

    @compact_tool(READ_ONLY_TOOL)
    def code_hierarchy(
        fqn: Annotated[
            str,
            Field(
                description=(
                    "Fully qualified type name to expand into parents, children, "
                    "interfaces, and implementors."
                )
            ),
        ],
        project: ProjectParam = None,
        format: FormatParam = "table_json",
    ) -> dict[str, Any]:
        """Return class ancestry, children, interfaces, and interface implementors.
        For type family enumeration (all subclasses of a base or all implementors of an
        interface), one call here is more efficient than separate code_lookup_type calls
        per class."""

        return tools.code_hierarchy(fqn, project, output_format=format)

    @compact_tool(READ_ONLY_TOOL)
    def code_test_context(
        test_fragment: Annotated[
            str,
            Field(description="Failing test class or method name fragment."),
        ],
        project: ProjectParam = None,
        limit: Annotated[
            int,
            Field(ge=1, description="Maximum matching tests and nearby test files."),
        ] = DISCOVERY_LIMIT,
        production_limit: Annotated[
            int, Field(ge=1, description="Maximum production callee rows.")
        ] = DISCOVERY_LIMIT,
        format: FormatParam = "table_json",
    ) -> dict[str, Any]:
        """Return matching tests and production callees for CI/test triage."""

        return tools.code_test_context(
            test_fragment=test_fragment,
            project=project,
            limit=limit,
            production_limit=production_limit,
            output_format=format,
        )

    @compact_tool(READ_ONLY_TOOL)
    def memory_orientation(
        project: ProjectParam = None,
        compact: Annotated[
            bool,
            Field(description="Return ids, titles, and status only; omit memory bodies."),
        ] = False,
    ) -> dict[str, Any]:
        """Return rules plus open findings, tasks, questions, and risks."""

        return tools.memory_orientation(project, compact)

    @compact_tool(READ_ONLY_TOOL)
    def memory_schema(
        memory_type: Annotated[
            str | None,
            Field(
                description=(
                    "Scope to one memory type (Decision, ADR, Rule, Context, Finding, "
                    "Task, Risk, Question, Idea); omit for all."
                )
            ),
        ] = None,
    ) -> dict[str, Any]:
        """Return allowed memory types, fields, controlled values, and CodeRef targets."""

        return tools.memory_schema(memory_type)

    @compact_tool(READ_ONLY_TOOL)
    def memory_search(
        query: Annotated[
            str,
            Field(description="Concise, hypothesis-specific query over MemoryChunk embeddings."),
        ],
        project: ProjectParam = None,
        limit: Annotated[int, Field(ge=1, description="Maximum hits.")] = 5,
    ) -> dict[str, Any]:
        """Search MemoryChunk embeddings and return index-only memory hits."""

        return tools.memory_search(query, project, limit)

    @compact_tool(READ_ONLY_TOOL)
    def memory_get(
        memory_id: MemoryIdParam,
        project: ProjectParam = None,
    ) -> dict[str, Any]:
        """Fetch one canonical memory node with resolved CodeRef targets."""

        return tools.memory_get(memory_id, project)

    @compact_tool(DESTRUCTIVE_TOOL)
    def delete_memory(
        memory_id: MemoryIdParam,
        project: ProjectParam = None,
    ) -> dict[str, Any]:
        """Delete one Memory node plus its derived chunk and orphan CodeRefs."""

        return tools.delete_memory(memory_id, project)

    @compact_tool(WRITE_TOOL)
    def memory_upsert(
        memory_type: MemoryTypeParam,
        memory_id: MemoryIdParam,
        fields: Annotated[
            dict[str, Any],
            Field(
                description=(
                    "Type-specific fields; call memory_schema first when allowed fields or "
                    "controlled values are not already known."
                )
            ),
        ],
        project: ProjectParam = None,
        code_ref: Annotated[
            dict[str, str] | None,
            Field(
                description=(
                    "Optional code link, e.g. {'type': 'Class', 'key': '<fqn>'}; allowed "
                    "types: Code, Package, File, Class, Interface, Annotation, Method, Field."
                )
            ),
        ] = None,
        refresh_chunk: RefreshChunkParam = True,
        embed: EmbedParam = True,
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

    @compact_tool(WRITE_TOOL)
    def memory_update_status(
        memory_type: MemoryTypeParam,
        memory_id: MemoryIdParam,
        status: Annotated[
            str,
            Field(
                description=(
                    "New lifecycle status; controlled per memory type — see memory_schema."
                )
            ),
        ],
        project: ProjectParam = None,
        refresh_chunk: RefreshChunkParam = True,
        embed: EmbedParam = True,
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

    @compact_tool(WRITE_TOOL)
    def memory_link_code_ref(
        memory_type: MemoryTypeParam,
        memory_id: MemoryIdParam,
        target_type: Annotated[
            str,
            Field(
                description=(
                    "CodeRef target label: Code, Package, File, Class, Interface, "
                    "Annotation, Method, or Field."
                )
            ),
        ],
        key: Annotated[
            str,
            Field(
                description=("Target identifier: FQN for types/members, indexed path for files.")
            ),
        ],
        project: ProjectParam = None,
        refresh_chunk: RefreshChunkParam = True,
        embed: EmbedParam = True,
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

    @compact_tool(WRITE_TOOL)
    def memory_refresh_chunk(
        memory_type: MemoryTypeParam,
        memory_id: MemoryIdParam,
        project: ProjectParam = None,
        embed: EmbedParam = True,
    ) -> dict[str, Any]:
        """Rebuild one derived MemoryChunk and optionally refresh its embedding."""

        return tools.memory_refresh_chunk(memory_type, memory_id, project, embed=embed)

    @compact_tool(WRITE_TOOL)
    def memory_refresh_embeddings(
        chunk_ids: Annotated[
            list[str],
            Field(description="Derived MemoryChunk ids to re-embed, e.g. ['MCH-TASK-...']."),
        ],
        project: ProjectParam = None,
    ) -> dict[str, Any]:
        """Refresh embeddings for selected MemoryChunk ids and stamp metadata."""

        return tools.memory_refresh_embeddings(chunk_ids, project)

    @compact_tool(READ_ONLY_TOOL)
    def raw_read_cypher(
        query: Annotated[
            str,
            Field(
                description=(
                    "Read-only Cypher; must be project-scoped (e.g. {project: $project}). "
                    "Write clauses and writeable procedures are rejected."
                )
            ),
        ],
        project: ProjectParam = None,
        parameters: Annotated[
            dict[str, Any] | None,
            Field(description="Query parameters; $project and $limit are injected when absent."),
        ] = None,
        limit: Annotated[
            int, Field(ge=1, description="Row cap; bounded to a maximum of 500.")
        ] = 200,
        format: FormatParam = "table_json",
    ) -> dict[str, Any]:
        """Run a project-scoped read-only Cypher query as a last-resort escape hatch."""

        return tools.raw_read_cypher(query, project, parameters, limit, format)

    return mcp


def main() -> None:
    """Start the stdio MCP server."""

    create_server().run()


__all__ = ["create_server", "main"]
