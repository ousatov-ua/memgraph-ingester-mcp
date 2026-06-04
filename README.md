# memgraph-ingester-mcp

`memgraph-ingester-mcp` exposes safe, high-level MCP tools for Memgraph graphs produced by
[`memgraph-ingester`](https://github.com/ousatov-ua/memgraph-ingester). It keeps agents out of long
Cypher instruction blocks for normal work while still offering a project-scoped read-only Cypher
escape hatch for unusual lookups.

The package is intended to be published to PyPI as `memgraph-ingester-mcp` under the
`ousatov-ua` account.

## Tools

Code graph tools:

- `server_status`: graph inventory, memory counts, and vector index status.
- `code_orientation`: compact package/type/call overview; pass `sections` to keep it focused.
- `code_quality_stats`: graph-wide code quality and quantity metrics; defaults to non-test code and a 5-row first page.
- `code_hot_paths`: compact hot-path candidates from size and call graph metrics; defaults to 5 owner/name rows per selected section.
- `code_search`: CodeChunk vector search for broad discovery; defaults to 5 non-test, deduped, range-first hits and omits text/source keys unless requested; supports compact filters.
- `code_text_search`: lexical search over indexed chunk text/path/source ids for concrete-term discovery.
- `code_discovery_context`: semantic discovery plus bounded exact/caller/callee/file context in one compact response.
- `code_lookup_type`: exact class/interface/annotation lookup; test code and member expansion are opt-in.
- `code_lookup_methods`: exact method lookup with source ranges; defaults to 10 compact owner/name/path rows.
- `code_lookup_field`: exact field/constant lookup by FQN or name fragment; defaults to compact owner/name/FQN/path rows.
- `code_lookup_file`: indexed file/resource lookup by path fragment with definition and RAG chunk counts.
- `code_callers`, `code_callees`: compact, paginated, non-test, range-first call graph lookups; default first page is 10 rows.
- `code_method_context`: bundled exact method lookup plus caller and callee context; use instead of separate lookup/callers/callees calls while tracing one target.
- `code_impact`: refactor blast-radius lookup for matching method signatures; returns target methods plus depth-1/depth-2 callers, test flags, and file/package boundary flags.
- `code_operation_hot_paths`: operation-sink hot paths for performance work, grouping methods that call APIs like run/query/write/read/save/delete; accepts owner/path filters for subsystem-focused audits.
- `code_test_context`: failing-test/CI triage from a test name or class fragment to tests, test files, and production callees.
- `code_hierarchy`: class ancestry, children, interfaces, and interface implementors.

Memory tools:

- `memory_orientation`: rules plus open findings, tasks, questions, and risks; `compact=true` omits body fields.
- `memory_schema`: allowed memory types, upsert fields, controlled values, and CodeRef targets.
- `memory_search`: MemoryChunk vector search with index-only hit metadata.
- `memory_get`: canonical memory node plus resolved CodeRefs.
- `memory_upsert`: create/update Decision, ADR, Rule, Context, Finding, Task, Risk, Question, or Idea.
- `memory_update_status`: lifecycle status update.
- `delete_memory`: delete one memory node plus its derived chunk and orphan CodeRefs.
- `memory_link_code_ref`: link memory to a resolved CodeRef target.
- `memory_refresh_chunk`, `memory_refresh_embeddings`: maintain derived memory RAG data.

Fallback:

- `raw_read_cypher`: read-only, project-scoped Cypher for edge cases.

## Configuration

Use namespaced environment variables so this server does not collide with generic shell settings:

| Variable | Default | Purpose |
| --- | --- | --- |
| `MEMGRAPH_INGESTER_MCP_BOLT_URI` | `bolt://127.0.0.1:7687` | Memgraph Bolt URI |
| `MEMGRAPH_INGESTER_MCP_USERNAME` | unset | Optional username |
| `MEMGRAPH_INGESTER_MCP_PASSWORD` | unset | Optional password |
| `MEMGRAPH_INGESTER_MCP_DATABASE` | unset | Optional database name |
| `MEMGRAPH_INGESTER_MCP_PROJECT` | unset | Default project name |
| `MEMGRAPH_INGESTER_MCP_QUERY_TIMEOUT_SECONDS` | `30` | Query timeout |
| `MEMGRAPH_INGESTER_MCP_READ_ONLY` | `false` | Disable write tools when `true` |
| `MEMGRAPH_INGESTER_MCP_EMBEDDING_MODEL` | `default` | Metadata stamped on refreshed chunks |
| `MEMGRAPH_INGESTER_MCP_EMBEDDING_DIMENSIONS` | `384` | Expected memory embedding dimension |

## Run Locally

```bash
uv sync --dev
uv run memgraph-ingester-mcp
```

Run tests:

```bash
uv run ruff check .
uv run pytest
```

Build the package:

```bash
uv build --no-sources
```

## Client Setup

Replace `my-project` with the project name used when running `memgraph-ingester`.

### Codex

Add this to `~/.codex/config.toml`:

```toml
[mcp_servers.memgraphIngester]
command = "uvx"
args = ["memgraph-ingester-mcp"]
startup_timeout_ms = 20_000

[mcp_servers.memgraphIngester.env]
MEMGRAPH_INGESTER_MCP_BOLT_URI = "bolt://127.0.0.1:7687"
MEMGRAPH_INGESTER_MCP_PROJECT = "my-project"
```

### Claude Desktop

Add this to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "memgraphIngester": {
      "type": "stdio",
      "command": "uvx",
      "args": ["memgraph-ingester-mcp"],
      "env": {
        "MEMGRAPH_INGESTER_MCP_BOLT_URI": "bolt://127.0.0.1:7687",
        "MEMGRAPH_INGESTER_MCP_PROJECT": "my-project"
      }
    }
  }
}
```

Claude Code can also add the same stdio server directly:

```bash
claude mcp add-json memgraphIngester \
  '{"type":"stdio","command":"uvx","args":["memgraph-ingester-mcp"],"env":{"MEMGRAPH_INGESTER_MCP_BOLT_URI":"bolt://127.0.0.1:7687","MEMGRAPH_INGESTER_MCP_PROJECT":"my-project"}}'
```

### GitHub Copilot in VS Code

Create `.vscode/mcp.json` in the workspace or add the server to your user MCP configuration:

```json
{
  "servers": {
    "memgraphIngester": {
      "type": "stdio",
      "command": "uvx",
      "args": ["memgraph-ingester-mcp"],
      "env": {
        "MEMGRAPH_INGESTER_MCP_BOLT_URI": "bolt://127.0.0.1:7687",
        "MEMGRAPH_INGESTER_MCP_PROJECT": "my-project"
      }
    }
  }
}
```

### Gemini CLI

Add this to Gemini CLI `settings.json`:

```json
{
  "mcpServers": {
    "memgraphIngester": {
      "command": "uvx",
      "args": ["memgraph-ingester-mcp"],
      "env": {
        "MEMGRAPH_INGESTER_MCP_BOLT_URI": "bolt://127.0.0.1:7687",
        "MEMGRAPH_INGESTER_MCP_PROJECT": "my-project"
      },
      "trust": false
    }
  }
}
```

## Agent Guidance

Prefer the dedicated tools over `raw_read_cypher`. Use RAG search tools only for broad discovery,
then follow up with exact lookup tools before making claims or edits. The default first pages are
small on purpose: use `meta.hasMore` and `meta.nextSkip` to paginate instead of raising limits
preemptively. For performance work, start with `code_hot_paths`; add `code_quality_stats` only when
you need graph-wide baselines. Compact method/search/call-graph rows are range-first and omit full
signatures; request non-compact output only when signatures or modifiers are needed. Use
`code_method_context` when tracing one method so one MCP call replaces lookup plus callers plus
callees. Use `code_discovery_context` for concept-first work before separate RAG/refine loops, and
use `code_text_search` when concrete terms are likely present. Most code tools exclude `src/test/`
by default; pass `include_tests=true` only for test coverage or test-specific questions. Use
`code_impact` first for method signature changes and blast-radius review; it includes tests by
default and flags file/package boundaries. Use `code_operation_hot_paths` alongside
`code_hot_paths` for performance/write-path audits; pass `owner_fragment` or `path_contains` when
the question names a subsystem such as writer, storage, parser, or adapter. Use `code_test_context`
first for failing test or CI triage. Use `code_lookup_field` for constants and member variables, and `code_lookup_file`
for indexed source, resource, template, and Cypher path discovery before falling back to text
search. Use `memory_orientation(compact=true)` for status checks and memory write tools for task
lifecycle changes and CodeRef links so derived MemoryChunks stay refreshable.
