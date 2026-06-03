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
- `code_quality_stats`: graph-wide code quality and quantity metrics.
- `code_hot_paths`: compact hot-path candidates from size and call graph metrics.
- `code_search`: CodeChunk vector search for broad discovery; text is omitted by default.
- `code_lookup_type`: exact class/interface/annotation lookup; member expansion is opt-in.
- `code_lookup_methods`: exact method lookup with source ranges.
- `code_callers`, `code_callees`: compact, paginated call graph lookups.
- `code_hierarchy`: class ancestry, children, interfaces, and interface implementors.

Memory tools:

- `memory_orientation`: rules plus open findings, tasks, questions, and risks.
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
then follow up with exact lookup tools before making claims or edits. Keep responses compact:
prefer `code_quality_stats`, `code_hot_paths`, `code_orientation(sections=[...])`, compact
callers/callees, and `code_lookup_type(include_members=false)`. Use memory write tools for task
lifecycle changes and CodeRef links so derived MemoryChunks stay refreshable.
