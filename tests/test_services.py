import pytest

from memgraph_ingester_mcp.config import MemgraphConfig
from memgraph_ingester_mcp.db import MemgraphError
from memgraph_ingester_mcp.services import MemgraphIngesterTools


class FakeClient:
    def __init__(self):
        self.calls = []

    def run(self, query, parameters=None, *, write=False):
        params = dict(parameters or {})
        self.calls.append({"query": query, "parameters": params, "write": write})

        if "RETURN labels(memory) AS labels" in query:
            return [
                {
                    "labels": ["Task"],
                    "properties": {
                        "id": params["memory_id"],
                        "title": "Implement MCP",
                        "status": "doing",
                        "priority": "1",
                        "description": "Build high-level tools.",
                    },
                    "codeRefs": [
                        {
                            "targetType": "File",
                            "key": "src/memgraph_ingester_mcp/server.py",
                            "targetLabels": ["File"],
                        }
                    ],
                }
            ]

        if "MERGE (root:Memory" in query:
            return [
                {
                    "labels": ["Task"],
                    "properties": {"id": params["memory_id"], **params["properties"]},
                }
            ]

        if "MERGE (chunk:MemoryChunk" in query:
            return [
                {
                    "id": params["chunk_id"],
                    "textHash": params["text_hash"],
                    "dirty": True,
                }
            ]

        if "AND (chunk.embedding IS NULL" in query:
            return [{"id": chunk_id} for chunk_id in params["ids"]]

        if "CALL embeddings.node_sentence" in query:
            return [{"success": True, "dimension": 384, "ids": params["ids"]}]

        if "SET chunk.embeddingModel" in query:
            return [{"id": chunk_id} for chunk_id in params["ids"]]

        if "MATCH (node:Task" in query and "MATCH (target:File" in query:
            return [
                {
                    "memoryId": params["memory_id"],
                    "targetType": params["target_type"],
                    "key": params["target_key"],
                    "targetLabels": ["File"],
                }
            ]

        return [{"ok": True}]


def make_tools():
    return MemgraphIngesterTools(FakeClient(), MemgraphConfig(default_project="demo"))


def test_memory_schema_lists_fields_controlled_values_and_targets():
    tools = make_tools()

    result = tools.memory_schema()

    context = result["memoryTypes"]["Context"]
    task = result["memoryTypes"]["Task"]
    assert context["fields"] == ["content", "source", "title", "topic"]
    assert context["controlledValues"] == {}
    assert task["controlledValues"]["status"] == [
        "blocked",
        "cancelled",
        "doing",
        "done",
        "todo",
    ]
    assert task["controlledValues"]["priority"] == ["0", "1", "2", "3", "4"]
    assert "File" in result["targetTypes"]
    assert tools.client.calls == []


def test_memory_schema_can_be_scoped_to_one_memory_type():
    tools = make_tools()

    result = tools.memory_schema("Context")

    assert list(result["memoryTypes"]) == ["Context"]
    assert result["memoryTypes"]["Context"]["fields"] == ["content", "source", "title", "topic"]


def test_memory_schema_rejects_unknown_memory_type():
    tools = make_tools()

    with pytest.raises(MemgraphError, match="Unsupported memory_type"):
        tools.memory_schema("Note")


def test_memory_upsert_validates_fields_and_normalizes_priority():
    tools = make_tools()

    result = tools.memory_upsert(
        "Task",
        "TASK-demo",
        {"title": "Demo", "status": "doing", "priority": 2},
        refresh_chunk=False,
        embed=False,
    )

    assert result["memory"]["properties"]["priority"] == "2"
    write_call = tools.client.calls[0]
    assert write_call["write"] is True
    assert "MERGE (node:Task" in write_call["query"]


def test_memory_upsert_rejects_unknown_fields():
    tools = make_tools()

    with pytest.raises(MemgraphError, match="Unsupported Task field"):
        tools.memory_upsert("Task", "TASK-demo", {"surprise": "nope"}, refresh_chunk=False)


def test_memory_upsert_rejects_invalid_status():
    tools = make_tools()

    with pytest.raises(MemgraphError, match=r"Task\.status"):
        tools.memory_upsert("Task", "TASK-demo", {"status": "halfway"}, refresh_chunk=False)


def test_memory_link_code_ref_uses_whitelisted_target_label():
    tools = make_tools()

    result = tools.memory_link_code_ref(
        "Task",
        "TASK-demo",
        "File",
        "src/memgraph_ingester_mcp/server.py",
    )

    assert result["resolved"] is True
    call = tools.client.calls[0]
    assert call["write"] is True
    assert "MATCH (target:File" in call["query"]
    assert result["chunk"]["chunk"]["id"] == "MCH-TASK-demo"
    assert result["chunk"]["embedding"]["embedded"] == ["MCH-TASK-demo"]


def test_memory_upsert_with_code_ref_refreshes_chunk_once_after_link():
    tools = make_tools()

    result = tools.memory_upsert(
        "Task",
        "TASK-demo",
        {"title": "Demo", "status": "doing", "priority": 2},
        code_ref={"target_type": "File", "key": "src/memgraph_ingester_mcp/server.py"},
    )

    chunk_calls = [
        call for call in tools.client.calls if "MERGE (chunk:MemoryChunk" in call["query"]
    ]
    assert result["codeRef"]["resolved"] is True
    assert len(chunk_calls) == 1


def test_memory_refresh_chunk_builds_text_and_hash():
    tools = make_tools()

    result = tools.memory_refresh_chunk("Task", "TASK-demo", embed=False)

    assert result["chunk"]["id"] == "MCH-TASK-demo"
    chunk_call = tools.client.calls[-1]
    assert chunk_call["write"] is True
    assert "CodeRefs: File src/memgraph_ingester_mcp/server.py" in chunk_call["parameters"]["text"]
    assert len(chunk_call["parameters"]["text_hash"]) == 64


def test_memory_refresh_chunk_reports_clean_after_embedding():
    tools = make_tools()

    result = tools.memory_refresh_chunk("Task", "TASK-demo", embed=True)

    assert result["chunk"]["dirty"] is False
    assert result["embedding"]["embedded"] == ["MCH-TASK-demo"]


def test_raw_read_cypher_rejects_writes():
    tools = make_tools()

    with pytest.raises(MemgraphError, match="write keyword"):
        tools.raw_read_cypher("MATCH (n {project: $project}) DELETE n")


def test_raw_read_cypher_requires_project_scope():
    tools = make_tools()

    with pytest.raises(MemgraphError, match="project filter"):
        tools.raw_read_cypher("MATCH (n) RETURN n")


def test_raw_read_cypher_adds_project_and_bounds_limit():
    tools = make_tools()

    tools.raw_read_cypher(
        "MATCH (n {project: $project}) RETURN n LIMIT $limit",
        limit=999,
    )

    call = tools.client.calls[0]
    assert call["parameters"]["project"] == "demo"
    assert call["parameters"]["limit"] == 500
    assert call["write"] is False


def test_code_lookup_type_orders_by_return_alias_after_collect():
    tools = make_tools()

    tools.code_lookup_type(type_name="GraphWriter", include_members=False)

    query = tools.client.calls[0]["query"]
    assert "collect(DISTINCT file.path) AS files" in query
    assert "ORDER BY fqn" in query
    assert "ORDER BY t.fqn" not in query


def test_code_lookup_methods_orders_by_return_alias_after_collect():
    tools = make_tools()

    tools.code_lookup_methods("GraphWriter")

    query = tools.client.calls[0]["query"]
    assert "collect(DISTINCT file.path) AS files" in query
    assert "ORDER BY signature" in query
    assert "ORDER BY method.signature" not in query
