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


def test_memory_refresh_chunk_builds_text_and_hash():
    tools = make_tools()

    result = tools.memory_refresh_chunk("Task", "TASK-demo", embed=False)

    assert result["chunk"]["id"] == "MCH-TASK-demo"
    chunk_call = tools.client.calls[-1]
    assert chunk_call["write"] is True
    assert "CodeRefs: File src/memgraph_ingester_mcp/server.py" in chunk_call["parameters"]["text"]
    assert len(chunk_call["parameters"]["text_hash"]) == 64


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
