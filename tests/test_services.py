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

        if "AS chunkIds" in query:
            if params["memory_id"] == "MISSING":
                return []
            return [
                {
                    "labels": ["Task"],
                    "properties": {"id": params["memory_id"], "title": "Implement MCP"},
                    "chunkIds": [f"MCH-{params['memory_id']}"],
                    "codeRefs": [
                        {
                            "targetType": "File",
                            "key": "src/memgraph_ingester_mcp/server.py",
                        }
                    ],
                }
            ]

        if "RETURN size(refs) AS deleted" in query:
            return [{"deleted": 1}]

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


class CodeLookupClient:
    def __init__(self):
        self.calls = []

    def run(self, query, parameters=None, *, write=False):
        params = dict(parameters or {})
        self.calls.append({"query": query, "parameters": params, "write": write})
        if "RETURN count(t) AS count" in query:
            return [{"count": 1}]
        if "collect(DISTINCT file.path) AS files" in query:
            return [
                {
                    "labels": ["Class"],
                    "fqn": "demo.Foo",
                    "name": "Foo",
                    "kind": "class",
                    "visibility": "public",
                    "isExternal": False,
                    "language": "java",
                    "framework": "",
                    "modulePath": "",
                    "files": ["src/main/java/demo/Foo.java"],
                }
            ]
        if "RETURN methods AS methods" in query:
            return [{"methods": 7, "fields": 2}]
        if "RETURN m.signature AS signature" in query:
            return [{"signature": "demo.Foo.a()", "name": "a"}]
        if "RETURN field.fqn AS fqn" in query:
            return [{"fqn": "demo.Foo.x", "name": "x"}]
        return []


class SearchClient:
    def __init__(self):
        self.calls = []

    def run(self, query, parameters=None, *, write=False):
        self.calls.append({"query": query, "parameters": dict(parameters or {}), "write": write})
        return [
            {
                "sourceType": ["Method"],
                "sourceId": "demo.Foo.a()",
                "path": "src/main/java/demo/Foo.java",
                "ownerFqn": "demo.Foo",
                "signature": "demo.Foo.a()",
                "similarity": 0.9,
                "text": "x" * 500,
            },
            {
                "sourceType": ["Method"],
                "sourceId": "demo.Foo.a()",
                "path": "src/main/java/demo/Foo.java",
                "ownerFqn": "demo.Foo",
                "signature": "demo.Foo.a()",
                "similarity": 0.8,
                "text": "duplicate",
            },
        ]


class CallGraphClient:
    def __init__(self):
        self.calls = []

    def run(self, query, parameters=None, *, write=False):
        params = dict(parameters or {})
        self.calls.append({"query": query, "parameters": params, "write": write})
        if "RETURN count(*) AS count" in query:
            return [{"count": 100}]
        return [
            {
                "callerSignature": "demo.Foo.a()",
                "callerOwner": "Foo",
                "callerStartLine": 10,
                "callerEndLine": 20,
                "calleeSignature": "demo.Bar.b()",
                "calleeOwner": "Bar",
            }
        ]


class OrientationClient:
    def __init__(self):
        self.calls = []

    def run(self, query, parameters=None, *, write=False):
        self.calls.append({"query": query, "parameters": dict(parameters or {}), "write": write})
        return [{"ok": True}]


def test_code_lookup_type_is_compact_by_default():
    client = CodeLookupClient()
    tools = MemgraphIngesterTools(client, MemgraphConfig(default_project="demo"))

    result = tools.code_lookup_type(type_name="Foo")

    item = result["types"][0]
    assert item["memberCounts"] == {"methods": 7, "fields": 2}
    assert "methods" not in item
    assert "fields" not in item
    assert result["meta"]["includeMembers"] is False
    assert all("ORDER BY m.name" not in call["query"] for call in client.calls)


def test_code_lookup_type_expands_members_only_when_requested():
    client = CodeLookupClient()
    tools = MemgraphIngesterTools(client, MemgraphConfig(default_project="demo"))

    result = tools.code_lookup_type(type_name="Foo", include_members=True, member_limit=3)

    item = result["types"][0]
    assert item["methods"] == [{"signature": "demo.Foo.a()", "name": "a"}]
    assert item["fields"] == [{"fqn": "demo.Foo.x", "name": "x"}]
    member_calls = [call for call in client.calls if "LIMIT $limit" in call["query"]]
    assert member_calls[-2]["parameters"]["limit"] == 3
    assert member_calls[-1]["parameters"]["limit"] == 3


def test_code_search_omits_text_and_dedupes_by_default():
    client = SearchClient()
    tools = MemgraphIngesterTools(client, MemgraphConfig(default_project="demo"))

    result = tools.code_search("hot path")

    assert len(result["hits"]) == 1
    assert "text" not in result["hits"][0]
    assert "chunk.text AS text" not in client.calls[0]["query"]
    assert result["meta"]["dedupeBySource"] is True


def test_code_search_can_include_bounded_text():
    client = SearchClient()
    tools = MemgraphIngesterTools(client, MemgraphConfig(default_project="demo"))

    result = tools.code_search("hot path", include_text=True, text_limit=20)

    assert result["hits"][0]["text"].endswith("...")
    assert len(result["hits"][0]["text"]) <= 23
    assert "chunk.text AS text" in client.calls[0]["query"]


def test_code_callers_are_compact_and_low_limit_by_default():
    client = CallGraphClient()
    tools = MemgraphIngesterTools(client, MemgraphConfig(default_project="demo"))

    result = tools.code_callers("demo.Bar.b")

    assert client.calls[0]["parameters"]["limit"] == 25
    assert result["callers"] == [
        {
            "caller": "demo.Foo.a()",
            "owner": "Foo",
            "startLine": 10,
            "endLine": 20,
            "calleeOwner": "Bar",
        }
    ]
    assert result["meta"]["totalCount"] == 100
    assert result["meta"]["hasMore"] is True


def test_code_callees_can_return_legacy_shape():
    client = CallGraphClient()
    tools = MemgraphIngesterTools(client, MemgraphConfig(default_project="demo"))

    result = tools.code_callees("demo.Foo", compact=False, limit=5)

    assert client.calls[0]["parameters"]["limit"] == 5
    assert "callerSignature" in result["callees"][0]
    assert result["meta"]["compact"] is False


def test_code_orientation_runs_only_requested_sections():
    client = OrientationClient()
    tools = MemgraphIngesterTools(client, MemgraphConfig(default_project="demo"))

    result = tools.code_orientation(sections=["languages"])

    assert result["sections"] == ["languages"]
    assert "languages" in result
    assert "packages" not in result
    assert len(client.calls) == 1
    assert "MATCH (l:Language" in client.calls[0]["query"]


def test_code_hot_paths_returns_compact_sections():
    tools = make_tools()

    result = tools.code_hot_paths(limit=2, include_evidence=False)

    assert result["meta"]["includeEvidence"] is False
    assert {row["section"] for row in result["hotPaths"]} == {
        "largestTypes",
        "longestMethods",
        "fanIn",
        "fanOut",
    }


def test_code_quality_stats_returns_aggregate_sections():
    tools = make_tools()

    result = tools.code_quality_stats(limit=3)

    assert result["project"] == "demo"
    assert "inventory" in result
    assert "methodLengths" in result
    assert "fanIn" in result
    assert "fanOut" in result
    assert result["meta"]["limit"] == 3


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


def test_delete_memory_removes_memory_chunk_and_orphan_code_refs():
    tools = make_tools()

    result = tools.delete_memory("TASK-demo")

    assert result["deleted"] is True
    assert result["chunkIds"] == ["MCH-TASK-demo"]
    assert result["orphanCodeRefsDeleted"] == 1
    delete_calls = [call for call in tools.client.calls if call["write"] is True]
    assert len(delete_calls) == 2
    assert "DETACH DELETE memory" in delete_calls[0]["query"]
    assert "DETACH DELETE chunk" in delete_calls[0]["query"]
    assert "DETACH DELETE ref" in delete_calls[1]["query"]


def test_delete_memory_reports_missing_without_writes():
    tools = make_tools()

    result = tools.delete_memory("MISSING")

    assert result["deleted"] is False
    assert result["memory"] is None
    assert all(call["write"] is False for call in tools.client.calls)


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

    query = next(
        call["query"]
        for call in tools.client.calls
        if "collect(DISTINCT file.path) AS files" in call["query"]
    )
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


def test_code_lookup_methods_can_return_compact_ranges():
    tools = make_tools()

    result = tools.code_lookup_methods("GraphWriter", compact=True)

    query = tools.client.calls[0]["query"]
    assert "method.startLine AS startLine" in query
    assert "method.returnType AS returnType" not in query
    assert "method.isSynthetic AS isSynthetic" not in query
    assert result["meta"]["compact"] is True


def test_memory_orientation_can_be_compact():
    tools = make_tools()

    tools.memory_orientation(compact=True)

    queries = "\n".join(call["query"] for call in tools.client.calls)
    assert "rule.description AS description" not in queries
    assert "finding.summary AS summary" not in queries
    assert "task.description AS description" not in queries
    assert "risk.mitigation AS mitigation" not in queries


def test_memory_orientation_is_full_by_default():
    tools = make_tools()

    tools.memory_orientation()

    queries = "\n".join(call["query"] for call in tools.client.calls)
    assert "rule.description AS description" in queries
    assert "finding.summary AS summary" in queries
    assert "task.description AS description" in queries
    assert "risk.mitigation AS mitigation" in queries
