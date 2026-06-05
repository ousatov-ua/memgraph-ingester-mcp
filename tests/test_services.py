import pytest

from memgraph_ingester_mcp.config import MemgraphConfig
from memgraph_ingester_mcp.db import MemgraphError
from memgraph_ingester_mcp.server import create_server
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

        if "WHERE method.signature CONTAINS $fragment" in query:
            if "RETURN count(method) AS count" in query:
                return [{"count": 1}]
            return [
                {
                    "ownerDisplayName": "GraphWriter",
                    "name": "upsertFile",
                    "startLine": 10,
                    "endLine": 20,
                    "files": ["src/main/java/demo/GraphWriter.java"],
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
            row = {
                "labels": ["Class"],
                "fqn": "demo.Foo",
                "name": "Foo",
                "kind": "class",
            }
            if "t.visibility AS visibility" in query:
                row.update(
                    {
                        "visibility": "public",
                        "isExternal": False,
                        "language": "java",
                        "framework": "",
                        "modulePath": "",
                    }
                )
            row["files"] = ["src/main/java/demo/Foo.java"]
            if "methodCount" in query:
                row["methodCount"] = 7
                row["fieldCount"] = 2
            return [row]
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
                "kind": "Method",
                "sourceId": "demo.Foo.a()",
                "owner": "Foo",
                "name": "a",
                "path": "src/main/java/demo/Foo.java",
                "startLine": 10,
                "endLine": 20,
                "score": 0.9,
                "text": "x" * 500,
            },
            {
                "kind": "Method",
                "sourceId": "demo.Foo.a()",
                "owner": "Foo",
                "name": "a",
                "path": "src/main/java/demo/Foo.java",
                "startLine": 10,
                "endLine": 20,
                "score": 0.8,
                "text": "duplicate",
            },
        ]


class CallGraphClient:
    def __init__(self):
        self.calls = []

    def run(self, query, parameters=None, *, write=False):
        params = dict(parameters or {})
        self.calls.append({"query": query, "parameters": params, "write": write})
        if "WHERE method.signature CONTAINS $fragment" in query:
            if "RETURN count(method) AS count" in query:
                return [{"count": 1}]
            return [
                {
                    "ownerDisplayName": "GraphWriter",
                    "name": "upsertFile",
                    "startLine": 10,
                    "endLine": 20,
                    "files": ["src/main/java/demo/GraphWriter.java"],
                }
            ]
        if "RETURN count(*) AS count" in query:
            return [{"count": 100}]
        if "calleeFile.path AS calleePath" in query:
            return [
                {
                    "callerSignature": "demo.Foo.a()",
                    "callerName": "a",
                    "callerOwner": "Foo",
                    "calleeSignature": "demo.Bar.b()",
                    "calleeName": "b",
                    "calleeOwner": "Bar",
                    "calleeStartLine": 30,
                    "calleeEndLine": 40,
                    "calleePath": "src/main/java/demo/Bar.java",
                }
            ]
        return [
            {
                "callerSignature": "demo.Foo.a()",
                "callerName": "a",
                "callerOwner": "Foo",
                "callerStartLine": 10,
                "callerEndLine": 20,
                "calleeSignature": "demo.Bar.b()",
                "calleeName": "b",
                "calleeOwner": "Bar",
                "callerPath": "src/main/java/demo/Foo.java",
            }
        ]


class FieldFileLookupClient:
    def __init__(self):
        self.calls = []

    def run(self, query, parameters=None, *, write=False):
        self.calls.append({"query": query, "parameters": dict(parameters or {}), "write": write})
        if "RETURN count(DISTINCT field) AS count" in query:
            return [{"count": 1}]
        if "MATCH (field:Field" in query:
            return [
                {
                    "fqn": "demo.GraphWriter#cypher",
                    "name": "cypher",
                    "owner": "GraphWriter",
                    "ownerFqn": "demo.GraphWriter",
                    "startLine": 12,
                    "endLine": 12,
                    "files": ["src/main/java/demo/GraphWriter.java"],
                }
            ]
        if "RETURN count(DISTINCT file) AS count" in query:
            return [{"count": 1}]
        if "MATCH (file:File" in query:
            return [
                {
                    "path": "src/main/java/demo/GraphWriter.java",
                    "language": "java",
                    "definitionCount": 4,
                    "chunkCount": 9,
                }
            ]
        return []


class ImpactClient:
    def __init__(self):
        self.calls = []

    def run(self, query, parameters=None, *, write=False):
        self.calls.append({"query": query, "parameters": dict(parameters or {}), "write": write})
        if "RETURN count(hit) AS count" in query:
            return [{"count": 2}]
        if "target.signature AS signature" in query:
            return [
                {
                    "signature": (
                        "demo.writer.GraphWriter.refreshCodeChunkEmbeddings(Settings, boolean)"
                    ),
                    "owner": "GraphWriter",
                    "ownerFqn": "demo.writer.GraphWriter",
                    "name": "refreshCodeChunkEmbeddings",
                    "startLine": 620,
                    "endLine": 623,
                    "files": ["src/main/java/demo/writer/GraphWriter.java"],
                }
            ]
        if "UNION ALL" in query:
            return [
                {
                    "depth": 1,
                    "callerSignature": "demo.ingestion.IngestionOrchestrator.refresh()",
                    "callerOwner": "IngestionOrchestrator",
                    "callerOwnerFqn": "demo.ingestion.IngestionOrchestrator",
                    "callerName": "refresh",
                    "callerStartLine": 400,
                    "callerEndLine": 420,
                    "callerPath": "src/main/java/demo/ingestion/IngestionOrchestrator.java",
                    "viaSignature": None,
                    "viaOwner": None,
                    "viaOwnerFqn": None,
                    "viaName": None,
                    "viaPath": None,
                    "targetSignature": (
                        "demo.writer.GraphWriter.refreshCodeChunkEmbeddings(Settings, boolean)"
                    ),
                    "targetOwner": "GraphWriter",
                    "targetOwnerFqn": "demo.writer.GraphWriter",
                    "targetName": "refreshCodeChunkEmbeddings",
                    "targetPath": "src/main/java/demo/writer/GraphWriter.java",
                },
                {
                    "depth": 2,
                    "callerSignature": "demo.IngesterCliTest.run()",
                    "callerOwner": "IngesterCliTest",
                    "callerOwnerFqn": "demo.IngesterCliTest",
                    "callerName": "run",
                    "callerStartLine": 50,
                    "callerEndLine": 70,
                    "callerPath": "src/test/java/demo/IngesterCliTest.java",
                    "viaSignature": "demo.ingestion.IngestionOrchestrator.refresh()",
                    "viaOwner": "IngestionOrchestrator",
                    "viaOwnerFqn": "demo.ingestion.IngestionOrchestrator",
                    "viaName": "refresh",
                    "viaPath": "src/main/java/demo/ingestion/IngestionOrchestrator.java",
                    "targetSignature": (
                        "demo.writer.GraphWriter.refreshCodeChunkEmbeddings(Settings, boolean)"
                    ),
                    "targetOwner": "GraphWriter",
                    "targetOwnerFqn": "demo.writer.GraphWriter",
                    "targetName": "refreshCodeChunkEmbeddings",
                    "targetPath": "src/main/java/demo/writer/GraphWriter.java",
                },
            ]
        return []


class UniversalFlowClient:
    def __init__(self):
        self.calls = []

    def run(self, query, parameters=None, *, write=False):
        params = dict(parameters or {})
        self.calls.append({"query": query, "parameters": params, "write": write})
        if "HAS_RAG_CHUNK" in query and "haystack" in query:
            return [
                {
                    "kind": "Method",
                    "sourceId": "demo.Writer.refresh()",
                    "owner": "Writer",
                    "name": "refresh",
                    "path": "src/main/java/demo/Writer.java",
                    "startLine": 10,
                    "endLine": 20,
                    "termMatches": 2,
                    "text": "stale chunks are refreshed",
                }
            ]
        if "chunk.sourceLabel = 'File'" in query and "chunk.text AS text" in query:
            return [
                {
                    "path": "src/main/resources/demo/resolve-pending-calls.cypher",
                    "language": "cypher",
                    "text": (
                        "Language: cypher\n"
                        "Path: src/main/resources/demo/resolve-pending-calls.cypher\n"
                        "Source excerpt:\n"
                        "OPTIONAL MATCH (c:Class)-[:EXTENDS*1..]->(p:Class)\n"
                        "CALL {\n  MATCH (file:File {path: $path}) RETURN file\n}\n"
                        "CALL {\n  MATCH (file:File {path: $path}) RETURN file\n}\n"
                        "CALL {\n  MATCH (file:File {path: $path}) RETURN file\n}\n"
                    ),
                },
                {
                    "path": "src/main/resources/demo/upsert-calls-by-name-batch.cypher",
                    "language": "cypher",
                    "text": (
                        "Language: cypher\n"
                        "Path: src/main/resources/demo/upsert-calls-by-name-batch.cypher\n"
                        "Source excerpt:\n"
                        "UNWIND $rows AS row\n"
                        "OPTIONAL MATCH (c:Class)-[:EXTENDS*1..]->(p:Class)\n"
                    ),
                },
            ]
        if "sinkCallEdges" in query:
            return [
                {
                    "owner": "Writer",
                    "name": "refresh",
                    "signature": "demo.Writer.refresh()",
                    "path": "src/main/java/demo/Writer.java",
                    "startLine": 10,
                    "endLine": 60,
                    "lines": 51,
                    "sinkCallEdges": 6,
                    "distinctSinks": 3,
                    "sinks": ["Executor.run", "Executor.read"],
                    "score": 6051,
                }
            ]
        if "file.path STARTS WITH 'src/test/'" in query or "testFile.path STARTS" in query:
            if "CALLS" in query:
                return [
                    {
                        "owner": "Writer",
                        "name": "refresh",
                        "signature": "demo.Writer.refresh()",
                        "path": "src/main/java/demo/Writer.java",
                        "startLine": 10,
                        "endLine": 20,
                        "testOwner": "WriterTest",
                        "testName": "refreshes",
                    }
                ]
            if "RETURN file.path AS path, file.language AS language" in query:
                return [{"path": "src/test/java/demo/WriterTest.java", "language": "java"}]
            return [
                {
                    "owner": "WriterTest",
                    "name": "refreshes",
                    "signature": "demo.WriterTest.refreshes()",
                    "path": "src/test/java/demo/WriterTest.java",
                    "startLine": 30,
                    "endLine": 40,
                    "exactish": True,
                    "termMatches": 1,
                }
            ]
        return []


class CodeContextClient:
    def __init__(self):
        self.calls = []

    def run(self, query, parameters=None, *, write=False):
        params = dict(parameters or {})
        self.calls.append({"query": query, "parameters": params, "write": write})
        if "CALL vector_search.search" in query:
            return [
                {
                    "kind": "Method",
                    "sourceId": "demo.Writer.refresh()",
                    "owner": "Writer",
                    "name": "refresh",
                    "path": "src/main/java/demo/Writer.java",
                    "ragRole": "primary",
                    "startLine": 10,
                    "endLine": 40,
                    "score": 0.91,
                },
                {
                    "kind": "File",
                    "sourceId": "src/main/java/demo/Orchestrator.java",
                    "owner": None,
                    "name": "src/main/java/demo/Orchestrator.java",
                    "path": "src/main/java/demo/Orchestrator.java",
                    "ragRole": "file",
                    "startLine": None,
                    "endLine": None,
                    "score": 0.84,
                },
            ]
        if "HAS_RAG_CHUNK" in query and "haystack" in query:
            return [
                {
                    "kind": "Method",
                    "sourceId": "demo.Orchestrator.run()",
                    "owner": "Orchestrator",
                    "name": "run",
                    "path": "src/main/java/demo/Orchestrator.java",
                    "startLine": 50,
                    "endLine": 90,
                }
            ]
        if "definitionCount" in query and "MATCH (file:File" in query:
            rows = [
                {
                    "path": "src/main/java/demo/Writer.java",
                    "language": "java",
                    "definitionCount": 3,
                    "chunkCount": 6,
                },
                {
                    "path": "src/main/java/demo/Orchestrator.java",
                    "language": "java",
                    "definitionCount": 2,
                    "chunkCount": 4,
                },
            ]
            fragments = params.get("fragments") or []
            if not fragments:
                return rows
            return [row for row in rows if any(fragment in row["path"] for fragment in fragments)]
        if "node:Class OR node:Interface OR node:Annotation" in query:
            return [
                {
                    "path": "src/main/java/demo/Writer.java",
                    "label": "Class",
                    "name": "Writer",
                    "fqn": "demo.Writer",
                    "kind": "class",
                    "startLine": 1,
                    "endLine": 80,
                },
                {
                    "path": "src/main/java/demo/Orchestrator.java",
                    "label": "Class",
                    "name": "Orchestrator",
                    "fqn": "demo.Orchestrator",
                    "kind": "class",
                    "startLine": 1,
                    "endLine": 120,
                },
            ]
        if "method:Method" in query and "RETURN file.path AS path" in query:
            return [
                {
                    "path": "src/main/java/demo/Writer.java",
                    "owner": "Writer",
                    "name": "refresh",
                    "signature": "demo.Writer.refresh()",
                    "startLine": 10,
                    "endLine": 40,
                },
                {
                    "path": "src/main/java/demo/Orchestrator.java",
                    "owner": "Orchestrator",
                    "name": "run",
                    "signature": "demo.Orchestrator.run()",
                    "startLine": 50,
                    "endLine": 90,
                },
            ]
        if "field:Field" in query:
            return [
                {
                    "path": "src/main/java/demo/Writer.java",
                    "owner": "Writer",
                    "name": "cypher",
                    "fqn": "demo.Writer.cypher",
                    "startLine": 7,
                    "endLine": 7,
                }
            ]
        if "ragRole" in query and "count(*) AS count" in query:
            return [
                {"path": "src/main/java/demo/Writer.java", "ragRole": "primary", "count": 3},
                {"path": "src/main/java/demo/Writer.java", "ragRole": "file", "count": 1},
                {"path": "src/main/java/demo/Orchestrator.java", "ragRole": "primary", "count": 2},
            ]
        if "-[:CALLS]->" in query and "callerFile.path IN $paths" in query:
            return [
                {
                    "callerPath": "src/main/java/demo/Orchestrator.java",
                    "callerOwner": "Orchestrator",
                    "callerName": "run",
                    "callerStartLine": 50,
                    "calleePath": "src/main/java/demo/Writer.java",
                    "calleeOwner": "Writer",
                    "calleeName": "refresh",
                    "calleeStartLine": 10,
                }
            ]
        return []


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
    assert "memberCounts" not in item
    assert "visibility" not in item
    assert "isExternal" not in item
    assert "methods" not in item
    assert "fields" not in item
    assert all("methodCount" not in call["query"] for call in client.calls)
    assert all("ORDER BY m.name" not in call["query"] for call in client.calls)


def test_code_lookup_type_member_summary_is_opt_in():
    client = CodeLookupClient()
    tools = MemgraphIngesterTools(client, MemgraphConfig(default_project="demo"))

    result = tools.code_lookup_type(type_name="Foo", member_summary=True)

    assert result["types"][0]["memberCounts"] == {"methods": 7, "fields": 2}
    assert any("methodCount" in call["query"] for call in client.calls)


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


def test_code_lookup_type_can_return_table_json():
    client = CodeLookupClient()
    tools = MemgraphIngesterTools(client, MemgraphConfig(default_project="demo"))

    result = tools.code_lookup_type(type_name="Foo", output_format="table_json")

    assert result["types"]["cols"] == [
        "labels",
        "fqn",
        "name",
        "kind",
        "files",
    ]
    assert result["types"]["rows"] == [
        [
            ["Class"],
            "demo.Foo",
            "Foo",
            "class",
            ["src/main/java/demo/Foo.java"],
        ]
    ]
    assert result["meta"]["format"] == "table_json"


def test_code_lookup_type_compact_omits_low_value_type_fields():
    client = CodeLookupClient()
    tools = MemgraphIngesterTools(client, MemgraphConfig(default_project="demo"))

    result = tools.code_lookup_type(type_name="Foo", compact=True)

    item = result["types"][0]
    assert "visibility" not in item
    assert "isExternal" not in item
    assert "language" not in item
    assert "framework" not in item
    assert "modulePath" not in item


def test_code_lookup_type_table_json_compacts_nested_members():
    client = CodeLookupClient()
    tools = MemgraphIngesterTools(client, MemgraphConfig(default_project="demo"))

    result = tools.code_lookup_type(
        type_name="Foo",
        include_members=True,
        member_limit=3,
        output_format="table_json",
    )

    methods_index = result["types"]["cols"].index("methods")
    fields_index = result["types"]["cols"].index("fields")
    row = result["types"]["rows"][0]
    assert row[methods_index] == {
        "cols": ["signature", "name"],
        "rows": [["demo.Foo.a()", "a"]],
    }
    assert row[fields_index] == {
        "cols": ["fqn", "name"],
        "rows": [["demo.Foo.x", "x"]],
    }


def test_code_search_omits_text_and_dedupes_by_default():
    client = SearchClient()
    tools = MemgraphIngesterTools(client, MemgraphConfig(default_project="demo"))

    result = tools.code_search("hot path")

    assert len(result["hits"]) == 1
    assert "text" not in result["hits"][0]
    assert "chunk.text AS text" not in client.calls[0]["query"]
    assert client.calls[0]["parameters"]["rag_roles"] == ["primary", "file"]
    assert result["meta"]["hasMore"] is False


def test_code_search_demotes_synthetic_methods_before_stored_rag_role():
    client = SearchClient()
    tools = MemgraphIngesterTools(client, MemgraphConfig(default_project="demo"))

    tools.code_search("hot path")

    query = client.calls[0]["query"]
    assert "coalesce(source.startLine, 0) <= 0" in query
    synthetic_index = query.index("THEN 'synthetic'")
    stored_role_index = query.index("ELSE coalesce(chunk.ragRole, 'primary')")
    assert synthetic_index < stored_role_index


def test_code_search_can_include_bounded_text():
    client = SearchClient()
    tools = MemgraphIngesterTools(client, MemgraphConfig(default_project="demo"))

    result = tools.code_search("hot path", include_text=True, text_limit=20)

    assert result["hits"][0]["text"].endswith("...")
    assert len(result["hits"][0]["text"]) <= 23
    assert "chunk.text AS text" in client.calls[0]["query"]


def test_code_search_compression_hook_is_noop():
    client = SearchClient()
    config = MemgraphConfig(
        default_project="demo",
        compression_enabled=True,
        compression_min_chars=100,
    )
    tools = MemgraphIngesterTools(client, config)

    result = tools.code_search(
        "hot path",
        include_text=True,
        text_limit=500,
        output_format="json",
    )

    assert result["hits"][0]["path"] == "src/main/java/demo/Foo.java"
    assert result["hits"][0]["text"] == "x" * 500
    assert "compression" not in result["meta"]


def test_code_search_can_return_table_json():
    client = SearchClient()
    tools = MemgraphIngesterTools(client, MemgraphConfig(default_project="demo"))

    result = tools.code_search("hot path", output_format="table_json")

    assert result["hits"]["cols"] == [
        "kind",
        "owner",
        "name",
        "path",
        "startLine",
        "endLine",
        "score",
    ]
    assert result["hits"]["rows"] == [
        [
            "Method",
            "Foo",
            "a",
            "src/main/java/demo/Foo.java",
            10,
            20,
            0.9,
        ]
    ]
    assert result["meta"]["format"] == "table_json"


def test_code_search_can_include_keys_and_filters():
    client = SearchClient()
    tools = MemgraphIngesterTools(client, MemgraphConfig(default_project="demo"))

    result = tools.code_search(
        "hot path",
        kinds=["Method"],
        path_contains="Foo",
        include_keys=True,
        output_format="table_json",
    )

    assert "sourceId" in result["hits"]["cols"]


def test_code_search_can_include_secondary_chunks():
    client = SearchClient()
    tools = MemgraphIngesterTools(client, MemgraphConfig(default_project="demo"))

    result = tools.code_search("hot path", include_secondary=True)

    assert client.calls[0]["parameters"]["rag_roles"] == []


def test_code_text_search_returns_compact_hits():
    client = UniversalFlowClient()
    tools = MemgraphIngesterTools(client, MemgraphConfig(default_project="demo"))

    result = tools.code_text_search(
        all_terms=["stale", "chunks"],
        include_text=False,
        output_format="table_json",
    )

    assert result["hits"]["cols"] == [
        "kind",
        "sourceId",
        "owner",
        "name",
        "path",
        "startLine",
        "endLine",
        "termMatches",
    ]
    assert client.calls[0]["parameters"]["rag_roles"] == ["primary", "file"]
    assert client.calls[0]["parameters"]["search_terms"] == ["stale", "chunks"]
    assert result["hits"]["rows"][0][3] == "refresh"
    assert result["meta"]["format"] == "table_json"


def test_code_text_search_demotes_synthetic_methods_before_stored_rag_role():
    client = UniversalFlowClient()
    tools = MemgraphIngesterTools(client, MemgraphConfig(default_project="demo"))

    tools.code_text_search(query="stale chunks")

    query = client.calls[0]["query"]
    assert "coalesce(source.startLine, 0) <= 0" in query
    synthetic_index = query.index("THEN 'synthetic'")
    stored_role_index = query.index("ELSE coalesce(chunk.ragRole, 'primary')")
    assert synthetic_index < stored_role_index


def test_code_text_search_tokenizes_plain_query():
    client = UniversalFlowClient()
    tools = MemgraphIngesterTools(client, MemgraphConfig(default_project="demo"))

    tools.code_text_search(
        query="stale chunks",
        include_text=False,
        output_format="table_json",
    )

    assert client.calls[0]["parameters"]["all_terms"] == []
    assert client.calls[0]["parameters"]["any_terms"] == ["stale", "chunks"]


def test_code_file_context_returns_compact_file_outlines():
    client = CodeContextClient()
    tools = MemgraphIngesterTools(client, MemgraphConfig(default_project="demo"))

    result = tools.code_file_context(
        ["Writer.java", "Orchestrator.java"],
        symbol_limit=1,
        output_format="table_json",
    )

    assert result["files"]["cols"] == [
        "path",
        "language",
        "definitionCount",
        "chunkCount",
        "chunkRoles",
        "types",
        "methods",
        "fields",
    ]
    writer_row = result["files"]["rows"][0]
    assert writer_row[0] == "src/main/java/demo/Writer.java"
    assert writer_row[4] == {
        "cols": ["ragRole", "count"],
        "rows": [["primary", 3]],
    }
    assert writer_row[5]["rows"] == [["Class", "Writer", "demo.Writer", "class", 1, 80]]
    assert writer_row[6]["rows"] == [["Writer", "refresh", "demo.Writer.refresh()", 10, 40]]
    assert writer_row[7]["rows"] == [["Writer", "cypher", "demo.Writer.cypher", 7, 7]]
    assert client.calls[0]["parameters"]["fragments"] == ["Writer.java", "Orchestrator.java"]


def test_code_flow_context_bundles_anchors_files_and_edges():
    client = CodeContextClient()
    tools = MemgraphIngesterTools(client, MemgraphConfig(default_project="demo"))

    result = tools.code_flow_context(
        "refresh stale code chunks",
        limit_files=2,
        anchor_limit=2,
        symbol_limit=1,
        output_format="table_json",
    )

    assert result["anchors"]["cols"] == [
        "kind",
        "sourceId",
        "owner",
        "name",
        "path",
        "ragRole",
        "startLine",
        "endLine",
        "score",
    ]
    assert result["lexicalAnchors"]["rows"][0][3] == "run"
    assert result["files"]["rows"][0][0] == "src/main/java/demo/Orchestrator.java"
    assert result["flowEdges"]["rows"] == [
        [
            "src/main/java/demo/Orchestrator.java",
            "Orchestrator",
            "run",
            50,
            "src/main/java/demo/Writer.java",
            "Writer",
            "refresh",
            10,
        ]
    ]
    assert " AS caller," not in client.calls[-1]["query"]
    assert " AS callee," not in client.calls[-1]["query"]
    assert result["meta"]["lexicalTerms"] == ["refresh", "stale", "code", "chunks"]
    edge_call = client.calls[-1]
    assert "(callerFile.path IN $paths OR calleeFile.path IN $paths)" in edge_call["query"]
    assert edge_call["parameters"]["include_tests"] is False
    assert edge_call["parameters"]["limit"] == 2


def test_code_flow_context_promotes_non_selected_edge_endpoint_files():
    client = CodeContextClient()
    tools = MemgraphIngesterTools(client, MemgraphConfig(default_project="demo"))

    result = tools.code_flow_context(
        "refresh stale code chunks",
        limit_files=1,
        anchor_limit=2,
        symbol_limit=1,
        output_format="table_json",
    )

    assert result["files"]["rows"][0][0] == "src/main/java/demo/Orchestrator.java"
    assert result["relatedFiles"]["rows"][0][0] == "src/main/java/demo/Writer.java"
    assert result["meta"]["detail"] == "compact"


def test_code_flow_context_defaults_are_compact():
    client = CodeContextClient()
    tools = MemgraphIngesterTools(client, MemgraphConfig(default_project="demo"))

    result = tools.code_flow_context("refresh stale code chunks")

    assert result["meta"]["detail"] == "compact"
    edge_calls = [
        call
        for call in client.calls
        if "-[:CALLS]->" in call["query"] and "callerFile.path IN $paths" in call["query"]
    ]
    assert edge_calls[0]["parameters"]["limit"] == 9


def test_code_flow_context_full_detail_keeps_expanded_edges_and_related_files():
    client = CodeContextClient()
    tools = MemgraphIngesterTools(client, MemgraphConfig(default_project="demo"))

    result = tools.code_flow_context(
        "refresh stale code chunks",
        limit_files=2,
        symbol_limit=4,
        detail="full",
    )

    assert result["meta"]["detail"] == "full"


def test_table_json_rejects_unknown_format():
    tools = make_tools()

    with pytest.raises(MemgraphError, match="Unsupported format"):
        tools.code_hot_paths(output_format="yaml")


def test_registered_code_tool_defaults_are_discovery_sized():
    mcp = create_server(MemgraphConfig(default_project="demo"), client=FakeClient())
    registered = mcp._tool_manager._tools

    assert registered["code_search"].parameters["properties"]["limit"]["default"] == 5
    assert registered["code_search"].parameters["properties"]["include_tests"]["default"] is False
    assert registered["code_search"].parameters["properties"]["include_keys"]["default"] is False
    assert (
        registered["code_search"].parameters["properties"]["include_secondary"]["default"] is False
    )
    assert "rag_roles" in registered["code_search"].parameters["properties"]
    assert (
        registered["code_text_search"].parameters["properties"]["include_secondary"]["default"]
        is False
    )
    assert "rag_roles" in registered["code_text_search"].parameters["properties"]
    assert registered["code_text_search"].parameters["properties"]["limit"]["default"] == 5
    assert registered["code_discovery_context"].parameters["properties"]["limit"]["default"] == 3
    assert registered["code_file_context"].parameters["properties"]["limit_files"]["default"] == 5
    assert registered["code_file_context"].parameters["properties"]["symbol_limit"]["default"] == 8
    assert registered["code_flow_context"].parameters["properties"]["limit_files"]["default"] == 3
    assert registered["code_flow_context"].parameters["properties"]["anchor_limit"]["default"] == 3
    assert registered["code_flow_context"].parameters["properties"]["symbol_limit"]["default"] == 3
    assert (
        registered["code_flow_context"].parameters["properties"]["detail"]["default"] == "compact"
    )
    assert registered["code_lookup_type"].parameters["properties"]["limit"]["default"] == 10
    assert (
        registered["code_lookup_type"].parameters["properties"]["include_tests"]["default"] is False
    )
    assert registered["code_lookup_type"].parameters["properties"]["member_limit"]["default"] == 25
    assert registered["code_lookup_methods"].parameters["properties"]["limit"]["default"] == 10
    assert (
        registered["code_lookup_methods"].parameters["properties"]["include_tests"]["default"]
        is False
    )
    assert registered["code_lookup_field"].parameters["properties"]["limit"]["default"] == 10
    assert (
        registered["code_lookup_field"].parameters["properties"]["include_tests"]["default"]
        is False
    )
    assert registered["code_lookup_file"].parameters["properties"]["limit"]["default"] == 10
    assert (
        registered["code_lookup_file"].parameters["properties"]["include_tests"]["default"] is False
    )
    assert registered["code_impact"].parameters["properties"]["limit"]["default"] == 10
    assert registered["code_impact"].parameters["properties"]["depth"]["default"] == 2
    assert registered["code_impact"].parameters["properties"]["include_tests"]["default"] is True
    assert registered["code_impact"].parameters["properties"]["view"]["default"] == "callers"
    assert registered["code_callers"].parameters["properties"]["limit"]["default"] == 10
    assert registered["code_callers"].parameters["properties"]["include_tests"]["default"] is False
    assert registered["code_callees"].parameters["properties"]["limit"]["default"] == 10
    assert registered["code_callees"].parameters["properties"]["include_tests"]["default"] is False
    context_defaults = registered["code_method_context"].parameters["properties"]
    assert context_defaults["method_limit"]["default"] == 5
    assert context_defaults["include_tests"]["default"] is False
    assert context_defaults["neighbor_limit"]["default"] == 5
    assert registered["code_hot_paths"].parameters["properties"]["limit"]["default"] == 5
    assert registered["code_operation_hot_paths"].parameters["properties"]["limit"]["default"] == 5
    assert "owner_fragment" in registered["code_operation_hot_paths"].parameters["properties"]
    assert "path_contains" in registered["code_operation_hot_paths"].parameters["properties"]
    assert registered["code_resource_risk_scan"].parameters["properties"]["limit"]["default"] == 5
    assert "extensions" in registered["code_resource_risk_scan"].parameters["properties"]
    assert registered["code_test_context"].parameters["properties"]["limit"]["default"] == 5
    assert registered["code_quality_stats"].parameters["properties"]["limit"]["default"] == 5
    quality_defaults = registered["code_quality_stats"].parameters["properties"]
    assert quality_defaults["include_tests"]["default"] is False


def test_code_callers_are_compact_and_low_limit_by_default():
    client = CallGraphClient()
    tools = MemgraphIngesterTools(client, MemgraphConfig(default_project="demo"))

    result = tools.code_callers("demo.Bar.b")

    assert client.calls[0]["parameters"]["limit"] == 10
    assert result["callers"] == [
        {
            "owner": "Foo",
            "name": "a",
            "path": "src/main/java/demo/Foo.java",
            "startLine": 10,
            "endLine": 20,
            "calleeOwner": "Bar",
            "calleeName": "b",
        }
    ]
    assert result["meta"]["totalCount"] == 100
    assert result["meta"]["hasMore"] is True


def test_code_callers_can_return_table_json():
    client = CallGraphClient()
    tools = MemgraphIngesterTools(client, MemgraphConfig(default_project="demo"))

    result = tools.code_callers("demo.Bar.b", output_format="table_json")

    assert result["callers"]["cols"] == [
        "owner",
        "name",
        "path",
        "startLine",
        "endLine",
        "calleeOwner",
        "calleeName",
    ]
    assert result["callers"]["rows"] == [
        [
            "Foo",
            "a",
            "src/main/java/demo/Foo.java",
            10,
            20,
            "Bar",
            "b",
        ]
    ]
    assert result["meta"]["format"] == "table_json"


def test_code_callees_can_return_legacy_shape():
    client = CallGraphClient()
    tools = MemgraphIngesterTools(client, MemgraphConfig(default_project="demo"))

    result = tools.code_callees("demo.Foo", compact=False, limit=5)

    assert client.calls[0]["parameters"]["limit"] == 5
    assert "callerSignature" in result["callees"][0]
    assert "calleePath" in result["callees"][0]


def test_code_callees_compact_includes_path():
    client = CallGraphClient()
    tools = MemgraphIngesterTools(client, MemgraphConfig(default_project="demo"))

    result = tools.code_callees("demo.Foo", compact=True)

    assert result["callees"] == [
        {
            "callerOwner": "Foo",
            "callerName": "a",
            "owner": "Bar",
            "name": "b",
            "path": "src/main/java/demo/Bar.java",
            "startLine": 30,
            "endLine": 40,
        }
    ]


def test_code_callees_compact_can_return_table_json():
    client = CallGraphClient()
    tools = MemgraphIngesterTools(client, MemgraphConfig(default_project="demo"))

    result = tools.code_callees("demo.Foo", compact=True, output_format="table_json")

    assert result["callees"]["cols"] == [
        "callerOwner",
        "callerName",
        "owner",
        "name",
        "path",
        "startLine",
        "endLine",
    ]
    assert result["callees"]["rows"] == [
        ["Foo", "a", "Bar", "b", "src/main/java/demo/Bar.java", 30, 40]
    ]
    assert result["meta"]["format"] == "table_json"


def test_code_method_context_bundles_methods_callers_and_callees():
    client = CallGraphClient()
    tools = MemgraphIngesterTools(client, MemgraphConfig(default_project="demo"))

    result = tools.code_method_context("upsertFile", output_format="table_json")

    assert result["methods"]["cols"] == ["owner", "name", "path", "startLine", "endLine"]
    assert result["methods"]["rows"] == [
        ["GraphWriter", "upsertFile", "src/main/java/demo/GraphWriter.java", 10, 20]
    ]
    assert result["callers"]["cols"] == [
        "owner",
        "name",
        "path",
        "startLine",
        "endLine",
        "calleeOwner",
        "calleeName",
    ]
    assert result["callees"]["cols"] == [
        "callerOwner",
        "callerName",
        "owner",
        "name",
        "path",
        "startLine",
        "endLine",
    ]
    assert "hasMore" in result["meta"]["methods"]
    assert "hasMore" in result["meta"]["callers"]
    assert "hasMore" in result["meta"]["callees"]
    assert result["meta"]["format"] == "table_json"


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

    assert "includeEvidence" not in result["meta"]
    assert {row["section"] for row in result["hotPaths"]} == {
        "longestMethods",
        "fanIn",
    }


def test_code_hot_paths_can_filter_sections():
    tools = make_tools()

    result = tools.code_hot_paths(
        limit=2,
        include_evidence=False,
        sections=["fanIn"],
    )

    assert result["hotPaths"] == [{"ok": True, "section": "fanIn"}]
    assert "sections" not in result["meta"]
    assert len(tools.client.calls) == 1
    assert "count(call) AS callers" in tools.client.calls[0]["query"]


def test_code_hot_paths_rejects_unknown_sections():
    tools = make_tools()

    with pytest.raises(MemgraphError, match="Unknown section"):
        tools.code_hot_paths(sections=["everything"])


def test_code_hot_paths_can_return_table_json():
    tools = make_tools()

    result = tools.code_hot_paths(limit=2, include_evidence=False, output_format="table_json")

    assert result["hotPaths"]["cols"] == ["ok", "section"]
    assert result["hotPaths"]["rows"] == [
        [True, "longestMethods"],
        [True, "fanIn"],
    ]
    assert result["meta"]["format"] == "table_json"


def test_code_quality_stats_returns_aggregate_sections():
    tools = make_tools()

    result = tools.code_quality_stats(limit=3)

    assert result["project"] == "demo"
    assert "inventory" in result
    assert "methodLengths" in result
    assert "fanIn" in result
    assert "fanOut" in result
    assert "inventory" in result


def test_code_quality_stats_can_return_table_json_sections():
    tools = make_tools()

    result = tools.code_quality_stats(limit=3, output_format="table_json")

    assert result["inventory"] == {"cols": ["ok"], "rows": [[True]]}
    assert result["chunksByLabel"] == {"cols": ["ok"], "rows": [[True]]}
    assert result["filesByMethods"] == {"cols": ["ok"], "rows": [[True]]}
    assert result["methodLengths"] == {"ok": True}
    assert result["meta"]["format"] == "table_json"


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
    assert "memory" not in result
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


def test_raw_read_cypher_can_return_table_json():
    tools = make_tools()

    result = tools.raw_read_cypher(
        "MATCH (n {project: $project}) RETURN n LIMIT $limit",
        output_format="table_json",
    )

    assert result["rows"] == {"cols": ["ok"], "rows": [[True]]}
    assert result["meta"]["format"] == "table_json"


def test_code_lookup_type_orders_by_return_alias_after_collect():
    tools = make_tools()

    tools.code_lookup_type(type_name="GraphWriter", include_members=False)

    query = next(
        call["query"]
        for call in tools.client.calls
        if "collect(DISTINCT file.path) AS files" in call["query"]
    )
    assert "collect(DISTINCT file.path) AS files" in query
    assert "ORDER BY t.fqn" in query


def test_code_lookup_methods_orders_by_return_alias_after_collect():
    tools = make_tools()

    tools.code_lookup_methods("GraphWriter")

    query = tools.client.calls[0]["query"]
    assert "collect(DISTINCT file.path) AS files" in query
    assert "ORDER BY sortSignature" in query
    assert "ORDER BY method.signature" not in query


def test_code_lookup_methods_can_return_compact_ranges():
    tools = make_tools()

    tools.code_lookup_methods("GraphWriter", compact=True)

    query = tools.client.calls[0]["query"]
    assert "method.startLine AS startLine" in query
    assert "method.returnType AS returnType" not in query
    assert "method.isSynthetic AS isSynthetic" not in query


def test_code_lookup_methods_can_return_table_json():
    tools = make_tools()

    result = tools.code_lookup_methods("GraphWriter", compact=True, output_format="table_json")

    assert result["methods"] == {
        "cols": ["owner", "name", "path", "startLine", "endLine"],
        "rows": [["GraphWriter", "upsertFile", "src/main/java/demo/GraphWriter.java", 10, 20]],
    }
    assert result["meta"]["format"] == "table_json"


def test_code_lookup_field_can_return_table_json():
    client = FieldFileLookupClient()
    tools = MemgraphIngesterTools(client, MemgraphConfig(default_project="demo"))

    result = tools.code_lookup_field("cypher", compact=True, output_format="table_json")

    assert result["fields"] == {
        "cols": ["owner", "name", "fqn", "path", "startLine", "endLine"],
        "rows": [
            [
                "GraphWriter",
                "cypher",
                "demo.GraphWriter#cypher",
                "src/main/java/demo/GraphWriter.java",
                12,
                12,
            ]
        ],
    }
    assert result["meta"]["format"] == "table_json"


def test_code_lookup_file_can_return_table_json():
    client = FieldFileLookupClient()
    tools = MemgraphIngesterTools(client, MemgraphConfig(default_project="demo"))

    result = tools.code_lookup_file("GraphWriter.java", compact=True, output_format="table_json")

    assert result["files"] == {
        "cols": ["path", "language", "definitionCount", "chunkCount"],
        "rows": [["src/main/java/demo/GraphWriter.java", "java", 4, 9]],
    }
    assert result["meta"]["format"] == "table_json"


def test_code_impact_returns_targets_and_boundary_flags():
    client = ImpactClient()
    tools = MemgraphIngesterTools(client, MemgraphConfig(default_project="demo"))

    result = tools.code_impact("refreshCodeChunkEmbeddings", output_format="table_json")

    assert result["targetMethods"] == {
        "cols": ["owner", "name", "signature", "path", "startLine", "endLine"],
        "rows": [
            [
                "GraphWriter",
                "refreshCodeChunkEmbeddings",
                "demo.writer.GraphWriter.refreshCodeChunkEmbeddings(Settings, boolean)",
                "src/main/java/demo/writer/GraphWriter.java",
                620,
                623,
            ]
        ],
    }
    assert result["impacts"]["cols"] == [
        "depth",
        "owner",
        "name",
        "path",
        "startLine",
        "endLine",
        "viaOwner",
        "viaName",
        "targetOwner",
        "targetName",
        "isTest",
        "crossesPackageBoundary",
    ]
    assert result["impacts"]["rows"][0] == [
        1,
        "IngestionOrchestrator",
        "refresh",
        "src/main/java/demo/ingestion/IngestionOrchestrator.java",
        400,
        420,
        None,
        None,
        "GraphWriter",
        "refreshCodeChunkEmbeddings",
        False,
        True,
    ]
    assert result["impacts"]["rows"][1][10:] == [True, True]
    assert "targetCount" not in result["meta"]
    assert result["meta"]["format"] == "table_json"


def test_code_impact_can_return_file_view():
    client = ImpactClient()
    tools = MemgraphIngesterTools(client, MemgraphConfig(default_project="demo"))

    result = tools.code_impact(
        "refreshCodeChunkEmbeddings",
        view="files",
        output_format="table_json",
    )

    assert result["files"]["cols"] == [
        "path",
        "role",
        "minDepth",
        "callerCount",
        "testCallerCount",
        "crossPackageCount",
        "risk",
    ]
    assert "view" not in result["meta"]


def test_code_operation_hot_paths_returns_risk_hints():
    client = UniversalFlowClient()
    tools = MemgraphIngesterTools(client, MemgraphConfig(default_project="demo"))

    result = tools.code_operation_hot_paths(
        owner_fragment="Writer",
        path_contains="demo",
        output_format="table_json",
    )

    assert result["operationHotPaths"]["cols"] == [
        "owner",
        "name",
        "path",
        "startLine",
        "endLine",
        "sinkCallEdges",
        "distinctSinks",
        "sinks",
        "riskHints",
    ]
    assert result["operationHotPaths"]["rows"][0][-1] == [
        "many-sink-calls",
        "large-method",
        "multi-sink",
    ]
    assert client.calls[-1]["parameters"]["owner_fragment"] == "writer"
    assert client.calls[-1]["parameters"]["path_contains"] == "demo"


def test_code_resource_risk_scan_returns_compact_resource_risks():
    client = UniversalFlowClient()
    tools = MemgraphIngesterTools(client, MemgraphConfig(default_project="demo"))

    result = tools.code_resource_risk_scan(
        path_contains="resources",
        extensions=["cypher"],
        limit=5,
        output_format="table_json",
    )

    assert result["resourceRisks"]["cols"] == [
        "path",
        "language",
        "risk",
        "score",
        "pattern",
        "line",
        "evidence",
        "why",
        "occurrences",
        "heuristic",
    ]
    patterns = [row[4] for row in result["resourceRisks"]["rows"]]
    assert "per-row-unbounded-traversal" in patterns
    assert "unbounded-variable-length-traversal" in patterns
    assert "filters" not in result["meta"]
    assert "scannedFiles" not in result["meta"]


def test_code_test_context_returns_tests_and_production_callees():
    client = UniversalFlowClient()
    tools = MemgraphIngesterTools(client, MemgraphConfig(default_project="demo"))

    result = tools.code_test_context("refreshes", output_format="table_json")

    assert result["tests"]["cols"] == [
        "owner",
        "name",
        "signature",
        "path",
        "startLine",
        "endLine",
    ]
    assert result["productionCallees"]["rows"][0][0] == "Writer"
    assert result["meta"]["exactMatches"] == 1
    assert "methodFragment" not in result["meta"]


def test_code_test_context_anchors_class_method_fragments():
    client = UniversalFlowClient()
    tools = MemgraphIngesterTools(client, MemgraphConfig(default_project="demo"))

    result = tools.code_test_context(
        "WriterTest.refreshesDirtyCodeEmbeddingsInWatchMode",
        output_format="json",
    )

    test_query_params = client.calls[-3]["parameters"]
    assert test_query_params["owner_fragment"] == "WriterTest"
    assert test_query_params["min_term_matches"] == 3
    assert "terms" not in result["meta"]


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
