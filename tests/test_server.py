import asyncio
import json

import pytest
from memgraph_ingester_tool import ToolConfig, ToolError

from memgraph_ingester_mcp.server import _compact_json_response, create_server


class FakeClient:
    def __init__(self):
        self.calls = []

    def run(self, query, parameters=None, *, write=False):
        self.calls.append({"query": query, "parameters": dict(parameters or {}), "write": write})
        if "RETURN inventory, methodLengths" in query:
            return [
                {
                    "inventory": [{"ok": True}],
                    "methodLengths": {"ok": True},
                    "fanOut": {"ok": True},
                    "fanIn": {"ok": True},
                    "typeSizes": {"ok": True},
                    "chunksByLabel": [{"ok": True}],
                    "filesByMethods": [{"ok": True}],
                }
            ]
        return [{"ok": True}]


def test_compact_json_response_serializes_database_temporal_values():
    class TemporalValue:
        def __str__(self):
            return "2026-06-08T21:18:39Z"

    assert (
        _compact_json_response({"createdAt": TemporalValue()})
        == '{"createdAt":"2026-06-08T21:18:39Z"}'
    )


def test_server_declares_usage_discipline_instructions():
    mcp = create_server(ToolConfig(default_project="demo"), client=FakeClient())

    instructions = mcp.instructions or ""
    assert "edit-compile-test loop" in instructions
    assert "meta.nextSkip" in instructions
    assert "discovery anchors" in instructions
    assert "code_lookup_type(include_members=true)" in instructions
    assert "batch independent calls" in instructions


def test_registered_code_tool_defaults_are_discovery_sized():
    mcp = create_server(ToolConfig(default_project="demo"), client=FakeClient())
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


def test_all_tools_declare_behavior_annotations():
    mcp = create_server(ToolConfig(default_project="demo"), client=FakeClient())
    registered = mcp._tool_manager._tools

    write_tools = {
        "memory_upsert",
        "memory_update_status",
        "memory_link_code_ref",
        "memory_refresh_chunk",
        "memory_refresh_embeddings",
    }
    destructive_tools = {"delete_memory"}

    for name, tool in registered.items():
        annotations = tool.annotations
        assert annotations is not None, f"{name} is missing tool annotations"
        assert annotations.openWorldHint is False
        assert annotations.idempotentHint is True
        if name in destructive_tools:
            assert annotations.readOnlyHint is False
            assert annotations.destructiveHint is True
        elif name in write_tools:
            assert annotations.readOnlyHint is False
            assert annotations.destructiveHint is False
        else:
            assert annotations.readOnlyHint is True, f"{name} should be read-only"
            assert annotations.destructiveHint is False


def test_all_tool_parameters_have_descriptions():
    mcp = create_server(ToolConfig(default_project="demo"), client=FakeClient())
    registered = mcp._tool_manager._tools

    for name, tool in registered.items():
        for param_name, schema in tool.parameters["properties"].items():
            described = "description" in schema or any(
                "description" in option
                for option in schema.get("anyOf", [])
                if isinstance(option, dict)
            )
            assert described, f"{name}.{param_name} is missing a description"


def test_controlled_values_are_documented_in_schemas():
    mcp = create_server(ToolConfig(default_project="demo"), client=FakeClient())
    registered = mcp._tool_manager._tools

    view = registered["code_impact"].parameters["properties"]["view"]["description"]
    assert "'callers'" in view and "'files'" in view
    detail = registered["code_flow_context"].parameters["properties"]["detail"]["description"]
    assert "'compact'" in detail and "'full'" in detail
    sections = registered["code_hot_paths"].parameters["properties"]["sections"]["description"]
    assert "longestMethods" in sections
    memory_type = registered["memory_upsert"].parameters["properties"]["memory_type"]["description"]
    assert "Decision" in memory_type and "Idea" in memory_type


def test_tool_errors_pass_through_unwrapped():
    class GuardedClient:
        def run(self, query, parameters=None, *, write=False):
            raise ToolError("Only read-oriented Cypher is allowed.")

    mcp = create_server(ToolConfig(default_project="demo"), client=GuardedClient())
    tool = mcp._tool_manager._tools["server_status"]

    with pytest.raises(ToolError, match="Only read-oriented Cypher is allowed"):
        tool.fn()


def test_connectivity_errors_are_rewrapped_with_actionable_hint():
    class DownClient:
        def run(self, query, parameters=None, *, write=False):
            raise ConnectionRefusedError("bolt://localhost:7687 refused")

    mcp = create_server(ToolConfig(default_project="demo"), client=DownClient())
    tool = mcp._tool_manager._tools["server_status"]

    with pytest.raises(ToolError) as exc_info:
        tool.fn()
    message = str(exc_info.value)
    assert "Memgraph is unreachable" in message
    assert "MEMGRAPH_TOOLS_BOLT_URI" in message


def test_unexpected_errors_are_rewrapped_as_tool_errors():
    class BrokenClient:
        def run(self, query, parameters=None, *, write=False):
            raise ValueError("bad row shape")

    mcp = create_server(ToolConfig(default_project="demo"), client=BrokenClient())
    tool = mcp._tool_manager._tools["server_status"]

    with pytest.raises(ToolError) as exc_info:
        tool.fn()
    message = str(exc_info.value)
    assert "Unexpected ValueError" in message
    assert "bad row shape" in message


def test_registered_tools_return_compact_json_text():
    mcp = create_server(ToolConfig(default_project="demo"), client=FakeClient())
    tool = mcp._tool_manager._tools["code_quality_stats"]

    content = asyncio.run(tool.run({"limit": 3}, convert_result=True))

    assert len(content) == 1
    text = content[0].text
    assert "\n" not in text
    parsed = json.loads(text)
    assert parsed["inventory"] == {"cols": ["ok"], "rows": [[True]]}
    assert parsed["methodLengths"] == {"ok": True}
