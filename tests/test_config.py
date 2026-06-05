from memgraph_ingester_mcp.config import MemgraphConfig


def test_config_uses_namespaced_environment(monkeypatch):
    monkeypatch.setenv("MEMGRAPH_INGESTER_MCP_BOLT_URI", "bolt://memgraph:7687")
    monkeypatch.setenv("MEMGRAPH_INGESTER_MCP_USERNAME", "neo")
    monkeypatch.setenv("MEMGRAPH_INGESTER_MCP_PASSWORD", "secret")
    monkeypatch.setenv("MEMGRAPH_INGESTER_MCP_PROJECT", "demo")
    monkeypatch.setenv("MEMGRAPH_INGESTER_MCP_READ_ONLY", "true")
    monkeypatch.setenv("MEMGRAPH_INGESTER_MCP_QUERY_TIMEOUT_SECONDS", "12.5")
    monkeypatch.setenv("MEMGRAPH_INGESTER_MCP_EMBEDDING_DIMENSIONS", "768")
    monkeypatch.setenv("MEMGRAPH_INGESTER_MCP_COMPRESSION_ENABLED", "true")
    monkeypatch.setenv("MEMGRAPH_INGESTER_MCP_COMPRESSION_MODEL", "demo-model")
    monkeypatch.setenv("MEMGRAPH_INGESTER_MCP_COMPRESSION_DEVICE", "mps")
    monkeypatch.setenv("MEMGRAPH_INGESTER_MCP_COMPRESSION_RATE", "0.4")
    monkeypatch.setenv("MEMGRAPH_INGESTER_MCP_COMPRESSION_MIN_CHARS", "1200")
    monkeypatch.setenv("MEMGRAPH_INGESTER_MCP_COMPRESSION_FAIL_OPEN", "false")

    config = MemgraphConfig.from_environment()

    assert config.bolt_uri == "bolt://memgraph:7687"
    assert config.username == "neo"
    assert config.password == "secret"
    assert config.default_project == "demo"
    assert config.read_only is True
    assert config.query_timeout_seconds == 12.5
    assert config.embedding_dimensions == 768
    assert config.compression_enabled is True
    assert config.compression_model_name == "demo-model"
    assert config.compression_device == "mps"
    assert config.compression_rate == 0.4
    assert config.compression_min_chars == 1200
    assert config.compression_fail_open is False
