"""Cypher query templates loaded from .cypher files at import time.

All strings are raw Cypher text. Templates that require runtime substitution
use ``__PLACEHOLDER__`` markers; callers call ``.replace()`` before execution.

Placeholder reference
---------------------
Code queries
  ``__RETURN_PROJECTION__``   – SELECT-list projection (code_search, code_lookup_*)
  ``__ORDER_BY__``            – ORDER BY expression
  ``__PROJECTION__``          – full RETURN projection (code_lookup_field/file)
  ``__TEXT_PROJECTION__``     – optional ``", chunk.text AS text"`` suffix
  ``__PREDICATE__``           – WHERE predicate for type lookup (``t.fqn = $fqn`` etc.)
  ``__MEMBER_COUNT_CYPHER__`` – optional MATCH block for member counts
  ``__EXTRA_TYPE_COLS__``     – optional extra column expressions
  ``__MEMBER_COUNT_COLS__``   – optional extra column aliases
  ``__METHOD_PROJECTION__``   – method RETURN columns
  ``__FIELD_PROJECTION__``    – field RETURN columns

Memory queries
  ``__MEMORY_LABEL_PREDICATE__`` – OR-joined label predicate for all memory types
  ``__LABEL__``               – concrete node label (e.g. ``Task``)
  ``__RELATION__``            – concrete relationship type (e.g. ``HAS_TASK``)
  ``__TARGET_LABEL__``        – CodeRef target node label
  ``__TARGET_PREDICATE__``    – CodeRef target match predicate
"""

from __future__ import annotations

from pathlib import Path

_QUERIES_DIR = Path(__file__).parent


def _load(rel: str) -> str:
    """Read a .cypher file relative to this package directory."""
    return (_QUERIES_DIR / rel).read_text(encoding="utf-8")


# ── server status ─────────────────────────────────────────────────────────────
SERVER_STATUS_LANGUAGES: str = _load("code/server_status_languages.cypher")
SERVER_STATUS_INVENTORY: str = _load("code/server_status_inventory.cypher")
SERVER_STATUS_MEMORIES: str = _load("code/server_status_memories.cypher")

# ── code orientation ──────────────────────────────────────────────────────────
CODE_ORIENTATION_LANGUAGES: str = _load("code/code_orientation_languages.cypher")
CODE_ORIENTATION_PACKAGES: str = _load("code/code_orientation_packages.cypher")
CODE_ORIENTATION_LARGEST_TYPES: str = _load("code/code_orientation_largest_types.cypher")
CODE_ORIENTATION_CROSS_OWNER_CALLS: str = _load("code/code_orientation_cross_owner_calls.cypher")

# ── search ────────────────────────────────────────────────────────────────────
CODE_SEARCH: str = _load("code/code_search.cypher")
LEXICAL_CHUNK_ROWS: str = _load("code/lexical_chunk_rows.cypher")

# ── lookup: types ─────────────────────────────────────────────────────────────
CODE_LOOKUP_TYPE: str = _load("code/code_lookup_type.cypher")
CODE_LOOKUP_TYPE_COUNT: str = _load("code/code_lookup_type_count.cypher")
CODE_LOOKUP_TYPE_MEMBERS_METHODS: str = _load("code/code_lookup_type_members_methods.cypher")
CODE_LOOKUP_TYPE_MEMBERS_FIELDS: str = _load("code/code_lookup_type_members_fields.cypher")

# ── lookup: methods ───────────────────────────────────────────────────────────
CODE_LOOKUP_METHODS: str = _load("code/code_lookup_methods.cypher")
CODE_LOOKUP_METHODS_COUNT: str = _load("code/code_lookup_methods_count.cypher")

# ── lookup: fields ────────────────────────────────────────────────────────────
CODE_LOOKUP_FIELD: str = _load("code/code_lookup_field.cypher")
CODE_LOOKUP_FIELD_COUNT: str = _load("code/code_lookup_field_count.cypher")

# ── lookup: files ─────────────────────────────────────────────────────────────
CODE_LOOKUP_FILE: str = _load("code/code_lookup_file.cypher")
CODE_LOOKUP_FILE_COUNT: str = _load("code/code_lookup_file_count.cypher")

# ── impact ────────────────────────────────────────────────────────────────────
CODE_IMPACT_TARGETS: str = _load("code/code_impact_targets.cypher")
CODE_IMPACT_CALLERS: str = _load("code/code_impact_callers.cypher")
CODE_IMPACT_TEXT_REFERENCE: str = _load("code/code_impact_text_reference.cypher")

# ── call graph ────────────────────────────────────────────────────────────────
CODE_CALLERS: str = _load("code/code_callers.cypher")
CODE_CALLERS_COUNT: str = _load("code/code_callers_count.cypher")
CODE_CALLEES: str = _load("code/code_callees.cypher")
CODE_CALLEES_COUNT: str = _load("code/code_callees_count.cypher")

# ── hot paths ─────────────────────────────────────────────────────────────────
CODE_HOT_PATHS_LARGEST_TYPES: str = _load("code/code_hot_paths_largest_types.cypher")
CODE_HOT_PATHS_LONGEST_METHODS: str = _load("code/code_hot_paths_longest_methods.cypher")
CODE_HOT_PATHS_FAN_IN: str = _load("code/code_hot_paths_fan_in.cypher")
CODE_HOT_PATHS_FAN_OUT: str = _load("code/code_hot_paths_fan_out.cypher")
CODE_OPERATION_HOT_PATHS: str = _load("code/code_operation_hot_paths.cypher")

# ── resource / quality ────────────────────────────────────────────────────────
CODE_RESOURCE_RISK_SCAN: str = _load("code/code_resource_risk_scan.cypher")
CODE_QUALITY_STATS: str = _load("code/code_quality_stats.cypher")

# ── hierarchy ─────────────────────────────────────────────────────────────────
CODE_HIERARCHY_CLASS: str = _load("code/code_hierarchy_class.cypher")
CODE_HIERARCHY_ANCESTORS: str = _load("code/code_hierarchy_ancestors.cypher")
CODE_HIERARCHY_IMPLEMENTORS: str = _load("code/code_hierarchy_implementors.cypher")

# ── test context ──────────────────────────────────────────────────────────────
CODE_TEST_METHODS: str = _load("code/code_test_methods.cypher")
CODE_TEST_FILES: str = _load("code/code_test_files.cypher")
CODE_TEST_PRODUCTION_CALLEES: str = _load("code/code_test_production_callees.cypher")

# ── file / flow context (from code_context.py) ────────────────────────────────
CODE_FILE_CONTEXT: str = _load("code/code_file_context.cypher")
CODE_FLOW_CONTEXT_EDGES: str = _load("code/code_flow_context_edges.cypher")

# ── memory orientation ────────────────────────────────────────────────────────
MEMORY_ORIENTATION_RULES: str = _load("memory/memory_orientation_rules.cypher")
MEMORY_ORIENTATION_OPEN_FINDINGS: str = _load("memory/memory_orientation_open_findings.cypher")
MEMORY_ORIENTATION_ACTIVE_TASKS: str = _load("memory/memory_orientation_active_tasks.cypher")
MEMORY_ORIENTATION_OPEN_QUESTIONS: str = _load(
    "memory/memory_orientation_open_questions.cypher"
)
MEMORY_ORIENTATION_OPEN_RISKS: str = _load("memory/memory_orientation_open_risks.cypher")

# ── memory CRUD ───────────────────────────────────────────────────────────────
MEMORY_SEARCH: str = _load("memory/memory_search.cypher")
MEMORY_GET: str = _load("memory/memory_get.cypher")
MEMORY_DELETE_READ: str = _load("memory/memory_delete_read.cypher")
MEMORY_DELETE_WRITE: str = _load("memory/memory_delete_write.cypher")
MEMORY_DELETE_ORPHAN_REFS: str = _load("memory/memory_delete_orphan_refs.cypher")
MEMORY_UPSERT: str = _load("memory/memory_upsert.cypher")
MEMORY_UPDATE_STATUS: str = _load("memory/memory_update_status.cypher")
MEMORY_LINK_CODE_REF: str = _load("memory/memory_link_code_ref.cypher")
MEMORY_REFRESH_CHUNK: str = _load("memory/memory_refresh_chunk.cypher")
MEMORY_REFRESH_EMBEDDINGS_PENDING: str = _load(
    "memory/memory_refresh_embeddings_pending.cypher"
)
MEMORY_REFRESH_EMBEDDINGS_EMBED: str = _load("memory/memory_refresh_embeddings_embed.cypher")
MEMORY_REFRESH_EMBEDDINGS_MARK: str = _load("memory/memory_refresh_embeddings_mark.cypher")
