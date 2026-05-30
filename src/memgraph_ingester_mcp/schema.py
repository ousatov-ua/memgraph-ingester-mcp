"""Whitelists for graph labels, memory fields, and lifecycle values."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MemorySpec:
    label: str
    relation: str
    fields: frozenset[str]
    statuses: frozenset[str] = frozenset()


MEMORY_SPECS: dict[str, MemorySpec] = {
    "Decision": MemorySpec(
        label="Decision",
        relation="HAS_DECISION",
        fields=frozenset({"title", "topic", "status", "rationale", "consequences"}),
        statuses=frozenset({"proposed", "accepted", "rejected", "superseded"}),
    ),
    "ADR": MemorySpec(
        label="ADR",
        relation="HAS_ADR",
        fields=frozenset({"number", "title", "status", "context", "decision", "consequences"}),
        statuses=frozenset({"draft", "proposed", "accepted", "rejected", "superseded"}),
    ),
    "Rule": MemorySpec(
        label="Rule",
        relation="HAS_RULE",
        fields=frozenset({"title", "topic", "severity", "description"}),
        statuses=frozenset({"hard", "soft", "recommendation"}),
    ),
    "Context": MemorySpec(
        label="Context",
        relation="HAS_CONTEXT",
        fields=frozenset({"title", "topic", "content", "source"}),
    ),
    "Finding": MemorySpec(
        label="Finding",
        relation="HAS_FINDING",
        fields=frozenset({"title", "topic", "type", "status", "summary", "evidence"}),
        statuses=frozenset({"open", "resolved", "obsolete"}),
    ),
    "Task": MemorySpec(
        label="Task",
        relation="HAS_TASK",
        fields=frozenset({"title", "status", "priority", "description"}),
        statuses=frozenset({"todo", "doing", "done", "blocked", "cancelled"}),
    ),
    "Risk": MemorySpec(
        label="Risk",
        relation="HAS_RISK",
        fields=frozenset({"title", "topic", "severity", "status", "mitigation"}),
        statuses=frozenset({"open", "mitigated", "accepted", "obsolete"}),
    ),
    "Question": MemorySpec(
        label="Question",
        relation="HAS_QUESTION",
        fields=frozenset({"title", "status", "answer"}),
        statuses=frozenset({"open", "answered", "obsolete"}),
    ),
    "Idea": MemorySpec(
        label="Idea",
        relation="HAS_IDEA",
        fields=frozenset({"title", "topic", "status", "notes"}),
        statuses=frozenset({"proposed", "accepted", "rejected", "obsolete"}),
    ),
}

TARGET_TYPES = frozenset(
    {
        "Code",
        "Package",
        "File",
        "Class",
        "Interface",
        "Annotation",
        "Method",
        "Field",
    }
)

READ_ONLY_PREFIXES = (
    "match ",
    "optional match ",
    "with ",
    "return ",
    "call ",
    "show ",
)

WRITE_KEYWORDS = frozenset(
    {
        "create",
        "merge",
        "set",
        "delete",
        "detach",
        "remove",
        "drop",
        "load",
        "foreach",
    }
)
