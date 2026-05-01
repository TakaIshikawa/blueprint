"""Importers for various design brief sources."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from blueprint.importers.adr_markdown_importer import AdrMarkdownImporter
    from blueprint.importers.csv_backlog_importer import CsvBacklogImporter
    from blueprint.importers.execution_event_jsonl_importer import ExecutionEventJsonlImporter
    from blueprint.importers.manual_importer import ManualBriefImporter
    from blueprint.importers.meeting_notes_importer import MeetingNotesImporter
    from blueprint.importers.obsidian_importer import ObsidianImporter
    from blueprint.importers.plan_markdown_importer import PlanMarkdownImporter
    from blueprint.importers.slack_thread_importer import SlackThreadImporter
    from blueprint.importers.source_jsonl_importer import SourceJsonlImporter
    from blueprint.importers.toml_backlog_importer import TomlBacklogImporter
    from blueprint.importers.yaml_backlog_importer import YamlBacklogImporter


_EXPORTS = {
    "AdrMarkdownImporter": "blueprint.importers.adr_markdown_importer",
    "CsvBacklogImporter": "blueprint.importers.csv_backlog_importer",
    "ExecutionEventJsonlImporter": "blueprint.importers.execution_event_jsonl_importer",
    "ManualBriefImporter": "blueprint.importers.manual_importer",
    "MeetingNotesImporter": "blueprint.importers.meeting_notes_importer",
    "ObsidianImporter": "blueprint.importers.obsidian_importer",
    "PlanMarkdownImporter": "blueprint.importers.plan_markdown_importer",
    "SlackThreadImporter": "blueprint.importers.slack_thread_importer",
    "SourceJsonlImporter": "blueprint.importers.source_jsonl_importer",
    "TomlBacklogImporter": "blueprint.importers.toml_backlog_importer",
    "YamlBacklogImporter": "blueprint.importers.yaml_backlog_importer",
}


def __getattr__(name: str) -> Any:
    """Load importer classes on demand to avoid unrelated dependency side effects."""
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    value = getattr(import_module(_EXPORTS[name]), name)
    globals()[name] = value
    return value


__all__ = [
    "AdrMarkdownImporter",
    "CsvBacklogImporter",
    "ExecutionEventJsonlImporter",
    "ManualBriefImporter",
    "MeetingNotesImporter",
    "ObsidianImporter",
    "PlanMarkdownImporter",
    "SlackThreadImporter",
    "SourceJsonlImporter",
    "TomlBacklogImporter",
    "YamlBacklogImporter",
]
