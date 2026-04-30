"""Importers for various design brief sources."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from blueprint.importers.csv_backlog_importer import CsvBacklogImporter
    from blueprint.importers.manual_importer import ManualBriefImporter
    from blueprint.importers.obsidian_importer import ObsidianImporter
    from blueprint.importers.plan_markdown_importer import PlanMarkdownImporter
    from blueprint.importers.slack_thread_importer import SlackThreadImporter
    from blueprint.importers.source_jsonl_importer import SourceJsonlImporter
    from blueprint.importers.toml_backlog_importer import TomlBacklogImporter


_EXPORTS = {
    "CsvBacklogImporter": "blueprint.importers.csv_backlog_importer",
    "ManualBriefImporter": "blueprint.importers.manual_importer",
    "ObsidianImporter": "blueprint.importers.obsidian_importer",
    "PlanMarkdownImporter": "blueprint.importers.plan_markdown_importer",
    "SlackThreadImporter": "blueprint.importers.slack_thread_importer",
    "SourceJsonlImporter": "blueprint.importers.source_jsonl_importer",
    "TomlBacklogImporter": "blueprint.importers.toml_backlog_importer",
}


def __getattr__(name: str) -> Any:
    """Load importer classes on demand to avoid unrelated dependency side effects."""
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    value = getattr(import_module(_EXPORTS[name]), name)
    globals()[name] = value
    return value

__all__ = [
    "CsvBacklogImporter",
    "ManualBriefImporter",
    "ObsidianImporter",
    "PlanMarkdownImporter",
    "SlackThreadImporter",
    "SourceJsonlImporter",
    "TomlBacklogImporter",
]
