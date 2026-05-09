"""Importers for various design brief sources."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from blueprint.importers.azure_devops_importer import AzureDevOpsImporter
    from blueprint.importers.adr_markdown_importer import AdrMarkdownImporter
    from blueprint.importers.gitlab_importer import GitLabImporter
    from blueprint.importers.csv_backlog_importer import CsvBacklogImporter
    from blueprint.importers.execution_event_jsonl_importer import ExecutionEventJsonlImporter
    from blueprint.importers.jira import JiraImporter
    from blueprint.importers.linear import LinearImporter
    from blueprint.importers.manual_importer import ManualBriefImporter
    from blueprint.importers.meeting_notes_importer import MeetingNotesImporter
    from blueprint.importers.notion_advanced import NotionAdvancedImporter
    from blueprint.importers.obsidian_importer import ObsidianImporter
    from blueprint.importers.plan_markdown_importer import PlanMarkdownImporter
    from blueprint.importers.slack_thread_importer import SlackThreadImporter
    from blueprint.importers.source_jsonl_importer import SourceJsonlImporter
    from blueprint.importers.toml_backlog_importer import TomlBacklogImporter
    from blueprint.importers.yaml_backlog_importer import YamlBacklogImporter


_EXPORTS = {
    "AzureDevOpsImporter": "blueprint.importers.azure_devops_importer",
    "AdrMarkdownImporter": "blueprint.importers.adr_markdown_importer",
    "GitLabImporter": "blueprint.importers.gitlab_importer",
    "CsvBacklogImporter": "blueprint.importers.csv_backlog_importer",
    "ExecutionEventJsonlImporter": "blueprint.importers.execution_event_jsonl_importer",
    "JiraImporter": "blueprint.importers.jira",
    "LinearImporter": "blueprint.importers.linear",
    "ManualBriefImporter": "blueprint.importers.manual_importer",
    "MeetingNotesImporter": "blueprint.importers.meeting_notes_importer",
    "NotionAdvancedImporter": "blueprint.importers.notion_advanced",
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
    "AzureDevOpsImporter",
    "AdrMarkdownImporter",
    "GitLabImporter",
    "CsvBacklogImporter",
    "ExecutionEventJsonlImporter",
    "JiraImporter",
    "LinearImporter",
    "ManualBriefImporter",
    "MeetingNotesImporter",
    "NotionAdvancedImporter",
    "ObsidianImporter",
    "PlanMarkdownImporter",
    "SlackThreadImporter",
    "SourceJsonlImporter",
    "TomlBacklogImporter",
    "YamlBacklogImporter",
]
