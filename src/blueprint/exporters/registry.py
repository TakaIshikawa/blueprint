"""Registry of supported execution plan exporters."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from blueprint.exporters.adr import ADRExporter
from blueprint.exporters.agent_prompt_pack import AgentPromptPackExporter
from blueprint.exporters.asana_csv import AsanaCsvExporter
from blueprint.exporters.azure_devops_csv import AzureDevOpsCsvExporter
from blueprint.exporters.calendar import CalendarExporter
from blueprint.exporters.checklist import ChecklistExporter
from blueprint.exporters.claude_code import ClaudeCodeExporter
from blueprint.exporters.clickup_csv import ClickUpCsvExporter
from blueprint.exporters.codex import CodexExporter
from blueprint.exporters.confluence_markdown import ConfluenceMarkdownExporter
from blueprint.exporters.coverage_matrix import CoverageMatrixExporter
from blueprint.exporters.critical_path_report import CriticalPathReportExporter
from blueprint.exporters.csv_tasks import CsvTasksExporter
from blueprint.exporters.discord_digest import DiscordDigestExporter
from blueprint.exporters.file_impact_map import FileImpactMapExporter
from blueprint.exporters.gantt import GanttExporter
from blueprint.exporters.github_actions import GitHubActionsExporter
from blueprint.exporters.github_issues import GitHubIssuesExporter
from blueprint.exporters.github_projects_csv import GitHubProjectsCsvExporter
from blueprint.exporters.gitlab_issues import GitLabIssuesExporter
from blueprint.exporters.html_report import HtmlReportExporter
from blueprint.exporters.jira_csv import JiraCsvExporter
from blueprint.exporters.junit_tasks import JUnitTasksExporter
from blueprint.exporters.kanban import KanbanExporter
from blueprint.exporters.linear import LinearExporter
from blueprint.exporters.mermaid import MermaidExporter
from blueprint.exporters.milestone_burndown_csv import MilestoneBurndownCsvExporter
from blueprint.exporters.milestone_summary import MilestoneSummaryExporter
from blueprint.exporters.notion_markdown import NotionMarkdownExporter
from blueprint.exporters.openproject_csv import OpenProjectCsvExporter
from blueprint.exporters.opsgenie_digest import OpsgenieDigestExporter
from blueprint.exporters.pagerduty_digest import PagerDutyDigestExporter
from blueprint.exporters.plan_snapshot import PlanSnapshotExporter
from blueprint.exporters.raci_matrix import RaciMatrixExporter
from blueprint.exporters.relay import RelayExporter
from blueprint.exporters.relay_yaml import RelayYamlExporter
from blueprint.exporters.release_notes import ReleaseNotesExporter
from blueprint.exporters.risk_register import RiskRegisterExporter
from blueprint.exporters.sarif_audit import SarifAuditExporter
from blueprint.exporters.slack_digest import SlackDigestExporter
from blueprint.exporters.smoothie import SmoothieExporter
from blueprint.exporters.status_report import StatusReportExporter
from blueprint.exporters.task_bundle import TaskBundleExporter
from blueprint.exporters.task_queue_jsonl import TaskQueueJsonlExporter
from blueprint.exporters.taskfile import TaskfileExporter
from blueprint.exporters.teamwork_csv import TeamworkCsvExporter
from blueprint.exporters.teams_digest import TeamsDigestExporter
from blueprint.exporters.trello_json import TrelloJsonExporter
from blueprint.exporters.vscode_tasks import VSCodeTasksExporter
from blueprint.exporters.wave_schedule import WaveScheduleExporter

ExporterFactory = Callable[[], Any]


@dataclass(frozen=True, slots=True)
class ExporterRegistration:
    """Metadata and constructor for one supported export target."""

    target: str
    factory: ExporterFactory
    default_format: str
    extension: str
    aliases: tuple[str, ...] = ()

    def create(self) -> Any:
        """Create a fresh exporter instance."""
        return self.factory()


_REGISTRATIONS: tuple[ExporterRegistration, ...] = (
    ExporterRegistration("adr", ADRExporter, "markdown", ""),
    ExporterRegistration("agent-prompt-pack", AgentPromptPackExporter, "markdown", ""),
    ExporterRegistration("relay", RelayExporter, "json", ".json"),
    ExporterRegistration("relay-yaml", RelayYamlExporter, "yaml", ".yaml"),
    ExporterRegistration("smoothie", SmoothieExporter, "markdown", ".md"),
    ExporterRegistration("codex", CodexExporter, "markdown", ".md"),
    ExporterRegistration("claude-code", ClaudeCodeExporter, "markdown", ".md"),
    ExporterRegistration("confluence-markdown", ConfluenceMarkdownExporter, "markdown", ".md"),
    ExporterRegistration("asana-csv", AsanaCsvExporter, "csv", ".csv"),
    ExporterRegistration("azure-devops-csv", AzureDevOpsCsvExporter, "csv", ".csv"),
    ExporterRegistration("calendar", CalendarExporter, "icalendar", ".ics"),
    ExporterRegistration("checklist", ChecklistExporter, "markdown", ".md"),
    ExporterRegistration("clickup-csv", ClickUpCsvExporter, "csv", ".csv"),
    ExporterRegistration("coverage-matrix", CoverageMatrixExporter, "markdown", ".md"),
    ExporterRegistration("critical-path-report", CriticalPathReportExporter, "markdown", ".md"),
    ExporterRegistration("discord-digest", DiscordDigestExporter, "markdown", ".md"),
    ExporterRegistration("mermaid", MermaidExporter, "mermaid", ".mmd"),
    ExporterRegistration("milestone-burndown-csv", MilestoneBurndownCsvExporter, "csv", ".csv"),
    ExporterRegistration("milestone-summary", MilestoneSummaryExporter, "markdown", ".md"),
    ExporterRegistration("notion-markdown", NotionMarkdownExporter, "markdown", ".md"),
    ExporterRegistration("openproject-csv", OpenProjectCsvExporter, "csv", ".csv"),
    ExporterRegistration("opsgenie-digest", OpsgenieDigestExporter, "json", ".json"),
    ExporterRegistration("pagerduty-digest", PagerDutyDigestExporter, "markdown", ".md"),
    ExporterRegistration("plan-snapshot", PlanSnapshotExporter, "json", ".json"),
    ExporterRegistration("csv-tasks", CsvTasksExporter, "csv", ".csv"),
    ExporterRegistration("file-impact-map", FileImpactMapExporter, "markdown", ".md"),
    ExporterRegistration("gantt", GanttExporter, "mermaid", ".mmd"),
    ExporterRegistration("github-actions", GitHubActionsExporter, "yaml", ".yml"),
    ExporterRegistration("github-issues", GitHubIssuesExporter, "markdown", ""),
    ExporterRegistration("github-projects-csv", GitHubProjectsCsvExporter, "csv", ".csv"),
    ExporterRegistration("gitlab-issues", GitLabIssuesExporter, "json", ".json"),
    ExporterRegistration("html-report", HtmlReportExporter, "html", ".html"),
    ExporterRegistration("jira-csv", JiraCsvExporter, "csv", ".csv"),
    ExporterRegistration("linear", LinearExporter, "json", ".json"),
    ExporterRegistration("junit-tasks", JUnitTasksExporter, "xml", ".xml"),
    ExporterRegistration("kanban", KanbanExporter, "markdown", ".md"),
    ExporterRegistration("raci-matrix", RaciMatrixExporter, "markdown", ".md"),
    ExporterRegistration("release-notes", ReleaseNotesExporter, "markdown", ".md"),
    ExporterRegistration("risk-register", RiskRegisterExporter, "markdown", ".md"),
    ExporterRegistration("sarif-audit", SarifAuditExporter, "json", ".json"),
    ExporterRegistration("slack-digest", SlackDigestExporter, "markdown", ".md"),
    ExporterRegistration("status-report", StatusReportExporter, "markdown", ".md"),
    ExporterRegistration("task-bundle", TaskBundleExporter, "markdown", ""),
    ExporterRegistration("taskfile", TaskfileExporter, "yaml", ".yml"),
    ExporterRegistration("task-queue-jsonl", TaskQueueJsonlExporter, "jsonl", ".jsonl"),
    ExporterRegistration("teamwork-csv", TeamworkCsvExporter, "csv", ".csv"),
    ExporterRegistration("teams-digest", TeamsDigestExporter, "json", ".json"),
    ExporterRegistration("trello-json", TrelloJsonExporter, "json", ".json"),
    ExporterRegistration("vscode-tasks", VSCodeTasksExporter, "json", ".json"),
    ExporterRegistration("wave-schedule", WaveScheduleExporter, "json", ".json"),
)


def _implicit_aliases(target: str) -> tuple[str, ...]:
    """Return default underscore spellings for hyphenated target names."""
    underscored = target.replace("-", "_")
    return (underscored,) if underscored != target else ()


def _build_lookup() -> dict[str, ExporterRegistration]:
    lookup: dict[str, ExporterRegistration] = {}
    for registration in _REGISTRATIONS:
        for name in (
            registration.target,
            *registration.aliases,
            *_implicit_aliases(registration.target),
        ):
            lookup[name] = registration
    return lookup


_REGISTRY = {registration.target: registration for registration in _REGISTRATIONS}
_LOOKUP = _build_lookup()


def supported_target_names(*, include_aliases: bool = False) -> tuple[str, ...]:
    """Return supported target names in CLI display order."""
    names = [registration.target for registration in _REGISTRATIONS]
    if include_aliases:
        aliases = [
            name
            for registration in _REGISTRATIONS
            for name in (*registration.aliases, *_implicit_aliases(registration.target))
        ]
        names.extend(alias for alias in aliases if alias not in names)
    return tuple(names)


def supported_target_aliases() -> dict[str, str]:
    """Return alias names mapped to their canonical target names."""
    aliases: dict[str, str] = {}
    for registration in _REGISTRATIONS:
        for alias in (*registration.aliases, *_implicit_aliases(registration.target)):
            aliases[alias] = registration.target
    return aliases


def resolve_target_name(target: str) -> str:
    """Resolve a target name or alias to its canonical registry target."""
    registration = _LOOKUP.get(target)
    if registration is None:
        raise ValueError(f"Unknown export target: {target}")
    return registration.target


def get_exporter_registration(target: str) -> ExporterRegistration:
    """Return registry metadata for a target name or alias."""
    canonical_target = resolve_target_name(target)
    return _REGISTRY[canonical_target]


def create_exporter(target: str) -> Any:
    """Create an exporter for a target name or alias."""
    return get_exporter_registration(target).create()
