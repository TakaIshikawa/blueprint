"""Reusable validation helpers for rendered exporter artifacts."""

from __future__ import annotations

import csv
import json
import re
import tempfile
from collections import Counter
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Callable
from xml.etree import ElementTree

import yaml

from blueprint.audits.critical_path import analyze_critical_path
from blueprint.exporters.adr import ADRExporter
from blueprint.exporters.agent_prompt_pack import AgentPromptPackExporter
from blueprint.exporters.asana_csv import AsanaCsvExporter
from blueprint.exporters.azure_devops_csv import AzureDevOpsCsvExporter
from blueprint.exporters.calendar import CalendarExporter
from blueprint.exporters.checklist import ChecklistExporter
from blueprint.exporters.claude_code import ClaudeCodeExporter
from blueprint.exporters.codex import CodexExporter
from blueprint.exporters.coverage_matrix import CoverageMatrixExporter
from blueprint.exporters.critical_path_report import CriticalPathReportExporter
from blueprint.exporters.csv_tasks import CsvTasksExporter
from blueprint.exporters.file_impact_map import FileImpactMapExporter, UNASSIGNED_SECTION
from blueprint.exporters.gantt import GanttExporter
from blueprint.exporters.github_actions import GitHubActionsExporter
from blueprint.exporters.github_issues import GitHubIssuesExporter
from blueprint.exporters.gitlab_issues import GitLabIssuesExporter
from blueprint.exporters.html_report import HtmlReportExporter
from blueprint.exporters.jira_csv import JiraCsvExporter
from blueprint.exporters.junit_tasks import JUnitTasksExporter
from blueprint.exporters.kanban import KanbanExporter
from blueprint.exporters.linear import LinearExporter
from blueprint.exporters.mermaid import MermaidExporter
from blueprint.exporters.milestone_summary import MilestoneSummaryExporter
from blueprint.exporters.plan_snapshot import PlanSnapshotExporter
from blueprint.exporters.plan_snapshot import SCHEMA_VERSION as PLAN_SNAPSHOT_SCHEMA_VERSION
from blueprint.exporters.raci_matrix import RaciMatrixExporter
from blueprint.exporters.release_notes import ReleaseNotesExporter
from blueprint.exporters.relay import RelayExporter
from blueprint.exporters.relay_yaml import RelayYamlExporter
from blueprint.exporters.risk_register import RiskRegisterExporter
from blueprint.exporters.risk_register import risk_identifier
from blueprint.exporters.slack_digest import SlackDigestExporter
from blueprint.exporters.smoothie import SmoothieExporter
from blueprint.exporters.status_report import StatusReportExporter
from blueprint.exporters.task_bundle import TaskBundleExporter
from blueprint.exporters.taskfile import TaskfileExporter
from blueprint.exporters.task_queue_jsonl import TaskQueueJsonlExporter
from blueprint.exporters.trello_json import TrelloJsonExporter
from blueprint.exporters.vscode_tasks import VSCodeTasksExporter
from blueprint.exporters.wave_schedule import SCHEMA_VERSION as WAVE_SCHEDULE_SCHEMA_VERSION
from blueprint.exporters.wave_schedule import WaveScheduleExporter


ValidationCheck = Callable[[Path, dict[str, Any], dict[str, Any]], list["ValidationFinding"]]


@dataclass(frozen=True, slots=True)
class ValidationFinding:
    """One validation issue discovered while inspecting an export artifact."""

    code: str
    message: str
    path: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize the finding for human-readable or JSON output."""
        payload: dict[str, Any] = {
            "code": self.code,
            "message": self.message,
        }
        if self.path:
            payload["path"] = self.path
        return payload


@dataclass(frozen=True, slots=True)
class ExportValidationResult:
    """Structured result returned by export validation."""

    target: str
    findings: list[ValidationFinding]

    @property
    def passed(self) -> bool:
        """Return True when the artifact has no validation findings."""
        return not self.findings

    def to_dict(self) -> dict[str, Any]:
        """Serialize the result for CLI JSON output."""
        return {
            "target": self.target,
            "passed": self.passed,
            "findings": [finding.to_dict() for finding in self.findings],
        }


def validate_export(
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any],
    target: str,
) -> ExportValidationResult:
    """Render a target export to a temporary artifact and validate it."""
    exporter = create_exporter(target)
    with tempfile.TemporaryDirectory() as temp_dir:
        artifact_path = _render_temporary_artifact(
            exporter,
            execution_plan,
            implementation_brief,
            Path(temp_dir),
        )
        findings = validate_rendered_export(
            target=target,
            artifact_path=artifact_path,
            execution_plan=execution_plan,
            implementation_brief=implementation_brief,
        )
    return ExportValidationResult(target=target, findings=findings)


def validate_rendered_export(
    *,
    target: str,
    artifact_path: Path,
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any],
) -> list[ValidationFinding]:
    """Validate a previously rendered export artifact."""
    validator = _VALIDATORS.get(target)
    if validator is None:
        return [
            ValidationFinding(
                code="validation.unsupported_target",
                message=f"Validation is not supported for target '{target}'.",
            )
        ]
    return validator(artifact_path, execution_plan, implementation_brief)


def create_exporter(target: str):
    """Create the exporter used by validation and export commands."""
    exporters = {
        "adr": ADRExporter(),
        "agent-prompt-pack": AgentPromptPackExporter(),
        "relay": RelayExporter(),
        "relay-yaml": RelayYamlExporter(),
        "smoothie": SmoothieExporter(),
        "codex": CodexExporter(),
        "claude-code": ClaudeCodeExporter(),
        "asana-csv": AsanaCsvExporter(),
        "azure-devops-csv": AzureDevOpsCsvExporter(),
        "calendar": CalendarExporter(),
        "checklist": ChecklistExporter(),
        "coverage-matrix": CoverageMatrixExporter(),
        "critical-path-report": CriticalPathReportExporter(),
        "mermaid": MermaidExporter(),
        "milestone-summary": MilestoneSummaryExporter(),
        "plan-snapshot": PlanSnapshotExporter(),
        "raci-matrix": RaciMatrixExporter(),
        "csv-tasks": CsvTasksExporter(),
        "file-impact-map": FileImpactMapExporter(),
        "gantt": GanttExporter(),
        "github-actions": GitHubActionsExporter(),
        "github-issues": GitHubIssuesExporter(),
        "gitlab-issues": GitLabIssuesExporter(),
        "html-report": HtmlReportExporter(),
        "jira-csv": JiraCsvExporter(),
        "linear": LinearExporter(),
        "junit-tasks": JUnitTasksExporter(),
        "kanban": KanbanExporter(),
        "release-notes": ReleaseNotesExporter(),
        "risk-register": RiskRegisterExporter(),
        "slack-digest": SlackDigestExporter(),
        "status-report": StatusReportExporter(),
        "task-bundle": TaskBundleExporter(),
        "taskfile": TaskfileExporter(),
        "task-queue-jsonl": TaskQueueJsonlExporter(),
        "trello-json": TrelloJsonExporter(),
        "vscode-tasks": VSCodeTasksExporter(),
        "wave-schedule": WaveScheduleExporter(),
    }
    exporter = exporters.get(target)
    if exporter is None:
        raise ValueError(f"Unknown export target: {target}")
    return exporter


def _render_temporary_artifact(
    exporter,
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any],
    temp_dir: Path,
) -> Path:
    """Write the export artifact to a temporary path for inspection."""
    extension = exporter.get_extension()
    if extension:
        output_path = temp_dir / f"export{extension}"
    else:
        output_path = temp_dir / "export"

    exporter.export(execution_plan, implementation_brief, str(output_path))
    return output_path


def _validate_relay(
    artifact_path: Path,
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any],
) -> list[ValidationFinding]:
    """Validate the Relay JSON export."""
    try:
        payload = json.loads(artifact_path.read_text())
    except json.JSONDecodeError as exc:
        return [
            ValidationFinding(
                code="relay.invalid_json",
                message=f"Relay export is not valid JSON: {exc.msg}",
                path=str(artifact_path),
            )
        ]

    return _validate_relay_payload(
        payload,
        artifact_path,
        execution_plan,
        invalid_shape_code="relay.invalid_shape",
        code_prefix="relay",
        format_label="JSON",
    )


def _validate_relay_yaml(
    artifact_path: Path,
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any],
) -> list[ValidationFinding]:
    """Validate the Relay YAML export."""
    try:
        payload = yaml.safe_load(artifact_path.read_text())
    except yaml.YAMLError as exc:
        return [
            ValidationFinding(
                code="relay_yaml.invalid_yaml",
                message=f"Relay YAML export is not valid YAML: {exc}",
                path=str(artifact_path),
            )
        ]

    return _validate_relay_payload(
        payload,
        artifact_path,
        execution_plan,
        invalid_shape_code="relay_yaml.invalid_shape",
        code_prefix="relay_yaml",
        format_label="YAML",
    )


def _validate_relay_payload(
    payload: Any,
    artifact_path: Path,
    execution_plan: dict[str, Any],
    *,
    invalid_shape_code: str,
    code_prefix: str,
    format_label: str,
) -> list[ValidationFinding]:
    """Validate a parsed Relay export payload."""
    if not isinstance(payload, dict):
        return [
            ValidationFinding(
                code=invalid_shape_code,
                message=f"Relay export must be a {format_label} object.",
                path=str(artifact_path),
            )
        ]

    findings: list[ValidationFinding] = []
    required_keys = [
        "schema_version",
        "objective",
        "target_repo",
        "milestones",
        "tasks",
        "validation",
    ]
    findings.extend(
        _missing_keys(payload, required_keys, f"{code_prefix}.missing_key", str(artifact_path))
    )

    objective = payload.get("objective")
    if isinstance(objective, dict):
        findings.extend(
            _missing_keys(
                objective,
                ["title", "problem", "mvp_goal", "success_criteria"],
                f"{code_prefix}.objective.missing_key",
                str(artifact_path),
            )
        )
    else:
        findings.append(
            ValidationFinding(
                code=f"{code_prefix}.objective.invalid_shape",
                message="Relay export objective must be an object.",
                path=str(artifact_path),
            )
        )

    milestones = payload.get("milestones")
    if not isinstance(milestones, list):
        findings.append(
            ValidationFinding(
                code=f"{code_prefix}.milestones.invalid_shape",
                message="Relay export milestones must be a list.",
                path=str(artifact_path),
            )
        )
    else:
        for index, milestone in enumerate(milestones, 1):
            if not isinstance(milestone, dict):
                findings.append(
                    ValidationFinding(
                        code=f"{code_prefix}.milestone.invalid_shape",
                        message=f"Relay milestone {index} must be an object.",
                        path=str(artifact_path),
                    )
                )
                continue
            findings.extend(
                _missing_keys(
                    milestone,
                    ["id", "name"],
                    f"{code_prefix}.milestone.missing_key",
                    str(artifact_path),
                )
            )

    tasks = payload.get("tasks")
    if not isinstance(tasks, list):
        findings.append(
            ValidationFinding(
                code=f"{code_prefix}.tasks.invalid_shape",
                message="Relay export tasks must be a list.",
                path=str(artifact_path),
            )
        )
    else:
        expected_task_ids = {task["id"] for task in execution_plan.get("tasks", [])}
        rendered_task_ids: list[str] = []
        for index, task in enumerate(tasks, 1):
            if not isinstance(task, dict):
                findings.append(
                    ValidationFinding(
                        code=f"{code_prefix}.task.invalid_shape",
                        message=f"Relay task {index} must be an object.",
                        path=str(artifact_path),
                    )
                )
                continue
            task_id = task.get("id")
            if task_id is not None:
                rendered_task_ids.append(task_id)
            findings.extend(
                _missing_keys(
                    task,
                    ["id", "milestone_id", "title", "description", "depends_on", "files"],
                    f"{code_prefix}.task.missing_key",
                    str(artifact_path),
                )
            )
        rendered_task_id_set = set(rendered_task_ids)
        if len(tasks) != len(expected_task_ids):
            findings.append(
                ValidationFinding(
                    code=f"{code_prefix}.task_count_mismatch",
                    message=(
                        f"Relay export contains {len(tasks)} tasks, "
                        f"expected {len(expected_task_ids)}."
                    ),
                    path=str(artifact_path),
                )
            )
        for missing_task_id in sorted(expected_task_ids - rendered_task_id_set):
            findings.append(
                ValidationFinding(
                    code=f"{code_prefix}.missing_task",
                    message=f"Relay export is missing task '{missing_task_id}'.",
                    path=str(artifact_path),
                )
            )

    validation = payload.get("validation")
    if isinstance(validation, dict):
        findings.extend(
            _missing_keys(
                validation,
                ["commands"],
                f"{code_prefix}.validation.missing_key",
                str(artifact_path),
            )
        )
    else:
        findings.append(
            ValidationFinding(
                code=f"{code_prefix}.validation.invalid_shape",
                message="Relay export validation block must be an object.",
                path=str(artifact_path),
            )
        )

    return findings


def _validate_markdown(
    artifact_path: Path,
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any],
    *,
    required_headings: list[str],
    required_snippets: list[str] | None = None,
    path_label: str | None = None,
) -> list[ValidationFinding]:
    """Validate common Markdown exporter expectations."""
    content = artifact_path.read_text()
    findings: list[ValidationFinding] = []
    label = path_label or str(artifact_path)

    for heading in required_headings:
        if not _has_heading(content, heading):
            findings.append(
                ValidationFinding(
                    code="markdown.missing_heading",
                    message=f"Missing required Markdown heading: {heading}",
                    path=label,
                )
            )

    for snippet in required_snippets or []:
        if snippet not in content:
            findings.append(
                ValidationFinding(
                    code="markdown.missing_identifier",
                    message=f"Missing required identifier text: {snippet}",
                    path=label,
                )
            )

    if not content.strip():
        findings.append(
            ValidationFinding(
                code="markdown.empty",
                message="Markdown export is empty.",
                path=label,
            )
        )

    return findings


def _validate_codex(
    artifact_path: Path,
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any],
) -> list[ValidationFinding]:
    """Validate the Codex Markdown export."""
    return _validate_markdown(
        artifact_path,
        execution_plan,
        implementation_brief,
        required_headings=[
            "# BUILD:",
            "## Overview",
            "## Technical Specification",
            "## Build Plan",
            "## Feature Scope",
            "## Quality Requirements",
            "## Implementation Notes",
        ],
        required_snippets=[
            f"Blueprint Plan ID: `{execution_plan['id']}`",
        ],
    )


def _validate_claude_code(
    artifact_path: Path,
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any],
) -> list[ValidationFinding]:
    """Validate the Claude Code Markdown export."""
    return _validate_markdown(
        artifact_path,
        execution_plan,
        implementation_brief,
        required_headings=[
            "# Implementation:",
            "## Context",
            "## Implementation Plan",
            "## In Scope",
            "## Out of Scope",
            "## Constraints & Guidelines",
            "## Validation",
        ],
        required_snippets=[
            f"- Implementation Brief: `{implementation_brief['id']}`",
            f"- Execution Plan: `{execution_plan['id']}`",
        ],
    )


def _validate_smoothie(
    artifact_path: Path,
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any],
) -> list[ValidationFinding]:
    """Validate the Smoothie Markdown export."""
    return _validate_markdown(
        artifact_path,
        execution_plan,
        implementation_brief,
        required_headings=[
            f"# {implementation_brief['title']}",
            "## Problem",
            "## Solution",
            "## Target User",
            "## Primary User Flow",
            "## Key Interactions",
            "## Validation Questions",
            "## Definition of Done",
            "## Additional Context",
        ],
        required_snippets=[
            f"Blueprint implementation brief {implementation_brief['id']}",
            f"**Execution Plan**: {execution_plan['id']}",
        ],
    )


def _validate_status_report(
    artifact_path: Path,
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any],
) -> list[ValidationFinding]:
    """Validate the status report Markdown export."""
    return _validate_markdown(
        artifact_path,
        execution_plan,
        implementation_brief,
        required_headings=[
            f"# Execution Plan Status Report: {execution_plan['id']}",
            "## Plan Metadata",
            "## Implementation Brief Summary",
            "## Task Counts By Status",
            "## Milestone Progress",
            "## Blocked Tasks",
            "## Ready Tasks",
        ],
        required_snippets=[
            f"- Plan ID: `{execution_plan['id']}`",
            f"- Title: {implementation_brief['title']}",
        ],
    )


class _HtmlReportParser(HTMLParser):
    """Extract structural markers from the HTML report."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.sections: set[str] = set()
        self.task_ids: list[str] = []
        self._table_depth = 0
        self._in_task_table = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs_dict = dict(attrs)
        section = attrs_dict.get("data-section")
        if section:
            self.sections.add(section)

        if tag == "table" and (
            attrs_dict.get("id") == "task-table"
            or attrs_dict.get("data-task-table") == "true"
        ):
            self._in_task_table = True
            self._table_depth = 1
            return

        if self._in_task_table:
            if tag == "table":
                self._table_depth += 1
            if tag == "tr" and attrs_dict.get("data-task-id") is not None:
                self.task_ids.append(attrs_dict["data-task-id"] or "")

    def handle_endtag(self, tag: str) -> None:
        if self._in_task_table and tag == "table":
            self._table_depth -= 1
            if self._table_depth <= 0:
                self._in_task_table = False


def _validate_html_report(
    artifact_path: Path,
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any],
) -> list[ValidationFinding]:
    """Validate the stakeholder HTML report structural contract."""
    content = artifact_path.read_text()
    parser = _HtmlReportParser()
    parser.feed(content)

    findings: list[ValidationFinding] = []
    required_sections = {
        "summary",
        "brief-summary",
        "status-counts",
        "milestones",
        "tasks",
        "dependencies",
        "risks",
        "validation-plan",
    }
    for section in sorted(required_sections - parser.sections):
        findings.append(
            ValidationFinding(
                code="html_report.missing_section",
                message=f"HTML report is missing section marker '{section}'.",
                path=str(artifact_path),
            )
        )

    if not content.strip():
        findings.append(
            ValidationFinding(
                code="html_report.empty",
                message="HTML report is empty.",
                path=str(artifact_path),
            )
        )

    expected_counts = Counter(task["id"] for task in execution_plan.get("tasks", []))
    rendered_counts = Counter(parser.task_ids)
    expected_total = sum(expected_counts.values())
    rendered_total = sum(rendered_counts.values())
    if rendered_total != expected_total:
        findings.append(
            ValidationFinding(
                code="html_report.task_count_mismatch",
                message=(
                    f"HTML report task table contains {rendered_total} task rows, "
                    f"expected {expected_total}."
                ),
                path=str(artifact_path),
            )
        )

    for task_id in sorted(expected_counts):
        if rendered_counts[task_id] != 1:
            findings.append(
                ValidationFinding(
                    code="html_report.task_occurrence_mismatch",
                    message=(
                        f"Task '{task_id}' appears {rendered_counts[task_id]} times "
                        "in the HTML report task table; expected exactly once."
                    ),
                    path=str(artifact_path),
                )
            )

    for task_id in sorted(set(rendered_counts) - set(expected_counts)):
        findings.append(
            ValidationFinding(
                code="html_report.unexpected_task",
                message=f"HTML report task table includes unexpected task '{task_id}'.",
                path=str(artifact_path),
            )
        )

    return findings


def _validate_slack_digest(
    artifact_path: Path,
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any],
) -> list[ValidationFinding]:
    """Validate the Slack digest Markdown export."""
    content = artifact_path.read_text()
    findings = _validate_markdown(
        artifact_path,
        execution_plan,
        implementation_brief,
        required_headings=[
            f"# Slack Digest: {execution_plan['id']}",
            "## Status Counts",
            "## Ready Tasks",
            "## Blocked Tasks",
            "## Next Recommended Tasks",
        ],
        required_snippets=[
            f"*Plan:* `{execution_plan['id']}`",
            (
                f"*Implementation Brief:* <blueprint://implementation-brief/"
                f"{implementation_brief['id']}|{implementation_brief['title']}>"
            ),
        ],
    )

    for status in ["pending", "in_progress", "completed", "blocked", "skipped"]:
        if f"*{status}:*" not in content:
            findings.append(
                ValidationFinding(
                    code="slack_digest.missing_status_count",
                    message=f"Slack digest is missing status count for {status}.",
                    path=str(artifact_path),
                )
            )

    for task in execution_plan.get("tasks", []):
        if task.get("status") != "blocked":
            continue
        if f"`{task['id']}`" not in content:
            findings.append(
                ValidationFinding(
                    code="slack_digest.missing_blocked_task",
                    message=f"Slack digest is missing blocked task {task['id']}.",
                    path=str(artifact_path),
                )
            )
        blocked_reason = _blocked_reason(task)
        if blocked_reason and blocked_reason not in content:
            findings.append(
                ValidationFinding(
                    code="slack_digest.missing_blocked_reason",
                    message=f"Slack digest is missing blocked reason for {task['id']}.",
                    path=str(artifact_path),
                )
            )

    return findings


def _validate_milestone_summary(
    artifact_path: Path,
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any],
) -> list[ValidationFinding]:
    """Validate the milestone summary Markdown export."""
    content = artifact_path.read_text()
    findings = _validate_markdown(
        artifact_path,
        execution_plan,
        implementation_brief,
        required_headings=[
            f"# Milestone Summary: {implementation_brief['title']}",
            "## Plan Overview",
            "## Cross-Milestone Dependencies",
            "## Milestones",
        ],
        required_snippets=[
            f"- Plan ID: `{execution_plan['id']}`",
            f"- Implementation Brief: `{implementation_brief['id']}`",
        ],
    )

    for task in execution_plan.get("tasks", []):
        if f"`{task['id']}`" not in content:
            findings.append(
                ValidationFinding(
                    code="milestone_summary.missing_task",
                    message=f"Milestone summary is missing task {task['id']}.",
                    path=str(artifact_path),
                )
            )

    for dependency_line in _expected_cross_milestone_dependency_lines(execution_plan):
        if dependency_line not in content:
            findings.append(
                ValidationFinding(
                    code="milestone_summary.missing_cross_dependency",
                    message=(
                        "Milestone summary is missing cross-milestone dependency: "
                        f"{dependency_line}"
                    ),
                    path=str(artifact_path),
                )
            )

    return findings


def _validate_critical_path_report(
    artifact_path: Path,
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any],
) -> list[ValidationFinding]:
    """Validate the critical path report Markdown export."""
    content = artifact_path.read_text()
    findings = _validate_markdown(
        artifact_path,
        execution_plan,
        implementation_brief,
        required_headings=[
            f"# Critical Path Report: {implementation_brief['title']}",
            "## Plan Overview",
            "## Critical Path",
            "## Blocked Or Incomplete Critical Path Tasks",
            "## Parallelizable Off-Path Tasks",
            "## Per-Task Dependency Details",
        ],
        required_snippets=[
            f"- Plan ID: `{execution_plan['id']}`",
            f"- Implementation Brief: `{implementation_brief['id']}`",
        ],
    )

    result = analyze_critical_path(execution_plan)
    if f"- Critical Path Weight: {result.total_weight}" not in content:
        findings.append(
            ValidationFinding(
                code="critical_path_report.missing_path_weight",
                message="Critical path report is missing the computed path weight.",
                path=str(artifact_path),
            )
        )

    for task_id in result.task_ids:
        if f"`{task_id}`" not in content:
            findings.append(
                ValidationFinding(
                    code="critical_path_report.missing_critical_task",
                    message=f"Critical path report is missing critical task {task_id}.",
                    path=str(artifact_path),
                )
            )

    return findings


def _validate_plan_snapshot(
    artifact_path: Path,
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any],
) -> list[ValidationFinding]:
    """Validate the immutable plan snapshot JSON export."""
    try:
        payload = json.loads(artifact_path.read_text())
    except json.JSONDecodeError as exc:
        return [
            ValidationFinding(
                code="plan_snapshot.invalid_json",
                message=f"Plan snapshot is not valid JSON: {exc.msg}",
                path=str(artifact_path),
            )
        ]

    if not isinstance(payload, dict):
        return [
            ValidationFinding(
                code="plan_snapshot.invalid_shape",
                message="Plan snapshot must be a JSON object.",
                path=str(artifact_path),
            )
        ]

    findings: list[ValidationFinding] = []
    required_keys = [
        "schema_version",
        "exported_at",
        "content_hash",
        "hash_algorithm",
        "plan",
        "brief",
        "milestones",
        "tasks",
        "dependencies",
        "metrics",
    ]
    findings.extend(
        _missing_keys(payload, required_keys, "plan_snapshot.missing_key", str(artifact_path))
    )

    if payload.get("schema_version") != PLAN_SNAPSHOT_SCHEMA_VERSION:
        findings.append(
            ValidationFinding(
                code="plan_snapshot.schema_version_mismatch",
                message="Plan snapshot schema_version is not supported.",
                path=str(artifact_path),
            )
        )

    content_hash = payload.get("content_hash")
    if not isinstance(content_hash, str) or not re.fullmatch(r"[0-9a-f]{64}", content_hash):
        findings.append(
            ValidationFinding(
                code="plan_snapshot.invalid_content_hash",
                message="Plan snapshot must include a SHA-256 content_hash.",
                path=str(artifact_path),
            )
        )

    for section in ["plan", "brief", "metrics"]:
        if section in payload and not isinstance(payload[section], dict):
            findings.append(
                ValidationFinding(
                    code="plan_snapshot.section.invalid_shape",
                    message=f"Plan snapshot section '{section}' must be an object.",
                    path=str(artifact_path),
                )
            )

    for section in ["milestones", "tasks", "dependencies"]:
        if section in payload and not isinstance(payload[section], list):
            findings.append(
                ValidationFinding(
                    code="plan_snapshot.section.invalid_shape",
                    message=f"Plan snapshot section '{section}' must be a list.",
                    path=str(artifact_path),
                )
            )

    plan_summary = payload.get("plan")
    if isinstance(plan_summary, dict):
        findings.extend(
            _missing_keys(
                plan_summary,
                ["id", "implementation_brief_id", "status"],
                "plan_snapshot.plan.missing_key",
                str(artifact_path),
            )
        )
        if plan_summary.get("id") != execution_plan.get("id"):
            findings.append(
                ValidationFinding(
                    code="plan_snapshot.plan_id_mismatch",
                    message="Plan snapshot plan.id does not match the execution plan.",
                    path=str(artifact_path),
                )
            )

    brief_summary = payload.get("brief")
    if isinstance(brief_summary, dict):
        findings.extend(
            _missing_keys(
                brief_summary,
                ["id", "source_brief_id", "title", "problem_statement", "mvp_goal"],
                "plan_snapshot.brief.missing_key",
                str(artifact_path),
            )
        )
        if brief_summary.get("id") != implementation_brief.get("id"):
            findings.append(
                ValidationFinding(
                    code="plan_snapshot.brief_id_mismatch",
                    message="Plan snapshot brief.id does not match the implementation brief.",
                    path=str(artifact_path),
                )
            )

    tasks = payload.get("tasks")
    if isinstance(tasks, list):
        expected_task_ids = {task["id"] for task in execution_plan.get("tasks", [])}
        rendered_task_ids = {
            task.get("id") for task in tasks if isinstance(task, dict) and task.get("id")
        }
        for missing_task_id in sorted(expected_task_ids - rendered_task_ids):
            findings.append(
                ValidationFinding(
                    code="plan_snapshot.missing_task",
                    message=f"Plan snapshot is missing task '{missing_task_id}'.",
                    path=str(artifact_path),
                )
            )

    return findings


def _validate_release_notes(
    artifact_path: Path,
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any],
) -> list[ValidationFinding]:
    """Validate the release notes Markdown export."""
    return _validate_markdown(
        artifact_path,
        execution_plan,
        implementation_brief,
        required_headings=[
            f"# Release Notes: {implementation_brief['title']}",
            "## Summary",
            "## Milestones",
            "## Completed Tasks",
            "## Pending Tasks",
            "## Validation Notes",
            "## Known Risks",
        ],
        required_snippets=[
            f"- Plan ID: `{execution_plan['id']}`",
            f"- Implementation Brief: `{implementation_brief['id']}`",
        ],
    )


def _validate_risk_register(
    artifact_path: Path,
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any],
) -> list[ValidationFinding]:
    """Validate the risk register Markdown export."""
    content = artifact_path.read_text()
    findings = _validate_markdown(
        artifact_path,
        execution_plan,
        implementation_brief,
        required_headings=[
            f"# Risk Register: {implementation_brief['title']}",
            "## Plan Metadata",
            "## Register",
        ],
        required_snippets=[
            f"- Plan ID: `{execution_plan['id']}`",
            f"- Implementation Brief: `{implementation_brief['id']}`",
        ],
    )

    rows = _risk_register_rendered_rows(content)
    expected_risks = _string_items(implementation_brief.get("risks"))
    valid_task_ids = {str(task["id"]) for task in execution_plan.get("tasks", [])}

    if not expected_risks:
        return findings

    for index, risk in enumerate(expected_risks, 1):
        risk_id = risk_identifier(index)
        matching_rows = [
            row for row in rows if _unwrapped_code_cell(row.get("risk_id", "")) == risk_id
        ]
        if len(matching_rows) != 1:
            findings.append(
                ValidationFinding(
                    code="risk_register.risk_occurrence_mismatch",
                    message=f"Risk register renders {risk_id} {len(matching_rows)} times.",
                    path=str(artifact_path),
                )
            )
            continue

        row = matching_rows[0]
        if row.get("source_risk") != _risk_register_table_cell(risk):
            findings.append(
                ValidationFinding(
                    code="risk_register.source_risk_mismatch",
                    message=f"Risk register row {risk_id} does not match the brief risk text.",
                    path=str(artifact_path),
                )
            )

        referenced_task_ids = _risk_register_referenced_task_ids(row.get("affected", ""))
        invalid_task_ids = sorted(set(referenced_task_ids) - valid_task_ids)
        if invalid_task_ids:
            findings.append(
                ValidationFinding(
                    code="risk_register.invalid_task_reference",
                    message=(
                        f"Risk register row {risk_id} references unknown tasks: "
                        f"{', '.join(invalid_task_ids)}"
                    ),
                    path=str(artifact_path),
                )
            )

        valid_references = [
            task_id for task_id in referenced_task_ids if task_id in valid_task_ids
        ]
        if not valid_references:
            findings.append(
                ValidationFinding(
                    code="risk_register.missing_risk_coverage",
                    message=f"Risk register row {risk_id} has no valid linked task ids.",
                    path=str(artifact_path),
                )
            )

        if row.get("status") not in {"tracked", "mitigated", "blocked", "uncovered", "accepted"}:
            findings.append(
                ValidationFinding(
                    code="risk_register.invalid_status",
                    message=(
                        f"Risk register row {risk_id} has invalid status "
                        f"{row.get('status')!r}."
                    ),
                    path=str(artifact_path),
                )
            )

    expected_risk_ids = {risk_identifier(index) for index in range(1, len(expected_risks) + 1)}
    extra_risk_ids = sorted(
        {
            _unwrapped_code_cell(row.get("risk_id", ""))
            for row in rows
            if _unwrapped_code_cell(row.get("risk_id", "")) not in expected_risk_ids
        }
    )
    if extra_risk_ids:
        findings.append(
            ValidationFinding(
                code="risk_register.unexpected_risk",
                message=f"Risk register contains unexpected risks: {', '.join(extra_risk_ids)}",
                path=str(artifact_path),
            )
        )

    return findings


def _validate_file_impact_map(
    artifact_path: Path,
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any],
) -> list[ValidationFinding]:
    """Validate the file impact map Markdown export."""
    content = artifact_path.read_text()
    findings = _validate_markdown(
        artifact_path,
        execution_plan,
        implementation_brief,
        required_headings=[
            f"# File Impact Map: {execution_plan['id']}",
            "## Files and Modules",
            "## Unassigned",
        ],
        required_snippets=[
            f"- Plan ID: `{execution_plan['id']}`",
            f"- Implementation Brief: `{implementation_brief['id']}`",
        ],
    )

    sections = _file_impact_rendered_sections(content)

    for task in execution_plan.get("tasks", []):
        files_or_modules = task.get("files_or_modules") or []
        expected_sections = files_or_modules or [UNASSIGNED_SECTION]
        for section_name in expected_sections:
            rendered_task_ids = sections.get(section_name, [])
            if task["id"] not in rendered_task_ids:
                findings.append(
                    ValidationFinding(
                        code="file_impact_map.missing_task",
                        message=(
                            f"File impact map is missing task {task['id']} "
                            f"from section {section_name}."
                        ),
                        path=str(artifact_path),
                    )
                )

    return findings


def _validate_raci_matrix(
    artifact_path: Path,
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any],
) -> list[ValidationFinding]:
    """Validate the RACI matrix Markdown export."""
    content = artifact_path.read_text()
    findings = _validate_markdown(
        artifact_path,
        execution_plan,
        implementation_brief,
        required_headings=[
            f"# RACI Matrix: {implementation_brief['title']}",
            "## Plan Metadata",
            "## Responsibility Matrix",
        ],
        required_snippets=[
            f"- Plan ID: `{execution_plan['id']}`",
            f"- Implementation Brief: `{implementation_brief['id']}`",
        ],
    )

    rows = _raci_rendered_rows(content)
    required_columns = [
        "task_id",
        "task",
        "responsible",
        "accountable",
        "consulted",
        "informed",
        "milestone",
        "suggested_engine",
    ]
    for index, row in enumerate(rows, 1):
        missing_columns = [column for column in required_columns if column not in row]
        if missing_columns:
            findings.append(
                ValidationFinding(
                    code="raci_matrix.row_missing_columns",
                    message=(
                        f"RACI matrix row {index} is missing columns: "
                        f"{', '.join(missing_columns)}"
                    ),
                    path=str(artifact_path),
                )
            )

    rendered_task_counts: dict[str, int] = {}
    for row in rows:
        task_id = _unwrapped_code_cell(row.get("task_id", ""))
        if task_id:
            rendered_task_counts[task_id] = rendered_task_counts.get(task_id, 0) + 1

    for task in execution_plan.get("tasks", []):
        task_id = str(task["id"])
        count = rendered_task_counts.get(task_id, 0)
        if count != 1:
            findings.append(
                ValidationFinding(
                    code="raci_matrix.task_row_occurrence_mismatch",
                    message=f"RACI matrix renders task {task_id} {count} times.",
                    path=str(artifact_path),
                )
            )

    return findings


def _validate_kanban(
    artifact_path: Path,
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any],
) -> list[ValidationFinding]:
    """Validate the Kanban board Markdown export."""
    content = artifact_path.read_text()
    findings = _validate_markdown(
        artifact_path,
        execution_plan,
        implementation_brief,
        required_headings=[
            f"# Execution Plan Kanban Board: {execution_plan['id']}",
            "## pending",
            "## in_progress",
            "## blocked",
            "## completed",
            "## skipped",
        ],
        required_snippets=[
            f"- Plan ID: `{execution_plan['id']}`",
            f"- Implementation Brief: `{implementation_brief['id']}`",
        ],
    )

    missing_column_findings = [
        finding
        for finding in findings
        if finding.code == "markdown.missing_heading"
        and finding.message.startswith("Missing required Markdown heading: ## ")
    ]
    for finding in missing_column_findings:
        findings.append(
            ValidationFinding(
                code="kanban.missing_column",
                message=finding.message.replace("Markdown heading", "Kanban column"),
                path=finding.path,
            )
        )

    rendered_tasks = _kanban_rendered_tasks_by_column(content)
    expected_tasks = execution_plan.get("tasks", [])
    rendered_task_ids = [task_id for task_ids in rendered_tasks.values() for task_id in task_ids]

    if len(rendered_task_ids) != len(expected_tasks):
        findings.append(
            ValidationFinding(
                code="kanban.task_count_mismatch",
                message="Kanban task count does not match the number of execution tasks.",
                path=str(artifact_path),
            )
        )

    for task in expected_tasks:
        expected_column = task.get("status") or "pending"
        occurrences = [
            column
            for column, task_ids in rendered_tasks.items()
            for task_id in task_ids
            if task_id == task["id"]
        ]
        if len(occurrences) != 1:
            findings.append(
                ValidationFinding(
                    code="kanban.task_occurrence_mismatch",
                    message=f"Task {task['id']} appears {len(occurrences)} times in the Kanban board.",
                    path=str(artifact_path),
                )
            )
            continue
        if occurrences[0] != expected_column:
            findings.append(
                ValidationFinding(
                    code="kanban.task_wrong_column",
                    message=(
                        f"Task {task['id']} appears under {occurrences[0]} "
                        f"instead of {expected_column}."
                    ),
                    path=str(artifact_path),
                )
            )

    extra_task_ids = sorted(set(rendered_task_ids) - {task["id"] for task in expected_tasks})
    if extra_task_ids:
        findings.append(
            ValidationFinding(
                code="kanban.unexpected_task",
                message=f"Kanban board contains unexpected tasks: {', '.join(extra_task_ids)}",
                path=str(artifact_path),
            )
        )

    return findings


def _validate_checklist(
    artifact_path: Path,
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any],
) -> list[ValidationFinding]:
    """Validate the execution checklist Markdown export."""
    content = artifact_path.read_text()
    findings = _validate_markdown(
        artifact_path,
        execution_plan,
        implementation_brief,
        required_headings=[
            f"# Execution Checklist: {implementation_brief['title']}",
            "## Plan Metadata",
            "## Milestones",
        ],
        required_snippets=[
            f"- Plan ID: `{execution_plan['id']}`",
            f"- Implementation Brief: `{implementation_brief['id']}`",
        ],
    )

    rendered_task_ids = _checklist_rendered_task_ids(content)
    expected_task_ids = [task["id"] for task in execution_plan.get("tasks", [])]

    if len(rendered_task_ids) != len(expected_task_ids):
        findings.append(
            ValidationFinding(
                code="checklist.task_count_mismatch",
                message="Checklist task count does not match the number of execution tasks.",
                path=str(artifact_path),
            )
        )

    for task_id in expected_task_ids:
        occurrences = rendered_task_ids.count(task_id)
        if occurrences != 1:
            findings.append(
                ValidationFinding(
                    code="checklist.task_occurrence_mismatch",
                    message=f"Task {task_id} appears {occurrences} times in the checklist.",
                    path=str(artifact_path),
                )
            )

    extra_task_ids = sorted(set(rendered_task_ids) - set(expected_task_ids))
    if extra_task_ids:
        findings.append(
            ValidationFinding(
                code="checklist.unexpected_task",
                message=f"Checklist contains unexpected tasks: {', '.join(extra_task_ids)}",
                path=str(artifact_path),
            )
        )

    for task in execution_plan.get("tasks", []):
        for dependency_id in task.get("depends_on") or []:
            if dependency_id not in content:
                findings.append(
                    ValidationFinding(
                        code="checklist.missing_dependency",
                        message=f"Checklist is missing dependency {dependency_id} for {task['id']}.",
                        path=str(artifact_path),
                    )
                )
        for affected_file in task.get("files_or_modules") or []:
            if affected_file not in content:
                findings.append(
                    ValidationFinding(
                        code="checklist.missing_affected_file",
                        message=f"Checklist is missing affected file {affected_file} for {task['id']}.",
                        path=str(artifact_path),
                    )
                )

    return findings


def _validate_coverage_matrix(
    artifact_path: Path,
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any],
) -> list[ValidationFinding]:
    """Validate the brief coverage matrix Markdown export."""
    content = artifact_path.read_text()
    findings = _validate_markdown(
        artifact_path,
        execution_plan,
        implementation_brief,
        required_headings=[
            f"# Coverage Matrix: {implementation_brief['title']}",
            "## Plan Metadata",
            "## Scope Coverage",
            "## Risk Coverage",
            "## Validation Coverage",
            "## Definition of Done Coverage",
        ],
        required_snippets=[
            f"- Plan ID: `{execution_plan['id']}`",
            f"- Implementation Brief: `{implementation_brief['id']}`",
        ],
    )

    expected_sections = {
        "Scope Coverage": _string_items(implementation_brief.get("scope")),
        "Risk Coverage": _string_items(implementation_brief.get("risks")),
        "Validation Coverage": [str(implementation_brief.get("validation_plan") or "").strip()],
        "Definition of Done Coverage": _string_items(
            implementation_brief.get("definition_of_done")
        ),
    }

    for section, expected_items in expected_sections.items():
        section_lines = _coverage_section_lines(content, section)
        for expected_item in expected_items:
            occurrences = _coverage_item_occurrences(section_lines, expected_item)
            if occurrences != 1:
                findings.append(
                    ValidationFinding(
                        code="coverage_matrix.item_occurrence_mismatch",
                        message=(
                            f"Coverage matrix section '{section}' renders item "
                            f"{expected_item!r} {occurrences} times."
                        ),
                        path=str(artifact_path),
                    )
                )

        rendered_rows = _coverage_rendered_rows(section_lines)
        for row in rendered_rows:
            if row["status"] not in {"covered", "partial", "uncovered"}:
                findings.append(
                    ValidationFinding(
                        code="coverage_matrix.invalid_status",
                        message=(
                            f"Coverage matrix item {row['item']!r} has invalid status "
                            f"{row['status']!r}."
                        ),
                        path=str(artifact_path),
                    )
                )
            if row["status"] == "uncovered" and row["matching_tasks"] != "none":
                findings.append(
                    ValidationFinding(
                        code="coverage_matrix.uncovered_has_matches",
                        message=(
                            f"Coverage matrix item {row['item']!r} is uncovered but lists "
                            "matching tasks."
                        ),
                        path=str(artifact_path),
                    )
                )

    return findings


def _validate_task_bundle(
    artifact_path: Path,
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any],
) -> list[ValidationFinding]:
    """Validate the task bundle directory export."""
    findings: list[ValidationFinding] = []
    if not artifact_path.is_dir():
        return [
            ValidationFinding(
                code="task_bundle.invalid_shape",
                message="Task bundle export must be a directory.",
                path=str(artifact_path),
            )
        ]

    readme_path = artifact_path / "README.md"
    if not readme_path.exists():
        findings.append(
            ValidationFinding(
                code="task_bundle.missing_readme",
                message="Task bundle export is missing README.md.",
                path=str(readme_path),
            )
        )
        return findings

    content = readme_path.read_text()
    for heading in [
        f"# Task Bundle: {execution_plan['id']}",
        "## Plan Summary",
        "## Brief Summary",
        "## Task Order",
    ]:
        if not _has_heading(content, heading):
            findings.append(
                ValidationFinding(
                    code="task_bundle.missing_heading",
                    message=f"Missing required Markdown heading: {heading}",
                    path=str(readme_path),
                )
            )

    for index, task in enumerate(execution_plan.get("tasks", []), 1):
        filename = _task_bundle_filename(index, task["id"])
        task_path = artifact_path / filename
        if not task_path.exists():
            findings.append(
                ValidationFinding(
                    code="task_bundle.missing_task_file",
                    message=f"Missing task file for {task['id']}: {filename}",
                    path=str(task_path),
                )
            )
            continue

        task_content = task_path.read_text()
        for heading in [
            f"# {task['title']}",
            "## Task Metadata",
            "## Description",
            "## Acceptance Criteria",
            "## Validation Context",
        ]:
            if not _has_heading(task_content, heading):
                findings.append(
                    ValidationFinding(
                        code="task_bundle.missing_heading",
                        message=f"Missing required Markdown heading: {heading}",
                        path=str(task_path),
                    )
                )
        for snippet in [
            f"- Task ID: `{task['id']}`",
            f"- Plan ID: `{execution_plan['id']}`",
        ]:
            if snippet not in task_content:
                findings.append(
                    ValidationFinding(
                        code="task_bundle.missing_identifier",
                        message=f"Missing required identifier text: {snippet}",
                        path=str(task_path),
                    )
                )

    if not any(path.suffix == ".md" for path in artifact_path.iterdir()):
        findings.append(
            ValidationFinding(
                code="task_bundle.empty",
                message="Task bundle export contains no Markdown files.",
                path=str(artifact_path),
            )
        )

    return findings


def _validate_agent_prompt_pack(
    artifact_path: Path,
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any],
) -> list[ValidationFinding]:
    """Validate the agent prompt pack directory export."""
    findings: list[ValidationFinding] = []
    if not artifact_path.is_dir():
        return [
            ValidationFinding(
                code="agent_prompt_pack.invalid_shape",
                message="Agent prompt pack export must be a directory.",
                path=str(artifact_path),
            )
        ]

    manifest_path = artifact_path / "manifest.json"
    if not manifest_path.exists():
        return [
            ValidationFinding(
                code="agent_prompt_pack.missing_manifest",
                message="Agent prompt pack export is missing manifest.json.",
                path=str(manifest_path),
            )
        ]

    try:
        manifest = json.loads(manifest_path.read_text())
    except json.JSONDecodeError as exc:
        return [
            ValidationFinding(
                code="agent_prompt_pack.invalid_manifest_json",
                message=f"Agent prompt pack manifest is not valid JSON: {exc.msg}",
                path=str(manifest_path),
            )
        ]

    for key, expected in [
        ("plan_id", execution_plan["id"]),
        ("implementation_brief_id", implementation_brief["id"]),
        ("prompt_format", "markdown"),
        ("manifest_format", "json"),
    ]:
        if manifest.get(key) != expected:
            findings.append(
                ValidationFinding(
                    code="agent_prompt_pack.invalid_manifest_field",
                    message=f"Manifest field {key!r} must be {expected!r}.",
                    path=str(manifest_path),
                )
            )

    manifest_tasks = manifest.get("tasks")
    if not isinstance(manifest_tasks, dict):
        findings.append(
            ValidationFinding(
                code="agent_prompt_pack.invalid_manifest_tasks",
                message="Manifest tasks must be an object keyed by task ID.",
                path=str(manifest_path),
            )
        )
        manifest_tasks = {}

    for task in execution_plan.get("tasks", []):
        entry = manifest_tasks.get(task["id"])
        if not isinstance(entry, dict):
            findings.append(
                ValidationFinding(
                    code="agent_prompt_pack.missing_manifest_task",
                    message=f"Manifest is missing task entry for {task['id']}.",
                    path=str(manifest_path),
                )
            )
            continue

        expected_dependencies = task.get("depends_on") or []
        if entry.get("dependencies") != expected_dependencies:
            findings.append(
                ValidationFinding(
                    code="agent_prompt_pack.invalid_dependencies",
                    message=f"Manifest dependencies for {task['id']} do not match the plan.",
                    path=str(manifest_path),
                )
            )

        prompt_path_value = entry.get("prompt_path")
        if not isinstance(prompt_path_value, str) or not prompt_path_value:
            findings.append(
                ValidationFinding(
                    code="agent_prompt_pack.missing_prompt_path",
                    message=f"Manifest task {task['id']} is missing prompt_path.",
                    path=str(manifest_path),
                )
            )
            continue

        prompt_path = artifact_path / prompt_path_value
        if not prompt_path.exists():
            findings.append(
                ValidationFinding(
                    code="agent_prompt_pack.missing_prompt_file",
                    message=f"Missing prompt file for {task['id']}: {prompt_path_value}",
                    path=str(prompt_path),
                )
            )
            continue

        prompt_content = prompt_path.read_text()
        for heading in [
            f"# Agent Task: {task['title']}",
            "## Operating Instructions",
            "## Project Context",
            "## Task",
            "## Acceptance Criteria",
            "## Plan Validation",
        ]:
            if not _has_heading(prompt_content, heading):
                findings.append(
                    ValidationFinding(
                        code="agent_prompt_pack.missing_heading",
                        message=f"Missing required Markdown heading: {heading}",
                        path=str(prompt_path),
                    )
                )

        for snippet in [
            f"- Task ID: `{task['id']}`",
            f"- Plan ID: `{execution_plan['id']}`",
            f"- Brief ID: `{implementation_brief['id']}`",
            "- Work on an isolated branch for this task before making changes.",
        ]:
            if snippet not in prompt_content:
                findings.append(
                    ValidationFinding(
                        code="agent_prompt_pack.missing_prompt_context",
                        message=f"Missing required prompt context: {snippet}",
                        path=str(prompt_path),
                    )
                )

    return findings


def _validate_adr(
    artifact_path: Path,
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any],
) -> list[ValidationFinding]:
    """Validate the ADR directory export."""
    findings: list[ValidationFinding] = []
    if not artifact_path.is_dir():
        return [
            ValidationFinding(
                code="adr.invalid_shape",
                message="ADR export must be a directory.",
                path=str(artifact_path),
            )
        ]

    readme_path = artifact_path / "README.md"
    if not readme_path.exists():
        findings.append(
            ValidationFinding(
                code="adr.missing_readme",
                message="ADR export is missing README.md.",
                path=str(readme_path),
            )
        )
        return findings

    readme_content = readme_path.read_text()
    for heading in [
        f"# Architecture Decision Records: {execution_plan['id']}",
        "## Source Blueprint",
        "## ADR Index",
    ]:
        if not _has_heading(readme_content, heading):
            findings.append(
                ValidationFinding(
                    code="adr.missing_heading",
                    message=f"Missing required Markdown heading: {heading}",
                    path=str(readme_path),
                )
            )

    for snippet in [
        f"- Plan ID: `{execution_plan['id']}`",
        f"- Implementation Brief ID: `{implementation_brief['id']}`",
    ]:
        if snippet not in readme_content:
            findings.append(
                ValidationFinding(
                    code="adr.missing_identifier",
                    message=f"Missing required identifier text: {snippet}",
                    path=str(readme_path),
                )
            )

    adr_paths = sorted(path for path in artifact_path.glob("*.md") if path.name != "README.md")
    if not adr_paths:
        findings.append(
            ValidationFinding(
                code="adr.empty",
                message="ADR export contains no ADR Markdown files.",
                path=str(artifact_path),
            )
        )
        return findings

    expected_pattern = re.compile(r"^\d{3}-[A-Za-z0-9._-]+-[A-Za-z0-9._-]+\.md$")
    for adr_path in adr_paths:
        if not expected_pattern.match(adr_path.name):
            findings.append(
                ValidationFinding(
                    code="adr.invalid_filename",
                    message=f"ADR filename is not deterministic: {adr_path.name}",
                    path=str(adr_path),
                )
            )

        adr_content = adr_path.read_text()
        for heading in [
            "# ADR-",
            "## Status",
            "## Context",
            "## Decision",
            "## Consequences",
            "## Related Tasks",
            "## Source Blueprint IDs",
        ]:
            if not _has_heading(adr_content, heading):
                findings.append(
                    ValidationFinding(
                        code="adr.missing_heading",
                        message=f"Missing required Markdown heading: {heading}",
                        path=str(adr_path),
                    )
                )

        for snippet in [
            f"- Plan ID: `{execution_plan['id']}`",
            f"- Implementation Brief ID: `{implementation_brief['id']}`",
            f"- Source Brief ID: `{implementation_brief['source_brief_id']}`",
        ]:
            if snippet not in adr_content:
                findings.append(
                    ValidationFinding(
                        code="adr.missing_identifier",
                        message=f"Missing required identifier text: {snippet}",
                        path=str(adr_path),
                    )
                )

    return findings


def _validate_github_issues_bundle(
    artifact_path: Path,
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any],
) -> list[ValidationFinding]:
    """Validate the GitHub issues directory export."""
    findings: list[ValidationFinding] = []
    if not artifact_path.is_dir():
        return [
            ValidationFinding(
                code="github_issues.invalid_shape",
                message="GitHub issue export must be a directory.",
                path=str(artifact_path),
            )
        ]

    manifest_path = artifact_path / "manifest.json"
    if not manifest_path.exists():
        findings.append(
            ValidationFinding(
                code="github_issues.missing_manifest",
                message="GitHub issue export is missing manifest.json.",
                path=str(manifest_path),
            )
        )
        return findings

    try:
        manifest = json.loads(manifest_path.read_text())
    except json.JSONDecodeError as exc:
        return [
            ValidationFinding(
                code="github_issues.invalid_manifest",
                message=f"GitHub issue manifest is not valid JSON: {exc.msg}",
                path=str(manifest_path),
            )
        ]

    if not isinstance(manifest, dict):
        return [
            ValidationFinding(
                code="github_issues.invalid_manifest_shape",
                message="GitHub issue manifest must be a JSON object.",
                path=str(manifest_path),
            )
        ]

    required_keys = ["schema_version", "repository", "plan", "issues", "milestone_groups"]
    findings.extend(
        _missing_keys(manifest, required_keys, "github_issues.missing_key", str(manifest_path))
    )

    repository = manifest.get("repository")
    if isinstance(repository, dict):
        findings.extend(
            _missing_keys(
                repository,
                ["raw_target_repo", "full_name", "issues_url"],
                "github_issues.repository.missing_key",
                str(manifest_path),
            )
        )
    else:
        findings.append(
            ValidationFinding(
                code="github_issues.repository.invalid_shape",
                message="GitHub issue repository metadata must be an object.",
                path=str(manifest_path),
            )
        )

    issues = manifest.get("issues")
    if not isinstance(issues, list):
        findings.append(
            ValidationFinding(
                code="github_issues.issues.invalid_shape",
                message="GitHub issue manifest issues must be a list.",
                path=str(manifest_path),
            )
        )
        return findings

    for index, task in enumerate(execution_plan.get("tasks", []), 1):
        filename = _github_issue_filename(index, task["id"])
        task_path = artifact_path / filename
        if not task_path.exists():
            findings.append(
                ValidationFinding(
                    code="github_issues.missing_issue_file",
                    message=f"Missing issue draft for {task['id']}: {filename}",
                    path=str(task_path),
                )
            )
            continue

        task_content = task_path.read_text()
        labels = _task_labels(task)
        milestone_name = task.get("milestone") or "Ungrouped"
        for heading in [
            f"# {task['title']}",
            "## Task Metadata",
            "## Description",
            "## Acceptance Criteria",
            "## Dependencies",
            "## Labels",
            "## Validation Context",
            "## Milestone Group",
        ]:
            if not _has_heading(task_content, heading):
                findings.append(
                    ValidationFinding(
                        code="github_issues.missing_heading",
                        message=f"Missing required Markdown heading: {heading}",
                        path=str(task_path),
                    )
                )
        for snippet in [
            f"- Task ID: `{task['id']}`",
            f"- Plan ID: `{execution_plan['id']}`",
            f"- Repository: {execution_plan.get('target_repo') or 'N/A'}",
            f"- Milestone: {milestone_name}",
            f"- Labels: {', '.join(labels) if labels else 'None'}",
        ]:
            if snippet not in task_content:
                findings.append(
                    ValidationFinding(
                        code="github_issues.missing_identifier",
                        message=f"Missing required identifier text: {snippet}",
                        path=str(task_path),
                    )
                )

    if not any(path.suffix == ".md" for path in artifact_path.rglob("*") if path.is_file()):
        findings.append(
            ValidationFinding(
                code="github_issues.empty",
                message="GitHub issue export does not contain any issue drafts.",
                path=str(artifact_path),
            )
        )

    return findings


def _validate_csv_tasks(
    artifact_path: Path,
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any],
) -> list[ValidationFinding]:
    """Validate the CSV task export."""
    findings: list[ValidationFinding] = []
    try:
        with artifact_path.open(newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            fieldnames = list(reader.fieldnames or [])
    except csv.Error as exc:
        return [
            ValidationFinding(
                code="csv.invalid_structure",
                message=f"CSV export could not be parsed: {exc}",
                path=str(artifact_path),
            )
        ]

    expected_columns = CsvTasksExporter.FIELDNAMES
    if fieldnames != expected_columns:
        missing = [column for column in expected_columns if column not in fieldnames]
        extra = [column for column in fieldnames if column not in expected_columns]
        if missing:
            findings.append(
                ValidationFinding(
                    code="csv.missing_column",
                    message=f"CSV export is missing required columns: {', '.join(missing)}",
                    path=str(artifact_path),
                )
            )
        if extra:
            findings.append(
                ValidationFinding(
                    code="csv.unexpected_column",
                    message=f"CSV export has unexpected columns: {', '.join(extra)}",
                    path=str(artifact_path),
                )
            )

    if len(rows) != len(execution_plan.get("tasks", [])):
        findings.append(
            ValidationFinding(
                code="csv.row_count_mismatch",
                message=("CSV export row count does not match the number of execution tasks."),
                path=str(artifact_path),
            )
        )

    for index, row in enumerate(rows, 1):
        if row.get("plan_id") != execution_plan["id"]:
            findings.append(
                ValidationFinding(
                    code="csv.row_plan_id_mismatch",
                    message=f"CSV row {index} has the wrong plan_id value.",
                    path=str(artifact_path),
                )
            )
        if not row.get("task_id"):
            findings.append(
                ValidationFinding(
                    code="csv.row_missing_task_id",
                    message=f"CSV row {index} is missing task_id.",
                    path=str(artifact_path),
                )
            )

    return findings


def _validate_jira_csv(
    artifact_path: Path,
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any],
) -> list[ValidationFinding]:
    """Validate the Jira CSV issue export."""
    findings: list[ValidationFinding] = []
    try:
        with artifact_path.open(newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            fieldnames = list(reader.fieldnames or [])
    except csv.Error as exc:
        return [
            ValidationFinding(
                code="jira_csv.invalid_structure",
                message=f"Jira CSV export could not be parsed: {exc}",
                path=str(artifact_path),
            )
        ]

    expected_columns = JiraCsvExporter.FIELDNAMES
    if fieldnames != expected_columns:
        missing = [column for column in expected_columns if column not in fieldnames]
        extra = [column for column in fieldnames if column not in expected_columns]
        if missing:
            findings.append(
                ValidationFinding(
                    code="jira_csv.missing_column",
                    message=f"Jira CSV export is missing required columns: {', '.join(missing)}",
                    path=str(artifact_path),
                )
            )
        if extra:
            findings.append(
                ValidationFinding(
                    code="jira_csv.unexpected_column",
                    message=f"Jira CSV export has unexpected columns: {', '.join(extra)}",
                    path=str(artifact_path),
                )
            )

    milestones = execution_plan.get("milestones", [])
    tasks = execution_plan.get("tasks", [])
    expected_row_count = len(milestones) + len(tasks)
    if len(rows) != expected_row_count:
        findings.append(
            ValidationFinding(
                code="jira_csv.row_count_mismatch",
                message=(
                    "Jira CSV row count does not match one row per milestone "
                    "plus one row per task."
                ),
                path=str(artifact_path),
            )
        )

    epic_rows = [row for row in rows if row.get("Issue Type") == "Epic"]
    if len(epic_rows) != len(milestones):
        findings.append(
            ValidationFinding(
                code="jira_csv.epic_count_mismatch",
                message="Jira CSV epic row count does not match the number of milestones.",
                path=str(artifact_path),
            )
        )

    child_rows = [row for row in rows if row.get("Issue Type") != "Epic"]
    if len(child_rows) != len(tasks):
        findings.append(
            ValidationFinding(
                code="jira_csv.task_count_mismatch",
                message="Jira CSV child issue row count does not match the number of tasks.",
                path=str(artifact_path),
            )
        )

    for index, row in enumerate(rows, 1):
        for column in ["Summary", "Description", "Issue Type", "External ID"]:
            if not row.get(column):
                findings.append(
                    ValidationFinding(
                        code="jira_csv.row_missing_required_value",
                        message=f"Jira CSV row {index} is missing {column}.",
                        path=str(artifact_path),
                    )
                )

    return findings


def _validate_azure_devops_csv(
    artifact_path: Path,
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any],
) -> list[ValidationFinding]:
    """Validate the Azure DevOps CSV work item export."""
    findings: list[ValidationFinding] = []
    try:
        with artifact_path.open(newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            fieldnames = list(reader.fieldnames or [])
    except csv.Error as exc:
        return [
            ValidationFinding(
                code="azure_devops_csv.invalid_structure",
                message=f"Azure DevOps CSV export could not be parsed: {exc}",
                path=str(artifact_path),
            )
        ]

    missing = [column for column in AzureDevOpsCsvExporter.FIELDNAMES if column not in fieldnames]
    if missing:
        findings.append(
            ValidationFinding(
                code="azure_devops_csv.missing_column",
                message=(
                    "Azure DevOps CSV export is missing required columns: " f"{', '.join(missing)}"
                ),
                path=str(artifact_path),
            )
        )

    tasks = execution_plan.get("tasks", [])
    if len(rows) != len(tasks):
        findings.append(
            ValidationFinding(
                code="azure_devops_csv.row_count_mismatch",
                message="Azure DevOps CSV row count does not match one row per task.",
                path=str(artifact_path),
            )
        )

    for index, row in enumerate(rows, 1):
        for column in ["Work Item Type", "Title", "Description", "Tags"]:
            if column in fieldnames and not row.get(column):
                findings.append(
                    ValidationFinding(
                        code="azure_devops_csv.row_missing_required_value",
                        message=f"Azure DevOps CSV row {index} is missing {column}.",
                        path=str(artifact_path),
                    )
                )

    return findings


def _validate_asana_csv(
    artifact_path: Path,
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any],
) -> list[ValidationFinding]:
    """Validate the Asana CSV task export."""
    findings: list[ValidationFinding] = []
    try:
        with artifact_path.open(newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            fieldnames = list(reader.fieldnames or [])
    except csv.Error as exc:
        return [
            ValidationFinding(
                code="asana_csv.invalid_structure",
                message=f"Asana CSV export could not be parsed: {exc}",
                path=str(artifact_path),
            )
        ]

    missing = [column for column in AsanaCsvExporter.FIELDNAMES if column not in fieldnames]
    if missing:
        findings.append(
            ValidationFinding(
                code="asana_csv.missing_column",
                message=f"Asana CSV export is missing required columns: {', '.join(missing)}",
                path=str(artifact_path),
            )
        )

    tasks = execution_plan.get("tasks", [])
    if len(rows) != len(tasks):
        findings.append(
            ValidationFinding(
                code="asana_csv.row_count_mismatch",
                message="Asana CSV row count does not match one row per task.",
                path=str(artifact_path),
            )
        )

    for index, row in enumerate(rows, 1):
        for column in ["Name", "Notes"]:
            if column in fieldnames and not row.get(column):
                findings.append(
                    ValidationFinding(
                        code="asana_csv.row_missing_required_value",
                        message=f"Asana CSV row {index} is missing {column}.",
                        path=str(artifact_path),
                    )
                )

    return findings


def _validate_linear(
    artifact_path: Path,
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any],
) -> list[ValidationFinding]:
    """Validate the Linear issue JSON export."""
    try:
        payload = json.loads(artifact_path.read_text())
    except json.JSONDecodeError as exc:
        return [
            ValidationFinding(
                code="linear.invalid_json",
                message=f"Linear export is not valid JSON: {exc.msg}",
                path=str(artifact_path),
            )
        ]

    if not isinstance(payload, dict):
        return [
            ValidationFinding(
                code="linear.invalid_shape",
                message="Linear export must be a JSON object.",
                path=str(artifact_path),
            )
        ]

    findings: list[ValidationFinding] = []
    findings.extend(
        _missing_keys(
            payload,
            ["schema_version", "exporter", "plan", "issues"],
            "linear.missing_key",
            str(artifact_path),
        )
    )

    issues = payload.get("issues")
    if not isinstance(issues, list):
        findings.append(
            ValidationFinding(
                code="linear.issues.invalid_shape",
                message="Linear export issues must be a list.",
                path=str(artifact_path),
            )
        )
        return findings

    expected_tasks = execution_plan.get("tasks", [])
    if len(issues) != len(expected_tasks):
        findings.append(
            ValidationFinding(
                code="linear.task_count_mismatch",
                message="Linear issue count does not match the number of execution tasks.",
                path=str(artifact_path),
            )
        )

    task_occurrences: dict[str, int] = {}
    for index, issue in enumerate(issues, 1):
        if not isinstance(issue, dict):
            findings.append(
                ValidationFinding(
                    code="linear.issue.invalid_shape",
                    message=f"Linear issue {index} must be an object.",
                    path=str(artifact_path),
                )
            )
            continue

        findings.extend(
            _missing_keys(
                issue,
                [
                    "externalId",
                    "title",
                    "description",
                    "teamKey",
                    "labels",
                    "priority",
                    "estimate",
                    "relations",
                    "metadata",
                ],
                "linear.issue.missing_key",
                str(artifact_path),
            )
        )

        metadata = issue.get("metadata")
        if isinstance(metadata, dict):
            task_id = metadata.get("taskId")
            if isinstance(task_id, str):
                task_occurrences[task_id] = task_occurrences.get(task_id, 0) + 1
        else:
            findings.append(
                ValidationFinding(
                    code="linear.issue.metadata.invalid_shape",
                    message=f"Linear issue {index} metadata must be an object.",
                    path=str(artifact_path),
                )
            )

        if not isinstance(issue.get("relations"), list):
            findings.append(
                ValidationFinding(
                    code="linear.issue.relations.invalid_shape",
                    message=f"Linear issue {index} relations must be a list.",
                    path=str(artifact_path),
                )
            )

    for task in expected_tasks:
        occurrences = task_occurrences.get(task["id"], 0)
        if occurrences != 1:
            findings.append(
                ValidationFinding(
                    code="linear.task_occurrence_mismatch",
                    message=f"Task {task['id']} appears {occurrences} times in the Linear export.",
                    path=str(artifact_path),
                )
            )

    extra_task_ids = sorted(set(task_occurrences) - {task["id"] for task in expected_tasks})
    if extra_task_ids:
        findings.append(
            ValidationFinding(
                code="linear.unexpected_task",
                message=f"Linear export contains unexpected tasks: {', '.join(extra_task_ids)}",
                path=str(artifact_path),
            )
        )

    return findings


def _validate_trello_json(
    artifact_path: Path,
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any],
) -> list[ValidationFinding]:
    """Validate the Trello board JSON export."""
    try:
        payload = json.loads(artifact_path.read_text())
    except json.JSONDecodeError as exc:
        return [
            ValidationFinding(
                code="trello_json.invalid_json",
                message=f"Trello JSON export is not valid JSON: {exc.msg}",
                path=str(artifact_path),
            )
        ]

    if not isinstance(payload, dict):
        return [
            ValidationFinding(
                code="trello_json.invalid_shape",
                message="Trello JSON export must be a JSON object.",
                path=str(artifact_path),
            )
        ]

    findings: list[ValidationFinding] = []
    findings.extend(
        _missing_keys(
            payload,
            ["schema_version", "exporter", "board", "lists", "labels", "cards"],
            "trello_json.missing_key",
            str(artifact_path),
        )
    )

    lists = payload.get("lists")
    cards = payload.get("cards")
    labels = payload.get("labels")
    if not isinstance(lists, list):
        findings.append(
            ValidationFinding(
                code="trello_json.lists.invalid_shape",
                message="Trello JSON lists must be a list.",
                path=str(artifact_path),
            )
        )
        lists = []
    if not isinstance(cards, list):
        findings.append(
            ValidationFinding(
                code="trello_json.cards.invalid_shape",
                message="Trello JSON cards must be a list.",
                path=str(artifact_path),
            )
        )
        cards = []
    if not isinstance(labels, list):
        findings.append(
            ValidationFinding(
                code="trello_json.labels.invalid_shape",
                message="Trello JSON labels must be a list.",
                path=str(artifact_path),
            )
        )

    expected_tasks = execution_plan.get("tasks", [])
    if len(cards) != len(expected_tasks):
        findings.append(
            ValidationFinding(
                code="trello_json.card_count_mismatch",
                message="Trello card count does not match the number of execution tasks.",
                path=str(artifact_path),
            )
        )

    list_names = {
        list_item.get("name")
        for list_item in lists
        if isinstance(list_item, dict) and isinstance(list_item.get("name"), str)
    }
    for list_name in _expected_trello_list_names(execution_plan):
        if list_name not in list_names:
            findings.append(
                ValidationFinding(
                    code="trello_json.missing_list",
                    message=f"Trello JSON export is missing list: {list_name}",
                    path=str(artifact_path),
                )
            )

    list_ids = {
        list_item.get("id")
        for list_item in lists
        if isinstance(list_item, dict) and isinstance(list_item.get("id"), str)
    }
    task_occurrences: dict[str, int] = {}
    expected_by_id = {task["id"]: task for task in expected_tasks}
    for index, card in enumerate(cards, 1):
        if not isinstance(card, dict):
            findings.append(
                ValidationFinding(
                    code="trello_json.card.invalid_shape",
                    message=f"Trello card {index} must be an object.",
                    path=str(artifact_path),
                )
            )
            continue

        findings.extend(
            _missing_keys(
                card,
                ["id", "name", "desc", "idList", "labels", "checklists", "metadata"],
                "trello_json.card.missing_key",
                str(artifact_path),
            )
        )
        if card.get("idList") not in list_ids:
            findings.append(
                ValidationFinding(
                    code="trello_json.card.missing_list_reference",
                    message=f"Trello card {index} references a missing list.",
                    path=str(artifact_path),
                )
            )

        metadata = card.get("metadata")
        task_id = None
        if isinstance(metadata, dict) and isinstance(metadata.get("taskId"), str):
            task_id = metadata["taskId"]
            task_occurrences[task_id] = task_occurrences.get(task_id, 0) + 1
        else:
            findings.append(
                ValidationFinding(
                    code="trello_json.card.metadata.invalid_shape",
                    message=f"Trello card {index} metadata must include a string taskId.",
                    path=str(artifact_path),
                )
            )

        checklists = card.get("checklists")
        if not isinstance(checklists, list):
            findings.append(
                ValidationFinding(
                    code="trello_json.card.checklists.invalid_shape",
                    message=f"Trello card {index} checklists must be a list.",
                    path=str(artifact_path),
                )
            )
            continue

        if task_id in expected_by_id:
            _validate_trello_acceptance_checklist(
                findings,
                artifact_path,
                index,
                task_id,
                expected_by_id[task_id],
                checklists,
            )

    for task in expected_tasks:
        occurrences = task_occurrences.get(task["id"], 0)
        if occurrences != 1:
            findings.append(
                ValidationFinding(
                    code="trello_json.task_occurrence_mismatch",
                    message=f"Task {task['id']} appears {occurrences} times in the Trello JSON export.",
                    path=str(artifact_path),
                )
            )

    extra_task_ids = sorted(set(task_occurrences) - {task["id"] for task in expected_tasks})
    if extra_task_ids:
        findings.append(
            ValidationFinding(
                code="trello_json.unexpected_task",
                message=f"Trello JSON export contains unexpected tasks: {', '.join(extra_task_ids)}",
                path=str(artifact_path),
            )
        )

    return findings


def _validate_gitlab_issues(
    artifact_path: Path,
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any],
) -> list[ValidationFinding]:
    """Validate the GitLab issue JSON export."""
    try:
        payload = json.loads(artifact_path.read_text())
    except json.JSONDecodeError as exc:
        return [
            ValidationFinding(
                code="gitlab_issues.invalid_json",
                message=f"GitLab issues export is not valid JSON: {exc.msg}",
                path=str(artifact_path),
            )
        ]

    if not isinstance(payload, list):
        return [
            ValidationFinding(
                code="gitlab_issues.invalid_shape",
                message="GitLab issues export must be a JSON array.",
                path=str(artifact_path),
            )
        ]

    findings: list[ValidationFinding] = []
    expected_tasks = execution_plan.get("tasks", [])
    if len(payload) != len(expected_tasks):
        findings.append(
            ValidationFinding(
                code="gitlab_issues.task_count_mismatch",
                message="GitLab issue count does not match the number of execution tasks.",
                path=str(artifact_path),
            )
        )

    task_occurrences: dict[str, int] = {}
    for index, issue in enumerate(payload, 1):
        if not isinstance(issue, dict):
            findings.append(
                ValidationFinding(
                    code="gitlab_issues.issue.invalid_shape",
                    message=f"GitLab issue {index} must be an object.",
                    path=str(artifact_path),
                )
            )
            continue

        findings.extend(
            _missing_keys(
                issue,
                ["title", "description", "labels", "milestone", "weight", "metadata"],
                "gitlab_issues.issue.missing_key",
                str(artifact_path),
            )
        )

        for field in ["title", "description", "milestone"]:
            if not issue.get(field):
                findings.append(
                    ValidationFinding(
                        code="gitlab_issues.issue.missing_required_value",
                        message=f"GitLab issue {index} is missing {field}.",
                        path=str(artifact_path),
                    )
                )

        if not isinstance(issue.get("labels"), list):
            findings.append(
                ValidationFinding(
                    code="gitlab_issues.issue.labels.invalid_shape",
                    message=f"GitLab issue {index} labels must be a list.",
                    path=str(artifact_path),
                )
            )

        metadata = issue.get("metadata")
        if isinstance(metadata, dict):
            task_id = metadata.get("task_id")
            if isinstance(task_id, str):
                task_occurrences[task_id] = task_occurrences.get(task_id, 0) + 1
            else:
                findings.append(
                    ValidationFinding(
                        code="gitlab_issues.issue.metadata.missing_task_id",
                        message=f"GitLab issue {index} metadata is missing task_id.",
                        path=str(artifact_path),
                    )
                )
        else:
            findings.append(
                ValidationFinding(
                    code="gitlab_issues.issue.metadata.invalid_shape",
                    message=f"GitLab issue {index} metadata must be an object.",
                    path=str(artifact_path),
                )
            )

    for task in expected_tasks:
        occurrences = task_occurrences.get(task["id"], 0)
        if occurrences != 1:
            findings.append(
                ValidationFinding(
                    code="gitlab_issues.task_occurrence_mismatch",
                    message=(
                        f"Task {task['id']} appears {occurrences} times "
                        "in the GitLab issues export."
                    ),
                    path=str(artifact_path),
                )
            )

    extra_task_ids = sorted(set(task_occurrences) - {task["id"] for task in expected_tasks})
    if extra_task_ids:
        findings.append(
            ValidationFinding(
                code="gitlab_issues.unexpected_task",
                message=(
                    "GitLab issues export contains unexpected tasks: "
                    f"{', '.join(extra_task_ids)}"
                ),
                path=str(artifact_path),
            )
        )

    return findings


def _validate_junit_tasks(
    artifact_path: Path,
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any],
) -> list[ValidationFinding]:
    """Validate the JUnit XML task export."""
    try:
        root = ElementTree.parse(artifact_path).getroot()
    except ElementTree.ParseError as exc:
        return [
            ValidationFinding(
                code="junit.invalid_xml",
                message=f"JUnit export could not be parsed: {exc}",
                path=str(artifact_path),
            )
        ]

    findings: list[ValidationFinding] = []
    if root.tag != "testsuites":
        findings.append(
            ValidationFinding(
                code="junit.invalid_root",
                message="JUnit export root element must be testsuites.",
                path=str(artifact_path),
            )
        )
        return findings

    expected_tests = str(len(execution_plan.get("tasks", [])))
    if root.attrib.get("tests") != expected_tests:
        findings.append(
            ValidationFinding(
                code="junit.tests_count_mismatch",
                message="JUnit export tests count does not match the execution plan.",
                path=str(artifact_path),
            )
        )

    testcase_elements = root.findall(".//testcase")
    if len(testcase_elements) != len(execution_plan.get("tasks", [])):
        findings.append(
            ValidationFinding(
                code="junit.testcase_count_mismatch",
                message="JUnit export testcase count does not match the execution plan.",
                path=str(artifact_path),
            )
        )

    for index, testcase in enumerate(testcase_elements, 1):
        properties = testcase.find("properties")
        if properties is None:
            findings.append(
                ValidationFinding(
                    code="junit.missing_properties",
                    message=f"JUnit testcase {index} is missing a properties block.",
                    path=str(artifact_path),
                )
            )
            continue

        task_id = _xml_property_value(properties, "task_id")
        status = _xml_property_value(properties, "status")
        if not task_id:
            findings.append(
                ValidationFinding(
                    code="junit.missing_task_id",
                    message=f"JUnit testcase {index} is missing the task_id property.",
                    path=str(artifact_path),
                )
            )
        if not status:
            findings.append(
                ValidationFinding(
                    code="junit.missing_status",
                    message=f"JUnit testcase {index} is missing the status property.",
                    path=str(artifact_path),
                )
            )

    return findings


def _validate_task_queue_jsonl(
    artifact_path: Path,
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any],
) -> list[ValidationFinding]:
    """Validate the JSON Lines task queue export."""
    findings: list[ValidationFinding] = []
    rows: list[dict[str, Any]] = []

    for index, line in enumerate(artifact_path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            findings.append(
                ValidationFinding(
                    code="task_queue_jsonl.blank_line",
                    message=f"JSONL line {index} is blank.",
                    path=str(artifact_path),
                )
            )
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as exc:
            findings.append(
                ValidationFinding(
                    code="task_queue_jsonl.invalid_json",
                    message=f"JSONL line {index} is not valid JSON: {exc.msg}",
                    path=str(artifact_path),
                )
            )
            continue
        if not isinstance(payload, dict):
            findings.append(
                ValidationFinding(
                    code="task_queue_jsonl.invalid_line_shape",
                    message=f"JSONL line {index} must be a JSON object.",
                    path=str(artifact_path),
                )
            )
            continue
        rows.append(payload)

    expected_task_ids = [task["id"] for task in execution_plan.get("tasks", [])]
    if len(rows) != len(expected_task_ids):
        findings.append(
            ValidationFinding(
                code="task_queue_jsonl.line_count_mismatch",
                message="JSONL line count does not match the number of execution tasks.",
                path=str(artifact_path),
            )
        )

    seen_task_ids: dict[str, int] = {}
    for index, row in enumerate(rows, 1):
        if row.get("plan_id") != execution_plan["id"]:
            findings.append(
                ValidationFinding(
                    code="task_queue_jsonl.plan_id_mismatch",
                    message=f"JSONL line {index} has the wrong plan_id value.",
                    path=str(artifact_path),
                )
            )
        task_id = row.get("task_id")
        if not task_id:
            findings.append(
                ValidationFinding(
                    code="task_queue_jsonl.missing_task_id",
                    message=f"JSONL line {index} is missing task_id.",
                    path=str(artifact_path),
                )
            )
            continue
        seen_task_ids[task_id] = seen_task_ids.get(task_id, 0) + 1

    duplicate_task_ids = sorted(task_id for task_id, count in seen_task_ids.items() if count > 1)
    if duplicate_task_ids:
        findings.append(
            ValidationFinding(
                code="task_queue_jsonl.duplicate_task_id",
                message=f"JSONL export repeats task ids: {', '.join(duplicate_task_ids)}",
                path=str(artifact_path),
            )
        )

    expected_task_id_set = set(expected_task_ids)
    missing_task_ids = sorted(expected_task_id_set - set(seen_task_ids))
    if missing_task_ids:
        findings.append(
            ValidationFinding(
                code="task_queue_jsonl.missing_task",
                message=f"JSONL export is missing task ids: {', '.join(missing_task_ids)}",
                path=str(artifact_path),
            )
        )

    unexpected_task_ids = sorted(set(seen_task_ids) - expected_task_id_set)
    if unexpected_task_ids:
        findings.append(
            ValidationFinding(
                code="task_queue_jsonl.unexpected_task",
                message=f"JSONL export has unexpected task ids: {', '.join(unexpected_task_ids)}",
                path=str(artifact_path),
            )
        )

    return findings


def _validate_vscode_tasks(
    artifact_path: Path,
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any],
) -> list[ValidationFinding]:
    """Validate the VS Code tasks.json export."""
    try:
        payload = json.loads(artifact_path.read_text())
    except json.JSONDecodeError as exc:
        return [
            ValidationFinding(
                code="vscode_tasks.invalid_json",
                message=f"VS Code tasks export could not be parsed: {exc}",
                path=str(artifact_path),
            )
        ]

    findings: list[ValidationFinding] = []
    if payload.get("version") != "2.0.0":
        findings.append(
            ValidationFinding(
                code="vscode_tasks.invalid_version",
                message="VS Code tasks export must use top-level version 2.0.0.",
                path=str(artifact_path),
            )
        )

    tasks = payload.get("tasks")
    if not isinstance(tasks, list):
        return findings + [
            ValidationFinding(
                code="vscode_tasks.invalid_tasks",
                message="VS Code tasks export must include a top-level tasks list.",
                path=str(artifact_path),
            )
        ]

    plan_tasks = execution_plan.get("tasks", [])
    expected_labels = {task["id"]: f"{task['id']}: {task['title']}" for task in plan_tasks}
    rendered_by_label = {
        task.get("label"): task
        for task in tasks
        if isinstance(task, dict) and isinstance(task.get("label"), str)
    }

    if len(tasks) != len(plan_tasks):
        findings.append(
            ValidationFinding(
                code="vscode_tasks.task_count_mismatch",
                message="VS Code tasks count does not match the execution plan.",
                path=str(artifact_path),
            )
        )

    for task in plan_tasks:
        label = expected_labels[task["id"]]
        rendered = rendered_by_label.get(label)
        if rendered is None:
            findings.append(
                ValidationFinding(
                    code="vscode_tasks.missing_task",
                    message=f"VS Code tasks export is missing task label: {label}",
                    path=str(artifact_path),
                )
            )
            continue

        if rendered.get("type") != "shell":
            findings.append(
                ValidationFinding(
                    code="vscode_tasks.invalid_task_type",
                    message=f"VS Code task '{label}' must be a shell task.",
                    path=str(artifact_path),
                )
            )
        if not isinstance(rendered.get("command"), str) or not rendered.get("command"):
            findings.append(
                ValidationFinding(
                    code="vscode_tasks.missing_command",
                    message=f"VS Code task '{label}' must include a shell command.",
                    path=str(artifact_path),
                )
            )

        expected_dependencies = [
            expected_labels[task_id]
            for task_id in task.get("depends_on", [])
            if task_id in expected_labels
        ]
        rendered_dependencies = rendered.get("dependsOn", [])
        if isinstance(rendered_dependencies, str):
            rendered_dependencies = [rendered_dependencies]
        if rendered_dependencies != expected_dependencies:
            findings.append(
                ValidationFinding(
                    code="vscode_tasks.dependency_mismatch",
                    message=f"VS Code task '{label}' dependencies do not match the plan.",
                    path=str(artifact_path),
                )
            )

    return findings


def _validate_taskfile(
    artifact_path: Path,
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any],
) -> list[ValidationFinding]:
    """Validate the go-task Taskfile.yml export."""
    try:
        import yaml

        payload = yaml.safe_load(artifact_path.read_text())
    except yaml.YAMLError as exc:
        return [
            ValidationFinding(
                code="taskfile.invalid_yaml",
                message=f"Taskfile export could not be parsed: {exc}",
                path=str(artifact_path),
            )
        ]

    if not isinstance(payload, dict):
        return [
            ValidationFinding(
                code="taskfile.invalid_shape",
                message="Taskfile export must be a YAML object.",
                path=str(artifact_path),
            )
        ]

    findings: list[ValidationFinding] = []
    if str(payload.get("version")) != "3":
        findings.append(
            ValidationFinding(
                code="taskfile.invalid_version",
                message="Taskfile export must use top-level version 3.",
                path=str(artifact_path),
            )
        )

    rendered_tasks = payload.get("tasks")
    if not isinstance(rendered_tasks, dict):
        return findings + [
            ValidationFinding(
                code="taskfile.invalid_tasks",
                message="Taskfile export must include a top-level tasks object.",
                path=str(artifact_path),
            )
        ]

    plan_tasks = execution_plan.get("tasks", [])
    expected_names = TaskfileExporter().task_names_by_id(plan_tasks)
    generated_names = set(expected_names.values())

    if "default" not in rendered_tasks:
        findings.append(
            ValidationFinding(
                code="taskfile.missing_default",
                message="Taskfile export must include a default task.",
                path=str(artifact_path),
            )
        )

    for task_id, task_name in expected_names.items():
        rendered = rendered_tasks.get(task_name)
        if not isinstance(rendered, dict):
            findings.append(
                ValidationFinding(
                    code="taskfile.missing_task",
                    message=f"Taskfile export is missing generated task: {task_name}",
                    path=str(artifact_path),
                )
            )
            continue

        if not isinstance(rendered.get("desc"), str) or not rendered.get("desc"):
            findings.append(
                ValidationFinding(
                    code="taskfile.missing_desc",
                    message=f"Taskfile task '{task_name}' must include a description.",
                    path=str(artifact_path),
                )
            )

        if not isinstance(rendered.get("cmds"), list) or not rendered.get("cmds"):
            findings.append(
                ValidationFinding(
                    code="taskfile.missing_cmds",
                    message=f"Taskfile task '{task_name}' must include commands.",
                    path=str(artifact_path),
                )
            )

        rendered_deps = rendered.get("deps", [])
        if isinstance(rendered_deps, str):
            rendered_deps = [rendered_deps]
        if not isinstance(rendered_deps, list):
            findings.append(
                ValidationFinding(
                    code="taskfile.invalid_deps",
                    message=f"Taskfile task '{task_name}' deps must be a list.",
                    path=str(artifact_path),
                )
            )
            continue

        unknown_deps = [
            dep
            for dep in rendered_deps
            if not isinstance(dep, str) or dep not in rendered_tasks
        ]
        if unknown_deps:
            findings.append(
                ValidationFinding(
                    code="taskfile.unknown_dependency",
                    message=(
                        f"Taskfile task '{task_name}' references unknown deps: "
                        + ", ".join(str(dep) for dep in unknown_deps)
                    ),
                    path=str(artifact_path),
                )
            )

        plan_task = next(task for task in plan_tasks if task["id"] == task_id)
        expected_deps = [
            expected_names[dependency_id]
            for dependency_id in plan_task.get("depends_on", [])
            if dependency_id in expected_names
        ]
        generated_rendered_deps = [
            dep for dep in rendered_deps if isinstance(dep, str) and dep in generated_names
        ]
        if generated_rendered_deps != expected_deps:
            findings.append(
                ValidationFinding(
                    code="taskfile.dependency_mismatch",
                    message=f"Taskfile task '{task_name}' dependencies do not match the plan.",
                    path=str(artifact_path),
                )
            )

    return findings


def _validate_wave_schedule(
    artifact_path: Path,
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any],
) -> list[ValidationFinding]:
    """Validate the dependency wave schedule JSON export."""
    try:
        payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return [
            ValidationFinding(
                code="wave_schedule.invalid_json",
                message=f"Wave schedule export is not valid JSON: {exc.msg}",
                path=str(artifact_path),
            )
        ]

    if not isinstance(payload, dict):
        return [
            ValidationFinding(
                code="wave_schedule.invalid_shape",
                message="Wave schedule export must be a JSON object.",
                path=str(artifact_path),
            )
        ]

    findings: list[ValidationFinding] = []
    findings.extend(
        _missing_keys(
            payload,
            ["schema_version", "plan_id", "total_waves", "waves"],
            "wave_schedule.missing_key",
            str(artifact_path),
        )
    )

    if payload.get("schema_version") != WAVE_SCHEDULE_SCHEMA_VERSION:
        findings.append(
            ValidationFinding(
                code="wave_schedule.schema_version_mismatch",
                message="Wave schedule schema_version is not supported.",
                path=str(artifact_path),
            )
        )

    if payload.get("plan_id") != execution_plan.get("id"):
        findings.append(
            ValidationFinding(
                code="wave_schedule.plan_id_mismatch",
                message="Wave schedule plan_id does not match the execution plan.",
                path=str(artifact_path),
            )
        )

    waves = payload.get("waves")
    if not isinstance(waves, list):
        findings.append(
            ValidationFinding(
                code="wave_schedule.waves.invalid_shape",
                message="Wave schedule waves must be a list.",
                path=str(artifact_path),
            )
        )
        return findings

    if isinstance(payload.get("total_waves"), int) and payload["total_waves"] != len(waves):
        findings.append(
            ValidationFinding(
                code="wave_schedule.total_waves_mismatch",
                message="Wave schedule total_waves does not match rendered waves.",
                path=str(artifact_path),
            )
        )

    expected_task_ids = [task["id"] for task in execution_plan.get("tasks", [])]
    expected_task_id_set = set(expected_task_ids)
    rendered_task_counts: dict[str, int] = {}
    task_wave_by_id: dict[str, int] = {}

    for wave_index, wave in enumerate(waves, 1):
        wave_path = f"{artifact_path}#/waves/{wave_index - 1}"
        if not isinstance(wave, dict):
            findings.append(
                ValidationFinding(
                    code="wave_schedule.wave.invalid_shape",
                    message=f"Wave {wave_index} must be an object.",
                    path=wave_path,
                )
            )
            continue

        findings.extend(
            _missing_keys(
                wave,
                ["wave_number", "task_ids", "tasks"],
                "wave_schedule.wave.missing_key",
                wave_path,
            )
        )

        wave_number = wave.get("wave_number")
        if not isinstance(wave_number, int):
            findings.append(
                ValidationFinding(
                    code="wave_schedule.wave_number.invalid_shape",
                    message=f"Wave {wave_index} wave_number must be an integer.",
                    path=wave_path,
                )
            )
            wave_number = wave_index

        task_ids = wave.get("task_ids")
        tasks = wave.get("tasks")
        if not isinstance(task_ids, list) or not all(
            isinstance(task_id, str) for task_id in task_ids
        ):
            findings.append(
                ValidationFinding(
                    code="wave_schedule.task_ids.invalid_shape",
                    message=f"Wave {wave_index} task_ids must be a list of strings.",
                    path=wave_path,
                )
            )
            task_ids = []

        if not isinstance(tasks, list):
            findings.append(
                ValidationFinding(
                    code="wave_schedule.tasks.invalid_shape",
                    message=f"Wave {wave_index} tasks must be a list.",
                    path=wave_path,
                )
            )
            continue

        rendered_ids_in_wave: list[str] = []
        for task_index, task in enumerate(tasks, 1):
            task_path = f"{wave_path}/tasks/{task_index - 1}"
            if not isinstance(task, dict):
                findings.append(
                    ValidationFinding(
                        code="wave_schedule.task.invalid_shape",
                        message=f"Wave {wave_index} task {task_index} must be an object.",
                        path=task_path,
                    )
                )
                continue

            findings.extend(
                _missing_keys(
                    task,
                    [
                        "id",
                        "suggested_engine",
                        "owner_type",
                        "files_or_modules",
                        "dependencies",
                        "status",
                        "status_metadata",
                    ],
                    "wave_schedule.task.missing_key",
                    task_path,
                )
            )

            task_id = task.get("id")
            if not isinstance(task_id, str) or not task_id:
                findings.append(
                    ValidationFinding(
                        code="wave_schedule.task.missing_id",
                        message=f"Wave {wave_index} task {task_index} is missing id.",
                        path=task_path,
                    )
                )
                continue

            rendered_ids_in_wave.append(task_id)
            rendered_task_counts[task_id] = rendered_task_counts.get(task_id, 0) + 1
            task_wave_by_id[task_id] = wave_number

        if task_ids and rendered_ids_in_wave != task_ids:
            findings.append(
                ValidationFinding(
                    code="wave_schedule.task_ids_mismatch",
                    message=f"Wave {wave_index} task_ids do not match task objects.",
                    path=wave_path,
                )
            )

    duplicate_task_ids = sorted(
        task_id for task_id, count in rendered_task_counts.items() if count > 1
    )
    if duplicate_task_ids:
        findings.append(
            ValidationFinding(
                code="wave_schedule.duplicate_task",
                message=f"Wave schedule repeats task ids: {', '.join(duplicate_task_ids)}",
                path=str(artifact_path),
            )
        )

    missing_task_ids = sorted(expected_task_id_set - set(rendered_task_counts))
    if missing_task_ids:
        findings.append(
            ValidationFinding(
                code="wave_schedule.missing_task",
                message=f"Wave schedule is missing task ids: {', '.join(missing_task_ids)}",
                path=str(artifact_path),
            )
        )

    unexpected_task_ids = sorted(set(rendered_task_counts) - expected_task_id_set)
    if unexpected_task_ids:
        findings.append(
            ValidationFinding(
                code="wave_schedule.unexpected_task",
                message=f"Wave schedule has unexpected task ids: {', '.join(unexpected_task_ids)}",
                path=str(artifact_path),
            )
        )

    for task in execution_plan.get("tasks", []):
        task_id = task["id"]
        task_wave = task_wave_by_id.get(task_id)
        if task_wave is None:
            continue
        for dependency_id in task.get("depends_on") or []:
            dependency_wave = task_wave_by_id.get(dependency_id)
            if dependency_wave is None:
                continue
            if dependency_wave >= task_wave:
                findings.append(
                    ValidationFinding(
                        code="wave_schedule.dependency_order",
                        message=(
                            f"Task {task_id} is scheduled before or alongside dependency "
                            f"{dependency_id}."
                        ),
                        path=str(artifact_path),
                    )
                )

    return findings


def _validate_github_actions(
    artifact_path: Path,
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any],
) -> list[ValidationFinding]:
    """Validate the GitHub Actions workflow export."""
    try:
        import yaml

        payload = yaml.safe_load(artifact_path.read_text())
    except yaml.YAMLError as exc:
        return [
            ValidationFinding(
                code="github_actions.invalid_yaml",
                message=f"GitHub Actions workflow could not be parsed: {exc}",
                path=str(artifact_path),
            )
        ]

    if not isinstance(payload, dict):
        return [
            ValidationFinding(
                code="github_actions.invalid_shape",
                message="GitHub Actions workflow must be a YAML object.",
                path=str(artifact_path),
            )
        ]

    findings: list[ValidationFinding] = []
    findings.extend(
        _missing_keys(
            payload,
            ["name", "on", "jobs"],
            "github_actions.missing_key",
            str(artifact_path),
        )
    )

    jobs = payload.get("jobs")
    if not isinstance(jobs, dict) or not jobs:
        return findings + [
            ValidationFinding(
                code="github_actions.invalid_jobs",
                message="GitHub Actions workflow must include a non-empty jobs object.",
                path=str(artifact_path),
            )
        ]

    known_job_ids = set(jobs)
    for job_id, job in jobs.items():
        if not isinstance(job, dict):
            findings.append(
                ValidationFinding(
                    code="github_actions.invalid_job",
                    message=f"GitHub Actions job '{job_id}' must be an object.",
                    path=str(artifact_path),
                )
            )
            continue

        needs = job.get("needs", [])
        if isinstance(needs, str):
            needs = [needs]
        if not isinstance(needs, list):
            findings.append(
                ValidationFinding(
                    code="github_actions.invalid_needs",
                    message=f"GitHub Actions job '{job_id}' needs must be a string or list.",
                    path=str(artifact_path),
                )
            )
            continue

        unknown_needs = sorted(
            (need for need in needs if not isinstance(need, str) or need not in known_job_ids),
            key=str,
        )
        if unknown_needs:
            findings.append(
                ValidationFinding(
                    code="github_actions.unknown_needs",
                    message=(
                        f"GitHub Actions job '{job_id}' references unknown needs: "
                        f"{', '.join(str(need) for need in unknown_needs)}"
                    ),
                    path=str(artifact_path),
                )
            )

    return findings


def _validate_mermaid(
    artifact_path: Path,
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any],
) -> list[ValidationFinding]:
    """Validate the Mermaid flowchart export."""
    content = artifact_path.read_text()
    if _has_heading(content, "flowchart TD"):
        return []

    return [
        ValidationFinding(
            code="mermaid.missing_flowchart",
            message="Mermaid export must start with 'flowchart TD'.",
            path=str(artifact_path),
        )
    ]


def _validate_gantt(
    artifact_path: Path,
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any],
) -> list[ValidationFinding]:
    """Validate the Mermaid Gantt export."""
    content = artifact_path.read_text()
    label = str(artifact_path)
    if not content.startswith("gantt\n"):
        return [
            ValidationFinding(
                code="gantt.missing_header",
                message="Gantt export must start with 'gantt'.",
                path=label,
            )
        ]

    findings: list[ValidationFinding] = []
    for milestone in execution_plan.get("milestones", []):
        name = milestone.get("name") or milestone.get("title")
        if name and f"    section {name}" not in content:
            findings.append(
                ValidationFinding(
                    code="gantt.missing_milestone_section",
                    message=f"Gantt export is missing milestone section: {name}",
                    path=label,
                )
            )

    rendered_tasks = _gantt_rendered_tasks_by_mermaid_id(content)
    expected_ids = [task["id"] for task in execution_plan.get("tasks", [])]
    mermaid_ids_by_task_id = {
        task["id"]: _gantt_mermaid_id(task["id"], index)
        for index, task in enumerate(execution_plan.get("tasks", []), 1)
    }
    task_id_by_mermaid_id = {
        mermaid_id: task_id for task_id, mermaid_id in mermaid_ids_by_task_id.items()
    }
    rendered_task_ids = {
        task_id_by_mermaid_id[mermaid_id]
        for mermaid_id in rendered_tasks
        if mermaid_id in task_id_by_mermaid_id
    }
    expected_id_set = set(expected_ids)
    rendered_id_set = set(rendered_task_ids)

    missing_task_ids = sorted(expected_id_set - rendered_id_set)
    if missing_task_ids:
        findings.append(
            ValidationFinding(
                code="gantt.missing_task",
                message=f"Gantt export is missing task ids: {', '.join(missing_task_ids)}",
                path=label,
            )
        )

    unexpected_task_ids = sorted(
        mermaid_id for mermaid_id in rendered_tasks if mermaid_id not in task_id_by_mermaid_id
    )
    if unexpected_task_ids:
        findings.append(
            ValidationFinding(
                code="gantt.unexpected_task",
                message=(
                    "Gantt export has unexpected Mermaid task ids: "
                    f"{', '.join(unexpected_task_ids)}"
                ),
                path=label,
            )
        )

    rendered_order = {
        task_id_by_mermaid_id[mermaid_id]: index
        for index, mermaid_id in enumerate(rendered_tasks)
        if mermaid_id in task_id_by_mermaid_id
    }
    for task in execution_plan.get("tasks", []):
        task_id = task["id"]
        rendered = rendered_tasks.get(mermaid_ids_by_task_id[task_id])
        if rendered is None:
            continue
        timing = rendered["timing"]
        if timing.startswith("after "):
            dependency_mermaid_id = timing.removeprefix("after ").split(",", 1)[0].strip()
            dependency_id = task_id_by_mermaid_id.get(dependency_mermaid_id)
            if dependency_id is None or dependency_id not in task.get("depends_on", []):
                findings.append(
                    ValidationFinding(
                        code="gantt.invalid_after_dependency",
                        message=f"Gantt task {task_id} references an invalid after dependency.",
                        path=label,
                    )
                )
            elif rendered_order.get(dependency_id, -1) > rendered_order.get(task_id, -1):
                findings.append(
                    ValidationFinding(
                        code="gantt.dependency_order",
                        message=f"Gantt task {task_id} is rendered before dependency {dependency_id}.",
                        path=label,
                    )
                )

    return findings


def _validate_calendar(
    artifact_path: Path,
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any],
) -> list[ValidationFinding]:
    """Validate an iCalendar execution-plan export."""
    content = artifact_path.read_text()
    label = str(artifact_path)
    if not content.strip():
        return [
            ValidationFinding(
                code="calendar.empty",
                message="Calendar export is empty.",
                path=label,
            )
        ]

    lines = _unfold_icalendar_lines(content)
    findings: list[ValidationFinding] = []
    if "BEGIN:VCALENDAR" not in lines or "END:VCALENDAR" not in lines:
        findings.append(
            ValidationFinding(
                code="calendar.invalid_structure",
                message="Calendar export must contain a VCALENDAR wrapper.",
                path=label,
            )
        )
    if "VERSION:2.0" not in lines:
        findings.append(
            ValidationFinding(
                code="calendar.missing_version",
                message="Calendar export must declare VERSION:2.0.",
                path=label,
            )
        )

    events = _icalendar_event_blocks(lines)
    if not events:
        findings.append(
            ValidationFinding(
                code="calendar.empty",
                message="Calendar export contains no VEVENT entries.",
                path=label,
            )
        )

    expected_task_ids = _dated_task_ids(execution_plan)
    expected_uids = {
        f"blueprint-{_calendar_uid_token(execution_plan['id'])}-{_calendar_uid_token(task_id)}@blueprint"
        for task_id in expected_task_ids
    }
    rendered_uids = {_icalendar_property(event, "UID") for event in events}
    missing_uids = sorted(expected_uids - rendered_uids)
    if missing_uids:
        findings.append(
            ValidationFinding(
                code="calendar.missing_task_event",
                message=f"Calendar export is missing dated task events: {', '.join(missing_uids)}",
                path=label,
            )
        )

    for index, event in enumerate(events, 1):
        uid = _icalendar_property(event, "UID")
        if not uid:
            findings.append(
                ValidationFinding(
                    code="calendar.event_missing_uid",
                    message=f"VEVENT {index} is missing UID.",
                    path=label,
                )
            )
        if not _icalendar_property(event, "SUMMARY"):
            findings.append(
                ValidationFinding(
                    code="calendar.event_missing_summary",
                    message=f"VEVENT {index} is missing SUMMARY.",
                    path=label,
                )
            )
        has_start = any(line.startswith("DTSTART") for line in event)
        has_end = any(line.startswith("DTEND") for line in event)
        if not has_start or not has_end:
            findings.append(
                ValidationFinding(
                    code="calendar.event_missing_dates",
                    message=f"VEVENT {index} must include DTSTART and DTEND.",
                    path=label,
                )
            )

    skipped_count = _icalendar_property(lines, "X-BLUEPRINT-SKIPPED-TASK-COUNT")
    expected_skipped_count = len(execution_plan.get("tasks", [])) - len(expected_task_ids)
    if skipped_count != str(expected_skipped_count):
        findings.append(
            ValidationFinding(
                code="calendar.skipped_task_count_mismatch",
                message="Calendar skipped task metadata does not match undated task count.",
                path=label,
            )
        )

    return findings


def _missing_keys(
    payload: dict[str, Any],
    required_keys: list[str],
    code: str,
    path: str,
) -> list[ValidationFinding]:
    """Return one finding for each missing required key."""
    findings = []
    for key in required_keys:
        if key not in payload:
            findings.append(
                ValidationFinding(
                    code=code,
                    message=f"Missing required key: {key}",
                    path=path,
                )
            )
    return findings


def _unfold_icalendar_lines(content: str) -> list[str]:
    """Unfold iCalendar continuation lines."""
    normalized = content.replace("\r\n", "\n").replace("\r", "\n")
    lines: list[str] = []
    for raw_line in normalized.split("\n"):
        if not raw_line:
            continue
        if raw_line.startswith((" ", "\t")) and lines:
            lines[-1] += raw_line[1:]
        else:
            lines.append(raw_line)
    return lines


def _icalendar_event_blocks(lines: list[str]) -> list[list[str]]:
    """Extract VEVENT blocks from unfolded iCalendar lines."""
    events: list[list[str]] = []
    current: list[str] | None = None
    for line in lines:
        if line == "BEGIN:VEVENT":
            current = [line]
            continue
        if current is not None:
            current.append(line)
            if line == "END:VEVENT":
                events.append(current)
                current = None
    return events


def _icalendar_property(lines: list[str], name: str) -> str | None:
    """Read one iCalendar property by name, ignoring parameters."""
    prefix = f"{name}:"
    parameter_prefix = f"{name};"
    for line in lines:
        if line.startswith(prefix):
            return line[len(prefix) :]
        if line.startswith(parameter_prefix):
            _, _, value = line.partition(":")
            return value
    return None


def _dated_task_ids(execution_plan: dict[str, Any]) -> list[str]:
    """Return task IDs that should appear as calendar events."""
    exporter = CalendarExporter()
    dated_task_ids = []
    for task in execution_plan.get("tasks", []):
        date_range, _ = exporter._date_range(task)
        if date_range is not None:
            dated_task_ids.append(task["id"])
    return dated_task_ids


def _calendar_uid_token(value: str) -> str:
    """Recreate the CalendarExporter UID token scheme."""
    return re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip("-") or "item"


def _has_heading(content: str, expected: str) -> bool:
    """Check whether a Markdown heading or structural line is present."""
    normalized = expected.strip()
    for line in content.splitlines():
        candidate = line.strip()
        if not candidate:
            continue
        if candidate == normalized:
            return True
        if normalized.startswith("#") and candidate.startswith(normalized):
            return True
    return False


def _coverage_section_lines(content: str, section: str) -> list[str]:
    """Return lines belonging to one coverage matrix section."""
    lines: list[str] = []
    in_section = False
    heading = f"## {section}"
    for line in content.splitlines():
        stripped = line.strip()
        if stripped == heading:
            in_section = True
            continue
        if in_section and stripped.startswith("## "):
            break
        if in_section:
            lines.append(line)
    return lines


def _coverage_item_occurrences(section_lines: list[str], expected_item: str) -> int:
    """Count expected item rows in a coverage matrix section."""
    expected = _coverage_table_cell(expected_item)
    return sum(1 for row in _coverage_rendered_rows(section_lines) if row["item"] == expected)


def _coverage_rendered_rows(section_lines: list[str]) -> list[dict[str, str]]:
    """Extract coverage matrix rows from a Markdown table."""
    rows: list[dict[str, str]] = []
    for line in section_lines:
        stripped = line.strip()
        if not stripped.startswith("|") or stripped in {
            "| Item | Status | Matching Tasks |",
            "| --- | --- | --- |",
        }:
            continue

        cells = _split_markdown_table_row(stripped)
        if len(cells) != 3:
            continue
        rows.append(
            {
                "item": cells[0],
                "status": cells[1],
                "matching_tasks": cells[2],
            }
        )
    return rows


def _raci_rendered_rows(content: str) -> list[dict[str, str]]:
    """Extract RACI matrix rows from the responsibility table."""
    rows: list[dict[str, str]] = []
    for line in _markdown_section_lines(content, "Responsibility Matrix"):
        stripped = line.strip()
        if not stripped.startswith("|") or stripped in {
            (
                "| Task ID | Task | Responsible | Accountable | Consulted | Informed | "
                "Milestone | Suggested Engine |"
            ),
            "| --- | --- | --- | --- | --- | --- | --- | --- |",
        }:
            continue

        cells = _split_markdown_table_row(stripped)
        if len(cells) != 8:
            rows.append({})
            continue
        rows.append(
            {
                "task_id": cells[0],
                "task": cells[1],
                "responsible": cells[2],
                "accountable": cells[3],
                "consulted": cells[4],
                "informed": cells[5],
                "milestone": cells[6],
                "suggested_engine": cells[7],
            }
        )
    return rows


def _risk_register_rendered_rows(content: str) -> list[dict[str, str]]:
    """Extract risk register rows from the register table."""
    rows: list[dict[str, str]] = []
    header = (
        "| Risk ID | Source Risk | Affected Milestones/Tasks | Mitigation Evidence | "
        "Owner/Suggested Engine | Status |"
    )
    for line in _markdown_section_lines(content, "Register"):
        stripped = line.strip()
        if not stripped.startswith("|") or stripped in {
            header,
            "| --- | --- | --- | --- | --- | --- |",
        }:
            continue

        cells = _split_markdown_table_row(stripped)
        if len(cells) != 6:
            rows.append({})
            continue
        rows.append(
            {
                "risk_id": cells[0],
                "source_risk": cells[1],
                "affected": cells[2],
                "mitigation_evidence": cells[3],
                "owner_suggested_engine": cells[4],
                "status": cells[5],
            }
        )
    return rows


def _risk_register_referenced_task_ids(value: str) -> list[str]:
    """Extract Markdown code-formatted task references from a register cell."""
    return re.findall(r"`([^`]+)`", value)


def _markdown_section_lines(content: str, section: str) -> list[str]:
    """Return lines belonging to one second-level Markdown section."""
    lines: list[str] = []
    in_section = False
    heading = f"## {section}"
    for line in content.splitlines():
        stripped = line.strip()
        if stripped == heading:
            in_section = True
            continue
        if in_section and stripped.startswith("## "):
            break
        if in_section:
            lines.append(line)
    return lines


def _split_markdown_table_row(row: str) -> list[str]:
    """Split a simple Markdown table row while preserving escaped pipes."""
    inner = row.strip().strip("|")
    cells: list[str] = []
    current: list[str] = []
    for index, char in enumerate(inner):
        if char == "|" and (index == 0 or inner[index - 1] != "\\"):
            cells.append("".join(current).strip())
            current = []
            continue
        current.append(char)
    cells.append("".join(current).strip())
    return cells


def _coverage_table_cell(value: str) -> str:
    """Recreate coverage matrix table escaping for validation."""
    return str(value).replace("\\", "\\\\").replace("|", "\\|").replace("\n", " ")


def _risk_register_table_cell(value: str) -> str:
    """Recreate risk register table escaping for validation."""
    return str(value).replace("\\", "\\\\").replace("|", "\\|").replace("\n", " ")


def _unwrapped_code_cell(value: str) -> str:
    """Return a Markdown code cell value without wrapping backticks."""
    stripped = value.strip()
    if stripped.startswith("`") and stripped.endswith("`") and len(stripped) >= 2:
        return stripped[1:-1]
    return stripped


def _string_items(value: Any) -> list[str]:
    """Return non-empty string representations from a list."""
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _kanban_rendered_tasks_by_column(content: str) -> dict[str, list[str]]:
    """Extract rendered Kanban task IDs by status heading."""
    columns = {"pending", "in_progress", "blocked", "completed", "skipped"}
    rendered_tasks = {column: [] for column in columns}
    current_column: str | None = None

    for line in content.splitlines():
        stripped = line.strip()
        if stripped.startswith("## "):
            heading = stripped[3:].strip()
            current_column = heading if heading in columns else None
            continue

        match = re.match(r"^### `([^`]+)` - .+", stripped)
        if match and current_column:
            rendered_tasks[current_column].append(match.group(1))

    return rendered_tasks


def _checklist_rendered_task_ids(content: str) -> list[str]:
    """Extract rendered checklist task IDs from task checkbox lines."""
    task_ids = []
    for line in content.splitlines():
        match = re.match(r"^- \[[ xX]\] `([^`]+)` .+", line.strip())
        if match:
            task_ids.append(match.group(1))
    return task_ids


def _gantt_rendered_tasks_by_mermaid_id(content: str) -> dict[str, dict[str, str]]:
    """Extract rendered Gantt task Mermaid ids and timing fields."""
    rendered_tasks: dict[str, dict[str, str]] = {}
    for line in content.splitlines():
        stripped = line.strip()
        if not stripped or stripped in {"gantt"}:
            continue
        if stripped.startswith(("title ", "dateFormat ", "axisFormat ", "section ")):
            continue
        if ":" not in stripped:
            continue
        label, fields_text = stripped.rsplit(":", 1)
        fields = [field.strip() for field in fields_text.split(",") if field.strip()]
        mermaid_id = next(
            (
                field
                for field in fields
                if field not in {"crit", "done", "active", "milestone"}
                and not field.startswith("after ")
                and not re.fullmatch(r"\d{4}-\d{2}-\d{2}", field)
                and not re.fullmatch(r"\d+d", field)
            ),
            None,
        )
        if mermaid_id is None:
            continue
        timing_fields = fields[fields.index(mermaid_id) + 1 :]
        rendered_tasks[mermaid_id] = {
            "label": label.strip(),
            "mermaid_id": mermaid_id,
            "timing": ", ".join(timing_fields),
        }
    return rendered_tasks


def _gantt_mermaid_id(task_id: str, index: int) -> str:
    """Recreate the Gantt exporter Mermaid id scheme."""
    mermaid_id = re.sub(r"[^0-9A-Za-z_]", "_", task_id)
    if not mermaid_id or mermaid_id[0].isdigit():
        mermaid_id = f"task_{index}_{mermaid_id}"
    return mermaid_id


def _file_impact_rendered_sections(content: str) -> dict[str, list[str]]:
    """Extract rendered file impact task IDs by file/module heading."""
    sections: dict[str, list[str]] = {}
    current_section: str | None = None

    for line in content.splitlines():
        stripped = line.strip()
        file_match = re.match(r"^### `(.+)`$", stripped)
        if file_match:
            current_section = file_match.group(1)
            sections.setdefault(current_section, [])
            continue

        if stripped == "## Unassigned":
            current_section = UNASSIGNED_SECTION
            sections.setdefault(current_section, [])
            continue

        task_match = re.match(r"^- `([^`]+)` .+", stripped)
        if task_match and current_section:
            sections[current_section].append(task_match.group(1))

    return sections


def _xml_property_value(properties: ElementTree.Element, name: str) -> str | None:
    """Return the value of a JUnit property by name."""
    for property_node in properties.findall("property"):
        if property_node.attrib.get("name") == name:
            return property_node.attrib.get("value")
    return None


def _task_bundle_filename(index: int, task_id: str) -> str:
    """Recreate the task bundle filename scheme."""
    slug = re.sub(r"[^A-Za-z0-9._-]+", "-", task_id).strip("-")
    return f"{index:03d}-{slug or 'task'}.md"


def _blocked_reason(task: dict[str, Any]) -> str | None:
    """Return a blocked reason from supported task shapes."""
    metadata = task.get("metadata") or {}
    return task.get("blocked_reason") or metadata.get("blocked_reason")


def _expected_trello_list_names(execution_plan: dict[str, Any]) -> list[str]:
    """Return the list names expected from the Trello JSON exporter."""
    tasks = execution_plan.get("tasks", [])
    milestone_names = [
        str(milestone.get("name")).strip()
        for milestone in execution_plan.get("milestones", [])
        if isinstance(milestone, dict) and str(milestone.get("name", "")).strip()
    ]
    task_milestones = [
        str(task.get("milestone")).strip()
        for task in tasks
        if str(task.get("milestone") or "").strip()
    ]
    names: list[str] = []
    for name in milestone_names + task_milestones:
        if name and name not in names:
            names.append(name)
    if names:
        if any(not str(task.get("milestone") or "").strip() for task in tasks):
            names.append("Ungrouped")
        return names
    return ["pending", "in_progress", "blocked", "completed", "skipped"]


def _validate_trello_acceptance_checklist(
    findings: list[ValidationFinding],
    artifact_path: Path,
    card_index: int,
    task_id: str,
    task: dict[str, Any],
    checklists: list[Any],
) -> None:
    """Validate one card's acceptance criteria checklist."""
    acceptance_checklist = None
    for checklist in checklists:
        if isinstance(checklist, dict) and checklist.get("name") == "Acceptance Criteria":
            acceptance_checklist = checklist
            break

    if not isinstance(acceptance_checklist, dict):
        findings.append(
            ValidationFinding(
                code="trello_json.card.missing_acceptance_checklist",
                message=f"Trello card {card_index} for task {task_id} is missing an Acceptance Criteria checklist.",
                path=str(artifact_path),
            )
        )
        return

    items = acceptance_checklist.get("items")
    if not isinstance(items, list):
        findings.append(
            ValidationFinding(
                code="trello_json.card.checklist_items.invalid_shape",
                message=f"Trello card {card_index} Acceptance Criteria checklist items must be a list.",
                path=str(artifact_path),
            )
        )
        return

    rendered_items = [
        item.get("name")
        for item in items
        if isinstance(item, dict) and isinstance(item.get("name"), str)
    ]
    expected_items = task.get("acceptance_criteria") or []
    if rendered_items != expected_items:
        findings.append(
            ValidationFinding(
                code="trello_json.card.acceptance_criteria_mismatch",
                message=f"Trello card {card_index} checklist items do not match task {task_id} acceptance criteria.",
                path=str(artifact_path),
            )
        )


_VALIDATORS: dict[str, ValidationCheck] = {
    "adr": _validate_adr,
    "agent-prompt-pack": _validate_agent_prompt_pack,
    "relay": _validate_relay,
    "relay-yaml": _validate_relay_yaml,
    "codex": _validate_codex,
    "claude-code": _validate_claude_code,
    "smoothie": _validate_smoothie,
    "task-bundle": _validate_task_bundle,
    "asana-csv": _validate_asana_csv,
    "azure-devops-csv": _validate_azure_devops_csv,
    "github-issues": _validate_github_issues_bundle,
    "gitlab-issues": _validate_gitlab_issues,
    "html-report": _validate_html_report,
    "jira-csv": _validate_jira_csv,
    "linear": _validate_linear,
    "calendar": _validate_calendar,
    "checklist": _validate_checklist,
    "coverage-matrix": _validate_coverage_matrix,
    "critical-path-report": _validate_critical_path_report,
    "kanban": _validate_kanban,
    "release-notes": _validate_release_notes,
    "risk-register": _validate_risk_register,
    "slack-digest": _validate_slack_digest,
    "status-report": _validate_status_report,
    "csv-tasks": _validate_csv_tasks,
    "taskfile": _validate_taskfile,
    "task-queue-jsonl": _validate_task_queue_jsonl,
    "trello-json": _validate_trello_json,
    "file-impact-map": _validate_file_impact_map,
    "gantt": _validate_gantt,
    "github-actions": _validate_github_actions,
    "junit-tasks": _validate_junit_tasks,
    "mermaid": _validate_mermaid,
    "milestone-summary": _validate_milestone_summary,
    "plan-snapshot": _validate_plan_snapshot,
    "raci-matrix": _validate_raci_matrix,
    "vscode-tasks": _validate_vscode_tasks,
    "wave-schedule": _validate_wave_schedule,
}


def _expected_cross_milestone_dependency_lines(
    execution_plan: dict[str, Any],
) -> list[str]:
    """Return the cross-milestone dependency lines expected from the exporter."""
    tasks = execution_plan.get("tasks", [])
    tasks_by_id = {task["id"]: task for task in tasks}
    lines: list[str] = []
    for task in sorted(tasks, key=lambda item: item["id"]):
        task_milestone = task.get("milestone") or "Ungrouped"
        for dependency_id in task.get("depends_on") or []:
            dependency = tasks_by_id.get(dependency_id)
            if dependency is None:
                continue
            dependency_milestone = dependency.get("milestone") or "Ungrouped"
            if dependency_milestone == task_milestone:
                continue
            lines.append(
                f"- `{task['id']}` ({task_milestone}) depends on "
                f"`{dependency_id}` ({dependency_milestone})"
            )
    return lines


def _task_labels(task: dict[str, Any]) -> list[str]:
    """Extract task labels from metadata."""
    metadata = task.get("metadata") or {}
    labels = metadata.get("labels") or []
    return [label for label in labels if isinstance(label, str) and label]


def _github_issue_filename(index: int, task_id: str) -> str:
    """Recreate the GitHub issue draft filename."""
    slug = re.sub(r"[^A-Za-z0-9._-]+", "-", task_id).strip("-")
    return f"issues/{index:03d}-{slug or 'task'}.md"
