"""Reusable validation helpers for rendered exporter artifacts."""

from __future__ import annotations

import csv
import json
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable
from xml.etree import ElementTree

from blueprint.exporters.adr import ADRExporter
from blueprint.exporters.calendar import CalendarExporter
from blueprint.exporters.checklist import ChecklistExporter
from blueprint.exporters.claude_code import ClaudeCodeExporter
from blueprint.exporters.codex import CodexExporter
from blueprint.exporters.csv_tasks import CsvTasksExporter
from blueprint.exporters.file_impact_map import FileImpactMapExporter, UNASSIGNED_SECTION
from blueprint.exporters.github_issues import GitHubIssuesExporter
from blueprint.exporters.junit_tasks import JUnitTasksExporter
from blueprint.exporters.kanban import KanbanExporter
from blueprint.exporters.mermaid import MermaidExporter
from blueprint.exporters.milestone_summary import MilestoneSummaryExporter
from blueprint.exporters.release_notes import ReleaseNotesExporter
from blueprint.exporters.relay import RelayExporter
from blueprint.exporters.slack_digest import SlackDigestExporter
from blueprint.exporters.smoothie import SmoothieExporter
from blueprint.exporters.status_report import StatusReportExporter
from blueprint.exporters.task_bundle import TaskBundleExporter
from blueprint.exporters.vscode_tasks import VSCodeTasksExporter


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
        "relay": RelayExporter(),
        "smoothie": SmoothieExporter(),
        "codex": CodexExporter(),
        "claude-code": ClaudeCodeExporter(),
        "calendar": CalendarExporter(),
        "checklist": ChecklistExporter(),
        "mermaid": MermaidExporter(),
        "milestone-summary": MilestoneSummaryExporter(),
        "csv-tasks": CsvTasksExporter(),
        "file-impact-map": FileImpactMapExporter(),
        "github-issues": GitHubIssuesExporter(),
        "junit-tasks": JUnitTasksExporter(),
        "kanban": KanbanExporter(),
        "release-notes": ReleaseNotesExporter(),
        "slack-digest": SlackDigestExporter(),
        "status-report": StatusReportExporter(),
        "task-bundle": TaskBundleExporter(),
        "vscode-tasks": VSCodeTasksExporter(),
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

    if not isinstance(payload, dict):
        return [
            ValidationFinding(
                code="relay.invalid_shape",
                message="Relay export must be a JSON object.",
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
    findings.extend(_missing_keys(payload, required_keys, "relay.missing_key", str(artifact_path)))

    objective = payload.get("objective")
    if isinstance(objective, dict):
        findings.extend(
            _missing_keys(
                objective,
                ["title", "problem", "mvp_goal", "success_criteria"],
                "relay.objective.missing_key",
                str(artifact_path),
            )
        )
    else:
        findings.append(
            ValidationFinding(
                code="relay.objective.invalid_shape",
                message="Relay export objective must be an object.",
                path=str(artifact_path),
            )
        )

    milestones = payload.get("milestones")
    if not isinstance(milestones, list):
        findings.append(
            ValidationFinding(
                code="relay.milestones.invalid_shape",
                message="Relay export milestones must be a list.",
                path=str(artifact_path),
            )
        )
    else:
        for index, milestone in enumerate(milestones, 1):
            if not isinstance(milestone, dict):
                findings.append(
                    ValidationFinding(
                        code="relay.milestone.invalid_shape",
                        message=f"Relay milestone {index} must be an object.",
                        path=str(artifact_path),
                    )
                )
                continue
            findings.extend(
                _missing_keys(
                    milestone,
                    ["id", "name"],
                    "relay.milestone.missing_key",
                    str(artifact_path),
                )
            )

    tasks = payload.get("tasks")
    if not isinstance(tasks, list):
        findings.append(
            ValidationFinding(
                code="relay.tasks.invalid_shape",
                message="Relay export tasks must be a list.",
                path=str(artifact_path),
            )
        )
    else:
        for index, task in enumerate(tasks, 1):
            if not isinstance(task, dict):
                findings.append(
                    ValidationFinding(
                        code="relay.task.invalid_shape",
                        message=f"Relay task {index} must be an object.",
                        path=str(artifact_path),
                    )
                )
                continue
            findings.extend(
                _missing_keys(
                    task,
                    ["id", "milestone_id", "title", "description", "depends_on", "files"],
                    "relay.task.missing_key",
                    str(artifact_path),
                )
            )

    validation = payload.get("validation")
    if isinstance(validation, dict):
        findings.extend(
            _missing_keys(
                validation,
                ["commands"],
                "relay.validation.missing_key",
                str(artifact_path),
            )
        )
    else:
        findings.append(
            ValidationFinding(
                code="relay.validation.invalid_shape",
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
    expected_labels = {
        task["id"]: f"{task['id']}: {task['title']}"
        for task in plan_tasks
    }
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


_VALIDATORS: dict[str, ValidationCheck] = {
    "adr": _validate_adr,
    "relay": _validate_relay,
    "codex": _validate_codex,
    "claude-code": _validate_claude_code,
    "smoothie": _validate_smoothie,
    "task-bundle": _validate_task_bundle,
    "github-issues": _validate_github_issues_bundle,
    "calendar": _validate_calendar,
    "checklist": _validate_checklist,
    "kanban": _validate_kanban,
    "release-notes": _validate_release_notes,
    "slack-digest": _validate_slack_digest,
    "status-report": _validate_status_report,
    "csv-tasks": _validate_csv_tasks,
    "file-impact-map": _validate_file_impact_map,
    "junit-tasks": _validate_junit_tasks,
    "mermaid": _validate_mermaid,
    "milestone-summary": _validate_milestone_summary,
    "vscode-tasks": _validate_vscode_tasks,
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
