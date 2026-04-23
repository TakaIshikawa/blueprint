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

from blueprint.exporters.claude_code import ClaudeCodeExporter
from blueprint.exporters.codex import CodexExporter
from blueprint.exporters.csv_tasks import CsvTasksExporter
from blueprint.exporters.junit_tasks import JUnitTasksExporter
from blueprint.exporters.mermaid import MermaidExporter
from blueprint.exporters.relay import RelayExporter
from blueprint.exporters.smoothie import SmoothieExporter
from blueprint.exporters.status_report import StatusReportExporter
from blueprint.exporters.task_bundle import TaskBundleExporter


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
        "relay": RelayExporter(),
        "smoothie": SmoothieExporter(),
        "codex": CodexExporter(),
        "claude-code": ClaudeCodeExporter(),
        "mermaid": MermaidExporter(),
        "csv-tasks": CsvTasksExporter(),
        "junit-tasks": JUnitTasksExporter(),
        "status-report": StatusReportExporter(),
        "task-bundle": TaskBundleExporter(),
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
                message=(
                    "CSV export row count does not match the number of execution tasks."
                ),
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


_VALIDATORS: dict[str, ValidationCheck] = {
    "relay": _validate_relay,
    "codex": _validate_codex,
    "claude-code": _validate_claude_code,
    "smoothie": _validate_smoothie,
    "task-bundle": _validate_task_bundle,
    "status-report": _validate_status_report,
    "csv-tasks": _validate_csv_tasks,
    "junit-tasks": _validate_junit_tasks,
    "mermaid": _validate_mermaid,
}
