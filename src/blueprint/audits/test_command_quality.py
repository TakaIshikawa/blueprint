"""Test command quality audit for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from pathlib import PurePosixPath
from typing import Any, Literal


Severity = Literal["blocking", "warning"]

_VALIDATION_PATTERNS = (
    "pytest",
    "tox",
    "nox",
    "unittest",
    "ruff",
    "mypy",
    "pyright",
    "npm test",
    "npm run test",
    "npm run lint",
    "npm run typecheck",
    "pnpm test",
    "pnpm run test",
    "pnpm lint",
    "pnpm typecheck",
    "yarn test",
    "yarn lint",
    "yarn typecheck",
    "vitest",
    "jest",
    "make test",
    "make lint",
    "go test",
    "cargo test",
    "cargo clippy",
    "mvn test",
    "gradle test",
    "dotnet test",
    "rspec",
    "phpunit",
    "terraform plan",
    "kubectl dry-run",
    "kubectl rollout status",
)
_DESTRUCTIVE_PATTERNS = (
    "rm -rf",
    "rm -fr",
    "git reset --hard",
    "git clean -fd",
    "git clean -xdf",
    "docker system prune",
    "docker volume prune",
    "docker container prune",
    "docker image prune",
    "drop database",
    "truncate table",
)
_INTERACTIVE_PATTERNS = (
    " read -p ",
    "read -p ",
    " input(",
    " prompt(",
    "npm init",
    "yarn init",
    "pnpm init",
    "poetry init",
    "git rebase -i",
    "git add -p",
    "vim ",
    "vi ",
    "nano ",
)
_PYTEST_REPO_WIDE_COMMANDS = {
    "pytest",
    "python -m pytest",
    "poetry run pytest",
    "uv run pytest",
    "pipenv run pytest",
}
_REPO_WIDE_TEST_COMMANDS = {
    *_PYTEST_REPO_WIDE_COMMANDS,
    "tox",
    "nox",
    "npm test",
    "npm run test",
    "pnpm test",
    "pnpm run test",
    "yarn test",
    "make test",
    "go test ./...",
    "cargo test",
}
_WHITESPACE_RE = re.compile(r"\s+")


@dataclass(frozen=True)
class TestCommandQualityFinding:
    """A single test command quality finding."""

    severity: Severity
    code: str
    task_id: str
    task_title: str
    field: str
    message: str
    remediation: str
    value: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "severity": self.severity,
            "code": self.code,
            "task_id": self.task_id,
            "task_title": self.task_title,
            "field": self.field,
            "message": self.message,
            "remediation": self.remediation,
        }
        if self.value is not None:
            payload["value"] = self.value
        return payload


@dataclass(frozen=True)
class TestCommandQualityTaskResult:
    """Test command quality findings for one execution task."""

    task_id: str
    title: str
    findings: list[TestCommandQualityFinding] = field(default_factory=list)

    @property
    def blocking_count(self) -> int:
        return sum(1 for finding in self.findings if finding.severity == "blocking")

    @property
    def warning_count(self) -> int:
        return sum(1 for finding in self.findings if finding.severity == "warning")

    @property
    def ready(self) -> bool:
        return self.blocking_count == 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "title": self.title,
            "ready": self.ready,
            "summary": {
                "blocking": self.blocking_count,
                "warning": self.warning_count,
            },
            "findings": [finding.to_dict() for finding in self.findings],
        }


@dataclass(frozen=True)
class TestCommandQualityResult:
    """Test command quality audit result for an execution plan."""

    plan_id: str
    tasks: list[TestCommandQualityTaskResult] = field(default_factory=list)

    @property
    def blocking_count(self) -> int:
        return sum(task.blocking_count for task in self.tasks)

    @property
    def warning_count(self) -> int:
        return sum(task.warning_count for task in self.tasks)

    @property
    def passed(self) -> bool:
        return self.blocking_count == 0

    @property
    def findings(self) -> list[TestCommandQualityFinding]:
        return [finding for task in self.tasks for finding in task.findings]

    def findings_by_severity(self) -> dict[str, list[TestCommandQualityFinding]]:
        return {
            "blocking": [
                finding for finding in self.findings if finding.severity == "blocking"
            ],
            "warning": [
                finding for finding in self.findings if finding.severity == "warning"
            ],
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "passed": self.passed,
            "summary": {
                "blocking": self.blocking_count,
                "warning": self.warning_count,
                "tasks": len(self.tasks),
            },
            "tasks": [task.to_dict() for task in self.tasks],
            "findings": [finding.to_dict() for finding in self.findings],
        }


def audit_test_command_quality(plan_dict: dict[str, Any]) -> TestCommandQualityResult:
    """Check execution task test_command values for specificity and safety."""
    plan_id = str(plan_dict.get("id") or "")
    tasks: list[TestCommandQualityTaskResult] = []

    for task in _list_of_dicts(plan_dict.get("tasks")):
        findings = _task_findings(task)
        tasks.append(
            TestCommandQualityTaskResult(
                task_id=str(task.get("id") or ""),
                title=str(task.get("title") or ""),
                findings=findings,
            )
        )

    return TestCommandQualityResult(plan_id=plan_id, tasks=tasks)


def _task_findings(task: dict[str, Any]) -> list[TestCommandQualityFinding]:
    task_id = str(task.get("id") or "")
    task_title = str(task.get("title") or "")
    command = _string_value(task.get("test_command"))
    files = _string_list(task.get("files_or_modules"))
    findings: list[TestCommandQualityFinding] = []

    if not command:
        return [
            TestCommandQualityFinding(
                severity="blocking",
                code="missing_test_command",
                task_id=task_id,
                task_title=task_title,
                field="test_command",
                message=f"Task {task_id} has no test command.",
                remediation="Add a focused validation command for this implementation task.",
            )
        ]

    if _has_destructive_pattern(command):
        findings.append(
            TestCommandQualityFinding(
                severity="blocking",
                code="destructive_test_command",
                task_id=task_id,
                task_title=task_title,
                field="test_command",
                value=command,
                message=f"Task {task_id} test command appears destructive.",
                remediation=(
                    "Replace destructive cleanup or reset commands with non-mutating "
                    "validation commands."
                ),
            )
        )

    if _has_interactive_pattern(command):
        findings.append(
            TestCommandQualityFinding(
                severity="blocking",
                code="interactive_test_command",
                task_id=task_id,
                task_title=task_title,
                field="test_command",
                value=command,
                message=f"Task {task_id} test command appears interactive.",
                remediation="Use a non-interactive command suitable for autonomous execution.",
            )
        )

    if not _has_validation_tool(command):
        findings.append(
            TestCommandQualityFinding(
                severity="blocking",
                code="non_validation_test_command",
                task_id=task_id,
                task_title=task_title,
                field="test_command",
                value=command,
                message=f"Task {task_id} test command does not include a validation tool.",
                remediation=(
                    "Use a recognizable test, lint, typecheck, build, dry-run, or "
                    "verification command."
                ),
            )
        )

    if _is_overbroad_for_single_test_file(command, task, files):
        findings.append(
            TestCommandQualityFinding(
                severity="warning",
                code="overbroad_test_command",
                task_id=task_id,
                task_title=task_title,
                field="test_command",
                value=command,
                message=(
                    f"Task {task_id} uses a repo-wide test command for one test file."
                ),
                remediation=(
                    "Run the specific test file named in files_or_modules instead of "
                    "the whole repository suite."
                ),
            )
        )

    return findings


def _has_destructive_pattern(command: str) -> bool:
    normalized = _normalized_command(command)
    return any(pattern in normalized for pattern in _DESTRUCTIVE_PATTERNS)


def _has_interactive_pattern(command: str) -> bool:
    normalized = f" {_normalized_command(command)} "
    return any(pattern in normalized for pattern in _INTERACTIVE_PATTERNS)


def _has_validation_tool(command: str) -> bool:
    normalized = _normalized_command(command)
    return any(pattern in normalized for pattern in _VALIDATION_PATTERNS)


def _is_overbroad_for_single_test_file(
    command: str,
    task: dict[str, Any],
    files: list[str],
) -> bool:
    if _normalized_command(str(task.get("risk_level") or "")) != "low":
        return False
    if len(files) != 1 or not _is_test_file(files[0]):
        return False
    normalized = _normalized_command(command)
    if files[0].lower() in normalized:
        return False
    return normalized in _REPO_WIDE_TEST_COMMANDS or normalized in {
        f"{base} -o addopts=''" for base in _PYTEST_REPO_WIDE_COMMANDS
    }


def _is_test_file(path: str) -> bool:
    name = PurePosixPath(path).name.lower()
    return (
        (name.startswith("test_") and name.endswith(".py"))
        or (name.endswith("_test.py"))
        or (name.endswith(".test.js"))
        or (name.endswith(".test.ts"))
        or (name.endswith(".spec.js"))
        or (name.endswith(".spec.ts"))
    )


def _normalized_command(command: str) -> str:
    return _WHITESPACE_RE.sub(" ", command.strip().lower())


def _string_value(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item.strip() for item in value if isinstance(item, str) and item.strip()]


def _list_of_dicts(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]
