"""Environment readiness audit for execution task handoff."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from pathlib import PurePosixPath
from typing import Any, Literal


Severity = Literal["blocking", "warning"]

_COMMAND_RE = re.compile(
    r"\b("
    r"poetry\s+run|uv\s+run|pytest|tox|ruff|mypy|npm\s+(run\s+)?test|"
    r"pnpm\s+(run\s+)?test|yarn\s+test|make\s+test|go\s+test|cargo\s+test|"
    r"mvn\s+test|gradle\s+test|docker\s+compose|terraform\s+plan|"
    r"kubectl\b.+\b(dry-run|rollout\s+status)"
    r")\b",
    re.IGNORECASE,
)
_ENV_VAR_RE = re.compile(r"\b[A-Z][A-Z0-9]+_[A-Z0-9_]*\b")
_ENV_NOTE_RE = re.compile(
    r"\b(env(ironment)?\s*(var(iable)?s?)?|\.env|secret|credential|config var)\b",
    re.IGNORECASE,
)
_ROLLBACK_RE = re.compile(
    r"\b(rollback|roll back|revert|restore|backout|back out|undo|"
    r"previous version|disable flag)\b",
    re.IGNORECASE,
)
_VERIFICATION_RE = re.compile(
    r"\b(verify|verification|validate|validation|test|pytest|smoke|health check|"
    r"monitor|rollout status|dry-run|dry run|assert|pass)\b",
    re.IGNORECASE,
)

_SETUP_FILE_NAMES = {
    ".env.example",
    ".env.sample",
    ".env.template",
    ".nvmrc",
    ".python-version",
    "docker-compose.yml",
    "docker-compose.yaml",
    "dockerfile",
    "gemfile",
    "go.mod",
    "makefile",
    "package.json",
    "pnpm-lock.yaml",
    "poetry.lock",
    "pyproject.toml",
    "requirements.txt",
    "setup.cfg",
    "setup.py",
    "tox.ini",
    "uv.lock",
    "yarn.lock",
}
_SETUP_FILE_PREFIXES = (
    ".github/actions/",
    ".github/workflows/",
    "config/",
    "configs/",
    "deploy/",
    "deployment/",
    "docker/",
    "helm/",
    "infra/",
    "k8s/",
    "kubernetes/",
    "scripts/",
)
_RISK_FILE_PATTERNS = (
    re.compile(r"(^|/)\.github/workflows/[^/]+\.ya?ml$", re.IGNORECASE),
    re.compile(r"(^|/)\.gitlab-ci\.ya?ml$", re.IGNORECASE),
    re.compile(r"(^|/)azure-pipelines\.ya?ml$", re.IGNORECASE),
    re.compile(r"(^|/)circleci/config\.ya?ml$", re.IGNORECASE),
    re.compile(r"(^|/)jenkinsfile$", re.IGNORECASE),
    re.compile(r"(^|/)(deploy|deployment|helm|k8s|kubernetes|terraform|infra)/"),
    re.compile(r"(^|/)(dockerfile|docker-compose\.ya?ml)$", re.IGNORECASE),
    re.compile(r"(^|/)(fly\.toml|render\.ya?ml|app\.ya?ml)$", re.IGNORECASE),
)


@dataclass(frozen=True)
class EnvReadinessFinding:
    """A single environment readiness finding."""

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
class EnvReadinessTaskResult:
    """Environment readiness findings for one task."""

    task_id: str
    title: str
    findings: list[EnvReadinessFinding] = field(default_factory=list)

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
class EnvReadinessResult:
    """Environment readiness audit result for an execution plan."""

    plan_id: str
    tasks: list[EnvReadinessTaskResult] = field(default_factory=list)

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
    def findings(self) -> list[EnvReadinessFinding]:
        return [finding for task in self.tasks for finding in task.findings]

    def findings_by_severity(self) -> dict[str, list[EnvReadinessFinding]]:
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


def audit_env_readiness(plan_dict: dict[str, Any]) -> EnvReadinessResult:
    """Check whether tasks carry enough environment and verification context."""
    plan_id = str(plan_dict.get("id") or "")
    plan_context = _plan_context(plan_dict)
    tasks: list[EnvReadinessTaskResult] = []

    for task in _list_of_dicts(plan_dict.get("tasks")):
        findings = _task_findings(task, plan_context)
        tasks.append(
            EnvReadinessTaskResult(
                task_id=str(task.get("id") or ""),
                title=str(task.get("title") or ""),
                findings=findings,
            )
        )

    return EnvReadinessResult(plan_id=plan_id, tasks=tasks)


def _task_findings(
    task: dict[str, Any],
    plan_context: dict[str, Any],
) -> list[EnvReadinessFinding]:
    task_id = str(task.get("id") or "")
    task_title = str(task.get("title") or "")
    files = _string_list(task.get("files_or_modules"))
    metadata = task.get("metadata") if isinstance(task.get("metadata"), dict) else {}
    findings: list[EnvReadinessFinding] = []

    if not _has_setup_file_context(files, metadata, plan_context):
        findings.append(
            EnvReadinessFinding(
                severity="warning",
                code="missing_setup_files",
                task_id=task_id,
                task_title=task_title,
                field="files_or_modules",
                message=f"Task {task_id} does not identify setup or dependency files.",
                remediation=(
                    "Add relevant setup files such as pyproject.toml, package.json, "
                    ".env.example, Dockerfile, or document them in task metadata."
                ),
            )
        )

    if not _has_test_command(task, plan_context):
        findings.append(
            EnvReadinessFinding(
                severity="warning",
                code="missing_test_command",
                task_id=task_id,
                task_title=task_title,
                field="acceptance_criteria",
                message=f"Task {task_id} has no concrete test or verification command.",
                remediation=(
                    "Add a runnable command such as poetry run pytest, npm test, "
                    "make test, terraform plan, or document it in metadata."
                ),
            )
        )

    if not _has_env_var_notes(task, plan_context):
        findings.append(
            EnvReadinessFinding(
                severity="warning",
                code="missing_env_var_notes",
                task_id=task_id,
                task_title=task_title,
                field="metadata",
                message=f"Task {task_id} does not state required environment variables.",
                remediation=(
                    "List required environment variables, secrets, .env files, or "
                    "explicitly state that no environment variables are required."
                ),
            )
        )

    risky_files = [path for path in files if _is_ci_or_deploy_file(path)]
    if risky_files and not _has_rollback_criteria(task):
        findings.append(
            EnvReadinessFinding(
                severity="blocking",
                code="missing_rollback_criteria",
                task_id=task_id,
                task_title=task_title,
                field="acceptance_criteria",
                value=", ".join(risky_files),
                message=(
                    f"Task {task_id} touches CI or deploy files without rollback criteria."
                ),
                remediation=(
                    "Add acceptance criteria or metadata describing how to roll back, "
                    "revert, or disable the change if deployment fails."
                ),
            )
        )

    if risky_files and not _has_deploy_verification_criteria(task, plan_context):
        findings.append(
            EnvReadinessFinding(
                severity="blocking",
                code="missing_deploy_verification",
                task_id=task_id,
                task_title=task_title,
                field="acceptance_criteria",
                value=", ".join(risky_files),
                message=(
                    f"Task {task_id} touches CI or deploy files without verification criteria."
                ),
                remediation=(
                    "Add explicit verification such as a dry run, smoke test, health "
                    "check, rollout status check, or monitored validation command."
                ),
            )
        )

    return findings


def _plan_context(plan_dict: dict[str, Any]) -> dict[str, Any]:
    metadata = plan_dict.get("metadata") if isinstance(plan_dict.get("metadata"), dict) else {}
    return {
        "test_strategy": str(plan_dict.get("test_strategy") or ""),
        "metadata": metadata,
        "text": " ".join(
            [
                str(plan_dict.get("test_strategy") or ""),
                _flatten_metadata_text(metadata),
            ]
        ),
    }


def _has_setup_file_context(
    files: list[str],
    metadata: dict[str, Any],
    plan_context: dict[str, Any],
) -> bool:
    if any(_is_setup_file(path) for path in files):
        return True
    if _metadata_has_any(metadata, ("setup_files", "environment_files", "dependency_files")):
        return True
    return _metadata_has_any(
        plan_context["metadata"],
        ("setup_files", "environment_files", "dependency_files"),
    )


def _has_test_command(task: dict[str, Any], plan_context: dict[str, Any]) -> bool:
    text = " ".join(
        [
            str(task.get("description") or ""),
            " ".join(_string_list(task.get("acceptance_criteria"))),
            _flatten_metadata_text(task.get("metadata")),
            plan_context["test_strategy"],
        ]
    )
    return bool(_COMMAND_RE.search(text))


def _has_env_var_notes(task: dict[str, Any], plan_context: dict[str, Any]) -> bool:
    task_metadata = task.get("metadata") if isinstance(task.get("metadata"), dict) else {}
    if _metadata_has_any(task_metadata, ("env", "env_vars", "environment", "secrets")):
        return True
    if _metadata_has_any(
        plan_context["metadata"],
        ("env", "env_vars", "environment", "secrets"),
    ):
        return True

    text = " ".join(
        [
            str(task.get("description") or ""),
            " ".join(_string_list(task.get("acceptance_criteria"))),
            _flatten_metadata_text(task_metadata),
            plan_context["text"],
        ]
    )
    return bool(_ENV_NOTE_RE.search(text) or _ENV_VAR_RE.search(text))


def _has_rollback_criteria(task: dict[str, Any]) -> bool:
    text = " ".join(
        [
            " ".join(_string_list(task.get("acceptance_criteria"))),
            _flatten_metadata_text(task.get("metadata")),
        ]
    )
    return bool(_ROLLBACK_RE.search(text))


def _has_deploy_verification_criteria(
    task: dict[str, Any],
    plan_context: dict[str, Any],
) -> bool:
    text = " ".join(
        [
            " ".join(_string_list(task.get("acceptance_criteria"))),
            _flatten_metadata_text(task.get("metadata")),
            plan_context["test_strategy"],
        ]
    )
    return bool(_VERIFICATION_RE.search(text) or _COMMAND_RE.search(text))


def _is_setup_file(path: str) -> bool:
    normalized = _normalized_path(path)
    name = PurePosixPath(normalized).name.lower()
    return name in _SETUP_FILE_NAMES or normalized.startswith(_SETUP_FILE_PREFIXES)


def _is_ci_or_deploy_file(path: str) -> bool:
    normalized = _normalized_path(path)
    return any(pattern.search(normalized) for pattern in _RISK_FILE_PATTERNS)


def _normalized_path(path: str) -> str:
    normalized = str(path).replace("\\", "/").strip().lower()
    while normalized.startswith("./"):
        normalized = normalized[2:]
    return normalized


def _metadata_has_any(metadata: Any, keys: tuple[str, ...]) -> bool:
    if not isinstance(metadata, dict):
        return False
    wanted = {key.lower() for key in keys}
    for key, value in metadata.items():
        normalized_key = str(key).lower()
        if any(want in normalized_key for want in wanted) and _has_value(value):
            return True
    return False


def _flatten_metadata_text(value: Any) -> str:
    if isinstance(value, dict):
        return " ".join(
            str(part)
            for item in value.items()
            for part in (item[0], _flatten_metadata_text(item[1]))
            if _has_text(str(part))
        )
    if isinstance(value, list):
        return " ".join(_flatten_metadata_text(item) for item in value)
    if value is None:
        return ""
    return str(value)


def _has_value(value: Any) -> bool:
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, set)):
        return any(_has_value(item) for item in value)
    if isinstance(value, dict):
        return any(_has_value(item) for item in value.values())
    return value is not None


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if _has_text(item)]


def _list_of_dicts(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _has_text(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())
