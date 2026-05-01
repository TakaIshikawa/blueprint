"""Plan compatibility checks for dependency and runtime version changes."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


DependencyCompatibilityRisk = Literal["low", "medium", "high"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SEMVER_RE = re.compile(r"\b[v^~]?\d+\.\d+(?:\.\d+)?(?:[-+][0-9A-Za-z.-]+)?\b")
_MAJOR_UPGRADE_RE = re.compile(
    r"\b(?:major|breaking|incompatible|migrat(?:e|ion)\s+to\s+v?\d+|upgrade\s+to\s+v?\d+)\b",
    re.IGNORECASE,
)
_VERSION_TRANSITION_RE = re.compile(
    r"\bv?(?P<from>\d+)(?:\.\d+){0,2}\s*(?:->|to)\s*v?(?P<to>\d+)(?:\.\d+){0,2}\b",
    re.IGNORECASE,
)
_PATCH_UPDATE_RE = re.compile(r"\b(?:patch|hotfix|bugfix|security patch)\b", re.IGNORECASE)
_MINOR_UPDATE_RE = re.compile(r"\b(?:minor|feature release)\b", re.IGNORECASE)
_DEPRECATION_RE = re.compile(r"\b(?:deprecated|deprecation|removed|removal|end[- ]of[- ]life|eol)\b", re.I)
_SDK_RE = re.compile(r"\b(?:sdk|client library|library api|external library api|generated client)\b", re.I)
_PACKAGE_MANAGER_RE = re.compile(r"\b(?:poetry|pip|pipenv|uv|npm|pnpm|yarn|bun|cargo|go modules?|maven|gradle)\b", re.I)
_RUNTIME_RE = re.compile(r"\b(?:python|node(?:\.js)?|ruby|go|java|jdk|runtime)\s+v?\d+(?:\.\d+)?\b", re.I)
_DEPENDENCY_ACTION_RE = re.compile(
    r"\b(?:upgrade|upgrad(?:e|ing)|bump|pin|unpin|downgrade|update|migrate|"
    r"replace|remove|add)\b.*\b(?:dependency|dependencies|package|packages|library|libraries|sdk|runtime)\b|"
    r"\b(?:dependency|dependencies|package|packages|library|libraries|sdk|runtime)\b.*"
    r"\b(?:upgrade|bump|pin|downgrade|update|migration|migrate|replace|remove|add)\b",
    re.I,
)
_VERSIONED_NAME_RE = re.compile(
    r"\b(?P<name>@?[A-Za-z][A-Za-z0-9_.@/-]{1,80})\s*(?:from\s+)?"
    r"(?:v?\d+\.\d+(?:\.\d+)?|v?\d+)\s*(?:->|to)\s*(?:v?\d+\.\d+(?:\.\d+)?|v?\d+)\b",
    re.I,
)
_NAMED_DEP_RE = re.compile(
    r"\b(?:upgrade|bump|pin|downgrade|update|migrate|replace|remove|add)\s+"
    r"(?P<name>@?[A-Za-z][A-Za-z0-9_.@/-]{1,80})\b",
    re.I,
)
_COMMAND_RE = re.compile(
    r"\b(?:poetry|pytest|python|npm|pnpm|yarn|bun|pip|uv|cargo|go|mvn|gradle)\s+[^.;\n]+",
    re.I,
)
_MANIFEST_NAMES = {
    "pyproject.toml": "python package manifest",
    "requirements.txt": "python package manifest",
    "requirements.in": "python package manifest",
    "setup.py": "python package manifest",
    "setup.cfg": "python package manifest",
    "package.json": "node package manifest",
    "cargo.toml": "rust package manifest",
    "go.mod": "go module manifest",
    "gemfile": "ruby package manifest",
    "composer.json": "php package manifest",
    "pom.xml": "java package manifest",
    "build.gradle": "java package manifest",
    "build.gradle.kts": "java package manifest",
}
_LOCKFILE_NAMES = {
    "poetry.lock": "poetry lockfile",
    "uv.lock": "uv lockfile",
    "pipfile.lock": "pipenv lockfile",
    "package-lock.json": "npm lockfile",
    "pnpm-lock.yaml": "pnpm lockfile",
    "yarn.lock": "yarn lockfile",
    "bun.lockb": "bun lockfile",
    "cargo.lock": "rust lockfile",
    "go.sum": "go checksum lockfile",
    "gemfile.lock": "ruby lockfile",
    "composer.lock": "php lockfile",
}
_RUNTIME_FILES = {
    ".python-version",
    ".node-version",
    ".nvmrc",
    ".ruby-version",
    "runtime.txt",
    "dockerfile",
    "docker-compose.yml",
    "docker-compose.yaml",
    "mise.toml",
    ".tool-versions",
}
_DEPENDENCY_NAME_STOPWORDS = {
    "add",
    "apply",
    "back",
    "deprecated",
    "dependency",
    "dependencies",
    "library",
    "major",
    "minor",
    "package",
    "patch",
    "runtime",
    "sdk",
    "update",
    "upgrade",
}


@dataclass(frozen=True, slots=True)
class TaskDependencyVersionImpact:
    """Compatibility planning guidance for one dependency-sensitive task."""

    task_id: str
    task_title: str
    affected_dependency_surfaces: tuple[str, ...]
    dependency_names: tuple[str, ...]
    compatibility_risk: DependencyCompatibilityRisk
    required_checks: tuple[str, ...]
    validation_commands: tuple[str, ...]
    rollback_notes: tuple[str, ...]
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "task_title": self.task_title,
            "affected_dependency_surfaces": list(self.affected_dependency_surfaces),
            "dependency_names": list(self.dependency_names),
            "compatibility_risk": self.compatibility_risk,
            "required_checks": list(self.required_checks),
            "validation_commands": list(self.validation_commands),
            "rollback_notes": list(self.rollback_notes),
            "evidence": list(self.evidence),
        }


def generate_task_dependency_version_impact(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> tuple[TaskDependencyVersionImpact, ...]:
    """Return dependency-version compatibility records for relevant execution tasks."""
    _, tasks = _source_payload(source)
    records = [
        record
        for index, task in enumerate(tasks, start=1)
        if (record := _record_for_task(task, index)) is not None
    ]
    return tuple(sorted(records, key=lambda record: (record.task_id, record.task_title)))


def task_dependency_version_impacts_to_dicts(
    records: tuple[TaskDependencyVersionImpact, ...] | list[TaskDependencyVersionImpact],
) -> list[dict[str, Any]]:
    """Serialize dependency-version impact records to dictionaries."""
    return [record.to_dict() for record in records]


def _record_for_task(task: Mapping[str, Any], index: int) -> TaskDependencyVersionImpact | None:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    surfaces: list[str] = []
    dependency_names: list[str] = []
    evidence: list[str] = []
    risk_signals: set[str] = set()

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        _inspect_path(path, surfaces, evidence, risk_signals)

    for source_field, text in _candidate_texts(task):
        _inspect_text(source_field, text, surfaces, dependency_names, evidence, risk_signals)

    if not surfaces and not evidence:
        return None

    risk = _compatibility_risk(risk_signals)
    return TaskDependencyVersionImpact(
        task_id=task_id,
        task_title=title,
        affected_dependency_surfaces=tuple(_dedupe(surfaces)),
        dependency_names=tuple(_dedupe(dependency_names)),
        compatibility_risk=risk,
        required_checks=tuple(_required_checks(surfaces, dependency_names, risk_signals)),
        validation_commands=tuple(_validation_commands(task)),
        rollback_notes=tuple(_rollback_notes(surfaces, risk_signals, task)),
        evidence=tuple(_dedupe(evidence)),
    )


def _inspect_path(path: str, surfaces: list[str], evidence: list[str], risk_signals: set[str]) -> None:
    normalized = _normalized_path(path)
    if not normalized:
        return
    name = PurePosixPath(normalized).name.casefold()
    surface = _MANIFEST_NAMES.get(name)
    if surface:
        surfaces.append(surface)
        evidence.append(f"files_or_modules: {path}")
        risk_signals.add("manifest")
    surface = _LOCKFILE_NAMES.get(name)
    if surface:
        surfaces.append(surface)
        evidence.append(f"files_or_modules: {path}")
        risk_signals.add("lockfile")
    if name in _RUNTIME_FILES:
        surfaces.append("runtime version")
        evidence.append(f"files_or_modules: {path}")
        risk_signals.add("runtime")


def _inspect_text(
    source_field: str,
    text: str,
    surfaces: list[str],
    dependency_names: list[str],
    evidence: list[str],
    risk_signals: set[str],
) -> None:
    lowered = text.casefold()
    matched = False
    if _DEPENDENCY_ACTION_RE.search(text) or _SEMVER_RE.search(text):
        surfaces.append("dependency version")
        matched = True
    if _SDK_RE.search(text):
        surfaces.append("sdk or external library api")
        risk_signals.add("sdk")
        matched = True
    if _PACKAGE_MANAGER_RE.search(text):
        surfaces.append("package manager")
        matched = True
    if _RUNTIME_RE.search(text) or "runtime upgrade" in lowered:
        surfaces.append("runtime version")
        risk_signals.add("runtime")
        matched = True
    if _MAJOR_UPGRADE_RE.search(text):
        risk_signals.add("major")
        matched = True
    if _has_major_version_transition(text):
        risk_signals.add("major")
        matched = True
    if _DEPRECATION_RE.search(text):
        risk_signals.add("deprecation")
        matched = True
    if _PATCH_UPDATE_RE.search(text):
        risk_signals.add("patch")
        matched = True
    if _MINOR_UPDATE_RE.search(text):
        risk_signals.add("minor")
        matched = True

    for match in _VERSIONED_NAME_RE.finditer(text):
        dependency_names.append(_clean_dependency_name(match.group("name")))
    for match in _NAMED_DEP_RE.finditer(text):
        dependency_names.append(_clean_dependency_name(match.group("name")))

    if matched:
        evidence.append(_evidence_snippet(source_field, text))


def _compatibility_risk(risk_signals: set[str]) -> DependencyCompatibilityRisk:
    if bool({"major", "runtime", "deprecation"} & risk_signals):
        return "high"
    if bool({"manifest", "lockfile", "sdk", "minor"} & risk_signals):
        return "medium"
    return "low"


def _required_checks(
    surfaces: list[str],
    dependency_names: list[str],
    risk_signals: set[str],
) -> list[str]:
    checks = [
        "Review upstream release notes for behavior changes, fixed vulnerabilities, and known regressions.",
        "Confirm the resolved dependency tree and lockfile diff match the intended upgrade scope.",
    ]
    if dependency_names:
        checks.append(
            "Check migration guides for affected dependencies: " + ", ".join(_dedupe(dependency_names)) + "."
        )
    else:
        checks.append("Check migration guides for any upgraded dependencies, SDKs, package managers, or runtimes.")
    if bool({"major", "runtime", "deprecation", "sdk"} & risk_signals):
        checks.append("Verify breaking changes, deprecated APIs, runtime support windows, and transitive compatibility.")
    if any("lockfile" in surface for surface in surfaces):
        checks.append("Ensure lockfile regeneration is deterministic and does not introduce unrelated dependency churn.")
    return _dedupe(checks)


def _validation_commands(task: Mapping[str, Any]) -> list[str]:
    commands: list[str] = []
    if text := _optional_text(task.get("test_command")):
        commands.append(text)
    for source_field, text in _candidate_texts(task):
        if source_field == "test_command":
            continue
        if "command" in source_field.casefold() or "validation" in source_field.casefold():
            commands.extend(_command_candidates(text))
        elif re.search(r"\b(?:run|execute|validate with)\b", text, re.I):
            commands.extend(_command_candidates(text))
    return _dedupe(commands)


def _command_candidates(text: str) -> list[str]:
    return [_clean_command(match.group(0)) for match in _COMMAND_RE.finditer(text)]


def _rollback_notes(
    surfaces: list[str],
    risk_signals: set[str],
    task: Mapping[str, Any],
) -> list[str]:
    notes = [
        "Keep the previous manifest and lockfile entries available so the dependency change can be reverted cleanly."
    ]
    if any("runtime" in surface for surface in surfaces) or "runtime" in risk_signals:
        notes.append("Document how to restore the previous runtime or SDK version and redeploy with the old toolchain.")
    if "major" in risk_signals or "deprecation" in risk_signals:
        notes.append("Identify code paths changed for the upgrade so API migrations can be backed out with the version pin.")
    for source_field, text in _candidate_texts(task):
        if "rollback" in source_field.casefold() or re.search(r"\b(?:rollback|roll back|backout|revert)\b", text, re.I):
            notes.append(_evidence_snippet(source_field, text))
    return _dedupe(notes)


def _candidate_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "risk_level",
        "test_command",
        "blocked_reason",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in ("acceptance_criteria", "tags", "labels"):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    for source_field, text in _metadata_texts(task.get("metadata")):
        texts.append((source_field, text))
    return texts


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> tuple[str | None, list[dict[str, Any]]]:
    if isinstance(source, ExecutionTask):
        return None, [source.model_dump(mode="python")]
    if isinstance(source, ExecutionPlan):
        return _optional_text(source.id), [task.model_dump(mode="python") for task in source.tasks]
    if isinstance(source, Mapping):
        if "tasks" in source:
            payload = _plan_payload(source)
            return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))
        return None, [dict(source)]
    try:
        iterator = iter(source)
    except TypeError:
        return None, []
    tasks: list[dict[str, Any]] = []
    for item in iterator:
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return None, tasks


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        return dict(plan) if isinstance(plan, Mapping) else {}


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, (list, tuple, set)):
        return []
    items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
    tasks: list[dict[str, Any]] = []
    for item in items:
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts = []
        for index, item in enumerate(items):
            field = f"{prefix}[{index}]"
            if isinstance(item, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(item, field))
            elif text := _optional_text(item):
                texts.append((field, text))
        return texts
    text = _optional_text(value)
    return [(prefix, text)] if text else []


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = _optional_text(value)
        return [text] if text else []
    if isinstance(value, Mapping):
        strings: list[str] = []
        for key in sorted(value, key=lambda item: str(item)):
            strings.extend(_strings(value[key]))
        return strings
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        strings: list[str] = []
        for item in items:
            strings.extend(_strings(item))
        return strings
    text = _optional_text(value)
    return [text] if text else []


def _normalized_path(value: str) -> str:
    return value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")


def _clean_dependency_name(value: str) -> str:
    name = value.strip().strip("`'\",;:()[]{}").rstrip(".")
    if name.casefold() in _DEPENDENCY_NAME_STOPWORDS:
        return ""
    return name.casefold()


def _has_major_version_transition(text: str) -> bool:
    for match in _VERSION_TRANSITION_RE.finditer(text):
        if match.group("from") != match.group("to"):
            return True
    return False


def _clean_command(value: str) -> str:
    return value.strip().strip("`'\",;:()[]{}").rstrip(".")


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None or isinstance(value, (bytes, bytearray)):
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value:
            continue
        key = str(value).casefold()
        if key in seen:
            continue
        deduped.append(value)
        seen.add(key)
    return deduped


__all__ = [
    "DependencyCompatibilityRisk",
    "TaskDependencyVersionImpact",
    "generate_task_dependency_version_impact",
    "task_dependency_version_impacts_to_dicts",
]
