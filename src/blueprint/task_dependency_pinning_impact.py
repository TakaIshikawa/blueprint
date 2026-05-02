"""Plan dependency pinning and reproducibility impact checks for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


DependencyPinningSurface = Literal[
    "package_manifest",
    "lockfile",
    "docker_image",
    "sdk_client",
    "runtime_version",
    "dependency_version",
]
DependencyPinningSignal = Literal[
    "version_bump",
    "major_upgrade",
    "lockfile_update",
    "transitive_dependency_drift",
    "runtime_image_change",
    "runtime_version_change",
    "sdk_client_upgrade",
]
DependencyPinningRisk = Literal["pinned", "floating", "major-upgrade", "transitive-drift"]
DependencyPinningCheck = Literal[
    "pinned_versions",
    "changelog_review",
    "compatibility_tests",
    "rollback_version",
    "security_advisory_review",
    "lockfile_reproducibility",
]
DependencyPinningImpactLevel = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SEMVER_RE = re.compile(r"\b[v^~]?\d+\.\d+(?:\.\d+)?(?:[-+][0-9A-Za-z.-]+)?\b")
_VERSION_TRANSITION_RE = re.compile(
    r"\b(?:from\s+)?v?(?P<from>\d+)(?:\.\d+){0,2}\s*(?:->|to)\s*v?(?P<to>\d+)(?:\.\d+){0,2}\b",
    re.I,
)
_IMAGE_VERSION_TRANSITION_RE = re.compile(
    r"\b[a-z0-9_.-]+:v?(?P<from>\d+)(?:[.\w-]*)\s*(?:->|to)\s*[a-z0-9_.-]+:v?(?P<to>\d+)(?:[.\w-]*)\b",
    re.I,
)
_VERSION_BUMP_RE = re.compile(
    r"\b(?:upgrade|bump|update|downgrade|migrate|move|change|refresh|regenerate|pin)\b.*"
    r"\b(?:dependency|dependencies|package|packages|library|libraries|sdk|client|runtime|version|lockfile|base image)\b|"
    r"\b(?:dependency|dependencies|package|packages|library|libraries|sdk|client|runtime|version|lockfile|base image)\b.*"
    r"\b(?:upgrade|bump|update|downgrade|migrate|move|change|refresh|regenerate|pin)\b",
    re.I,
)
_MAJOR_UPGRADE_RE = re.compile(r"\b(?:major|breaking|incompatible|upgrade\s+to\s+v?\d+|migrate\s+to\s+v?\d+)\b", re.I)
_LOCKFILE_RE = re.compile(
    r"\b(?:lockfile|lock file|poetry\.lock|uv\.lock|package-lock\.json|pnpm-lock\.yaml|"
    r"yarn\.lock|cargo\.lock|go\.sum|gemfile\.lock|composer\.lock|frozen-lockfile)\b",
    re.I,
)
_TRANSITIVE_RE = re.compile(
    r"\b(?:transitive|indirect|resolved dependency|dependency tree|lockfile churn|unrelated dependency churn|"
    r"subdependencies|sub-dependencies|refresh(?:ing)? dependencies)\b",
    re.I,
)
_RUNTIME_IMAGE_RE = re.compile(
    r"\b(?:docker base image|base image|container image|runtime image|from\s+[a-z0-9_.-]+:[a-z0-9_.-]+)\b",
    re.I,
)
_RUNTIME_RE = re.compile(r"\b(?:python|node(?:\.js)?|ruby|go|java|jdk|runtime)\s+v?\d+(?:\.\d+)?\b", re.I)
_SDK_RE = re.compile(r"\b(?:sdk|client library|generated client|api client|external client)\b", re.I)
_FLOATING_RE = re.compile(
    r"\b(?:floating|unpinned|unpin|latest|any version|version range|loose range|caret range|tilde range|"
    r"without lockfile reproducibility)\b|(?:(?:[<>]=?|[~^*])\s*v?\d+\.\d+)|(?:['\"]latest['\"])",
    re.I,
)
_LOCKFILE_UPDATE_CONTEXT_RE = re.compile(r"\b(?:update|refresh|regenerate|change|bump|install|resolve|drift|churn)\b", re.I)
_PINNED_RE = re.compile(
    r"\b(?:pin|pinned|exact version|fixed version|freeze|frozen|constraints?|"
    r"==\s*v?\d+\.\d+|rollback version|restore .*lockfile)\b",
    re.I,
)

_SURFACE_ORDER: tuple[DependencyPinningSurface, ...] = (
    "package_manifest",
    "lockfile",
    "docker_image",
    "sdk_client",
    "runtime_version",
    "dependency_version",
)
_SIGNAL_ORDER: tuple[DependencyPinningSignal, ...] = (
    "version_bump",
    "major_upgrade",
    "lockfile_update",
    "transitive_dependency_drift",
    "runtime_image_change",
    "runtime_version_change",
    "sdk_client_upgrade",
)
_RISK_ORDER: tuple[DependencyPinningRisk, ...] = ("pinned", "floating", "major-upgrade", "transitive-drift")
_CHECK_ORDER: tuple[DependencyPinningCheck, ...] = (
    "pinned_versions",
    "changelog_review",
    "compatibility_tests",
    "rollback_version",
    "security_advisory_review",
    "lockfile_reproducibility",
)
_IMPACT_ORDER: dict[DependencyPinningImpactLevel, int] = {"high": 0, "medium": 1, "low": 2}
_CHECK_PATTERNS: dict[DependencyPinningCheck, re.Pattern[str]] = {
    "pinned_versions": re.compile(r"\b(?:pin|pinned|exact version|fixed version|freeze|frozen|==\s*v?\d+\.\d+)\b", re.I),
    "changelog_review": re.compile(r"\b(?:changelog|change log|release notes?|migration guide|upgrade guide)\b", re.I),
    "compatibility_tests": re.compile(r"\b(?:compatibility tests?|regression tests?|integration tests?|smoke tests?|test matrix)\b", re.I),
    "rollback_version": re.compile(r"\b(?:rollback version|roll back|rollback|revert|restore previous|previous version|backout)\b", re.I),
    "security_advisory_review": re.compile(r"\b(?:security advisories|security advisory|cve|vulnerabilit(?:y|ies)|audit)\b", re.I),
    "lockfile_reproducibility": re.compile(
        r"\b(?:lockfile reproducibility|reproducible lockfile|frozen-lockfile|frozen lockfile|"
        r"poetry lock --no-update|npm ci|pnpm install --frozen-lockfile|yarn install --immutable)\b",
        re.I,
    ),
}
_CHECK_GUIDANCE: dict[DependencyPinningCheck, str] = {
    "pinned_versions": "Confirm every changed direct dependency, runtime, SDK, and image tag is pinned to an intentional version.",
    "changelog_review": "Review changelogs, release notes, and migration guides for behavior changes and breaking changes.",
    "compatibility_tests": "Run compatibility, integration, and regression tests against the resolved dependency set.",
    "rollback_version": "Record the prior dependency, SDK, runtime, or image version needed for rollback.",
    "security_advisory_review": "Check security advisories and vulnerability scans for both old and new resolved versions.",
    "lockfile_reproducibility": "Verify lockfile generation is reproducible and does not introduce unrelated transitive drift.",
}
_MANIFEST_NAMES = {
    "pyproject.toml",
    "requirements.txt",
    "requirements.in",
    "setup.py",
    "setup.cfg",
    "package.json",
    "cargo.toml",
    "go.mod",
    "gemfile",
    "composer.json",
    "pom.xml",
    "build.gradle",
    "build.gradle.kts",
}
_LOCKFILE_NAMES = {
    "poetry.lock",
    "uv.lock",
    "pipfile.lock",
    "package-lock.json",
    "pnpm-lock.yaml",
    "yarn.lock",
    "bun.lockb",
    "cargo.lock",
    "go.sum",
    "gemfile.lock",
    "composer.lock",
}
_RUNTIME_FILES = {".python-version", ".node-version", ".nvmrc", ".ruby-version", "runtime.txt", "mise.toml", ".tool-versions"}
_DOCKER_FILES = {"dockerfile", "docker-compose.yml", "docker-compose.yaml"}


@dataclass(frozen=True, slots=True)
class TaskDependencyPinningImpactRecord:
    """Dependency pinning and reproducibility guidance for one task."""

    task_id: str
    title: str
    impact_level: DependencyPinningImpactLevel
    dependency_surfaces: tuple[DependencyPinningSurface, ...] = field(default_factory=tuple)
    detected_signals: tuple[DependencyPinningSignal, ...] = field(default_factory=tuple)
    risk_classifications: tuple[DependencyPinningRisk, ...] = field(default_factory=tuple)
    present_checks: tuple[DependencyPinningCheck, ...] = field(default_factory=tuple)
    missing_checks: tuple[DependencyPinningCheck, ...] = field(default_factory=tuple)
    recommended_checks: tuple[DependencyPinningCheck, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "impact_level": self.impact_level,
            "dependency_surfaces": list(self.dependency_surfaces),
            "detected_signals": list(self.detected_signals),
            "risk_classifications": list(self.risk_classifications),
            "present_checks": list(self.present_checks),
            "missing_checks": list(self.missing_checks),
            "recommended_checks": list(self.recommended_checks),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskDependencyPinningImpactPlan:
    """Plan-level dependency pinning impact review."""

    plan_id: str | None = None
    records: tuple[TaskDependencyPinningImpactRecord, ...] = field(default_factory=tuple)
    pinning_task_ids: tuple[str, ...] = field(default_factory=tuple)
    ignored_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def recommendations(self) -> tuple[TaskDependencyPinningImpactRecord, ...]:
        """Compatibility view for callers that expose planner rows as recommendations."""
        return self.records

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "recommendations": [record.to_dict() for record in self.recommendations],
            "pinning_task_ids": list(self.pinning_task_ids),
            "ignored_task_ids": list(self.ignored_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return pinning impact records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render the dependency pinning impact plan as deterministic Markdown."""
        title = "# Task Dependency Pinning Impact"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        impact_counts = self.summary.get("impact_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Pinning task count: {self.summary.get('pinning_task_count', 0)}",
            f"- Missing check count: {self.summary.get('missing_check_count', 0)}",
            "- Impact counts: " + ", ".join(f"{level} {impact_counts.get(level, 0)}" for level in _IMPACT_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No dependency pinning impact records were inferred."])
            if self.ignored_task_ids:
                lines.extend(["", f"Ignored tasks: {_markdown_cell(', '.join(self.ignored_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Records",
                "",
                "| Task | Title | Impact | Surfaces | Signals | Risks | Missing Checks | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.impact_level} | "
                f"{_markdown_cell(', '.join(record.dependency_surfaces) or 'none')} | "
                f"{_markdown_cell(', '.join(record.detected_signals) or 'none')} | "
                f"{_markdown_cell(', '.join(record.risk_classifications) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_checks) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.ignored_task_ids:
            lines.extend(["", f"Ignored tasks: {_markdown_cell(', '.join(self.ignored_task_ids))}"])
        return "\n".join(lines)


def build_task_dependency_pinning_impact_plan(source: Any) -> TaskDependencyPinningImpactPlan:
    """Build dependency pinning impact records for execution tasks."""
    plan_id, tasks = _source_payload(source)
    candidates = [_record_for_task(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (_IMPACT_ORDER[record.impact_level], record.task_id, record.title.casefold()),
        )
    )
    ignored_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if candidates[index - 1] is None
    )
    return TaskDependencyPinningImpactPlan(
        plan_id=plan_id,
        records=records,
        pinning_task_ids=tuple(record.task_id for record in records),
        ignored_task_ids=ignored_task_ids,
        summary=_summary(records, task_count=len(tasks), ignored_task_ids=ignored_task_ids),
    )


def analyze_task_dependency_pinning_impact(source: Any) -> TaskDependencyPinningImpactPlan:
    """Compatibility alias for building dependency pinning impact plans."""
    return build_task_dependency_pinning_impact_plan(source)


def recommend_task_dependency_pinning_impact(source: Any) -> TaskDependencyPinningImpactPlan:
    """Compatibility alias for recommending dependency pinning checks."""
    return build_task_dependency_pinning_impact_plan(source)


def generate_task_dependency_pinning_impact(source: Any) -> TaskDependencyPinningImpactPlan:
    """Compatibility alias for generating dependency pinning impact plans."""
    return build_task_dependency_pinning_impact_plan(source)


def summarize_task_dependency_pinning_impact(source: Any) -> TaskDependencyPinningImpactPlan:
    """Compatibility alias for summarizing dependency pinning impact plans."""
    return build_task_dependency_pinning_impact_plan(source)


def extract_task_dependency_pinning_impact(source: Any) -> TaskDependencyPinningImpactPlan:
    """Compatibility alias for extracting dependency pinning impact plans."""
    return build_task_dependency_pinning_impact_plan(source)


def task_dependency_pinning_impact_plan_to_dict(result: TaskDependencyPinningImpactPlan) -> dict[str, Any]:
    """Serialize a dependency pinning impact plan to a plain dictionary."""
    return result.to_dict()


task_dependency_pinning_impact_plan_to_dict.__test__ = False


def task_dependency_pinning_impact_plan_to_dicts(
    result: TaskDependencyPinningImpactPlan | Iterable[TaskDependencyPinningImpactRecord],
) -> list[dict[str, Any]]:
    """Serialize dependency pinning impact records to plain dictionaries."""
    if isinstance(result, TaskDependencyPinningImpactPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_dependency_pinning_impact_plan_to_dicts.__test__ = False


def task_dependency_pinning_impact_plan_to_markdown(result: TaskDependencyPinningImpactPlan) -> str:
    """Render a dependency pinning impact plan as Markdown."""
    return result.to_markdown()


task_dependency_pinning_impact_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    surfaces: tuple[DependencyPinningSurface, ...] = field(default_factory=tuple)
    signals: tuple[DependencyPinningSignal, ...] = field(default_factory=tuple)
    risks: tuple[DependencyPinningRisk, ...] = field(default_factory=tuple)
    checks: tuple[DependencyPinningCheck, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)


def _record_for_task(task: Mapping[str, Any], index: int) -> TaskDependencyPinningImpactRecord | None:
    signals = _signals(task)
    if not signals.surfaces and not signals.signals:
        return None

    missing_checks = tuple(check for check in _CHECK_ORDER if check not in signals.checks)
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    return TaskDependencyPinningImpactRecord(
        task_id=task_id,
        title=title,
        impact_level=_impact_level(signals.signals, signals.risks, missing_checks),
        dependency_surfaces=signals.surfaces,
        detected_signals=signals.signals,
        risk_classifications=signals.risks,
        present_checks=signals.checks,
        missing_checks=missing_checks,
        recommended_checks=tuple(check for check in _CHECK_ORDER if check in missing_checks),
        evidence=signals.evidence,
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    surfaces: set[DependencyPinningSurface] = set()
    signals: set[DependencyPinningSignal] = set()
    risks: set[DependencyPinningRisk] = set()
    checks: set[DependencyPinningCheck] = set()
    evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        if _inspect_path(path, surfaces, signals):
            evidence.append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        matched = _inspect_text(text, surfaces, signals, risks, checks)
        if matched:
            evidence.append(_evidence_snippet(source_field, text))

    if "lockfile" in surfaces:
        signals.add("lockfile_update")
    if "docker_image" in surfaces:
        signals.add("runtime_image_change")
    if "runtime_version" in surfaces:
        signals.add("runtime_version_change")
    if "sdk_client" in surfaces and "version_bump" in signals:
        signals.add("sdk_client_upgrade")
    if "dependency_version" in surfaces and not risks:
        risks.add("pinned" if "pinned_versions" in checks else "floating")

    return _Signals(
        surfaces=tuple(surface for surface in _SURFACE_ORDER if surface in surfaces),
        signals=tuple(signal for signal in _SIGNAL_ORDER if signal in signals),
        risks=tuple(risk for risk in _RISK_ORDER if risk in risks),
        checks=tuple(check for check in _CHECK_ORDER if check in checks),
        evidence=tuple(_dedupe(evidence)),
    )


def _inspect_path(
    path: str,
    surfaces: set[DependencyPinningSurface],
    signals: set[DependencyPinningSignal],
) -> bool:
    normalized = _normalized_path(path)
    if not normalized:
        return False
    name = PurePosixPath(normalized).name.casefold()
    searchable = normalized.casefold().replace("/", " ").replace("_", " ").replace("-", " ")
    matched = False
    if name in _MANIFEST_NAMES:
        surfaces.add("package_manifest")
        matched = True
    if name in _LOCKFILE_NAMES:
        surfaces.add("lockfile")
        signals.add("lockfile_update")
        matched = True
    if name in _RUNTIME_FILES:
        surfaces.add("runtime_version")
        signals.add("runtime_version_change")
        matched = True
    if name in _DOCKER_FILES:
        surfaces.add("docker_image")
        signals.add("runtime_image_change")
        matched = True
    if any(token in searchable for token in (" sdk ", " client ", "generated client", "api client")):
        surfaces.add("sdk_client")
        matched = True
    return matched


def _inspect_text(
    text: str,
    surfaces: set[DependencyPinningSurface],
    signals: set[DependencyPinningSignal],
    risks: set[DependencyPinningRisk],
    checks: set[DependencyPinningCheck],
) -> bool:
    matched = False
    if _VERSION_BUMP_RE.search(text) or _SEMVER_RE.search(text):
        surfaces.add("dependency_version")
        signals.add("version_bump")
        matched = True
    if _MAJOR_UPGRADE_RE.search(text) or _has_major_version_transition(text):
        signals.add("major_upgrade")
        risks.add("major-upgrade")
        matched = True
    if _LOCKFILE_RE.search(text) and _LOCKFILE_UPDATE_CONTEXT_RE.search(text):
        surfaces.add("lockfile")
        signals.add("lockfile_update")
        matched = True
    if _TRANSITIVE_RE.search(text):
        signals.add("transitive_dependency_drift")
        risks.add("transitive-drift")
        matched = True
    if _RUNTIME_IMAGE_RE.search(text):
        surfaces.add("docker_image")
        signals.add("runtime_image_change")
        matched = True
    if _RUNTIME_RE.search(text):
        surfaces.add("runtime_version")
        signals.add("runtime_version_change")
        matched = True
    if _SDK_RE.search(text):
        surfaces.add("sdk_client")
        signals.add("sdk_client_upgrade")
        matched = True
    if _FLOATING_RE.search(text):
        risks.add("floating")
        matched = True
    if _PINNED_RE.search(text):
        risks.add("pinned")
        matched = True
    for check, pattern in _CHECK_PATTERNS.items():
        if pattern.search(text):
            if check == "lockfile_reproducibility" and re.search(r"\b(?:without|no|missing)\s+lockfile reproducibility\b", text, re.I):
                continue
            checks.add(check)
            matched = True
    return matched


def _impact_level(
    signals: tuple[DependencyPinningSignal, ...],
    risks: tuple[DependencyPinningRisk, ...],
    missing_checks: tuple[DependencyPinningCheck, ...],
) -> DependencyPinningImpactLevel:
    if "major_upgrade" in signals or "runtime_image_change" in signals or "major-upgrade" in risks:
        return "high"
    if "floating" in risks or "transitive-drift" in risks:
        return "high" if len(missing_checks) >= 3 else "medium"
    if any(signal in signals for signal in ("sdk_client_upgrade", "runtime_version_change", "lockfile_update")):
        return "medium"
    return "low"


def _summary(
    records: tuple[TaskDependencyPinningImpactRecord, ...],
    *,
    task_count: int,
    ignored_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "pinning_task_count": len(records),
        "ignored_task_ids": list(ignored_task_ids),
        "missing_check_count": sum(len(record.missing_checks) for record in records),
        "impact_counts": {
            level: sum(1 for record in records if record.impact_level == level)
            for level in _IMPACT_ORDER
        },
        "surface_counts": {
            surface: sum(1 for record in records if surface in record.dependency_surfaces)
            for surface in _SURFACE_ORDER
        },
        "signal_counts": {
            signal: sum(1 for record in records if signal in record.detected_signals)
            for signal in _SIGNAL_ORDER
        },
        "risk_counts": {
            risk: sum(1 for record in records if risk in record.risk_classifications)
            for risk in _RISK_ORDER
        },
        "present_check_counts": {
            check: sum(1 for record in records if check in record.present_checks)
            for check in _CHECK_ORDER
        },
        "missing_check_counts": {
            check: sum(1 for record in records if check in record.missing_checks)
            for check in _CHECK_ORDER
        },
    }


def _source_payload(source: Any) -> tuple[str | None, list[dict[str, Any]]]:
    if isinstance(source, ExecutionTask):
        return None, [source.model_dump(mode="python")]
    if isinstance(source, ExecutionPlan):
        return _optional_text(source.id), [task.model_dump(mode="python") for task in source.tasks]
    if isinstance(source, Mapping):
        if "tasks" in source:
            payload = _plan_payload(source)
            return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))
        return None, [dict(source)]
    if _looks_like_task(source):
        return None, [_object_payload(source)]
    if _looks_like_plan(source):
        payload = _object_payload(source)
        return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))
    try:
        iterator = iter(source)
    except TypeError:
        return None, []
    tasks: list[dict[str, Any]] = []
    for item in iterator:
        if task := _task_payload(item):
            tasks.append(task)
    return None, tasks


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan | object) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        if isinstance(plan, Mapping):
            return dict(plan)
        return _object_payload(plan)


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, (list, tuple, set)):
        return []
    items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
    tasks: list[dict[str, Any]] = []
    for item in items:
        if task := _task_payload(item):
            tasks.append(task)
    return tasks


def _task_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, ExecutionTask):
        return value.model_dump(mode="python")
    if hasattr(value, "model_dump"):
        task = value.model_dump(mode="python")
        return dict(task) if isinstance(task, Mapping) else {}
    if isinstance(value, Mapping):
        return dict(value)
    if _looks_like_task(value):
        return _object_payload(value)
    return {}


def _looks_like_plan(value: object) -> bool:
    return not isinstance(value, (str, bytes, bytearray)) and hasattr(value, "tasks")


def _looks_like_task(value: object) -> bool:
    return not isinstance(value, (str, bytes, bytearray)) and any(
        hasattr(value, field_name) for field_name in ("id", "title", "description")
    )


def _object_payload(value: object) -> dict[str, Any]:
    fields = (
        "id",
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "depends_on",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "estimated_complexity",
        "estimated_hours",
        "risk_level",
        "test_command",
        "status",
        "metadata",
        "blocked_reason",
        "tasks",
        "tags",
        "labels",
        "notes",
        "risks",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


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
    for field_name in ("acceptance_criteria", "tags", "labels", "notes", "risks", "depends_on"):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    for source_field, text in _metadata_texts(task.get("metadata")):
        texts.append((source_field, text))
    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            key_text = str(key).replace("_", " ").replace("-", " ")
            if _metadata_key_is_signal(key_text):
                texts.append((field, key_text))
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if _metadata_key_is_signal(key_text):
                    texts.append((field, f"{key_text}: {text}"))
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


def _metadata_key_is_signal(value: str) -> bool:
    return any(
        pattern.search(value)
        for pattern in [
            _VERSION_BUMP_RE,
            _LOCKFILE_RE,
            _TRANSITIVE_RE,
            _RUNTIME_IMAGE_RE,
            _RUNTIME_RE,
            _SDK_RE,
            *_CHECK_PATTERNS.values(),
        ]
    )


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id")) or f"task-{index}"


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


def _has_major_version_transition(text: str) -> bool:
    for match in _VERSION_TRANSITION_RE.finditer(text):
        if match.group("from") != match.group("to"):
            return True
    for match in _IMAGE_VERSION_TRANSITION_RE.finditer(text):
        if match.group("from") != match.group("to"):
            return True
    return False


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


def _markdown_cell(value: str) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[str] = set()
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
    "DependencyPinningCheck",
    "DependencyPinningImpactLevel",
    "DependencyPinningRisk",
    "DependencyPinningSignal",
    "DependencyPinningSurface",
    "TaskDependencyPinningImpactPlan",
    "TaskDependencyPinningImpactRecord",
    "analyze_task_dependency_pinning_impact",
    "build_task_dependency_pinning_impact_plan",
    "extract_task_dependency_pinning_impact",
    "generate_task_dependency_pinning_impact",
    "recommend_task_dependency_pinning_impact",
    "summarize_task_dependency_pinning_impact",
    "task_dependency_pinning_impact_plan_to_dict",
    "task_dependency_pinning_impact_plan_to_dicts",
    "task_dependency_pinning_impact_plan_to_markdown",
]
