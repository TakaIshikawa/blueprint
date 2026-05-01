"""Plan third-party license compliance review for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


LicenseReviewStatus = Literal[
    "license_review_required",
    "license_review_recommended",
    "license_review_not_needed",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_DEPENDENCY_ACTION_RE = re.compile(
    r"\b(?:add|adds|added|adding|introduce|introduces|introduced|install|installs|"
    r"installed|upgrade|upgrades|upgraded|update|updates|updated|bump|bumps|bumped|"
    r"replace|replaces|replaced|vendor|vendors|vendored|import|imports|imported)\b",
    re.IGNORECASE,
)
_DEPENDENCY_TEXT_RE = re.compile(
    r"\b(?:dependenc(?:y|ies)|third[- ]?party|external package|package|library|"
    r"module|sdk|client library|open[- ]?source|oss|npm|pip|poetry|cargo|gem|go mod|"
    r"base image|container image)\b",
    re.IGNORECASE,
)
_LICENSE_TEXT_RE = re.compile(
    r"\b(?:license|licence|licensing|legal|notice|notices|attribution|spdx|"
    r"copyright|copyleft|gpl|agpl|lgpl|mit|apache|bsd|mpl|epl)\b",
    re.IGNORECASE,
)
_DOC_TEST_PATH_RE = re.compile(
    r"(?:^|/)(?:docs?|documentation|test|tests|spec|specs|fixtures?)(?:/|$)|"
    r"(?:^|/)(?:README|CHANGELOG|CONTRIBUTING|TESTING)(?:\.[^/]*)?$|"
    r"(?:_test|\.test|\.spec)\.",
    re.IGNORECASE,
)
_LOCKFILE_NAMES = {
    "poetry.lock",
    "package-lock.json",
    "pnpm-lock.yaml",
    "yarn.lock",
    "go.sum",
    "Cargo.lock",
    "Gemfile.lock",
    "Pipfile.lock",
}
_MANIFEST_ECOSYSTEMS: tuple[tuple[str, str], ...] = (
    ("pyproject.toml", "python"),
    ("poetry.lock", "python"),
    ("requirements.txt", "python"),
    ("requirements.in", "python"),
    ("Pipfile", "python"),
    ("Pipfile.lock", "python"),
    ("setup.py", "python"),
    ("setup.cfg", "python"),
    ("package.json", "javascript"),
    ("package-lock.json", "javascript"),
    ("pnpm-lock.yaml", "javascript"),
    ("yarn.lock", "javascript"),
    ("go.mod", "go"),
    ("go.sum", "go"),
    ("Cargo.toml", "rust"),
    ("Cargo.lock", "rust"),
    ("Gemfile", "ruby"),
    ("Gemfile.lock", "ruby"),
    ("Dockerfile", "container"),
)
_ECOSYSTEM_ORDER = {
    "python": 0,
    "javascript": 1,
    "go": 2,
    "rust": 3,
    "ruby": 4,
    "container": 5,
    "vendored": 6,
}
_STATUS_ORDER: dict[LicenseReviewStatus, int] = {
    "license_review_required": 0,
    "license_review_recommended": 1,
    "license_review_not_needed": 2,
}


@dataclass(frozen=True, slots=True)
class TaskLicenseComplianceRecord:
    """License compliance review guidance for one execution task."""

    task_id: str
    title: str
    license_review_status: LicenseReviewStatus
    detected_ecosystems: tuple[str, ...] = field(default_factory=tuple)
    review_reasons: tuple[str, ...] = field(default_factory=tuple)
    required_evidence: tuple[str, ...] = field(default_factory=tuple)
    suggested_reviewer_roles: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "license_review_status": self.license_review_status,
            "detected_ecosystems": list(self.detected_ecosystems),
            "review_reasons": list(self.review_reasons),
            "required_evidence": list(self.required_evidence),
            "suggested_reviewer_roles": list(self.suggested_reviewer_roles),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskLicenseCompliancePlan:
    """Plan-level third-party license compliance review."""

    plan_id: str | None = None
    records: tuple[TaskLicenseComplianceRecord, ...] = field(default_factory=tuple)
    review_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "review_task_ids": list(self.review_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return license compliance records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render the license compliance plan as deterministic Markdown."""
        title = "# Task License Compliance Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.records:
            lines.extend(["", "No execution tasks were available for license review planning."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Status | Ecosystems | Reasons | Required Evidence | Reviewers |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{record.license_review_status} | "
                f"{_markdown_cell(', '.join(record.detected_ecosystems) or 'none')} | "
                f"{_markdown_cell('; '.join(record.review_reasons) or 'none')} | "
                f"{_markdown_cell('; '.join(record.required_evidence) or 'none')} | "
                f"{_markdown_cell(', '.join(record.suggested_reviewer_roles) or 'none')} |"
            )
        return "\n".join(lines)


def build_task_license_compliance_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskLicenseCompliancePlan:
    """Build third-party license review guidance for execution tasks."""
    plan_id, tasks = _source_payload(source)
    records = tuple(
        sorted(
            (_task_record(task, index) for index, task in enumerate(tasks, start=1)),
            key=lambda record: (
                _STATUS_ORDER[record.license_review_status],
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    review_task_ids = tuple(
        record.task_id
        for record in records
        if record.license_review_status != "license_review_not_needed"
    )
    status_counts = {
        status: sum(1 for record in records if record.license_review_status == status)
        for status in _STATUS_ORDER
    }
    ecosystem_counts = {
        ecosystem: sum(1 for record in records if ecosystem in record.detected_ecosystems)
        for ecosystem in _ECOSYSTEM_ORDER
    }
    return TaskLicenseCompliancePlan(
        plan_id=plan_id,
        records=records,
        review_task_ids=review_task_ids,
        summary={
            "task_count": len(tasks),
            "review_task_count": len(review_task_ids),
            "status_counts": status_counts,
            "ecosystem_counts": ecosystem_counts,
        },
    )


def analyze_task_license_compliance(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskLicenseCompliancePlan:
    """Compatibility alias for building license compliance plans."""
    return build_task_license_compliance_plan(source)


def summarize_task_license_compliance(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskLicenseCompliancePlan:
    """Compatibility alias for building license compliance plans."""
    return build_task_license_compliance_plan(source)


def task_license_compliance_plan_to_dict(
    result: TaskLicenseCompliancePlan,
) -> dict[str, Any]:
    """Serialize a task license compliance plan to a plain dictionary."""
    return result.to_dict()


task_license_compliance_plan_to_dict.__test__ = False


def task_license_compliance_plan_to_markdown(
    result: TaskLicenseCompliancePlan,
) -> str:
    """Render a task license compliance plan as Markdown."""
    return result.to_markdown()


task_license_compliance_plan_to_markdown.__test__ = False


def _task_record(task: Mapping[str, Any], index: int) -> TaskLicenseComplianceRecord:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    signals = _signals(task)
    ecosystems = tuple(
        ecosystem for ecosystem in _ECOSYSTEM_ORDER if ecosystem in signals.ecosystems
    )
    status = _review_status(signals, task)
    return TaskLicenseComplianceRecord(
        task_id=task_id,
        title=title,
        license_review_status=status,
        detected_ecosystems=ecosystems,
        review_reasons=_review_reasons(signals, status),
        required_evidence=_required_evidence(signals, status),
        suggested_reviewer_roles=_reviewer_roles(signals, status),
        evidence=tuple(_dedupe(signals.evidence)),
    )


@dataclass(frozen=True, slots=True)
class _Signals:
    ecosystems: frozenset[str] = frozenset()
    manifest_evidence: tuple[str, ...] = field(default_factory=tuple)
    lockfile_evidence: tuple[str, ...] = field(default_factory=tuple)
    vendored_evidence: tuple[str, ...] = field(default_factory=tuple)
    dependency_text_evidence: tuple[str, ...] = field(default_factory=tuple)
    license_text_evidence: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    @property
    def has_manifest(self) -> bool:
        return bool(self.manifest_evidence)

    @property
    def has_lockfile_only(self) -> bool:
        return bool(self.lockfile_evidence) and not self.manifest_evidence

    @property
    def has_vendored_code(self) -> bool:
        return bool(self.vendored_evidence)

    @property
    def has_dependency_text(self) -> bool:
        return bool(self.dependency_text_evidence)

    @property
    def has_license_text(self) -> bool:
        return bool(self.license_text_evidence)


def _signals(task: Mapping[str, Any]) -> _Signals:
    ecosystems: set[str] = set()
    manifest_evidence: list[str] = []
    lockfile_evidence: list[str] = []
    vendored_evidence: list[str] = []
    dependency_text_evidence: list[str] = []
    license_text_evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        path_evidence = f"files_or_modules: {path}"
        ecosystem = _manifest_ecosystem(normalized)
        if ecosystem:
            ecosystems.add(ecosystem)
            if _is_lockfile(normalized):
                lockfile_evidence.append(path_evidence)
            else:
                manifest_evidence.append(path_evidence)
        if _is_vendored_path(normalized):
            ecosystems.add("vendored")
            vendored_evidence.append(path_evidence)
        path_text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        if _DEPENDENCY_TEXT_RE.search(path_text):
            dependency_text_evidence.append(path_evidence)
        if _LICENSE_TEXT_RE.search(path_text):
            license_text_evidence.append(path_evidence)

    for source_field, text in _candidate_texts(task):
        if _DEPENDENCY_TEXT_RE.search(text):
            dependency_text_evidence.append(_evidence_snippet(source_field, text))
        if _LICENSE_TEXT_RE.search(text):
            license_text_evidence.append(_evidence_snippet(source_field, text))
        if _DEPENDENCY_ACTION_RE.search(text) and _DEPENDENCY_TEXT_RE.search(text):
            dependency_text_evidence.append(_evidence_snippet(source_field, text))

    evidence = _dedupe(
        [
            *manifest_evidence,
            *lockfile_evidence,
            *vendored_evidence,
            *dependency_text_evidence,
            *license_text_evidence,
        ]
    )
    return _Signals(
        ecosystems=frozenset(ecosystems),
        manifest_evidence=tuple(_dedupe(manifest_evidence)),
        lockfile_evidence=tuple(_dedupe(lockfile_evidence)),
        vendored_evidence=tuple(_dedupe(vendored_evidence)),
        dependency_text_evidence=tuple(_dedupe(dependency_text_evidence)),
        license_text_evidence=tuple(_dedupe(license_text_evidence)),
        evidence=tuple(evidence),
    )


def _review_status(signals: _Signals, task: Mapping[str, Any]) -> LicenseReviewStatus:
    if not signals.evidence:
        return "license_review_not_needed"
    if _is_doc_or_test_only(task) and not (
        signals.has_manifest
        or signals.has_lockfile_only
        or signals.has_vendored_code
        or signals.has_license_text
    ):
        return "license_review_not_needed"
    if signals.has_vendored_code or signals.has_license_text:
        return "license_review_required"
    if signals.has_manifest and _has_dependency_action(task):
        return "license_review_required"
    if signals.has_manifest and signals.has_dependency_text:
        return "license_review_required"
    if signals.has_lockfile_only or signals.has_manifest or signals.has_dependency_text:
        return "license_review_recommended"
    return "license_review_not_needed"


def _review_reasons(
    signals: _Signals,
    status: LicenseReviewStatus,
) -> tuple[str, ...]:
    if status == "license_review_not_needed":
        return ()
    reasons: list[str] = []
    if signals.has_vendored_code:
        reasons.append("Vendored or third-party source paths may require attribution, redistribution, and license compatibility review.")
    if signals.has_license_text:
        reasons.append("Task text explicitly mentions license, legal, NOTICE, attribution, SPDX, or named open-source licenses.")
    if signals.has_manifest:
        reasons.append("Package manifest or dependency definition files are in scope.")
    if signals.has_lockfile_only:
        reasons.append("Lockfile-only dependency changes can alter transitive package license obligations.")
    if signals.has_dependency_text:
        reasons.append("Task text references third-party dependencies, packages, libraries, SDKs, or external modules.")
    return tuple(_dedupe(reasons))


def _required_evidence(
    signals: _Signals,
    status: LicenseReviewStatus,
) -> tuple[str, ...]:
    if status == "license_review_not_needed":
        return ()
    evidence = [
        "Record package name, version, source, and resolved license for each added or changed third-party component.",
        "Attach dependency license scan output or package-manager license report for the final resolved dependency graph.",
    ]
    if signals.has_lockfile_only:
        evidence.append("Confirm the lockfile diff matches an approved manifest change or document why the transitive update is acceptable.")
    if signals.has_vendored_code:
        evidence.append("Attach upstream source URL, commit or release, license file, and NOTICE or attribution update for vendored code.")
    if signals.has_license_text or signals.has_vendored_code:
        evidence.append("Capture legal or open-source program approval for copyleft, restricted, unknown, or redistribution-sensitive licenses.")
    if signals.ecosystems and "container" in signals.ecosystems:
        evidence.append("Record base image name, digest, distribution license, and included package license scan results.")
    return tuple(_dedupe(evidence))


def _reviewer_roles(
    signals: _Signals,
    status: LicenseReviewStatus,
) -> tuple[str, ...]:
    if status == "license_review_not_needed":
        return ()
    roles = ["engineering owner", "open-source compliance reviewer"]
    if signals.has_license_text or signals.has_vendored_code:
        roles.append("legal reviewer")
    if signals.has_manifest or signals.has_lockfile_only:
        roles.append("security or dependency management reviewer")
    return tuple(_dedupe(roles))


def _has_dependency_action(task: Mapping[str, Any]) -> bool:
    return any(_DEPENDENCY_ACTION_RE.search(text) for _, text in _candidate_texts(task))


def _manifest_ecosystem(path_value: str) -> str | None:
    path = PurePosixPath(path_value)
    name = path.name
    for manifest_name, ecosystem in _MANIFEST_ECOSYSTEMS:
        if name == manifest_name:
            return ecosystem
    if name.startswith("requirements-") and name.endswith(".txt"):
        return "python"
    if name.endswith(".gemspec"):
        return "ruby"
    return None


def _is_lockfile(path_value: str) -> bool:
    return PurePosixPath(path_value).name in _LOCKFILE_NAMES


def _is_vendored_path(path_value: str) -> bool:
    parts = {part.casefold() for part in PurePosixPath(path_value).parts}
    return bool(
        parts
        & {
            "vendor",
            "vendors",
            "vendored",
            "third_party",
            "third-party",
            "thirdparty",
            "external",
            "externals",
        }
    )


def _is_doc_or_test_only(task: Mapping[str, Any]) -> bool:
    paths = _strings(task.get("files_or_modules") or task.get("files"))
    if not paths:
        return False
    return all(_DOC_TEST_PATH_RE.search(_normalized_path(path)) for path in paths)


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
    for field_name in ("acceptance_criteria", "tags", "labels", "notes"):
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
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                key_text = str(key).replace("_", " ")
                if _DEPENDENCY_TEXT_RE.search(key_text) or _LICENSE_TEXT_RE.search(key_text):
                    texts.append((field, f"{key_text}: {text}"))
            elif _DEPENDENCY_TEXT_RE.search(str(key).replace("_", " ")) or _LICENSE_TEXT_RE.search(
                str(key).replace("_", " ")
            ):
                texts.append((field, str(key)))
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


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _markdown_cell(value: str) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


__all__ = [
    "LicenseReviewStatus",
    "TaskLicenseCompliancePlan",
    "TaskLicenseComplianceRecord",
    "analyze_task_license_compliance",
    "build_task_license_compliance_plan",
    "summarize_task_license_compliance",
    "task_license_compliance_plan_to_dict",
    "task_license_compliance_plan_to_markdown",
]
