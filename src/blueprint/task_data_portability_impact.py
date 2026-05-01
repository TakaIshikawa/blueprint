"""Identify task-level user data portability impact risks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


DataPortabilityRiskLevel = Literal["high", "medium", "low"]
DataPortabilitySurface = Literal[
    "user_data_export",
    "account_transfer",
    "portable_archive",
    "structured_file_export",
    "gdpr_access_request",
    "migration_out",
    "self_service_copy",
]
DataPortabilitySafeguard = Literal[
    "format_contract",
    "authorization_check",
    "pii_redaction",
    "async_delivery",
    "audit_logging",
    "expiration_policy",
    "large_export_handling",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_RISK_ORDER: dict[DataPortabilityRiskLevel, int] = {"high": 0, "medium": 1, "low": 2}
_SURFACE_ORDER: dict[DataPortabilitySurface, int] = {
    "user_data_export": 0,
    "account_transfer": 1,
    "portable_archive": 2,
    "structured_file_export": 3,
    "gdpr_access_request": 4,
    "migration_out": 5,
    "self_service_copy": 6,
}
_SAFEGUARD_ORDER: dict[DataPortabilitySafeguard, int] = {
    "format_contract": 0,
    "authorization_check": 1,
    "pii_redaction": 2,
    "async_delivery": 3,
    "audit_logging": 4,
    "expiration_policy": 5,
    "large_export_handling": 6,
}
_SURFACE_PATTERNS: dict[DataPortabilitySurface, re.Pattern[str]] = {
    "user_data_export": re.compile(
        r"\b(?:export my data|export user data|export customer data|download my data|data export)\b",
        re.I,
    ),
    "account_transfer": re.compile(r"\b(?:account transfer|transfer account|account handoff)\b", re.I),
    "portable_archive": re.compile(r"\b(?:download archive|data archive|export archive|zip archive)\b", re.I),
    "structured_file_export": re.compile(r"\b(?:csv export|export csv|json export|export json)\b", re.I),
    "gdpr_access_request": re.compile(
        r"\b(?:gdpr data access|gdpr access|data subject access|dsar|data access request)\b",
        re.I,
    ),
    "migration_out": re.compile(r"\b(?:migration out|migrate out|data portability|portability export)\b", re.I),
    "self_service_copy": re.compile(
        r"\b(?:self[- ]service data copy|self[- ]service export|copy my data|export my data)\b",
        re.I,
    ),
}
_PATH_PATTERNS: dict[DataPortabilitySurface, re.Pattern[str]] = {
    "user_data_export": re.compile(r"user[_-]?data[_-]?export|customer[_-]?data[_-]?export|data[_-]?export", re.I),
    "account_transfer": re.compile(r"account[_-]?transfer|transfer[_-]?account", re.I),
    "portable_archive": re.compile(r"download[_-]?archive|export[_-]?archive|data[_-]?archive", re.I),
    "structured_file_export": re.compile(r"(?:csv|json)[_-]?export|export[_-]?(?:csv|json)", re.I),
    "gdpr_access_request": re.compile(r"gdpr|dsar|data[_-]?access[_-]?request", re.I),
    "migration_out": re.compile(r"migration[_-]?out|migrate[_-]?out|portability", re.I),
    "self_service_copy": re.compile(r"self[_-]?service[_-]?(?:data[_-]?)?(?:copy|export)", re.I),
}
_USER_DATA_RE = re.compile(
    r"\b(?:user|users|customer|customers|account|accounts|profile|profiles|member|members|"
    r"personal data|personally identifiable|pii|data subject|my data|customer data|user data)\b",
    re.I,
)
_AMBIGUOUS_EXPORT_RE = re.compile(r"\b(?:csv export|export csv|json export|export json|report export|admin export)\b", re.I)
_GENERIC_REPORTING_RE = re.compile(
    r"\b(?:admin|report|reports|reporting|analytics|dashboard|metrics|aggregate|summary|finance|ops)\b",
    re.I,
)
_SAFEGUARD_PATTERNS: dict[DataPortabilitySafeguard, re.Pattern[str]] = {
    "format_contract": re.compile(
        r"\b(?:format contract|schema|csv schema|json schema|manifest|versioned format|field mapping|export contract)\b",
        re.I,
    ),
    "authorization_check": re.compile(
        r"\b(?:authorization check|authorisation check|authz|permission check|ownership check|"
        r"verify owner|access control|authenticated user|requester authorization)\b",
        re.I,
    ),
    "pii_redaction": re.compile(
        r"\b(?:pii redaction|redact|redaction|mask|masking|sensitive field|data minimization|"
        r"exclude secrets?|privacy filter)\b",
        re.I,
    ),
    "async_delivery": re.compile(
        r"\b(?:async delivery|asynchronous|background job|queue|queued|worker|email when ready|"
        r"notify when ready|out-of-band delivery)\b",
        re.I,
    ),
    "audit_logging": re.compile(r"\b(?:audit log|audit event|audit trail|access log|activity log|security log)\b", re.I),
    "expiration_policy": re.compile(
        r"\b(?:expiration policy|expires?|expiry|ttl|time[- ]limited|signed url|download window|retention window)\b",
        re.I,
    ),
    "large_export_handling": re.compile(
        r"\b(?:large export|bulk export|chunk|chunking|pagination|streaming|batch size|rate limit|"
        r"throttle|resume|backpressure|size limit)\b",
        re.I,
    ),
}


@dataclass(frozen=True, slots=True)
class TaskDataPortabilityImpactFinding:
    """Data portability guidance for one affected execution task."""

    task_id: str
    title: str
    impacted_surfaces: tuple[DataPortabilitySurface, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[DataPortabilitySafeguard, ...] = field(default_factory=tuple)
    risk_level: DataPortabilityRiskLevel = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "impacted_surfaces": list(self.impacted_surfaces),
            "missing_safeguards": list(self.missing_safeguards),
            "risk_level": self.risk_level,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskDataPortabilityImpactPlan:
    """Plan-level summary of user data portability impact."""

    plan_id: str | None = None
    findings: tuple[TaskDataPortabilityImpactFinding, ...] = field(default_factory=tuple)
    impacted_task_ids: tuple[str, ...] = field(default_factory=tuple)
    ignored_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "findings": [finding.to_dict() for finding in self.findings],
            "impacted_task_ids": list(self.impacted_task_ids),
            "ignored_task_ids": list(self.ignored_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return data portability findings as plain dictionaries."""
        return [finding.to_dict() for finding in self.findings]

    @property
    def records(self) -> tuple[TaskDataPortabilityImpactFinding, ...]:
        """Compatibility view matching planners that name task findings records."""
        return self.findings


def build_task_data_portability_impact_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskDataPortabilityImpactPlan:
    """Build task-level user data portability recommendations."""
    plan_id, tasks = _source_payload(source)
    findings = tuple(
        sorted(
            (
                finding
                for index, task in enumerate(tasks, start=1)
                if (finding := _finding_for_task(task, index)) is not None
            ),
            key=lambda finding: (
                _RISK_ORDER[finding.risk_level],
                -len(finding.missing_safeguards),
                finding.task_id,
                finding.title.casefold(),
            ),
        )
    )
    impacted_task_ids = tuple(finding.task_id for finding in findings)
    impacted_task_id_set = set(impacted_task_ids)
    ignored_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if _task_id(task, index) not in impacted_task_id_set
    )
    return TaskDataPortabilityImpactPlan(
        plan_id=plan_id,
        findings=findings,
        impacted_task_ids=impacted_task_ids,
        ignored_task_ids=ignored_task_ids,
        summary=_summary(findings, total_task_count=len(tasks), ignored_task_count=len(ignored_task_ids)),
    )


def recommend_task_data_portability_impact(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> tuple[TaskDataPortabilityImpactFinding, ...]:
    """Return user data portability recommendations for relevant execution tasks."""
    return build_task_data_portability_impact_plan(source).findings


def summarize_task_data_portability_impact(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskDataPortabilityImpactPlan:
    """Compatibility alias for building data portability impact plans."""
    return build_task_data_portability_impact_plan(source)


def task_data_portability_impact_plan_to_dict(
    result: TaskDataPortabilityImpactPlan,
) -> dict[str, Any]:
    """Serialize a data portability impact plan to a plain dictionary."""
    return result.to_dict()


task_data_portability_impact_plan_to_dict.__test__ = False


def _finding_for_task(
    task: Mapping[str, Any],
    index: int,
) -> TaskDataPortabilityImpactFinding | None:
    surfaces: dict[DataPortabilitySurface, list[str]] = {}
    safeguards: set[DataPortabilitySafeguard] = set()
    has_user_data_signal = False
    has_ambiguous_export_signal = False
    has_generic_reporting_signal = False

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        path_user_signal = bool(_USER_DATA_RE.search(path))
        has_user_data_signal = has_user_data_signal or path_user_signal
        _inspect_path(path, surfaces)

    for source_field, text in _candidate_texts(task):
        has_user_data_signal = has_user_data_signal or bool(_USER_DATA_RE.search(text))
        has_ambiguous_export_signal = has_ambiguous_export_signal or bool(_AMBIGUOUS_EXPORT_RE.search(text))
        has_generic_reporting_signal = has_generic_reporting_signal or bool(_GENERIC_REPORTING_RE.search(text))
        _inspect_text(source_field, text, surfaces, safeguards)

    if not surfaces:
        return None
    if (
        has_generic_reporting_signal
        and not has_user_data_signal
        and not any(
            surface in surfaces
            for surface in ("account_transfer", "gdpr_access_request", "migration_out", "self_service_copy")
        )
    ):
        return None
    if has_ambiguous_export_signal and has_generic_reporting_signal and not has_user_data_signal:
        return None
    if tuple(surfaces) == ("structured_file_export",) and not has_user_data_signal:
        return None

    impacted_surfaces = tuple(surface for surface in _SURFACE_ORDER if surface in surfaces)
    missing_safeguards = tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard not in safeguards)
    task_id = _task_id(task, index)
    return TaskDataPortabilityImpactFinding(
        task_id=task_id,
        title=_optional_text(task.get("title")) or task_id,
        impacted_surfaces=impacted_surfaces,
        missing_safeguards=missing_safeguards,
        risk_level=_risk_level(impacted_surfaces, missing_safeguards),
        evidence=tuple(
            _dedupe(
                evidence
                for surface in impacted_surfaces
                for evidence in surfaces.get(surface, [])
            )
        ),
    )


def _inspect_path(
    path: str,
    surfaces: dict[DataPortabilitySurface, list[str]],
) -> None:
    normalized = _normalized_path(path)
    if not normalized:
        return
    evidence = f"files_or_modules: {path}"
    searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    for surface, pattern in _PATH_PATTERNS.items():
        if pattern.search(normalized) or pattern.search(searchable):
            surfaces.setdefault(surface, []).append(evidence)


def _inspect_text(
    source_field: str,
    text: str,
    surfaces: dict[DataPortabilitySurface, list[str]],
    safeguards: set[DataPortabilitySafeguard],
) -> None:
    evidence = _evidence_snippet(source_field, text)
    for surface, pattern in _SURFACE_PATTERNS.items():
        if pattern.search(text):
            surfaces.setdefault(surface, []).append(evidence)
    for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
        if pattern.search(text):
            safeguards.add(safeguard)


def _risk_level(
    impacted_surfaces: tuple[DataPortabilitySurface, ...],
    missing_safeguards: tuple[DataPortabilitySafeguard, ...],
) -> DataPortabilityRiskLevel:
    if not missing_safeguards:
        return "low"
    if any(
        safeguard in missing_safeguards
        for safeguard in ("authorization_check", "pii_redaction", "expiration_policy", "large_export_handling")
    ) and any(
        surface in impacted_surfaces
        for surface in ("account_transfer", "portable_archive", "gdpr_access_request", "migration_out")
    ):
        return "high"
    return "medium"


def _summary(
    findings: tuple[TaskDataPortabilityImpactFinding, ...],
    *,
    total_task_count: int,
    ignored_task_count: int,
) -> dict[str, Any]:
    return {
        "task_count": total_task_count,
        "impacted_task_count": len(findings),
        "ignored_task_count": ignored_task_count,
        "risk_counts": {
            level: sum(1 for finding in findings if finding.risk_level == level)
            for level in ("high", "medium", "low")
        },
        "surface_counts": {
            surface: sum(1 for finding in findings if surface in finding.impacted_surfaces)
            for surface in _SURFACE_ORDER
        },
        "missing_safeguard_counts": {
            safeguard: sum(1 for finding in findings if safeguard in finding.missing_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
    }


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
    for field_name in ("acceptance_criteria", "tags", "labels", "notes", "risks"):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    for source_field, text in _metadata_texts(task.get("metadata")):
        texts.append((source_field, text))
    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        patterns = (*_SURFACE_PATTERNS.values(), _USER_DATA_RE, *_SAFEGUARD_PATTERNS.values())
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            key_text = str(key).replace("_", " ")
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if any(pattern.search(key_text) for pattern in patterns):
                    texts.append((field, f"{key_text}: {text}"))
            elif any(pattern.search(key_text) for pattern in patterns):
                texts.append((field, key_text))
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
    "DataPortabilityRiskLevel",
    "DataPortabilitySafeguard",
    "DataPortabilitySurface",
    "TaskDataPortabilityImpactFinding",
    "TaskDataPortabilityImpactPlan",
    "build_task_data_portability_impact_plan",
    "recommend_task_data_portability_impact",
    "summarize_task_data_portability_impact",
    "task_data_portability_impact_plan_to_dict",
]
