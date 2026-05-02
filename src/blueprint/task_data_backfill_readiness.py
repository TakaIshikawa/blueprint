"""Identify task-level readiness gaps for data backfills and repair jobs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


BackfillReadinessRiskLevel = Literal["high", "medium", "low"]
BackfillWorkType = Literal[
    "backfill",
    "reprocessing",
    "historical_import",
    "recalculation",
    "repair_job",
    "one_time_script",
]
BackfillReadinessCheck = Literal[
    "batching",
    "resumability",
    "idempotency",
    "monitoring",
    "rollback_or_restore",
    "data_validation",
    "production_throttling",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_RISK_ORDER: dict[BackfillReadinessRiskLevel, int] = {"high": 0, "medium": 1, "low": 2}
_WORK_TYPE_ORDER: dict[BackfillWorkType, int] = {
    "backfill": 0,
    "reprocessing": 1,
    "historical_import": 2,
    "recalculation": 3,
    "repair_job": 4,
    "one_time_script": 5,
}
_CHECK_ORDER: dict[BackfillReadinessCheck, int] = {
    "batching": 0,
    "resumability": 1,
    "idempotency": 2,
    "monitoring": 3,
    "rollback_or_restore": 4,
    "data_validation": 5,
    "production_throttling": 6,
}
_WORK_PATTERNS: dict[BackfillWorkType, re.Pattern[str]] = {
    "backfill": re.compile(r"\b(?:backfill|back fill|back-populate|backpopulate|retrofit data)\b", re.I),
    "reprocessing": re.compile(r"\b(?:reprocess(?:ing)?|re-run|rerun|replay(?:ing)?|reingest(?:ion)?)\b", re.I),
    "historical_import": re.compile(
        r"\b(?:historical imports?|historic imports?|import historical|import history|legacy imports?|migrate historical)\b",
        re.I,
    ),
    "recalculation": re.compile(r"\b(?:recalculat(?:e|ion|ing)|recompute|rebuild derived|refresh aggregates?)\b", re.I),
    "repair_job": re.compile(r"\b(?:repair job|data repair|fix bad data|correct records?|reconcile|remediate data)\b", re.I),
    "one_time_script": re.compile(r"\b(?:one[- ]time script|one[- ]off script|ad hoc script|manual script|rake task)\b", re.I),
}
_PATH_PATTERNS: dict[BackfillWorkType, re.Pattern[str]] = {
    "backfill": re.compile(r"backfill|back[_-]?fill|backpopulate", re.I),
    "reprocessing": re.compile(r"reprocess|rerun|replay|reingest", re.I),
    "historical_import": re.compile(r"historical|historic|legacy[_-]?import|import[_-]?history", re.I),
    "recalculation": re.compile(r"recalculat|recompute|aggregate|derived", re.I),
    "repair_job": re.compile(r"repair|reconcile|remediat|fix[_-]?data", re.I),
    "one_time_script": re.compile(r"one[_-]?time|one[_-]?off|adhoc|ad[_-]?hoc|script|rake", re.I),
}
_CHECK_PATTERNS: dict[BackfillReadinessCheck, re.Pattern[str]] = {
    "batching": re.compile(r"\b(?:batch(?:es|ing| size)?|chunk(?:s|ing)?|page through|windowed|pagination)\b", re.I),
    "resumability": re.compile(r"\b(?:resum(?:e|able|ability)|restart(?:able)?|checkpoint|cursor|continue from)\b", re.I),
    "idempotency": re.compile(r"\b(?:idempot(?:ent|ency)|safe to rerun|dedupe|deduplicate|upsert|exactly once)\b", re.I),
    "monitoring": re.compile(r"\b(?:monitor(?:ing)?|metrics?|alert(?:s|ing)?|dashboard|progress log|observability)\b", re.I),
    "rollback_or_restore": re.compile(
        r"\b(?:rollback|roll back|restore|backup|snapshot|point-in-time|pit[rt]|undo|revert|recovery)\b",
        re.I,
    ),
    "data_validation": re.compile(
        r"\b(?:validat(?:e|es|ed|ing|ion)|verify|reconcile|checksum|row count|sample audit|data quality|dry run)\b",
        re.I,
    ),
    "production_throttling": re.compile(
        r"\b(?:throttl(?:e|ing)|rate limit|pace|production guard|off[- ]peak|slow rollout|pause between)\b",
        re.I,
    ),
}
_PRODUCTION_SCALE_RE = re.compile(
    r"\b(?:prod(?:uction)?|live data|customer data|customers?|tenant(?:s)?|all accounts|all users|"
    r"pii|personal data|billing data|large[- ]scale|millions?|billions?|warehouse|full table|entire table)\b",
    re.I,
)
_LOCAL_LOW_RISK_RE = re.compile(r"\b(?:local|dev|development|staging|test data|sandbox|fixture|sample data)\b", re.I)

_SUGGESTED_ACCEPTANCE_CRITERIA: dict[BackfillReadinessCheck, str] = {
    "batching": "Define batch size, ordering, and pause behavior for the backfill.",
    "resumability": "Backfill can resume from a recorded checkpoint after interruption.",
    "idempotency": "Backfill is safe to rerun without duplicating or corrupting records.",
    "monitoring": "Progress, failures, and completion metrics are observable during execution.",
    "rollback_or_restore": "Rollback, restore, or recovery steps are documented before execution.",
    "data_validation": "Pre-run and post-run validation confirms expected record counts and data quality.",
    "production_throttling": "Production execution includes throttling or off-peak pacing limits.",
}


@dataclass(frozen=True, slots=True)
class TaskDataBackfillReadinessFinding:
    """Readiness guidance for one task involving backfill-style data work."""

    task_id: str
    title: str
    work_types: tuple[BackfillWorkType, ...] = field(default_factory=tuple)
    readiness_checks: tuple[BackfillReadinessCheck, ...] = field(default_factory=tuple)
    missing_acceptance_criteria: tuple[BackfillReadinessCheck, ...] = field(default_factory=tuple)
    risk_level: BackfillReadinessRiskLevel = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "work_types": list(self.work_types),
            "readiness_checks": list(self.readiness_checks),
            "missing_acceptance_criteria": list(self.missing_acceptance_criteria),
            "risk_level": self.risk_level,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskDataBackfillReadinessPlan:
    """Plan-level summary of task data backfill readiness."""

    plan_id: str | None = None
    findings: tuple[TaskDataBackfillReadinessFinding, ...] = field(default_factory=tuple)
    impacted_task_ids: tuple[str, ...] = field(default_factory=tuple)
    ignored_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[TaskDataBackfillReadinessFinding, ...]:
        """Compatibility view matching planners that name task findings records."""
        return self.findings

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
        """Return backfill readiness findings as plain dictionaries."""
        return [finding.to_dict() for finding in self.findings]


def build_task_data_backfill_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | Any]
        | Any
    ),
) -> TaskDataBackfillReadinessPlan:
    """Build task-level readiness recommendations for data backfills."""
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
                -len(finding.missing_acceptance_criteria),
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
    return TaskDataBackfillReadinessPlan(
        plan_id=plan_id,
        findings=findings,
        impacted_task_ids=impacted_task_ids,
        ignored_task_ids=ignored_task_ids,
        summary=_summary(findings, total_task_count=len(tasks), ignored_task_count=len(ignored_task_ids)),
    )


def analyze_task_data_backfill_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | Any]
        | Any
    ),
) -> tuple[TaskDataBackfillReadinessFinding, ...]:
    """Return backfill readiness findings for relevant execution tasks."""
    return build_task_data_backfill_readiness_plan(source).findings


def summarize_task_data_backfill_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | Any]
        | Any
    ),
) -> TaskDataBackfillReadinessPlan:
    """Compatibility alias for building a data backfill readiness plan."""
    return build_task_data_backfill_readiness_plan(source)


def summarize_task_data_backfill_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | Any]
        | Any
    ),
) -> TaskDataBackfillReadinessPlan:
    """Compatibility alias for building a data backfill readiness plan."""
    return build_task_data_backfill_readiness_plan(source)


def task_data_backfill_readiness_plan_to_dict(
    result: TaskDataBackfillReadinessPlan,
) -> dict[str, Any]:
    """Serialize a data backfill readiness plan to a plain dictionary."""
    return result.to_dict()


task_data_backfill_readiness_plan_to_dict.__test__ = False


def _finding_for_task(task: Mapping[str, Any], index: int) -> TaskDataBackfillReadinessFinding | None:
    work_evidence: dict[BackfillWorkType, list[str]] = {}
    acceptance_checks: set[BackfillReadinessCheck] = set()
    production_evidence: list[str] = []
    local_evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        _inspect_path(path, work_evidence)
    for source_field, text in _candidate_texts(task):
        _inspect_text(source_field, text, work_evidence, production_evidence, local_evidence)
        if source_field.startswith("acceptance_criteria"):
            acceptance_checks.update(_checks_in(text))

    if not work_evidence:
        return None

    work_types = tuple(work_type for work_type in _WORK_TYPE_ORDER if work_type in work_evidence)
    readiness_checks = tuple(_CHECK_ORDER)
    missing_acceptance_criteria = tuple(check for check in _CHECK_ORDER if check not in acceptance_checks)
    task_id = _task_id(task, index)
    return TaskDataBackfillReadinessFinding(
        task_id=task_id,
        title=_optional_text(task.get("title")) or task_id,
        work_types=work_types,
        readiness_checks=readiness_checks,
        missing_acceptance_criteria=missing_acceptance_criteria,
        risk_level=_risk_level(
            work_types=work_types,
            missing_acceptance_criteria=missing_acceptance_criteria,
            production_evidence=production_evidence,
            local_evidence=local_evidence,
        ),
        evidence=tuple(
            _dedupe(
                [
                    *(
                        evidence
                        for work_type in work_types
                        for evidence in work_evidence.get(work_type, [])
                    ),
                    *production_evidence,
                    *local_evidence,
                ]
            )
        ),
    )


def _inspect_path(path: str, work_evidence: dict[BackfillWorkType, list[str]]) -> None:
    normalized = _normalized_path(path)
    if not normalized:
        return
    evidence = f"files_or_modules: {path}"
    searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    for work_type, pattern in _PATH_PATTERNS.items():
        if pattern.search(normalized) or pattern.search(searchable):
            work_evidence.setdefault(work_type, []).append(evidence)


def _inspect_text(
    source_field: str,
    text: str,
    work_evidence: dict[BackfillWorkType, list[str]],
    production_evidence: list[str],
    local_evidence: list[str],
) -> None:
    evidence = _evidence_snippet(source_field, text)
    for work_type, pattern in _WORK_PATTERNS.items():
        if pattern.search(text):
            work_evidence.setdefault(work_type, []).append(evidence)
    if _PRODUCTION_SCALE_RE.search(text):
        production_evidence.append(evidence)
    if _LOCAL_LOW_RISK_RE.search(text):
        local_evidence.append(evidence)


def _checks_in(text: str) -> set[BackfillReadinessCheck]:
    return {check for check, pattern in _CHECK_PATTERNS.items() if pattern.search(text)}


def _risk_level(
    *,
    work_types: tuple[BackfillWorkType, ...],
    missing_acceptance_criteria: tuple[BackfillReadinessCheck, ...],
    production_evidence: list[str],
    local_evidence: list[str],
) -> BackfillReadinessRiskLevel:
    if production_evidence:
        return "high"
    if len(missing_acceptance_criteria) >= 4 and any(
        work_type in work_types for work_type in ("backfill", "reprocessing", "historical_import")
    ):
        return "high"
    if local_evidence and len(missing_acceptance_criteria) <= 2:
        return "low"
    if local_evidence and set(missing_acceptance_criteria) <= {"rollback_or_restore", "production_throttling"}:
        return "low"
    if len(missing_acceptance_criteria) <= 1:
        return "low"
    return "medium"


def _summary(
    findings: tuple[TaskDataBackfillReadinessFinding, ...],
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
        "work_type_counts": {
            work_type: sum(1 for finding in findings if work_type in finding.work_types)
            for work_type in _WORK_TYPE_ORDER
        },
        "missing_acceptance_criteria_counts": {
            check: sum(1 for finding in findings if check in finding.missing_acceptance_criteria)
            for check in _CHECK_ORDER
        },
    }


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | Any]
        | Any
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
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        if isinstance(value, Mapping):
            return _source_payload(value)
    if _looks_like_task(source):
        return None, [_object_task_payload(source)]

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
        elif _looks_like_task(item):
            tasks.append(_object_task_payload(item))
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
        elif _looks_like_task(item):
            tasks.append(_object_task_payload(item))
    return tasks


def _object_task_payload(value: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for field_name in (
        "id",
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "risk_level",
        "test_command",
        "blocked_reason",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "tags",
        "labels",
        "notes",
        "risks",
        "metadata",
    ):
        if hasattr(value, field_name):
            payload[field_name] = getattr(value, field_name)
    return payload


def _looks_like_task(value: Any) -> bool:
    return any(hasattr(value, field_name) for field_name in ("id", "title", "description", "acceptance_criteria"))


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
        patterns = (*_WORK_PATTERNS.values(), *_CHECK_PATTERNS.values(), _PRODUCTION_SCALE_RE, _LOCAL_LOW_RISK_RE)
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            try:
                child = value[key]
            except (KeyError, TypeError):
                continue
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
            try:
                strings.extend(_strings(value[key]))
            except (KeyError, TypeError):
                continue
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
    "BackfillReadinessCheck",
    "BackfillReadinessRiskLevel",
    "BackfillWorkType",
    "TaskDataBackfillReadinessFinding",
    "TaskDataBackfillReadinessPlan",
    "analyze_task_data_backfill_readiness",
    "build_task_data_backfill_readiness_plan",
    "summarize_task_data_backfill_readiness",
    "summarize_task_data_backfill_readiness_plan",
    "task_data_backfill_readiness_plan_to_dict",
]
