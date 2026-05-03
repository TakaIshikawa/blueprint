"""Build plan-level data archival readiness matrices."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


ArchivalReadinessArea = Literal[
    "archival_trigger_definition",
    "storage_tier_validation",
    "retrieval_sla",
    "legal_hold_handling",
    "restore_testing",
    "deletion_handoff_ownership",
]
ArchivalReadiness = Literal["ready", "partial", "blocked"]
ArchivalRisk = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_READINESS_ORDER: dict[ArchivalReadiness, int] = {"blocked": 0, "partial": 1, "ready": 2}
_RISK_ORDER: dict[ArchivalRisk, int] = {"high": 0, "medium": 1, "low": 2}
_AREA_ORDER: dict[ArchivalReadinessArea, int] = {
    "archival_trigger_definition": 0,
    "storage_tier_validation": 1,
    "retrieval_sla": 2,
    "legal_hold_handling": 3,
    "restore_testing": 4,
    "deletion_handoff_ownership": 5,
}
_ARCHIVAL_RE = re.compile(
    r"\b(?:archive|archival|cold storage|glacier|nearline|retention|data lifecycle|"
    r"data deletion|purge|legal hold|restore|retrieval sla)\b",
    re.I,
)
_AREA_PATTERNS: dict[ArchivalReadinessArea, re.Pattern[str]] = {
    "archival_trigger_definition": re.compile(
        r"\b(?:archive trigger|archival trigger|retention window|retention period|ttl|"
        r"time to live|age threshold|lifecycle rule|archive after|inactive for|data lifecycle)\b",
        re.I,
    ),
    "storage_tier_validation": re.compile(
        r"\b(?:storage tier|cold storage|glacier|deep archive|nearline|archive bucket|"
        r"lifecycle policy|tier validation|storage class|cost tier)\b",
        re.I,
    ),
    "retrieval_sla": re.compile(
        r"\b(?:retrieval sla|restore sla|retrieval time|restore time|rto|recovery time|"
        r"within \d+ (?:minutes?|hours?|days?)|retrieval window)\b",
        re.I,
    ),
    "legal_hold_handling": re.compile(
        r"\b(?:legal hold|litigation hold|hold exemption|hold handling|e[- ]?discovery|"
        r"compliance hold|retention lock|do not delete)\b",
        re.I,
    ),
    "restore_testing": re.compile(
        r"\b(?:restore test|restore testing|recovery drill|restore drill|sample restore|"
        r"test restore|rehydration test|backup restore validation)\b",
        re.I,
    ),
    "deletion_handoff_ownership": re.compile(
        r"\b(?:deletion handoff|deletion owner|purge owner|deletion ownership|"
        r"handoff owner|data deletion|purge handoff|privacy deletion|hard delete)\b",
        re.I,
    ),
}
_OWNER_KEYS = (
    "owner",
    "owners",
    "owner_hint",
    "owner_team",
    "team",
    "dri",
    "data_owner",
    "privacy_owner",
    "legal_owner",
    "archive_owner",
    "archival_owner",
    "storage_owner",
    "deletion_owner",
    "restore_owner",
)
_DEFAULT_OWNERS: dict[ArchivalReadinessArea, str] = {
    "archival_trigger_definition": "data_owner",
    "storage_tier_validation": "data_platform_owner",
    "retrieval_sla": "operations_owner",
    "legal_hold_handling": "legal_owner",
    "restore_testing": "data_platform_owner",
    "deletion_handoff_ownership": "privacy_owner",
}
_GAP_MESSAGES: dict[ArchivalReadinessArea, str] = {
    "archival_trigger_definition": "Missing archival trigger definition.",
    "storage_tier_validation": "Missing storage tier validation.",
    "retrieval_sla": "Missing retrieval SLA.",
    "legal_hold_handling": "Missing legal hold handling.",
    "restore_testing": "Missing restore testing.",
    "deletion_handoff_ownership": "Missing deletion handoff ownership.",
}
_NEXT_ACTIONS: dict[ArchivalReadinessArea, str] = {
    "archival_trigger_definition": "Define the data age, state, or lifecycle trigger that moves records to archive.",
    "storage_tier_validation": "Validate the archive storage tier, lifecycle policy, and cost or durability constraints.",
    "retrieval_sla": "Document the retrieval SLA and recovery-time expectation for archived records.",
    "legal_hold_handling": "Document how legal holds suspend archive deletion or purge automation.",
    "restore_testing": "Run and record a restore test against representative archived data.",
    "deletion_handoff_ownership": "Assign the owner who receives deletion or purge handoff after archival.",
}
_HIGH_GAP_AREAS: frozenset[ArchivalReadinessArea] = frozenset(
    {"retrieval_sla", "legal_hold_handling", "restore_testing"}
)


@dataclass(frozen=True, slots=True)
class PlanDataArchivalReadinessRow:
    """One plan-level data archival readiness row."""

    area: ArchivalReadinessArea
    owner: str
    evidence: tuple[str, ...] = field(default_factory=tuple)
    gaps: tuple[str, ...] = field(default_factory=tuple)
    readiness: ArchivalReadiness = "partial"
    risk: ArchivalRisk = "medium"
    next_action: str = ""
    task_ids: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "area": self.area,
            "owner": self.owner,
            "evidence": list(self.evidence),
            "gaps": list(self.gaps),
            "readiness": self.readiness,
            "risk": self.risk,
            "next_action": self.next_action,
            "task_ids": list(self.task_ids),
        }


@dataclass(frozen=True, slots=True)
class PlanDataArchivalReadinessMatrix:
    """Plan-level data archival readiness matrix and summary counts."""

    plan_id: str | None = None
    rows: tuple[PlanDataArchivalReadinessRow, ...] = field(default_factory=tuple)
    archival_task_ids: tuple[str, ...] = field(default_factory=tuple)
    gap_areas: tuple[ArchivalReadinessArea, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[PlanDataArchivalReadinessRow, ...]:
        """Compatibility view for consumers that call matrix rows records."""
        return self.rows

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "records": [row.to_dict() for row in self.records],
            "archival_task_ids": list(self.archival_task_ids),
            "gap_areas": list(self.gap_areas),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return archival readiness rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the archival readiness matrix as deterministic Markdown."""
        title = "# Plan Data Archival Readiness Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        risk_counts = self.summary.get("risk_counts", {})
        lines = [
            title,
            "",
            (
                f"Summary: {self.summary.get('ready_area_count', 0)} of "
                f"{self.summary.get('area_count', 0)} archival readiness areas ready "
                f"(high: {risk_counts.get('high', 0)}, medium: {risk_counts.get('medium', 0)}, "
                f"low: {risk_counts.get('low', 0)})."
            ),
        ]
        if not self.rows:
            lines.extend(["", "No data archival readiness rows were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Area | Owner | Readiness | Risk | Evidence | Gaps | Next Action | Tasks |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"{row.area} | "
                f"{_markdown_cell(row.owner)} | "
                f"{row.readiness} | "
                f"{row.risk} | "
                f"{_markdown_cell('; '.join(row.evidence) or 'none')} | "
                f"{_markdown_cell('; '.join(row.gaps) or 'none')} | "
                f"{_markdown_cell(row.next_action)} | "
                f"{_markdown_cell(', '.join(row.task_ids) or 'none')} |"
            )
        return "\n".join(lines)


def build_plan_data_archival_readiness_matrix(source: Any) -> PlanDataArchivalReadinessMatrix:
    """Build required data archival readiness rows for an execution plan."""
    plan_id, tasks = _source_payload(source)
    area_evidence: dict[ArchivalReadinessArea, list[str]] = {area: [] for area in _AREA_ORDER}
    area_task_ids: dict[ArchivalReadinessArea, list[str]] = {area: [] for area in _AREA_ORDER}
    owner_hints: dict[ArchivalReadinessArea, list[str]] = {area: [] for area in _AREA_ORDER}
    archival_task_ids: list[str] = []

    for index, task in enumerate(tasks, start=1):
        task_id = _task_id(task, index)
        texts = _candidate_texts(task)
        context = " ".join(text for _, text in texts)
        task_has_archival_signal = bool(_ARCHIVAL_RE.search(context))
        owners = _owner_hints(task)
        for area, pattern in _AREA_PATTERNS.items():
            matches = [
                _evidence_snippet(source_field, text)
                for source_field, text in texts
                if pattern.search(text)
            ]
            if matches:
                task_has_archival_signal = True
                area_evidence[area].extend(matches)
                area_task_ids[area].append(task_id)
                owner_hints[area].extend(owners)
        if task_has_archival_signal:
            archival_task_ids.append(task_id)
            for area in _AREA_ORDER:
                owner_hints[area].extend(owners)

    rows = tuple(_row(area, area_evidence[area], area_task_ids[area], owner_hints[area]) for area in _AREA_ORDER)
    return PlanDataArchivalReadinessMatrix(
        plan_id=plan_id,
        rows=rows,
        archival_task_ids=tuple(_dedupe(archival_task_ids)),
        gap_areas=tuple(row.area for row in rows if row.gaps),
        summary=_summary(len(tasks), rows, archival_task_ids),
    )


def generate_plan_data_archival_readiness_matrix(source: Any) -> PlanDataArchivalReadinessMatrix:
    """Generate a data archival readiness matrix from a plan-like source."""
    return build_plan_data_archival_readiness_matrix(source)


def plan_data_archival_readiness_matrix_to_dict(
    matrix: PlanDataArchivalReadinessMatrix,
) -> dict[str, Any]:
    """Serialize a data archival readiness matrix to a plain dictionary."""
    return matrix.to_dict()


plan_data_archival_readiness_matrix_to_dict.__test__ = False


def plan_data_archival_readiness_matrix_to_dicts(
    matrix: PlanDataArchivalReadinessMatrix | Iterable[PlanDataArchivalReadinessRow],
) -> list[dict[str, Any]]:
    """Serialize data archival readiness rows to plain dictionaries."""
    if isinstance(matrix, PlanDataArchivalReadinessMatrix):
        return matrix.to_dicts()
    return [row.to_dict() for row in matrix]


plan_data_archival_readiness_matrix_to_dicts.__test__ = False


def plan_data_archival_readiness_matrix_to_markdown(
    matrix: PlanDataArchivalReadinessMatrix,
) -> str:
    """Render a data archival readiness matrix as Markdown."""
    return matrix.to_markdown()


plan_data_archival_readiness_matrix_to_markdown.__test__ = False


def _row(
    area: ArchivalReadinessArea,
    evidence: Iterable[str],
    task_ids: Iterable[str],
    owners: Iterable[str],
) -> PlanDataArchivalReadinessRow:
    evidence_tuple = tuple(_dedupe(evidence))
    gaps = () if evidence_tuple else (_GAP_MESSAGES[area],)
    readiness: ArchivalReadiness = "ready" if not gaps else "partial"
    risk: ArchivalRisk = "low" if not gaps else ("high" if area in _HIGH_GAP_AREAS else "medium")
    return PlanDataArchivalReadinessRow(
        area=area,
        owner=next(iter(_dedupe(owners)), _DEFAULT_OWNERS[area]),
        evidence=evidence_tuple,
        gaps=gaps,
        readiness=readiness,
        risk=risk,
        next_action="Ready for archival handoff." if not gaps else _NEXT_ACTIONS[area],
        task_ids=tuple(_dedupe(task_ids)),
    )


def _summary(
    task_count: int,
    rows: Iterable[PlanDataArchivalReadinessRow],
    archival_task_ids: Iterable[str],
) -> dict[str, Any]:
    row_list = list(rows)
    return {
        "task_count": task_count,
        "area_count": len(row_list),
        "ready_area_count": sum(1 for row in row_list if row.readiness == "ready"),
        "gap_area_count": sum(1 for row in row_list if row.gaps),
        "archival_task_count": len(tuple(_dedupe(archival_task_ids))),
        "readiness_counts": {
            readiness: sum(1 for row in row_list if row.readiness == readiness)
            for readiness in _READINESS_ORDER
        },
        "risk_counts": {risk: sum(1 for row in row_list if row.risk == risk) for risk in _RISK_ORDER},
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
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
        elif _looks_like_task(item):
            tasks.append(_object_payload(item))
    return None, tasks


def _plan_payload(plan: Mapping[str, Any] | object) -> dict[str, Any]:
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
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
        elif _looks_like_task(item):
            tasks.append(_object_payload(item))
    return tasks


def _candidate_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in ("title", "description", "milestone", "owner_type", "risk_level", "test_command", "blocked_reason"):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in ("depends_on", "files_or_modules", "files", "acceptance_criteria", "tags", "labels", "notes", "risks"):
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
            key_text = str(key).replace("_", " ")
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                texts.append((field, f"{key_text}: {text}"))
            elif key_text:
                texts.append((field, key_text))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts: list[tuple[str, str]] = []
        for index, item in enumerate(items):
            field = f"{prefix}[{index}]"
            if isinstance(item, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(item, field))
            elif text := _optional_text(item):
                texts.append((field, text))
        return texts
    text = _optional_text(value)
    return [(prefix, text)] if text else []


def _owner_hints(task: Mapping[str, Any]) -> list[str]:
    owners = []
    if owner := _optional_text(task.get("owner_type")):
        owners.append(owner)
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key in _OWNER_KEYS:
            owners.extend(_strings(metadata.get(key)))
    return _dedupe(owners)


def _looks_like_plan(value: object) -> bool:
    return not isinstance(value, (str, bytes)) and hasattr(value, "tasks")


def _looks_like_task(value: object) -> bool:
    return not isinstance(value, (str, bytes)) and any(
        hasattr(value, field_name) for field_name in ("id", "title", "description")
    )


def _object_payload(value: object) -> dict[str, Any]:
    fields = (
        "id",
        "title",
        "description",
        "milestone",
        "owner_type",
        "depends_on",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "risk_level",
        "test_command",
        "status",
        "tags",
        "labels",
        "notes",
        "risks",
        "metadata",
        "blocked_reason",
        "tasks",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


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
    "ArchivalReadiness",
    "ArchivalReadinessArea",
    "ArchivalRisk",
    "PlanDataArchivalReadinessMatrix",
    "PlanDataArchivalReadinessRow",
    "build_plan_data_archival_readiness_matrix",
    "generate_plan_data_archival_readiness_matrix",
    "plan_data_archival_readiness_matrix_to_dict",
    "plan_data_archival_readiness_matrix_to_dicts",
    "plan_data_archival_readiness_matrix_to_markdown",
]
