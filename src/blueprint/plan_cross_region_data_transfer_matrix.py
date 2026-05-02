"""Build cross-region customer data transfer matrices for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


CrossRegionDataRegion = Literal[
    "US",
    "EU",
    "UK",
    "APAC",
    "Canada",
    "Australia",
    "global",
]
CrossRegionTransferType = Literal[
    "replication",
    "export",
    "logs",
    "backup",
    "vendor_processing",
    "support_access",
    "object_storage",
    "analytics",
    "cross_border_transfer",
]
CrossRegionTransferPriority = Literal["low", "medium", "high"]
CrossRegionControl = Literal[
    "DPA review",
    "encryption",
    "residency exception",
    "retention bounds",
    "owner approval",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_REGION_ORDER: dict[CrossRegionDataRegion, int] = {
    "US": 0,
    "EU": 1,
    "UK": 2,
    "APAC": 3,
    "Canada": 4,
    "Australia": 5,
    "global": 6,
}
_TRANSFER_ORDER: dict[CrossRegionTransferType, int] = {
    "replication": 0,
    "export": 1,
    "logs": 2,
    "backup": 3,
    "vendor_processing": 4,
    "support_access": 5,
    "object_storage": 6,
    "analytics": 7,
    "cross_border_transfer": 8,
}
_PRIORITY_ORDER: dict[CrossRegionTransferPriority, int] = {"high": 0, "medium": 1, "low": 2}

_REGION_PATTERNS: dict[CrossRegionDataRegion, re.Pattern[str]] = {
    "US": re.compile(r"\b(?:us|u\.s\.|usa|united states|north america|na)\b", re.I),
    "EU": re.compile(r"\b(?:eu|e\.u\.|european union|europe|eea)\b", re.I),
    "UK": re.compile(r"\b(?:uk|u\.k\.|united kingdom|britain|gb|great britain)\b", re.I),
    "APAC": re.compile(r"\b(?:apac|asia pacific|asia-pacific|singapore|japan|india)\b", re.I),
    "Canada": re.compile(r"\b(?:canada|canadian|ca region|ca-residency)\b", re.I),
    "Australia": re.compile(r"\b(?:australia|australian|au region|au-residency)\b", re.I),
    "global": re.compile(
        r"\b(?:global|worldwide|multi[- ]?region|all regions|any region|follow[- ]?the[- ]?sun)\b",
        re.I,
    ),
}
_TRANSFER_PATTERNS: dict[CrossRegionTransferType, re.Pattern[str]] = {
    "replication": re.compile(
        r"\b(?:replication|replicate|replicated|replica|mirror|mirroring|sync|synchroni[sz]e)\b",
        re.I,
    ),
    "export": re.compile(
        r"\b(?:export|exports|extract|data extract|csv|parquet|dump|download)\b",
        re.I,
    ),
    "logs": re.compile(r"\b(?:log|logs|logging|audit trail|telemetry|observability|siem)\b", re.I),
    "backup": re.compile(r"\b(?:backup|backups|snapshot|snapshots|restore|archive|archives)\b", re.I),
    "vendor_processing": re.compile(
        r"\b(?:vendor|processor|subprocessor|third[- ]?party|partner|provider|managed service|"
        r"outsourc(?:e|ed|ing))\b",
        re.I,
    ),
    "support_access": re.compile(
        r"\b(?:support access|support engineer|support agent|admin access|"
        r"break[- ]?glass|follow[- ]?the[- ]?sun|case access)\b",
        re.I,
    ),
    "object_storage": re.compile(
        r"\b(?:object storage|bucket|buckets|s3|gcs|blob storage|azure blob|data lake)\b",
        re.I,
    ),
    "analytics": re.compile(
        r"\b(?:analytics|warehouse|data warehouse|bi|business intelligence|dashboard|reporting|"
        r"metrics|segment|snowflake|bigquery|databricks)\b",
        re.I,
    ),
    "cross_border_transfer": re.compile(
        r"\b(?:cross[- ]?region|cross[- ]?border|international transfer|data transfer|"
        r"transfer impact assessment|tia|scc|standard contractual clauses|residency exception|"
        r"outside (?:the )?(?:us|u\.s\.|eu|europe|uk|canada|australia|apac))\b",
        re.I,
    ),
}
_CUSTOMER_DATA_RE = re.compile(
    r"\b(?:customer data|personal data|pii|profile|account data|user data|tenant data|"
    r"production data|personal information)\b",
    re.I,
)
_CONTROL_ORDER: tuple[CrossRegionControl, ...] = (
    "DPA review",
    "encryption",
    "residency exception",
    "retention bounds",
    "owner approval",
)


@dataclass(frozen=True, slots=True)
class PlanCrossRegionDataTransferRow:
    """One task-level cross-region data transfer planning row."""

    task_id: str
    task_title: str
    source_regions: tuple[CrossRegionDataRegion, ...]
    destination_regions: tuple[CrossRegionDataRegion, ...]
    transfer_type: CrossRegionTransferType
    required_controls: tuple[CrossRegionControl, ...]
    compliance_evidence: tuple[str, ...] = field(default_factory=tuple)
    owner_hint: str | None = None
    priority: CrossRegionTransferPriority = "low"

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "task_title": self.task_title,
            "source_regions": list(self.source_regions),
            "destination_regions": list(self.destination_regions),
            "transfer_type": self.transfer_type,
            "required_controls": list(self.required_controls),
            "compliance_evidence": list(self.compliance_evidence),
            "owner_hint": self.owner_hint,
            "priority": self.priority,
        }


@dataclass(frozen=True, slots=True)
class PlanCrossRegionDataTransferMatrix:
    """Plan-level cross-region data transfer matrix and summary counts."""

    plan_id: str | None = None
    rows: tuple[PlanCrossRegionDataTransferRow, ...] = field(default_factory=tuple)
    transfer_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_signal_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "transfer_task_ids": list(self.transfer_task_ids),
            "no_signal_task_ids": list(self.no_signal_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return cross-region data transfer rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the matrix as deterministic Markdown."""
        title = "# Plan Cross-Region Data Transfer Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        counts = self.summary.get("priority_counts", {})
        lines = [
            title,
            "",
            (
                f"Summary: {self.summary.get('transfer_task_count', 0)} of "
                f"{self.summary.get('task_count', 0)} tasks require transfer planning "
                f"(high: {counts.get('high', 0)}, medium: {counts.get('medium', 0)}, "
                f"low: {counts.get('low', 0)})."
            ),
        ]
        if not self.rows:
            lines.extend(["", "No cross-region data transfer rows were inferred."])
            if self.no_signal_task_ids:
                lines.extend(
                    [
                        "",
                        f"No transfer signals: {_markdown_cell(', '.join(self.no_signal_task_ids))}",
                    ]
                )
            return "\n".join(lines)

        lines.extend(
            [
                "",
                (
                    "| Task | Title | Source Regions | Destination Regions | Transfer Type | "
                    "Controls | Priority | Owner | Evidence |"
                ),
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"`{_markdown_cell(row.task_id)}` | "
                f"{_markdown_cell(row.task_title)} | "
                f"{_markdown_cell(', '.join(row.source_regions))} | "
                f"{_markdown_cell(', '.join(row.destination_regions))} | "
                f"{row.transfer_type} | "
                f"{_markdown_cell(', '.join(row.required_controls))} | "
                f"{row.priority} | "
                f"{_markdown_cell(row.owner_hint or 'Unassigned')} | "
                f"{_markdown_cell('; '.join(row.compliance_evidence))} |"
            )
        if self.no_signal_task_ids:
            lines.extend(
                [
                    "",
                    f"No transfer signals: {_markdown_cell(', '.join(self.no_signal_task_ids))}",
                ]
            )
        return "\n".join(lines)


def build_plan_cross_region_data_transfer_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> PlanCrossRegionDataTransferMatrix:
    """Build task-level cross-region data transfer planning rows for an execution plan."""
    plan_id, tasks = _source_payload(source)
    candidates = [_task_rows(task, index) for index, task in enumerate(tasks, start=1)]
    rows = tuple(row for task_rows in candidates for row in task_rows)
    transfer_task_ids = tuple(_dedupe(row.task_id for row in rows))
    no_signal_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if not candidates[index - 1]
    )
    transfer_type_counts = {
        transfer_type: sum(1 for row in rows if row.transfer_type == transfer_type)
        for transfer_type in _TRANSFER_ORDER
    }
    region_counts = {
        region: sum(
            1
            for row in rows
            if region in row.source_regions or region in row.destination_regions
        )
        for region in _REGION_ORDER
    }
    priority_counts = {
        priority: sum(1 for row in rows if row.priority == priority)
        for priority in _PRIORITY_ORDER
    }
    return PlanCrossRegionDataTransferMatrix(
        plan_id=plan_id,
        rows=rows,
        transfer_task_ids=transfer_task_ids,
        no_signal_task_ids=no_signal_task_ids,
        summary={
            "task_count": len(tasks),
            "transfer_task_count": len(transfer_task_ids),
            "no_signal_task_count": len(no_signal_task_ids),
            "transfer_type_counts": transfer_type_counts,
            "region_counts": region_counts,
            "priority_counts": priority_counts,
        },
    )


def plan_cross_region_data_transfer_matrix_to_dict(
    result: PlanCrossRegionDataTransferMatrix,
) -> dict[str, Any]:
    """Serialize a cross-region data transfer matrix to a plain dictionary."""
    return result.to_dict()


plan_cross_region_data_transfer_matrix_to_dict.__test__ = False


def plan_cross_region_data_transfer_matrix_to_markdown(
    result: PlanCrossRegionDataTransferMatrix,
) -> str:
    """Render a cross-region data transfer matrix as Markdown."""
    return result.to_markdown()


plan_cross_region_data_transfer_matrix_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    transfer_types: tuple[CrossRegionTransferType, ...] = field(default_factory=tuple)
    source_regions: tuple[CrossRegionDataRegion, ...] = field(default_factory=tuple)
    destination_regions: tuple[CrossRegionDataRegion, ...] = field(default_factory=tuple)
    regions: tuple[CrossRegionDataRegion, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    customer_data: bool = False


def _task_rows(task: Mapping[str, Any], index: int) -> tuple[PlanCrossRegionDataTransferRow, ...]:
    task_id = _task_id(task, index)
    signals = _signals(task)
    if not signals.transfer_types:
        return ()

    source_regions, destination_regions = _region_pair(signals)
    if not source_regions and not destination_regions:
        return ()

    return tuple(
        PlanCrossRegionDataTransferRow(
            task_id=task_id,
            task_title=_optional_text(task.get("title")) or task_id,
            source_regions=source_regions,
            destination_regions=destination_regions,
            transfer_type=transfer_type,
            required_controls=_required_controls(transfer_type, source_regions, destination_regions),
            compliance_evidence=signals.evidence,
            owner_hint=_owner_hint(task, transfer_type),
            priority=_priority(task, transfer_type, source_regions, destination_regions, signals),
        )
        for transfer_type in signals.transfer_types
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    transfer_types: set[CrossRegionTransferType] = set()
    source_regions: set[CrossRegionDataRegion] = set()
    destination_regions: set[CrossRegionDataRegion] = set()
    regions: list[CrossRegionDataRegion] = []
    evidence: list[str] = []
    customer_data = False

    for source_field, text in _candidate_texts(task):
        snippet = _evidence_snippet(source_field, text)
        if _CUSTOMER_DATA_RE.search(text):
            customer_data = True
            evidence.append(snippet)
        matched_regions = _regions_in_text(text)
        if matched_regions:
            regions.extend(matched_regions)
            evidence.append(snippet)
            if _source_field(source_field):
                source_regions.update(matched_regions)
            if _destination_field(source_field):
                destination_regions.update(matched_regions)
        for transfer_type, pattern in _TRANSFER_PATTERNS.items():
            if pattern.search(text):
                transfer_types.add(transfer_type)
                evidence.append(snippet)

    if "global" in regions:
        transfer_types.add("cross_border_transfer")
    if len(set(regions)) > 1:
        transfer_types.add("cross_border_transfer")

    return _Signals(
        transfer_types=tuple(
            transfer_type for transfer_type in _TRANSFER_ORDER if transfer_type in transfer_types
        ),
        source_regions=tuple(region for region in _REGION_ORDER if region in source_regions),
        destination_regions=tuple(region for region in _REGION_ORDER if region in destination_regions),
        regions=tuple(_dedupe(regions)),
        evidence=tuple(_dedupe(evidence)),
        customer_data=customer_data,
    )


def _region_pair(
    signals: _Signals,
) -> tuple[tuple[CrossRegionDataRegion, ...], tuple[CrossRegionDataRegion, ...]]:
    source_regions = list(signals.source_regions)
    destination_regions = list(signals.destination_regions)
    all_regions = list(signals.regions)

    if not source_regions and destination_regions:
        source_regions = [region for region in all_regions if region not in destination_regions]
    if source_regions and not destination_regions:
        destination_regions = [region for region in all_regions if region not in source_regions]
    if not source_regions and not destination_regions and "global" in all_regions:
        local_regions = [region for region in all_regions if region != "global"]
        source_regions = local_regions[:1]
        destination_regions = [*local_regions[1:], "global"]
    if not source_regions and not destination_regions and len(all_regions) > 1:
        source_regions = [all_regions[0]]
        destination_regions = all_regions[1:]
    if not source_regions and not destination_regions and len(all_regions) == 1:
        source_regions = [all_regions[0]]
        destination_regions = ["global"] if all_regions[0] != "global" else []

    return (
        tuple(region for region in _REGION_ORDER if region in source_regions),
        tuple(region for region in _REGION_ORDER if region in destination_regions),
    )


def _required_controls(
    transfer_type: CrossRegionTransferType,
    source_regions: Iterable[CrossRegionDataRegion],
    destination_regions: Iterable[CrossRegionDataRegion],
) -> tuple[CrossRegionControl, ...]:
    controls: set[CrossRegionControl] = {"encryption", "owner approval"}
    source_set = set(source_regions)
    destination_set = set(destination_regions)
    if transfer_type in {"vendor_processing", "support_access", "cross_border_transfer"}:
        controls.add("DPA review")
    if transfer_type in {"export", "logs", "backup", "analytics", "object_storage"}:
        controls.add("retention bounds")
    if (
        "global" in source_set
        or "global" in destination_set
        or bool(source_set and destination_set and source_set != destination_set)
        or transfer_type == "cross_border_transfer"
    ):
        controls.add("DPA review")
        controls.add("residency exception")
    return tuple(control for control in _CONTROL_ORDER if control in controls)


def _priority(
    task: Mapping[str, Any],
    transfer_type: CrossRegionTransferType,
    source_regions: Iterable[CrossRegionDataRegion],
    destination_regions: Iterable[CrossRegionDataRegion],
    signals: _Signals,
) -> CrossRegionTransferPriority:
    risk = (_optional_text(task.get("risk_level")) or "").casefold()
    if risk in {"critical", "blocker", "high"}:
        return "high"
    region_count = len(set(source_regions) | set(destination_regions))
    if transfer_type in {"vendor_processing", "support_access", "cross_border_transfer"}:
        return "high"
    if signals.customer_data and transfer_type in {"replication", "backup", "object_storage"}:
        return "high"
    if region_count > 1 or transfer_type in {"export", "logs", "object_storage", "analytics"}:
        return "medium"
    return "low"


def _owner_hint(task: Mapping[str, Any], transfer_type: CrossRegionTransferType) -> str | None:
    explicit = _optional_text(task.get("owner_type"))
    if explicit:
        return explicit
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key in ("owner", "owner_hint", "team", "service_owner", "data_owner"):
            if text := _optional_text(metadata.get(key)):
                return text
    if transfer_type in {"vendor_processing", "support_access"}:
        return "privacy_owner"
    if transfer_type in {"replication", "backup", "object_storage"}:
        return "data_platform_owner"
    if transfer_type in {"export", "analytics", "logs"}:
        return "data_owner"
    if transfer_type == "cross_border_transfer":
        return "privacy_owner"
    return None


def _regions_in_text(text: str) -> list[CrossRegionDataRegion]:
    return [region for region, pattern in _REGION_PATTERNS.items() if pattern.search(text)]


def _source_field(source_field: str) -> bool:
    return bool(
        re.search(r"(?:^|\.)source|origin|from|primary|residen(?:cy|t)|home", source_field, re.I)
    )


def _destination_field(source_field: str) -> bool:
    return bool(
        re.search(r"(?:^|\.)dest|target|to|replica|backup|vendor|processor|support|storage", source_field, re.I)
    )


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
    for field_name in (
        "depends_on",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "tags",
        "labels",
        "notes",
    ):
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
                if _any_signal(key_text):
                    texts.append((field, str(key)))
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if _any_signal(key_text):
                    texts.append((field, f"{key_text}: {text}"))
            elif _any_signal(key_text):
                texts.append((field, str(key)))
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


def _any_signal(text: str) -> bool:
    return (
        _CUSTOMER_DATA_RE.search(text) is not None
        or any(pattern.search(text) for pattern in _REGION_PATTERNS.values())
        or any(pattern.search(text) for pattern in _TRANSFER_PATTERNS.values())
    )


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
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
    if _looks_like_task(source):
        return None, [_object_payload(source)]
    if _looks_like_plan(source):
        payload = _object_payload(source)
        return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))

    try:
        iterator = iter(source)  # type: ignore[arg-type]
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
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
        elif _looks_like_task(item):
            tasks.append(_object_payload(item))
    return tasks


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
        "tags",
        "labels",
        "notes",
        "tasks",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


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
    "CrossRegionControl",
    "CrossRegionDataRegion",
    "CrossRegionTransferPriority",
    "CrossRegionTransferType",
    "PlanCrossRegionDataTransferMatrix",
    "PlanCrossRegionDataTransferRow",
    "build_plan_cross_region_data_transfer_matrix",
    "plan_cross_region_data_transfer_matrix_to_dict",
    "plan_cross_region_data_transfer_matrix_to_markdown",
]
