"""Plan data residency review for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


ResidencyReviewStatus = Literal[
    "residency_review_required",
    "residency_review_recommended",
    "residency_review_not_needed",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_REGION_RE = re.compile(
    r"\b(?:eu[-_ ]?west[-_ ]?\d+|eu[-_ ]?central[-_ ]?\d+|"
    r"us[-_ ]?east[-_ ]?\d+|us[-_ ]?west[-_ ]?\d+|ca[-_ ]?central[-_ ]?\d+|"
    r"ap[-_ ](?:southeast|northeast|south)[-_ ]?\d+|eu|europe|european union|"
    r"eea|gdpr|us|usa|united states|north america|uk|united kingdom|apac|"
    r"asia[- ]?pacific|canada|ca|australia|au|germany|france|ireland)\b",
    re.I,
)
_CROSS_BORDER_RE = re.compile(
    r"\b(?:cross[- ]?border|data transfer|transfer data|route data|routing|replicate "
    r"to|replica|replicas|multi[- ]?region|region routing|geo[- ]?routing|"
    r"regional|region failover|data localization|data residency|sovereign|"
    r"processor|subprocessor|third[- ]?party processor|vendor processing)\b",
    re.I,
)
_DOC_TEST_PATH_RE = re.compile(
    r"(?:^|/)(?:docs?|documentation|test|tests|spec|specs|fixtures?)(?:/|$)|"
    r"(?:^|/)(?:README|CHANGELOG|CONTRIBUTING|TESTING)(?:\.[^/]*)?$|"
    r"(?:_test|\.test|\.spec)\.",
    re.I,
)
_PATH_SURFACE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "primary data store",
        re.compile(
            r"(?:^|/)(?:db|database|datastore|storage|models?|repositories?|migrations?)(?:/|$)|\.sql$",
            re.I,
        ),
    ),
    (
        "region routing",
        re.compile(
            r"(?:^|/)(?:routing|routes?|regions?|geo|traffic|load_balanc(?:er|ing))(?:/|$)|region",
            re.I,
        ),
    ),
    ("replicas", re.compile(r"(?:^|/)(?:replicas?|replication|mirrors?)(?:/|$)|replica", re.I)),
    (
        "backups",
        re.compile(
            r"(?:^|/)(?:backups?|snapshots?|restore|retention)(?:/|$)|backup|snapshot", re.I
        ),
    ),
    (
        "analytics exports",
        re.compile(
            r"(?:^|/)(?:analytics|bi|warehouse|exports?|etl|pipelines?)(?:/|$)|analytics|export",
            re.I,
        ),
    ),
    (
        "logs",
        re.compile(r"(?:^|/)(?:logs?|logging|observability|telemetry|audit)(?:/|$)|log", re.I),
    ),
    (
        "cdn caches",
        re.compile(r"(?:^|/)(?:cdn|cache|edge|cloudfront|fastly|akamai)(?:/|$)|cdn|cache", re.I),
    ),
    (
        "third-party processors",
        re.compile(
            r"(?:^|/)(?:integrations?|vendors?|processors?|webhooks?|partners?)(?:/|$)|processor|subprocessor",
            re.I,
        ),
    ),
)
_TEXT_SURFACE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "primary data store",
        re.compile(
            r"\b(?:database|data store|storage bucket|object storage|table|persist|stored)\b", re.I
        ),
    ),
    (
        "region routing",
        re.compile(
            r"\b(?:region routing|geo[- ]?routing|route data|regional endpoint|routing)\b", re.I
        ),
    ),
    ("replicas", re.compile(r"\b(?:replica|replicas|replication|replicate|mirror)\b", re.I)),
    (
        "backups",
        re.compile(r"\b(?:backup|backups|snapshot|restore point|disaster recovery|dr)\b", re.I),
    ),
    (
        "analytics exports",
        re.compile(
            r"\b(?:analytics export|analytics|warehouse|bi export|etl|data export|csv export)\b",
            re.I,
        ),
    ),
    ("logs", re.compile(r"\b(?:logs?|logging|audit trail|telemetry|observability)\b", re.I)),
    ("cdn caches", re.compile(r"\b(?:cdn|edge cache|cache|cloudfront|fastly|akamai)\b", re.I)),
    (
        "third-party processors",
        re.compile(
            r"\b(?:third[- ]?party processor|subprocessor|vendor|processor|webhook|partner)\b", re.I
        ),
    ),
)
_DATA_CATEGORY_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "personal data",
        re.compile(
            r"\b(?:personal data|pii|user data|customer data|profile|email|address|phone)\b", re.I
        ),
    ),
    (
        "authentication data",
        re.compile(r"\b(?:auth|authentication|session|token|credential|login)\b", re.I),
    ),
    ("payment data", re.compile(r"\b(?:payment|billing|invoice|card|stripe|bank)\b", re.I)),
    (
        "analytics data",
        re.compile(r"\b(?:analytics|event|tracking|metrics|telemetry|behavioral)\b", re.I),
    ),
    ("logs", re.compile(r"\b(?:logs?|logging|audit trail|request id|ip address)\b", re.I)),
    ("backup data", re.compile(r"\b(?:backup|snapshot|restore|archive)\b", re.I)),
)
_SURFACE_ORDER = {
    "primary data store": 0,
    "region routing": 1,
    "replicas": 2,
    "backups": 3,
    "analytics exports": 4,
    "logs": 5,
    "cdn caches": 6,
    "third-party processors": 7,
}
_DATA_CATEGORY_ORDER = {
    "personal data": 0,
    "authentication data": 1,
    "payment data": 2,
    "analytics data": 3,
    "logs": 4,
    "backup data": 5,
}
_STATUS_ORDER: dict[ResidencyReviewStatus, int] = {
    "residency_review_required": 0,
    "residency_review_recommended": 1,
    "residency_review_not_needed": 2,
}


@dataclass(frozen=True, slots=True)
class TaskDataResidencyRecord:
    """Data residency review guidance for one execution task."""

    task_id: str
    title: str
    residency_review_status: ResidencyReviewStatus
    region_signals: tuple[str, ...] = field(default_factory=tuple)
    affected_surfaces: tuple[str, ...] = field(default_factory=tuple)
    data_categories: tuple[str, ...] = field(default_factory=tuple)
    review_reasons: tuple[str, ...] = field(default_factory=tuple)
    recommended_actions: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "residency_review_status": self.residency_review_status,
            "region_signals": list(self.region_signals),
            "affected_surfaces": list(self.affected_surfaces),
            "data_categories": list(self.data_categories),
            "review_reasons": list(self.review_reasons),
            "recommended_actions": list(self.recommended_actions),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskDataResidencyPlan:
    """Plan-level data residency impact review."""

    plan_id: str | None = None
    records: tuple[TaskDataResidencyRecord, ...] = field(default_factory=tuple)
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
        """Return data residency records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render the data residency plan as deterministic Markdown."""
        title = "# Task Data Residency Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.records:
            lines.extend(
                ["", "No execution tasks were available for data residency review planning."]
            )
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Status | Regions | Surfaces | Data Categories | Actions |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{record.residency_review_status} | "
                f"{_markdown_cell(', '.join(record.region_signals) or 'none')} | "
                f"{_markdown_cell(', '.join(record.affected_surfaces) or 'none')} | "
                f"{_markdown_cell(', '.join(record.data_categories) or 'none')} | "
                f"{_markdown_cell('; '.join(record.recommended_actions) or 'none')} |"
            )
        return "\n".join(lines)


def build_task_data_residency_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskDataResidencyPlan:
    """Build data residency review guidance for execution tasks."""
    plan_id, tasks = _source_payload(source)
    records = tuple(
        sorted(
            (_task_record(task, index) for index, task in enumerate(tasks, start=1)),
            key=lambda record: (
                _STATUS_ORDER[record.residency_review_status],
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    review_task_ids = tuple(
        record.task_id
        for record in records
        if record.residency_review_status != "residency_review_not_needed"
    )
    status_counts = {
        status: sum(1 for record in records if record.residency_review_status == status)
        for status in _STATUS_ORDER
    }
    surface_counts = {
        surface: sum(1 for record in records if surface in record.affected_surfaces)
        for surface in _SURFACE_ORDER
    }
    data_category_counts = {
        category: sum(1 for record in records if category in record.data_categories)
        for category in _DATA_CATEGORY_ORDER
    }
    return TaskDataResidencyPlan(
        plan_id=plan_id,
        records=records,
        review_task_ids=review_task_ids,
        summary={
            "task_count": len(tasks),
            "review_task_count": len(review_task_ids),
            "status_counts": status_counts,
            "surface_counts": surface_counts,
            "data_category_counts": data_category_counts,
        },
    )


def analyze_task_data_residency(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskDataResidencyPlan:
    """Compatibility alias for building data residency plans."""
    return build_task_data_residency_plan(source)


def summarize_task_data_residency(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskDataResidencyPlan:
    """Compatibility alias for building data residency plans."""
    return build_task_data_residency_plan(source)


def task_data_residency_plan_to_dict(result: TaskDataResidencyPlan) -> dict[str, Any]:
    """Serialize a task data residency plan to a plain dictionary."""
    return result.to_dict()


task_data_residency_plan_to_dict.__test__ = False


def task_data_residency_plan_to_markdown(result: TaskDataResidencyPlan) -> str:
    """Render a task data residency plan as Markdown."""
    return result.to_markdown()


task_data_residency_plan_to_markdown.__test__ = False


def _task_record(task: Mapping[str, Any], index: int) -> TaskDataResidencyRecord:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    signals = _signals(task)
    status = _review_status(signals, task)
    return TaskDataResidencyRecord(
        task_id=task_id,
        title=title,
        residency_review_status=status,
        region_signals=signals.regions,
        affected_surfaces=signals.surfaces,
        data_categories=signals.data_categories,
        review_reasons=_review_reasons(signals, status),
        recommended_actions=_recommended_actions(signals, status),
        evidence=signals.evidence,
    )


@dataclass(frozen=True, slots=True)
class _Signals:
    regions: tuple[str, ...] = field(default_factory=tuple)
    surfaces: tuple[str, ...] = field(default_factory=tuple)
    data_categories: tuple[str, ...] = field(default_factory=tuple)
    region_evidence: tuple[str, ...] = field(default_factory=tuple)
    cross_border_evidence: tuple[str, ...] = field(default_factory=tuple)
    surface_evidence: tuple[str, ...] = field(default_factory=tuple)
    data_evidence: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    @property
    def has_region(self) -> bool:
        return bool(self.region_evidence)

    @property
    def has_cross_border(self) -> bool:
        return bool(self.cross_border_evidence)

    @property
    def has_surface(self) -> bool:
        return bool(self.surface_evidence)

    @property
    def has_data_category(self) -> bool:
        return bool(self.data_evidence)


def _signals(task: Mapping[str, Any]) -> _Signals:
    regions: set[str] = set()
    surfaces: set[str] = set()
    data_categories: set[str] = set()
    region_evidence: list[str] = []
    cross_border_evidence: list[str] = []
    surface_evidence: list[str] = []
    data_evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        path_evidence = f"files_or_modules: {path}"
        path_text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        for surface, pattern in _PATH_SURFACE_PATTERNS:
            if pattern.search(normalized) or pattern.search(path_text):
                surfaces.add(surface)
                surface_evidence.append(path_evidence)
        for region in _region_tokens(path_text):
            regions.add(region)
            region_evidence.append(path_evidence)
        if _CROSS_BORDER_RE.search(path_text):
            cross_border_evidence.append(path_evidence)
        for category, pattern in _DATA_CATEGORY_PATTERNS:
            if pattern.search(path_text):
                data_categories.add(category)
                data_evidence.append(path_evidence)

    for source_field, text in _candidate_texts(task):
        for region in _region_tokens(text):
            regions.add(region)
            region_evidence.append(_evidence_snippet(source_field, text))
        if _CROSS_BORDER_RE.search(text):
            cross_border_evidence.append(_evidence_snippet(source_field, text))
        for surface, pattern in _TEXT_SURFACE_PATTERNS:
            if pattern.search(text):
                surfaces.add(surface)
                surface_evidence.append(_evidence_snippet(source_field, text))
        for category, pattern in _DATA_CATEGORY_PATTERNS:
            if pattern.search(text):
                data_categories.add(category)
                data_evidence.append(_evidence_snippet(source_field, text))

    ordered_regions = tuple(sorted(regions, key=lambda value: value.casefold()))
    ordered_surfaces = tuple(surface for surface in _SURFACE_ORDER if surface in surfaces)
    ordered_categories = tuple(
        category for category in _DATA_CATEGORY_ORDER if category in data_categories
    )
    evidence = tuple(
        _dedupe([*region_evidence, *cross_border_evidence, *surface_evidence, *data_evidence])
    )
    return _Signals(
        regions=ordered_regions,
        surfaces=ordered_surfaces,
        data_categories=ordered_categories,
        region_evidence=tuple(_dedupe(region_evidence)),
        cross_border_evidence=tuple(_dedupe(cross_border_evidence)),
        surface_evidence=tuple(_dedupe(surface_evidence)),
        data_evidence=tuple(_dedupe(data_evidence)),
        evidence=evidence,
    )


def _review_status(signals: _Signals, task: Mapping[str, Any]) -> ResidencyReviewStatus:
    if not signals.evidence:
        return "residency_review_not_needed"
    if _is_doc_or_test_only(task) and not (signals.has_region or signals.has_cross_border):
        return "residency_review_not_needed"
    if signals.has_cross_border or (signals.has_region and signals.has_surface):
        return "residency_review_required"
    if signals.has_region or (signals.has_surface and signals.has_data_category):
        return "residency_review_recommended"
    return "residency_review_not_needed"


def _review_reasons(
    signals: _Signals,
    status: ResidencyReviewStatus,
) -> tuple[str, ...]:
    if status == "residency_review_not_needed":
        return ()
    reasons: list[str] = []
    if signals.has_region:
        reasons.append("Task references geographic regions or residency jurisdictions.")
    if signals.has_cross_border:
        reasons.append(
            "Task references cross-border movement, routing, replication, or processor changes."
        )
    if signals.has_surface:
        reasons.append(
            "Task touches storage, routing, backup, analytics, cache, log, or processor surfaces."
        )
    if signals.has_data_category:
        reasons.append(
            "Task references data categories with residency or localization implications."
        )
    return tuple(_dedupe(reasons))


def _recommended_actions(
    signals: _Signals,
    status: ResidencyReviewStatus,
) -> tuple[str, ...]:
    if status == "residency_review_not_needed":
        return ()
    actions = [
        "Confirm source and destination regions, residency commitments, and allowed transfer mechanism before implementation.",
        "Document affected storage, processing, cache, log, backup, and analytics surfaces with owners.",
    ]
    if signals.has_data_category:
        actions.append(
            "Classify the affected data categories and confirm whether personal, payment, authentication, log, backup, or analytics data leaves its approved region."
        )
    if "backups" in signals.surfaces or "replicas" in signals.surfaces:
        actions.append(
            "Verify replica, backup, restore, retention, and disaster-recovery locations comply with residency requirements."
        )
    if "logs" in signals.surfaces or "analytics exports" in signals.surfaces:
        actions.append(
            "Review log, telemetry, warehouse, and analytics export destinations plus retention and access controls."
        )
    if "cdn caches" in signals.surfaces:
        actions.append(
            "Confirm CDN or edge cache locations, purge behavior, and cache-key data do not violate localization commitments."
        )
    if "third-party processors" in signals.surfaces:
        actions.append(
            "Validate processor or subprocessor region coverage, data processing terms, and transfer impact assessment needs."
        )
    return tuple(_dedupe(actions))


def _region_tokens(text: str) -> list[str]:
    tokens: list[str] = []
    for match in _REGION_RE.finditer(text):
        token = match.group(0).replace("_", "-").lower()
        token = re.sub(r"\s+", "-", token)
        aliases = {
            "europe": "eu",
            "european-union": "eu",
            "eea": "eu",
            "gdpr": "eu",
            "usa": "us",
            "united-states": "us",
            "north-america": "us",
            "united-kingdom": "uk",
            "asia-pacific": "apac",
            "canada": "ca",
            "australia": "au",
        }
        tokens.append(aliases.get(token, token))
    return _dedupe(tokens)


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
            key_text = str(key).replace("_", " ")
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if _REGION_RE.search(key_text) or _CROSS_BORDER_RE.search(key_text):
                    texts.append((field, f"{key_text}: {text}"))
            elif _REGION_RE.search(key_text) or _CROSS_BORDER_RE.search(key_text):
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
    "ResidencyReviewStatus",
    "TaskDataResidencyPlan",
    "TaskDataResidencyRecord",
    "analyze_task_data_residency",
    "build_task_data_residency_plan",
    "summarize_task_data_residency",
    "task_data_residency_plan_to_dict",
    "task_data_residency_plan_to_markdown",
]
