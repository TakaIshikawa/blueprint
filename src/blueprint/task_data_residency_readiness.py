"""Plan data residency readiness work for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


DataResidencySignal = Literal[
    "data_residency",
    "strict_region",
    "allowed_regions",
    "prohibited_regions",
    "multi_region_constraint",
    "storage_boundary",
    "replication_policy",
    "backup_location",
    "observability",
    "compliance_evidence",
    "rollout_validation",
]
DataResidencyReadinessCategory = Literal[
    "region_selection",
    "storage_boundaries",
    "replication_policy",
    "backup_location",
    "observability",
    "compliance_evidence",
    "rollout_validation",
]
DataResidencyReadinessLevel = Literal["needs_planning", "partial", "ready"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_READINESS_ORDER: dict[DataResidencyReadinessLevel, int] = {
    "needs_planning": 0,
    "partial": 1,
    "ready": 2,
}
_SIGNAL_ORDER: tuple[DataResidencySignal, ...] = (
    "data_residency",
    "strict_region",
    "allowed_regions",
    "prohibited_regions",
    "multi_region_constraint",
    "storage_boundary",
    "replication_policy",
    "backup_location",
    "observability",
    "compliance_evidence",
    "rollout_validation",
)
_CATEGORY_ORDER: tuple[DataResidencyReadinessCategory, ...] = (
    "region_selection",
    "storage_boundaries",
    "replication_policy",
    "backup_location",
    "observability",
    "compliance_evidence",
    "rollout_validation",
)
_SIGNAL_PATTERNS: dict[DataResidencySignal, re.Pattern[str]] = {
    "data_residency": re.compile(
        r"\b(?:data residency|data sovereignty|resident data|residency requirement|regional residency|"
        r"jurisdictional data|in[- ]region data|tenant region|regional tenant|geo[- ]fenc(?:e|ing)|"
        r"residency|residency test|residency validation|residency checks?)\b",
        re.I,
    ),
    "strict_region": re.compile(
        r"\b(?:must|shall|required|only|never|cannot|must not|restricted to|remain(?:s)? in|stored in|"
        r"hosted in|processed in).{0,80}\b(?:region|jurisdiction|country|eu|eea|germany|france|"
        r"uk|united kingdom|us|united states|canada|australia|japan|singapore)\b|"
        r"\b(?:eu|eea|germany|france|uk|us|canada|australia|japan|singapore)[- ]only\b",
        re.I,
    ),
    "allowed_regions": re.compile(
        r"\b(?:allowed regions?|approved regions?|permitted regions?|supported regions?|region allowlist|"
        r"region whitelist|available regions?|choose region|region selection|data region)\b",
        re.I,
    ),
    "prohibited_regions": re.compile(
        r"\b(?:prohibited regions?|blocked regions?|disallowed regions?|forbidden regions?|region denylist|"
        r"region blacklist|must not leave|cannot leave|outside (?:the )?(?:eu|eea|us|uk|region))\b",
        re.I,
    ),
    "multi_region_constraint": re.compile(
        r"\b(?:multi[- ]region|multiple regions?|per[- ]region|regional shard|regional isolation|"
        r"region isolated|tenant isolation by region|eu and us|us and eu|cross[- ]region|"
        r"regional failover)\b",
        re.I,
    ),
    "storage_boundary": re.compile(
        r"\b(?:storage boundary|data boundary|storage location|database region|bucket region|object storage|"
        r"blob storage|regional database|regional datastore|data store region|queue region|cache region|"
        r"profile storage|resident storage|stored in|store resident|resident data stores?)\b",
        re.I,
    ),
    "replication_policy": re.compile(
        r"\b(?:replication policy|replicate|replication|read replica|replica region|cross[- ]region replica|"
        r"failover replica|secondary region|dr replica|disaster recovery)\b",
        re.I,
    ),
    "backup_location": re.compile(
        r"\b(?:backup location|backup region|regional backup|snapshots?|snapshot region|restore region|"
        r"archive region|backup storage|point[- ]in[- ]time restore|pitr)\b",
        re.I,
    ),
    "observability": re.compile(
        r"\b(?:observability|monitor(?:ing)?|alert(?:ing)?|metric|log|audit log|region drift|"
        r"residency drift|egress alert|cross[- ]region transfer|location telemetry)\b",
        re.I,
    ),
    "compliance_evidence": re.compile(
        r"\b(?:compliance evidence|audit evidence|evidence pack|attestation|auditor|audit trail|"
        r"data map|processing record|ropa|dpia|soc 2|iso 27001|gdpr evidence)\b",
        re.I,
    ),
    "rollout_validation": re.compile(
        r"\b(?:rollout validation|launch validation|cutover validation|pre[- ]launch validation|"
        r"residency test|regional test|migration validation|production validation|release gate)\b",
        re.I,
    ),
}
_PATH_SIGNAL_PATTERNS: dict[DataResidencySignal, re.Pattern[str]] = {
    "data_residency": re.compile(r"(?:data[_-]?residency|sovereignty|resident[_-]?data|tenant[_-]?region)", re.I),
    "strict_region": re.compile(r"(?:eu[_-]?only|us[_-]?only|region[_-]?lock|geo[_-]?fenc)", re.I),
    "allowed_regions": re.compile(r"(?:allowed[_-]?regions?|region[_-]?selection|region[_-]?allow)", re.I),
    "prohibited_regions": re.compile(r"(?:prohibited[_-]?regions?|blocked[_-]?regions?|region[_-]?deny)", re.I),
    "multi_region_constraint": re.compile(r"(?:multi[_-]?region|regional[_-]?shard|regional[_-]?isolation)", re.I),
    "storage_boundary": re.compile(r"(?:storage[_-]?boundary|database[_-]?region|bucket[_-]?region|datastore[_-]?region)", re.I),
    "replication_policy": re.compile(r"(?:replicat|read[_-]?replica|failover|disaster[_-]?recovery|dr[_-]?replica)", re.I),
    "backup_location": re.compile(r"(?:backup[_-]?region|snapshot[_-]?region|restore[_-]?region|archive[_-]?region|pitr)", re.I),
    "observability": re.compile(r"(?:observability|monitor|alert|region[_-]?drift|egress)", re.I),
    "compliance_evidence": re.compile(r"(?:compliance[_-]?evidence|audit[_-]?evidence|attestation|data[_-]?map)", re.I),
    "rollout_validation": re.compile(r"(?:rollout[_-]?validation|residency[_-]?test|regional[_-]?test|release[_-]?gate)", re.I),
}
_NO_RESIDENCY_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:data residency|data sovereignty|regional residency|"
    r"residency requirement|tenant region|regional storage|cross[- ]region|backup region)\b.{0,80}"
    r"\b(?:scope|impact|changes?|required|needed|requirements?)\b",
    re.I,
)
_REGION_RE = re.compile(
    r"\b(?:eu|eea|europe|germany|france|ireland|uk|united kingdom|us|usa|united states|"
    r"canada|australia|japan|singapore|apac|emea|aws [a-z]{2}-[a-z]+-\d|"
    r"gcp [a-z]+-[a-z]+\d|azure [a-z ]+)\b",
    re.I,
)
_CATEGORY_GUIDANCE: dict[DataResidencyReadinessCategory, tuple[str, tuple[str, ...]]] = {
    "region_selection": (
        "Select and enforce the allowed runtime regions for resident data.",
        (
            "Allowed regions are encoded in configuration or policy code with a deterministic default.",
            "Requests for unsupported regions fail closed with a test-covered error path.",
            "The selected region is visible in deployment or tenant metadata for downstream automation.",
        ),
    ),
    "storage_boundaries": (
        "Constrain databases, object stores, queues, caches, and derived data to approved regions.",
        (
            "Every storage service touched by the source task is mapped to an approved region.",
            "Infrastructure definitions prevent resident data stores from being created outside the boundary.",
            "Unit or integration tests cover at least one rejected out-of-bound storage configuration.",
        ),
    ),
    "replication_policy": (
        "Define where replicas, failover targets, and disaster recovery copies may exist.",
        (
            "Replication destinations are limited to approved regions or explicitly disabled.",
            "Failover behavior documents how residency is preserved during regional incidents.",
            "Automated validation checks replica or secondary-region configuration before rollout.",
        ),
    ),
    "backup_location": (
        "Keep backups, snapshots, archives, and restores inside the residency boundary.",
        (
            "Backup and snapshot locations are configured for the same approved region set as primary data.",
            "Restore procedures verify the target region before data is materialized.",
            "Retention and deletion settings are documented for residency-scoped backup artifacts.",
        ),
    ),
    "observability": (
        "Monitor residency drift, cross-region data movement, and regional configuration changes.",
        (
            "Metrics or logs identify the active data region for resident storage and processing paths.",
            "Alerts fire on unapproved region configuration, cross-region transfer, or residency drift.",
            "Dashboards or runbooks name the owner and response steps for residency alerts.",
        ),
    ),
    "compliance_evidence": (
        "Produce evidence that implementation choices satisfy the residency requirement.",
        (
            "The task records source evidence, approved regions, and implementation decisions in a reviewable artifact.",
            "Compliance evidence links to infrastructure, tests, dashboards, or runbooks that enforce residency.",
            "Evidence can be exported or inspected without requiring production data access.",
        ),
    ),
    "rollout_validation": (
        "Gate rollout on automated and manual checks that prove residency behavior in the target environment.",
        (
            "A pre-launch validation command or checklist verifies region selection and storage placement.",
            "Rollout has a rollback or halt condition for failed residency checks.",
            "Post-deploy validation confirms no resident data was written outside approved regions.",
        ),
    ),
}


@dataclass(frozen=True, slots=True)
class DataResidencyReadinessTask:
    """One generated implementation task for data residency readiness."""

    category: DataResidencyReadinessCategory
    title: str
    description: str
    acceptance_criteria: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "category": self.category,
            "title": self.title,
            "description": self.description,
            "acceptance_criteria": list(self.acceptance_criteria),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskDataResidencyReadinessRecord:
    """Data-residency readiness guidance for one execution task."""

    task_id: str
    title: str
    detected_signals: tuple[DataResidencySignal, ...]
    regions: tuple[str, ...] = field(default_factory=tuple)
    generated_tasks: tuple[DataResidencyReadinessTask, ...] = field(default_factory=tuple)
    readiness: DataResidencyReadinessLevel = "needs_planning"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    @property
    def matched_signals(self) -> tuple[DataResidencySignal, ...]:
        """Compatibility view for planners that name detected signals matched signals."""
        return self.detected_signals

    @property
    def recommended_tasks(self) -> tuple[DataResidencyReadinessTask, ...]:
        """Compatibility view for generated readiness tasks."""
        return self.generated_tasks

    @property
    def acceptance_criteria(self) -> tuple[str, ...]:
        """Flatten generated task acceptance criteria for simple consumers."""
        return tuple(criteria for task in self.generated_tasks for criteria in task.acceptance_criteria)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "detected_signals": list(self.detected_signals),
            "regions": list(self.regions),
            "generated_tasks": [task.to_dict() for task in self.generated_tasks],
            "readiness": self.readiness,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskDataResidencyReadinessPlan:
    """Plan-level data residency readiness tasks."""

    plan_id: str | None = None
    records: tuple[TaskDataResidencyReadinessRecord, ...] = field(default_factory=tuple)
    data_residency_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_impact_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def findings(self) -> tuple[TaskDataResidencyReadinessRecord, ...]:
        """Compatibility view matching planners that expose findings."""
        return self.records

    @property
    def recommendations(self) -> tuple[TaskDataResidencyReadinessRecord, ...]:
        """Compatibility view matching planners that expose recommendations."""
        return self.records

    @property
    def impacted_task_ids(self) -> tuple[str, ...]:
        """Compatibility view matching planners that expose impacted task ids."""
        return self.data_residency_task_ids

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "findings": [record.to_dict() for record in self.findings],
            "recommendations": [record.to_dict() for record in self.recommendations],
            "data_residency_task_ids": list(self.data_residency_task_ids),
            "impacted_task_ids": list(self.impacted_task_ids),
            "no_impact_task_ids": list(self.no_impact_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return data residency readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render data residency readiness guidance as deterministic Markdown."""
        title = "# Task Data Residency Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        readiness_counts = self.summary.get("readiness_counts", {})
        signal_counts = self.summary.get("signal_counts", {})
        category_counts = self.summary.get("generated_task_category_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Data-residency task count: {self.summary.get('data_residency_task_count', 0)}",
            f"- Generated readiness task count: {self.summary.get('generated_task_count', 0)}",
            "- Readiness counts: "
            + ", ".join(f"{level} {readiness_counts.get(level, 0)}" for level in _READINESS_ORDER),
            "- Signal counts: "
            + ", ".join(f"{signal} {signal_counts.get(signal, 0)}" for signal in _SIGNAL_ORDER),
            "- Generated task counts: "
            + ", ".join(f"{category} {category_counts.get(category, 0)}" for category in _CATEGORY_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No task data residency readiness records were inferred."])
            if self.no_impact_task_ids:
                lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Readiness | Signals | Regions | Generated Tasks | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            generated = "; ".join(f"{task.category}: {task.title}" for task in record.generated_tasks)
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.readiness} | "
                f"{_markdown_cell(', '.join(record.detected_signals) or 'none')} | "
                f"{_markdown_cell(', '.join(record.regions) or 'unspecified')} | "
                f"{_markdown_cell(generated or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.no_impact_task_ids:
            lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
        return "\n".join(lines)


def build_task_data_residency_readiness_plan(source: Any) -> TaskDataResidencyReadinessPlan:
    """Build data residency readiness records for task-shaped input."""
    plan_id, tasks = _source_payload(source)
    candidates = [_task_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (
                _READINESS_ORDER[record.readiness],
                -len(record.generated_tasks),
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    data_residency_task_ids = tuple(record.task_id for record in records)
    impacted = set(data_residency_task_ids)
    no_impact_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if _task_id(task, index) not in impacted
    )
    return TaskDataResidencyReadinessPlan(
        plan_id=plan_id,
        records=records,
        data_residency_task_ids=data_residency_task_ids,
        no_impact_task_ids=no_impact_task_ids,
        summary=_summary(records, task_count=len(tasks), no_impact_task_ids=no_impact_task_ids),
    )


def analyze_task_data_residency_readiness(source: Any) -> TaskDataResidencyReadinessPlan:
    """Compatibility alias for building data residency readiness plans."""
    return build_task_data_residency_readiness_plan(source)


def recommend_task_data_residency_readiness(source: Any) -> TaskDataResidencyReadinessPlan:
    """Compatibility alias for recommending data residency readiness tasks."""
    return build_task_data_residency_readiness_plan(source)


def summarize_task_data_residency_readiness(source: Any) -> TaskDataResidencyReadinessPlan:
    """Compatibility alias for summarizing data residency readiness plans."""
    return build_task_data_residency_readiness_plan(source)


def generate_task_data_residency_readiness(source: Any) -> TaskDataResidencyReadinessPlan:
    """Compatibility alias for generating data residency readiness plans."""
    return build_task_data_residency_readiness_plan(source)


def extract_task_data_residency_readiness(source: Any) -> TaskDataResidencyReadinessPlan:
    """Compatibility alias for extracting data residency readiness plans."""
    return build_task_data_residency_readiness_plan(source)


def derive_task_data_residency_readiness(source: Any) -> TaskDataResidencyReadinessPlan:
    """Compatibility alias for deriving data residency readiness plans."""
    return build_task_data_residency_readiness_plan(source)


def task_data_residency_readiness_plan_to_dict(result: TaskDataResidencyReadinessPlan) -> dict[str, Any]:
    """Serialize a data residency readiness plan to a plain dictionary."""
    return result.to_dict()


task_data_residency_readiness_plan_to_dict.__test__ = False


def task_data_residency_readiness_plan_to_dicts(
    result: TaskDataResidencyReadinessPlan | Iterable[TaskDataResidencyReadinessRecord],
) -> list[dict[str, Any]]:
    """Serialize data residency readiness records to plain dictionaries."""
    if isinstance(result, TaskDataResidencyReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_data_residency_readiness_plan_to_dicts.__test__ = False
task_data_residency_readiness_to_dicts = task_data_residency_readiness_plan_to_dicts
task_data_residency_readiness_to_dicts.__test__ = False


def task_data_residency_readiness_plan_to_markdown(result: TaskDataResidencyReadinessPlan) -> str:
    """Render a data residency readiness plan as Markdown."""
    return result.to_markdown()


task_data_residency_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    signals: tuple[DataResidencySignal, ...] = field(default_factory=tuple)
    regions: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    explicitly_no_impact: bool = False


def _task_record(task: Mapping[str, Any], index: int) -> TaskDataResidencyReadinessRecord | None:
    signals = _signals(task)
    if signals.explicitly_no_impact or not signals.signals:
        return None

    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    generated_tasks = _generated_tasks(title, signals)
    return TaskDataResidencyReadinessRecord(
        task_id=task_id,
        title=title,
        detected_signals=signals.signals,
        regions=signals.regions,
        generated_tasks=generated_tasks,
        readiness=_readiness(signals.signals, generated_tasks),
        evidence=signals.evidence,
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    signal_hits: set[DataResidencySignal] = set()
    regions: list[str] = []
    evidence: list[str] = []
    explicitly_no_impact = False

    for path in _strings(
        task.get("files_or_modules")
        or task.get("files")
        or task.get("expected_file_paths")
        or task.get("expected_files")
        or task.get("paths")
    ):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        matched = False
        for signal, pattern in _PATH_SIGNAL_PATTERNS.items():
            if pattern.search(normalized) or _SIGNAL_PATTERNS[signal].search(searchable):
                signal_hits.add(signal)
                matched = True
        regions.extend(_regions_from_text(searchable))
        if matched:
            evidence.append(f"files_or_modules: {path}")

    for source_field, text in [*_candidate_texts(task), *_validation_command_texts(task)]:
        if _NO_RESIDENCY_RE.search(text):
            explicitly_no_impact = True
        searchable = text.replace("/", " ").replace("_", " ").replace("-", " ")
        matched = False
        for signal, pattern in _SIGNAL_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                signal_hits.add(signal)
                matched = True
        regions.extend([*_regions_from_text(text), *_regions_from_text(searchable)])
        if matched:
            evidence.append(_evidence_snippet(source_field, text))

    if signal_hits & {
        "strict_region",
        "allowed_regions",
        "prohibited_regions",
        "multi_region_constraint",
        "storage_boundary",
        "replication_policy",
        "backup_location",
    }:
        signal_hits.add("data_residency")
    if signal_hits & {"replication_policy", "backup_location"}:
        signal_hits.add("storage_boundary")

    return _Signals(
        signals=tuple(signal for signal in _SIGNAL_ORDER if signal in signal_hits),
        regions=tuple(_dedupe(_normalize_region(region) for region in regions)),
        evidence=tuple(_dedupe(evidence)),
        explicitly_no_impact=explicitly_no_impact,
    )


def _generated_tasks(
    source_title: str,
    signals: _Signals,
) -> tuple[DataResidencyReadinessTask, ...]:
    categories: set[DataResidencyReadinessCategory] = {
        "region_selection",
        "storage_boundaries",
        "observability",
        "compliance_evidence",
        "rollout_validation",
    }
    if signals.signals:
        categories.update({"replication_policy", "backup_location"})
    if "replication_policy" in signals.signals or "multi_region_constraint" in signals.signals:
        categories.add("replication_policy")
    if "backup_location" in signals.signals:
        categories.add("backup_location")

    evidence = tuple(sorted(signals.evidence, key=_evidence_priority))[:3]
    rationale = "; ".join(evidence) if evidence else "Residency-related task context was detected."
    region_text = ", ".join(signals.regions) if signals.regions else "the approved residency region set"
    tasks: list[DataResidencyReadinessTask] = []
    for category in _CATEGORY_ORDER:
        if category not in categories:
            continue
        guidance, acceptance = _CATEGORY_GUIDANCE[category]
        title = f"{_category_title(category)} for {source_title}"
        description = f"{guidance} Target regions: {region_text}. Rationale: {rationale}"
        tasks.append(
            DataResidencyReadinessTask(
                category=category,
                title=title,
                description=description,
                acceptance_criteria=acceptance,
                evidence=evidence,
            )
        )
    return tuple(tasks)


def _readiness(
    signals: tuple[DataResidencySignal, ...],
    generated_tasks: tuple[DataResidencyReadinessTask, ...],
) -> DataResidencyReadinessLevel:
    categories = {task.category for task in generated_tasks}
    if set(_CATEGORY_ORDER) <= categories and {"observability", "compliance_evidence", "rollout_validation"} <= set(signals):
        return "ready"
    if {"replication_policy", "backup_location", "compliance_evidence"} & set(signals):
        return "partial"
    return "needs_planning"


def _summary(
    records: tuple[TaskDataResidencyReadinessRecord, ...],
    *,
    task_count: int,
    no_impact_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    generated_tasks = [task for record in records for task in record.generated_tasks]
    return {
        "task_count": task_count,
        "data_residency_task_count": len(records),
        "data_residency_task_ids": [record.task_id for record in records],
        "impacted_task_ids": [record.task_id for record in records],
        "no_impact_task_ids": list(no_impact_task_ids),
        "generated_task_count": len(generated_tasks),
        "readiness_counts": {
            readiness: sum(1 for record in records if record.readiness == readiness)
            for readiness in _READINESS_ORDER
        },
        "signal_counts": {
            signal: sum(1 for record in records if signal in record.detected_signals)
            for signal in _SIGNAL_ORDER
        },
        "generated_task_category_counts": {
            category: sum(1 for task in generated_tasks if task.category == category)
            for category in _CATEGORY_ORDER
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
        "expected_file_paths",
        "expected_files",
        "paths",
        "acceptance_criteria",
        "validation_plan",
        "validation_command",
        "validation_commands",
        "test_command",
        "test_commands",
        "estimated_complexity",
        "estimated_hours",
        "risk_level",
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
        "validation_plan",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in (
        "acceptance_criteria",
        "tags",
        "labels",
        "notes",
        "risks",
        "depends_on",
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
    return any(pattern.search(value) for pattern in [*_SIGNAL_PATTERNS.values()])


def _validation_command_texts(task: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    commands: list[str] = []
    metadata = task.get("metadata")
    for key in ("validation_commands", "validation_command", "test_commands", "test_command"):
        value = task.get(key)
        if isinstance(value, Mapping):
            commands.extend(flatten_validation_commands(value) or _strings(value))
        else:
            commands.extend(_strings(value))
        if isinstance(metadata, Mapping):
            metadata_value = metadata.get(key)
            if isinstance(metadata_value, Mapping):
                commands.extend(flatten_validation_commands(metadata_value) or _strings(metadata_value))
            else:
                commands.extend(_strings(metadata_value))
    return tuple(("validation_commands", command) for command in _dedupe(commands))


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
    path = value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")
    return str(PurePosixPath(path)) if path else ""


def _regions_from_text(text: str) -> list[str]:
    return [_text(match.group(0)) for match in _REGION_RE.finditer(text)]


def _normalize_region(value: str) -> str:
    text = _text(value)
    lower = text.casefold()
    if lower in {"eu", "eea", "uk", "us", "usa"}:
        return text.upper()
    return " ".join(part.capitalize() for part in text.split(" "))


def _category_title(category: DataResidencyReadinessCategory) -> str:
    return category.replace("_", " ").title()


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _evidence_priority(value: str) -> tuple[int, str]:
    if value.startswith("description:"):
        return (0, value)
    if value.startswith("metadata."):
        return (1, value)
    if value.startswith("acceptance_criteria"):
        return (2, value)
    return (3, value)


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
    "DataResidencyReadinessCategory",
    "DataResidencyReadinessLevel",
    "DataResidencyReadinessTask",
    "DataResidencySignal",
    "TaskDataResidencyReadinessPlan",
    "TaskDataResidencyReadinessRecord",
    "analyze_task_data_residency_readiness",
    "build_task_data_residency_readiness_plan",
    "derive_task_data_residency_readiness",
    "extract_task_data_residency_readiness",
    "generate_task_data_residency_readiness",
    "recommend_task_data_residency_readiness",
    "summarize_task_data_residency_readiness",
    "task_data_residency_readiness_plan_to_dict",
    "task_data_residency_readiness_plan_to_dicts",
    "task_data_residency_readiness_plan_to_markdown",
    "task_data_residency_readiness_to_dicts",
]
