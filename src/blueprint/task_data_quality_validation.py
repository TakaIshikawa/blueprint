"""Plan data quality validation for data-moving execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


DataQualityValidationCategory = Literal[
    "completeness",
    "uniqueness",
    "referential_integrity",
    "freshness",
    "range_validation",
    "reconciliation",
    "anomaly_checks",
]
DataQualityValidationSeverity = Literal["critical", "high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_CATEGORY_ORDER: dict[DataQualityValidationCategory, int] = {
    "completeness": 0,
    "uniqueness": 1,
    "referential_integrity": 2,
    "freshness": 3,
    "range_validation": 4,
    "reconciliation": 5,
    "anomaly_checks": 6,
}
_SEVERITY_ORDER: dict[DataQualityValidationSeverity, int] = {
    "critical": 0,
    "high": 1,
    "medium": 2,
    "low": 3,
}
_OPERATION_PATTERNS: dict[str, re.Pattern[str]] = {
    "migration": re.compile(r"\b(?:migration|migrate|data migration|cutover|move data|port data)\b", re.I),
    "destructive_migration": re.compile(
        r"\b(?:destructive migration|drop table|drop column|truncate|delete existing|purge|"
        r"overwrite production|irreversible|destructive backfill)\b",
        re.I,
    ),
    "import_export": re.compile(r"\b(?:import|export|csv|jsonl|extract|load file|bulk upload|bulk download)\b", re.I),
    "etl": re.compile(r"\b(?:etl|elt|pipeline|transform|data pipeline|ingest|loader|sync|replicate)\b", re.I),
    "backfill": re.compile(r"\b(?:backfill|reprocess|replay|historical load|catch up)\b", re.I),
    "reporting": re.compile(r"\b(?:report|reporting|dashboard|metric|kpi|business intelligence|bi)\b", re.I),
    "analytics": re.compile(r"\b(?:analytics|warehouse|data mart|event stream|tracking event|cohort)\b", re.I),
    "deduplication": re.compile(r"\b(?:dedupe|deduplicate|de-duplicate|duplicate|unique|idempotent)\b", re.I),
    "normalization": re.compile(r"\b(?:normalize|normalise|canonical|standardize|standardise|cleanse|sanitize)\b", re.I),
    "schema_mapping": re.compile(r"\b(?:schema mapping|field mapping|column mapping|map fields?|mapping table|crosswalk)\b", re.I),
    "reconciliation": re.compile(r"\b(?:reconcile|reconciliation|tie[- ]?out|source[- ]?to[- ]?target|source of truth)\b", re.I),
}
_QUALITY_PATTERNS: dict[DataQualityValidationCategory, re.Pattern[str]] = {
    "completeness": re.compile(r"\b(?:complete|completeness|null|missing|row count|record count|required field)\b", re.I),
    "uniqueness": re.compile(r"\b(?:unique|uniqueness|duplicate|dedupe|primary key|natural key|idempotenc)\b", re.I),
    "referential_integrity": re.compile(
        r"\b(?:referential integrity|foreign key|relationship|orphan|parent|child|join integrity)\b",
        re.I,
    ),
    "freshness": re.compile(r"\b(?:freshness|fresh|stale|lag|latency|watermark|as of|sla|window)\b", re.I),
    "range_validation": re.compile(
        r"\b(?:range|bounds?|min|max|negative|positive|currency|amount|date boundary|valid values?|enum)\b",
        re.I,
    ),
    "reconciliation": re.compile(r"\b(?:reconcile|reconciliation|checksum|diff|parity|variance|threshold|tie[- ]?out)\b", re.I),
    "anomaly_checks": re.compile(r"\b(?:anomaly|outlier|spike|drop|drift|distribution|baseline)\b", re.I),
}
_SENSITIVE_PATTERNS: dict[str, re.Pattern[str]] = {
    "customer_data": re.compile(r"\b(?:customer data|user data|pii|personal data|profile|email address|phone)\b", re.I),
    "billing_data": re.compile(r"\b(?:billing|invoice|payment|ledger|revenue|stripe|card|bank|subscription)\b", re.I),
    "compliance_data": re.compile(r"\b(?:compliance|gdpr|ccpa|hipaa|sox|audit evidence|regulated|retention)\b", re.I),
}
_EXPLICIT_METADATA_KEYS: dict[str, re.Pattern[str]] = {
    "expected_checks": re.compile(r"\b(?:expected checks?|validation checks?|quality checks?|data quality)\b", re.I),
    "source_datasets": re.compile(r"\b(?:source datasets?|sources?|source tables?|input datasets?)\b", re.I),
    "destination_datasets": re.compile(
        r"\b(?:destination datasets?|target datasets?|destinations?|target tables?|output datasets?)\b",
        re.I,
    ),
    "freshness_windows": re.compile(r"\b(?:freshness windows?|freshness|sla|latency windows?|watermark)\b", re.I),
    "reconciliation_thresholds": re.compile(r"\b(?:reconciliation thresholds?|thresholds?|variance|tolerance)\b", re.I),
}


@dataclass(frozen=True, slots=True)
class TaskDataQualityValidationRecord:
    """Data quality validation guidance for one execution task."""

    task_id: str
    title: str
    severity: DataQualityValidationSeverity
    validation_categories: tuple[DataQualityValidationCategory, ...]
    expected_checks: tuple[str, ...] = field(default_factory=tuple)
    source_datasets: tuple[str, ...] = field(default_factory=tuple)
    destination_datasets: tuple[str, ...] = field(default_factory=tuple)
    freshness_windows: tuple[str, ...] = field(default_factory=tuple)
    reconciliation_thresholds: tuple[str, ...] = field(default_factory=tuple)
    suggested_validation_artifacts: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    follow_up_questions: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "severity": self.severity,
            "validation_categories": list(self.validation_categories),
            "expected_checks": list(self.expected_checks),
            "source_datasets": list(self.source_datasets),
            "destination_datasets": list(self.destination_datasets),
            "freshness_windows": list(self.freshness_windows),
            "reconciliation_thresholds": list(self.reconciliation_thresholds),
            "suggested_validation_artifacts": list(self.suggested_validation_artifacts),
            "evidence": list(self.evidence),
            "follow_up_questions": list(self.follow_up_questions),
        }


@dataclass(frozen=True, slots=True)
class TaskDataQualityValidationPlan:
    """Plan-level data quality validation review."""

    plan_id: str | None = None
    records: tuple[TaskDataQualityValidationRecord, ...] = field(default_factory=tuple)
    validation_required_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_validation_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "validation_required_task_ids": list(self.validation_required_task_ids),
            "no_validation_task_ids": list(self.no_validation_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return data quality validation records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    @property
    def findings(self) -> tuple[TaskDataQualityValidationRecord, ...]:
        """Compatibility view matching planners that name task records findings."""
        return self.records

    def to_markdown(self) -> str:
        """Render the data quality validation plan as deterministic Markdown."""
        title = "# Task Data Quality Validation Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        category_counts = self.summary.get("category_counts", {})
        severity_counts = self.summary.get("severity_counts", {})
        lines = [
            title,
            "",
            (
                f"Summary: {self.summary.get('validation_required_task_count', 0)} validation-required tasks "
                f"across {self.summary.get('task_count', 0)} total tasks "
                f"(no validation: {self.summary.get('no_validation_task_count', 0)})."
            ),
            (
                "Categories: "
                f"completeness {category_counts.get('completeness', 0)}, "
                f"uniqueness {category_counts.get('uniqueness', 0)}, "
                f"referential_integrity {category_counts.get('referential_integrity', 0)}, "
                f"freshness {category_counts.get('freshness', 0)}, "
                f"range_validation {category_counts.get('range_validation', 0)}, "
                f"reconciliation {category_counts.get('reconciliation', 0)}, "
                f"anomaly_checks {category_counts.get('anomaly_checks', 0)}."
            ),
            (
                "Severity: "
                f"critical {severity_counts.get('critical', 0)}, "
                f"high {severity_counts.get('high', 0)}, "
                f"medium {severity_counts.get('medium', 0)}, "
                f"low {severity_counts.get('low', 0)}."
            ),
        ]
        if not self.records:
            lines.extend(["", "No data quality validation records were inferred."])
            if self.no_validation_task_ids:
                lines.extend(["", f"No-validation tasks: {_markdown_cell(', '.join(self.no_validation_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Severity | Categories | Expected Checks | Source Data | Destination Data | Freshness | Reconciliation | Artifacts | Evidence | Follow-up Questions |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` {_markdown_cell(record.title)} | "
                f"{record.severity} | "
                f"{_markdown_cell(', '.join(record.validation_categories))} | "
                f"{_markdown_cell('; '.join(record.expected_checks) or 'infer from task')} | "
                f"{_markdown_cell('; '.join(record.source_datasets) or 'unspecified')} | "
                f"{_markdown_cell('; '.join(record.destination_datasets) or 'unspecified')} | "
                f"{_markdown_cell('; '.join(record.freshness_windows) or 'unspecified')} | "
                f"{_markdown_cell('; '.join(record.reconciliation_thresholds) or 'unspecified')} | "
                f"{_markdown_cell('; '.join(record.suggested_validation_artifacts))} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} | "
                f"{_markdown_cell('; '.join(record.follow_up_questions) or 'none')} |"
            )
        if self.no_validation_task_ids:
            lines.extend(["", f"No-validation tasks: {_markdown_cell(', '.join(self.no_validation_task_ids))}"])
        return "\n".join(lines)


def build_task_data_quality_validation_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskDataQualityValidationPlan:
    """Build data quality validation guidance for data-moving execution tasks."""
    plan_id, tasks = _source_payload(source)
    candidates = [_task_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (
                _SEVERITY_ORDER[record.severity],
                min(_CATEGORY_ORDER[category] for category in record.validation_categories),
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    no_validation_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if candidates[index - 1] is None
    )
    category_counts = {
        category: sum(1 for record in records if category in record.validation_categories)
        for category in _CATEGORY_ORDER
    }
    severity_counts = {
        severity: sum(1 for record in records if record.severity == severity)
        for severity in _SEVERITY_ORDER
    }
    return TaskDataQualityValidationPlan(
        plan_id=plan_id,
        records=records,
        validation_required_task_ids=tuple(record.task_id for record in records),
        no_validation_task_ids=no_validation_task_ids,
        summary={
            "task_count": len(tasks),
            "validation_required_task_count": len(records),
            "no_validation_task_count": len(no_validation_task_ids),
            "category_counts": category_counts,
            "severity_counts": severity_counts,
        },
    )


def derive_task_data_quality_validation_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskDataQualityValidationPlan:
    """Compatibility alias for building data quality validation plans."""
    return build_task_data_quality_validation_plan(source)


def summarize_task_data_quality_validation(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskDataQualityValidationPlan:
    """Compatibility alias matching other task-level planners."""
    return build_task_data_quality_validation_plan(source)


def task_data_quality_validation_plan_to_dict(
    result: TaskDataQualityValidationPlan,
) -> dict[str, Any]:
    """Serialize a data quality validation plan to a plain dictionary."""
    return result.to_dict()


task_data_quality_validation_plan_to_dict.__test__ = False


def task_data_quality_validation_plan_to_markdown(
    result: TaskDataQualityValidationPlan,
) -> str:
    """Render a data quality validation plan as Markdown."""
    return result.to_markdown()


task_data_quality_validation_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    categories: tuple[DataQualityValidationCategory, ...] = field(default_factory=tuple)
    operations: tuple[str, ...] = field(default_factory=tuple)
    sensitive_data: tuple[str, ...] = field(default_factory=tuple)
    expected_checks: tuple[str, ...] = field(default_factory=tuple)
    source_datasets: tuple[str, ...] = field(default_factory=tuple)
    destination_datasets: tuple[str, ...] = field(default_factory=tuple)
    freshness_windows: tuple[str, ...] = field(default_factory=tuple)
    reconciliation_thresholds: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    explicit_metadata: tuple[str, ...] = field(default_factory=tuple)


def _task_record(task: Mapping[str, Any], index: int) -> TaskDataQualityValidationRecord | None:
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    signals = _signals(task)
    if not signals.categories:
        return None
    return TaskDataQualityValidationRecord(
        task_id=task_id,
        title=title,
        severity=_severity(signals),
        validation_categories=signals.categories,
        expected_checks=signals.expected_checks,
        source_datasets=signals.source_datasets,
        destination_datasets=signals.destination_datasets,
        freshness_windows=signals.freshness_windows,
        reconciliation_thresholds=signals.reconciliation_thresholds,
        suggested_validation_artifacts=_suggested_validation_artifacts(signals),
        evidence=signals.evidence,
        follow_up_questions=_follow_up_questions(signals),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    categories: set[DataQualityValidationCategory] = set()
    operations: set[str] = set()
    sensitive_data: set[str] = set()
    expected_checks: list[str] = []
    source_datasets: list[str] = []
    destination_datasets: list[str] = []
    freshness_windows: list[str] = []
    reconciliation_thresholds: list[str] = []
    evidence: list[str] = []
    explicit_metadata: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        path_operations, path_categories = _path_signals(normalized)
        if path_operations or path_categories:
            operations.update(path_operations)
            categories.update(path_categories)
            evidence.append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        snippet = _evidence_snippet(source_field, text)
        metadata_kind = _explicit_metadata_kind(source_field)
        if metadata_kind:
            explicit_metadata.append(source_field)
            if metadata_kind == "expected_checks":
                expected_checks.append(text)
                categories.update(_categories_from_expected_check(text))
            elif metadata_kind == "source_datasets":
                source_datasets.append(text)
            elif metadata_kind == "destination_datasets":
                destination_datasets.append(text)
            elif metadata_kind == "freshness_windows":
                freshness_windows.append(text)
                categories.add("freshness")
            elif metadata_kind == "reconciliation_thresholds":
                reconciliation_thresholds.append(text)
                categories.add("reconciliation")
            evidence.append(snippet)
            continue

        matched = False
        for operation, pattern in _OPERATION_PATTERNS.items():
            if pattern.search(text):
                operations.add(operation)
                matched = True
        for category, pattern in _QUALITY_PATTERNS.items():
            if pattern.search(text):
                categories.add(category)
                matched = True
        for data_type, pattern in _SENSITIVE_PATTERNS.items():
            if pattern.search(text):
                sensitive_data.add(data_type)
                matched = True
        if matched:
            evidence.append(snippet)

    for command in _validation_commands(task):
        snippet = _evidence_snippet("validation_commands", command)
        command_text = command.replace("/", " ").replace("_", " ").replace("-", " ")
        matched = False
        for category, pattern in _QUALITY_PATTERNS.items():
            if pattern.search(command) or pattern.search(command_text):
                categories.add(category)
                matched = True
        if any(pattern.search(command) or pattern.search(command_text) for pattern in _OPERATION_PATTERNS.values()):
            matched = True
        if matched:
            evidence.append(snippet)

    categories.update(_categories_from_operations(operations))

    return _Signals(
        categories=tuple(category for category in _CATEGORY_ORDER if category in categories),
        operations=tuple(sorted(operations)),
        sensitive_data=tuple(sorted(sensitive_data)),
        expected_checks=tuple(_dedupe(expected_checks)),
        source_datasets=tuple(_dedupe(source_datasets)),
        destination_datasets=tuple(_dedupe(destination_datasets)),
        freshness_windows=tuple(_dedupe(freshness_windows)),
        reconciliation_thresholds=tuple(_dedupe(reconciliation_thresholds)),
        evidence=tuple(_dedupe(evidence)),
        explicit_metadata=tuple(_dedupe(explicit_metadata)),
    )


def _path_signals(path: str) -> tuple[set[str], set[DataQualityValidationCategory]]:
    normalized = path.casefold()
    posix = PurePosixPath(normalized)
    parts = set(posix.parts)
    suffix = posix.suffix
    text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    operations: set[str] = set()
    categories: set[DataQualityValidationCategory] = set()
    if bool({"migrations", "migration", "alembic", "db", "database"} & parts):
        operations.add("migration")
    if bool({"imports", "exports", "import", "export", "csv"} & parts) or suffix in {".csv", ".jsonl"}:
        operations.add("import_export")
    if bool({"etl", "elt", "pipelines", "pipeline", "ingest", "loaders", "warehouse"} & parts):
        operations.add("etl")
    if "backfill" in text or "reprocess" in text:
        operations.add("backfill")
    if any(token in text for token in ("report", "dashboard", "analytics", "metrics")) or "bi" in parts:
        operations.add("analytics")
    if any(token in text for token in ("dedupe", "dedup", "duplicate", "unique")):
        operations.add("deduplication")
        categories.add("uniqueness")
    if any(token in text for token in ("normalize", "normalise", "canonical", "standardize", "cleanse")):
        operations.add("normalization")
        categories.add("range_validation")
    if any(token in text for token in ("mapping", "crosswalk", "foreign key", "fk")):
        operations.add("schema_mapping")
        categories.add("referential_integrity")
    if any(token in text for token in ("reconcile", "reconciliation", "checksum", "parity")):
        operations.add("reconciliation")
        categories.add("reconciliation")
    if any(token in text for token in ("freshness", "watermark", "stale", "lag")):
        categories.add("freshness")
    if any(token in text for token in ("anomaly", "outlier", "distribution", "drift")):
        categories.add("anomaly_checks")
    return operations, categories


def _categories_from_operations(operations: Iterable[str]) -> set[DataQualityValidationCategory]:
    operation_set = set(operations)
    categories: set[DataQualityValidationCategory] = set()
    if operation_set & {"migration", "import_export", "etl", "backfill", "reporting", "analytics"}:
        categories.add("completeness")
    if operation_set & {"migration", "schema_mapping", "etl"}:
        categories.add("referential_integrity")
    if operation_set & {"etl", "backfill", "reporting", "analytics"}:
        categories.add("freshness")
    if operation_set & {"etl", "normalization", "schema_mapping", "analytics", "reporting"}:
        categories.add("range_validation")
    if operation_set & {"migration", "import_export", "etl", "backfill", "reconciliation"}:
        categories.add("reconciliation")
    if operation_set & {"deduplication", "migration", "import_export", "etl"}:
        categories.add("uniqueness")
    if operation_set & {"etl", "analytics", "reporting"}:
        categories.add("anomaly_checks")
    return categories


def _categories_from_expected_check(text: str) -> set[DataQualityValidationCategory]:
    normalized = text.replace("_", " ").replace("-", " ")
    return {category for category, pattern in _QUALITY_PATTERNS.items() if pattern.search(normalized)}


def _severity(signals: _Signals) -> DataQualityValidationSeverity:
    sensitive = set(signals.sensitive_data)
    operations = set(signals.operations)
    if "destructive_migration" in operations or "compliance_data" in sensitive or "billing_data" in sensitive:
        return "critical"
    if "customer_data" in sensitive:
        return "high"
    if (
        signals.explicit_metadata
        and signals.expected_checks
        and signals.source_datasets
        and signals.destination_datasets
        and ("freshness" not in signals.categories or signals.freshness_windows)
        and ("reconciliation" not in signals.categories or signals.reconciliation_thresholds)
    ):
        return "low"
    if operations & {"migration", "backfill", "reconciliation"}:
        return "high"
    if len(signals.categories) >= 4:
        return "high"
    return "medium"


def _suggested_validation_artifacts(signals: _Signals) -> tuple[str, ...]:
    categories = set(signals.categories)
    artifacts: list[str] = []
    if "completeness" in categories:
        artifacts.append("Row-count, null-rate, and required-field completeness report.")
    if "uniqueness" in categories:
        artifacts.append("Uniqueness query for primary, natural, and idempotency keys.")
    if "referential_integrity" in categories:
        artifacts.append("Referential integrity query covering foreign keys, joins, and orphan records.")
    if "freshness" in categories:
        artifacts.append("Freshness SLA check with watermark, lag, and last-success timestamp evidence.")
    if "range_validation" in categories:
        artifacts.append("Range, enum, currency, date-boundary, and domain-value validation fixtures.")
    if "reconciliation" in categories:
        artifacts.append("Source-to-destination reconciliation report with count, checksum, and variance thresholds.")
    if "anomaly_checks" in categories:
        artifacts.append("Anomaly check comparing distributions, spikes, drops, and historical baselines.")
    if not signals.source_datasets or not signals.destination_datasets:
        artifacts.append("Dataset inventory naming each source, destination, owner, and refresh cadence.")
    return tuple(_dedupe(artifacts))


def _follow_up_questions(signals: _Signals) -> tuple[str, ...]:
    questions: list[str] = []
    if not signals.expected_checks:
        questions.append("Which data quality checks must pass before the task can be accepted?")
    if not signals.source_datasets:
        questions.append("Which source datasets, tables, files, or event streams are authoritative?")
    if not signals.destination_datasets:
        questions.append("Which destination datasets must be reconciled after implementation?")
    if "freshness" in signals.categories and not signals.freshness_windows:
        questions.append("What freshness window, watermark, or maximum lag is acceptable?")
    if "reconciliation" in signals.categories and not signals.reconciliation_thresholds:
        questions.append("What reconciliation threshold or allowed variance is acceptable?")
    return tuple(questions)


def _explicit_metadata_kind(source_field: str) -> str | None:
    if not source_field.startswith("metadata."):
        return None
    key = source_field.rsplit(".", 1)[-1].replace("_", " ")
    for kind, pattern in _EXPLICIT_METADATA_KEYS.items():
        if pattern.search(key):
            return kind
    parent = source_field.split(".")[-2].replace("_", " ") if "." in source_field else ""
    for kind, pattern in _EXPLICIT_METADATA_KEYS.items():
        if pattern.search(parent):
            return kind
    return None


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
        "test_commands",
        "validation_command",
        "validation_commands",
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
    for field_name in ("depends_on", "files_or_modules", "files", "acceptance_criteria", "tags", "labels", "notes"):
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
                if _any_signal(key_text) and not any(
                    pattern.search(key_text) for pattern in _EXPLICIT_METADATA_KEYS.values()
                ) and _explicit_metadata_kind(field) is None:
                    texts.append((field, str(key)))
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if _any_signal(key_text) and not any(
                    pattern.search(key_text) for pattern in _EXPLICIT_METADATA_KEYS.values()
                ) and _explicit_metadata_kind(field) is None:
                    texts.append((field, f"{key_text}: {text}"))
            elif _any_signal(key_text):
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


def _any_signal(text: str) -> bool:
    return any(
        pattern.search(text)
        for pattern in (*_OPERATION_PATTERNS.values(), *_QUALITY_PATTERNS.values(), *_SENSITIVE_PATTERNS.values())
    ) or any(pattern.search(text) for pattern in _EXPLICIT_METADATA_KEYS.values())


def _validation_commands(task: Mapping[str, Any]) -> list[str]:
    commands: list[str] = []
    metadata = task.get("metadata")
    for key in ("validation_commands", "validation_command", "test_commands", "test_command"):
        if value := task.get(key):
            if isinstance(value, Mapping):
                commands.extend(flatten_validation_commands(value))
            else:
                commands.extend(_strings(value))
        if isinstance(metadata, Mapping) and (value := metadata.get(key)):
            if isinstance(value, Mapping):
                commands.extend(flatten_validation_commands(value))
            else:
                commands.extend(_strings(value))
    return _dedupe(commands)


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
    "DataQualityValidationCategory",
    "DataQualityValidationSeverity",
    "TaskDataQualityValidationPlan",
    "TaskDataQualityValidationRecord",
    "build_task_data_quality_validation_plan",
    "derive_task_data_quality_validation_plan",
    "summarize_task_data_quality_validation",
    "task_data_quality_validation_plan_to_dict",
    "task_data_quality_validation_plan_to_markdown",
]
