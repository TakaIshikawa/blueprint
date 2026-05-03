"""Plan task-level readiness for rate-limit backoff behavior."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


TaskRateLimitBackoffCategory = Literal[
    "rate_limit_response",
    "retry_after_header",
    "exponential_backoff",
    "jitter",
    "worker_retry",
    "dead_lettering",
    "batch_retry",
    "user_retry_message",
]
TaskRateLimitBackoffSeverity = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_CATEGORY_ORDER: tuple[TaskRateLimitBackoffCategory, ...] = (
    "rate_limit_response",
    "retry_after_header",
    "exponential_backoff",
    "jitter",
    "worker_retry",
    "dead_lettering",
    "batch_retry",
    "user_retry_message",
)
_CATEGORY_SORT: dict[TaskRateLimitBackoffCategory, int] = {
    category: index for index, category in enumerate(_CATEGORY_ORDER)
}
_SEVERITY_ORDER: dict[TaskRateLimitBackoffSeverity, int] = {"high": 0, "medium": 1, "low": 2}
_CATEGORY_PATTERNS: dict[TaskRateLimitBackoffCategory, re.Pattern[str]] = {
    "rate_limit_response": re.compile(
        r"\b(?:429|too many requests|rate[- ]limit(?:ed|ing)?|throttl(?:e|ed|ing)|quota exhaustion|quota exceeded)\b",
        re.I,
    ),
    "retry_after_header": re.compile(r"\b(?:retry[- ]after|retry after header|x[- ]rate[- ]limit[- ]reset)\b", re.I),
    "exponential_backoff": re.compile(
        r"\b(?:exponential backoff|backoff schedule|backoff policy|retry backoff|progressive delay)\b",
        re.I,
    ),
    "jitter": re.compile(r"\b(?:jitter|randomi[sz]ed delay|decorrelated jitter|full jitter)\b", re.I),
    "worker_retry": re.compile(
        r"\b(?:worker retries?|queue retries?|job retries?|retry budget|retry storm|idempotent retries?|transient failure)\b",
        re.I,
    ),
    "dead_lettering": re.compile(
        r"\b(?:dead[- ]letter(?:ing)?|dlq|dead letter queue|poison (?:message|job)|failed jobs? queue)\b",
        re.I,
    ),
    "batch_retry": re.compile(
        r"\b(?:batch retries?|bulk retries?|chunk retries?|partial retry|retry failed batch|backfill retries?|fan[- ]?out retries?)\b",
        re.I,
    ),
    "user_retry_message": re.compile(
        r"\b(?:user[- ]facing retry|retry message|retry messaging|try again later|retry later|rate limit message|too many requests message|customer[- ]facing)\b",
        re.I,
    ),
}
_PATH_PATTERNS: dict[TaskRateLimitBackoffCategory, re.Pattern[str]] = {
    "rate_limit_response": re.compile(r"(?:rate[_-]?limit|throttl|quota|429)", re.I),
    "retry_after_header": re.compile(r"(?:retry[_-]?after|rate[_-]?limit[_-]?reset)", re.I),
    "exponential_backoff": re.compile(r"(?:backoff|retry[_-]?policy)", re.I),
    "jitter": re.compile(r"(?:jitter)", re.I),
    "worker_retry": re.compile(r"(?:worker|queue|job|retry[_-]?budget|retry[_-]?storm)", re.I),
    "dead_lettering": re.compile(r"(?:dead[_-]?letter|dlq|poison)", re.I),
    "batch_retry": re.compile(r"(?:batch|bulk|chunk|backfill|fan[_-]?out)", re.I),
    "user_retry_message": re.compile(r"(?:retry[_-]?message|rate[_-]?limit[_-]?message|customer|user)", re.I),
}
_AC_PATTERNS: dict[TaskRateLimitBackoffCategory, re.Pattern[str]] = {
    category: pattern for category, pattern in _CATEGORY_PATTERNS.items()
}
_VALIDATION_RE = re.compile(
    r"\b(?:test|tests|pytest|unit|integration|simulate|simulation|fixture|assert|coverage|validation)\b",
    re.I,
)
_NO_IMPACT_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:rate[- ]limit|429|throttl|quota|retry|backoff)\b.{0,80}\b(?:scope|impact|changes?|required|needed)\b",
    re.I,
)
_METADATA_CATEGORY_KEYS = (
    "rate_limit_backoff_categories",
    "backoff_categories",
    "rate_limit_categories",
    "categories",
)
_METADATA_ARTIFACT_KEYS = (
    "validation_evidence",
    "test_evidence",
    "tests",
    "validation_commands",
    "test_commands",
)
_MISSING_CRITERIA: dict[TaskRateLimitBackoffCategory, str] = {
    "rate_limit_response": "Acceptance criteria should define 429 or throttling behavior for rate-limit responses.",
    "retry_after_header": "Acceptance criteria should require honoring Retry-After or reset-window headers.",
    "exponential_backoff": "Acceptance criteria should specify exponential backoff timing and retry caps.",
    "jitter": "Acceptance criteria should include jitter or randomized delays to prevent retry synchronization.",
    "worker_retry": "Acceptance criteria should cover worker retry budgets, idempotency, and stop conditions.",
    "dead_lettering": "Acceptance criteria should define dead-letter handling for exhausted retry attempts.",
    "batch_retry": "Acceptance criteria should cover batch retry chunking, partial failure, and resumability.",
    "user_retry_message": "Acceptance criteria should cover user-facing retry timing and quota messaging.",
}
_TEST_EVIDENCE: dict[TaskRateLimitBackoffCategory, str] = {
    "rate_limit_response": "Add tests that simulate 429 or throttled responses and assert controlled retry behavior.",
    "retry_after_header": "Add tests that assert Retry-After or reset-window headers drive the next retry time.",
    "exponential_backoff": "Add tests for exponential backoff intervals, retry caps, and exhaustion behavior.",
    "jitter": "Add tests or bounded assertions proving jitter is applied without exceeding retry limits.",
    "worker_retry": "Add worker tests for retry budget, idempotency, and stop conditions after transient failures.",
    "dead_lettering": "Add tests that exhaust retries and assert the job or message reaches the dead-letter path.",
    "batch_retry": "Add batch tests for partial failures, chunk retry, and resume without duplicating completed work.",
    "user_retry_message": "Add UI/API tests for retry messaging, Retry-After timing, and support or upgrade paths.",
}


@dataclass(frozen=True, slots=True)
class TaskRateLimitBackoffReadinessRecord:
    """Backoff readiness recommendation for one execution task."""

    task_id: str
    title: str
    severity: TaskRateLimitBackoffSeverity
    categories: tuple[TaskRateLimitBackoffCategory, ...]
    covered_acceptance_criteria: tuple[TaskRateLimitBackoffCategory, ...] = field(default_factory=tuple)
    missing_acceptance_criteria: tuple[str, ...] = field(default_factory=tuple)
    suggested_test_evidence: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "severity": self.severity,
            "categories": list(self.categories),
            "covered_acceptance_criteria": list(self.covered_acceptance_criteria),
            "missing_acceptance_criteria": list(self.missing_acceptance_criteria),
            "suggested_test_evidence": list(self.suggested_test_evidence),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskRateLimitBackoffReadinessPlan:
    """Plan-level task rate-limit backoff readiness recommendations."""

    plan_id: str | None = None
    records: tuple[TaskRateLimitBackoffReadinessRecord, ...] = field(default_factory=tuple)
    impacted_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_impact_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def findings(self) -> tuple[TaskRateLimitBackoffReadinessRecord, ...]:
        """Compatibility view matching planners that name records findings."""
        return self.records

    @property
    def recommendations(self) -> tuple[TaskRateLimitBackoffReadinessRecord, ...]:
        """Compatibility view matching planners that name records recommendations."""
        return self.records

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "impacted_task_ids": list(self.impacted_task_ids),
            "no_impact_task_ids": list(self.no_impact_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render readiness records as deterministic Markdown."""
        title = "# Task Rate-Limit Backoff Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        severity_counts = self.summary.get("severity_counts", {})
        lines = [
            title,
            "",
            (
                f"Summary: {self.summary.get('impacted_task_count', 0)} impacted tasks "
                f"across {self.summary.get('task_count', 0)} total tasks "
                f"(no impact: {self.summary.get('no_impact_task_count', 0)})."
            ),
            (
                "Severity: "
                f"high {severity_counts.get('high', 0)}, "
                f"medium {severity_counts.get('medium', 0)}, "
                f"low {severity_counts.get('low', 0)}."
            ),
        ]
        if not self.records:
            lines.extend(["", "No task rate-limit backoff readiness records were inferred."])
            if self.no_impact_task_ids:
                lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Severity | Categories | Covered Criteria | Missing Criteria | Suggested Test Evidence | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.severity} | "
                f"{_markdown_cell(', '.join(record.categories) or 'none')} | "
                f"{_markdown_cell(', '.join(record.covered_acceptance_criteria) or 'none')} | "
                f"{_markdown_cell('; '.join(record.missing_acceptance_criteria) or 'none')} | "
                f"{_markdown_cell('; '.join(record.suggested_test_evidence) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.no_impact_task_ids:
            lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
        return "\n".join(lines)


def build_task_rate_limit_backoff_readiness_plan(source: Any) -> TaskRateLimitBackoffReadinessPlan:
    """Build task-level rate-limit backoff readiness recommendations."""
    plan_id, tasks = _source_payload(source)
    candidates = [_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (
                _SEVERITY_ORDER[record.severity],
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    impacted_task_ids = tuple(record.task_id for record in records)
    impacted_set = set(impacted_task_ids)
    no_impact_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if _task_id(task, index) not in impacted_set
    )
    return TaskRateLimitBackoffReadinessPlan(
        plan_id=plan_id,
        records=records,
        impacted_task_ids=impacted_task_ids,
        no_impact_task_ids=no_impact_task_ids,
        summary=_summary(records, task_count=len(tasks), no_impact_task_ids=no_impact_task_ids),
    )


def analyze_task_rate_limit_backoff_readiness(source: Any) -> TaskRateLimitBackoffReadinessPlan:
    """Compatibility alias for building rate-limit backoff readiness plans."""
    return build_task_rate_limit_backoff_readiness_plan(source)


def recommend_task_rate_limit_backoff_readiness(source: Any) -> TaskRateLimitBackoffReadinessPlan:
    """Compatibility alias for recommending rate-limit backoff readiness plans."""
    return build_task_rate_limit_backoff_readiness_plan(source)


def extract_task_rate_limit_backoff_readiness(source: Any) -> TaskRateLimitBackoffReadinessPlan:
    """Compatibility alias for extracting rate-limit backoff readiness plans."""
    return build_task_rate_limit_backoff_readiness_plan(source)


def task_rate_limit_backoff_readiness_plan_to_dict(
    result: TaskRateLimitBackoffReadinessPlan,
) -> dict[str, Any]:
    """Serialize a rate-limit backoff readiness plan to a plain dictionary."""
    return result.to_dict()


task_rate_limit_backoff_readiness_plan_to_dict.__test__ = False


def task_rate_limit_backoff_readiness_plan_to_dicts(
    result: TaskRateLimitBackoffReadinessPlan | Iterable[TaskRateLimitBackoffReadinessRecord],
) -> list[dict[str, Any]]:
    """Serialize rate-limit backoff readiness records to plain dictionaries."""
    if isinstance(result, TaskRateLimitBackoffReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_rate_limit_backoff_readiness_plan_to_dicts.__test__ = False


def task_rate_limit_backoff_readiness_plan_to_markdown(
    result: TaskRateLimitBackoffReadinessPlan,
) -> str:
    """Render a rate-limit backoff readiness plan as Markdown."""
    return result.to_markdown()


task_rate_limit_backoff_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    categories: tuple[TaskRateLimitBackoffCategory, ...]
    covered_acceptance_criteria: tuple[TaskRateLimitBackoffCategory, ...]
    validation_covered: tuple[TaskRateLimitBackoffCategory, ...]
    evidence: tuple[str, ...]
    explicitly_no_impact: bool = False


def _record(task: Mapping[str, Any], index: int) -> TaskRateLimitBackoffReadinessRecord | None:
    signals = _signals(task)
    if signals.explicitly_no_impact or not signals.categories:
        return None

    missing_acceptance = tuple(
        _MISSING_CRITERIA[category]
        for category in signals.categories
        if category not in signals.covered_acceptance_criteria
    )
    suggested_test_evidence = tuple(
        _TEST_EVIDENCE[category]
        for category in signals.categories
        if category not in signals.validation_covered
    )
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    return TaskRateLimitBackoffReadinessRecord(
        task_id=task_id,
        title=title,
        severity=_severity(
            set(signals.categories),
            missing_acceptance=missing_acceptance,
            suggested_test_evidence=suggested_test_evidence,
        ),
        categories=signals.categories,
        covered_acceptance_criteria=signals.covered_acceptance_criteria,
        missing_acceptance_criteria=missing_acceptance,
        suggested_test_evidence=suggested_test_evidence,
        evidence=signals.evidence,
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    category_hits: set[TaskRateLimitBackoffCategory] = set()
    covered_acceptance: set[TaskRateLimitBackoffCategory] = set()
    validation_covered: set[TaskRateLimitBackoffCategory] = set()
    evidence: list[str] = []
    explicitly_no_impact = False

    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for category in _metadata_categories(metadata):
            category_hits.add(category)
            evidence.append(f"metadata.rate_limit_backoff_categories: {category}")

    for path in _strings(task.get("files_or_modules") or task.get("files") or task.get("paths")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        path_text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        matched = False
        for category, pattern in _PATH_PATTERNS.items():
            if pattern.search(normalized) or _CATEGORY_PATTERNS[category].search(path_text):
                category_hits.add(category)
                matched = True
        if matched:
            evidence.append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        if _NO_IMPACT_RE.search(text):
            explicitly_no_impact = True
        matched_categories = _categories_from_text(text)
        if not matched_categories:
            continue
        category_hits.update(matched_categories)
        evidence.append(_evidence_snippet(source_field, text))
        if source_field.startswith("acceptance_criteria"):
            covered_acceptance.update(matched_categories)
            if _VALIDATION_RE.search(text):
                validation_covered.update(matched_categories)
        elif source_field.startswith("metadata."):
            if _VALIDATION_RE.search(text) or any(key in source_field for key in _METADATA_ARTIFACT_KEYS):
                validation_covered.update(matched_categories)

    for command in _validation_commands(task):
        command_text = command.replace("/", " ").replace("_", " ").replace("-", " ")
        matched_categories = tuple(_dedupe([*_categories_from_text(command), *_categories_from_text(command_text)]))
        if not matched_categories:
            continue
        category_hits.update(matched_categories)
        validation_covered.update(matched_categories)
        evidence.append(_evidence_snippet("validation_commands", command))

    categories = tuple(category for category in _CATEGORY_ORDER if category in category_hits)
    return _Signals(
        categories=categories,
        covered_acceptance_criteria=tuple(
            category for category in _CATEGORY_ORDER if category in covered_acceptance and category in category_hits
        ),
        validation_covered=tuple(
            category for category in _CATEGORY_ORDER if category in validation_covered and category in category_hits
        ),
        evidence=tuple(_dedupe(evidence)),
        explicitly_no_impact=explicitly_no_impact,
    )


def _categories_from_text(text: str) -> tuple[TaskRateLimitBackoffCategory, ...]:
    searchable = text.replace("/", " ").replace("_", " ").replace("-", " ")
    return tuple(
        category
        for category, pattern in _CATEGORY_PATTERNS.items()
        if pattern.search(text) or pattern.search(searchable)
    )


def _metadata_categories(metadata: Mapping[str, Any]) -> tuple[TaskRateLimitBackoffCategory, ...]:
    categories: list[TaskRateLimitBackoffCategory] = []
    aliases = {
        "429": "rate_limit_response",
        "rate_limit": "rate_limit_response",
        "rate_limit_response": "rate_limit_response",
        "throttling": "rate_limit_response",
        "retry_after": "retry_after_header",
        "retry_after_header": "retry_after_header",
        "backoff": "exponential_backoff",
        "exponential_backoff": "exponential_backoff",
        "jitter": "jitter",
        "worker": "worker_retry",
        "worker_retry": "worker_retry",
        "dead_letter": "dead_lettering",
        "dead_lettering": "dead_lettering",
        "dlq": "dead_lettering",
        "batch": "batch_retry",
        "batch_retry": "batch_retry",
        "user_message": "user_retry_message",
        "user_retry_message": "user_retry_message",
        "customer_messaging": "user_retry_message",
    }
    for key in _METADATA_CATEGORY_KEYS:
        for value in _strings(metadata.get(key)):
            normalized = value.casefold().replace("-", "_").replace(" ", "_").replace("/", "_")
            normalized = aliases.get(normalized, normalized)
            if normalized in _CATEGORY_SORT:
                categories.append(normalized)  # type: ignore[arg-type]
    return tuple(category for category in _CATEGORY_ORDER if category in set(categories))


def _severity(
    categories: set[TaskRateLimitBackoffCategory],
    *,
    missing_acceptance: tuple[str, ...],
    suggested_test_evidence: tuple[str, ...],
) -> TaskRateLimitBackoffSeverity:
    critical = {"rate_limit_response", "retry_after_header", "worker_retry", "dead_lettering", "batch_retry"}
    if categories & critical and (missing_acceptance or suggested_test_evidence):
        return "high"
    if len(categories) >= 3 and suggested_test_evidence:
        return "high"
    if missing_acceptance or suggested_test_evidence:
        return "medium"
    return "low"


def _summary(
    records: tuple[TaskRateLimitBackoffReadinessRecord, ...],
    *,
    task_count: int,
    no_impact_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "record_count": len(records),
        "impacted_task_count": len(records),
        "no_impact_task_count": len(no_impact_task_ids),
        "missing_acceptance_criteria_count": sum(
            len(record.missing_acceptance_criteria) for record in records
        ),
        "suggested_test_evidence_count": sum(len(record.suggested_test_evidence) for record in records),
        "severity_counts": {
            severity: sum(1 for record in records if record.severity == severity)
            for severity in _SEVERITY_ORDER
        },
        "category_counts": {
            category: sum(1 for record in records if category in record.categories)
            for category in _CATEGORY_ORDER
        },
        "impacted_task_ids": [record.task_id for record in records],
        "no_impact_task_ids": list(no_impact_task_ids),
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
        "dependencies",
        "files_or_modules",
        "files",
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
        "dependencies",
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
            if _metadata_key_has_signal(key_text) and key not in _METADATA_CATEGORY_KEYS:
                texts.append((field, key_text))
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if _metadata_key_has_signal(key_text) and key not in _METADATA_CATEGORY_KEYS:
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


def _metadata_key_has_signal(text: str) -> bool:
    return bool(
        any(pattern.search(text) for pattern in _CATEGORY_PATTERNS.values())
        or text.replace(" ", "_") in {*_METADATA_CATEGORY_KEYS, *_METADATA_ARTIFACT_KEYS}
    )


def _validation_commands(task: Mapping[str, Any]) -> tuple[str, ...]:
    commands: list[str] = []
    metadata = task.get("metadata")
    for key in ("validation_commands", "validation_command", "test_commands", "test_command"):
        value = task.get(key)
        if isinstance(value, Mapping):
            commands.extend(flatten_validation_commands(value))
        else:
            commands.extend(_strings(value))
        if isinstance(metadata, Mapping):
            metadata_value = metadata.get(key)
            if isinstance(metadata_value, Mapping):
                commands.extend(flatten_validation_commands(metadata_value))
            else:
                commands.extend(_strings(metadata_value))
    return tuple(_dedupe(commands))


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
    return str(PurePosixPath(value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")))


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
    "TaskRateLimitBackoffCategory",
    "TaskRateLimitBackoffReadinessPlan",
    "TaskRateLimitBackoffReadinessRecord",
    "TaskRateLimitBackoffSeverity",
    "analyze_task_rate_limit_backoff_readiness",
    "build_task_rate_limit_backoff_readiness_plan",
    "extract_task_rate_limit_backoff_readiness",
    "recommend_task_rate_limit_backoff_readiness",
    "task_rate_limit_backoff_readiness_plan_to_dict",
    "task_rate_limit_backoff_readiness_plan_to_dicts",
    "task_rate_limit_backoff_readiness_plan_to_markdown",
]
