"""Identify task-level readiness gaps for search indexing and reindexing work."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


SearchReindexingReadinessRiskLevel = Literal["high", "medium", "low"]
SearchWorkType = Literal[
    "index_mapping_change",
    "reindex_job",
    "backfill",
    "facet_filter",
    "autocomplete",
    "ranking_change",
    "eventual_consistency",
]
SearchReadinessCheck = Literal[
    "index_schema_compatibility",
    "backfill_reindex_plan",
    "freshness_lag_validation",
    "rollback_dual_write_strategy",
    "ranking_filter_regression_checks",
    "failure_retry_handling",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_RISK_ORDER: dict[SearchReindexingReadinessRiskLevel, int] = {"high": 0, "medium": 1, "low": 2}
_WORK_TYPE_ORDER: dict[SearchWorkType, int] = {
    "index_mapping_change": 0,
    "reindex_job": 1,
    "backfill": 2,
    "ranking_change": 3,
    "facet_filter": 4,
    "autocomplete": 5,
    "eventual_consistency": 6,
}
_CHECK_ORDER: dict[SearchReadinessCheck, int] = {
    "index_schema_compatibility": 0,
    "backfill_reindex_plan": 1,
    "freshness_lag_validation": 2,
    "rollback_dual_write_strategy": 3,
    "ranking_filter_regression_checks": 4,
    "failure_retry_handling": 5,
}
_WORK_PATTERNS: dict[SearchWorkType, re.Pattern[str]] = {
    "index_mapping_change": re.compile(
        r"\b(?:index mapping|mapping change|schema change|field mapping|index schema|mapping update|"
        r"elasticsearch mapping|opensearch mapping|solr schema|index field|add field to index)\b",
        re.I,
    ),
    "reindex_job": re.compile(
        r"\b(?:reindex(?:ing)?|re-index|rebuild index|recreate index|"
        r"full reindex|partial reindex|reindexing job|index rebuild)\b",
        re.I,
    ),
    "backfill": re.compile(
        r"\b(?:backfill(?:ing)?.*(?:index|search)|index backfill|search backfill|"
        r"populate index|bulk index|bulk import.*(?:index|search)|historical index)\b",
        re.I,
    ),
    "facet_filter": re.compile(
        r"\b(?:facet(?:s|ed|ing)?|filter(?:s|ing|ed)?|"
        r"search filter|faceted search|filter options|facet field)\b",
        re.I,
    ),
    "autocomplete": re.compile(
        r"\b(?:autocomplete|auto-complete|auto complete|typeahead|type-ahead|"
        r"search suggest|suggestion|search completion|completion suggester)\b",
        re.I,
    ),
    "ranking_change": re.compile(
        r"\b(?:ranking|rank|relevance|relevancy|scoring|score|boost|boosting|"
        r"search ranking|ranking algorithm|relevance tuning|search score)\b",
        re.I,
    ),
    "eventual_consistency": re.compile(
        r"\b(?:eventual(?:ly)? consisten(?:t|cy)|search consistency|index lag|indexing lag|"
        r"search freshness|stale.*(?:index|search)|search delay|indexing delay)\b",
        re.I,
    ),
}
_PATH_PATTERNS: dict[SearchWorkType, re.Pattern[str]] = {
    "index_mapping_change": re.compile(r"index[_-]?mapping|mapping|schema|elasticsearch|opensearch|solr", re.I),
    "reindex_job": re.compile(r"reindex|re[_-]?index|rebuild[_-]?index", re.I),
    "backfill": re.compile(r"backfill|bulk[_-]?index|populate[_-]?index", re.I),
    "facet_filter": re.compile(r"facet|filter|filtering", re.I),
    "autocomplete": re.compile(r"autocomplete|typeahead|suggest|completion", re.I),
    "ranking_change": re.compile(r"ranking|relevance|scoring|boost", re.I),
    "eventual_consistency": re.compile(r"consistency|lag|freshness|stale", re.I),
}
_CHECK_PATTERNS: dict[SearchReadinessCheck, re.Pattern[str]] = {
    "index_schema_compatibility": re.compile(
        r"\b(?:schema compat(?:ibility|ible)?|backward compat(?:ible|ibility)?|forward compat(?:ible|ibility)?|"
        r"mapping compat(?:ibility|ible)?|version compat(?:ibility|ible)?|breaking change|"
        r"non-breaking change|schema migration|mapping migration|schema change|index schema)\b",
        re.I,
    ),
    "backfill_reindex_plan": re.compile(
        r"\b(?:(?:backfill|reindex(?:ing)?) plan|reindex(?:ing)? strategy|"
        r"backfill strategy|index migration plan|zero[- ]downtime|"
        r"online reindex|offline reindex|batch(?:es|ing|ed)? reindex|plan (?:is )?(?:ready|documented))\b",
        re.I,
    ),
    "freshness_lag_validation": re.compile(
        r"\b(?:freshness|lag|staleness|index(?:ing)? delay|replication lag|"
        r"time[- ]to[- ]index|indexing latency|monitor(?:ing|ed)?.*(?:freshness|lag))\b",
        re.I,
    ),
    "rollback_dual_write_strategy": re.compile(
        r"\b(?:rollback|dual[- ]write|parallel index|"
        r"blue[- ]green index|alias swap|index alias|old and new index|"
        r"cutover plan|migration path|revert|(?:rollback|revert) (?:plan|strategy|tested))\b",
        re.I,
    ),
    "ranking_filter_regression_checks": re.compile(
        r"\b(?:regression|ranking.*(?:test|validation|check|pass)|"
        r"relevance.*(?:test|validation)|search quality|filter.*(?:test|validation|check|pass)|"
        r"facet.*(?:test|validation)|result(?:s)? validation|search result(?:s)? validation)\b",
        re.I,
    ),
    "failure_retry_handling": re.compile(
        r"\b(?:failure|retry|retries|error handling|failure recovery|"
        r"partial failure|bulk error|indexing error|timeout|circuit breaker|(?:failure|retry).*logic)\b",
        re.I,
    ),
}
_PRODUCTION_SCALE_RE = re.compile(
    r"\b(?:prod(?:uction)?|live (?:index|search)|customer data|customers?|tenant(?:s)?|"
    r"all (?:accounts|users|documents)|pii|personal data|large[- ]scale|"
    r"millions?|billions?|full index|entire index)\b",
    re.I,
)
_LOCAL_LOW_RISK_RE = re.compile(
    r"\b(?:local|dev|development|staging|test (?:index|data)|sandbox|fixture|sample (?:index|data))\b",
    re.I,
)
_SEARCH_RE = re.compile(
    r"\b(?:search|index(?:ing)?|reindex(?:ing)?|elasticsearch|opensearch|solr|lucene|"
    r"algolia|typesense|meilisearch|query|queries|facet|filter|autocomplete|ranking|relevance)\b",
    re.I,
)

_SUGGESTED_ACCEPTANCE_CRITERIA: dict[SearchReadinessCheck, str] = {
    "index_schema_compatibility": "Verify index schema changes are backward compatible or have a migration plan.",
    "backfill_reindex_plan": "Define reindexing approach, batching, zero-downtime strategy, and completion criteria.",
    "freshness_lag_validation": "Monitor and validate indexing lag and search freshness within acceptable bounds.",
    "rollback_dual_write_strategy": "Document rollback plan, dual-write strategy, or alias swap approach.",
    "ranking_filter_regression_checks": "Validate search ranking, filters, and facets with regression tests or manual checks.",
    "failure_retry_handling": "Handle indexing failures, partial errors, and implement retry logic.",
}


@dataclass(frozen=True, slots=True)
class TaskSearchReindexingReadinessFinding:
    """Readiness guidance for one task involving search indexing or reindexing work."""

    task_id: str
    title: str
    work_types: tuple[SearchWorkType, ...] = field(default_factory=tuple)
    readiness_checks: tuple[SearchReadinessCheck, ...] = field(default_factory=tuple)
    missing_acceptance_criteria: tuple[SearchReadinessCheck, ...] = field(default_factory=tuple)
    risk_level: SearchReindexingReadinessRiskLevel = "medium"
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
class TaskSearchReindexingReadinessPlan:
    """Plan-level summary of task search reindexing readiness."""

    plan_id: str | None = None
    findings: tuple[TaskSearchReindexingReadinessFinding, ...] = field(default_factory=tuple)
    impacted_task_ids: tuple[str, ...] = field(default_factory=tuple)
    ignored_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[TaskSearchReindexingReadinessFinding, ...]:
        """Compatibility view matching planners that name task findings records."""
        return self.findings

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "findings": [finding.to_dict() for finding in self.findings],
            "records": [record.to_dict() for record in self.records],
            "impacted_task_ids": list(self.impacted_task_ids),
            "ignored_task_ids": list(self.ignored_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return search reindexing readiness findings as plain dictionaries."""
        return [finding.to_dict() for finding in self.findings]

    def to_markdown(self) -> str:
        """Render the search reindexing readiness plan as deterministic Markdown."""
        lines = ["# Task Search Reindexing Readiness Plan"]
        if self.plan_id:
            lines[0] = f"{lines[0]}: {self.plan_id}"

        summary = self.summary
        lines.extend(
            [
                "",
                "## Summary",
                "",
                f"- Total tasks: {summary.get('task_count', 0)}",
                f"- Impacted tasks: {summary.get('impacted_task_count', 0)}",
                f"- Ignored tasks: {summary.get('ignored_task_count', 0)}",
                "",
                "### Risk Distribution",
                "",
            ]
        )
        risk_counts = summary.get("risk_counts", {})
        for level in ("high", "medium", "low"):
            lines.append(f"- {level.capitalize()}: {risk_counts.get(level, 0)}")

        lines.extend(["", "### Work Type Distribution", ""])
        work_type_counts = summary.get("work_type_counts", {})
        for work_type in _WORK_TYPE_ORDER:
            count = work_type_counts.get(work_type, 0)
            if count > 0:
                lines.append(f"- {work_type}: {count}")

        if not self.findings:
            lines.extend(["", "No search reindexing readiness findings were identified."])
            return "\n".join(lines)

        lines.extend(["", "## Findings", ""])
        for finding in self.findings:
            lines.extend(
                [
                    f"### {finding.task_id}: {finding.title}",
                    "",
                    f"- **Risk Level**: {finding.risk_level}",
                    f"- **Work Types**: {', '.join(finding.work_types)}",
                    f"- **Missing Safeguards**: {len(finding.missing_acceptance_criteria)}/{len(finding.readiness_checks)}",
                    "",
                ]
            )
            if finding.missing_acceptance_criteria:
                lines.append("**Missing Acceptance Criteria:**")
                lines.append("")
                for check in finding.missing_acceptance_criteria:
                    suggestion = _SUGGESTED_ACCEPTANCE_CRITERIA.get(check, "")
                    lines.append(f"- `{check}`: {suggestion}")
                lines.append("")

        return "\n".join(lines)


def build_task_search_reindexing_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | Any]
        | Any
    ),
) -> TaskSearchReindexingReadinessPlan:
    """Build task-level readiness recommendations for search indexing and reindexing."""
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
    return TaskSearchReindexingReadinessPlan(
        plan_id=plan_id,
        findings=findings,
        impacted_task_ids=impacted_task_ids,
        ignored_task_ids=ignored_task_ids,
        summary=_summary(findings, total_task_count=len(tasks), ignored_task_count=len(ignored_task_ids)),
    )


def analyze_task_search_reindexing_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | Any]
        | Any
    ),
) -> tuple[TaskSearchReindexingReadinessFinding, ...]:
    """Return search reindexing readiness findings for relevant execution tasks."""
    return build_task_search_reindexing_readiness_plan(source).findings


def summarize_task_search_reindexing_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | Any]
        | Any
    ),
) -> TaskSearchReindexingReadinessPlan:
    """Compatibility alias for building a search reindexing readiness plan."""
    return build_task_search_reindexing_readiness_plan(source)


def summarize_task_search_reindexing_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | Any]
        | Any
    ),
) -> TaskSearchReindexingReadinessPlan:
    """Compatibility alias for building a search reindexing readiness plan."""
    return build_task_search_reindexing_readiness_plan(source)


def task_search_reindexing_readiness_plan_to_dict(
    result: TaskSearchReindexingReadinessPlan,
) -> dict[str, Any]:
    """Serialize a search reindexing readiness plan to a plain dictionary."""
    return result.to_dict()


task_search_reindexing_readiness_plan_to_dict.__test__ = False  # type: ignore[attr-defined]


def _finding_for_task(task: Mapping[str, Any], index: int) -> TaskSearchReindexingReadinessFinding | None:
    # First check if task has any search-related content
    has_search_content = False
    for _, text in _candidate_texts(task):
        if _SEARCH_RE.search(text):
            has_search_content = True
            break

    # Also check file paths for search-related work
    for path in _strings(task.get("files_or_modules") or task.get("files")):
        if _SEARCH_RE.search(path):
            has_search_content = True
            break

    if not has_search_content:
        return None

    work_evidence: dict[SearchWorkType, list[str]] = {}
    acceptance_checks: set[SearchReadinessCheck] = set()
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
    return TaskSearchReindexingReadinessFinding(
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


def _inspect_path(path: str, work_evidence: dict[SearchWorkType, list[str]]) -> None:
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
    work_evidence: dict[SearchWorkType, list[str]],
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


def _checks_in(text: str) -> set[SearchReadinessCheck]:
    return {check for check, pattern in _CHECK_PATTERNS.items() if pattern.search(text)}


def _risk_level(
    *,
    work_types: tuple[SearchWorkType, ...],
    missing_acceptance_criteria: tuple[SearchReadinessCheck, ...],
    production_evidence: list[str],
    local_evidence: list[str],
) -> SearchReindexingReadinessRiskLevel:
    # High risk if production scale with reindex/backfill/mapping changes
    if production_evidence and any(
        work_type in work_types for work_type in ("index_mapping_change", "reindex_job", "backfill")
    ):
        return "high"

    # High risk if many missing safeguards on critical work types
    if len(missing_acceptance_criteria) >= 4 and any(
        work_type in work_types for work_type in ("index_mapping_change", "reindex_job", "backfill", "ranking_change")
    ):
        return "high"

    # Low risk if local/dev environment with most safeguards present
    if local_evidence and len(missing_acceptance_criteria) <= 2:
        return "low"

    # Low risk if almost all safeguards are present
    if len(missing_acceptance_criteria) <= 1:
        return "low"

    return "medium"


def _summary(
    findings: tuple[TaskSearchReindexingReadinessFinding, ...],
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
    "SearchReadinessCheck",
    "SearchReindexingReadinessRiskLevel",
    "SearchWorkType",
    "TaskSearchReindexingReadinessFinding",
    "TaskSearchReindexingReadinessPlan",
    "analyze_task_search_reindexing_readiness",
    "build_task_search_reindexing_readiness_plan",
    "summarize_task_search_reindexing_readiness",
    "summarize_task_search_reindexing_readiness_plan",
    "task_search_reindexing_readiness_plan_to_dict",
]
