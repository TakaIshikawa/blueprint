"""Assess task-level readiness for search reindex backfills and index migrations."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


SearchReindexSignal = Literal[
    "search_index",
    "elasticsearch",
    "opensearch",
    "algolia",
    "meilisearch",
    "solr",
    "vector_index",
    "reindex",
    "backfill",
    "dual_write",
    "alias_cutover",
    "index_migration",
    "query_compatibility",
]
SearchReindexSafeguard = Literal[
    "resumable_jobs",
    "alias_cutover",
    "stale_read_tolerance",
    "throttling",
    "rollback",
    "validation_counts",
    "monitoring",
]
SearchReindexRiskLevel = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SIGNAL_ORDER: tuple[SearchReindexSignal, ...] = (
    "search_index",
    "elasticsearch",
    "opensearch",
    "algolia",
    "meilisearch",
    "solr",
    "vector_index",
    "reindex",
    "backfill",
    "dual_write",
    "alias_cutover",
    "index_migration",
    "query_compatibility",
)
_SAFEGUARD_ORDER: tuple[SearchReindexSafeguard, ...] = (
    "resumable_jobs",
    "alias_cutover",
    "stale_read_tolerance",
    "throttling",
    "rollback",
    "validation_counts",
    "monitoring",
)
_RISK_ORDER: dict[SearchReindexRiskLevel, int] = {"high": 0, "medium": 1, "low": 2}

_SIGNAL_PATTERNS: dict[SearchReindexSignal, re.Pattern[str]] = {
    "search_index": re.compile(
        r"\b(?:search index(?:es|ing)?|index(?:ing)? for search|searchable index|search documents?)\b",
        re.I,
    ),
    "elasticsearch": re.compile(r"\b(?:elastic ?search|es index(?:es)?)\b", re.I),
    "opensearch": re.compile(r"\bopen ?search\b", re.I),
    "algolia": re.compile(r"\balgolia\b", re.I),
    "meilisearch": re.compile(r"\bmeili(?:search)?\b", re.I),
    "solr": re.compile(r"\bsolr\b", re.I),
    "vector_index": re.compile(
        r"\b(?:vector index(?:es)?|embedding index(?:es)?|semantic search|ann index|hnsw|pgvector)\b",
        re.I,
    ),
    "reindex": re.compile(r"\b(?:reindex|re-index|bulk reindex|full reindex|rebuild search index)\b", re.I),
    "backfill": re.compile(r"\b(?:backfill|back fill|back-populate|bulk load|historical records?|catch up)\b", re.I),
    "dual_write": re.compile(
        r"\b(?:dual[- ]write|double[- ]write|write to both|shadow write|parallel write)\b",
        re.I,
    ),
    "alias_cutover": re.compile(
        r"\b(?:alias cutover|index alias|swap alias|alias swap|blue[- ]green index|read alias|write alias)\b",
        re.I,
    ),
    "index_migration": re.compile(
        r"\b(?:index migration|migrate (?:the )?(?:search )?index|new index schema|index version|v\d+ index)\b",
        re.I,
    ),
    "query_compatibility": re.compile(
        r"\b(?:query compatibility|compatible quer(?:y|ies)|old query|new query|query parser|result compatibility|"
        r"ranking compatibility|filter compatibility|facet compatibility)\b",
        re.I,
    ),
}
_PATH_SIGNAL_PATTERNS: dict[SearchReindexSignal, re.Pattern[str]] = {
    "search_index": re.compile(r"(?:^|/)(?:search|indexes?|indices|indexing)(?:/|\.|_|-|$)", re.I),
    "elasticsearch": re.compile(r"elastic[_-]?search|(?:^|/)es(?:/|_|-|$)", re.I),
    "opensearch": re.compile(r"open[_-]?search", re.I),
    "algolia": re.compile(r"algolia", re.I),
    "meilisearch": re.compile(r"meili(?:search)?", re.I),
    "solr": re.compile(r"(?:^|/)solr(?:/|\.|_|-|$)", re.I),
    "vector_index": re.compile(r"vector|embedding|semantic|pgvector|hnsw", re.I),
    "reindex": re.compile(r"re[_-]?index|rebuild[_-]?(?:search[_-]?)?index", re.I),
    "backfill": re.compile(r"back[_-]?fill|backpopulate|bulk[_-]?load", re.I),
    "dual_write": re.compile(r"dual[_-]?write|shadow[_-]?write|double[_-]?write", re.I),
    "alias_cutover": re.compile(r"alias|cutover|blue[_-]?green", re.I),
    "index_migration": re.compile(r"index[_-]?migration|migrate[_-]?index|schema[_-]?index", re.I),
    "query_compatibility": re.compile(r"query[_-]?compat|ranking|filters?|facets?", re.I),
}
_SAFEGUARD_PATTERNS: dict[SearchReindexSafeguard, re.Pattern[str]] = {
    "resumable_jobs": re.compile(
        r"\b(?:resum(?:e|able|ability)|restart(?:able)?|checkpoint|cursor|watermark|continue from|job state)\b",
        re.I,
    ),
    "alias_cutover": re.compile(
        r"\b(?:alias cutover|index alias|swap alias|alias swap|read alias|write alias|blue[- ]green index|atomic cutover)\b",
        re.I,
    ),
    "stale_read_tolerance": re.compile(
        r"\b(?:stale[- ]read|stale results?|staleness|eventual consistency|eventually consistent|dual read|"
        r"fallback to old index|old index remains readable|read compatibility)\b",
        re.I,
    ),
    "throttling": re.compile(
        r"\b(?:throttl(?:e|ing)|rate limit|pace|batch size|chunk size|off[- ]peak|load shed|pause between)\b",
        re.I,
    ),
    "rollback": re.compile(
        r"\b(?:rollback|roll back|revert|restore|snapshot|backup|old index|previous index|undo cutover)\b",
        re.I,
    ),
    "validation_counts": re.compile(
        r"\b(?:validat(?:e|es|ed|ing|ion)|verify|document count|doc count|record count|row count|hit count|"
        r"checksum|sample audit|diff results?|compare results?|parity)\b",
        re.I,
    ),
    "monitoring": re.compile(
        r"\b(?:monitor(?:ing)?|metrics?|alerts?|dashboard|observability|logs?|progress|error rate|index lag|freshness)\b",
        re.I,
    ),
}
_RECOMMENDED_CHECKS: dict[SearchReindexSafeguard, str] = {
    "resumable_jobs": "Make the reindex job resumable with checkpoints, cursors, or watermarks.",
    "alias_cutover": "Define alias or blue-green index cutover steps for read and write traffic.",
    "stale_read_tolerance": "Document stale-read tolerance, query compatibility, and fallback behavior during the transition.",
    "throttling": "Throttle or batch the backfill to protect production search and source systems.",
    "rollback": "Document rollback to the prior index, snapshot, or alias target before cutover.",
    "validation_counts": "Validate document counts, sampled records, and query result parity before and after cutover.",
    "monitoring": "Monitor progress, failures, lag, query errors, and post-cutover search health.",
}
_HIGH_RISK_CORE: frozenset[SearchReindexSafeguard] = frozenset(
    {"alias_cutover", "throttling", "rollback", "validation_counts"}
)


@dataclass(frozen=True, slots=True)
class TaskSearchReindexBackfillReadinessRecord:
    """Readiness guidance for one search reindex or index migration task."""

    task_id: str
    title: str
    matched_signals: tuple[SearchReindexSignal, ...]
    required_safeguards: tuple[SearchReindexSafeguard, ...]
    present_safeguards: tuple[SearchReindexSafeguard, ...]
    missing_safeguards: tuple[SearchReindexSafeguard, ...]
    risk_level: SearchReindexRiskLevel
    recommended_checks: tuple[str, ...]
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "matched_signals": list(self.matched_signals),
            "required_safeguards": list(self.required_safeguards),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "risk_level": self.risk_level,
            "recommended_checks": list(self.recommended_checks),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskSearchReindexBackfillReadinessPlan:
    """Plan-level summary for search reindex backfill readiness."""

    plan_id: str | None = None
    readiness_records: tuple[TaskSearchReindexBackfillReadinessRecord, ...] = field(default_factory=tuple)
    impacted_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_impact_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[TaskSearchReindexBackfillReadinessRecord, ...]:
        """Compatibility view matching planners that expose rows as records."""
        return self.readiness_records

    @property
    def recommendations(self) -> tuple[TaskSearchReindexBackfillReadinessRecord, ...]:
        """Compatibility view for recommendation-oriented consumers."""
        return self.readiness_records

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "readiness_records": [record.to_dict() for record in self.readiness_records],
            "records": [record.to_dict() for record in self.records],
            "impacted_task_ids": list(self.impacted_task_ids),
            "no_impact_task_ids": list(self.no_impact_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.readiness_records]

    def to_markdown(self) -> str:
        """Render the readiness plan as deterministic Markdown."""
        title = "# Task Search Reindex Backfill Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Total tasks: {self.summary.get('task_count', 0)}",
            f"- Impacted tasks: {self.summary.get('impacted_task_count', 0)}",
            f"- No-impact tasks: {self.summary.get('no_impact_task_count', 0)}",
            f"- High risk: {self.summary.get('high_risk_count', 0)}",
            f"- Medium risk: {self.summary.get('medium_risk_count', 0)}",
            f"- Low risk: {self.summary.get('low_risk_count', 0)}",
            f"- Missing safeguards: {self.summary.get('missing_safeguard_count', 0)}",
            f"- Impacted task IDs: {_markdown_cell(', '.join(self.impacted_task_ids) or 'none')}",
            f"- No-impact task IDs: {_markdown_cell(', '.join(self.no_impact_task_ids) or 'none')}",
        ]
        if not self.readiness_records:
            lines.extend(["", "No search reindex backfill readiness records were inferred."])
            return "\n".join(lines)
        lines.extend(
            [
                "",
                "## Records",
                "",
                "| Task | Title | Signals | Risk | Present Safeguards | Missing Safeguards | Recommended Checks | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.readiness_records:
            lines.append(
                "| "
                f"{_markdown_cell(record.task_id)} | "
                f"{_markdown_cell(record.title)} | "
                f"{_markdown_cell(', '.join(record.matched_signals))} | "
                f"{record.risk_level} | "
                f"{_markdown_cell(', '.join(record.present_safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.recommended_checks) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        return "\n".join(lines)


def build_task_search_reindex_backfill_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskSearchReindexBackfillReadinessPlan:
    """Build search reindex backfill readiness records from task-shaped input."""
    plan_id, plan_context, tasks = _source_payload(source)
    records = [
        record
        for index, task in enumerate(tasks, start=1)
        if (record := _record(task, index, plan_context)) is not None
    ]
    records.sort(
        key=lambda record: (
            _RISK_ORDER[record.risk_level],
            -len(record.missing_safeguards),
            record.task_id,
            record.title.casefold(),
        )
    )
    result = tuple(records)
    impacted_task_ids = tuple(record.task_id for record in result)
    impacted = set(impacted_task_ids)
    no_impact_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if _task_id(task, index) not in impacted
    )
    risk_counts = {risk: sum(1 for record in result if record.risk_level == risk) for risk in _RISK_ORDER}
    return TaskSearchReindexBackfillReadinessPlan(
        plan_id=plan_id,
        readiness_records=result,
        impacted_task_ids=impacted_task_ids,
        no_impact_task_ids=no_impact_task_ids,
        summary={
            "task_count": len(tasks),
            "impacted_task_count": len(result),
            "no_impact_task_count": len(no_impact_task_ids),
            "record_count": len(result),
            "high_risk_count": risk_counts["high"],
            "medium_risk_count": risk_counts["medium"],
            "low_risk_count": risk_counts["low"],
            "missing_safeguard_count": sum(len(record.missing_safeguards) for record in result),
            "signal_counts": {
                signal: sum(1 for record in result if signal in record.matched_signals)
                for signal in _SIGNAL_ORDER
            },
            "present_safeguard_counts": {
                safeguard: sum(1 for record in result if safeguard in record.present_safeguards)
                for safeguard in _SAFEGUARD_ORDER
            },
            "missing_safeguard_counts": {
                safeguard: sum(1 for record in result if safeguard in record.missing_safeguards)
                for safeguard in _SAFEGUARD_ORDER
            },
            "impacted_task_ids": list(impacted_task_ids),
            "no_impact_task_ids": list(no_impact_task_ids),
        },
    )


def build_task_search_reindex_backfill_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskSearchReindexBackfillReadinessPlan:
    """Compatibility alias for building search reindex backfill readiness plans."""
    return build_task_search_reindex_backfill_readiness_plan(source)


def generate_task_search_reindex_backfill_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | TaskSearchReindexBackfillReadinessPlan
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskSearchReindexBackfillReadinessPlan:
    """Compatibility alias for generating search reindex backfill readiness plans."""
    if isinstance(source, TaskSearchReindexBackfillReadinessPlan):
        return source
    return build_task_search_reindex_backfill_readiness_plan(source)


def derive_task_search_reindex_backfill_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | TaskSearchReindexBackfillReadinessPlan
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskSearchReindexBackfillReadinessPlan:
    """Compatibility alias for deriving search reindex backfill readiness plans."""
    return generate_task_search_reindex_backfill_readiness_plan(source)


def analyze_task_search_reindex_backfill_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | TaskSearchReindexBackfillReadinessPlan
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> tuple[TaskSearchReindexBackfillReadinessRecord, ...]:
    """Return search reindex backfill readiness records from task-shaped input."""
    return derive_task_search_reindex_backfill_readiness_plan(source).readiness_records


def summarize_task_search_reindex_backfill_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | TaskSearchReindexBackfillReadinessPlan
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic summary counts for search reindex backfill readiness."""
    return derive_task_search_reindex_backfill_readiness_plan(source).summary


def task_search_reindex_backfill_readiness_to_dict(
    plan: TaskSearchReindexBackfillReadinessPlan,
) -> dict[str, Any]:
    """Serialize a search reindex backfill readiness plan to a plain dictionary."""
    return plan.to_dict()


task_search_reindex_backfill_readiness_to_dict.__test__ = False


def task_search_reindex_backfill_readiness_to_dicts(
    records: (
        tuple[TaskSearchReindexBackfillReadinessRecord, ...]
        | list[TaskSearchReindexBackfillReadinessRecord]
        | TaskSearchReindexBackfillReadinessPlan
    ),
) -> list[dict[str, Any]]:
    """Serialize search reindex backfill readiness records to dictionaries."""
    if isinstance(records, TaskSearchReindexBackfillReadinessPlan):
        return records.to_dicts()
    return [record.to_dict() for record in records]


task_search_reindex_backfill_readiness_to_dicts.__test__ = False


def task_search_reindex_backfill_readiness_to_markdown(
    plan: TaskSearchReindexBackfillReadinessPlan,
) -> str:
    """Render a search reindex backfill readiness plan as Markdown."""
    return plan.to_markdown()


task_search_reindex_backfill_readiness_to_markdown.__test__ = False


def _record(
    task: Mapping[str, Any],
    index: int,
    plan_context: tuple[tuple[str, str], ...],
) -> TaskSearchReindexBackfillReadinessRecord | None:
    signals, signal_evidence = _signals(task, plan_context)
    if not signals:
        return None
    present = _present_safeguards(task, plan_context)
    required = _required_safeguards()
    missing = tuple(safeguard for safeguard in required if safeguard not in present)
    task_id = _task_id(task, index)
    return TaskSearchReindexBackfillReadinessRecord(
        task_id=task_id,
        title=_optional_text(task.get("title")) or task_id,
        matched_signals=signals,
        required_safeguards=required,
        present_safeguards=tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in present),
        missing_safeguards=missing,
        risk_level=_risk_level(signals, missing),
        recommended_checks=tuple(_RECOMMENDED_CHECKS[safeguard] for safeguard in missing),
        evidence=tuple(_dedupe([*signal_evidence, *_safeguard_evidence(task, plan_context)])),
    )


def _signals(
    task: Mapping[str, Any],
    plan_context: tuple[tuple[str, str], ...],
) -> tuple[tuple[SearchReindexSignal, ...], list[str]]:
    del plan_context
    signals: set[SearchReindexSignal] = set()
    evidence: list[str] = []
    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        path_text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        matched = False
        for signal in _SIGNAL_ORDER:
            if _PATH_SIGNAL_PATTERNS[signal].search(normalized) or _SIGNAL_PATTERNS[signal].search(path_text):
                signals.add(signal)
                matched = True
        if matched:
            evidence.append(f"files_or_modules: {path}")
    for source_field, text in _task_texts(task):
        matched = False
        for signal in _SIGNAL_ORDER:
            if _SIGNAL_PATTERNS[signal].search(text):
                signals.add(signal)
                matched = True
        if matched:
            evidence.append(_evidence_snippet(source_field, text))
    return tuple(signal for signal in _SIGNAL_ORDER if signal in signals), evidence


def _present_safeguards(
    task: Mapping[str, Any],
    plan_context: tuple[tuple[str, str], ...],
) -> set[SearchReindexSafeguard]:
    context = " ".join(text for _, text in (*_task_texts(task), *plan_context))
    return {safeguard for safeguard, pattern in _SAFEGUARD_PATTERNS.items() if pattern.search(context)}


def _safeguard_evidence(
    task: Mapping[str, Any],
    plan_context: tuple[tuple[str, str], ...],
) -> list[str]:
    evidence: list[str] = []
    for source_field, text in (*_task_texts(task), *plan_context):
        if any(pattern.search(text) for pattern in _SAFEGUARD_PATTERNS.values()):
            evidence.append(_evidence_snippet(source_field, text))
    return evidence


def _required_safeguards() -> tuple[SearchReindexSafeguard, ...]:
    return _SAFEGUARD_ORDER


def _risk_level(
    signals: tuple[SearchReindexSignal, ...],
    missing: tuple[SearchReindexSafeguard, ...],
) -> SearchReindexRiskLevel:
    missing_set = set(missing)
    if set(signals) & {"reindex", "backfill", "index_migration"} and _HIGH_RISK_CORE <= missing_set:
        return "high"
    if len(missing) >= 5:
        return "high"
    if len(missing) <= 1:
        return "low"
    return "medium"


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> tuple[str | None, tuple[tuple[str, str], ...], list[dict[str, Any]]]:
    if isinstance(source, ExecutionTask):
        return None, (), [source.model_dump(mode="python")]
    if isinstance(source, ExecutionPlan):
        payload = source.model_dump(mode="python")
        return _optional_text(payload.get("id")), tuple(_plan_context(payload)), [
            task.model_dump(mode="python") for task in source.tasks
        ]
    if isinstance(source, Mapping):
        if "tasks" in source:
            payload = _plan_payload(source)
            return _optional_text(payload.get("id")), tuple(_plan_context(payload)), _task_payloads(payload.get("tasks"))
        return None, (), [dict(source)]
    if _looks_like_task(source):
        return None, (), [_object_payload(source)]
    if _looks_like_plan(source):
        payload = _object_payload(source)
        return _optional_text(payload.get("id")), tuple(_plan_context(payload)), _task_payloads(payload.get("tasks"))
    try:
        iterator = iter(source)  # type: ignore[arg-type]
    except TypeError:
        return None, (), []
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
    return None, (), tasks


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


def _plan_context(plan: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in ("target_engine", "target_repo", "project_type", "test_strategy", "handoff_prompt", "risk"):
        if text := _optional_text(plan.get(field_name)):
            texts.append((field_name, text))
    for field_name in ("milestones", "risks", "acceptance_criteria", "metadata", "implementation_brief", "brief"):
        texts.extend(_metadata_texts(plan.get(field_name), prefix=field_name))
    return texts


def _task_texts(task: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "owner_type",
        "owner_role",
        "suggested_engine",
        "risk",
        "risk_level",
        "test_command",
        "blocked_reason",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in (
        "acceptance_criteria",
        "criteria",
        "definition_of_done",
        "risks",
        "depends_on",
        "dependencies",
        "tags",
        "labels",
        "notes",
        "validation_commands",
    ):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    texts.extend(_metadata_texts(task.get("metadata")))
    return tuple(texts)


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        patterns = (*_SIGNAL_PATTERNS.values(), *_SAFEGUARD_PATTERNS.values())
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            key_text = str(key).replace("_", " ").replace("-", " ")
            if isinstance(child, (Mapping, list, tuple, set)):
                if any(pattern.search(key_text) for pattern in patterns):
                    texts.append((field, key_text))
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
        "owner_role",
        "suggested_engine",
        "depends_on",
        "dependencies",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "criteria",
        "definition_of_done",
        "risks",
        "risk",
        "risk_level",
        "test_command",
        "validation_commands",
        "status",
        "metadata",
        "blocked_reason",
        "tags",
        "labels",
        "notes",
        "tasks",
        "milestones",
        "implementation_brief",
        "brief",
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


def _normalized_path(value: str) -> str:
    return str(
        PurePosixPath(value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/"))
    )


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
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


def _markdown_cell(value: str) -> str:
    return value.replace("\\", "\\\\").replace("|", "\\|").replace("\n", " ").strip()


__all__ = [
    "SearchReindexRiskLevel",
    "SearchReindexSafeguard",
    "SearchReindexSignal",
    "TaskSearchReindexBackfillReadinessPlan",
    "TaskSearchReindexBackfillReadinessRecord",
    "analyze_task_search_reindex_backfill_readiness",
    "build_task_search_reindex_backfill_readiness",
    "build_task_search_reindex_backfill_readiness_plan",
    "derive_task_search_reindex_backfill_readiness_plan",
    "generate_task_search_reindex_backfill_readiness_plan",
    "summarize_task_search_reindex_backfill_readiness",
    "task_search_reindex_backfill_readiness_to_dict",
    "task_search_reindex_backfill_readiness_to_dicts",
    "task_search_reindex_backfill_readiness_to_markdown",
]
