"""Plan search relevance readiness safeguards for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


SearchRelevanceSignal = Literal[
    "ranking",
    "relevance",
    "stemming",
    "synonyms",
    "typo_tolerance",
    "facets",
    "filters",
    "embeddings",
    "indexing_weights",
    "personalization",
    "query_analytics",
]
SearchRelevanceSafeguard = Literal[
    "golden_queries",
    "offline_evaluation",
    "relevance_metrics",
    "rollback_plan",
    "index_rebuild_validation",
    "analytics_instrumentation",
    "manual_review",
]
SearchRelevanceReviewLevel = Literal["standard", "elevated", "sensitive"]
SearchRelevanceReadinessLevel = Literal["ready", "needs_safeguards", "needs_sensitive_review"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SIGNAL_ORDER: tuple[SearchRelevanceSignal, ...] = (
    "ranking",
    "relevance",
    "stemming",
    "synonyms",
    "typo_tolerance",
    "facets",
    "filters",
    "embeddings",
    "indexing_weights",
    "personalization",
    "query_analytics",
)
_SAFEGUARD_ORDER: tuple[SearchRelevanceSafeguard, ...] = (
    "golden_queries",
    "offline_evaluation",
    "relevance_metrics",
    "rollback_plan",
    "index_rebuild_validation",
    "analytics_instrumentation",
    "manual_review",
)
_READINESS_ORDER: dict[SearchRelevanceReadinessLevel, int] = {
    "needs_sensitive_review": 0,
    "needs_safeguards": 1,
    "ready": 2,
}
_SIGNAL_PATTERNS: dict[SearchRelevanceSignal, tuple[re.Pattern[str], ...]] = {
    "ranking": (
        re.compile(r"\b(?:ranking|ranked|ranker|rank order|sort order|scoring|score|boost(?:ing)?|pinned results?)\b", re.I),
    ),
    "relevance": (
        re.compile(r"\b(?:relevance|relevancy|search quality|precision|recall|ndcg|mrr|mean reciprocal rank|dcg)\b", re.I),
    ),
    "stemming": (
        re.compile(r"\b(?:stemming|stemmer|lemmati[sz]ation|morphological matching|tokeni[sz]er|analy[sz]er)\b", re.I),
    ),
    "synonyms": (
        re.compile(r"\b(?:synonym|synonyms|thesaurus|alias terms?|equivalent terms?)\b", re.I),
    ),
    "typo_tolerance": (
        re.compile(r"\b(?:typo tolerance|typos?|fuzzy match|fuzziness|spell ?check|misspell(?:ing)?s?|edit distance)\b", re.I),
    ),
    "facets": (
        re.compile(r"\b(?:facet|facets|faceted|facet counts?)\b", re.I),
    ),
    "filters": (
        re.compile(r"\b(?:filter|filters|filtering|filtered results?|permission filter|tenant filter)\b", re.I),
    ),
    "embeddings": (
        re.compile(r"\b(?:embedding|embeddings|vector search|semantic search|hybrid search|nearest neighbor|ann retrieval|reranker|reranking)\b", re.I),
    ),
    "indexing_weights": (
        re.compile(r"\b(?:index(?:ing)? weights?|field weights?|weighted fields?|boost weights?|bm25|tf[- ]?idf|ranking weights?)\b", re.I),
    ),
    "personalization": (
        re.compile(r"\b(?:personalization|personalisation|personalized|personalised|user-specific ranking|behavioral ranking|behavioural ranking|profile-based ranking|recommendation signal)\b", re.I),
    ),
    "query_analytics": (
        re.compile(r"\b(?:query analytics|search analytics|zero-result queries?|no-result queries?|click[- ]?through|ctr|conversion from search|search events?|query logs?)\b", re.I),
    ),
}
_PATH_PATTERNS: dict[SearchRelevanceSignal, tuple[re.Pattern[str], ...]] = {
    "ranking": (re.compile(r"(?:rank|ranking|scor|boost|sort)", re.I),),
    "relevance": (re.compile(r"(?:relevance|relevancy|quality|judgment|judgement|eval)", re.I),),
    "stemming": (re.compile(r"(?:stem|lemmati|tokeni[sz]er|analy[sz]er)", re.I),),
    "synonyms": (re.compile(r"(?:synonym|thesaurus)", re.I),),
    "typo_tolerance": (re.compile(r"(?:typo|fuzzy|spell|misspell)", re.I),),
    "facets": (re.compile(r"(?:facet)", re.I),),
    "filters": (re.compile(r"(?:filter)", re.I),),
    "embeddings": (re.compile(r"(?:embedding|vector|semantic|rerank)", re.I),),
    "indexing_weights": (re.compile(r"(?:weight|bm25|tfidf|tf_idf)", re.I),),
    "personalization": (re.compile(r"(?:personalization|personalisation|personalized|personalised)", re.I),),
    "query_analytics": (re.compile(r"(?:query_analytics|search_analytics|query-log|query_log|clickthrough|zero_result)", re.I),),
}
_SAFEGUARD_PATTERNS: dict[SearchRelevanceSafeguard, tuple[re.Pattern[str], ...]] = {
    "golden_queries": (
        re.compile(r"\b(?:golden queries|golden query set|judgment list|judgement list|rated queries|query fixtures|representative queries)\b", re.I),
    ),
    "offline_evaluation": (
        re.compile(r"\b(?:offline evaluation|offline eval|relevance evaluation|eval(?:uation)? harness|a/b dry run|shadow evaluation|judgment-list comparison)\b", re.I),
    ),
    "relevance_metrics": (
        re.compile(r"\b(?:ndcg|mrr|precision|recall|map@|mean average precision|success metric|relevance metric|search quality metric)\b", re.I),
    ),
    "rollback_plan": (
        re.compile(r"\b(?:rollback|roll back|revert|feature flag|kill switch|alias swap|previous ranking|previous query path)\b", re.I),
    ),
    "index_rebuild_validation": (
        re.compile(r"\b(?:index rebuild validation|rebuild validation|document counts?|index counts?|mapping validation|schema validation|alias cutover|backfill validation|reindex validation)\b", re.I),
    ),
    "analytics_instrumentation": (
        re.compile(r"\b(?:analytics instrumentation|instrument(?:ed|ation)?|query analytics|search analytics|click[- ]?through|zero-result|search events?|query logs?)\b", re.I),
    ),
    "manual_review": (
        re.compile(r"\b(?:manual review|human review|search quality review|relevance review|editorial review|fairness review|ranking approval|sensitive ranking)\b", re.I),
    ),
}
_INDEX_ONLY_RE = re.compile(
    r"\b(?:search index|indexing pipeline|reindex|index freshness|document upsert|document sync|"
    r"index backfill|index rebuild|mapping change|field mapping|cdc|change data capture)\b",
    re.I,
)
_NEGATED_RELEVANCE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,120}"
    r"\b(?:ranking|relevance|synonym|typo|facet|filter|embedding|personalization|query analytics)\b|"
    r"\b(?:ranking|relevance|synonym|typo|facet|filter|embedding|personalization|query analytics)\b"
    r".{0,120}\b(?:out of scope|not required|not needed|no change)\b",
    re.I,
)
_LOW_ONLY_PATH_RE = re.compile(
    r"(?:^|/)(?:docs?|documentation|tests?|fixtures?)(?:/|$)|"
    r"(?:^|/)(?:README|CHANGELOG|CONTRIBUTING)(?:\.[^/]*)?$|(?:_test|\.test|\.spec)\.",
    re.I,
)


@dataclass(frozen=True, slots=True)
class TaskSearchRelevanceReadinessRecord:
    """Search relevance readiness guidance for one execution task."""

    task_id: str
    title: str
    relevance_signals: tuple[SearchRelevanceSignal, ...] = field(default_factory=tuple)
    review_level: SearchRelevanceReviewLevel = "standard"
    required_safeguards: tuple[SearchRelevanceSafeguard, ...] = field(default_factory=tuple)
    present_safeguards: tuple[SearchRelevanceSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[SearchRelevanceSafeguard, ...] = field(default_factory=tuple)
    readiness_level: SearchRelevanceReadinessLevel = "needs_safeguards"
    recommended_actions: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "relevance_signals": list(self.relevance_signals),
            "review_level": self.review_level,
            "required_safeguards": list(self.required_safeguards),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "readiness_level": self.readiness_level,
            "recommended_actions": list(self.recommended_actions),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskSearchRelevanceReadinessPlan:
    """Plan-level search relevance readiness guidance."""

    plan_id: str | None = None
    records: tuple[TaskSearchRelevanceReadinessRecord, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)
    ignored_task_ids: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
            "ignored_task_ids": list(self.ignored_task_ids),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return search relevance readiness records as dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render the readiness plan as deterministic Markdown."""
        title = "# Task Search Relevance Readiness Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Total tasks: {self.summary.get('total_task_count', 0)}",
            f"- Relevance tasks: {self.summary.get('relevance_task_count', 0)}",
            f"- Ignored tasks: {self.summary.get('ignored_task_count', 0)}",
            f"- Ready tasks: {self.summary.get('ready_task_count', 0)}",
            f"- Tasks needing safeguards: {self.summary.get('needs_safeguards_task_count', 0)}",
            f"- Sensitive review tasks: {self.summary.get('needs_sensitive_review_task_count', 0)}",
            f"- Missing safeguards: {self.summary.get('missing_safeguard_count', 0)}",
        ]
        if not self.records:
            lines.extend(["", "No search relevance readiness records were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Records",
                "",
                (
                    "| Task | Title | Signals | Review | Present Safeguards | Missing Safeguards | "
                    "Readiness | Recommended Actions | Evidence |"
                ),
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"{_markdown_cell(record.task_id)} | "
                f"{_markdown_cell(record.title)} | "
                f"{_markdown_cell(', '.join(record.relevance_signals))} | "
                f"{_markdown_cell(record.review_level)} | "
                f"{_markdown_cell(', '.join(record.present_safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell(record.readiness_level)} | "
                f"{_markdown_cell('; '.join(record.recommended_actions))} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        return "\n".join(lines)


def build_task_search_relevance_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskSearchRelevanceReadinessPlan:
    """Build search relevance readiness guidance for execution tasks."""
    plan_id, tasks = _source_payload(source)
    records: list[TaskSearchRelevanceReadinessRecord] = []
    ignored_task_ids: list[str] = []
    for index, task in enumerate(tasks, start=1):
        task_id = _optional_text(task.get("id")) or f"task-{index}"
        record = _task_record(task, index)
        if record is None:
            ignored_task_ids.append(task_id)
        else:
            records.append(record)
    ordered_records = tuple(
        sorted(
            records,
            key=lambda record: (
                _READINESS_ORDER[record.readiness_level],
                len(record.missing_safeguards),
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    ignored = tuple(sorted(_dedupe(ignored_task_ids), key=lambda value: value.casefold()))
    return TaskSearchRelevanceReadinessPlan(
        plan_id=plan_id,
        records=ordered_records,
        summary=_summary(ordered_records, total_task_count=len(tasks), ignored_task_count=len(ignored)),
        ignored_task_ids=ignored,
    )


def generate_task_search_relevance_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> tuple[TaskSearchRelevanceReadinessRecord, ...]:
    """Return readiness records for tasks with search relevance signals."""
    return build_task_search_relevance_readiness_plan(source).records


def derive_task_search_relevance_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | TaskSearchRelevanceReadinessPlan
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskSearchRelevanceReadinessPlan:
    """Return an existing relevance readiness plan or build one from source input."""
    if isinstance(source, TaskSearchRelevanceReadinessPlan):
        return source
    return build_task_search_relevance_readiness_plan(source)


def summarize_task_search_relevance_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | TaskSearchRelevanceReadinessPlan
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskSearchRelevanceReadinessPlan:
    """Compatibility alias for task search relevance readiness summaries."""
    return derive_task_search_relevance_readiness_plan(source)


def task_search_relevance_readiness_plan_to_dict(
    result: TaskSearchRelevanceReadinessPlan,
) -> dict[str, Any]:
    """Serialize a search relevance readiness plan to a plain dictionary."""
    return result.to_dict()


task_search_relevance_readiness_plan_to_dict.__test__ = False


def task_search_relevance_readiness_plan_to_dicts(
    records: (
        TaskSearchRelevanceReadinessPlan
        | tuple[TaskSearchRelevanceReadinessRecord, ...]
        | list[TaskSearchRelevanceReadinessRecord]
    ),
) -> list[dict[str, Any]]:
    """Serialize search relevance readiness records to dictionaries."""
    if isinstance(records, TaskSearchRelevanceReadinessPlan):
        return records.to_dicts()
    return [record.to_dict() for record in records]


task_search_relevance_readiness_plan_to_dicts.__test__ = False


def task_search_relevance_readiness_plan_to_markdown(
    result: TaskSearchRelevanceReadinessPlan,
) -> str:
    """Render a search relevance readiness plan as Markdown."""
    return result.to_markdown()


task_search_relevance_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    relevance_signals: tuple[SearchRelevanceSignal, ...] = field(default_factory=tuple)
    present_safeguards: tuple[SearchRelevanceSafeguard, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    context: str = ""
    index_only: bool = False
    doc_or_test_only: bool = False


def _task_record(
    task: Mapping[str, Any],
    index: int,
) -> TaskSearchRelevanceReadinessRecord | None:
    signals = _signals(task)
    if not signals.relevance_signals:
        return None
    if signals.doc_or_test_only and len(signals.relevance_signals) <= 1:
        return None

    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    review = _review_level(signals.relevance_signals)
    required = _required_safeguards(signals.relevance_signals)
    present = signals.present_safeguards
    missing = tuple(safeguard for safeguard in required if safeguard not in present)
    readiness = _readiness_level(review, missing)
    return TaskSearchRelevanceReadinessRecord(
        task_id=task_id,
        title=title,
        relevance_signals=signals.relevance_signals,
        review_level=review,
        required_safeguards=required,
        present_safeguards=present,
        missing_safeguards=missing,
        readiness_level=readiness,
        recommended_actions=_recommended_actions(missing, readiness),
        evidence=signals.evidence,
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    relevance_signals: set[SearchRelevanceSignal] = set()
    present_safeguards: set[SearchRelevanceSafeguard] = set()
    evidence: list[str] = []
    context_parts: list[str] = []
    index_only = False
    paths = _strings(task.get("files_or_modules") or task.get("files") or task.get("modules"))

    for source_field, text in _candidate_texts(task):
        if _NEGATED_RELEVANCE_RE.search(text) and not _explicit_relevance_field(source_field):
            continue
        context_parts.append(text)
        if _INDEX_ONLY_RE.search(text):
            index_only = True
        matched_signal = False
        for signal, patterns in _SIGNAL_PATTERNS.items():
            if any(pattern.search(text) for pattern in patterns):
                relevance_signals.add(signal)
                matched_signal = True
        for safeguard, patterns in _SAFEGUARD_PATTERNS.items():
            if any(pattern.search(text) for pattern in patterns):
                present_safeguards.add(safeguard)
                evidence.append(_evidence_snippet(source_field, text))
        if matched_signal:
            evidence.append(_evidence_snippet(source_field, text))

    for command in _validation_commands(task):
        context_parts.append(command)
        for safeguard, patterns in _SAFEGUARD_PATTERNS.items():
            if any(pattern.search(command) for pattern in patterns):
                present_safeguards.add(safeguard)
                evidence.append(_evidence_snippet("validation_commands", command))

    for path in paths:
        normalized = _normalized_path(path)
        path_text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        if _INDEX_ONLY_RE.search(path_text):
            index_only = True
        path_matched = False
        for signal, patterns in _PATH_PATTERNS.items():
            if any(pattern.search(normalized) or pattern.search(path_text) for pattern in patterns):
                relevance_signals.add(signal)
                path_matched = True
        if path_matched:
            evidence.append(f"files_or_modules: {path}")
            context_parts.append(path_text)

    ordered_signals = tuple(signal for signal in _SIGNAL_ORDER if signal in relevance_signals)
    return _Signals(
        relevance_signals=ordered_signals,
        present_safeguards=tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in present_safeguards),
        evidence=tuple(_dedupe(evidence)),
        context=" ".join(context_parts),
        index_only=index_only and not ordered_signals,
        doc_or_test_only=_is_doc_or_test_only(paths),
    )


def _required_safeguards(
    signals: tuple[SearchRelevanceSignal, ...],
) -> tuple[SearchRelevanceSafeguard, ...]:
    required: set[SearchRelevanceSafeguard] = {
        "golden_queries",
        "offline_evaluation",
        "relevance_metrics",
        "rollback_plan",
        "analytics_instrumentation",
    }
    if {"stemming", "synonyms", "typo_tolerance", "embeddings", "indexing_weights"} & set(signals):
        required.add("index_rebuild_validation")
    if {"ranking", "relevance", "embeddings", "personalization", "indexing_weights"} & set(signals):
        required.add("manual_review")
    return tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in required)


def _review_level(signals: tuple[SearchRelevanceSignal, ...]) -> SearchRelevanceReviewLevel:
    if "personalization" in signals or "embeddings" in signals:
        return "sensitive"
    if {"ranking", "relevance", "indexing_weights"} & set(signals):
        return "elevated"
    return "standard"


def _readiness_level(
    review_level: SearchRelevanceReviewLevel,
    missing: tuple[SearchRelevanceSafeguard, ...],
) -> SearchRelevanceReadinessLevel:
    if not missing:
        return "ready"
    if review_level == "sensitive" and (
        "manual_review" in missing
        or "offline_evaluation" in missing
        or "relevance_metrics" in missing
    ):
        return "needs_sensitive_review"
    return "needs_safeguards"


def _recommended_actions(
    missing: tuple[SearchRelevanceSafeguard, ...],
    readiness: SearchRelevanceReadinessLevel,
) -> tuple[str, ...]:
    if not missing:
        return ("Ready to implement after preserving the documented relevance safeguards.",)
    actions = {
        "golden_queries": "Add a golden query set covering exact match, synonyms, typos, filters, facets, and low-signal queries.",
        "offline_evaluation": "Run offline relevance evaluation or judgment-list comparison before production rollout.",
        "relevance_metrics": "Define relevance quality gates such as NDCG, MRR, precision, recall, and zero-result rate.",
        "rollback_plan": "Document a rollback path for query templates, weights, synonym sets, embeddings, or ranking code.",
        "index_rebuild_validation": "Validate index rebuilds with document counts, mapping checks, backfill checks, and alias cutover gates.",
        "analytics_instrumentation": "Instrument query analytics, click-through, zero-result, and conversion signals for rollout monitoring.",
        "manual_review": "Require manual review for sensitive ranking, embedding, personalization, or major scoring changes.",
    }
    prefix = "Require sensitive review before implementation" if readiness == "needs_sensitive_review" else "Before implementation"
    return tuple(f"{prefix}: {actions[safeguard]}" for safeguard in missing)


def _summary(
    records: tuple[TaskSearchRelevanceReadinessRecord, ...],
    *,
    total_task_count: int,
    ignored_task_count: int,
) -> dict[str, Any]:
    return {
        "total_task_count": total_task_count,
        "relevance_task_count": len(records),
        "ignored_task_count": ignored_task_count,
        "ready_task_count": sum(1 for record in records if record.readiness_level == "ready"),
        "needs_safeguards_task_count": sum(
            1 for record in records if record.readiness_level == "needs_safeguards"
        ),
        "needs_sensitive_review_task_count": sum(
            1 for record in records if record.readiness_level == "needs_sensitive_review"
        ),
        "missing_safeguard_count": sum(len(record.missing_safeguards) for record in records),
        "signal_counts": {
            signal: sum(1 for record in records if signal in record.relevance_signals)
            for signal in _SIGNAL_ORDER
        },
        "missing_safeguard_counts": {
            safeguard: sum(1 for record in records if safeguard in record.missing_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
    }


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
        "tasks",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _candidate_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "owner",
        "owner_type",
        "suggested_engine",
        "risk_level",
        "test_command",
        "blocked_reason",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in (
        "acceptance_criteria",
        "definition_of_done",
        "depends_on",
        "dependencies",
        "files_or_modules",
        "files",
        "modules",
        "tags",
        "labels",
        "notes",
        "risks",
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
            if _key_has_relevance_signal(key_text) or _key_has_safeguard_signal(key_text):
                texts.append((field, key_text))
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
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


def _key_has_relevance_signal(key_text: str) -> bool:
    return any(any(pattern.search(key_text) for pattern in patterns) for patterns in _SIGNAL_PATTERNS.values())


def _key_has_safeguard_signal(key_text: str) -> bool:
    return any(any(pattern.search(key_text) for pattern in patterns) for patterns in _SAFEGUARD_PATTERNS.values())


def _explicit_relevance_field(source_field: str) -> bool:
    normalized = source_field.casefold().replace("-", "_").replace(" ", "_")
    return any(
        token in normalized
        for token in (
            "relevance",
            "ranking",
            "synonym",
            "typo",
            "facet",
            "filter",
            "embedding",
            "personalization",
            "analytics",
        )
    )


def _is_doc_or_test_only(paths: list[str]) -> bool:
    if not paths:
        return False
    return all(_LOW_ONLY_PATH_RE.search(_normalized_path(path)) for path in paths)


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
    "SearchRelevanceReadinessLevel",
    "SearchRelevanceReviewLevel",
    "SearchRelevanceSafeguard",
    "SearchRelevanceSignal",
    "TaskSearchRelevanceReadinessPlan",
    "TaskSearchRelevanceReadinessRecord",
    "build_task_search_relevance_readiness_plan",
    "derive_task_search_relevance_readiness_plan",
    "generate_task_search_relevance_readiness",
    "summarize_task_search_relevance_readiness",
    "task_search_relevance_readiness_plan_to_dict",
    "task_search_relevance_readiness_plan_to_dicts",
    "task_search_relevance_readiness_plan_to_markdown",
]
