"""Plan search-index rollout safeguards for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


SearchIndexSurface = Literal[
    "elasticsearch",
    "opensearch",
    "solr",
    "algolia",
    "vector_index",
    "search_index",
    "analyzer",
    "synonym",
    "relevance_ranking",
    "pagination",
]
SearchReindexRequirement = Literal[
    "full_reindex",
    "incremental_index_update",
    "relevance_only",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SURFACE_ORDER: dict[SearchIndexSurface, int] = {
    "elasticsearch": 0,
    "opensearch": 1,
    "solr": 2,
    "algolia": 3,
    "vector_index": 4,
    "search_index": 5,
    "analyzer": 6,
    "synonym": 7,
    "relevance_ranking": 8,
    "pagination": 9,
}
_REINDEX_ORDER: dict[SearchReindexRequirement, int] = {
    "full_reindex": 0,
    "incremental_index_update": 1,
    "relevance_only": 2,
}
_SURFACE_PATTERNS: tuple[tuple[SearchIndexSurface, re.Pattern[str]], ...] = (
    ("elasticsearch", re.compile(r"\b(?:elasticsearch|elastic search|elastic)\b", re.I)),
    ("opensearch", re.compile(r"\bopen[- ]?search\b", re.I)),
    ("solr", re.compile(r"\bsolr\b", re.I)),
    ("algolia", re.compile(r"\balgolia\b", re.I)),
    (
        "vector_index",
        re.compile(
            r"\b(?:vector search|vector index|embedding index|embeddings?|ann index|"
            r"nearest neighbor|semantic search|pgvector|pinecone|weaviate|qdrant|milvus|faiss)\b",
            re.I,
        ),
    ),
    (
        "search_index",
        re.compile(
            r"\b(?:search engine|search index|search indexes|search indices|indexed document|"
            r"indexing pipeline|inverted index|full[- ]?text search|autocomplete|typeahead|reindex)\b",
            re.I,
        ),
    ),
    (
        "analyzer",
        re.compile(r"\b(?:analy[sz]er|tokeni[sz]er|stemming|stop words?|normalizer|n-?gram)\b", re.I),
    ),
    ("synonym", re.compile(r"\b(?:synonym|synonyms|thesaurus)\b", re.I)),
    (
        "relevance_ranking",
        re.compile(
            r"\b(?:relevance|ranking|ranked results?|boost|boosting|scoring|score|"
            r"sort order|recall|precision|ndcg|mrr|search quality)\b",
            re.I,
        ),
    ),
    (
        "pagination",
        re.compile(r"\b(?:search result pagination|result pagination|pagination|paginate|cursor|offset|page size|infinite scroll)\b", re.I),
    ),
)
_PATH_PATTERNS: tuple[tuple[SearchIndexSurface, re.Pattern[str]], ...] = (
    ("elasticsearch", re.compile(r"(?:^|/)(?:elasticsearch|elastic)(?:[._/-]|$)", re.I)),
    ("opensearch", re.compile(r"(?:^|/)opensearch(?:[._/-]|$)", re.I)),
    ("solr", re.compile(r"(?:^|/)solr(?:[._/-]|$)", re.I)),
    ("algolia", re.compile(r"(?:^|/)algolia(?:[._/-]|$)", re.I)),
    ("vector_index", re.compile(r"(?:vector|embedding|semantic|pgvector|pinecone|weaviate|qdrant|milvus|faiss)", re.I)),
    ("search_index", re.compile(r"(?:search|index|indices|reindex|fulltext|full_text|autocomplete|typeahead)", re.I)),
    ("analyzer", re.compile(r"(?:analy[sz]er|tokeni[sz]er|normalizer|stemming|ngram)", re.I)),
    ("synonym", re.compile(r"(?:synonym|thesaurus)", re.I)),
    ("relevance_ranking", re.compile(r"(?:relevance|rank|ranking|scoring|boost)", re.I)),
    ("pagination", re.compile(r"(?:paginat|cursor|offset|page)", re.I)),
)
_FULL_REINDEX_RE = re.compile(
    r"\b(?:full reindex|complete reindex|rebuild(?: the)? index|index rebuild|"
    r"new index schema|mapping change|field mapping|analy[sz]er change|tokeni[sz]er change|"
    r"normalizer change|synonym reload|reprocess all|backfill all|embedding backfill)\b",
    re.I,
)
_INCREMENTAL_REINDEX_RE = re.compile(
    r"\b(?:incremental index|incremental reindex|partial reindex|delta index|upsert|"
    r"update index|index update|reindex changed|backfill changed|sync changed|cdc|"
    r"change data capture|document update|refresh index|pagination cursor)\b",
    re.I,
)
_RELEVANCE_ONLY_RE = re.compile(
    r"\b(?:relevance only|ranking only|boost(?:ing)?|scoring|query rewrite|search quality|"
    r"synonym|synonyms|analy[sz]er tuning|ranking tuning|sort order|precision|recall|ndcg|mrr)\b",
    re.I,
)
_LOW_ONLY_PATH_RE = re.compile(
    r"(?:^|/)(?:docs?|documentation|tests?|fixtures?)(?:/|$)|"
    r"(?:^|/)(?:README|CHANGELOG|CONTRIBUTING)(?:\.[^/]*)?$|(?:_test|\.test|\.spec)\.",
    re.I,
)


@dataclass(frozen=True, slots=True)
class TaskSearchIndexingImpactRecord:
    """Search-index impact guidance for one execution task."""

    task_id: str
    title: str
    impacted_index_surfaces: tuple[SearchIndexSurface, ...] = field(default_factory=tuple)
    reindex_requirement: SearchReindexRequirement = "incremental_index_update"
    rollout_safeguards: tuple[str, ...] = field(default_factory=tuple)
    validation_checks: tuple[str, ...] = field(default_factory=tuple)
    customer_visible_risk_notes: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "impacted_index_surfaces": list(self.impacted_index_surfaces),
            "reindex_requirement": self.reindex_requirement,
            "rollout_safeguards": list(self.rollout_safeguards),
            "validation_checks": list(self.validation_checks),
            "customer_visible_risk_notes": list(self.customer_visible_risk_notes),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskSearchIndexingImpactPlan:
    """Plan-level search-index impact guidance."""

    plan_id: str | None = None
    records: tuple[TaskSearchIndexingImpactRecord, ...] = field(default_factory=tuple)
    impacted_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "impacted_task_ids": list(self.impacted_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return search-index impact records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render the search-index impact plan as deterministic Markdown."""
        title = "# Task Search Indexing Impact Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.records:
            lines.extend(["", "No search-index impacts were detected."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Surfaces | Reindex Requirement | Rollout Safeguards | Validation Checks | Customer-visible Risks |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(', '.join(record.impacted_index_surfaces))} | "
                f"{record.reindex_requirement} | "
                f"{_markdown_cell('; '.join(record.rollout_safeguards))} | "
                f"{_markdown_cell('; '.join(record.validation_checks))} | "
                f"{_markdown_cell('; '.join(record.customer_visible_risk_notes))} |"
            )
        return "\n".join(lines)


def generate_task_search_indexing_impact(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> tuple[TaskSearchIndexingImpactRecord, ...]:
    """Return search-index impact records for tasks with search indexing signals."""
    return build_task_search_indexing_impact_plan(source).records


def build_task_search_indexing_impact_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskSearchIndexingImpactPlan:
    """Build search-index impact guidance for execution tasks."""
    plan_id, tasks = _source_payload(source)
    records: list[TaskSearchIndexingImpactRecord] = []
    for index, task in enumerate(tasks, start=1):
        record = _task_record(task, index)
        if record:
            records.append(record)
    result = tuple(
        sorted(
            records,
            key=lambda record: (
                _REINDEX_ORDER[record.reindex_requirement],
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    impacted_task_ids = tuple(record.task_id for record in result)
    reindex_counts = {
        requirement: sum(1 for record in result if record.reindex_requirement == requirement)
        for requirement in _REINDEX_ORDER
    }
    surface_counts = {
        surface: sum(1 for record in result if surface in record.impacted_index_surfaces)
        for surface in _SURFACE_ORDER
    }
    return TaskSearchIndexingImpactPlan(
        plan_id=plan_id,
        records=result,
        impacted_task_ids=impacted_task_ids,
        summary={
            "task_count": len(tasks),
            "impacted_task_count": len(impacted_task_ids),
            "reindex_requirement_counts": reindex_counts,
            "surface_counts": surface_counts,
        },
    )


def derive_task_search_indexing_impact_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | TaskSearchIndexingImpactPlan
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskSearchIndexingImpactPlan:
    """Compatibility alias for building search-index impact guidance."""
    if isinstance(source, TaskSearchIndexingImpactPlan):
        return source
    return build_task_search_indexing_impact_plan(source)


def summarize_task_search_indexing_impact(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | TaskSearchIndexingImpactPlan
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskSearchIndexingImpactPlan:
    """Summarize search-index impact guidance for execution tasks."""
    return derive_task_search_indexing_impact_plan(source)


def task_search_indexing_impact_plan_to_dict(
    result: TaskSearchIndexingImpactPlan,
) -> dict[str, Any]:
    """Serialize a search-index impact plan to a plain dictionary."""
    return result.to_dict()


task_search_indexing_impact_plan_to_dict.__test__ = False


def task_search_indexing_impact_plan_to_markdown(result: TaskSearchIndexingImpactPlan) -> str:
    """Render a search-index impact plan as Markdown."""
    return result.to_markdown()


task_search_indexing_impact_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    surfaces: tuple[SearchIndexSurface, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    context: str = ""
    validation_command_evidence: tuple[str, ...] = field(default_factory=tuple)
    doc_or_test_only: bool = False


def _task_record(task: Mapping[str, Any], index: int) -> TaskSearchIndexingImpactRecord | None:
    signals = _signals(task)
    if not signals.surfaces:
        return None
    if signals.doc_or_test_only and len(signals.surfaces) <= 1:
        return None

    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    requirement = _reindex_requirement(signals)
    return TaskSearchIndexingImpactRecord(
        task_id=task_id,
        title=title,
        impacted_index_surfaces=signals.surfaces,
        reindex_requirement=requirement,
        rollout_safeguards=_rollout_safeguards(signals, requirement),
        validation_checks=_validation_checks(signals, requirement),
        customer_visible_risk_notes=_customer_visible_risk_notes(signals, requirement),
        evidence=signals.evidence,
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    surfaces: set[SearchIndexSurface] = set()
    evidence: list[str] = []
    validation_command_evidence: list[str] = []
    context_parts: list[str] = []
    paths = _strings(task.get("files_or_modules") or task.get("files"))

    for path in paths:
        normalized = _normalized_path(path)
        path_text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        path_matched = False
        for surface, pattern in _PATH_PATTERNS:
            if pattern.search(normalized) or pattern.search(path_text):
                surfaces.add(surface)
                path_matched = True
        if path_matched:
            evidence.append(f"files_or_modules: {path}")
            context_parts.append(path_text)

    for source_field, text in _candidate_texts(task):
        matched = False
        for surface, pattern in _SURFACE_PATTERNS:
            if pattern.search(text):
                surfaces.add(surface)
                matched = True
        if matched:
            evidence.append(_evidence_snippet(source_field, text))
        context_parts.append(text)

    for command in _validation_commands(task):
        command_text = command.replace("/", " ").replace("_", " ").replace("-", " ")
        matched = False
        for surface, pattern in (*_SURFACE_PATTERNS, *_PATH_PATTERNS):
            if pattern.search(command) or pattern.search(command_text):
                surfaces.add(surface)
                matched = True
        if matched:
            snippet = _evidence_snippet("validation_commands", command)
            evidence.append(snippet)
            validation_command_evidence.append(snippet)
        context_parts.append(command)

    ordered_surfaces = tuple(surface for surface in _SURFACE_ORDER if surface in surfaces)
    return _Signals(
        surfaces=ordered_surfaces,
        evidence=tuple(_dedupe(evidence)),
        context=" ".join(context_parts),
        validation_command_evidence=tuple(_dedupe(validation_command_evidence)),
        doc_or_test_only=_is_doc_or_test_only(paths),
    )


def _reindex_requirement(signals: _Signals) -> SearchReindexRequirement:
    context = signals.context
    surfaces = set(signals.surfaces)
    if _FULL_REINDEX_RE.search(context):
        return "full_reindex"
    if {"analyzer", "vector_index"} & surfaces:
        return "full_reindex"
    if "synonym" in surfaces and "relevance_ranking" not in surfaces:
        return "full_reindex"
    if _INCREMENTAL_REINDEX_RE.search(context):
        return "incremental_index_update"
    if _RELEVANCE_ONLY_RE.search(context) or surfaces <= {"relevance_ranking", "synonym", "pagination"}:
        return "relevance_only"
    return "incremental_index_update"


def _rollout_safeguards(
    signals: _Signals,
    requirement: SearchReindexRequirement,
) -> tuple[str, ...]:
    safeguards = [
        "Roll out search changes behind a feature flag, alias swap, or staged traffic ramp.",
        "Keep the previous index or query path available until stale, missing, and ranking checks pass.",
    ]
    if requirement == "full_reindex":
        safeguards.extend(
            [
                "Build the replacement index in parallel, verify document counts, then switch aliases atomically.",
                "Throttle reindex or embedding backfill jobs and monitor lag, failed documents, queue depth, and cluster load.",
            ]
        )
    elif requirement == "incremental_index_update":
        safeguards.extend(
            [
                "Define replayable incremental indexing with checkpoint, retry, and dead-letter handling.",
                "Monitor index refresh lag and compare updated document counts against source-of-truth writes during rollout.",
            ]
        )
    else:
        safeguards.extend(
            [
                "Ship ranking or query changes to a limited cohort before broad traffic exposure.",
                "Keep query templates, boosts, synonym sets, or pagination behavior revertible without rebuilding data.",
            ]
        )
    if "pagination" in signals.surfaces:
        safeguards.append("Preserve stable ordering and cursor compatibility across old and new result pages.")
    if "vector_index" in signals.surfaces:
        safeguards.append("Version embeddings and vector-index configuration so old and new retrieval paths can coexist.")
    return tuple(_dedupe(safeguards))


def _validation_checks(
    signals: _Signals,
    requirement: SearchReindexRequirement,
) -> tuple[str, ...]:
    checks = [
        "Validate representative queries for stale results after source records are changed or deleted.",
        "Validate missing-result cases by comparing indexed document IDs against the source of truth.",
        "Validate incorrectly ranked results with golden queries covering exact match, typo, synonym, and low-signal queries.",
    ]
    if requirement == "full_reindex":
        checks.append("Compare old and new index document counts, schema or mapping compatibility, and alias cutover behavior.")
    if requirement == "incremental_index_update":
        checks.append("Exercise create, update, delete, and retry paths through the incremental indexing pipeline.")
    if requirement == "relevance_only":
        checks.append("Run offline relevance evaluation or judgment-list comparison before changing production ranking.")
    if "pagination" in signals.surfaces:
        checks.append("Check pagination for duplicates, skipped results, cursor expiry, and stable rank order across pages.")
    if signals.validation_command_evidence:
        checks.append("Extend existing validation commands with search freshness, recall, and ranking assertions.")
    return tuple(_dedupe(checks))


def _customer_visible_risk_notes(
    signals: _Signals,
    requirement: SearchReindexRequirement,
) -> tuple[str, ...]:
    notes = [
        "Customers may see stale search results while the index catches up.",
        "Customers may miss eligible records if indexing drops, filters, or analyzer rules are wrong.",
        "Customers may see incorrectly ranked results if scoring, boosts, synonyms, or vector retrieval change relevance.",
    ]
    if requirement == "full_reindex":
        notes.append("Full reindex cutover can expose an empty, partial, or mismapped index if alias switching is not gated.")
    if "pagination" in signals.surfaces:
        notes.append("Pagination changes can show duplicate, skipped, or reshuffled results between pages.")
    return tuple(_dedupe(notes))


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
                if any(pattern.search(key_text) for _, pattern in _SURFACE_PATTERNS):
                    texts.append((field, f"{key_text}: {text}"))
            elif any(pattern.search(key_text) for _, pattern in _SURFACE_PATTERNS):
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
    "SearchIndexSurface",
    "SearchReindexRequirement",
    "TaskSearchIndexingImpactPlan",
    "TaskSearchIndexingImpactRecord",
    "build_task_search_indexing_impact_plan",
    "derive_task_search_indexing_impact_plan",
    "generate_task_search_indexing_impact",
    "summarize_task_search_indexing_impact",
    "task_search_indexing_impact_plan_to_dict",
    "task_search_indexing_impact_plan_to_markdown",
]
