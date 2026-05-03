"""Assess readiness safeguards for search index rebuild and reindexing tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


SearchIndexRebuildSignal = Literal[
    "search_index_rebuild",
    "full_reindex",
    "incremental_indexing",
    "index_alias_swap",
    "search_backfill",
]
SearchIndexRebuildSafeguard = Literal[
    "batching_or_incremental_rebuild",
    "alias_or_swap_strategy",
    "zero_downtime_rollout",
    "stale_result_handling",
    "validation_checks",
    "observability",
    "rollback_plan",
]
SearchIndexRebuildReadiness = Literal["blocked", "partial", "ready"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_READINESS_ORDER: dict[SearchIndexRebuildReadiness, int] = {"blocked": 0, "partial": 1, "ready": 2}
_SIGNAL_ORDER: tuple[SearchIndexRebuildSignal, ...] = (
    "search_index_rebuild",
    "full_reindex",
    "incremental_indexing",
    "index_alias_swap",
    "search_backfill",
)
_SAFEGUARD_ORDER: tuple[SearchIndexRebuildSafeguard, ...] = (
    "batching_or_incremental_rebuild",
    "alias_or_swap_strategy",
    "zero_downtime_rollout",
    "stale_result_handling",
    "validation_checks",
    "observability",
    "rollback_plan",
)
_SIGNAL_PATTERNS: dict[SearchIndexRebuildSignal, re.Pattern[str]] = {
    "search_index_rebuild": re.compile(
        r"\b(?:search index(?:es)?|search cluster|elasticsearch|opensearch|solr|meilisearch|"
        r"typesense).{0,80}(?:rebuild|recreate|reconstruct|refresh|migration|schema change|mapping change)\b|"
        r"\b(?:rebuild|recreate|reconstruct|refresh|migrate).{0,80}(?:search index(?:es)?|search cluster)\b",
        re.I,
    ),
    "full_reindex": re.compile(
        r"\b(?:full re[- ]?index|full reindex|re[- ]?index all|reindex all|complete re[- ]?index|"
        r"rebuild all (?:documents|records)|drop and rebuild index|index from scratch)\b",
        re.I,
    ),
    "incremental_indexing": re.compile(
        r"\b(?:incremental index(?:ing)?|incremental re[- ]?index|delta index(?:ing)?|change feed|cdc|"
        r"watermark(?:ed)? index|index only changed|partial re[- ]?index)\b",
        re.I,
    ),
    "index_alias_swap": re.compile(
        r"\b(?:index alias(?:es)?|alias swap|swap alias|blue[- ]?green index|shadow index|dual index|"
        r"read alias|write alias)\b",
        re.I,
    ),
    "search_backfill": re.compile(
        r"\b(?:search backfill|index backfill|reindex backfill|backfill search|backfill index|"
        r"bulk index(?:ing)?|bulk re[- ]?index)\b",
        re.I,
    ),
}
_PATH_PATTERNS: dict[SearchIndexRebuildSignal, re.Pattern[str]] = {
    "search_index_rebuild": re.compile(r"search.*index|index.*search|elasticsearch|opensearch|solr", re.I),
    "full_reindex": re.compile(r"full[_-]?reindex|reindex[_-]?all|rebuild[_-]?index", re.I),
    "incremental_indexing": re.compile(r"incremental[_-]?index|delta[_-]?index|cdc|watermark", re.I),
    "index_alias_swap": re.compile(r"alias[_-]?swap|swap[_-]?alias|blue[_-]?green|shadow[_-]?index", re.I),
    "search_backfill": re.compile(r"search[_-]?backfill|index[_-]?backfill|bulk[_-]?index", re.I),
}
_SAFEGUARD_PATTERNS: dict[SearchIndexRebuildSafeguard, re.Pattern[str]] = {
    "batching_or_incremental_rebuild": re.compile(
        r"\b(?:batch(?:ing|ed)?|chunk(?:ing|ed)?|page(?:d)?|cursor|checkpoint|resume|throttle|"
        r"rate limit|concurrency limit|incremental|delta|cdc|watermark|backfill window|bulk size)\b",
        re.I,
    ),
    "alias_or_swap_strategy": re.compile(
        r"\b(?:index alias(?:es)?|alias swap|swap alias|atomic swap|read alias|write alias|"
        r"blue[- ]?green index|shadow index|dual index|promote index)\b",
        re.I,
    ),
    "zero_downtime_rollout": re.compile(
        r"\b(?:zero[- ]?downtime|without downtime|no downtime|online rebuild|online re[- ]?index|"
        r"keep search available|serve traffic during|dual read|dual write|cutover window)\b",
        re.I,
    ),
    "stale_result_handling": re.compile(
        r"\b(?:stale results?|staleness|freshness|refresh lag|index lag|eventual consistency|"
        r"tombstone|deleted documents?|version check|source version|max age|fallback to database)\b",
        re.I,
    ),
    "validation_checks": re.compile(
        r"\b(?:validation checks?|validate|verification|verify|document count|record count|hit count|"
        r"checksum|sample quer(?:y|ies)|query parity|relevance check|mapping validation|smoke test)\b",
        re.I,
    ),
    "observability": re.compile(
        r"\b(?:observability|monitoring|metrics?|alerts?|dashboard|logs?|traces?|indexing lag|"
        r"queue depth|error rate|throughput|reindex progress|search latency|failed documents?)\b",
        re.I,
    ),
    "rollback_plan": re.compile(
        r"\b(?:rollback|roll back|revert|restore alias|swap back|previous index|old index|snapshot|"
        r"disable|kill switch|feature flag|pause indexing)\b",
        re.I,
    ),
}
_NO_IMPACT_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:re[- ]?index|search index|index rebuild|search backfill)\b"
    r".{0,80}\b(?:needed|required|planned|scope|impact|changes?|involved)\b|"
    r"\b(?:re[- ]?index|search index|index rebuild|search backfill)\b.{0,80}\b(?:not|no)\b"
    r".{0,80}\b(?:needed|required|planned|in scope|changed)\b",
    re.I,
)
_SAFEGUARD_GAPS: dict[SearchIndexRebuildSafeguard, str] = {
    "batching_or_incremental_rebuild": "Define batching, checkpoints, throttling, or incremental rebuild boundaries for the reindex.",
    "alias_or_swap_strategy": "Specify the index alias, shadow index, or swap strategy used to promote the rebuilt index.",
    "zero_downtime_rollout": "Document how search stays available during rebuild, dual-write, and cutover.",
    "stale_result_handling": "Define freshness checks, lag handling, deleted-document handling, and stale result fallback behavior.",
    "validation_checks": "Add validation checks for document counts, mappings, sample queries, and query parity before promotion.",
    "observability": "Track reindex progress, throughput, failures, lag, queue depth, and search latency with alerts.",
    "rollback_plan": "Provide rollback steps to restore the previous alias, old index, snapshot, or feature flag state.",
}


@dataclass(frozen=True, slots=True)
class TaskSearchIndexRebuildReadinessRecord:
    """Readiness guidance for one search index rebuild task."""

    task_id: str
    title: str
    signals: tuple[SearchIndexRebuildSignal, ...] = field(default_factory=tuple)
    safeguards: tuple[SearchIndexRebuildSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[SearchIndexRebuildSafeguard, ...] = field(default_factory=tuple)
    readiness_level: SearchIndexRebuildReadiness = "partial"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    readiness_gaps: tuple[str, ...] = field(default_factory=tuple)

    @property
    def present_safeguards(self) -> tuple[SearchIndexRebuildSafeguard, ...]:
        """Compatibility view matching other task readiness analyzers."""
        return self.safeguards

    @property
    def recommended_checks(self) -> tuple[str, ...]:
        """Compatibility view for gap recommendations."""
        return self.readiness_gaps

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "signals": list(self.signals),
            "safeguards": list(self.safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "readiness_level": self.readiness_level,
            "evidence": list(self.evidence),
            "readiness_gaps": list(self.readiness_gaps),
        }


@dataclass(frozen=True, slots=True)
class TaskSearchIndexRebuildReadinessPlan:
    """Plan-level search index rebuild readiness summary."""

    plan_id: str | None = None
    records: tuple[TaskSearchIndexRebuildReadinessRecord, ...] = field(default_factory=tuple)
    search_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_impact_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def findings(self) -> tuple[TaskSearchIndexRebuildReadinessRecord, ...]:
        """Compatibility view matching analyzers that expose findings."""
        return self.records

    @property
    def recommendations(self) -> tuple[TaskSearchIndexRebuildReadinessRecord, ...]:
        """Compatibility view matching analyzers that expose recommendations."""
        return self.records

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "findings": [record.to_dict() for record in self.findings],
            "recommendations": [record.to_dict() for record in self.recommendations],
            "search_task_ids": list(self.search_task_ids),
            "no_impact_task_ids": list(self.no_impact_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return search index rebuild records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render search index rebuild readiness guidance as deterministic Markdown."""
        title = "# Task Search Index Rebuild Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        readiness_counts = self.summary.get("readiness_counts", {})
        signal_counts = self.summary.get("signal_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Search index rebuild task count: {self.summary.get('search_task_count', 0)}",
            f"- Missing safeguard count: {self.summary.get('missing_safeguard_count', 0)}",
            "- Readiness counts: "
            + ", ".join(level + f" {readiness_counts.get(level, 0)}" for level in _READINESS_ORDER),
            "- Signal counts: "
            + ", ".join(f"{signal} {signal_counts.get(signal, 0)}" for signal in _SIGNAL_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No task search index rebuild readiness records were inferred."])
            if self.no_impact_task_ids:
                lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Readiness | Signals | Safeguards | Missing Safeguards | Evidence | Gaps |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.readiness_level} | "
                f"{_markdown_cell(', '.join(record.signals) or 'none')} | "
                f"{_markdown_cell(', '.join(record.safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} | "
                f"{_markdown_cell('; '.join(record.readiness_gaps) or 'none')} |"
            )
        if self.no_impact_task_ids:
            lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
        return "\n".join(lines)


def build_task_search_index_rebuild_readiness_plan(source: Any) -> TaskSearchIndexRebuildReadinessPlan:
    """Build readiness records for search index rebuild and reindexing tasks."""
    if isinstance(source, TaskSearchIndexRebuildReadinessPlan):
        return source
    plan_id, tasks = _source_payload(source)
    candidates = [_record_for_task(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (
                _READINESS_ORDER[record.readiness_level],
                len(record.missing_safeguards),
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    search_task_ids = tuple(record.task_id for record in records)
    search_task_id_set = set(search_task_ids)
    no_impact_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if _task_id(task, index) not in search_task_id_set
    )
    return TaskSearchIndexRebuildReadinessPlan(
        plan_id=plan_id,
        records=records,
        search_task_ids=search_task_ids,
        no_impact_task_ids=no_impact_task_ids,
        summary=_summary(records, task_count=len(tasks), no_impact_task_ids=no_impact_task_ids),
    )


def analyze_task_search_index_rebuild_readiness(source: Any) -> TaskSearchIndexRebuildReadinessPlan:
    """Compatibility alias for building search index rebuild readiness plans."""
    return build_task_search_index_rebuild_readiness_plan(source)


def derive_task_search_index_rebuild_readiness(source: Any) -> TaskSearchIndexRebuildReadinessPlan:
    """Compatibility alias for deriving search index rebuild readiness plans."""
    return build_task_search_index_rebuild_readiness_plan(source)


def extract_task_search_index_rebuild_readiness(source: Any) -> TaskSearchIndexRebuildReadinessPlan:
    """Compatibility alias for extracting search index rebuild readiness plans."""
    return build_task_search_index_rebuild_readiness_plan(source)


def generate_task_search_index_rebuild_readiness(source: Any) -> TaskSearchIndexRebuildReadinessPlan:
    """Compatibility alias for generating search index rebuild readiness plans."""
    return build_task_search_index_rebuild_readiness_plan(source)


def recommend_task_search_index_rebuild_readiness(source: Any) -> TaskSearchIndexRebuildReadinessPlan:
    """Compatibility alias for recommending search index rebuild safeguards."""
    return build_task_search_index_rebuild_readiness_plan(source)


def summarize_task_search_index_rebuild_readiness(source: Any) -> TaskSearchIndexRebuildReadinessPlan:
    """Compatibility alias for summarizing search index rebuild readiness."""
    return build_task_search_index_rebuild_readiness_plan(source)


def task_search_index_rebuild_readiness_plan_to_dict(result: TaskSearchIndexRebuildReadinessPlan) -> dict[str, Any]:
    """Serialize a search index rebuild readiness plan to a plain dictionary."""
    return result.to_dict()


task_search_index_rebuild_readiness_plan_to_dict.__test__ = False


def task_search_index_rebuild_readiness_plan_to_dicts(
    result: TaskSearchIndexRebuildReadinessPlan | Iterable[TaskSearchIndexRebuildReadinessRecord],
) -> list[dict[str, Any]]:
    """Serialize search index rebuild readiness records to plain dictionaries."""
    if isinstance(result, TaskSearchIndexRebuildReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_search_index_rebuild_readiness_plan_to_dicts.__test__ = False


def task_search_index_rebuild_readiness_to_dicts(
    result: TaskSearchIndexRebuildReadinessPlan | Iterable[TaskSearchIndexRebuildReadinessRecord],
) -> list[dict[str, Any]]:
    """Compatibility alias for serializing search index rebuild readiness records."""
    return task_search_index_rebuild_readiness_plan_to_dicts(result)


task_search_index_rebuild_readiness_to_dicts.__test__ = False


def task_search_index_rebuild_readiness_plan_to_markdown(result: TaskSearchIndexRebuildReadinessPlan) -> str:
    """Render a search index rebuild readiness plan as Markdown."""
    return result.to_markdown()


task_search_index_rebuild_readiness_plan_to_markdown.__test__ = False


def _record_for_task(task: Mapping[str, Any], index: int) -> TaskSearchIndexRebuildReadinessRecord | None:
    signal_hits: set[SearchIndexRebuildSignal] = set()
    safeguard_hits: set[SearchIndexRebuildSafeguard] = set()
    signal_evidence: list[str] = []
    safeguard_evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        matched_signal = False
        matched_safeguard = False
        for signal, pattern in _PATH_PATTERNS.items():
            if pattern.search(normalized) or pattern.search(searchable):
                signal_hits.add(signal)
                matched_signal = True
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(normalized) or pattern.search(searchable):
                safeguard_hits.add(safeguard)
                matched_safeguard = True
        if matched_signal:
            signal_evidence.append(f"files_or_modules: {path}")
        if matched_safeguard:
            safeguard_evidence.append(f"files_or_modules: {path}")

    for source_field, text in [*_candidate_texts(task), *_validation_command_texts(task)]:
        if _NO_IMPACT_RE.search(text):
            continue
        searchable = text.replace("/", " ").replace("_", " ").replace("-", " ")
        matched_signal = False
        matched_safeguard = False
        for signal, pattern in _SIGNAL_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                signal_hits.add(signal)
                matched_signal = True
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                safeguard_hits.add(safeguard)
                matched_safeguard = True
        snippet = _evidence_snippet(source_field, text)
        if matched_signal:
            signal_evidence.append(snippet)
        if matched_safeguard:
            safeguard_evidence.append(snippet)

    if not signal_hits:
        return None

    signals = tuple(signal for signal in _SIGNAL_ORDER if signal in signal_hits)
    safeguards = tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguard_hits)
    missing = tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard not in safeguard_hits)
    task_id = _task_id(task, index)
    return TaskSearchIndexRebuildReadinessRecord(
        task_id=task_id,
        title=_optional_text(task.get("title")) or task_id,
        signals=signals,
        safeguards=safeguards,
        missing_safeguards=missing,
        readiness_level=_readiness_level(missing),
        evidence=tuple(_dedupe([*signal_evidence, *safeguard_evidence])),
        readiness_gaps=tuple(_SAFEGUARD_GAPS[safeguard] for safeguard in missing),
    )


def _readiness_level(missing: tuple[SearchIndexRebuildSafeguard, ...]) -> SearchIndexRebuildReadiness:
    if not missing:
        return "ready"
    if any(safeguard in missing for safeguard in ("rollback_plan", "validation_checks", "zero_downtime_rollout")):
        return "blocked"
    return "partial"


def _summary(
    records: tuple[TaskSearchIndexRebuildReadinessRecord, ...],
    *,
    task_count: int,
    no_impact_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "search_task_count": len(records),
        "search_task_ids": [record.task_id for record in records],
        "no_impact_task_count": len(no_impact_task_ids),
        "no_impact_task_ids": list(no_impact_task_ids),
        "missing_safeguard_count": sum(len(record.missing_safeguards) for record in records),
        "readiness_counts": {
            level: sum(1 for record in records if record.readiness_level == level)
            for level in _READINESS_ORDER
        },
        "signal_counts": {
            signal: sum(1 for record in records if signal in record.signals)
            for signal in _SIGNAL_ORDER
        },
        "safeguard_counts": {
            safeguard: sum(1 for record in records if safeguard in record.safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
        "missing_safeguard_counts": {
            safeguard: sum(1 for record in records if safeguard in record.missing_safeguards)
            for safeguard in _SAFEGUARD_ORDER
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
        "files_or_modules",
        "files",
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


def _validation_command_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
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
    return [(f"validation_commands[{index}]", command) for index, command in enumerate(_dedupe(commands))]


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


def _metadata_key_is_signal(key_text: str) -> bool:
    return any(
        pattern.search(key_text)
        for pattern in (*_SIGNAL_PATTERNS.values(), *_PATH_PATTERNS.values(), *_SAFEGUARD_PATTERNS.values())
    )


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
    "SearchIndexRebuildReadiness",
    "SearchIndexRebuildSafeguard",
    "SearchIndexRebuildSignal",
    "TaskSearchIndexRebuildReadinessPlan",
    "TaskSearchIndexRebuildReadinessRecord",
    "analyze_task_search_index_rebuild_readiness",
    "build_task_search_index_rebuild_readiness_plan",
    "derive_task_search_index_rebuild_readiness",
    "extract_task_search_index_rebuild_readiness",
    "generate_task_search_index_rebuild_readiness",
    "recommend_task_search_index_rebuild_readiness",
    "summarize_task_search_index_rebuild_readiness",
    "task_search_index_rebuild_readiness_plan_to_dict",
    "task_search_index_rebuild_readiness_plan_to_dicts",
    "task_search_index_rebuild_readiness_plan_to_markdown",
    "task_search_index_rebuild_readiness_to_dicts",
]
