"""Recommend realtime collaboration conflict readiness safeguards for tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


RealtimeCollaborationCategory = Literal[
    "realtime_transport",
    "presence_state",
    "concurrent_editing",
    "optimistic_sync",
    "conflict_resolution",
    "offline_reconcile",
]
RealtimeCollaborationSafeguard = Literal[
    "server_authority",
    "conflict_resolution_strategy",
    "reconnect_replay",
    "ordering_guarantee",
    "offline_merge_tests",
    "observability",
]
RealtimeCollaborationRiskLevel = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_RISK_ORDER: dict[RealtimeCollaborationRiskLevel, int] = {"high": 0, "medium": 1, "low": 2}
_CATEGORY_ORDER: tuple[RealtimeCollaborationCategory, ...] = (
    "realtime_transport",
    "presence_state",
    "concurrent_editing",
    "optimistic_sync",
    "conflict_resolution",
    "offline_reconcile",
)
_SAFEGUARD_ORDER: tuple[RealtimeCollaborationSafeguard, ...] = (
    "server_authority",
    "conflict_resolution_strategy",
    "reconnect_replay",
    "ordering_guarantee",
    "offline_merge_tests",
    "observability",
)
_CONFLICT_HEAVY_CATEGORIES = {
    "concurrent_editing",
    "optimistic_sync",
    "conflict_resolution",
    "offline_reconcile",
}
_CATEGORY_PATTERNS: dict[RealtimeCollaborationCategory, re.Pattern[str]] = {
    "realtime_transport": re.compile(
        r"\b(?:websockets?|web sockets?|socket\.io|sse|server[- ]sent events?|realtime|"
        r"real[- ]time|live updates?|subscription stream|pub/sub|pubsub|broadcast channel)\b",
        re.I,
    ),
    "presence_state": re.compile(
        r"\b(?:presence|online users?|typing indicators?|live cursors?|cursor presence|"
        r"remote cursors?|user activity|heartbeat|awareness state)\b",
        re.I,
    ),
    "concurrent_editing": re.compile(
        r"\b(?:collaborative editing|concurrent edits?|concurrent editing|multi[- ]user edit|"
        r"simultaneous edits?|shared document|co[- ]editing|coediting|concurrent writes?|"
        r"parallel writes?|document collaboration)\b",
        re.I,
    ),
    "optimistic_sync": re.compile(
        r"\b(?:optimistic updates?|optimistic ui|optimistic sync|local[- ]first|"
        r"client[- ]side pending changes?|pending operations?|speculative updates?)\b",
        re.I,
    ),
    "conflict_resolution": re.compile(
        r"\b(?:crdt|crdts|operational transform|ot\b|merge conflicts?|conflict resolution|"
        r"conflict resolver|last[- ]write[- ]wins|lww|version vectors?|vector clocks?|"
        r"causal ordering|concurrent conflict)\b",
        re.I,
    ),
    "offline_reconcile": re.compile(
        r"\b(?:offline edits?|offline mode|offline reconciliation|reconcile offline|"
        r"offline merge|replay queued|queued mutations?|sync after reconnect|"
        r"merge after reconnect|outbox)\b",
        re.I,
    ),
}
_PATH_CATEGORY_PATTERNS: dict[RealtimeCollaborationCategory, re.Pattern[str]] = {
    "realtime_transport": re.compile(
        r"(?:websocket|socket|sse|realtime|real[_-]?time|pubsub|subscription|broadcast)", re.I
    ),
    "presence_state": re.compile(r"(?:presence|awareness|cursor|heartbeat|typing)", re.I),
    "concurrent_editing": re.compile(
        r"(?:collab|collaboration|concurrent|shared[_-]?doc|coedit|document[_-]?edit)", re.I
    ),
    "optimistic_sync": re.compile(r"(?:optimistic|pending[_-]?op|speculative|local[_-]?first)", re.I),
    "conflict_resolution": re.compile(
        r"(?:crdt|operational[_-]?transform|merge[_-]?conflict|conflict|version[_-]?vector|vector[_-]?clock)",
        re.I,
    ),
    "offline_reconcile": re.compile(r"(?:offline|reconnect|replay|outbox|reconcile)", re.I),
}
_SAFEGUARD_PATTERNS: dict[RealtimeCollaborationSafeguard, re.Pattern[str]] = {
    "server_authority": re.compile(
        r"\b(?:server authority|authoritative server|server[- ]authoritative|server owns|"
        r"server validates?|server arbitration|single writer|canonical server state)\b",
        re.I,
    ),
    "conflict_resolution_strategy": re.compile(
        r"\b(?:conflict resolution strategy|merge strategy|conflict resolver|crdt|crdts|"
        r"operational transform|ot\b|version vectors?|vector clocks?|last[- ]write[- ]wins|"
        r"three[- ]way merge|causal merge)\b",
        re.I,
    ),
    "reconnect_replay": re.compile(
        r"\b(?:reconnect(?:ion)? replay|replay on reconnect|resume token|missed event replay|"
        r"catch[- ]up stream|backfill events?|resync after reconnect|queued mutation replay|outbox replay)\b",
        re.I,
    ),
    "ordering_guarantee": re.compile(
        r"\b(?:ordering guarantee|ordered delivery|monotonic sequence|sequence numbers?|"
        r"causal ordering|total ordering|idempotent operations?|deduplication|dedupe keys?|"
        r"operation ids?|acks?|acknowledgements?)\b",
        re.I,
    ),
    "offline_merge_tests": re.compile(
        r"\b(?:offline merge tests?|offline reconciliation tests?|merge tests?.{0,80}(?:offline|reconnect|conflict)|"
        r"(?:offline|reconnect|conflict).{0,80}merge tests?|concurrent edit tests?|"
        r"multi[- ]client tests?|collaboration e2e tests?)\b",
        re.I,
    ),
    "observability": re.compile(
        r"\b(?:observability|metrics?|tracing|logs?|audit trail|dead letter|sync lag|"
        r"conflict rate|reconnect rate|dropped events?|collaboration dashboard|alerts?)\b",
        re.I,
    ),
}
_RECOMMENDATIONS: dict[RealtimeCollaborationSafeguard, str] = {
    "server_authority": "Define the authoritative write path and server-side validation for collaborative state changes.",
    "conflict_resolution_strategy": "Specify the merge, CRDT, operational transform, or version-vector strategy for concurrent edits.",
    "reconnect_replay": "Cover reconnect replay or catch-up behavior for missed realtime events and queued local changes.",
    "ordering_guarantee": "Add ordering, idempotency, acknowledgement, or deduplication guarantees for operations.",
    "offline_merge_tests": "Add tests for offline edits, reconnect merges, and multi-client conflict scenarios.",
    "observability": "Instrument conflict rates, dropped events, reconnects, sync lag, and failed merges.",
}
_NO_IMPACT_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:realtime|real[- ]time|collaborative editing|"
    r"concurrent edits?|websockets?|presence|offline edits?|merge conflicts?)\b"
    r".{0,80}\b(?:scope|impact|changes?|required|needed)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class TaskRealtimeCollaborationConflictReadinessRecord:
    """Realtime collaboration conflict readiness guidance for one execution task."""

    task_id: str
    title: str
    categories: tuple[RealtimeCollaborationCategory, ...]
    risk_level: RealtimeCollaborationRiskLevel
    present_safeguards: tuple[RealtimeCollaborationSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[RealtimeCollaborationSafeguard, ...] = field(default_factory=tuple)
    recommended_checks: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    @property
    def collaboration_categories(self) -> tuple[RealtimeCollaborationCategory, ...]:
        """Compatibility alias for callers expecting a domain-specific category name."""
        return self.categories

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "categories": list(self.categories),
            "risk_level": self.risk_level,
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "recommended_checks": list(self.recommended_checks),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskRealtimeCollaborationConflictReadinessPlan:
    """Task-level realtime collaboration conflict readiness recommendations."""

    plan_id: str | None = None
    records: tuple[TaskRealtimeCollaborationConflictReadinessRecord, ...] = field(default_factory=tuple)
    realtime_task_ids: tuple[str, ...] = field(default_factory=tuple)
    not_applicable_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def recommendations(self) -> tuple[TaskRealtimeCollaborationConflictReadinessRecord, ...]:
        """Compatibility view matching planners that call records recommendations."""
        return self.records

    @property
    def findings(self) -> tuple[TaskRealtimeCollaborationConflictReadinessRecord, ...]:
        """Compatibility view matching planners that call records findings."""
        return self.records

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "realtime_task_ids": list(self.realtime_task_ids),
            "not_applicable_task_ids": list(self.not_applicable_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return realtime collaboration readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render realtime collaboration conflict readiness as deterministic Markdown."""
        title = "# Task Realtime Collaboration Conflict Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        risk_counts = self.summary.get("risk_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Realtime collaboration task count: {self.summary.get('realtime_task_count', 0)}",
            f"- Missing safeguards count: {self.summary.get('missing_safeguards_count', 0)}",
            "- Risk counts: "
            + ", ".join(f"{risk} {risk_counts.get(risk, 0)}" for risk in _RISK_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No realtime collaboration conflict readiness records were inferred."])
            if self.not_applicable_task_ids:
                lines.extend(
                    ["", f"Not applicable tasks: {_markdown_cell(', '.join(self.not_applicable_task_ids))}"]
                )
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Risk | Categories | Present Safeguards | Missing Safeguards | Recommended Checks | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.risk_level} | "
                f"{_markdown_cell(', '.join(record.categories) or 'none')} | "
                f"{_markdown_cell(', '.join(record.present_safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.recommended_checks) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.not_applicable_task_ids:
            lines.extend(
                ["", f"Not applicable tasks: {_markdown_cell(', '.join(self.not_applicable_task_ids))}"]
            )
        return "\n".join(lines)


def build_task_realtime_collaboration_conflict_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskRealtimeCollaborationConflictReadinessPlan:
    """Build realtime collaboration conflict readiness records for task-shaped input."""
    plan_id, tasks = _source_payload(source)
    candidates = [_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (
                _RISK_ORDER[record.risk_level],
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    realtime_task_ids = tuple(record.task_id for record in records)
    realtime_task_id_set = set(realtime_task_ids)
    not_applicable_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if _task_id(task, index) not in realtime_task_id_set
    )
    return TaskRealtimeCollaborationConflictReadinessPlan(
        plan_id=plan_id,
        records=records,
        realtime_task_ids=realtime_task_ids,
        not_applicable_task_ids=not_applicable_task_ids,
        summary=_summary(records, task_count=len(tasks), not_applicable_task_ids=not_applicable_task_ids),
    )


def analyze_task_realtime_collaboration_conflict_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskRealtimeCollaborationConflictReadinessPlan:
    """Compatibility alias for building realtime collaboration conflict readiness plans."""
    return build_task_realtime_collaboration_conflict_readiness_plan(source)


def extract_task_realtime_collaboration_conflict_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskRealtimeCollaborationConflictReadinessPlan:
    """Compatibility alias for extracting realtime collaboration conflict readiness plans."""
    return build_task_realtime_collaboration_conflict_readiness_plan(source)


def generate_task_realtime_collaboration_conflict_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskRealtimeCollaborationConflictReadinessPlan:
    """Compatibility alias for generating realtime collaboration conflict readiness plans."""
    return build_task_realtime_collaboration_conflict_readiness_plan(source)


def recommend_task_realtime_collaboration_conflict_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskRealtimeCollaborationConflictReadinessPlan:
    """Compatibility alias for recommending realtime collaboration conflict readiness plans."""
    return build_task_realtime_collaboration_conflict_readiness_plan(source)


def task_realtime_collaboration_conflict_readiness_plan_to_dict(
    result: TaskRealtimeCollaborationConflictReadinessPlan,
) -> dict[str, Any]:
    """Serialize a realtime collaboration conflict readiness plan to a plain dictionary."""
    return result.to_dict()


task_realtime_collaboration_conflict_readiness_plan_to_dict.__test__ = False


def task_realtime_collaboration_conflict_readiness_plan_to_dicts(
    result: TaskRealtimeCollaborationConflictReadinessPlan
    | Iterable[TaskRealtimeCollaborationConflictReadinessRecord],
) -> list[dict[str, Any]]:
    """Serialize realtime collaboration conflict readiness records to plain dictionaries."""
    if isinstance(result, TaskRealtimeCollaborationConflictReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_realtime_collaboration_conflict_readiness_plan_to_dicts.__test__ = False


def task_realtime_collaboration_conflict_readiness_plan_to_markdown(
    result: TaskRealtimeCollaborationConflictReadinessPlan,
) -> str:
    """Render a realtime collaboration conflict readiness plan as Markdown."""
    return result.to_markdown()


task_realtime_collaboration_conflict_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    categories: tuple[RealtimeCollaborationCategory, ...] = field(default_factory=tuple)
    present_safeguards: tuple[RealtimeCollaborationSafeguard, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    explicitly_no_impact: bool = False


def _record(
    task: Mapping[str, Any], index: int
) -> TaskRealtimeCollaborationConflictReadinessRecord | None:
    signals = _signals(task)
    category_set = set(signals.categories)
    if signals.explicitly_no_impact or not category_set:
        return None

    present = signals.present_safeguards
    missing = tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard not in present)
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    return TaskRealtimeCollaborationConflictReadinessRecord(
        task_id=task_id,
        title=title,
        categories=signals.categories,
        risk_level=_risk_level(category_set, set(present), missing),
        present_safeguards=present,
        missing_safeguards=missing,
        recommended_checks=tuple(_RECOMMENDATIONS[safeguard] for safeguard in missing),
        evidence=signals.evidence,
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    category_hits: set[RealtimeCollaborationCategory] = set()
    safeguard_hits: set[RealtimeCollaborationSafeguard] = set()
    evidence: list[str] = []
    explicitly_no_impact = False

    for path in _strings(task.get("files_or_modules") or task.get("files") or task.get("paths")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        path_matched = False
        for category, pattern in _PATH_CATEGORY_PATTERNS.items():
            if pattern.search(normalized) or _CATEGORY_PATTERNS[category].search(searchable):
                category_hits.add(category)
                path_matched = True
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(normalized) or pattern.search(searchable):
                safeguard_hits.add(safeguard)
                path_matched = True
        if path_matched:
            evidence.append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        if _NO_IMPACT_RE.search(text):
            explicitly_no_impact = True
        snippet = _evidence_snippet(source_field, text)
        matched = False
        for category, pattern in _CATEGORY_PATTERNS.items():
            if pattern.search(text):
                category_hits.add(category)
                matched = True
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(text):
                safeguard_hits.add(safeguard)
                matched = True
        if matched:
            evidence.append(snippet)

    if "conflict_resolution" in category_hits and any(
        category_pattern.search(" ".join(evidence))
        for category_pattern in (
            re.compile(r"\b(?:crdt|crdts|operational transform|version vectors?|vector clocks?)\b", re.I),
        )
    ):
        safeguard_hits.add("conflict_resolution_strategy")

    return _Signals(
        categories=tuple(category for category in _CATEGORY_ORDER if category in category_hits),
        present_safeguards=tuple(
            safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguard_hits
        ),
        evidence=tuple(_dedupe(evidence)),
        explicitly_no_impact=explicitly_no_impact,
    )


def _risk_level(
    category_set: set[RealtimeCollaborationCategory],
    present_set: set[RealtimeCollaborationSafeguard],
    missing: tuple[RealtimeCollaborationSafeguard, ...],
) -> RealtimeCollaborationRiskLevel:
    conflict_heavy = bool(category_set & _CONFLICT_HEAVY_CATEGORIES)
    has_core_conflict_safeguard = bool(
        present_set
        & {
            "server_authority",
            "conflict_resolution_strategy",
            "ordering_guarantee",
        }
    )
    if conflict_heavy and not has_core_conflict_safeguard:
        return "high"
    if conflict_heavy and len(missing) >= 3:
        return "medium"
    if "realtime_transport" in category_set and "reconnect_replay" in missing:
        return "medium"
    if len(missing) >= 4:
        return "medium"
    return "low"


def _summary(
    records: tuple[TaskRealtimeCollaborationConflictReadinessRecord, ...],
    *,
    task_count: int,
    not_applicable_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "realtime_task_count": len(records),
        "not_applicable_task_ids": list(not_applicable_task_ids),
        "missing_safeguards_count": sum(len(record.missing_safeguards) for record in records),
        "risk_counts": {
            risk: sum(1 for record in records if record.risk_level == risk) for risk in _RISK_ORDER
        },
        "category_counts": {
            category: sum(1 for record in records if category in record.categories)
            for category in _CATEGORY_ORDER
        },
        "missing_safeguard_counts": {
            safeguard: sum(1 for record in records if safeguard in record.missing_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
        "realtime_task_ids": [record.task_id for record in records],
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
        "dependencies",
        "files_or_modules",
        "files",
        "paths",
        "acceptance_criteria",
        "validation_plan",
        "validation_commands",
        "test_command",
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
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in (
        "acceptance_criteria",
        "validation_plan",
        "validation_commands",
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
    return any(
        pattern.search(value) for pattern in [*_CATEGORY_PATTERNS.values(), *_SAFEGUARD_PATTERNS.values()]
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
    return str(
        PurePosixPath(
            value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")
        )
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
    "RealtimeCollaborationCategory",
    "RealtimeCollaborationRiskLevel",
    "RealtimeCollaborationSafeguard",
    "TaskRealtimeCollaborationConflictReadinessPlan",
    "TaskRealtimeCollaborationConflictReadinessRecord",
    "analyze_task_realtime_collaboration_conflict_readiness",
    "build_task_realtime_collaboration_conflict_readiness_plan",
    "extract_task_realtime_collaboration_conflict_readiness",
    "generate_task_realtime_collaboration_conflict_readiness",
    "recommend_task_realtime_collaboration_conflict_readiness",
    "task_realtime_collaboration_conflict_readiness_plan_to_dict",
    "task_realtime_collaboration_conflict_readiness_plan_to_dicts",
    "task_realtime_collaboration_conflict_readiness_plan_to_markdown",
]
