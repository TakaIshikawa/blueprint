"""Build plan-level feature dependency readiness matrices."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


PlanFeatureDependencyType = Literal[
    "prerequisite_feature",
    "shared_schema",
    "shared_api_contract",
    "shared_flag",
    "migration_order",
    "rollout_order",
    "validation_order",
]
PlanFeatureDependencyReadinessStatus = Literal["ready", "needs_clarification"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_DEPENDENCY_TYPE_ORDER: tuple[PlanFeatureDependencyType, ...] = (
    "prerequisite_feature",
    "shared_schema",
    "shared_api_contract",
    "shared_flag",
    "migration_order",
    "rollout_order",
    "validation_order",
)
_READINESS_STATUS_ORDER: dict[PlanFeatureDependencyReadinessStatus, int] = {
    "needs_clarification": 0,
    "ready": 1,
}
_TYPE_PATTERNS: dict[PlanFeatureDependencyType, re.Pattern[str]] = {
    "prerequisite_feature": re.compile(
        r"\b(?:depends on|dependency|dependencies|prerequisite|blocked by|blocker|"
        r"upstream|downstream|requires|after|before|unblocks?)\b",
        re.I,
    ),
    "shared_schema": re.compile(
        r"\b(?:shared schema|schema dependency|common schema|data model|model contract|"
        r"database schema|shared table|shared column|payload schema)\b",
        re.I,
    ),
    "shared_api_contract": re.compile(
        r"\b(?:shared api contract|api contract|endpoint contract|openapi|request contract|"
        r"response contract|graphql contract|api schema|client contract)\b",
        re.I,
    ),
    "shared_flag": re.compile(
        r"\b(?:shared flag|feature flag|feature toggle|release flag|flag dependency|"
        r"flag gate|experiment flag|kill switch)\b",
        re.I,
    ),
    "migration_order": re.compile(
        r"\b(?:migration order|migration before|migration after|schema migration|data migration|"
        r"ddl|backfill before|backfill after|reindex before|reindex after)\b",
        re.I,
    ),
    "rollout_order": re.compile(
        r"\b(?:rollout order|roll out after|roll out before|rollout after|rollout before|"
        r"release order|launch after|launch before|phased rollout|canary before|deploy after|deploy before)\b",
        re.I,
    ),
    "validation_order": re.compile(
        r"\b(?:validation order|validate after|validate before|test after|test before|"
        r"contract test after|integration test after|validation depends on|run tests after|"
        r"verification after|verification before)\b",
        re.I,
    ),
}
_DEPENDENCY_SIGNAL_RE = re.compile(
    r"\b(?:depends on|dependency|dependencies|prerequisite|blocked by|blocker|waiting on|"
    r"upstream|downstream|requires|must follow|must precede|after|before|unblocks?|"
    r"shared schema|api contract|feature flag|migration order|rollout order|validation order)\b",
    re.I,
)
_DOWNSTREAM_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\b(?:depends on|blocked by|waiting on|requires|after|following)\s+(?P<target>[^.;\n]+)", re.I),
    re.compile(r"\b(?:downstream of|must follow)\s+(?P<target>[^.;\n]+)", re.I),
)
_UPSTREAM_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\b(?:before|must precede|unblocks?|blocks)\s+(?P<target>[^.;\n]+)", re.I),
    re.compile(r"\b(?:upstream of)\s+(?P<target>[^.;\n]+)", re.I),
)


@dataclass(frozen=True, slots=True)
class PlanFeatureDependencyReadinessRow:
    """One grouped feature dependency readiness record."""

    dependency_type: PlanFeatureDependencyType
    upstream_task_ids: tuple[str, ...] = field(default_factory=tuple)
    downstream_task_ids: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    readiness_status: PlanFeatureDependencyReadinessStatus = "ready"
    recommended_owner: str = "plan_owner"
    recommended_next_step: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "dependency_type": self.dependency_type,
            "upstream_task_ids": list(self.upstream_task_ids),
            "downstream_task_ids": list(self.downstream_task_ids),
            "evidence": list(self.evidence),
            "readiness_status": self.readiness_status,
            "recommended_owner": self.recommended_owner,
            "recommended_next_step": self.recommended_next_step,
        }


@dataclass(frozen=True, slots=True)
class PlanFeatureDependencyReadinessMatrix:
    """Plan-level feature dependency readiness matrix and summary counts."""

    plan_id: str | None = None
    rows: tuple[PlanFeatureDependencyReadinessRow, ...] = field(default_factory=tuple)
    no_dependency_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[PlanFeatureDependencyReadinessRow, ...]:
        """Compatibility view for consumers that call matrix rows records."""
        return self.rows

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "records": [row.to_dict() for row in self.rows],
            "no_dependency_task_ids": list(self.no_dependency_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return feature dependency readiness rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the matrix as deterministic Markdown."""
        title = "# Plan Feature Dependency Readiness Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        status_counts = self.summary.get("readiness_status_counts", {})
        lines = [
            title,
            "",
            (
                f"Summary: {self.summary.get('dependency_row_count', 0)} dependency rows "
                f"across {self.summary.get('task_count', 0)} tasks "
                f"(ready: {status_counts.get('ready', 0)}, "
                f"needs clarification: {status_counts.get('needs_clarification', 0)}, "
                f"no dependency: {self.summary.get('no_dependency_task_count', 0)})."
            ),
        ]
        if not self.rows:
            lines.extend(["", "No feature dependency readiness rows were inferred."])
            if self.no_dependency_task_ids:
                lines.extend(
                    [
                        "",
                        f"No-dependency tasks: {_markdown_cell(', '.join(self.no_dependency_task_ids))}",
                    ]
                )
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Dependency Type | Upstream Tasks | Downstream Tasks | Status | Owner | Next Step | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"{row.dependency_type} | "
                f"{_markdown_cell(', '.join(row.upstream_task_ids) or 'needs clarity')} | "
                f"{_markdown_cell(', '.join(row.downstream_task_ids) or 'needs clarity')} | "
                f"{row.readiness_status} | "
                f"{_markdown_cell(row.recommended_owner)} | "
                f"{_markdown_cell(row.recommended_next_step)} | "
                f"{_markdown_cell('; '.join(row.evidence) or 'none')} |"
            )
        if self.no_dependency_task_ids:
            lines.extend(
                [
                    "",
                    f"No-dependency tasks: {_markdown_cell(', '.join(self.no_dependency_task_ids))}",
                ]
            )
        return "\n".join(lines)


def build_plan_feature_dependency_readiness_matrix(source: Any) -> PlanFeatureDependencyReadinessMatrix:
    """Build feature dependency readiness rows for an execution plan."""
    plan_id, tasks = _source_payload(source)
    task_ids = tuple(_task_id(task, index) for index, task in enumerate(tasks, start=1))
    task_lookup = _task_lookup(tasks)
    buckets: dict[PlanFeatureDependencyType, _DependencyBucket] = {
        dependency_type: _DependencyBucket() for dependency_type in _DEPENDENCY_TYPE_ORDER
    }
    dependency_task_ids: set[str] = set()

    for index, task in enumerate(tasks, start=1):
        task_id = _task_id(task, index)
        for signal in _task_signals(task, task_id, task_lookup):
            bucket = buckets[signal.dependency_type]
            bucket.upstream_task_ids.extend(signal.upstream_task_ids)
            bucket.downstream_task_ids.extend(signal.downstream_task_ids)
            bucket.evidence.extend(signal.evidence)
            bucket.needs_clarification = bucket.needs_clarification or signal.needs_clarification
            dependency_task_ids.update(signal.upstream_task_ids)
            dependency_task_ids.update(signal.downstream_task_ids)
            if signal.needs_clarification:
                dependency_task_ids.add(task_id)

    rows = tuple(
        _row_from_bucket(dependency_type, bucket)
        for dependency_type, bucket in buckets.items()
        if bucket.evidence
    )
    sorted_rows = tuple(
        sorted(rows, key=lambda row: _DEPENDENCY_TYPE_ORDER.index(row.dependency_type))
    )
    no_dependency_task_ids = tuple(task_id for task_id in task_ids if task_id not in dependency_task_ids)
    return PlanFeatureDependencyReadinessMatrix(
        plan_id=plan_id,
        rows=sorted_rows,
        no_dependency_task_ids=no_dependency_task_ids,
        summary=_summary(
            sorted_rows,
            task_count=len(tasks),
            no_dependency_task_ids=no_dependency_task_ids,
        ),
    )


def generate_plan_feature_dependency_readiness_matrix(source: Any) -> PlanFeatureDependencyReadinessMatrix:
    """Generate a feature dependency readiness matrix from a plan-like source."""
    return build_plan_feature_dependency_readiness_matrix(source)


def derive_plan_feature_dependency_readiness_matrix(source: Any) -> PlanFeatureDependencyReadinessMatrix:
    """Derive a feature dependency readiness matrix from a plan-like source."""
    return build_plan_feature_dependency_readiness_matrix(source)


def extract_plan_feature_dependency_readiness_matrix(source: Any) -> PlanFeatureDependencyReadinessMatrix:
    """Extract a feature dependency readiness matrix from a plan-like source."""
    return derive_plan_feature_dependency_readiness_matrix(source)


def summarize_plan_feature_dependency_readiness_matrix(
    matrix: PlanFeatureDependencyReadinessMatrix | Iterable[PlanFeatureDependencyReadinessRow],
) -> dict[str, Any]:
    """Return deterministic summary counts for a matrix or row iterable."""
    if isinstance(matrix, PlanFeatureDependencyReadinessMatrix):
        return dict(matrix.summary)
    rows = tuple(matrix)
    return _summary(rows, task_count=len(rows), no_dependency_task_ids=())


def plan_feature_dependency_readiness_matrix_to_dict(
    matrix: PlanFeatureDependencyReadinessMatrix,
) -> dict[str, Any]:
    """Serialize a feature dependency readiness matrix to a plain dictionary."""
    return matrix.to_dict()


plan_feature_dependency_readiness_matrix_to_dict.__test__ = False


def plan_feature_dependency_readiness_matrix_to_dicts(
    matrix: PlanFeatureDependencyReadinessMatrix | Iterable[PlanFeatureDependencyReadinessRow],
) -> list[dict[str, Any]]:
    """Serialize feature dependency readiness rows to plain dictionaries."""
    if isinstance(matrix, PlanFeatureDependencyReadinessMatrix):
        return matrix.to_dicts()
    return [row.to_dict() for row in matrix]


plan_feature_dependency_readiness_matrix_to_dicts.__test__ = False


def plan_feature_dependency_readiness_matrix_to_markdown(
    matrix: PlanFeatureDependencyReadinessMatrix,
) -> str:
    """Render a feature dependency readiness matrix as Markdown."""
    return matrix.to_markdown()


plan_feature_dependency_readiness_matrix_to_markdown.__test__ = False


@dataclass(slots=True)
class _DependencySignal:
    dependency_type: PlanFeatureDependencyType
    upstream_task_ids: tuple[str, ...] = field(default_factory=tuple)
    downstream_task_ids: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    needs_clarification: bool = False


@dataclass(slots=True)
class _DependencyBucket:
    upstream_task_ids: list[str] = field(default_factory=list)
    downstream_task_ids: list[str] = field(default_factory=list)
    evidence: list[str] = field(default_factory=list)
    needs_clarification: bool = False


def _task_signals(
    task: Mapping[str, Any],
    task_id: str,
    task_lookup: Mapping[str, str],
) -> list[_DependencySignal]:
    signals: list[_DependencySignal] = []
    explicit_refs = _explicit_dependency_refs(task)
    for source_field, ref in explicit_refs:
        upstream = _resolve_task_ref(ref, task_lookup)
        signals.append(
            _DependencySignal(
                dependency_type="prerequisite_feature",
                upstream_task_ids=(upstream,) if upstream else (),
                downstream_task_ids=(task_id,),
                evidence=(_evidence_snippet(source_field, ref),),
                needs_clarification=upstream is None,
            )
        )

    for source_field, text in _candidate_texts(task):
        dependency_types = _dependency_types_from_text(text)
        if not dependency_types:
            continue
        upstream_ids, downstream_ids = _directional_task_ids(text, task_id, task_lookup)
        if source_field == "title" and not upstream_ids and not downstream_ids:
            continue
        needs_clarification = not upstream_ids or not downstream_ids
        if needs_clarification and not downstream_ids:
            downstream_ids = (task_id,)
        for dependency_type in dependency_types:
            signals.append(
                _DependencySignal(
                    dependency_type=dependency_type,
                    upstream_task_ids=upstream_ids,
                    downstream_task_ids=downstream_ids,
                    evidence=(_evidence_snippet(source_field, text),),
                    needs_clarification=needs_clarification,
                )
            )

    return _merge_signals(signals)


def _explicit_dependency_refs(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    refs: list[tuple[str, str]] = []
    metadata = task.get("metadata")
    for field_name in ("depends_on", "dependencies", "blocked_by"):
        for index, value in enumerate(_dependency_ref_values(task.get(field_name))):
            refs.append((f"{field_name}[{index}]", value))
        if isinstance(metadata, Mapping):
            for index, value in enumerate(_dependency_ref_values(metadata.get(field_name))):
                refs.append((f"metadata.{field_name}[{index}]", value))
    return refs


def _dependency_ref_values(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, Mapping):
        for key in ("id", "task_id", "task", "upstream_task_id", "dependency", "name", "title"):
            if text := _optional_text(value.get(key)):
                return [text]
        return _strings(value)
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        values: list[str] = []
        for item in items:
            values.extend(_dependency_ref_values(item))
        return values
    text = _optional_text(value)
    return [text] if text else []


def _dependency_types_from_text(text: str) -> tuple[PlanFeatureDependencyType, ...]:
    if not _DEPENDENCY_SIGNAL_RE.search(text):
        return ()
    found = {
        dependency_type
        for dependency_type, pattern in _TYPE_PATTERNS.items()
        if pattern.search(text)
    }
    if not found:
        found.add("prerequisite_feature")
    return tuple(dependency_type for dependency_type in _DEPENDENCY_TYPE_ORDER if dependency_type in found)


def _directional_task_ids(
    text: str,
    current_task_id: str,
    task_lookup: Mapping[str, str],
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    upstream: list[str] = []
    downstream: list[str] = []
    ordered_refs = _resolve_task_refs(text, task_lookup)
    if len(ordered_refs) >= 2 and re.search(r"\bbefore\b", text, re.I):
        return (ordered_refs[0],), tuple(_dedupe(ordered_refs[1:]))
    if len(ordered_refs) >= 2 and re.search(r"\bafter\b", text, re.I):
        return (ordered_refs[-1],), tuple(_dedupe(ordered_refs[:-1]))

    for pattern in _DOWNSTREAM_PATTERNS:
        for match in pattern.finditer(text):
            upstream.extend(_resolve_task_refs(_direction_target(match.group("target")), task_lookup))
    for pattern in _UPSTREAM_PATTERNS:
        for match in pattern.finditer(text):
            downstream.extend(_resolve_task_refs(_direction_target(match.group("target")), task_lookup))

    all_refs = [task_id for task_id in ordered_refs if task_id != current_task_id]
    if upstream:
        downstream.append(current_task_id)
    elif downstream:
        upstream.append(current_task_id)
    elif len(all_refs) >= 2 and current_task_id not in all_refs:
        upstream.append(all_refs[0])
        downstream.extend(all_refs[1:])
    elif len(all_refs) == 1 and all_refs[0] != current_task_id:
        upstream.append(all_refs[0])
        downstream.append(current_task_id)

    return tuple(_dedupe(upstream)), tuple(_dedupe(downstream))


def _direction_target(value: str) -> str:
    target = _text(value)
    target = re.split(
        r"\b(?:and|then)\s+(?:shares?|uses?|launch(?:es)?|roll(?:s)?|deploy(?:s)?|validate(?:s)?|test(?:s)?|run(?:s)?)\b",
        target,
        maxsplit=1,
        flags=re.I,
    )[0]
    target = re.split(r"\b(?:before|after|once|when|while)\b", target, maxsplit=1, flags=re.I)[0]
    return target.strip(" ,:")


def _resolve_task_refs(text: str, task_lookup: Mapping[str, str]) -> list[str]:
    matches: list[tuple[int, str]] = []
    haystack = f" {_normalize_lookup_key(text)} "
    for lookup_key, task_id in task_lookup.items():
        match = re.search(rf"(?<![a-z0-9]){re.escape(lookup_key)}(?![a-z0-9])", haystack)
        if match:
            matches.append((match.start(), task_id))
    return _dedupe(task_id for _, task_id in sorted(matches))


def _resolve_task_ref(ref: str, task_lookup: Mapping[str, str]) -> str | None:
    normalized = _normalize_lookup_key(ref)
    if normalized in task_lookup:
        return task_lookup[normalized]
    refs = _resolve_task_refs(ref, task_lookup)
    return refs[0] if len(refs) == 1 else None


def _task_lookup(tasks: list[dict[str, Any]]) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for index, task in enumerate(tasks, start=1):
        task_id = _task_id(task, index)
        if key := _optional_text(task_id):
            lookup[_normalize_lookup_key(key)] = task_id
        if title := _optional_text(task.get("title")):
            normalized_title = _normalize_lookup_key(title)
            if len(normalized_title.split()) >= 2:
                lookup[normalized_title] = task_id
    return dict(sorted(lookup.items()))


def _row_from_bucket(
    dependency_type: PlanFeatureDependencyType,
    bucket: _DependencyBucket,
) -> PlanFeatureDependencyReadinessRow:
    upstream_task_ids = tuple(_dedupe(bucket.upstream_task_ids))
    downstream_task_ids = tuple(_dedupe(bucket.downstream_task_ids))
    readiness_status: PlanFeatureDependencyReadinessStatus = (
        "needs_clarification"
        if bucket.needs_clarification or not upstream_task_ids or not downstream_task_ids
        else "ready"
    )
    return PlanFeatureDependencyReadinessRow(
        dependency_type=dependency_type,
        upstream_task_ids=upstream_task_ids,
        downstream_task_ids=downstream_task_ids,
        evidence=tuple(_dedupe(bucket.evidence)),
        readiness_status=readiness_status,
        recommended_owner=_recommended_owner(dependency_type, readiness_status),
        recommended_next_step=_recommended_next_step(
            dependency_type,
            readiness_status,
            upstream_task_ids=upstream_task_ids,
            downstream_task_ids=downstream_task_ids,
        ),
    )


def _recommended_owner(
    dependency_type: PlanFeatureDependencyType,
    readiness_status: PlanFeatureDependencyReadinessStatus,
) -> str:
    if readiness_status == "needs_clarification":
        return "plan_owner"
    return {
        "prerequisite_feature": "upstream_feature_owner",
        "shared_schema": "schema_owner",
        "shared_api_contract": "api_contract_owner",
        "shared_flag": "release_owner",
        "migration_order": "data_migration_owner",
        "rollout_order": "release_owner",
        "validation_order": "qa_owner",
    }[dependency_type]


def _recommended_next_step(
    dependency_type: PlanFeatureDependencyType,
    readiness_status: PlanFeatureDependencyReadinessStatus,
    *,
    upstream_task_ids: tuple[str, ...],
    downstream_task_ids: tuple[str, ...],
) -> str:
    if readiness_status == "needs_clarification":
        return "Clarify the upstream and downstream task ids before parallel execution starts."
    details = f"{', '.join(upstream_task_ids)} before {', '.join(downstream_task_ids)}"
    return {
        "prerequisite_feature": f"Confirm prerequisite feature completion order: {details}.",
        "shared_schema": f"Freeze the shared schema handoff and compatibility checks: {details}.",
        "shared_api_contract": f"Confirm the shared API contract and contract tests: {details}.",
        "shared_flag": f"Coordinate shared flag ownership, defaults, and cleanup: {details}.",
        "migration_order": f"Sequence migrations, backfills, and rollback checks: {details}.",
        "rollout_order": f"Sequence rollout gates and release ownership: {details}.",
        "validation_order": f"Run validation in dependency order: {details}.",
    }[dependency_type]


def _summary(
    rows: tuple[PlanFeatureDependencyReadinessRow, ...],
    *,
    task_count: int,
    no_dependency_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "dependency_row_count": len(rows),
        "no_dependency_task_count": len(no_dependency_task_ids),
        "readiness_status_counts": {
            status: sum(1 for row in rows if row.readiness_status == status)
            for status in _READINESS_STATUS_ORDER
        },
        "dependency_type_counts": {
            dependency_type: sum(1 for row in rows if row.dependency_type == dependency_type)
            for dependency_type in _DEPENDENCY_TYPE_ORDER
        },
        "upstream_task_count": len({task_id for row in rows for task_id in row.upstream_task_ids}),
        "downstream_task_count": len({task_id for row in rows for task_id in row.downstream_task_ids}),
        "no_dependency_task_ids": list(no_dependency_task_ids),
    }


def _merge_signals(signals: list[_DependencySignal]) -> list[_DependencySignal]:
    buckets: dict[PlanFeatureDependencyType, _DependencyBucket] = {}
    for signal in signals:
        bucket = buckets.setdefault(signal.dependency_type, _DependencyBucket())
        bucket.upstream_task_ids.extend(signal.upstream_task_ids)
        bucket.downstream_task_ids.extend(signal.downstream_task_ids)
        bucket.evidence.extend(signal.evidence)
        bucket.needs_clarification = bucket.needs_clarification or signal.needs_clarification
    return [
        _DependencySignal(
            dependency_type=dependency_type,
            upstream_task_ids=tuple(_dedupe(bucket.upstream_task_ids)),
            downstream_task_ids=tuple(_dedupe(bucket.downstream_task_ids)),
            evidence=tuple(_dedupe(bucket.evidence)),
            needs_clarification=bucket.needs_clarification,
        )
        for dependency_type, bucket in sorted(
            buckets.items(),
            key=lambda item: _DEPENDENCY_TYPE_ORDER.index(item[0]),
        )
    ]


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
        "blocked_by",
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
        "validation_plan",
        "blocked_reason",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in (
        "acceptance_criteria",
        "tags",
        "labels",
        "notes",
        "risks",
        "files_or_modules",
        "files",
        "paths",
        "depends_on",
        "dependencies",
        "blocked_by",
        "validation_commands",
        "test_commands",
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
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.append((field, key_text))
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                texts.append((field, f"{key_text}: {text}"))
            else:
                texts.append((field, key_text))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts: list[tuple[str, str]] = []
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


def _normalize_lookup_key(value: str) -> str:
    return _SPACE_RE.sub(" ", str(value).casefold().replace("_", " ").replace("-", " ")).strip()


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
    "PlanFeatureDependencyReadinessMatrix",
    "PlanFeatureDependencyReadinessRow",
    "PlanFeatureDependencyReadinessStatus",
    "PlanFeatureDependencyType",
    "build_plan_feature_dependency_readiness_matrix",
    "derive_plan_feature_dependency_readiness_matrix",
    "extract_plan_feature_dependency_readiness_matrix",
    "generate_plan_feature_dependency_readiness_matrix",
    "plan_feature_dependency_readiness_matrix_to_dict",
    "plan_feature_dependency_readiness_matrix_to_dicts",
    "plan_feature_dependency_readiness_matrix_to_markdown",
    "summarize_plan_feature_dependency_readiness_matrix",
]
