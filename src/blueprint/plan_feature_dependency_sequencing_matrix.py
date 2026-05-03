"""Build feature dependency sequencing matrices for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


FeatureDependencySequencingStatus = Literal["covered", "partial", "missing"]
FeatureDependencyCategory = Literal[
    "prerequisite_feature",
    "schema_before_api",
    "api_before_ui",
    "migration_before_backfill",
    "flag_before_rollout",
    "contract_before_integration",
    "docs_before_launch",
]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[FeatureDependencyCategory, ...] = (
    "prerequisite_feature",
    "schema_before_api",
    "api_before_ui",
    "migration_before_backfill",
    "flag_before_rollout",
    "contract_before_integration",
    "docs_before_launch",
)
_STATUS_RANK: dict[FeatureDependencySequencingStatus, int] = {
    "covered": 0,
    "partial": 1,
    "missing": 2,
}
_SPACE_RE = re.compile(r"\s+")
_EXPLICIT_DEPENDENCY_RE = re.compile(
    r"\b(?:depends on|dependent on|blocked by|after|requires|require|needs|need|following)\s+"
    r"(?P<target>[A-Za-z0-9_.:/#-]+)",
    re.I,
)

_ROLE_PATTERNS: dict[str, re.Pattern[str]] = {
    "schema": re.compile(
        r"\b(?:schema|schemas|database|db|model|models|table|tables|column|columns)\b", re.I
    ),
    "api": re.compile(
        r"\b(?:api|apis|endpoint|endpoints|route|routes|controller|controllers|handler|handlers|service)\b",
        re.I,
    ),
    "ui": re.compile(
        r"\b(?:ui|frontend|front[- ]end|client|screen|view|component|components|page|form|button)\b",
        re.I,
    ),
    "migration": re.compile(
        r"\b(?:migration|migrations|migrate|ddl|alembic|liquibase|schema change)\b", re.I
    ),
    "backfill": re.compile(
        r"\b(?:backfill|backfills|data repair|data fix|reprocess|reindex|populate existing)\b", re.I
    ),
    "flag": re.compile(
        r"\b(?:feature flag|flag|flags|kill switch|toggle|gating|gradual enablement)\b", re.I
    ),
    "rollout": re.compile(
        r"\b(?:rollout|roll out|launch|release|enable|production|go live|cutover)\b", re.I
    ),
    "contract": re.compile(
        r"\b(?:contract|contracts|openapi|swagger|schema contract|interface|api spec|protobuf|proto)\b",
        re.I,
    ),
    "integration": re.compile(
        r"\b(?:integration|integrate|partner|webhook|webhooks|client sdk|external api|third party)\b",
        re.I,
    ),
    "docs": re.compile(
        r"\b(?:docs|documentation|runbook|playbook|release notes|operator guide|support guide)\b",
        re.I,
    ),
}

_CATEGORY_ROLE_PAIRS: dict[FeatureDependencyCategory, tuple[str, str]] = {
    "schema_before_api": ("schema", "api"),
    "api_before_ui": ("api", "ui"),
    "migration_before_backfill": ("migration", "backfill"),
    "flag_before_rollout": ("flag", "rollout"),
    "contract_before_integration": ("contract", "integration"),
    "docs_before_launch": ("docs", "rollout"),
}


@dataclass(frozen=True, slots=True)
class PlanFeatureDependencySequencingRow:
    """One plan-level feature sequencing concern."""

    category: FeatureDependencyCategory
    affected_task_ids: tuple[str, ...] = field(default_factory=tuple)
    status: FeatureDependencySequencingStatus = "covered"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    dependency_ids: tuple[str, ...] = field(default_factory=tuple)
    missing_dependency_notes: tuple[str, ...] = field(default_factory=tuple)
    recommended_ordering: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "category": self.category,
            "affected_task_ids": list(self.affected_task_ids),
            "status": self.status,
            "evidence": list(self.evidence),
            "dependency_ids": list(self.dependency_ids),
            "missing_dependency_notes": list(self.missing_dependency_notes),
            "recommended_ordering": list(self.recommended_ordering),
        }


@dataclass(frozen=True, slots=True)
class PlanFeatureDependencySequencingMatrix:
    """Plan-level feature dependency sequencing matrix."""

    plan_id: str | None = None
    rows: tuple[PlanFeatureDependencySequencingRow, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return sequencing rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the sequencing matrix as deterministic Markdown."""
        title = "# Plan Feature Dependency Sequencing Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Row count: {self.summary.get('row_count', 0)}",
            f"- Covered: {self.summary.get('covered_count', 0)}",
            f"- Partial: {self.summary.get('partial_count', 0)}",
            f"- Missing: {self.summary.get('missing_count', 0)}",
        ]
        if not self.rows:
            lines.extend(["", "No feature dependency sequencing risks were detected."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Category | Affected Tasks | Status | Evidence | Recommended Ordering |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"{row.category} | "
                f"{_markdown_cell(', '.join(row.affected_task_ids))} | "
                f"{row.status} | "
                f"{_markdown_cell('; '.join(row.evidence) or 'none')} | "
                f"{_markdown_cell('; '.join(row.recommended_ordering) or 'none')} |"
            )
        return "\n".join(lines)


def build_plan_feature_dependency_sequencing_matrix(
    source: Mapping[str, Any] | ExecutionPlan | Iterable[Mapping[str, Any] | ExecutionTask],
) -> PlanFeatureDependencySequencingMatrix:
    """Detect explicit and implicit feature sequencing risks in an execution plan."""
    plan_id, tasks = _source_payload(source)
    records = _task_records(tasks)
    known_task_ids = {record.task_id for record in records}
    transitive_dependencies = {
        record.task_id: _transitive_dependencies(record.task_id, records) for record in records
    }

    buckets: dict[FeatureDependencyCategory, _RowBucket] = {}
    for record in records:
        for signal in _explicit_dependency_signals(record, known_task_ids):
            _add_signal(buckets, "prerequisite_feature", signal)

    for category in _CATEGORY_ORDER[1:]:
        prerequisite_role, dependent_role = _CATEGORY_ROLE_PAIRS[category]
        prerequisites = [record for record in records if prerequisite_role in record.roles]
        dependents = [record for record in records if dependent_role in record.roles]
        for dependent in dependents:
            candidates = [record for record in prerequisites if record.task_id != dependent.task_id]
            if not candidates:
                continue
            best = _best_prerequisite_candidate(
                dependent=dependent,
                candidates=candidates,
                transitive_dependencies=transitive_dependencies,
            )
            if best:
                _add_signal(buckets, category, best)

    rows = tuple(
        _row_from_bucket(category, buckets[category])
        for category in _CATEGORY_ORDER
        if category in buckets
    )
    return PlanFeatureDependencySequencingMatrix(
        plan_id=plan_id,
        rows=rows,
        summary={
            "row_count": len(rows),
            "covered_count": sum(1 for row in rows if row.status == "covered"),
            "partial_count": sum(1 for row in rows if row.status == "partial"),
            "missing_count": sum(1 for row in rows if row.status == "missing"),
            "affected_task_count": len(
                {task_id for row in rows for task_id in row.affected_task_ids}
            ),
        },
    )


def plan_feature_dependency_sequencing_matrix_to_dict(
    matrix: PlanFeatureDependencySequencingMatrix,
) -> dict[str, Any]:
    """Serialize a feature dependency sequencing matrix to a plain dictionary."""
    return matrix.to_dict()


plan_feature_dependency_sequencing_matrix_to_dict.__test__ = False


def plan_feature_dependency_sequencing_matrix_to_markdown(
    matrix: PlanFeatureDependencySequencingMatrix,
) -> str:
    """Render a feature dependency sequencing matrix as Markdown."""
    return matrix.to_markdown()


plan_feature_dependency_sequencing_matrix_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _TaskRecord:
    task: dict[str, Any]
    task_id: str
    title: str
    index: int
    depends_on: tuple[str, ...]
    search_text: str
    roles: frozenset[str]


@dataclass(frozen=True, slots=True)
class _SequencingSignal:
    status: FeatureDependencySequencingStatus
    affected_task_ids: tuple[str, ...]
    evidence: tuple[str, ...]
    dependency_ids: tuple[str, ...] = field(default_factory=tuple)
    missing_dependency_notes: tuple[str, ...] = field(default_factory=tuple)
    recommended_ordering: tuple[str, ...] = field(default_factory=tuple)


@dataclass(slots=True)
class _RowBucket:
    statuses: list[FeatureDependencySequencingStatus] = field(default_factory=list)
    affected_task_ids: list[str] = field(default_factory=list)
    evidence: list[str] = field(default_factory=list)
    dependency_ids: list[str] = field(default_factory=list)
    missing_dependency_notes: list[str] = field(default_factory=list)
    recommended_ordering: list[str] = field(default_factory=list)


def _explicit_dependency_signals(
    record: _TaskRecord,
    known_task_ids: set[str],
) -> list[_SequencingSignal]:
    signals: list[_SequencingSignal] = []
    for dependency_id in record.depends_on:
        dependency_known = dependency_id in known_task_ids
        signals.append(
            _SequencingSignal(
                status="covered" if dependency_known else "missing",
                affected_task_ids=tuple(
                    _dedupe(
                        [dependency_id, record.task_id] if dependency_known else [record.task_id]
                    )
                ),
                dependency_ids=(dependency_id,),
                evidence=(f"{record.task_id} depends_on {dependency_id}",),
                missing_dependency_notes=(
                    ()
                    if dependency_known
                    else (
                        f"{record.task_id} depends_on references unknown task '{dependency_id}'.",
                    )
                ),
                recommended_ordering=(f"{dependency_id} before {record.task_id}",),
            )
        )

    for target in _text_dependency_targets(record.search_text):
        if target in record.depends_on:
            continue
        if target in known_task_ids:
            signals.append(
                _SequencingSignal(
                    status="partial",
                    affected_task_ids=(target, record.task_id),
                    evidence=(f"{record.task_id} text references prerequisite {target}",),
                    missing_dependency_notes=(f"Add {target} to {record.task_id}.depends_on.",),
                    recommended_ordering=(f"{target} before {record.task_id}",),
                )
            )
        elif _looks_like_task_id(target):
            signals.append(
                _SequencingSignal(
                    status="missing",
                    affected_task_ids=(record.task_id,),
                    evidence=(f"{record.task_id} text references prerequisite {target}",),
                    missing_dependency_notes=(
                        f"{record.task_id} references unknown prerequisite '{target}'.",
                    ),
                    recommended_ordering=(f"{target} before {record.task_id}",),
                )
            )
    return signals


def _best_prerequisite_candidate(
    *,
    dependent: _TaskRecord,
    candidates: list[_TaskRecord],
    transitive_dependencies: Mapping[str, set[str]],
) -> _SequencingSignal | None:
    covered = [
        candidate
        for candidate in candidates
        if candidate.task_id in transitive_dependencies.get(dependent.task_id, set())
    ]
    if covered:
        prerequisite = sorted(covered, key=lambda item: (item.index, item.task_id))[0]
        return _implicit_signal("covered", prerequisite, dependent)

    before = [candidate for candidate in candidates if candidate.index < dependent.index]
    if before:
        prerequisite = sorted(before, key=lambda item: (item.index, item.task_id))[0]
        return _implicit_signal(
            "partial",
            prerequisite,
            dependent,
            note=f"Encode {prerequisite.task_id} as a dependency of {dependent.task_id} before parallel execution.",
        )

    prerequisite = sorted(candidates, key=lambda item: (item.index, item.task_id))[0]
    return _implicit_signal(
        "missing",
        prerequisite,
        dependent,
        note=(
            f"Move {prerequisite.task_id} before {dependent.task_id} and add an explicit dependency "
            f"before parallel execution."
        ),
    )


def _implicit_signal(
    status: FeatureDependencySequencingStatus,
    prerequisite: _TaskRecord,
    dependent: _TaskRecord,
    note: str | None = None,
) -> _SequencingSignal:
    dependency_ids = (prerequisite.task_id,) if prerequisite.task_id in dependent.depends_on else ()
    return _SequencingSignal(
        status=status,
        affected_task_ids=(prerequisite.task_id, dependent.task_id),
        dependency_ids=dependency_ids,
        evidence=(
            f"{prerequisite.task_id} signals {', '.join(sorted(prerequisite.roles))}",
            f"{dependent.task_id} signals {', '.join(sorted(dependent.roles))}",
        ),
        missing_dependency_notes=(note,) if note else (),
        recommended_ordering=(f"{prerequisite.task_id} before {dependent.task_id}",),
    )


def _add_signal(
    buckets: dict[FeatureDependencyCategory, _RowBucket],
    category: FeatureDependencyCategory,
    signal: _SequencingSignal,
) -> None:
    bucket = buckets.setdefault(category, _RowBucket())
    bucket.statuses.append(signal.status)
    bucket.affected_task_ids.extend(signal.affected_task_ids)
    bucket.evidence.extend(signal.evidence)
    bucket.dependency_ids.extend(signal.dependency_ids)
    bucket.missing_dependency_notes.extend(signal.missing_dependency_notes)
    bucket.recommended_ordering.extend(signal.recommended_ordering)


def _row_from_bucket(
    category: FeatureDependencyCategory,
    bucket: _RowBucket,
) -> PlanFeatureDependencySequencingRow:
    return PlanFeatureDependencySequencingRow(
        category=category,
        affected_task_ids=tuple(_dedupe(bucket.affected_task_ids)),
        status=max(bucket.statuses, key=lambda status: _STATUS_RANK[status]),
        evidence=tuple(_dedupe(bucket.evidence)),
        dependency_ids=tuple(_dedupe(bucket.dependency_ids)),
        missing_dependency_notes=tuple(_dedupe(bucket.missing_dependency_notes)),
        recommended_ordering=tuple(_dedupe(bucket.recommended_ordering)),
    )


def _source_payload(
    source: Mapping[str, Any] | ExecutionPlan | Iterable[Mapping[str, Any] | ExecutionTask],
) -> tuple[str | None, list[dict[str, Any]]]:
    if isinstance(source, ExecutionPlan):
        return _optional_text(source.id), [task.model_dump(mode="python") for task in source.tasks]
    if isinstance(source, Mapping):
        if "tasks" in source:
            plan = _plan_payload(source)
            return _optional_text(plan.get("id")), _task_payloads(plan.get("tasks"))
        return None, [dict(source)]
    if hasattr(source, "tasks"):
        plan = _object_payload(source)
        return _optional_text(plan.get("id")), _task_payloads(plan.get("tasks"))

    tasks: list[dict[str, Any]] = []
    for item in source:
        if task := _task_payload(item):
            tasks.append(task)
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


def _task_records(tasks: list[dict[str, Any]]) -> tuple[_TaskRecord, ...]:
    records: list[_TaskRecord] = []
    seen_ids: set[str] = set()
    for index, task in enumerate(tasks, start=1):
        task_id = _optional_text(task.get("id")) or f"task-{index}"
        if task_id in seen_ids:
            continue
        seen_ids.add(task_id)
        title = _optional_text(task.get("title")) or task_id
        search_text = _task_search_text(task)
        records.append(
            _TaskRecord(
                task=task,
                task_id=task_id,
                title=title,
                index=index,
                depends_on=tuple(
                    _dedupe(_strings(task.get("depends_on") or task.get("dependencies")))
                ),
                search_text=search_text,
                roles=frozenset(
                    role for role, pattern in _ROLE_PATTERNS.items() if pattern.search(search_text)
                ),
            )
        )
    return tuple(records)


def _task_search_text(task: Mapping[str, Any]) -> str:
    parts: list[str] = []
    for key in (
        "id",
        "title",
        "description",
        "files_or_modules",
        "acceptance_criteria",
        "blocked_reason",
        "risks",
    ):
        parts.extend(_strings(task.get(key)))
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        parts.extend(_strings(metadata))
    return " ".join(parts)


def _transitive_dependencies(task_id: str, records: tuple[_TaskRecord, ...]) -> set[str]:
    dependencies_by_task_id = {record.task_id: record.depends_on for record in records}
    known_task_ids = set(dependencies_by_task_id)
    visited: set[str] = set()
    pending = list(dependencies_by_task_id.get(task_id, ()))
    while pending:
        dependency_id = pending.pop(0)
        if dependency_id in visited:
            continue
        visited.add(dependency_id)
        if dependency_id in known_task_ids:
            pending.extend(dependencies_by_task_id.get(dependency_id, ()))
    return visited


def _text_dependency_targets(text: str) -> list[str]:
    return _dedupe(
        match.group("target").rstrip(".,;:)") for match in _EXPLICIT_DEPENDENCY_RE.finditer(text)
    )


def _looks_like_task_id(value: str) -> bool:
    return bool(re.search(r"(?:^task[-_:]|[-_:]task[-_:]|[A-Za-z]+-\d+$)", value))


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
    return _object_payload(value)


def _object_payload(value: Any) -> dict[str, Any]:
    if value is None or isinstance(value, (str, bytes)):
        return {}
    data: dict[str, Any] = {}
    for name in dir(value):
        if name.startswith("_"):
            continue
        try:
            item = getattr(value, name)
        except Exception:
            continue
        if not callable(item):
            data[name] = item
    return data


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


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = _SPACE_RE.sub(" ", str(value)).strip()
    return text or None


def _dedupe(values: Iterable[_T | None]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", "<br>")


__all__ = [
    "FeatureDependencyCategory",
    "FeatureDependencySequencingStatus",
    "PlanFeatureDependencySequencingMatrix",
    "PlanFeatureDependencySequencingRow",
    "build_plan_feature_dependency_sequencing_matrix",
    "plan_feature_dependency_sequencing_matrix_to_dict",
    "plan_feature_dependency_sequencing_matrix_to_markdown",
]
