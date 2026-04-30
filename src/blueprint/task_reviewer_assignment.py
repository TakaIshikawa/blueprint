"""Recommend reviewer roles for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


ReviewerRole = Literal[
    "code_owner",
    "qa",
    "security",
    "data",
    "docs",
    "product",
    "release",
]
ReviewerPriority = Literal["low", "medium", "high"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_CODE_PATH_RE = re.compile(
    r"\.(?:py|js|jsx|ts|tsx|go|rs|java|kt|rb|php|cs|swift|m|mm|c|cc|cpp|h|hpp)$|"
    r"(?:^|/)(?:src|app|lib|packages|blueprint)(?:/|$)",
    re.IGNORECASE,
)
_TEST_PATH_RE = re.compile(r"(?:^|/)(?:tests?|spec|e2e|integration)(?:/|$)|_test\.|\.spec\.", re.I)
_DOC_PATH_RE = re.compile(r"\.(?:md|mdx|rst|txt|adoc)$|(?:^|/)(?:docs?|guides?|runbooks?)(?:/|$)", re.I)
_DATA_PATH_RE = re.compile(
    r"\.(?:sql|ddl)$|(?:^|/)(?:migrations?|alembic|versions|models?|schemas?|"
    r"store|repository|repositories|db|database)(?:/|$)|models?\.py$",
    re.IGNORECASE,
)
_SECURITY_PATH_RE = re.compile(
    r"(?:^|/)(?:auth|authentication|authorization|security|crypto|oauth|"
    r"permissions?|secrets?|tokens?)(?:/|$)|(?:password|permission|secret|token|auth)\w*\.",
    re.IGNORECASE,
)
_RELEASE_PATH_RE = re.compile(
    r"(?:^|/)(?:deploy|deployment|release|releases|infra|ops|charts?|helm|k8s|"
    r"kubernetes|terraform|migrations?)(?:/|$)|Dockerfile$|docker-compose|\.github/workflows",
    re.IGNORECASE,
)

_QA_TEXT_RE = re.compile(
    r"\b(?:test|tests|tested|pytest|qa|quality|acceptance|validate|validation|"
    r"integration|e2e|regression)\b",
    re.IGNORECASE,
)
_SECURITY_TEXT_RE = re.compile(
    r"\b(?:auth|authentication|authorization|permission|permissions|rbac|oauth|"
    r"secret|secrets|token|tokens|password|encrypt|encryption|crypto|csrf|xss|"
    r"vulnerability|security|sanitize|sanitization)\b",
    re.IGNORECASE,
)
_DATA_TEXT_RE = re.compile(
    r"\b(?:database|db|sql|schema|migration|migrations|alembic|model|orm|"
    r"table|column|backfill|seed|persist|storage|repository|index|indexes)\b",
    re.IGNORECASE,
)
_DOC_TEXT_RE = re.compile(r"\b(?:docs?|document|documentation|readme|runbook|guide|changelog)\b", re.I)
_PRODUCT_TEXT_RE = re.compile(
    r"\b(?:user|customer|workflow|ux|ui|copy|pricing|feature|product|"
    r"acceptance criteria|design)\b",
    re.IGNORECASE,
)
_RELEASE_TEXT_RE = re.compile(
    r"\b(?:release|rollout|deploy|deployment|launch|migration window|rollback|"
    r"feature flag|flagged|production|prod|version|upgrade)\b",
    re.IGNORECASE,
)
_HIGH_RISK_VALUES = {"high", "critical", "blocker"}
_PRIORITY_RANK = {"low": 0, "medium": 1, "high": 2}


@dataclass(frozen=True, slots=True)
class TaskReviewerRecommendation:
    """One reviewer role recommendation for a task."""

    role: ReviewerRole
    priority: ReviewerPriority
    reason: str
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "role": self.role,
            "priority": self.priority,
            "reason": self.reason,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskReviewerAssignment:
    """Reviewer recommendations for one execution task."""

    task_id: str
    title: str
    recommendations: tuple[TaskReviewerRecommendation, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "recommendations": [recommendation.to_dict() for recommendation in self.recommendations],
        }


@dataclass(frozen=True, slots=True)
class TaskReviewerAssignmentResult:
    """Complete reviewer assignment result for a plan or task collection."""

    plan_id: str | None = None
    assignments: tuple[TaskReviewerAssignment, ...] = field(default_factory=tuple)
    reviewer_demand_by_role: dict[ReviewerRole, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "assignments": [assignment.to_dict() for assignment in self.assignments],
            "reviewer_demand_by_role": dict(self.reviewer_demand_by_role),
        }


def build_task_reviewer_assignment(
    source: Mapping[str, Any] | ExecutionPlan | ExecutionTask | Iterable[Mapping[str, Any] | ExecutionTask],
) -> TaskReviewerAssignmentResult:
    """Recommend reviewer roles for execution tasks from task context signals."""
    plan_id, tasks = _source_payload(source)
    assignments = tuple(_task_assignment(task, index) for index, task in enumerate(tasks, start=1))
    return TaskReviewerAssignmentResult(
        plan_id=plan_id,
        assignments=assignments,
        reviewer_demand_by_role=_reviewer_demand(assignments),
    )


def task_reviewer_assignment_to_dict(
    result: TaskReviewerAssignmentResult,
) -> dict[str, Any]:
    """Serialize reviewer assignments to a plain dictionary."""
    return result.to_dict()


task_reviewer_assignment_to_dict.__test__ = False


def _task_assignment(task: Mapping[str, Any], index: int) -> TaskReviewerAssignment:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    recommendations = _recommendations(task)
    return TaskReviewerAssignment(
        task_id=task_id,
        title=title,
        recommendations=tuple(sorted(recommendations.values(), key=_recommendation_sort_key)),
    )


def _recommendations(task: Mapping[str, Any]) -> dict[ReviewerRole, TaskReviewerRecommendation]:
    builders: dict[ReviewerRole, _RecommendationBuilder] = {}
    high_risk = _risk_level(task.get("risk_level")) == "high"

    for path in _strings(task.get("files_or_modules")):
        normalized = _normalized_path(path)
        if _SECURITY_PATH_RE.search(normalized):
            _add(builders, "security", "high", f"files_or_modules: {path}")
        if _DATA_PATH_RE.search(normalized):
            _add(builders, "data", "medium", f"files_or_modules: {path}")
        if _DOC_PATH_RE.search(normalized):
            _add(builders, "docs", "low", f"files_or_modules: {path}")
        if _RELEASE_PATH_RE.search(normalized):
            _add(builders, "release", "medium", f"files_or_modules: {path}")
        if _TEST_PATH_RE.search(normalized):
            _add(builders, "qa", "medium", f"files_or_modules: {path}")
        if _CODE_PATH_RE.search(normalized) and not _DOC_PATH_RE.search(normalized):
            _add(builders, "code_owner", "medium", f"files_or_modules: {path}")

    for source_field, text in _task_texts(task):
        _add_text_signals(builders, source_field, text)

    for source_field, text in _metadata_texts(task.get("metadata")):
        field = source_field.casefold()
        if "reviewer" in field or "review" in field:
            _add(builders, _metadata_reviewer_role(text), "medium", f"{source_field}: {text}")
        if "release" in field or "rollout" in field:
            _add(builders, "release", "medium", f"{source_field}: {text}")
        if "security" in field or "auth" in field:
            _add(builders, "security", "high", f"{source_field}: {text}")
        if "data" in field or "migration" in field or "database" in field:
            _add(builders, "data", "medium", f"{source_field}: {text}")
        _add_text_signals(builders, source_field, text)

    owner_type = _optional_text(task.get("owner_type"))
    if owner_type:
        _add(builders, "code_owner", "low", f"owner_type: {owner_type}")

    suggested_engine = _optional_text(task.get("suggested_engine"))
    if suggested_engine:
        _add(builders, "code_owner", "low", f"suggested_engine: {suggested_engine}")

    risk_level = _optional_text(task.get("risk_level"))
    if high_risk:
        _add(builders, "release", "high", f"risk_level: {risk_level or 'high'}")
    elif risk_level:
        _add(builders, "qa", "low", f"risk_level: {risk_level}")

    if not builders:
        _add(builders, "code_owner", "low", "default: No specialized reviewer signal detected.")

    if high_risk and not any(builder.priority == "high" for builder in builders.values()):
        _add(builders, "release", "high", f"risk_level: {risk_level or 'high'}")

    return {
        role: TaskReviewerRecommendation(
            role=role,
            priority=builder.priority,
            reason=_reason(role, builder.priority),
            evidence=tuple(_dedupe(builder.evidence)),
        )
        for role, builder in builders.items()
    }


def _add_text_signals(
    builders: dict[ReviewerRole, "_RecommendationBuilder"],
    source_field: str,
    text: str,
) -> None:
    evidence = f"{source_field}: {text}"
    if _SECURITY_TEXT_RE.search(text):
        _add(builders, "security", "high", evidence)
    if _DATA_TEXT_RE.search(text):
        _add(builders, "data", "medium", evidence)
    if _DOC_TEXT_RE.search(text):
        _add(builders, "docs", "low", evidence)
    if _QA_TEXT_RE.search(text):
        _add(builders, "qa", "medium", evidence)
    if _PRODUCT_TEXT_RE.search(text):
        _add(builders, "product", "low", evidence)
    if _RELEASE_TEXT_RE.search(text):
        _add(builders, "release", "medium", evidence)


def _metadata_reviewer_role(text: str) -> ReviewerRole:
    normalized = text.casefold().replace("_", " ")
    for role in ("security", "data", "docs", "product", "release", "qa"):
        if role in normalized:
            return role  # type: ignore[return-value]
    return "code_owner"


@dataclass(slots=True)
class _RecommendationBuilder:
    priority: ReviewerPriority
    evidence: list[str] = field(default_factory=list)


def _add(
    builders: dict[ReviewerRole, _RecommendationBuilder],
    role: ReviewerRole,
    priority: ReviewerPriority,
    evidence: str,
) -> None:
    if role not in builders:
        builders[role] = _RecommendationBuilder(priority=priority)
    elif _PRIORITY_RANK[priority] > _PRIORITY_RANK[builders[role].priority]:
        builders[role].priority = priority
    builders[role].evidence.append(evidence)


def _reason(role: ReviewerRole, priority: ReviewerPriority) -> str:
    prefix = "Elevated " if priority == "high" else ""
    return {
        "code_owner": f"{prefix}code ownership review is recommended for implementation ownership signals.",
        "qa": f"{prefix}QA review is recommended for validation and acceptance coverage signals.",
        "security": f"{prefix}security review is recommended for auth, secrets, or permission-sensitive signals.",
        "data": f"{prefix}data review is recommended for persistence, schema, migration, or backfill signals.",
        "docs": f"{prefix}documentation review is recommended for docs or user-facing guidance changes.",
        "product": f"{prefix}product review is recommended for workflow, UX, or acceptance-impacting changes.",
        "release": f"{prefix}release review is recommended for rollout, deployment, rollback, or high-risk signals.",
    }[role]


def _recommendation_sort_key(recommendation: TaskReviewerRecommendation) -> tuple[int, str]:
    return (-_PRIORITY_RANK[recommendation.priority], recommendation.role)


def _reviewer_demand(assignments: Iterable[TaskReviewerAssignment]) -> dict[ReviewerRole, int]:
    counts: dict[ReviewerRole, int] = {}
    for assignment in assignments:
        for recommendation in assignment.recommendations:
            counts[recommendation.role] = counts.get(recommendation.role, 0) + 1
    return {role: counts[role] for role in sorted(counts)}


def _source_payload(
    source: Mapping[str, Any] | ExecutionPlan | ExecutionTask | Iterable[Mapping[str, Any] | ExecutionTask],
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

    tasks: list[dict[str, Any]] = []
    for item in source:
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
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
    if not isinstance(value, list):
        return []
    tasks: list[dict[str, Any]] = []
    for item in value:
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


def _task_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in ("title", "description", "milestone", "owner_type", "suggested_engine"):
        text = _optional_text(task.get(field_name))
        if text:
            texts.append((field_name, text))
    for index, text in enumerate(_strings(task.get("acceptance_criteria"))):
        texts.append((f"acceptance_criteria[{index}]", text))
    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif (text := _optional_text(child)):
                texts.append((field, text))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts = []
        for index, item in enumerate(items):
            field = f"{prefix}[{index}]"
            if isinstance(item, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(item, field))
            elif (text := _optional_text(item)):
                texts.append((field, text))
        return texts
    text = _optional_text(value)
    return [(prefix, text)] if text else []


def _risk_level(value: Any) -> ReviewerPriority:
    text = (_optional_text(value) or "").casefold()
    if text in _HIGH_RISK_VALUES:
        return "high"
    if text in {"medium", "moderate"}:
        return "medium"
    return "low"


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


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
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


__all__ = [
    "ReviewerPriority",
    "ReviewerRole",
    "TaskReviewerAssignment",
    "TaskReviewerAssignmentResult",
    "TaskReviewerRecommendation",
    "build_task_reviewer_assignment",
    "task_reviewer_assignment_to_dict",
]
