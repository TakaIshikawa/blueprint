"""Task split recommendation audit for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any


MANY_ACCEPTANCE_CRITERIA_THRESHOLD = 4
BROAD_FILE_SCOPE_THRESHOLD = 4
BROAD_MODULE_SCOPE_THRESHOLD = 3

_TOKEN_RE = re.compile(r"[a-z0-9]+")
_VERB_RE = re.compile(
    r"\b("
    r"add|build|change|clean|configure|connect|create|delete|design|document|"
    r"export|extend|fetch|fix|generate|handle|implement|import|integrate|load|"
    r"migrate|move|parse|persist|read|refactor|remove|render|replace|report|"
    r"resolve|return|save|send|split|store|sync|test|update|validate|verify|write"
    r")\b",
    re.IGNORECASE,
)
_VAGUE_PATTERNS = {
    "as needed": re.compile(r"\bas\s+needed\b", re.IGNORECASE),
    "etc": re.compile(r"\betc\.?\b", re.IGNORECASE),
    "everything": re.compile(r"\beverything\b", re.IGNORECASE),
    "handle all": re.compile(r"\bhandle\s+all\b", re.IGNORECASE),
    "miscellaneous": re.compile(r"\bmisc(?:ellaneous)?\b", re.IGNORECASE),
    "stuff": re.compile(r"\bstuff\b", re.IGNORECASE),
    "things": re.compile(r"\bthings\b", re.IGNORECASE),
    "tbd": re.compile(r"\btbd\b", re.IGNORECASE),
    "various": re.compile(r"\bvarious\b", re.IGNORECASE),
}
_HIGH_COMPLEXITY_VALUES = {
    "complex",
    "extra large",
    "high",
    "large",
    "very high",
    "xl",
    "xxl",
}


@dataclass(frozen=True)
class TaskSplitReason:
    """A reason a task should be split before autonomous execution."""

    code: str
    message: str
    value: str | int
    threshold: int | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "code": self.code,
            "message": self.message,
            "value": self.value,
        }
        if self.threshold is not None:
            payload["threshold"] = self.threshold
        return payload


@dataclass(frozen=True)
class SuggestedSubtask:
    """A deterministic split point for an oversized task."""

    title: str
    description: str
    files_or_modules: list[str] = field(default_factory=list)
    acceptance_criteria: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "title": self.title,
            "description": self.description,
            "files_or_modules": self.files_or_modules,
            "acceptance_criteria": self.acceptance_criteria,
        }


@dataclass(frozen=True)
class TaskSplitRecommendation:
    """Split recommendation for one execution task."""

    task_id: str
    title: str
    reasons: list[TaskSplitReason] = field(default_factory=list)
    suggested_subtasks: list[SuggestedSubtask] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "title": self.title,
            "reasons": [reason.to_dict() for reason in self.reasons],
            "suggested_subtasks": [
                suggested_subtask.to_dict()
                for suggested_subtask in self.suggested_subtasks
            ],
        }


@dataclass(frozen=True)
class TaskSplittingResult:
    """Task split recommendation audit result for an execution plan."""

    plan_id: str
    recommendations: list[TaskSplitRecommendation] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.recommendations

    @property
    def recommendation_count(self) -> int:
        return len(self.recommendations)

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "passed": self.passed,
            "summary": {
                "recommendations": self.recommendation_count,
            },
            "recommendations": [
                recommendation.to_dict()
                for recommendation in self.recommendations
            ],
        }


def audit_task_splitting(plan: dict[str, Any]) -> TaskSplittingResult:
    """Identify tasks that are too broad and suggest deterministic split points."""
    recommendations: list[TaskSplitRecommendation] = []

    for task in _list_of_dicts(plan.get("tasks")):
        reasons = _split_reasons(task)
        if not reasons:
            continue
        recommendations.append(
            TaskSplitRecommendation(
                task_id=str(task.get("id") or ""),
                title=str(task.get("title") or ""),
                reasons=reasons,
                suggested_subtasks=_suggested_subtasks(task),
            )
        )

    return TaskSplittingResult(
        plan_id=str(plan.get("id") or ""),
        recommendations=recommendations,
    )


def _split_reasons(task: dict[str, Any]) -> list[TaskSplitReason]:
    reasons: list[TaskSplitReason] = []
    criteria = _string_list(task.get("acceptance_criteria"))
    files = _string_list(task.get("files_or_modules"))
    module_groups = _file_groups(files)
    complexity = _normalized_phrase(str(task.get("estimated_complexity") or ""))
    title = str(task.get("title") or "")
    description = str(task.get("description") or "")
    text = f"{title}. {description}".strip()

    if complexity in _HIGH_COMPLEXITY_VALUES:
        reasons.append(
            TaskSplitReason(
                code="high_complexity",
                message="Task is marked as high complexity.",
                value=complexity,
            )
        )

    if len(files) >= BROAD_FILE_SCOPE_THRESHOLD:
        reasons.append(
            TaskSplitReason(
                code="broad_file_scope",
                message="Task spans many files or modules.",
                value=len(files),
                threshold=BROAD_FILE_SCOPE_THRESHOLD,
            )
        )
    elif len(module_groups) >= BROAD_MODULE_SCOPE_THRESHOLD:
        reasons.append(
            TaskSplitReason(
                code="broad_module_scope",
                message="Task spans several independent module groups.",
                value=len(module_groups),
                threshold=BROAD_MODULE_SCOPE_THRESHOLD,
            )
        )

    if len(criteria) >= MANY_ACCEPTANCE_CRITERIA_THRESHOLD:
        reasons.append(
            TaskSplitReason(
                code="many_acceptance_criteria",
                message="Task has many acceptance criteria.",
                value=len(criteria),
                threshold=MANY_ACCEPTANCE_CRITERIA_THRESHOLD,
            )
        )

    vague_phrase = _matching_vague_phrase(description)
    if vague_phrase:
        reasons.append(
            TaskSplitReason(
                code="vague_description",
                message="Task description uses vague scope language.",
                value=vague_phrase,
            )
        )

    verbs = _independent_verbs(text)
    if len(verbs) >= 3:
        reasons.append(
            TaskSplitReason(
                code="multiple_independent_verbs",
                message="Task combines several independent implementation actions.",
                value=", ".join(verbs),
                threshold=3,
            )
        )

    return reasons


def _suggested_subtasks(task: dict[str, Any]) -> list[SuggestedSubtask]:
    criteria = _string_list(task.get("acceptance_criteria"))
    files = _string_list(task.get("files_or_modules"))
    file_groups = _file_groups(files)

    if len(file_groups) >= 2:
        return [
            SuggestedSubtask(
                title=f"Split {group_name} changes",
                description=f"Implement and validate the {group_name} portion of this task.",
                files_or_modules=group_files,
                acceptance_criteria=_criteria_for_group(criteria, group_name, group_files),
            )
            for group_name, group_files in file_groups.items()
        ]

    if len(criteria) >= 2:
        return [
            SuggestedSubtask(
                title=f"Address criterion {index}",
                description=_shorten(criterion),
                files_or_modules=files,
                acceptance_criteria=[criterion],
            )
            for index, criterion in enumerate(criteria, 1)
        ]

    if files:
        return [
            SuggestedSubtask(
                title="Narrow file-scope implementation",
                description="Limit the task to the listed files and move unrelated work into follow-up tasks.",
                files_or_modules=files,
                acceptance_criteria=criteria,
            )
        ]

    return [
        SuggestedSubtask(
            title="Clarify a focused implementation slice",
            description="Rewrite the task into one independently verifiable change.",
            acceptance_criteria=criteria,
        )
    ]


def _criteria_for_group(
    criteria: list[str],
    group_name: str,
    group_files: list[str],
) -> list[str]:
    tokens = set(_TOKEN_RE.findall(group_name.lower()))
    for path in group_files:
        tokens.update(_TOKEN_RE.findall(path.lower()))
    matches = [
        criterion
        for criterion in criteria
        if tokens.intersection(_TOKEN_RE.findall(criterion.lower()))
    ]
    return matches or criteria[:1]


def _file_groups(files: list[str]) -> dict[str, list[str]]:
    groups: dict[str, list[str]] = {}
    for path in files:
        group_name = _file_group_name(path)
        groups.setdefault(group_name, []).append(path)
    return groups


def _file_group_name(path: str) -> str:
    normalized = path.strip().replace("\\", "/")
    parts = [part for part in normalized.split("/") if part and part != "."]
    if not parts:
        return "unspecified"
    if parts[0] in {"src", "tests"} and len(parts) > 1:
        return f"{parts[0]}/{parts[1]}"
    return parts[0]


def _independent_verbs(text: str) -> list[str]:
    seen: list[str] = []
    for match in _VERB_RE.finditer(text):
        verb = match.group(1).lower()
        if verb not in seen:
            seen.append(verb)
    return seen


def _matching_vague_phrase(description: str) -> str | None:
    for phrase, pattern in _VAGUE_PATTERNS.items():
        if pattern.search(description):
            return phrase
    return None


def _shorten(text: str, *, max_length: int = 96) -> str:
    normalized = " ".join(text.split())
    if len(normalized) <= max_length:
        return normalized
    return normalized[: max_length - 3].rstrip() + "..."


def _normalized_phrase(value: str) -> str:
    return " ".join(_TOKEN_RE.findall(value.lower()))


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item.strip() for item in value if isinstance(item, str) and item.strip()]


def _list_of_dicts(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]
