"""Detect execution tasks that may violate brief non-goals."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ImplementationBrief


ConflictConfidence = Literal["high", "medium"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_TOKEN_RE = re.compile(r"[a-z0-9]+")
_SPLIT_RE = re.compile(r"(?:\r?\n|;)+")
_DIRECTIVE_RE = re.compile(
    r"^(?:do\s+not|don't|dont|should\s+not|must\s+not|will\s+not|never|no|avoid|"
    r"exclude|excluding|out\s+of\s+scope|non[- ]?goal|not)\s+",
    re.IGNORECASE,
)
_STOP_WORDS = {
    "a",
    "an",
    "and",
    "any",
    "are",
    "as",
    "at",
    "be",
    "by",
    "can",
    "do",
    "for",
    "from",
    "in",
    "into",
    "is",
    "it",
    "its",
    "not",
    "of",
    "on",
    "or",
    "our",
    "scope",
    "should",
    "the",
    "their",
    "this",
    "to",
    "with",
    "without",
}
_BOILERPLATE_TERMS = {
    "add",
    "avoid",
    "build",
    "change",
    "create",
    "deliver",
    "edit",
    "enable",
    "exclude",
    "feature",
    "fix",
    "goal",
    "handle",
    "implement",
    "include",
    "make",
    "new",
    "non",
    "plan",
    "provide",
    "remove",
    "ship",
    "support",
    "task",
    "update",
    "use",
    "work",
}


@dataclass(frozen=True, slots=True)
class BriefNonGoalConflict:
    """One non-goal with tasks that may contradict it."""

    non_goal_id: str
    non_goal: str
    matched_task_ids: tuple[str, ...]
    confidence: ConflictConfidence
    matched_terms: tuple[str, ...] = field(default_factory=tuple)
    remediation: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "non_goal_id": self.non_goal_id,
            "non_goal": self.non_goal,
            "matched_task_ids": list(self.matched_task_ids),
            "confidence": self.confidence,
            "matched_terms": list(self.matched_terms),
            "remediation": self.remediation,
        }


@dataclass(frozen=True, slots=True)
class BriefNonGoalConflictReport:
    """Non-goal conflict findings for an implementation brief and execution plan."""

    brief_id: str | None = None
    plan_id: str | None = None
    conflicts: tuple[BriefNonGoalConflict, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "brief_id": self.brief_id,
            "plan_id": self.plan_id,
            "conflicts": [conflict.to_dict() for conflict in self.conflicts],
            "summary": dict(self.summary),
        }


def detect_brief_non_goal_conflicts(
    implementation_brief: Mapping[str, Any] | ImplementationBrief,
    execution_plan: Mapping[str, Any] | ExecutionPlan,
) -> BriefNonGoalConflictReport:
    """Find plan tasks that may violate explicitly excluded brief scope."""
    brief = _brief_payload(implementation_brief)
    plan = _plan_payload(execution_plan)
    non_goals = _strings(brief.get("non_goals"))
    tasks = _task_payloads(plan.get("tasks"))
    conflicts = tuple(
        conflict
        for index, non_goal in enumerate(non_goals, start=1)
        if (conflict := _non_goal_conflict(non_goal, index, tasks)) is not None
    )
    high_count = sum(1 for conflict in conflicts if conflict.confidence == "high")
    medium_count = sum(1 for conflict in conflicts if conflict.confidence == "medium")

    return BriefNonGoalConflictReport(
        brief_id=_optional_text(brief.get("id")),
        plan_id=_optional_text(plan.get("id")),
        conflicts=conflicts,
        summary={
            "non_goal_count": len(non_goals),
            "conflict_count": len(conflicts),
            "high_confidence_count": high_count,
            "medium_confidence_count": medium_count,
        },
    )


def brief_non_goal_conflicts_to_dict(
    report: BriefNonGoalConflictReport,
) -> dict[str, Any]:
    """Serialize a brief non-goal conflict report to a plain dictionary."""
    return report.to_dict()


brief_non_goal_conflicts_to_dict.__test__ = False


def _non_goal_conflict(
    non_goal: str,
    index: int,
    tasks: list[dict[str, Any]],
) -> BriefNonGoalConflict | None:
    matches = [
        _task_match(non_goal, task, task_index)
        for task_index, task in enumerate(tasks, start=1)
    ]
    matches = [match for match in matches if match is not None]
    if not matches:
        return None

    confidence: ConflictConfidence = (
        "high" if any(match[1] == "high" for match in matches) else "medium"
    )
    task_ids = tuple(_dedupe(match[0] for match in matches))
    matched_terms = tuple(_dedupe(term for match in matches for term in match[2]))
    non_goal_id = f"non_goal-{index}"
    remediation_non_goal = _snippet(non_goal, 96).rstrip(".")
    return BriefNonGoalConflict(
        non_goal_id=non_goal_id,
        non_goal=non_goal,
        matched_task_ids=task_ids,
        confidence=confidence,
        matched_terms=matched_terms,
        remediation=(
            f"Review tasks {', '.join(task_ids)} against non-goal: {remediation_non_goal}. "
            "Remove the conflicting scope or document an explicit brief change."
        ),
    )


def _task_match(
    non_goal: str,
    task: Mapping[str, Any],
    index: int,
) -> tuple[str, ConflictConfidence, tuple[str, ...]] | None:
    task_id = _task_id(task, index)
    non_goal_phrase = _normalized(_scope_phrase(non_goal))
    non_goal_tokens = _significant_tokens(non_goal)
    if not non_goal_phrase or not non_goal_tokens:
        return None

    best_confidence: ConflictConfidence | None = None
    terms: list[str] = []
    task_overlap: set[str] = set()
    for _field, text in _task_text_fields(task):
        normalized_text = _normalized(text)
        if normalized_text and _boundary_search(normalized_text, non_goal_phrase):
            best_confidence = "high"
            terms.append(non_goal_phrase)
            continue

        overlap = non_goal_tokens & _significant_tokens(text)
        task_overlap.update(overlap)
        if _is_meaningful_overlap(non_goal_tokens, overlap):
            if best_confidence == "high":
                continue
            if best_confidence != "high":
                best_confidence = "medium"
            terms.extend(sorted(overlap))

    if best_confidence is None and _is_meaningful_overlap(non_goal_tokens, task_overlap):
        best_confidence = "medium"
        terms.extend(sorted(task_overlap))

    if best_confidence is None:
        return None
    return task_id, best_confidence, tuple(_dedupe(terms))


def _task_text_fields(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    fields: list[tuple[str, str]] = []
    for key in ("title", "description"):
        if text := _optional_text(task.get(key)):
            fields.append((key, text))
    for index, text in enumerate(_strings(task.get("acceptance_criteria"))):
        fields.append((f"acceptance_criteria[{index}]", text))
    for index, text in enumerate(_strings(task.get("files_or_modules"))):
        fields.append((f"files_or_modules[{index}]", text))
    for index, text in enumerate(_strings(task.get("tags"))):
        fields.append((f"tags[{index}]", text))
    for index, text in enumerate(_strings(_metadata_tags(task.get("metadata")))):
        fields.append((f"metadata.tags[{index}]", text))
    return fields


def _metadata_tags(value: Any) -> Any:
    if not isinstance(value, Mapping):
        return None
    return value.get("tags") or value.get("labels")


def _is_meaningful_overlap(non_goal_tokens: set[str], overlap: set[str]) -> bool:
    if not overlap:
        return False
    if len(non_goal_tokens) == 1:
        token = next(iter(non_goal_tokens))
        return token in overlap and len(token) >= 6
    if len(non_goal_tokens) <= 3:
        return len(overlap) >= 2
    return len(overlap) >= 3 and len(overlap) / len(non_goal_tokens) >= 0.5


def _scope_phrase(value: str) -> str:
    phrase = _text(value)
    previous = None
    while phrase and phrase != previous:
        previous = phrase
        phrase = _DIRECTIVE_RE.sub("", phrase).strip(" .,:;-")
    return phrase


def _brief_payload(brief: Mapping[str, Any] | ImplementationBrief) -> dict[str, Any]:
    if hasattr(brief, "model_dump"):
        value = brief.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ImplementationBrief.model_validate(brief).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        return dict(brief) if isinstance(brief, Mapping) else {}


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


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item for part in _SPLIT_RE.split(value) if (item := _optional_text(part))]
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


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id")) or f"task-{index}"


def _boundary_search(text: str, phrase: str) -> bool:
    return re.search(rf"(?<![a-z0-9]){re.escape(phrase)}(?![a-z0-9])", text) is not None


def _normalized(value: str) -> str:
    return " ".join(_tokens(value))


def _tokens(value: str) -> list[str]:
    return [_stem(token) for token in _TOKEN_RE.findall(value.casefold())]


def _significant_tokens(value: str) -> set[str]:
    return {
        token
        for token in _tokens(_scope_phrase(value))
        if token not in _STOP_WORDS
        and token not in _BOILERPLATE_TERMS
        and not token.isdigit()
        and len(token) > 1
    }


def _stem(token: str) -> str:
    for suffix in ("ing", "ies", "ied", "ed", "es", "s", "ers"):
        if len(token) > len(suffix) + 3 and token.endswith(suffix):
            if suffix in {"ies", "ied"}:
                return f"{token[:-3]}y"
            return token[: -len(suffix)]
    return token


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _snippet(value: str, limit: int = 140) -> str:
    text = _text(value)
    if len(text) <= limit:
        return text
    return f"{text[: limit - 1].rstrip()}..."


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
    "BriefNonGoalConflict",
    "BriefNonGoalConflictReport",
    "ConflictConfidence",
    "brief_non_goal_conflicts_to_dict",
    "detect_brief_non_goal_conflicts",
]
