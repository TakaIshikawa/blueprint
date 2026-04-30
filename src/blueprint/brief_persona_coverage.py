"""Check whether execution plans cover persona and workflow context from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ImplementationBrief


PersonaConcernStatus = Literal["covered", "weak", "missing"]
EvidenceStrength = Literal["strong", "weak"]
RecommendationType = Literal["add_task", "add_acceptance_criterion"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_TOKEN_RE = re.compile(r"[a-z0-9]+")
_SPLIT_RE = re.compile(r"(?:\r?\n|;)+")
_BRIEF_FIELDS: tuple[str, ...] = (
    "target_user",
    "buyer",
    "workflow_context",
    "product_surface",
    "mvp_goal",
    "validation_plan",
)
_FIELD_LABELS: dict[str, str] = {
    "target_user": "Target user",
    "buyer": "Buyer",
    "workflow_context": "Workflow context",
    "product_surface": "Product surface",
    "mvp_goal": "MVP goal",
    "validation_plan": "Validation plan",
}
_STOP_WORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "can",
    "for",
    "from",
    "in",
    "into",
    "is",
    "it",
    "of",
    "on",
    "or",
    "our",
    "the",
    "their",
    "to",
    "that",
    "this",
    "through",
    "with",
    "without",
}


@dataclass(frozen=True, slots=True)
class BriefPersonaEvidence:
    """One task field that mentions a brief persona or workflow concern."""

    task_id: str
    field: str
    snippet: str
    strength: EvidenceStrength

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "field": self.field,
            "snippet": self.snippet,
            "strength": self.strength,
        }


@dataclass(frozen=True, slots=True)
class BriefPersonaRecommendation:
    """Deterministic plan addition for missing or weak persona coverage."""

    concern_id: str
    recommendation_type: RecommendationType
    text: str
    task_ids: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "concern_id": self.concern_id,
            "recommendation_type": self.recommendation_type,
            "text": self.text,
            "task_ids": list(self.task_ids),
        }


@dataclass(frozen=True, slots=True)
class BriefPersonaCoverage:
    """Coverage status for one brief persona or workflow concern."""

    concern_id: str
    source_field: str
    concern: str
    status: PersonaConcernStatus
    matched_task_ids: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[BriefPersonaEvidence, ...] = field(default_factory=tuple)
    recommendation: BriefPersonaRecommendation | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "concern_id": self.concern_id,
            "source_field": self.source_field,
            "concern": self.concern,
            "status": self.status,
            "matched_task_ids": list(self.matched_task_ids),
            "evidence": [item.to_dict() for item in self.evidence],
            "recommendation": self.recommendation.to_dict() if self.recommendation else None,
        }


@dataclass(frozen=True, slots=True)
class BriefPersonaCoverageReport:
    """Persona and workflow coverage for an implementation brief and execution plan."""

    brief_id: str | None = None
    plan_id: str | None = None
    covered_personas: tuple[BriefPersonaCoverage, ...] = field(default_factory=tuple)
    weak_coverage: tuple[BriefPersonaCoverage, ...] = field(default_factory=tuple)
    missing_coverage: tuple[BriefPersonaCoverage, ...] = field(default_factory=tuple)
    recommendations: tuple[BriefPersonaRecommendation, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "brief_id": self.brief_id,
            "plan_id": self.plan_id,
            "covered_personas": [item.to_dict() for item in self.covered_personas],
            "weak_coverage": [item.to_dict() for item in self.weak_coverage],
            "missing_coverage": [item.to_dict() for item in self.missing_coverage],
            "recommendations": [item.to_dict() for item in self.recommendations],
            "summary": dict(self.summary),
        }

    def to_markdown(self) -> str:
        """Render the coverage report as deterministic Markdown."""
        title = "# Brief Persona Coverage"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        entries = (*self.covered_personas, *self.weak_coverage, *self.missing_coverage)
        if not entries:
            lines.extend(["", "No persona or workflow concerns were derived."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Concern | Status | Matched Tasks | Evidence | Recommendation |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for entry in entries:
            tasks = ", ".join(entry.matched_task_ids) or "None"
            evidence = "<br>".join(
                _markdown_cell(f"{item.task_id} {item.field}: {item.snippet}")
                for item in entry.evidence
            )
            recommendation = entry.recommendation.text if entry.recommendation else "None"
            lines.append(
                "| "
                f"{_markdown_cell(_label(entry.source_field))}: {_markdown_cell(entry.concern)} | "
                f"{entry.status} | "
                f"{_markdown_cell(tasks)} | "
                f"{evidence or 'None'} | "
                f"{_markdown_cell(recommendation)} |"
            )
        return "\n".join(lines)


def evaluate_brief_persona_coverage(
    implementation_brief: Mapping[str, Any] | ImplementationBrief,
    execution_plan: Mapping[str, Any] | ExecutionPlan,
) -> BriefPersonaCoverageReport:
    """Compare brief persona and workflow concerns against plan tasks."""
    brief = _brief_payload(implementation_brief)
    plan = _plan_payload(execution_plan)
    tasks = _task_payloads(plan.get("tasks"))

    entries = tuple(
        _coverage_entry(concern, source_field, index, tasks)
        for index, (source_field, concern) in enumerate(_brief_concerns(brief), start=1)
    )
    covered = tuple(entry for entry in entries if entry.status == "covered")
    weak = tuple(entry for entry in entries if entry.status == "weak")
    missing = tuple(entry for entry in entries if entry.status == "missing")
    recommendations = tuple(
        entry.recommendation for entry in (*weak, *missing) if entry.recommendation is not None
    )
    return BriefPersonaCoverageReport(
        brief_id=_optional_text(brief.get("id")),
        plan_id=_optional_text(plan.get("id")),
        covered_personas=covered,
        weak_coverage=weak,
        missing_coverage=missing,
        recommendations=recommendations,
        summary={
            "concern_count": len(entries),
            "covered_count": len(covered),
            "weak_count": len(weak),
            "missing_count": len(missing),
        },
    )


def brief_persona_coverage_to_dict(report: BriefPersonaCoverageReport) -> dict[str, Any]:
    """Serialize a brief persona coverage report to a plain dictionary."""
    return report.to_dict()


brief_persona_coverage_to_dict.__test__ = False


def brief_persona_coverage_to_markdown(report: BriefPersonaCoverageReport) -> str:
    """Render a brief persona coverage report as Markdown."""
    return report.to_markdown()


brief_persona_coverage_to_markdown.__test__ = False


def _coverage_entry(
    concern: str,
    source_field: str,
    index: int,
    tasks: list[dict[str, Any]],
) -> BriefPersonaCoverage:
    concern_id = f"{source_field}-{index}"
    evidence = tuple(
        item
        for task_index, task in enumerate(tasks, start=1)
        for item in _task_evidence(concern, task, task_index)
    )
    matched_task_ids = tuple(_dedupe(item.task_id for item in evidence))

    if any(item.strength == "strong" for item in evidence):
        status: PersonaConcernStatus = "covered"
        recommendation = None
    elif evidence:
        status = "weak"
        recommendation = BriefPersonaRecommendation(
            concern_id=concern_id,
            recommendation_type="add_acceptance_criterion",
            text=_acceptance_criterion(source_field, concern),
            task_ids=matched_task_ids,
        )
    else:
        status = "missing"
        recommendation = BriefPersonaRecommendation(
            concern_id=concern_id,
            recommendation_type="add_task",
            text=f"Add plan task: Cover {_label(source_field).lower()} - {_snippet(concern, 72)}.",
        )

    return BriefPersonaCoverage(
        concern_id=concern_id,
        source_field=source_field,
        concern=concern,
        status=status,
        matched_task_ids=matched_task_ids,
        evidence=evidence,
        recommendation=recommendation,
    )


def _task_evidence(
    concern: str,
    task: Mapping[str, Any],
    index: int,
) -> tuple[BriefPersonaEvidence, ...]:
    task_id = _task_id(task, index)
    evidence: list[BriefPersonaEvidence] = []

    for field, text in _task_text_fields(task):
        if not _matches_concern(concern, text):
            continue
        evidence.append(
            BriefPersonaEvidence(
                task_id=task_id,
                field=field,
                snippet=_snippet(text),
                strength="strong" if _is_strong_field(field) else "weak",
            )
        )
        if len(evidence) == 4:
            break

    return tuple(evidence)


def _task_text_fields(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    fields: list[tuple[str, str]] = []
    for key in ("title", "description"):
        if text := _optional_text(task.get(key)):
            fields.append((key, text))
    for index, text in enumerate(_strings(task.get("acceptance_criteria"))):
        fields.append((f"acceptance_criteria[{index}]", text))
    for key in ("test_command", "suggested_test_command", "validation_command"):
        if text := _optional_text(task.get(key)):
            fields.append((key, text))
    for field, text in _metadata_texts(task.get("metadata")):
        fields.append((field, text))
    return fields


def _brief_concerns(brief: Mapping[str, Any]) -> list[tuple[str, str]]:
    concerns: list[tuple[str, str]] = []
    for field_name in _BRIEF_FIELDS:
        for value in _strings(brief.get(field_name)):
            concerns.append((field_name, value))
    return concerns


def _matches_concern(concern: str, text: str) -> bool:
    normalized_concern = _normalized(concern)
    normalized_text = _normalized(text)
    if not normalized_concern or not normalized_text:
        return False
    if _boundary_search(normalized_text, normalized_concern):
        return True

    concern_tokens = _significant_tokens(concern)
    if not concern_tokens:
        return False
    text_tokens = _tokens(text)
    overlap = concern_tokens & text_tokens
    if len(concern_tokens) <= 2:
        return len(overlap) == len(concern_tokens)
    if len(concern_tokens) <= 5:
        return len(overlap) >= max(2, len(concern_tokens) - 1)
    return len(overlap) / len(concern_tokens) >= 0.55 and len(overlap) >= 3


def _is_strong_field(field: str) -> bool:
    return (
        field.startswith("acceptance_criteria")
        or "validation" in field
        or field == "test_command"
        or field.startswith("metadata.validation")
    )


def _acceptance_criterion(source_field: str, concern: str) -> str:
    label = _label(source_field)
    return f"{label} concern is validated: {_snippet(concern, 96)}."


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


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
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


def _tokens(value: str) -> set[str]:
    return {_stem(token) for token in _TOKEN_RE.findall(value.casefold())}


def _significant_tokens(value: str) -> set[str]:
    return {
        token
        for token in _tokens(value)
        if token not in _STOP_WORDS and not token.isdigit() and len(token) > 1
    }


def _stem(token: str) -> str:
    for suffix in ("ing", "ers", "ies", "ied", "ed", "es", "s"):
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


def _label(source_field: str) -> str:
    return _FIELD_LABELS.get(source_field, source_field.replace("_", " ").title())


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
    return value.replace("|", "\\|").replace("\n", " ")


__all__ = [
    "BriefPersonaCoverage",
    "BriefPersonaCoverageReport",
    "BriefPersonaEvidence",
    "BriefPersonaRecommendation",
    "EvidenceStrength",
    "PersonaConcernStatus",
    "RecommendationType",
    "brief_persona_coverage_to_dict",
    "brief_persona_coverage_to_markdown",
    "evaluate_brief_persona_coverage",
]
