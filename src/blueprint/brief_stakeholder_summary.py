"""Build compact stakeholder summaries for implementation briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief


@dataclass(frozen=True, slots=True)
class StakeholderSummary:
    """A concise review packet for implementation stakeholders."""

    brief_id: str | None
    title: str | None
    target_audience: str | None = None
    buyer: str | None = None
    workflow_context: str | None = None
    problem_statement: str | None = None
    mvp_goal: str | None = None
    scope_highlights: tuple[str, ...] = field(default_factory=tuple)
    non_goals: tuple[str, ...] = field(default_factory=tuple)
    risks: tuple[str, ...] = field(default_factory=tuple)
    validation_plan: str | None = None
    definition_of_done: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "brief_id": self.brief_id,
            "title": self.title,
            "target_audience": self.target_audience,
            "buyer": self.buyer,
            "workflow_context": self.workflow_context,
            "problem_statement": self.problem_statement,
            "mvp_goal": self.mvp_goal,
            "scope_highlights": list(self.scope_highlights),
            "non_goals": list(self.non_goals),
            "risks": list(self.risks),
            "validation_plan": self.validation_plan,
            "definition_of_done": list(self.definition_of_done),
        }

    def to_markdown(self) -> str:
        """Render the stakeholder summary as deterministic markdown."""
        lines = [f"# {self.title or 'Implementation Brief Stakeholder Summary'}", ""]

        _append_text_section(lines, "Target Audience", self.target_audience)
        _append_text_section(lines, "Buyer", self.buyer)
        _append_text_section(lines, "Workflow Context", self.workflow_context)
        _append_text_section(lines, "Problem Statement", self.problem_statement)
        _append_text_section(lines, "MVP Goal", self.mvp_goal)
        _append_list_section(lines, "Scope Highlights", self.scope_highlights)
        _append_list_section(lines, "Non-Goals", self.non_goals)
        _append_list_section(lines, "Risks", self.risks)
        _append_text_section(lines, "Validation Plan", self.validation_plan)
        _append_list_section(lines, "Definition of Done", self.definition_of_done)

        return "\n".join(lines).rstrip() + "\n"


def build_stakeholder_summary(
    brief: Mapping[str, Any] | ImplementationBrief,
) -> StakeholderSummary:
    """Build a compact stakeholder-facing summary from an implementation brief."""
    payload = _brief_payload(brief)
    return StakeholderSummary(
        brief_id=_optional_text(payload.get("id")),
        title=_optional_text(payload.get("title")),
        target_audience=_optional_text(payload.get("target_user")),
        buyer=_optional_text(payload.get("buyer")),
        workflow_context=_optional_text(payload.get("workflow_context")),
        problem_statement=_optional_text(payload.get("problem_statement")),
        mvp_goal=_optional_text(payload.get("mvp_goal")),
        scope_highlights=tuple(_strings(payload.get("scope"))),
        non_goals=tuple(_strings(payload.get("non_goals"))),
        risks=tuple(_strings(payload.get("risks"))),
        validation_plan=_optional_text(payload.get("validation_plan")),
        definition_of_done=tuple(_strings(payload.get("definition_of_done"))),
    )


def stakeholder_summary_to_dict(summary: StakeholderSummary) -> dict[str, Any]:
    """Serialize a stakeholder summary to a dictionary."""
    return summary.to_dict()


def _brief_payload(brief: Mapping[str, Any] | ImplementationBrief) -> dict[str, Any]:
    if hasattr(brief, "model_dump"):
        return brief.model_dump(mode="python")
    try:
        return ImplementationBrief.model_validate(brief).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        return dict(brief)


def _append_text_section(lines: list[str], heading: str, value: str | None) -> None:
    if not value:
        return
    lines.extend((f"## {heading}", value, ""))


def _append_list_section(lines: list[str], heading: str, values: tuple[str, ...]) -> None:
    if not values:
        return
    lines.append(f"## {heading}")
    lines.extend(f"- {value}" for value in values)
    lines.append("")


def _strings(value: Any) -> list[str]:
    if not isinstance(value, (list, tuple, set)):
        return []
    return [text for item in value if (text := _optional_text(item))]


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text if text else None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


stakeholder_summary_to_dict.__test__ = False


__all__ = [
    "StakeholderSummary",
    "build_stakeholder_summary",
    "stakeholder_summary_to_dict",
]
