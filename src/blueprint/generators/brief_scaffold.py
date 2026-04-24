"""Offline implementation brief scaffolding from normalized source briefs."""

from __future__ import annotations

from typing import Any

from blueprint.generators.brief_generator import generate_implementation_brief_id


def scaffold_implementation_brief(source_brief: dict[str, Any]) -> dict[str, Any]:
    """Build a draft ImplementationBrief without invoking an LLM provider."""
    normalized = _normalized_payload(source_brief)
    title = _first_string(normalized.get("title"), source_brief.get("title"), "Untitled brief")
    summary = _first_string(
        normalized.get("summary"),
        source_brief.get("summary"),
        f"Implementation work for {title}.",
    )
    mvp_goal = _first_string(
        normalized.get("mvp_goal"),
        normalized.get("goal"),
        summary,
    )

    return {
        "id": generate_implementation_brief_id(),
        "source_brief_id": source_brief["id"],
        "title": title,
        "domain": _optional_string(normalized.get("domain"))
        or _optional_string(source_brief.get("domain")),
        "target_user": _optional_string(normalized.get("target_user")),
        "buyer": _optional_string(normalized.get("buyer")),
        "workflow_context": _optional_string(normalized.get("workflow_context")),
        "problem_statement": summary,
        "mvp_goal": mvp_goal,
        "product_surface": _optional_string(normalized.get("product_surface")),
        "scope": _string_list(
            normalized.get("scope"),
            default=[f"Implement the core workflow described by source brief {source_brief['id']}."],
        ),
        "non_goals": _string_list(normalized.get("non_goals")),
        "assumptions": _string_list(normalized.get("assumptions")),
        "architecture_notes": _optional_string(normalized.get("architecture_notes")),
        "data_requirements": _optional_string(normalized.get("data_requirements")),
        "integration_points": _string_list(normalized.get("integration_points")),
        "risks": _string_list(normalized.get("risks")),
        "validation_plan": _first_string(
            normalized.get("validation_plan"),
            "Review the scaffolded brief, then validate the implemented scope with project tests.",
        ),
        "definition_of_done": _string_list(
            normalized.get("definition_of_done"),
            default=[
                "Implementation brief is reviewed and updated with project-specific details.",
                "Planned scope is validated against the source brief.",
            ],
        ),
        "status": "draft",
        "generation_model": "scaffold",
        "generation_tokens": 0,
        "generation_prompt": (
            "Offline scaffold generated from SourceBrief normalized payload when available; "
            "otherwise title and summary defaults were used."
        ),
    }


def _normalized_payload(source_brief: dict[str, Any]) -> dict[str, Any]:
    source_payload = source_brief.get("source_payload")
    if not isinstance(source_payload, dict):
        return {}

    normalized = source_payload.get("normalized")
    if isinstance(normalized, dict):
        return normalized
    return {}


def _optional_string(value: Any) -> str | None:
    if isinstance(value, str):
        stripped = value.strip()
        if stripped:
            return stripped
    return None


def _first_string(*values: Any) -> str:
    for value in values:
        string_value = _optional_string(value)
        if string_value is not None:
            return string_value
    raise ValueError("At least one fallback string is required")


def _string_list(value: Any, *, default: list[str] | None = None) -> list[str]:
    if isinstance(value, str):
        stripped = value.strip()
        return [stripped] if stripped else list(default or [])

    if isinstance(value, list):
        items = [item.strip() for item in value if isinstance(item, str) and item.strip()]
        if items:
            return items

    return list(default or [])
