"""LLM-powered brief enhancement generator for incomplete implementation briefs."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Literal

from blueprint.audits.brief_readiness import (
    BriefReadinessResult,
    audit_brief_readiness,
)
from blueprint.llm.json_parser import parse_json_response
from blueprint.llm.provider import LLMProvider


EnhancementCategory = Literal[
    "acceptance_criteria",
    "technical_constraints",
    "edge_cases",
    "testing_strategy",
]

ALL_CATEGORIES: tuple[EnhancementCategory, ...] = (
    "acceptance_criteria",
    "technical_constraints",
    "edge_cases",
    "testing_strategy",
)

_CATEGORY_FIELD_MAP: dict[EnhancementCategory, list[str]] = {
    "acceptance_criteria": ["definition_of_done", "scope", "validation_plan"],
    "technical_constraints": ["architecture_notes", "assumptions", "integration_points"],
    "edge_cases": ["risks", "non_goals"],
    "testing_strategy": ["validation_plan", "definition_of_done"],
}


@dataclass(frozen=True)
class EnhancementSuggestion:
    """A single enhancement suggestion for a brief field."""

    category: EnhancementCategory
    field: str
    original_value: str | list[str] | None
    suggested_value: str | list[str]
    rationale: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "category": self.category,
            "field": self.field,
            "original_value": self.original_value,
            "suggested_value": self.suggested_value,
            "rationale": self.rationale,
        }


@dataclass(frozen=True)
class EnhancementResult:
    """Result of a brief enhancement operation."""

    brief_id: str
    gaps_detected: list[str]
    suggestions: list[EnhancementSuggestion] = field(default_factory=list)
    enhanced_brief: dict[str, Any] | None = None
    generation_model: str | None = None
    generation_tokens: int | None = None

    @property
    def has_suggestions(self) -> bool:
        return len(self.suggestions) > 0

    @property
    def categories_covered(self) -> set[EnhancementCategory]:
        return {s.category for s in self.suggestions}

    def diff(self, original_brief: dict[str, Any]) -> dict[str, dict[str, Any]]:
        """Generate a diff view of original vs enhanced brief fields."""
        if self.enhanced_brief is None:
            return {}
        changes: dict[str, dict[str, Any]] = {}
        for suggestion in self.suggestions:
            field_name = suggestion.field
            original = original_brief.get(field_name)
            enhanced = self.enhanced_brief.get(field_name)
            if original != enhanced:
                changes[field_name] = {
                    "original": original,
                    "enhanced": enhanced,
                    "category": suggestion.category,
                    "rationale": suggestion.rationale,
                }
        return changes

    def to_dict(self) -> dict[str, Any]:
        return {
            "brief_id": self.brief_id,
            "gaps_detected": self.gaps_detected,
            "suggestions": [s.to_dict() for s in self.suggestions],
            "has_suggestions": self.has_suggestions,
            "categories_covered": sorted(self.categories_covered),
            "generation_model": self.generation_model,
            "generation_tokens": self.generation_tokens,
        }


@dataclass(frozen=True)
class EnhancementConfig:
    """Configuration for enhancement preferences."""

    categories: tuple[EnhancementCategory, ...] = ALL_CATEGORIES
    max_suggestions: int = 20
    min_field_length: int = 10
    min_list_items: int = 2
    require_rationale: bool = True


class LlmBriefEnhancer:
    """Enhance incomplete implementation briefs using LLM-powered gap analysis."""

    def __init__(
        self,
        llm_client: LLMProvider,
        config: EnhancementConfig | None = None,
    ):
        self.llm = llm_client
        self.config = config or EnhancementConfig()

    def detect_gaps(self, implementation_brief: dict[str, Any]) -> list[str]:
        """Detect completeness gaps in a brief using the readiness audit."""
        audit_result: BriefReadinessResult = audit_brief_readiness(implementation_brief)
        gaps: list[str] = []

        for finding in audit_result.findings:
            gaps.append(f"{finding.field}: {finding.message}")

        # Check for thin content in category-mapped fields
        for category in self.config.categories:
            for field_name in _CATEGORY_FIELD_MAP.get(category, []):
                value = implementation_brief.get(field_name)
                if isinstance(value, str) and len(value.strip()) < self.config.min_field_length:
                    gaps.append(f"{field_name}: Content is too brief to guide implementation")
                elif isinstance(value, list) and len(value) < self.config.min_list_items:
                    gaps.append(
                        f"{field_name}: List has fewer than {self.config.min_list_items} items"
                    )

        return gaps

    def enhance(
        self,
        implementation_brief: dict[str, Any],
        model: str | None = None,
    ) -> EnhancementResult:
        """Enhance a brief by detecting gaps and generating LLM suggestions."""
        brief_id = str(implementation_brief.get("id", ""))
        gaps = self.detect_gaps(implementation_brief)

        if not gaps:
            return EnhancementResult(
                brief_id=brief_id,
                gaps_detected=[],
                suggestions=[],
                enhanced_brief=implementation_brief,
            )

        prompt = self._build_prompt(implementation_brief, gaps)
        response = self.llm.generate(
            prompt=prompt,
            model=model,
            temperature=0.7,
            max_tokens=4096,
            system=self._get_system_prompt(),
        )

        raw_data = parse_json_response(
            response["content"],
            context="brief enhancement",
        )

        suggestions = self._parse_suggestions(raw_data, implementation_brief)
        suggestions = self._validate_suggestions(suggestions, implementation_brief)
        suggestions = suggestions[: self.config.max_suggestions]

        enhanced_brief = self._apply_suggestions(implementation_brief, suggestions)

        return EnhancementResult(
            brief_id=brief_id,
            gaps_detected=gaps,
            suggestions=suggestions,
            enhanced_brief=enhanced_brief,
            generation_model=response["model"],
            generation_tokens=response["usage"]["total_tokens"],
        )

    def _build_prompt(
        self,
        implementation_brief: dict[str, Any],
        gaps: list[str],
    ) -> str:
        brief_json = json.dumps(implementation_brief, indent=2, sort_keys=True, default=str)
        gaps_text = "\n".join(f"- {gap}" for gap in gaps)
        categories_text = ", ".join(self.config.categories)

        return f"""# Implementation Brief to Enhance
{brief_json}

# Detected Gaps
{gaps_text}

# Enhancement Categories
Focus on these categories: {categories_text}

---

Analyze the implementation brief above and generate enhancement suggestions for the
detected gaps. For each suggestion, specify which field to update, what the enhanced
value should be, and why the enhancement improves the brief.

Your output must be valid JSON matching this exact schema:

{{
  "suggestions": [
    {{
      "category": "acceptance_criteria | technical_constraints | edge_cases | testing_strategy",
      "field": "the brief field name to enhance (e.g. definition_of_done, risks, validation_plan)",
      "suggested_value": "enhanced string value or list of strings",
      "rationale": "why this enhancement improves the brief"
    }}
  ]
}}

Requirements:
- Each suggestion must target a specific brief field
- suggested_value must match the field's type (string for text fields, list for list fields)
- For list fields, include all original items plus new additions
- Rationale must explain the concrete improvement
- Focus on actionable, specific content — avoid generic placeholders
- Do not contradict existing brief content
- Output ONLY the JSON, no additional text."""

    def _get_system_prompt(self) -> str:
        return (
            "You are a technical product architect reviewing implementation briefs "
            "for completeness gaps. Your role is to suggest specific, actionable "
            "enhancements that improve brief quality and reduce implementation "
            "ambiguity. Focus on practical additions that an engineering team would "
            "need. Output ONLY valid JSON with no additional commentary."
        )

    def _parse_suggestions(
        self,
        raw_data: dict[str, Any],
        implementation_brief: dict[str, Any],
    ) -> list[EnhancementSuggestion]:
        raw_suggestions = raw_data.get("suggestions", [])
        if not isinstance(raw_suggestions, list):
            return []

        suggestions: list[EnhancementSuggestion] = []
        for raw in raw_suggestions:
            if not isinstance(raw, dict):
                continue

            category = raw.get("category")
            if category not in ALL_CATEGORIES:
                continue

            field_name = raw.get("field", "")
            if not isinstance(field_name, str) or not field_name:
                continue

            suggested_value = raw.get("suggested_value")
            if suggested_value is None:
                continue

            rationale = raw.get("rationale", "")
            if self.config.require_rationale and not rationale:
                continue

            original_value = implementation_brief.get(field_name)

            suggestions.append(
                EnhancementSuggestion(
                    category=category,
                    field=field_name,
                    original_value=original_value,
                    suggested_value=suggested_value,
                    rationale=rationale,
                )
            )

        return suggestions

    def _validate_suggestions(
        self,
        suggestions: list[EnhancementSuggestion],
        implementation_brief: dict[str, Any],
    ) -> list[EnhancementSuggestion]:
        """Filter suggestions that don't meet validation criteria."""
        _BRIEF_FIELDS = {
            "title",
            "domain",
            "target_user",
            "buyer",
            "workflow_context",
            "problem_statement",
            "mvp_goal",
            "product_surface",
            "scope",
            "non_goals",
            "assumptions",
            "architecture_notes",
            "data_requirements",
            "integration_points",
            "risks",
            "validation_plan",
            "definition_of_done",
        }

        _LIST_FIELDS = {
            "scope",
            "non_goals",
            "assumptions",
            "integration_points",
            "risks",
            "definition_of_done",
        }

        valid: list[EnhancementSuggestion] = []
        seen_fields: set[str] = set()

        for suggestion in suggestions:
            # Field must be a known brief field
            if suggestion.field not in _BRIEF_FIELDS:
                continue

            # Only keep first suggestion per field to avoid conflicts
            if suggestion.field in seen_fields:
                continue

            # Type check: list fields should get lists, string fields should get strings
            if suggestion.field in _LIST_FIELDS:
                if not isinstance(suggestion.suggested_value, list):
                    continue
            else:
                if not isinstance(suggestion.suggested_value, str):
                    continue

            # Suggested value must differ from original
            if suggestion.suggested_value == suggestion.original_value:
                continue

            seen_fields.add(suggestion.field)
            valid.append(suggestion)

        return valid

    @staticmethod
    def _apply_suggestions(
        implementation_brief: dict[str, Any],
        suggestions: list[EnhancementSuggestion],
    ) -> dict[str, Any]:
        """Apply validated suggestions to produce an enhanced brief."""
        enhanced = dict(implementation_brief)
        for suggestion in suggestions:
            enhanced[suggestion.field] = suggestion.suggested_value
        return enhanced


__all__ = [
    "ALL_CATEGORIES",
    "EnhancementCategory",
    "EnhancementConfig",
    "EnhancementResult",
    "EnhancementSuggestion",
    "LlmBriefEnhancer",
]
