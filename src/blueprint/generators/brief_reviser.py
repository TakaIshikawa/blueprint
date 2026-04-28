"""LLM-based implementation brief revision generator."""

import json
from typing import Any

from blueprint.generators.brief_generator import generate_implementation_brief_id
from blueprint.llm.json_parser import parse_json_response
from blueprint.llm.provider import LLMProvider


class BriefReviser:
    """Generate revised implementation briefs from an existing brief and feedback."""

    def __init__(self, llm_client: LLMProvider):
        """Initialize brief reviser with an LLM client."""
        self.llm = llm_client

    def generate(
        self,
        existing_brief: dict[str, Any],
        source_brief: dict[str, Any],
        feedback: str,
        model: str | None = None,
    ) -> dict[str, Any]:
        """
        Generate a revised implementation brief.

        The returned brief is a new record with a fresh ID. The original
        implementation brief is included in the prompt and remains unchanged.
        """
        prompt = self.build_prompt(existing_brief, source_brief, feedback)
        response = self.llm.generate(
            prompt=prompt,
            model=model,
            temperature=1.0,
            max_tokens=4096,
            system=self._get_system_prompt(),
        )

        brief_data = parse_json_response(
            response["content"],
            context="implementation brief revision",
        )

        return {
            "id": generate_implementation_brief_id(),
            "source_brief_id": existing_brief["source_brief_id"],
            "title": brief_data["title"],
            "domain": existing_brief.get("domain") or source_brief.get("domain"),
            "target_user": brief_data.get("target_user"),
            "buyer": brief_data.get("buyer"),
            "workflow_context": brief_data.get("workflow_context"),
            "problem_statement": brief_data["problem_statement"],
            "mvp_goal": brief_data["mvp_goal"],
            "product_surface": brief_data.get("product_surface"),
            "scope": brief_data["scope"],
            "non_goals": brief_data["non_goals"],
            "assumptions": brief_data["assumptions"],
            "architecture_notes": brief_data.get("architecture_notes"),
            "data_requirements": brief_data.get("data_requirements"),
            "integration_points": brief_data.get("integration_points", []),
            "risks": brief_data["risks"],
            "validation_plan": brief_data["validation_plan"],
            "definition_of_done": brief_data["definition_of_done"],
            "status": "draft",
            "generation_model": response["model"],
            "generation_tokens": response["usage"]["total_tokens"],
            "generation_prompt": prompt,
        }

    def _get_system_prompt(self) -> str:
        """Get system prompt for brief revision."""
        return """You are a technical product architect revising implementation-ready
specifications. You will receive the original source context, an existing
implementation brief, and human feedback. Produce a complete replacement
implementation brief that incorporates the feedback while preserving still-valid
constraints from the source context and existing brief.

Output ONLY valid JSON with no additional commentary. Use the exact schema
provided in the user prompt."""

    @staticmethod
    def build_prompt(
        existing_brief: dict[str, Any],
        source_brief: dict[str, Any],
        feedback: str,
    ) -> str:
        """Build revision prompt from source context, existing brief, and feedback."""
        source_json = json.dumps(source_brief, indent=2, sort_keys=True, default=str)
        brief_json = json.dumps(existing_brief, indent=2, sort_keys=True, default=str)

        return f"""# Original Source Brief Context
{source_json}

# Existing Implementation Brief
{brief_json}

# Human Feedback
{feedback}

---

Create a revised implementation-ready specification that incorporates the
feedback. The revised brief must be complete on its own; do not output a patch
or diff.

Your output must be valid JSON matching this exact schema:

{{
  "title": "Clear, actionable project title (50-80 chars)",
  "target_user": "Who will use this (persona description)",
  "buyer": "Who pays for this (person or role)",
  "workflow_context": "Where this fits in their workflow",
  "problem_statement": "1-2 paragraphs: What problem are we solving and why",
  "mvp_goal": "1-2 paragraphs: What we're building for the MVP, success criteria",
  "product_surface": "CLI | library | MCP server | web app | API | etc.",
  "scope": [
    "Specific feature 1 in scope",
    "Specific feature 2 in scope",
    "Specific feature 3 in scope"
  ],
  "non_goals": [
    "What we're explicitly NOT building in MVP",
    "Features deferred to later phases",
    "Edge cases we're not handling yet"
  ],
  "assumptions": [
    "Technical assumption 1",
    "User assumption 2",
    "Infrastructure assumption 3"
  ],
  "architecture_notes": "2-3 paragraphs: High-level architecture, key components, data flow, tech stack considerations",
  "data_requirements": "What data this needs to store, process, or integrate with",
  "integration_points": [
    "System or API we need to integrate with",
    "Another integration point"
  ],
  "risks": [
    "Technical risk 1 and mitigation",
    "Product risk 2 and mitigation",
    "Execution risk 3 and mitigation"
  ],
  "validation_plan": "How we'll know this works (testing strategy, success metrics, validation approach)",
  "definition_of_done": [
    "Specific completion criterion 1",
    "Specific completion criterion 2",
    "Specific completion criterion 3"
  ]
}}

Requirements:
- Incorporate the human feedback directly into scope, non-goals, assumptions, risks, validation, and definition of done where relevant.
- Preserve still-valid details and constraints from the original source brief context.
- Preserve still-valid implementation decisions from the existing implementation brief.
- MVP scope should remain achievable in 1-2 weeks unless the feedback explicitly changes scope.
- Output ONLY the JSON, no additional text."""

    def _build_prompt(
        self,
        existing_brief: dict[str, Any],
        source_brief: dict[str, Any],
        feedback: str,
    ) -> str:
        """Build revision prompt from source context, existing brief, and feedback."""
        return self.build_prompt(existing_brief, source_brief, feedback)
