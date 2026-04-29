"""LLM-based implementation brief generator."""

import re
import uuid
from typing import Any

from pydantic import ValidationError

from blueprint.domain import ImplementationBrief
from blueprint.llm.json_parser import parse_json_response
from blueprint.llm.provider import LLMProvider


def generate_implementation_brief_id() -> str:
    """Generate a unique implementation brief ID."""
    return f"ib-{uuid.uuid4().hex[:12]}"


class BriefGenerator:
    """Generate implementation briefs from source briefs using LLM reasoning."""

    def __init__(self, llm_client: LLMProvider):
        """
        Initialize brief generator.

        Args:
            llm_client: LLM client for generation
        """
        self.llm = llm_client

    def generate(
        self,
        source_brief: dict[str, Any],
        model: str | None = None,
    ) -> dict[str, Any]:
        """
        Generate an implementation brief from a source brief.

        Args:
            source_brief: Source brief dictionary from database
            model: Optional model override

        Returns:
            Dictionary ready for insertion into implementation_briefs table
        """
        # Build prompt from source brief
        prompt = self.build_prompt(source_brief)

        # Generate using LLM
        response = self.llm.generate(
            prompt=prompt,
            model=model,
            temperature=1.0,
            max_tokens=4096,
            system=self._get_system_prompt(),
        )

        brief_data = parse_json_response(
            response["content"],
            context="implementation brief generation",
        )

        implementation_brief = self._validate_generated_brief(
            self._build_implementation_brief_payload(
                source_brief=source_brief,
                brief_data=brief_data,
                response=response,
                prompt=prompt,
            ),
            raw_content=response["content"],
        )

        return implementation_brief

    @staticmethod
    def _build_implementation_brief_payload(
        *,
        source_brief: dict[str, Any],
        brief_data: dict[str, Any],
        response: dict[str, Any],
        prompt: str,
    ) -> dict[str, Any]:
        """Build a candidate ImplementationBrief payload from parsed LLM JSON."""
        implementation_brief = {
            "id": generate_implementation_brief_id(),
            "source_brief_id": source_brief["id"],
            "domain": source_brief.get("domain"),
            "status": "draft",
            "generation_model": response["model"],
            "generation_tokens": response["usage"]["total_tokens"],
            "generation_prompt": prompt[:1000],  # Store first 1000 chars for debugging
        }

        generated_fields = (
            "title",
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
            "risks",
            "validation_plan",
            "definition_of_done",
        )
        for field_name in generated_fields:
            if field_name in brief_data:
                implementation_brief[field_name] = brief_data[field_name]

        if brief_data.get("integration_points") is not None:
            implementation_brief["integration_points"] = brief_data["integration_points"]

        return implementation_brief

    def _validate_generated_brief(
        self,
        implementation_brief: dict[str, Any],
        *,
        raw_content: str,
    ) -> dict[str, Any]:
        """Validate and normalize an LLM-generated implementation brief."""
        try:
            return ImplementationBrief.model_validate(implementation_brief).model_dump(
                mode="python"
            )
        except ValidationError as exc:
            preview = self._invalid_output_preview(raw_content)
            raise ValueError(
                "Generated implementation brief failed domain validation.\n"
                f"{self._format_validation_errors(exc)}\n"
                f"LLM output preview: {preview}"
            ) from exc

    @staticmethod
    def _format_validation_errors(error: ValidationError) -> str:
        """Format Pydantic validation errors for generator callers."""
        messages = []
        for detail in error.errors(include_url=False):
            location = ".".join(str(part) for part in detail["loc"])
            messages.append(f"- {location}: {detail['msg']}")
        return "Validation errors:\n" + "\n".join(messages)

    @staticmethod
    def _invalid_output_preview(content: str, *, limit: int = 240) -> str:
        """Return a short, single-line preview of invalid LLM output."""
        sanitized = re.sub(r"[\x00-\x1f\x7f]+", " ", content)
        sanitized = re.sub(r"\s+", " ", sanitized).strip()
        if len(sanitized) > limit:
            sanitized = sanitized[: limit - 3].rstrip() + "..."
        return sanitized

    def _get_system_prompt(self) -> str:
        """Get system prompt for brief generation."""
        return """You are a technical product architect converting design briefs into
implementation-ready specifications. Your job is to reason about scope, architecture,
and execution strategy.

You will receive a design brief from various sources (Max, Graph, manual input).
Your task is to:

1. Define the MVP scope clearly (what's in, what's out)
2. Identify assumptions and constraints
3. Note architecture considerations
4. Assess risks and mitigation strategies
5. Define validation plan and definition of done

Output ONLY valid JSON with no additional commentary. Use the exact schema provided in the user prompt."""

    @staticmethod
    def build_prompt(source_brief: dict[str, Any]) -> str:
        """Build generation prompt from source brief."""
        # Extract context from source brief
        title = source_brief["title"]
        summary = source_brief["summary"]
        domain = source_brief.get("domain", "N/A")
        source_payload = source_brief.get("source_payload", {})

        # Build context section based on source type
        context_parts = [
            f"# Design Brief: {title}",
            f"\n## Domain: {domain}",
            f"\n## Summary\n{summary}",
        ]

        # Add Max-specific context if available
        if source_brief["source_project"] == "max":
            design_brief = source_payload.get("design_brief", {})
            source_ideas = source_payload.get("source_ideas", [])

            if design_brief.get("merged_product_concept"):
                context_parts.append(
                    f"\n## Product Concept\n{design_brief['merged_product_concept']}"
                )

            if design_brief.get("why_this_now"):
                context_parts.append(f"\n## Market Timing\n{design_brief['why_this_now']}")

            if design_brief.get("mvp_scope"):
                context_parts.append(
                    f"\n## Suggested MVP Scope (from Max)\n"
                    + "\n".join(f"- {item}" for item in design_brief["mvp_scope"])
                )

            if design_brief.get("risks"):
                context_parts.append(
                    f"\n## Known Risks (from Max)\n"
                    + "\n".join(f"- {risk}" for risk in design_brief["risks"])
                )

            if design_brief.get("buyer"):
                context_parts.append(f"\n## Buyer\n{design_brief['buyer']}")

            if design_brief.get("specific_user"):
                context_parts.append(f"\n## Target User\n{design_brief['specific_user']}")

            if design_brief.get("workflow_context"):
                context_parts.append(f"\n## Workflow Context\n{design_brief['workflow_context']}")

            if source_ideas:
                lead_ideas = [idea for idea in source_ideas if idea["role"] == "lead"]
                if lead_ideas:
                    lead = lead_ideas[0]
                    context_parts.append(
                        f"\n## Lead Idea: {lead['title']}\n"
                        f"Problem: {lead['problem']}\n"
                        f"Solution: {lead['solution']}\n"
                        f"Tech Approach: {lead.get('tech_approach', 'Not specified')}"
                    )

        context = "\n".join(context_parts)

        # Build full prompt
        prompt = f"""{context}

---

Based on the design brief above, create an **implementation-ready specification** that
a coding agent or engineering team can execute on.

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
- Be specific and actionable
- MVP scope should be achievable in 1-2 weeks
- Non-goals should be explicit about what we're NOT doing
- Architecture notes should guide implementation without over-specifying
- Risks should include mitigation strategies
- Definition of done should be measurable

Output ONLY the JSON, no additional text."""

        return prompt

    def _build_prompt(self, source_brief: dict[str, Any]) -> str:
        """Build generation prompt from source brief."""
        return self.build_prompt(source_brief)
