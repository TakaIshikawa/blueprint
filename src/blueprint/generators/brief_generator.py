"""LLM-based implementation brief generator."""

import json
import uuid
from typing import Any

from blueprint.llm.client import LLMClient


def generate_implementation_brief_id() -> str:
    """Generate a unique implementation brief ID."""
    return f"ib-{uuid.uuid4().hex[:12]}"


class BriefGenerator:
    """Generate implementation briefs from source briefs using LLM reasoning."""

    def __init__(self, llm_client: LLMClient):
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
        prompt = self._build_prompt(source_brief)

        # Generate using LLM
        response = self.llm.generate(
            prompt=prompt,
            model=model,
            temperature=1.0,
            max_tokens=4096,
            system=self._get_system_prompt(),
        )

        # Parse JSON response with multiple fallback strategies
        content = response["content"]
        brief_data = None
        parse_error = None

        # Strategy 1: Direct JSON parse
        try:
            brief_data = json.loads(content)
        except json.JSONDecodeError as e:
            parse_error = e

        # Strategy 2: Extract from markdown code blocks
        if brief_data is None and "```json" in content:
            try:
                start = content.find("```json") + 7
                end = content.find("```", start)
                json_str = content[start:end].strip()
                brief_data = json.loads(json_str)
            except json.JSONDecodeError as e:
                parse_error = e

        # Strategy 3: Extract from code blocks without language tag
        if brief_data is None and "```" in content:
            try:
                start = content.find("```") + 3
                end = content.find("```", start)
                json_str = content[start:end].strip()
                # Remove potential language tag
                if json_str.startswith("json\n"):
                    json_str = json_str[5:]
                brief_data = json.loads(json_str)
            except json.JSONDecodeError as e:
                parse_error = e

        # Strategy 4: Find JSON object boundaries
        if brief_data is None:
            try:
                # Find first { and last }
                start_idx = content.find("{")
                end_idx = content.rfind("}")
                if start_idx != -1 and end_idx != -1:
                    json_str = content[start_idx:end_idx+1]
                    brief_data = json.loads(json_str)
            except json.JSONDecodeError as e:
                parse_error = e

        if brief_data is None:
            # Save response to file for debugging
            import tempfile
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
                f.write(content)
                debug_file = f.name
            raise ValueError(
                f"Failed to parse LLM response as JSON after trying multiple strategies.\n"
                f"Last error: {parse_error}\n"
                f"Response saved to: {debug_file}\n"
                f"Please check the response and try again with a different model or regenerate."
            )

        # Build implementation brief dict
        implementation_brief = {
            "id": generate_implementation_brief_id(),
            "source_brief_id": source_brief["id"],
            "title": brief_data["title"],
            "domain": source_brief.get("domain"),
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
            "generation_prompt": prompt[:1000],  # Store first 1000 chars for debugging
        }

        return implementation_brief

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

    def _build_prompt(self, source_brief: dict[str, Any]) -> str:
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
