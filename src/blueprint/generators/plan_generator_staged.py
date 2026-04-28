"""Staged LLM-based execution plan generator (fixes JSON parsing issues)."""

import uuid
from typing import Any

from blueprint.generators.json_repair import (
    MilestoneOutlineResponse,
    MilestoneTasksResponse,
    PlanMetadataResponse,
    parse_and_validate_llm_json,
    validate_execution_plan_payload,
)
from blueprint.llm.provider import LLMProvider


def generate_execution_plan_id() -> str:
    """Generate a unique execution plan ID."""
    return f"plan-{uuid.uuid4().hex[:12]}"


def generate_task_id() -> str:
    """Generate a unique task ID."""
    return f"task-{uuid.uuid4().hex[:12]}"


class StagedPlanGenerator:
    """
    Generate execution plans using staged approach to avoid complex JSON.

    Stage 1: Generate milestones (simple list)
    Stage 2: For each milestone, generate tasks separately
    Stage 3: Combine into full plan

    This avoids the nested JSON structure that causes parsing issues.
    """

    def __init__(self, llm_client: LLMProvider):
        """
        Initialize staged plan generator.

        Args:
            llm_client: LLM client for generation
        """
        self.llm = llm_client

    def generate(
        self,
        implementation_brief: dict[str, Any],
        model: str | None = None,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        """
        Generate an execution plan using staged approach.

        Args:
            implementation_brief: Implementation brief dictionary from database
            model: Optional model override

        Returns:
            Tuple of (plan_dict, tasks_list) ready for insertion into database
        """
        # Stage 1: Generate milestones
        milestones_data = self._generate_milestones(implementation_brief, model)

        # Stage 2: Generate tasks for each milestone
        all_tasks = []
        total_tokens = milestones_data["tokens_used"]

        for milestone in milestones_data["milestones"]:
            tasks_data = self._generate_milestone_tasks(
                implementation_brief,
                milestone,
                model,
            )
            all_tasks.extend(tasks_data["tasks"])
            total_tokens += tasks_data["tokens_used"]

        # Stage 3: Generate plan metadata
        plan_metadata = self._generate_plan_metadata(implementation_brief, model)
        total_tokens += plan_metadata["tokens_used"]

        # Build execution plan dict
        execution_plan = {
            "id": generate_execution_plan_id(),
            "implementation_brief_id": implementation_brief["id"],
            "target_engine": plan_metadata.get("target_engine"),
            "target_repo": plan_metadata.get("target_repo"),
            "project_type": plan_metadata.get("project_type"),
            "milestones": milestones_data["milestones"],
            "test_strategy": plan_metadata.get("test_strategy"),
            "handoff_prompt": plan_metadata.get("handoff_prompt"),
            "status": "draft",
            "generation_model": model or self.llm.default_model,
            "generation_tokens": total_tokens,
            "generation_prompt": "Staged generation (milestones + tasks separately)",
        }

        validate_execution_plan_payload(execution_plan, all_tasks)

        return execution_plan, all_tasks

    def _generate_milestones(
        self,
        brief: dict[str, Any],
        model: str | None,
    ) -> dict[str, Any]:
        """Generate milestones (Stage 1)."""
        prompt = self.build_milestones_prompt(brief)

        response = self.llm.generate(
            prompt=prompt,
            model=model,
            temperature=1.0,
            max_tokens=2000,
        )

        milestones_json = parse_and_validate_llm_json(
            response["content"],
            MilestoneOutlineResponse,
            context="staged milestone generation",
        )

        return {
            "milestones": milestones_json["milestones"],
            "tokens_used": response["usage"]["total_tokens"],
        }

    @staticmethod
    def build_milestones_prompt(brief: dict[str, Any]) -> str:
        """Build the staged generation prompt for milestone outlining."""
        return f"""# Implementation Brief: {brief['title']}

## Problem
{brief['problem_statement']}

## MVP Goal
{brief['mvp_goal']}

## Scope
{chr(10).join(f"- {item}" for item in brief.get('scope', []))}

---

Based on this implementation brief, create 3-5 **sequential milestones** for the project.

Output ONLY valid JSON matching this schema:

{{
  "milestones": [
    {{
      "name": "Milestone 1: Foundation",
      "description": "What gets built in this phase"
    }},
    {{
      "name": "Milestone 2: Core Features",
      "description": "What gets built in this phase"
    }},
    {{
      "name": "Milestone 3: Polish & Ship",
      "description": "What gets built in this phase"
    }}
  ]
}}

Requirements:
- 3-5 milestones total
- Milestones should be sequential (complete one before next)
- Milestone 1 should establish foundation
- Final milestone should include testing and docs

Output ONLY the JSON, no additional text."""

    @staticmethod
    def build_prompt(brief: dict[str, Any]) -> str:
        """Build the first LLM prompt used by staged plan generation."""
        return StagedPlanGenerator.build_milestones_prompt(brief)

    def _build_prompt(self, brief: dict[str, Any]) -> str:
        """Build the first LLM prompt used by staged plan generation."""
        return self.build_prompt(brief)

    def _generate_milestone_tasks(
        self,
        brief: dict[str, Any],
        milestone: dict[str, Any],
        model: str | None,
    ) -> dict[str, Any]:
        """Generate tasks for a specific milestone (Stage 2)."""
        prompt = self.build_milestone_tasks_prompt(brief, milestone)

        response = self.llm.generate(
            prompt=prompt,
            model=model,
            temperature=1.0,
            max_tokens=3000,
        )

        tasks_json = parse_and_validate_llm_json(
            response["content"],
            MilestoneTasksResponse,
            context=f"staged task generation for {milestone['name']}",
        )

        # Add task metadata
        tasks_with_metadata = []
        for task_data in tasks_json["tasks"]:
            task = {
                "id": generate_task_id(),
                "title": task_data["title"],
                "description": task_data["description"],
                "milestone": milestone["name"],
                "owner_type": "agent",
                "suggested_engine": task_data.get("suggested_engine", "claude_code"),
                "depends_on": [],  # Dependencies added in Stage 3 if needed
                "files_or_modules": task_data.get("files_or_modules", []),
                "acceptance_criteria": task_data.get("acceptance_criteria", []),
                "estimated_complexity": task_data.get("estimated_complexity", "medium"),
                "status": "pending",
            }
            tasks_with_metadata.append(task)

        return {
            "tasks": tasks_with_metadata,
            "tokens_used": response["usage"]["total_tokens"],
        }

    @staticmethod
    def build_milestone_tasks_prompt(
        brief: dict[str, Any],
        milestone: dict[str, Any],
    ) -> str:
        """Build the staged generation prompt for one milestone's tasks."""
        architecture_notes = brief.get("architecture_notes") or "See brief"
        return f"""# Implementation Brief: {brief['title']}

## This Milestone: {milestone['name']}
{milestone['description']}

## Context
Product Surface: {brief.get('product_surface', 'TBD')}
Architecture: {architecture_notes[:200]}...

---

Create 3-6 **specific tasks** for this milestone.

Output ONLY valid JSON matching this schema:

{{
  "tasks": [
    {{
      "title": "Task title (actionable, specific)",
      "description": "What needs to be done and how to approach it",
      "files_or_modules": ["path/to/file.py", "module_name"],
      "acceptance_criteria": [
        "Specific, testable criterion 1",
        "Specific, testable criterion 2"
      ],
      "estimated_complexity": "low | medium | high",
      "suggested_engine": "claude_code | relay | manual"
    }}
  ]
}}

Requirements:
- 3-6 tasks for this milestone
- Each task should be completable in 30min - 4 hours
- Files should match the project (check product surface)
- Acceptance criteria should be measurable
- No dependencies needed (we'll add those later)

Output ONLY the JSON, no additional text."""

    def _generate_plan_metadata(
        self,
        brief: dict[str, Any],
        model: str | None,
    ) -> dict[str, Any]:
        """Generate plan metadata (Stage 3)."""
        prompt = self.build_plan_metadata_prompt(brief)

        response = self.llm.generate(
            prompt=prompt,
            model=model,
            temperature=1.0,
            max_tokens=1000,
        )

        metadata = parse_and_validate_llm_json(
            response["content"],
            PlanMetadataResponse,
            context="staged plan metadata generation",
        )

        return {
            **metadata,
            "tokens_used": response["usage"]["total_tokens"],
        }

    @staticmethod
    def build_plan_metadata_prompt(brief: dict[str, Any]) -> str:
        """Build the staged generation prompt for execution plan metadata."""
        return f"""# Implementation Brief: {brief['title']}

## Product Surface
{brief.get('product_surface', 'TBD')}

## Architecture
{brief.get('architecture_notes', 'See brief')}

---

Provide metadata for this execution plan.

Output ONLY valid JSON matching this schema:

{{
  "target_engine": "claude_code | relay | smoothie | codex | null",
  "target_repo": "Suggested repo name",
  "project_type": "python_library | cli_tool | web_app | mcp_server | etc",
  "test_strategy": "How we'll test (pytest, integration tests, etc)",
  "handoff_prompt": "Brief context for execution engine (2-3 sentences)"
}}

Output ONLY the JSON, no additional text."""
