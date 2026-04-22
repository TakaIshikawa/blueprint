"""LLM-based execution plan generator."""

import json
import uuid
from typing import Any

from blueprint.llm.provider import LLMProvider


def generate_execution_plan_id() -> str:
    """Generate a unique execution plan ID."""
    return f"plan-{uuid.uuid4().hex[:12]}"


def generate_task_id() -> str:
    """Generate a unique task ID."""
    return f"task-{uuid.uuid4().hex[:12]}"


class PlanGenerator:
    """Generate execution plans from implementation briefs using LLM reasoning."""

    def __init__(self, llm_client: LLMProvider):
        """
        Initialize plan generator.

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
        Generate an execution plan from an implementation brief.

        Args:
            implementation_brief: Implementation brief dictionary from database
            model: Optional model override

        Returns:
            Tuple of (plan_dict, tasks_list) ready for insertion into database
        """
        # Build prompt from implementation brief
        prompt = self.build_prompt(implementation_brief)

        # Generate using LLM
        response = self.llm.generate(
            prompt=prompt,
            model=model,
            temperature=1.0,
            max_tokens=6000,
            system=self._get_system_prompt(),
        )

        # Parse JSON response with multiple fallback strategies
        content = response["content"]
        plan_data = None
        parse_error = None

        # Strategy 1: Direct JSON parse
        try:
            plan_data = json.loads(content)
        except json.JSONDecodeError as e:
            parse_error = e

        # Strategy 2: Extract from markdown code blocks
        if plan_data is None and "```json" in content:
            try:
                start = content.find("```json") + 7
                end = content.find("```", start)
                json_str = content[start:end].strip()
                plan_data = json.loads(json_str)
            except json.JSONDecodeError as e:
                parse_error = e

        # Strategy 3: Extract from code blocks without language tag
        if plan_data is None and "```" in content:
            try:
                start = content.find("```") + 3
                end = content.find("```", start)
                json_str = content[start:end].strip()
                # Remove potential language tag
                if json_str.startswith("json\n"):
                    json_str = json_str[5:]
                plan_data = json.loads(json_str)
            except json.JSONDecodeError as e:
                parse_error = e

        # Strategy 4: Find JSON object boundaries
        if plan_data is None:
            try:
                # Find first { and last }
                start_idx = content.find("{")
                end_idx = content.rfind("}")
                if start_idx != -1 and end_idx != -1:
                    json_str = content[start_idx:end_idx+1]
                    plan_data = json.loads(json_str)
            except json.JSONDecodeError as e:
                parse_error = e

        if plan_data is None:
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

        # Generate task IDs and map dependencies
        task_id_map = {}  # milestone:task_index -> task_id
        tasks = []

        for milestone in plan_data["milestones"]:
            milestone_name = milestone["name"]
            for task_idx, task_data in enumerate(milestone["tasks"]):
                task_id = generate_task_id()
                task_key = f"{milestone_name}:{task_idx}"
                task_id_map[task_key] = task_id

                # Resolve dependencies to task IDs
                depends_on = []
                for dep in task_data.get("depends_on", []):
                    if dep in task_id_map:
                        depends_on.append(task_id_map[dep])

                task = {
                    "id": task_id,
                    "title": task_data["title"],
                    "description": task_data["description"],
                    "milestone": milestone_name,
                    "owner_type": task_data.get("owner_type"),
                    "suggested_engine": task_data.get("suggested_engine"),
                    "depends_on": depends_on,
                    "files_or_modules": task_data.get("files_or_modules", []),
                    "acceptance_criteria": task_data["acceptance_criteria"],
                    "estimated_complexity": task_data.get("estimated_complexity"),
                    "status": "pending",
                }
                tasks.append(task)

        # Build execution plan dict
        execution_plan = {
            "id": generate_execution_plan_id(),
            "implementation_brief_id": implementation_brief["id"],
            "target_engine": plan_data.get("target_engine"),
            "target_repo": plan_data.get("target_repo"),
            "project_type": plan_data.get("project_type"),
            "milestones": plan_data["milestones"],
            "test_strategy": plan_data.get("test_strategy"),
            "handoff_prompt": plan_data.get("handoff_prompt"),
            "status": "draft",
            "generation_model": response["model"],
            "generation_tokens": response["usage"]["total_tokens"],
            "generation_prompt": prompt[:1000],
        }

        return execution_plan, tasks

    def _get_system_prompt(self) -> str:
        """Get system prompt for plan generation."""
        return """You are a technical project planner converting implementation
briefs into detailed, actionable execution plans. Your job is to break down work
into milestones and tasks that coding agents or engineering teams can execute.

You will receive an implementation brief with defined scope, architecture, and
validation plans. Your task is to:

1. Break work into 3-5 sequential milestones
2. Define tasks within each milestone with clear acceptance criteria
3. Identify task dependencies
4. Suggest appropriate execution engines per task
5. Define a test strategy and handoff context

Output ONLY valid JSON with no additional commentary. Use the exact schema provided in the user prompt."""

    @staticmethod
    def build_prompt(brief: dict[str, Any]) -> str:
        """Build generation prompt from implementation brief."""
        # Extract key fields from brief
        title = brief["title"]
        problem = brief["problem_statement"]
        mvp_goal = brief["mvp_goal"]
        scope = brief["scope"]
        non_goals = brief["non_goals"]
        architecture = brief.get("architecture_notes", "Not specified")
        product_surface = brief.get("product_surface", "Not specified")
        validation = brief["validation_plan"]
        dod = brief["definition_of_done"]

        # Format scope items
        scope_text = "\n".join(f"  - {item}" for item in scope)
        non_goals_text = "\n".join(f"  - {item}" for item in non_goals)
        dod_text = "\n".join(f"  - {item}" for item in dod)

        # Build context
        context = f"""# Implementation Brief: {title}

## Problem Statement
{problem}

## MVP Goal
{mvp_goal}

## Product Surface
{product_surface}

## In Scope
{scope_text}

## Non-Goals
{non_goals_text}

## Architecture Notes
{architecture}

## Validation Plan
{validation}

## Definition of Done
{dod_text}"""

        # Build full prompt
        prompt = f"""{context}

---

Based on the implementation brief above, create a **detailed execution plan** that
breaks this project into milestones and tasks.

Your output must be valid JSON matching this exact schema:

{{
  "target_engine": "relay | smoothie | codex | claude_code | manual (or null if mixed)",
  "target_repo": "Suggested repo name or path",
  "project_type": "python_library | mcp_server | cli_tool | web_app | integration | etc.",
  "milestones": [
    {{
      "name": "Milestone 1: Foundation",
      "description": "What gets built in this phase",
      "tasks": [
        {{
          "title": "Task title (actionable, specific)",
          "description": "What needs to be done, how to approach it",
          "owner_type": "agent | human | either",
          "suggested_engine": "relay | smoothie | codex | claude_code | manual",
          "depends_on": ["Milestone 1:0"],
          "files_or_modules": ["path/to/file.py", "module_name"],
          "acceptance_criteria": [
            "Specific, testable criterion 1",
            "Specific, testable criterion 2"
          ],
          "estimated_complexity": "low | medium | high"
        }}
      ]
    }},
    {{
      "name": "Milestone 2: Core Features",
      "description": "What gets built in this phase",
      "tasks": [...]
    }}
  ],
  "test_strategy": "How we'll test this (unit tests, integration tests, manual validation, etc.)",
  "handoff_prompt": "Context for execution engine: what they need to know, key constraints, gotchas"
}}

Requirements for tasks:
- Tasks should be small (completable in 30 min - 4 hours for agents, 1-2 days for humans)
- Each task should have 2-4 acceptance criteria
- Dependencies use "Milestone Name:task_index" format (e.g., "Milestone 1:0" for first task)
- files_or_modules helps execution engines know where to work
- suggested_engine should match task nature:
  - "codex" or "claude_code" for implementation tasks
  - "smoothie" for UI/prototype tasks
  - "relay" for autonomous multi-step workflows
  - "manual" for tasks requiring human judgment

Requirements for milestones:
- 3-5 milestones total
- Milestones should be sequential (complete one before next)
- Each milestone should have 3-8 tasks
- Milestone 1 should establish foundation (project setup, core models, basic infra)
- Final milestone should include testing, documentation, deployment prep

Output ONLY the JSON, no additional text."""

        return prompt

    def _build_prompt(self, brief: dict[str, Any]) -> str:
        """Build generation prompt from implementation brief."""
        return self.build_prompt(brief)
