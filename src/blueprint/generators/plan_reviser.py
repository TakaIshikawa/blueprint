"""LLM-based execution plan revision generator."""

import json
import tempfile
from datetime import datetime
from typing import Any

from blueprint.generators.plan_generator import (
    generate_execution_plan_id,
    generate_task_id,
)
from blueprint.llm.provider import LLMProvider


class PlanReviser:
    """Generate revised execution plans from an existing plan and feedback."""

    def __init__(self, llm_client: LLMProvider):
        """Initialize plan reviser with an LLM client."""
        self.llm = llm_client

    def generate(
        self,
        implementation_brief: dict[str, Any],
        existing_plan: dict[str, Any],
        feedback: str,
        model: str | None = None,
        feedback_source: str | None = None,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        """
        Generate a revised execution plan.

        The returned plan and tasks are new records with fresh IDs. Lineage is
        stored in the plan metadata so the original plan remains unchanged.
        """
        prompt = self._build_prompt(implementation_brief, existing_plan, feedback)
        response = self.llm.generate(
            prompt=prompt,
            model=model,
            temperature=1.0,
            max_tokens=6000,
            system=self._get_system_prompt(),
        )

        plan_data = self._parse_json_response(response["content"])
        tasks = self._build_tasks(plan_data)
        lineage = {
            "revised_from_plan_id": existing_plan["id"],
            "revision_feedback": feedback,
            "revision_feedback_source": feedback_source or "inline",
            "revised_at": datetime.utcnow().isoformat(),
        }
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
            "metadata": {**lineage, "lineage": lineage},
        }

        return execution_plan, tasks

    def _get_system_prompt(self) -> str:
        """Get system prompt for plan revision."""
        return """You are a technical project planner revising execution plans.
You will receive an implementation brief, an existing execution plan, and human
feedback. Produce a complete replacement execution plan that incorporates the
feedback while preserving any still-valid work from the original plan.

Output ONLY valid JSON with no additional commentary. Use the exact schema
provided in the user prompt."""

    def _build_prompt(
        self,
        implementation_brief: dict[str, Any],
        existing_plan: dict[str, Any],
        feedback: str,
    ) -> str:
        """Build the revision prompt."""
        brief_json = json.dumps(implementation_brief, indent=2, sort_keys=True, default=str)
        plan_json = json.dumps(existing_plan, indent=2, sort_keys=True, default=str)

        return f"""# Implementation Brief
{brief_json}

# Existing Execution Plan
{plan_json}

# Human Feedback
{feedback}

---

Create a revised execution plan that incorporates the feedback. The revised plan
must be complete and executable on its own; do not output a patch or diff.

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
          "depends_on": ["Milestone 1: Foundation:0"],
          "files_or_modules": ["path/to/file.py", "module_name"],
          "acceptance_criteria": [
            "Specific, testable criterion 1",
            "Specific, testable criterion 2"
          ],
          "estimated_complexity": "low | medium | high"
        }}
      ]
    }}
  ],
  "test_strategy": "How we'll test this",
  "handoff_prompt": "Context for execution engine"
}}

Requirements:
- Incorporate the human feedback directly into milestones, tasks, acceptance criteria, and testing.
- Keep still-valid constraints from the implementation brief and original plan.
- Generate 3-5 sequential milestones unless the feedback clearly requires a smaller scope.
- Each task should have 2-4 acceptance criteria.
- Dependencies must refer to tasks in this revised plan using "Milestone Name:task_index" format.
- Output ONLY the JSON, no additional text."""

    def _parse_json_response(self, content: str) -> dict[str, Any]:
        """Parse JSON from an LLM response with common fallback strategies."""
        plan_data = None
        parse_error = None

        try:
            plan_data = json.loads(content)
        except json.JSONDecodeError as e:
            parse_error = e

        if plan_data is None and "```json" in content:
            try:
                start = content.find("```json") + 7
                end = content.find("```", start)
                plan_data = json.loads(content[start:end].strip())
            except json.JSONDecodeError as e:
                parse_error = e

        if plan_data is None and "```" in content:
            try:
                start = content.find("```") + 3
                end = content.find("```", start)
                json_str = content[start:end].strip()
                if json_str.startswith("json\n"):
                    json_str = json_str[5:]
                plan_data = json.loads(json_str)
            except json.JSONDecodeError as e:
                parse_error = e

        if plan_data is None:
            try:
                start_idx = content.find("{")
                end_idx = content.rfind("}")
                if start_idx != -1 and end_idx != -1:
                    plan_data = json.loads(content[start_idx : end_idx + 1])
            except json.JSONDecodeError as e:
                parse_error = e

        if plan_data is None:
            with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f:
                f.write(content)
                debug_file = f.name
            raise ValueError(
                "Failed to parse LLM response as JSON after trying multiple strategies.\n"
                f"Last error: {parse_error}\n"
                f"Response saved to: {debug_file}"
            )

        return plan_data

    def _build_tasks(self, plan_data: dict[str, Any]) -> list[dict[str, Any]]:
        """Build task records from revised plan JSON."""
        task_id_map: dict[str, str] = {}
        tasks = []

        for milestone in plan_data["milestones"]:
            milestone_name = milestone["name"]
            for task_idx, task_data in enumerate(milestone.get("tasks", [])):
                task_id = generate_task_id()
                task_id_map[f"{milestone_name}:{task_idx}"] = task_id

                depends_on = [
                    task_id_map[dep]
                    for dep in task_data.get("depends_on", [])
                    if dep in task_id_map
                ]

                tasks.append(
                    {
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
                )

        return tasks
