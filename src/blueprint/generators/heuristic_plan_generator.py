"""Deterministic heuristic execution plan generator."""

from __future__ import annotations

import hashlib
import re
from typing import Any

from blueprint.generators.json_repair import validate_execution_plan_payload


def generate_heuristic_execution_plan_id(brief_id: str) -> str:
    """Generate a stable execution plan ID for a brief."""
    return f"plan-heur-{_stable_token(brief_id, 8)}"


def generate_heuristic_task_id(brief_id: str, milestone_index: int, task_index: int) -> str:
    """Generate a stable task ID for a heuristic plan task."""
    return f"task-heur-{_stable_token(f'{brief_id}:{milestone_index}:{task_index}', 8)}"


class HeuristicPlanGenerator:
    """Build a useful execution plan without model access."""

    generation_model = "heuristic"

    def generate(
        self,
        implementation_brief: dict[str, Any],
        model: str | None = None,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        """Generate a deterministic execution plan from an implementation brief."""
        brief_id = implementation_brief["id"]
        milestones = self._build_milestones(implementation_brief)
        tasks: list[dict[str, Any]] = []
        prior_milestone_last_task_id: str | None = None

        for milestone_index, milestone in enumerate(milestones):
            milestone_tasks = milestone.pop("tasks")
            for task_index, task_data in enumerate(milestone_tasks):
                task_id = generate_heuristic_task_id(brief_id, milestone_index, task_index)
                depends_on = list(task_data.get("depends_on", []))
                if task_index == 0 and prior_milestone_last_task_id:
                    depends_on.append(prior_milestone_last_task_id)
                elif task_index > 0:
                    depends_on.append(
                        generate_heuristic_task_id(brief_id, milestone_index, task_index - 1)
                    )

                tasks.append(
                    {
                        "id": task_id,
                        "title": task_data["title"],
                        "description": task_data["description"],
                        "milestone": milestone["name"],
                        "owner_type": task_data.get("owner_type", "agent"),
                        "suggested_engine": task_data["suggested_engine"],
                        "depends_on": _dedupe(depends_on),
                        "files_or_modules": task_data["files_or_modules"],
                        "acceptance_criteria": task_data["acceptance_criteria"],
                        "estimated_complexity": task_data["estimated_complexity"],
                        "status": "pending",
                    }
                )

            if milestone_tasks:
                prior_milestone_last_task_id = tasks[-1]["id"]

        execution_plan = {
            "id": generate_heuristic_execution_plan_id(brief_id),
            "implementation_brief_id": brief_id,
            "target_engine": "codex",
            "target_repo": None,
            "project_type": self._infer_project_type(implementation_brief),
            "milestones": milestones,
            "test_strategy": self._build_test_strategy(implementation_brief),
            "handoff_prompt": self._build_handoff_prompt(implementation_brief),
            "status": "draft",
            "generation_model": self.generation_model,
            "generation_tokens": 0,
            "generation_prompt": None,
            "metadata": {
                "generation_method": "deterministic_heuristic",
                "source_fields": [
                    "scope",
                    "definition_of_done",
                    "product_surface",
                    "integration_points",
                ],
            },
        }

        validate_execution_plan_payload(execution_plan, tasks)
        return execution_plan, tasks

    def _build_milestones(self, brief: dict[str, Any]) -> list[dict[str, Any]]:
        scope_items = _clean_items(brief.get("scope"))
        dod_items = _clean_items(brief.get("definition_of_done"))
        integration_points = _clean_items(brief.get("integration_points"))
        product_surface = brief.get("product_surface") or "the product surface"

        foundation_tasks = [
            self._task(
                title="Confirm implementation boundaries and baseline structure",
                description=(
                    "Review the implementation brief, map the in-scope work to the "
                    "existing codebase, and identify the files or modules that will "
                    "anchor the change."
                ),
                files=self._foundation_files(brief),
                acceptance=[
                    "Scope items are mapped to concrete files, modules, or new components.",
                    "Non-goals and assumptions are reflected in the implementation notes.",
                    f"The {product_surface} surface has a clear baseline integration point.",
                ],
                complexity="low",
                engine="codex",
            )
        ]
        if integration_points:
            foundation_tasks.append(
                self._task(
                    title="Define integration contracts and data touchpoints",
                    description=(
                        "Document how the plan will interact with required integration "
                        "points before implementation begins."
                    ),
                    files=self._integration_files(integration_points),
                    acceptance=[
                        "Each integration point has an expected input, output, or interface.",
                        "Credential-free or offline behavior is explicitly accounted for.",
                        "Integration risks are captured before core implementation starts.",
                    ],
                    complexity="medium",
                    engine="codex",
                )
            )

        implementation_tasks = [
            self._task(
                title=f"Implement {item}",
                description=f"Build the in-scope capability: {item}.",
                files=self._files_for_scope_item(item, product_surface),
                acceptance=[
                    f"{item} is implemented for the {product_surface} surface.",
                    "The behavior follows the architecture notes and existing project patterns.",
                    "Relevant error or empty-state handling is included.",
                ],
                complexity=self._complexity_for_text(item),
                engine=self._engine_for_text(item, product_surface),
            )
            for item in (scope_items or [brief.get("mvp_goal") or brief["title"]])
        ]

        validation_tasks = [
            self._task(
                title="Add automated validation coverage",
                description=brief.get("validation_plan")
                or "Add deterministic validation for the generated behavior.",
                files=self._validation_files(brief),
                acceptance=[
                    "Automated tests cover the primary success path.",
                    "Automated tests cover at least one boundary or failure case.",
                    "The documented validation command passes locally.",
                ],
                complexity="medium",
                engine="codex",
            )
        ]
        validation_tasks.extend(
            self._task(
                title=f"Verify definition of done: {item}",
                description=f"Confirm and document completion criterion: {item}.",
                files=self._validation_files(brief),
                acceptance=[
                    f"The criterion is demonstrably satisfied: {item}.",
                    "Evidence is captured in tests, docs, or inspectable output.",
                ],
                complexity="low",
                engine="codex",
            )
            for item in dod_items[:3]
        )

        return [
            {
                "name": "Milestone 1: Foundation",
                "description": "Establish codebase boundaries, contracts, and baseline structure.",
                "tasks": foundation_tasks,
            },
            {
                "name": "Milestone 2: Implementation",
                "description": "Build the scoped product behavior and integration paths.",
                "tasks": implementation_tasks,
            },
            {
                "name": "Milestone 3: Validation",
                "description": "Prove the implementation satisfies the validation plan and definition of done.",
                "tasks": validation_tasks,
            },
        ]

    def _task(
        self,
        *,
        title: str,
        description: str,
        files: list[str],
        acceptance: list[str],
        complexity: str,
        engine: str,
    ) -> dict[str, Any]:
        return {
            "title": title,
            "description": description,
            "files_or_modules": _dedupe(files) or ["TBD"],
            "acceptance_criteria": acceptance,
            "estimated_complexity": complexity,
            "suggested_engine": engine,
            "owner_type": "agent",
        }

    def _infer_project_type(self, brief: dict[str, Any]) -> str:
        surface = (brief.get("product_surface") or "").lower()
        if "cli" in surface:
            return "cli_tool"
        if "api" in surface:
            return "api_service"
        if any(term in surface for term in ("ui", "web", "dashboard", "frontend")):
            return "web_app"
        if "library" in surface or "package" in surface:
            return "python_library"
        if _clean_items(brief.get("integration_points")):
            return "integration"
        return "application"

    def _build_test_strategy(self, brief: dict[str, Any]) -> str:
        validation_plan = brief.get("validation_plan") or "Run the focused test suite."
        dod = _clean_items(brief.get("definition_of_done"))
        dod_text = "; ".join(dod[:3]) if dod else "the definition of done"
        return f"{validation_plan} Confirm {dod_text}."

    def _build_handoff_prompt(self, brief: dict[str, Any]) -> str:
        scope = "; ".join(_clean_items(brief.get("scope"))[:5]) or brief.get("mvp_goal")
        integrations = ", ".join(_clean_items(brief.get("integration_points"))) or "none"
        return (
            f"Implement '{brief['title']}' with a deterministic, credential-free path. "
            f"Primary scope: {scope}. Integration points: {integrations}. "
            "Keep changes aligned with existing project patterns and validate before handoff."
        )

    def _foundation_files(self, brief: dict[str, Any]) -> list[str]:
        files = ["src", "tests"]
        surface = (brief.get("product_surface") or "").lower()
        if "cli" in surface:
            files.append("src/blueprint/cli.py")
        if brief.get("architecture_notes"):
            files.append("architecture_notes")
        return files

    def _integration_files(self, integration_points: list[str]) -> list[str]:
        return [f"integration:{_slugify(point)}" for point in integration_points]

    def _files_for_scope_item(self, item: str, product_surface: str) -> list[str]:
        slug = _slugify(item)
        files = [f"src/{slug}"]
        surface = product_surface.lower()
        if "cli" in surface:
            files.append("src/blueprint/cli.py")
        if any(term in surface for term in ("ui", "web", "dashboard", "frontend")):
            files.append(f"ui/{slug}")
        files.append(f"tests/test_{slug}.py")
        return files

    def _validation_files(self, brief: dict[str, Any]) -> list[str]:
        title_slug = _slugify(brief["title"])
        return [f"tests/test_{title_slug}.py", "README.md"]

    def _complexity_for_text(self, text: str) -> str:
        lowered = text.lower()
        if any(
            term in lowered
            for term in ("migration", "authentication", "authorization", "integration", "sync")
        ):
            return "high"
        if len(text) > 80 or any(term in lowered for term in ("workflow", "store", "api")):
            return "medium"
        return "low"

    def _engine_for_text(self, text: str, product_surface: str) -> str:
        combined = f"{text} {product_surface}".lower()
        if any(term in combined for term in ("visual", "prototype", "mockup")):
            return "smoothie"
        if any(term in combined for term in ("approve", "legal", "stakeholder")):
            return "manual"
        return "codex"


def _clean_items(value: Any) -> list[str]:
    if not value:
        return []
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    return [str(item).strip() for item in value if str(item).strip()]


def _dedupe(items: list[str]) -> list[str]:
    seen = set()
    result = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def _slugify(value: str) -> str:
    words = re.findall(r"[a-zA-Z0-9]+", value.lower())
    return "_".join(words[:6]) or "item"


def _stable_token(value: str, length: int) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:length]
