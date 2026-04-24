"""Immutable JSON snapshot exporter for archival plan review."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from typing import Any

from blueprint.audits.plan_metrics import calculate_plan_metrics
from blueprint.exporters.base import TargetExporter


SCHEMA_VERSION = "blueprint.plan_snapshot.v1"
HASH_ALGORITHM = "sha256"


class PlanSnapshotExporter(TargetExporter):
    """Export a compact canonical snapshot of a plan and its linked brief."""

    def get_format(self) -> str:
        """Get export format."""
        return "json"

    def get_extension(self) -> str:
        """Get file extension."""
        return ".json"

    def export(
        self,
        execution_plan: dict[str, Any],
        implementation_brief: dict[str, Any],
        output_path: str,
    ) -> str:
        """Export an immutable plan snapshot JSON artifact."""
        plan, brief = self.validate_export_payload(execution_plan, implementation_brief)
        self.ensure_output_dir(output_path)

        payload = self.render_payload(plan, brief)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, sort_keys=False)
            f.write("\n")

        return output_path

    def render_payload(
        self,
        execution_plan: dict[str, Any],
        implementation_brief: dict[str, Any],
    ) -> dict[str, Any]:
        """Build the snapshot payload from validated plan and brief dictionaries."""
        content_hash = compute_content_hash(execution_plan, implementation_brief)
        return {
            "schema_version": SCHEMA_VERSION,
            "exported_at": datetime.now(UTC).isoformat(),
            "content_hash": content_hash,
            "hash_algorithm": HASH_ALGORITHM,
            "plan": self._plan_summary(execution_plan),
            "brief": self._brief_summary(implementation_brief),
            "milestones": self._milestones(execution_plan),
            "tasks": self._tasks(execution_plan),
            "dependencies": self._dependency_edges(execution_plan),
            "metrics": calculate_plan_metrics(execution_plan).to_dict(),
        }

    def _plan_summary(self, plan: dict[str, Any]) -> dict[str, Any]:
        """Return compact plan-level metadata."""
        return {
            "id": plan["id"],
            "implementation_brief_id": plan["implementation_brief_id"],
            "status": plan.get("status"),
            "target_engine": plan.get("target_engine"),
            "target_repo": plan.get("target_repo"),
            "project_type": plan.get("project_type"),
            "test_strategy": plan.get("test_strategy"),
            "generation_model": plan.get("generation_model"),
            "generation_tokens": plan.get("generation_tokens"),
        }

    def _brief_summary(self, brief: dict[str, Any]) -> dict[str, Any]:
        """Return compact linked brief metadata and review context."""
        return {
            "id": brief["id"],
            "source_brief_id": brief["source_brief_id"],
            "title": brief["title"],
            "status": brief.get("status"),
            "domain": brief.get("domain"),
            "target_user": brief.get("target_user"),
            "buyer": brief.get("buyer"),
            "problem_statement": brief["problem_statement"],
            "mvp_goal": brief["mvp_goal"],
            "scope": brief.get("scope") or [],
            "non_goals": brief.get("non_goals") or [],
            "assumptions": brief.get("assumptions") or [],
            "risks": brief.get("risks") or [],
            "validation_plan": brief["validation_plan"],
            "definition_of_done": brief.get("definition_of_done") or [],
        }

    def _milestones(self, plan: dict[str, Any]) -> list[dict[str, Any]]:
        """Return milestones with stable IDs when the source lacks one."""
        milestones = []
        for index, milestone in enumerate(plan.get("milestones", []), start=1):
            milestones.append(
                {
                    "id": milestone.get("id") or f"m{index}",
                    "name": milestone.get("name"),
                    "description": milestone.get("description"),
                    "order": index,
                }
            )
        return milestones

    def _tasks(self, plan: dict[str, Any]) -> list[dict[str, Any]]:
        """Return task records relevant for archival review."""
        return [
            {
                "id": task["id"],
                "title": task["title"],
                "description": task["description"],
                "milestone": task.get("milestone"),
                "owner_type": task.get("owner_type"),
                "suggested_engine": task.get("suggested_engine"),
                "status": task.get("status"),
                "depends_on": task.get("depends_on") or [],
                "files_or_modules": task.get("files_or_modules") or [],
                "acceptance_criteria": task.get("acceptance_criteria") or [],
                "estimated_complexity": task.get("estimated_complexity"),
                "blocked_reason": self._blocked_reason(task),
                "metadata": task.get("metadata") or {},
            }
            for task in plan.get("tasks", [])
        ]

    def _dependency_edges(self, plan: dict[str, Any]) -> list[dict[str, str]]:
        """Return task dependency edges as dependency-to-dependent pairs."""
        edges = []
        for task in plan.get("tasks", []):
            for dependency_id in task.get("depends_on") or []:
                edges.append(
                    {
                        "from": dependency_id,
                        "to": task["id"],
                    }
                )
        return edges

    def _blocked_reason(self, task: dict[str, Any]) -> str | None:
        """Return blocked reason from first-class field or task metadata."""
        metadata = task.get("metadata") or {}
        return task.get("blocked_reason") or metadata.get("blocked_reason")


def compute_content_hash(
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any],
) -> str:
    """Compute a deterministic hash from canonical plan and brief content."""
    canonical_payload = {
        "plan": execution_plan,
        "brief": implementation_brief,
    }
    canonical_json = json.dumps(
        _canonicalize(canonical_payload),
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


def _canonicalize(value: Any) -> Any:
    """Convert supported domain values into deterministic JSON primitives."""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _canonicalize(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        return [_canonicalize(item) for item in value]
    return value
