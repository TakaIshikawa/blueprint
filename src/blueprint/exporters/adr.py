"""Architecture Decision Record exporter for execution plans."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from blueprint.exporters.base import TargetExporter


@dataclass(frozen=True, slots=True)
class ArchitectureDecision:
    """One rendered architecture decision record."""

    title: str
    group_type: str
    group_name: str
    context: list[str]
    decision: str
    consequences: list[str]
    related_tasks: list[dict[str, Any]]


class ADRExporter(TargetExporter):
    """Export plan and brief architecture context as Markdown ADR files."""

    def get_format(self) -> str:
        """Get export format."""
        return "markdown"

    def get_extension(self) -> str:
        """Get file extension."""
        return ""

    def export(
        self,
        execution_plan: dict[str, Any],
        implementation_brief: dict[str, Any],
        output_path: str,
    ) -> str:
        """Export architecture decisions to a directory of Markdown ADRs."""
        plan, brief = self.validate_export_payload(execution_plan, implementation_brief)

        adr_dir = Path(output_path)
        adr_dir.mkdir(parents=True, exist_ok=True)

        decisions = self._build_decisions(plan, brief)
        adr_files = [
            (decision, self._adr_filename(index, decision))
            for index, decision in enumerate(decisions, start=1)
        ]

        (adr_dir / "README.md").write_text(self._readme_content(plan, brief, adr_files))
        for index, (decision, filename) in enumerate(adr_files, start=1):
            (adr_dir / filename).write_text(self._adr_content(index, plan, brief, decision))

        return str(adr_dir)

    def _build_decisions(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
    ) -> list[ArchitectureDecision]:
        """Build deterministic decision groups from milestones and integrations."""
        decisions: list[ArchitectureDecision] = []

        for index, milestone in enumerate(plan.get("milestones", []), 1):
            milestone_name = self._milestone_name(milestone, index)
            tasks = [
                task for task in plan.get("tasks", []) if task.get("milestone") == milestone_name
            ]
            decisions.append(
                ArchitectureDecision(
                    title=f"Implement {milestone_name}",
                    group_type="Milestone",
                    group_name=milestone_name,
                    context=self._context_lines(brief, plan, milestone, tasks),
                    decision=(
                        f"Deliver the `{milestone_name}` milestone as an implementation "
                        "boundary for the execution plan."
                    ),
                    consequences=self._consequence_lines(brief, plan, tasks),
                    related_tasks=tasks,
                )
            )

        rendered_milestones = {
            self._milestone_name(milestone, index)
            for index, milestone in enumerate(plan.get("milestones", []), 1)
        }
        ungrouped_tasks = [
            task
            for task in plan.get("tasks", [])
            if (task.get("milestone") or "") not in rendered_milestones
        ]
        if ungrouped_tasks:
            decisions.append(
                ArchitectureDecision(
                    title="Implement Ungrouped Tasks",
                    group_type="Milestone",
                    group_name="Ungrouped",
                    context=self._context_lines(brief, plan, {}, ungrouped_tasks),
                    decision="Deliver ungrouped execution tasks as a separate implementation boundary.",
                    consequences=self._consequence_lines(brief, plan, ungrouped_tasks),
                    related_tasks=ungrouped_tasks,
                )
            )

        for integration in brief.get("integration_points") or []:
            related_tasks = self._integration_tasks(str(integration), plan.get("tasks", []))
            decisions.append(
                ArchitectureDecision(
                    title=f"Integrate {integration}",
                    group_type="Integration Point",
                    group_name=str(integration),
                    context=self._context_lines(
                        brief, plan, {"description": integration}, related_tasks
                    ),
                    decision=f"Treat `{integration}` as an explicit integration boundary for implementation work.",
                    consequences=self._consequence_lines(brief, plan, related_tasks),
                    related_tasks=related_tasks,
                )
            )

        if not decisions:
            decisions.append(
                ArchitectureDecision(
                    title="Establish Plan Architecture",
                    group_type="Plan",
                    group_name=plan["id"],
                    context=self._context_lines(brief, plan, {}, []),
                    decision="Use the implementation brief and execution plan as the architecture boundary.",
                    consequences=self._consequence_lines(brief, plan, []),
                    related_tasks=[],
                )
            )

        return decisions

    def _readme_content(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        adr_files: list[tuple[ArchitectureDecision, str]],
    ) -> str:
        """Build the ADR directory index."""
        lines = [
            f"# Architecture Decision Records: {plan['id']}",
            "",
            "## Source Blueprint",
            f"- Plan ID: `{plan['id']}`",
            f"- Implementation Brief ID: `{brief['id']}`",
            f"- Source Brief ID: `{brief['source_brief_id']}`",
            f"- Title: {brief['title']}",
            f"- Target Repo: {plan.get('target_repo') or 'N/A'}",
            "",
            "## ADR Index",
        ]
        if adr_files:
            lines.extend(
                f"{index}. [{decision.title}]({filename}) - {decision.group_type}: {decision.group_name}"
                for index, (decision, filename) in enumerate(adr_files, 1)
            )
        else:
            lines.append("No architecture decisions generated.")
        return "\n".join(lines) + "\n"

    def _adr_content(
        self,
        index: int,
        plan: dict[str, Any],
        brief: dict[str, Any],
        decision: ArchitectureDecision,
    ) -> str:
        """Build one Markdown ADR."""
        lines = [
            f"# ADR-{index:03d}: {decision.title}",
            "",
            "## Status",
            "Proposed",
            "",
            "## Context",
        ]
        lines.extend(self._bullet_lines(decision.context))
        lines.extend(
            [
                "",
                "## Decision",
                decision.decision,
                "",
                "## Consequences",
            ]
        )
        lines.extend(self._bullet_lines(decision.consequences))
        lines.extend(["", "## Related Tasks"])
        lines.extend(self._related_task_lines(decision.related_tasks))
        lines.extend(
            [
                "",
                "## Source Blueprint IDs",
                f"- Plan ID: `{plan['id']}`",
                f"- Implementation Brief ID: `{brief['id']}`",
                f"- Source Brief ID: `{brief['source_brief_id']}`",
                f"- Group Type: {decision.group_type}",
                f"- Group Name: {decision.group_name}",
            ]
        )
        return "\n".join(lines) + "\n"

    def _context_lines(
        self,
        brief: dict[str, Any],
        plan: dict[str, Any],
        group: dict[str, Any],
        tasks: list[dict[str, Any]],
    ) -> list[str]:
        """Render context from architecture notes or derived brief and plan fields."""
        lines: list[str] = []
        architecture_notes = brief.get("architecture_notes")
        if architecture_notes:
            lines.append(f"Architecture notes: {architecture_notes}")
        else:
            lines.extend(
                [
                    f"Problem: {brief.get('problem_statement') or 'N/A'}",
                    f"MVP goal: {brief.get('mvp_goal') or 'N/A'}",
                    f"Target repository: {plan.get('target_repo') or 'N/A'}",
                    f"Project type: {plan.get('project_type') or 'N/A'}",
                ]
            )

        description = group.get("description") or group.get("summary")
        if description:
            lines.append(f"Group context: {description}")
        lines.extend(f"Assumption: {assumption}" for assumption in brief.get("assumptions") or [])
        lines.extend(
            f"Integration point: {point}" for point in brief.get("integration_points") or []
        )
        lines.extend(f"Risk: {risk}" for risk in brief.get("risks") or [])
        if tasks:
            lines.append(
                "Related task scope: "
                + ", ".join(
                    f"`{task['id']}`" for task in sorted(tasks, key=lambda item: item["id"])
                )
            )
        return lines or ["No architecture context was provided."]

    def _consequence_lines(
        self,
        brief: dict[str, Any],
        plan: dict[str, Any],
        tasks: list[dict[str, Any]],
    ) -> list[str]:
        """Render expected consequences for a decision."""
        lines = [
            f"Validation follows: {plan.get('test_strategy') or brief.get('validation_plan') or 'N/A'}",
        ]
        lines.extend(f"Risk to manage: {risk}" for risk in brief.get("risks") or [])
        files = sorted(
            {
                file_or_module
                for task in tasks
                for file_or_module in (task.get("files_or_modules") or [])
            }
        )
        if files:
            lines.append("Implementation touches: " + ", ".join(files))
        if not tasks:
            lines.append("No execution tasks were directly mapped to this decision.")
        return lines

    def _related_task_lines(self, tasks: list[dict[str, Any]]) -> list[str]:
        """Render related tasks for an ADR."""
        if not tasks:
            return ["- None"]
        return [
            f"- `{task['id']}` {task['title']} ({task.get('status') or 'pending'})"
            for task in sorted(tasks, key=lambda item: item["id"])
        ]

    def _integration_tasks(
        self,
        integration: str,
        tasks: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Find tasks that mention an integration point in task planning fields."""
        needle = integration.lower()
        related = []
        for task in tasks:
            haystack = " ".join(
                [
                    task.get("title") or "",
                    task.get("description") or "",
                    " ".join(task.get("files_or_modules") or []),
                    " ".join(task.get("acceptance_criteria") or []),
                ]
            ).lower()
            if needle in haystack:
                related.append(task)
        return related

    def _adr_filename(self, index: int, decision: ArchitectureDecision) -> str:
        """Build a deterministic ADR filename."""
        return f"{index:03d}-{self._slug(decision.group_type)}-{self._slug(decision.group_name)}.md"

    def _milestone_name(self, milestone: dict[str, Any], index: int) -> str:
        """Get a display name for a milestone."""
        return milestone.get("name") or milestone.get("title") or f"Milestone {index}"

    def _slug(self, value: str) -> str:
        """Normalize labels for filesystem-safe filenames."""
        slug = re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip("-").lower()
        return slug or "adr"

    def _bullet_lines(self, value: list[str]) -> list[str]:
        """Render Markdown bullets."""
        return [f"- {item}" for item in value] or ["- None"]
