"""Claude Code exporter - Markdown implementation prompt format."""

from typing import Any

from blueprint.config import Config
from blueprint.exporters.base import TargetExporter
from blueprint.exporters.templates import MarkdownTemplateRenderer
from blueprint.validation_commands import format_validation_commands


class ClaudeCodeExporter(TargetExporter):
    """Export execution plans to Claude Code-compatible implementation prompts."""

    def __init__(self, config: Config | None = None):
        """Initialize exporter with optional configuration override."""
        self.template_renderer = MarkdownTemplateRenderer("claude_code", config=config)

    def get_format(self) -> str:
        """Get export format."""
        return "markdown"

    def get_extension(self) -> str:
        """Get file extension."""
        return ".md"

    def export(
        self,
        execution_plan: dict[str, Any],
        implementation_brief: dict[str, Any],
        output_path: str,
    ) -> str:
        """
        Export to Claude Code Markdown format.

        Claude Code expects:
        - Clear project context and goals
        - File paths and modules to work on
        - Acceptance criteria
        - Commands to run for validation
        - Constraints and guidelines
        """
        self.ensure_output_dir(output_path)
        execution_plan, implementation_brief = self.validate_export_payload(
            execution_plan,
            implementation_brief,
        )

        # Build Markdown content
        content = self._build_claude_code_prompt(execution_plan, implementation_brief)
        content = self.template_renderer.render(
            content,
            execution_plan,
            implementation_brief,
        )

        # Write to file
        with open(output_path, "w") as f:
            f.write(content)

        return output_path

    def _build_claude_code_prompt(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
    ) -> str:
        """Build Claude Code implementation prompt in Markdown."""
        sections = []

        # Header
        sections.append(f"# Implementation: {brief['title']}")
        sections.append(f"\n**Repository**: `{plan.get('target_repo', 'TBD')}`")
        sections.append(f"**Project Type**: {plan.get('project_type', 'TBD')}\n")

        # Context
        sections.append("## Context")
        sections.append("\n### Problem")
        sections.append(brief["problem_statement"])

        sections.append("\n### Goal")
        sections.append(brief["mvp_goal"])

        # Architecture
        if brief.get("architecture_notes"):
            sections.append("\n### Architecture")
            sections.append(brief["architecture_notes"])

        # Implementation Plan
        sections.append("\n## Implementation Plan")

        for milestone in plan["milestones"]:
            sections.append(f"\n### {milestone['name']}")
            if milestone.get("description"):
                sections.append(milestone["description"])

            # Get tasks for this milestone
            milestone_tasks = [t for t in plan["tasks"] if t.get("milestone") == milestone["name"]]

            if milestone_tasks:
                sections.append("\n**Tasks:**")
                for task in milestone_tasks:
                    sections.append(f"\n#### {task['title']}")
                    sections.append(f"\n{task['description']}")

                    if task.get("files_or_modules"):
                        sections.append("\n**Files to modify:**")
                        for file in task["files_or_modules"]:
                            sections.append(f"- `{file}`")

                    if task.get("acceptance_criteria"):
                        sections.append("\n**Acceptance Criteria:**")
                        for ac in task["acceptance_criteria"]:
                            sections.append(f"- {ac}")

                    if task.get("depends_on"):
                        sections.append(f"\n**Depends on:** {', '.join(task['depends_on'])}")

        # Scope
        sections.append("\n## In Scope")
        for item in brief.get("scope", []):
            sections.append(f"- {item}")

        # Non-Goals
        sections.append("\n## Out of Scope")
        for item in brief.get("non_goals", []):
            sections.append(f"- {item}")

        # Constraints
        sections.append("\n## Constraints & Guidelines")

        if brief.get("assumptions"):
            sections.append("\n**Assumptions:**")
            for assumption in brief["assumptions"]:
                sections.append(f"- {assumption}")

        if brief.get("risks"):
            sections.append("\n**Risks to watch:**")
            for risk in brief["risks"]:
                sections.append(f"- {risk}")

        # Validation
        sections.append("\n## Validation")
        sections.append(f"\n**Test Strategy:** {plan.get('test_strategy', 'TBD')}")
        validation_commands = (plan.get("metadata") or {}).get("validation_commands")
        if validation_commands:
            sections.append("\n**Recommended Commands:**")
            sections.append(format_validation_commands(validation_commands))

        sections.append("\n**Definition of Done:**")
        for item in brief.get("definition_of_done", []):
            sections.append(f"- {item}")

        # Handoff
        if plan.get("handoff_prompt"):
            sections.append("\n## Additional Context")
            sections.append(plan["handoff_prompt"])

        # Footer
        sections.append("\n---")
        sections.append("\n*Generated by Blueprint*")
        sections.append(f"- Implementation Brief: `{brief['id']}`")
        sections.append(f"- Execution Plan: `{plan['id']}`")

        return "\n".join(sections)
