"""Codex exporter - Markdown implementation prompt format."""

from typing import Any

from blueprint.exporters.base import TargetExporter


class CodexExporter(TargetExporter):
    """Export execution plans to Codex-compatible implementation prompts."""

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
        Export to Codex Markdown format.

        Similar to Claude Code but optimized for Codex's interface.
        """
        self.ensure_output_dir(output_path)
        execution_plan, implementation_brief = self.validate_export_payload(
            execution_plan,
            implementation_brief,
        )

        # Build Markdown content
        content = self._build_codex_prompt(execution_plan, implementation_brief)

        # Write to file
        with open(output_path, 'w') as f:
            f.write(content)

        return output_path

    def _build_codex_prompt(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
    ) -> str:
        """Build Codex implementation prompt in Markdown."""
        sections = []

        # Header - Codex style
        sections.append(f"# BUILD: {brief['title']}")
        sections.append(f"\n> {brief['mvp_goal'][:200]}...")
        sections.append(f"\n**Target Repository:** {plan.get('target_repo', 'TBD')}\n")

        # Quick Overview
        sections.append("## Overview")
        sections.append(brief['problem_statement'])

        # Technical Spec
        sections.append("\n## Technical Specification")

        if brief.get('architecture_notes'):
            sections.append("\n### Architecture")
            sections.append(brief['architecture_notes'])

        if brief.get('data_requirements'):
            sections.append("\n### Data Requirements")
            sections.append(brief['data_requirements'])

        if brief.get('integration_points'):
            sections.append("\n### Integration Points")
            for integration in brief['integration_points']:
                sections.append(f"- {integration}")

        # Milestones & Tasks
        sections.append("\n## Build Plan")

        for i, milestone in enumerate(plan['milestones'], 1):
            sections.append(f"\n### Phase {i}: {milestone['name']}")

            # Get tasks for this milestone
            milestone_tasks = [
                t for t in plan['tasks']
                if t.get('milestone') == milestone['name']
            ]

            for j, task in enumerate(milestone_tasks, 1):
                sections.append(f"\n**Task {i}.{j}: {task['title']}**")
                sections.append(f"\n{task['description']}")

                # Files
                if task.get('files_or_modules'):
                    files_str = ', '.join(f"`{f}`" for f in task['files_or_modules'])
                    sections.append(f"\n*Files:* {files_str}")

                # Acceptance
                if task.get('acceptance_criteria'):
                    sections.append("\n*Success criteria:*")
                    for ac in task['acceptance_criteria']:
                        sections.append(f"  - {ac}")

        # What to Build
        sections.append("\n## Feature Scope")

        sections.append("\n### ✅ In Scope")
        for item in brief.get('scope', []):
            sections.append(f"- {item}")

        sections.append("\n### ❌ Out of Scope")
        for item in brief.get('non_goals', []):
            sections.append(f"- {item}")

        # Quality Requirements
        sections.append("\n## Quality Requirements")

        sections.append(f"\n**Test Strategy:** {plan.get('test_strategy', 'TBD')}")

        sections.append("\n**Acceptance:**")
        for item in brief.get('definition_of_done', []):
            sections.append(f"- {item}")

        # Implementation Notes
        sections.append("\n## Implementation Notes")

        if brief.get('assumptions'):
            sections.append("\n**Assumptions:**")
            for assumption in brief['assumptions']:
                sections.append(f"- {assumption}")

        if brief.get('risks'):
            sections.append("\n**Watch out for:**")
            for risk in brief['risks']:
                sections.append(f"- {risk}")

        if plan.get('handoff_prompt'):
            sections.append(f"\n**Additional context:** {plan['handoff_prompt']}")

        # Footer
        sections.append("\n---")
        sections.append(f"\n📋 Blueprint Plan ID: `{plan['id']}`")

        return "\n".join(sections)
