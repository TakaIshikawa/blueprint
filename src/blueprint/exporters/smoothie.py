"""Smoothie exporter - Markdown product brief format."""

from typing import Any

from blueprint.exporters.base import TargetExporter


class SmoothieExporter(TargetExporter):
    """Export execution plans to Smoothie-compatible product briefs."""

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
        Export to Smoothie Markdown format.

        Smoothie expects:
        - Target user and workflow context
        - Product concept and goals
        - Screens/views to prototype
        - Interactions and user flows
        - Validation questions
        """
        self.ensure_output_dir(output_path)

        # Build Markdown content
        content = self._build_smoothie_brief(execution_plan, implementation_brief)

        # Write to file
        with open(output_path, 'w') as f:
            f.write(content)

        return output_path

    def _build_smoothie_brief(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
    ) -> str:
        """Build Smoothie product brief in Markdown."""
        sections = []

        # Header
        sections.append(f"# {brief['title']}")
        sections.append(f"\n*Product Brief for Smoothie Prototype*\n")

        # Problem & Solution
        sections.append("## Problem")
        sections.append(brief['problem_statement'])

        sections.append("\n## Solution")
        sections.append(brief['mvp_goal'])

        # Target User
        sections.append("\n## Target User")
        if brief.get('target_user'):
            sections.append(brief['target_user'])
        else:
            sections.append("*To be defined*")

        if brief.get('buyer'):
            sections.append(f"\n**Buyer:** {brief['buyer']}")

        # Workflow Context
        if brief.get('workflow_context'):
            sections.append("\n## Workflow Context")
            sections.append(brief['workflow_context'])

        # Product Surface
        if brief.get('product_surface'):
            sections.append(f"\n## Product Surface")
            sections.append(brief['product_surface'])

        # Product surfaces - adapt based on product type
        product_surface = brief.get('product_surface', '').lower()

        if 'library' in product_surface or 'api' in product_surface:
            sections.append("\n## API Surfaces to Prototype")
            sections.append("\nBased on the MVP scope, prototype these key interfaces:\n")
            for i, scope_item in enumerate(brief.get('scope', [])[:5], 1):
                sections.append(f"{i}. **{scope_item}**")
                sections.append(f"   - Implementation approach: Show how developers will interact with this")

        elif 'cli' in product_surface:
            sections.append("\n## Commands to Prototype")
            sections.append("\nBased on the MVP scope, prototype these key commands:\n")
            for i, scope_item in enumerate(brief.get('scope', [])[:5], 1):
                sections.append(f"{i}. **{scope_item}**")
                sections.append(f"   - Command flow: How user invokes and gets feedback")

        else:
            # Default to screens for UI/web apps
            sections.append("\n## Screens/Views to Prototype")
            sections.append("\nBased on the MVP scope, prototype these key views:\n")
            for i, scope_item in enumerate(brief.get('scope', [])[:5], 1):
                sections.append(f"{i}. **{self._scope_to_screen(scope_item)}**")
                sections.append(f"   - Purpose: {scope_item}")

        # User Flow - adapt based on product type
        product_surface = brief.get('product_surface', '').lower()

        sections.append("\n## Primary User Flow")

        if 'library' in product_surface or 'api' in product_surface:
            sections.append("\n```python")
            sections.append("# Developer integration flow")
            sections.append("1. Developer installs library")
            sections.append("2. Developer imports and configures")
            sections.append("3. Developer calls primary API")
            sections.append("4. Library provides result or error feedback")
            sections.append("```")

        elif 'cli' in product_surface:
            sections.append("\n```bash")
            sections.append("# Command-line workflow")
            sections.append("1. User runs primary command")
            sections.append("2. CLI prompts for input or uses flags")
            sections.append("3. System processes and shows progress")
            sections.append("4. CLI displays results or error")
            sections.append("```")

        else:
            sections.append("\n```")
            sections.append("1. User lands on main screen")
            sections.append("2. User performs primary action")
            sections.append("3. System provides feedback")
            sections.append("4. User sees result/completion state")
            sections.append("```")

        # Interactions - adapt based on product type
        sections.append("\n## Key Interactions")

        if 'library' in product_surface or 'api' in product_surface:
            sections.append("\n- **Primary API calls**: Core methods developers will use")
            sections.append("- **Return values**: What data structure is returned")
            sections.append("- **Exception handling**: What errors are raised and when")
            sections.append("- **Configuration options**: How developers customize behavior")

        elif 'cli' in product_surface:
            sections.append("\n- **Primary commands**: Main CLI commands users will run")
            sections.append("- **Input/output**: How data flows in and out")
            sections.append("- **Progress feedback**: How users know what's happening")
            sections.append("- **Error messages**: What users see when things fail")

        else:
            sections.append("\n- **Primary action**: Main user goal")
            sections.append("- **Feedback mechanism**: How system responds")
            sections.append("- **Error states**: What happens when things fail")

        # Validation
        sections.append("\n## Validation Questions")
        sections.append("\nPrototype should help answer:")
        sections.append(f"\n1. Can users understand {brief.get('product_surface', 'the interface')}?")
        sections.append("2. Is the primary workflow intuitive?")
        sections.append("3. Do users know what to do next at each step?")
        sections.append("4. Are error messages clear and actionable?")

        # Definition of Done
        sections.append("\n## Definition of Done")
        for item in brief.get('definition_of_done', []):
            sections.append(f"- {item}")

        # Non-Goals
        if brief.get('non_goals'):
            sections.append("\n## Out of Scope for Prototype")
            for item in brief['non_goals'][:5]:
                sections.append(f"- {item}")

        # Context
        sections.append("\n---")
        sections.append("\n## Additional Context")
        sections.append(f"\n**Source**: Blueprint implementation brief {brief['id']}")
        sections.append(f"**Execution Plan**: {plan['id']}")
        sections.append(f"\n*Generated by Blueprint for Smoothie prototyping*")

        return "\n".join(sections)

    def _scope_to_screen(self, scope_item: str) -> str:
        """Convert a scope item to a screen name."""
        # Simple heuristic: extract key nouns/actions
        item = scope_item.lower()

        if "yaml" in item or "schema" in item:
            return "Schema Editor"
        elif "cli" in item or "command" in item:
            return "CLI Interface"
        elif "test" in item:
            return "Test Results View"
        elif "config" in item:
            return "Configuration Screen"
        elif "list" in item or "view" in item:
            return "List View"
        elif "scorecard" in item or "report" in item:
            return "Results Dashboard"
        else:
            return "Feature Screen"
