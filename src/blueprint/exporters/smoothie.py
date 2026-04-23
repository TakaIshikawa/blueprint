"""Smoothie exporter - Markdown product brief format."""

from typing import Any

from blueprint.config import Config
from blueprint.exporters.base import TargetExporter
from blueprint.exporters.templates import MarkdownTemplateRenderer


class SmoothieExporter(TargetExporter):
    """Export execution plans to Smoothie-compatible product briefs."""

    def __init__(self, config: Config | None = None):
        """Initialize exporter with optional configuration override."""
        self.template_renderer = MarkdownTemplateRenderer("smoothie", config=config)

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
        execution_plan, implementation_brief = self.validate_export_payload(
            execution_plan,
            implementation_brief,
        )

        # Build Markdown content
        content = self._build_smoothie_brief(execution_plan, implementation_brief)
        content = self.template_renderer.render(
            content,
            execution_plan,
            implementation_brief,
        )

        # Write to file
        with open(output_path, "w") as f:
            f.write(content)

        return output_path

    def _build_smoothie_brief(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
    ) -> str:
        """Build Smoothie product brief in Markdown."""
        sections = []
        product_surface = brief.get("product_surface")
        surface_kind = self._classify_product_surface(product_surface)

        # Header
        sections.append(f"# {brief['title']}")
        sections.append(f"\n*Product Brief for Smoothie Prototype*\n")

        # Problem & Solution
        sections.append("## Problem")
        sections.append(brief["problem_statement"])

        sections.append("\n## Solution")
        sections.append(brief["mvp_goal"])

        # Target User
        sections.append("\n## Target User")
        if brief.get("target_user"):
            sections.append(brief["target_user"])
        else:
            sections.append("*To be defined*")

        if brief.get("buyer"):
            sections.append(f"\n**Buyer:** {brief['buyer']}")

        # Workflow Context
        if brief.get("workflow_context"):
            sections.append("\n## Workflow Context")
            sections.append(brief["workflow_context"])

        # Product Surface
        if brief.get("product_surface"):
            sections.append(f"\n## Product Surface")
            sections.append(brief["product_surface"])

        # Product surfaces - adapt based on product type
        if surface_kind == "library_api":
            sections.append("\n## API Surfaces to Prototype")
            sections.append(
                "\nBased on the MVP scope, prototype these key library/API surfaces:\n"
            )
            for i, scope_item in enumerate(brief.get("scope", [])[:5], 1):
                sections.append(f"{i}. **{self._scope_to_api_surface(scope_item)}**")
                sections.append(
                    "   - Developer flow: Show how developers initialize, call, and receive output"
                )

        elif surface_kind == "cli":
            sections.append("\n## Commands to Prototype")
            sections.append(
                "\nBased on the MVP scope, prototype these key commands and flags:\n"
            )
            for i, scope_item in enumerate(brief.get("scope", [])[:5], 1):
                sections.append(f"{i}. **{self._scope_to_command(scope_item)}**")
                sections.append("   - Command flow: How users invoke it and get feedback")

        elif surface_kind == "mcp":
            sections.append("\n## Endpoints/Integrations to Prototype")
            sections.append(
                "\nBased on the MVP scope, prototype these key endpoints, tools, or integrations:\n"
            )
            for i, scope_item in enumerate(brief.get("scope", [])[:5], 1):
                sections.append(f"{i}. **{self._scope_to_endpoint(scope_item)}**")
                sections.append(
                    "   - Integration flow: Show request/response handling and service boundaries"
                )

        else:
            # Default to screens for UI/web apps
            sections.append("\n## Screens/Views to Prototype")
            sections.append("\nBased on the MVP scope, prototype these key views:\n")
            for i, scope_item in enumerate(brief.get("scope", [])[:5], 1):
                sections.append(f"{i}. **{self._scope_to_screen(scope_item)}**")
                sections.append(f"   - Purpose: {scope_item}")

        # User Flow - adapt based on product type
        sections.append("\n## Primary User Flow")

        if surface_kind == "library_api":
            sections.append("\n```python")
            sections.append("# Developer integration flow")
            sections.append("1. Developer installs the library or SDK")
            sections.append("2. Developer imports and configures it")
            sections.append("3. Developer calls the primary API surface")
            sections.append("4. Library returns data, errors, or events")
            sections.append("```")

        elif surface_kind == "cli":
            sections.append("\n```bash")
            sections.append("# Command-line workflow")
            sections.append("1. User runs primary command")
            sections.append("2. CLI prompts for input or uses flags")
            sections.append("3. System processes and shows progress")
            sections.append("4. CLI displays results or error")
            sections.append("```")

        elif surface_kind == "mcp":
            sections.append("\n```json")
            sections.append("# MCP/server integration flow")
            sections.append("1. Client connects to the server or integration endpoint")
            sections.append("2. Client requests a tool, resource, or action")
            sections.append("3. Service returns structured response data")
            sections.append("4. Client handles success, errors, or retries")
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

        if surface_kind == "library_api":
            sections.append("\n- **Primary API calls**: Core methods developers will use")
            sections.append("- **Return values**: What data structure is returned")
            sections.append("- **Exception handling**: What errors are raised and when")
            sections.append("- **Configuration options**: How developers customize behavior")

        elif surface_kind == "cli":
            sections.append("\n- **Primary commands**: Main CLI commands users will run")
            sections.append("- **Input/output**: How data flows in and out")
            sections.append("- **Progress feedback**: How users know what's happening")
            sections.append("- **Error messages**: What users see when things fail")

        elif surface_kind == "mcp":
            sections.append("\n- **Endpoints and tools**: Core server actions or integrations")
            sections.append("- **Request/response shape**: What data is sent and returned")
            sections.append("- **Transport and auth**: How clients connect securely")
            sections.append("- **Retries and failures**: What happens when requests fail")

        else:
            sections.append("\n- **Primary action**: Main user goal")
            sections.append("- **Feedback mechanism**: How system responds")
            sections.append("- **Error states**: What happens when things fail")

        # Validation
        sections.append("\n## Validation Questions")
        sections.append("\nPrototype should help answer:")
        sections.append(
            f"\n1. Can users understand {brief.get('product_surface', 'the interface')}?"
        )
        sections.append("2. Is the primary workflow intuitive?")
        sections.append("3. Do users know what to do next at each step?")
        sections.append("4. Are error messages clear and actionable?")

        # Definition of Done
        sections.append("\n## Definition of Done")
        for item in brief.get("definition_of_done", []):
            sections.append(f"- {item}")

        # Non-Goals
        if brief.get("non_goals"):
            sections.append("\n## Out of Scope for Prototype")
            for item in brief["non_goals"][:5]:
                sections.append(f"- {item}")

        # Context
        sections.append("\n---")
        sections.append("\n## Additional Context")
        sections.append(f"\n**Source**: Blueprint implementation brief {brief['id']}")
        sections.append(f"**Execution Plan**: {plan['id']}")
        sections.append(f"\n*Generated by Blueprint for Smoothie prototyping*")

        return "\n".join(sections)

    def _classify_product_surface(self, product_surface: str | None) -> str:
        """Classify a brief surface into a Smoothie-friendly product type."""
        surface = (product_surface or "").lower()

        if any(token in surface for token in ("library", "api", "sdk", "package")):
            return "library_api"
        if any(token in surface for token in ("cli", "command line", "terminal")):
            return "cli"
        if any(token in surface for token in ("mcp", "integration", "server")):
            return "mcp"
        return "web_ui"

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

    def _scope_to_api_surface(self, scope_item: str) -> str:
        """Convert a scope item to an API surface name."""
        item = scope_item.lower()

        if "auth" in item or "login" in item:
            return "Authentication API"
        if "webhook" in item or "event" in item:
            return "Webhook/Event Surface"
        if "config" in item or "setup" in item:
            return "Configuration API"
        if "test" in item:
            return "Test Helper API"
        return scope_item

    def _scope_to_command(self, scope_item: str) -> str:
        """Convert a scope item to a command name."""
        item = scope_item.lower()

        if "test" in item:
            return "test"
        if "config" in item or "setup" in item:
            return "init"
        if "list" in item or "view" in item:
            return "list"
        return scope_item

    def _scope_to_endpoint(self, scope_item: str) -> str:
        """Convert a scope item to an endpoint or integration name."""
        item = scope_item.lower()

        if "auth" in item or "login" in item:
            return "Auth Endpoint"
        if "webhook" in item:
            return "Webhook Endpoint"
        if "tool" in item or "mcp" in item:
            return "Tool Endpoint"
        if "event" in item:
            return "Event Stream"
        return scope_item
