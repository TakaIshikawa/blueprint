"""Relay YAML exporter - human-editable Relay task graph format."""

from typing import Any

import yaml

from blueprint.exporters.relay import RelayExporter


class RelayYamlExporter(RelayExporter):
    """Export execution plans to Relay-compatible YAML task graphs."""

    def get_format(self) -> str:
        """Get export format."""
        return "yaml"

    def get_extension(self) -> str:
        """Get file extension."""
        return ".yaml"

    def export(
        self,
        execution_plan: dict[str, Any],
        implementation_brief: dict[str, Any],
        output_path: str,
    ) -> str:
        """Export to Relay YAML format."""
        self.ensure_output_dir(output_path)
        execution_plan, implementation_brief = self.validate_export_payload(
            execution_plan,
            implementation_brief,
        )
        relay_export = self.render_payload(execution_plan, implementation_brief)

        with open(output_path, "w") as f:
            yaml.safe_dump(
                relay_export,
                f,
                allow_unicode=False,
                sort_keys=False,
            )

        return output_path
