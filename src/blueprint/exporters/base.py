"""Base exporter interface for execution engines."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any


class TargetExporter(ABC):
    """Abstract base class for target engine exporters."""

    @abstractmethod
    def export(
        self,
        execution_plan: dict[str, Any],
        implementation_brief: dict[str, Any],
        output_path: str,
    ) -> str:
        """
        Export an execution plan to target engine format.

        Args:
            execution_plan: Execution plan dictionary from database
            implementation_brief: Implementation brief dictionary from database
            output_path: Path to write export file

        Returns:
            Path to exported file

        Raises:
            ExportError: If export fails
        """
        pass

    @abstractmethod
    def get_format(self) -> str:
        """Get export format (json, yaml, markdown)."""
        pass

    @abstractmethod
    def get_extension(self) -> str:
        """Get file extension for exports."""
        pass

    def ensure_output_dir(self, output_path: str) -> None:
        """Ensure output directory exists."""
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
