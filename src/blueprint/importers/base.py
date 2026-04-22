"""Base importer interface for design brief sources."""

from abc import ABC, abstractmethod
from typing import Any


class SourceImporter(ABC):
    """Abstract base class for source importers."""

    @abstractmethod
    def import_from_source(self, source_id: str) -> dict[str, Any]:
        """
        Import a design brief from source system.

        Args:
            source_id: Identifier in the source system

        Returns:
            Dictionary representing a SourceBrief

        Raises:
            ImportError: If source cannot be found or read
        """
        pass

    @abstractmethod
    def validate_source(self, source_id: str) -> bool:
        """
        Check if a source exists and is accessible.

        Args:
            source_id: Identifier in the source system

        Returns:
            True if source exists and can be imported
        """
        pass

    @abstractmethod
    def list_available(self, limit: int = 50) -> list[dict[str, Any]]:
        """
        List available items from source system.

        Args:
            limit: Maximum number of items to return

        Returns:
            List of dictionaries with id, title, and metadata
        """
        pass
