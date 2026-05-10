"""Plan version model for semantic versioning."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class PlanVersion(BaseModel):
    """Model representing a specific version of a plan."""

    model_config = ConfigDict(extra="forbid")

    id: str = Field(min_length=1, description="Unique version identifier")
    plan_id: str = Field(min_length=1, description="ID of the plan being versioned")
    version_number: str = Field(
        min_length=1, pattern=r"^\d+\.\d+\.\d+$", description="Semantic version (major.minor.patch)"
    )
    created_at: datetime = Field(description="When this version was created")
    author: str = Field(min_length=1, description="User who created this version")
    description: str = Field(default="", description="Version description or change summary")
    snapshot_data: dict[str, Any] = Field(description="Complete plan snapshot at this version")
    parent_version: str | None = Field(
        default=None, description="Parent version ID for branching history"
    )
    tags: list[str] = Field(
        default_factory=list, description="Tags marking important versions (e.g., 'approved', 'final')"
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict, description="Additional metadata (milestone, trigger, etc.)"
    )

    def get_version_parts(self) -> tuple[int, int, int]:
        """Parse version number into major, minor, patch components.

        Returns:
            Tuple of (major, minor, patch) as integers
        """
        parts = self.version_number.split(".")
        return int(parts[0]), int(parts[1]), int(parts[2])

    def is_major_version(self) -> bool:
        """Check if this is a major version (x.0.0).

        Returns:
            True if minor and patch are both 0
        """
        major, minor, patch = self.get_version_parts()
        return minor == 0 and patch == 0

    def is_tagged(self, tag: str) -> bool:
        """Check if version has a specific tag.

        Args:
            tag: Tag to check for

        Returns:
            True if tag is present
        """
        return tag in self.tags

    def add_tag(self, tag: str) -> None:
        """Add a tag to this version.

        Args:
            tag: Tag to add
        """
        if tag not in self.tags:
            self.tags.append(tag)

    def remove_tag(self, tag: str) -> None:
        """Remove a tag from this version.

        Args:
            tag: Tag to remove
        """
        if tag in self.tags:
            self.tags.remove(tag)
