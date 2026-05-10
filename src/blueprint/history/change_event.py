"""Change event model for tracking plan modifications."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


ChangeType = Literal["create", "update", "delete", "restore"]
EntityType = Literal["plan", "task", "milestone", "dependency", "brief"]


class ChangeEvent(BaseModel):
    """Model for tracking changes to plan entities."""

    model_config = ConfigDict(extra="forbid")

    id: str = Field(min_length=1, description="Unique event identifier")
    timestamp: datetime = Field(description="When the change occurred")
    user: str = Field(min_length=1, description="User who made the change")
    change_type: ChangeType = Field(description="Type of change operation")
    entity_type: EntityType = Field(description="Type of entity modified")
    entity_id: str = Field(min_length=1, description="ID of the modified entity")
    old_value: dict[str, Any] | None = Field(
        default=None, description="Entity state before change (for update/delete)"
    )
    new_value: dict[str, Any] | None = Field(
        default=None, description="Entity state after change (for create/update)"
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict, description="Additional context (reason, source, etc.)"
    )

    def get_field_changes(self) -> dict[str, tuple[Any, Any]]:
        """Extract field-level changes for update operations.

        Returns:
            Dict mapping field names to (old_value, new_value) tuples.
        """
        if self.change_type != "update" or not self.old_value or not self.new_value:
            return {}

        changes = {}
        all_keys = set(self.old_value.keys()) | set(self.new_value.keys())

        for key in all_keys:
            old = self.old_value.get(key)
            new = self.new_value.get(key)
            if old != new:
                changes[key] = (old, new)

        return changes

    def to_audit_log_entry(self) -> dict[str, Any]:
        """Convert to audit log format.

        Returns:
            Dict suitable for JSON/CSV export in audit format.
        """
        entry: dict[str, Any] = {
            "event_id": self.id,
            "timestamp": self.timestamp.isoformat(),
            "user": self.user,
            "action": f"{self.change_type}_{self.entity_type}",
            "entity_type": self.entity_type,
            "entity_id": self.entity_id,
        }

        # Add field changes for updates
        if self.change_type == "update":
            entry["changes"] = self.get_field_changes()
        elif self.change_type == "create":
            entry["created_data"] = self.new_value
        elif self.change_type == "delete":
            entry["deleted_data"] = self.old_value

        # Add metadata
        if self.metadata:
            entry["metadata"] = self.metadata

        return entry

    def get_summary(self) -> str:
        """Generate human-readable summary of the change.

        Returns:
            String describing the change.
        """
        action_verbs = {
            "create": "created",
            "update": "updated",
            "delete": "deleted",
            "restore": "restored",
        }
        verb = action_verbs.get(self.change_type, self.change_type)

        summary = f"{self.user} {verb} {self.entity_type} '{self.entity_id}'"

        if self.change_type == "update" and self.old_value and self.new_value:
            field_changes = self.get_field_changes()
            if field_changes:
                fields = ", ".join(field_changes.keys())
                summary += f" (modified: {fields})"

        return summary
