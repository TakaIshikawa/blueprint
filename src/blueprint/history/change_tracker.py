"""Change tracking system with observer pattern and timeline query API."""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Callable, Literal

from blueprint.history.change_event import ChangeEvent, ChangeType, EntityType


class ChangeTracker:
    """Tracks all plan modifications with observer pattern for automatic event capture."""

    def __init__(self) -> None:
        """Initialize change tracker with empty event store and observers."""
        self._events: list[ChangeEvent] = []
        self._observers: dict[tuple[ChangeType, EntityType], list[Callable]] = defaultdict(list)
        self._next_event_id = 1

    def register_observer(
        self,
        change_type: ChangeType,
        entity_type: EntityType,
        callback: Callable[[ChangeEvent], None],
    ) -> None:
        """Register observer for automatic event notifications.

        Args:
            change_type: Type of change to observe
            entity_type: Type of entity to observe
            callback: Function to call when matching event occurs
        """
        self._observers[(change_type, entity_type)].append(callback)

    def unregister_observer(
        self,
        change_type: ChangeType,
        entity_type: EntityType,
        callback: Callable[[ChangeEvent], None],
    ) -> None:
        """Unregister an observer callback.

        Args:
            change_type: Type of change
            entity_type: Type of entity
            callback: Callback to remove
        """
        key = (change_type, entity_type)
        if key in self._observers and callback in self._observers[key]:
            self._observers[key].remove(callback)

    def track_create(
        self,
        entity_type: EntityType,
        entity_id: str,
        new_value: dict[str, Any],
        user: str,
        metadata: dict[str, Any] | None = None,
    ) -> ChangeEvent:
        """Track creation of a new entity.

        Args:
            entity_type: Type of entity created
            entity_id: ID of created entity
            new_value: Entity data
            user: User who created the entity
            metadata: Additional context

        Returns:
            Created ChangeEvent
        """
        return self._record_event(
            change_type="create",
            entity_type=entity_type,
            entity_id=entity_id,
            old_value=None,
            new_value=new_value,
            user=user,
            metadata=metadata or {},
        )

    def track_update(
        self,
        entity_type: EntityType,
        entity_id: str,
        old_value: dict[str, Any],
        new_value: dict[str, Any],
        user: str,
        metadata: dict[str, Any] | None = None,
    ) -> ChangeEvent:
        """Track update of an existing entity.

        Args:
            entity_type: Type of entity updated
            entity_id: ID of updated entity
            old_value: Entity state before update
            new_value: Entity state after update
            user: User who updated the entity
            metadata: Additional context

        Returns:
            Created ChangeEvent
        """
        return self._record_event(
            change_type="update",
            entity_type=entity_type,
            entity_id=entity_id,
            old_value=old_value,
            new_value=new_value,
            user=user,
            metadata=metadata or {},
        )

    def track_delete(
        self,
        entity_type: EntityType,
        entity_id: str,
        old_value: dict[str, Any],
        user: str,
        metadata: dict[str, Any] | None = None,
    ) -> ChangeEvent:
        """Track deletion of an entity.

        Args:
            entity_type: Type of entity deleted
            entity_id: ID of deleted entity
            old_value: Entity state before deletion
            user: User who deleted the entity
            metadata: Additional context

        Returns:
            Created ChangeEvent
        """
        return self._record_event(
            change_type="delete",
            entity_type=entity_type,
            entity_id=entity_id,
            old_value=old_value,
            new_value=None,
            user=user,
            metadata=metadata or {},
        )

    def track_restore(
        self,
        entity_type: EntityType,
        entity_id: str,
        new_value: dict[str, Any],
        user: str,
        metadata: dict[str, Any] | None = None,
    ) -> ChangeEvent:
        """Track restoration of a deleted entity.

        Args:
            entity_type: Type of entity restored
            entity_id: ID of restored entity
            new_value: Restored entity data
            user: User who restored the entity
            metadata: Additional context

        Returns:
            Created ChangeEvent
        """
        return self._record_event(
            change_type="restore",
            entity_type=entity_type,
            entity_id=entity_id,
            old_value=None,
            new_value=new_value,
            user=user,
            metadata=metadata or {},
        )

    def _record_event(
        self,
        change_type: ChangeType,
        entity_type: EntityType,
        entity_id: str,
        old_value: dict[str, Any] | None,
        new_value: dict[str, Any] | None,
        user: str,
        metadata: dict[str, Any],
    ) -> ChangeEvent:
        """Internal method to record and notify observers of an event.

        Args:
            change_type: Type of change
            entity_type: Type of entity
            entity_id: Entity ID
            old_value: Old state
            new_value: New state
            user: User making change
            metadata: Additional context

        Returns:
            Created ChangeEvent
        """
        event = ChangeEvent(
            id=f"evt_{self._next_event_id:08d}",
            timestamp=datetime.now(timezone.utc),
            user=user,
            change_type=change_type,
            entity_type=entity_type,
            entity_id=entity_id,
            old_value=old_value,
            new_value=new_value,
            metadata=metadata,
        )
        self._next_event_id += 1
        self._events.append(event)

        # Notify observers
        observers = self._observers.get((change_type, entity_type), [])
        for callback in observers:
            callback(event)

        return event

    def query_timeline(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        entity_type: EntityType | None = None,
        entity_id: str | None = None,
        user: str | None = None,
        change_type: ChangeType | None = None,
        limit: int | None = None,
    ) -> list[ChangeEvent]:
        """Query timeline of events with filtering.

        Args:
            start_date: Filter events after this date (inclusive)
            end_date: Filter events before this date (inclusive)
            entity_type: Filter by entity type
            entity_id: Filter by specific entity ID
            user: Filter by user
            change_type: Filter by change type
            limit: Maximum number of events to return (most recent first)

        Returns:
            List of matching events, sorted by timestamp descending
        """
        filtered = self._events

        if start_date:
            filtered = [e for e in filtered if e.timestamp >= start_date]

        if end_date:
            filtered = [e for e in filtered if e.timestamp <= end_date]

        if entity_type:
            filtered = [e for e in filtered if e.entity_type == entity_type]

        if entity_id:
            filtered = [e for e in filtered if e.entity_id == entity_id]

        if user:
            filtered = [e for e in filtered if e.user == user]

        if change_type:
            filtered = [e for e in filtered if e.change_type == change_type]

        # Sort by timestamp descending (most recent first)
        filtered = sorted(filtered, key=lambda e: e.timestamp, reverse=True)

        if limit:
            filtered = filtered[:limit]

        return filtered

    def generate_diff(
        self, entity_id: str, timestamp1: datetime, timestamp2: datetime
    ) -> dict[str, Any]:
        """Generate diff comparing entity state at two points in time.

        Args:
            entity_id: ID of entity to compare
            timestamp1: Earlier timestamp
            timestamp2: Later timestamp

        Returns:
            Dict with 'state_at_timestamp1', 'state_at_timestamp2', and 'changes'
        """
        # Get state at timestamp1
        state1 = self._get_entity_state_at(entity_id, timestamp1)

        # Get state at timestamp2
        state2 = self._get_entity_state_at(entity_id, timestamp2)

        # Compute field-level changes
        changes = {}
        if state1 and state2:
            all_keys = set(state1.keys()) | set(state2.keys())
            for key in all_keys:
                val1 = state1.get(key)
                val2 = state2.get(key)
                if val1 != val2:
                    changes[key] = {"from": val1, "to": val2}

        return {
            "entity_id": entity_id,
            "timestamp1": timestamp1.isoformat(),
            "timestamp2": timestamp2.isoformat(),
            "state_at_timestamp1": state1,
            "state_at_timestamp2": state2,
            "changes": changes,
        }

    def _get_entity_state_at(self, entity_id: str, timestamp: datetime) -> dict[str, Any] | None:
        """Reconstruct entity state at a specific point in time.

        Args:
            entity_id: Entity ID
            timestamp: Point in time

        Returns:
            Entity state dict, or None if entity didn't exist
        """
        # Get all events for this entity up to timestamp, sorted by time
        events = [
            e for e in self._events if e.entity_id == entity_id and e.timestamp <= timestamp
        ]
        events = sorted(events, key=lambda e: e.timestamp)

        if not events:
            return None

        # Replay events to reconstruct state
        state = None
        for event in events:
            if event.change_type == "create" or event.change_type == "restore":
                state = event.new_value.copy() if event.new_value else {}
            elif event.change_type == "update":
                if state is not None and event.new_value:
                    state.update(event.new_value)
            elif event.change_type == "delete":
                state = None

        return state

    def rollback_to(
        self,
        entity_id: str,
        timestamp: datetime,
        user: str,
        metadata: dict[str, Any] | None = None,
    ) -> ChangeEvent | None:
        """Rollback entity to its state at a specific point in time.

        Args:
            entity_id: Entity to rollback
            timestamp: Target timestamp
            user: User performing rollback
            metadata: Additional context

        Returns:
            ChangeEvent for the rollback operation, or None if entity didn't exist at timestamp
        """
        # Get target state
        target_state = self._get_entity_state_at(entity_id, timestamp)

        if target_state is None:
            return None

        # Get current state
        current_state = self._get_entity_state_at(entity_id, datetime.now(timezone.utc))

        # Determine entity type from most recent event
        entity_events = [e for e in self._events if e.entity_id == entity_id]
        if not entity_events:
            return None
        entity_type = entity_events[-1].entity_type

        # Create rollback metadata
        rollback_metadata = {
            "reason": "rollback",
            "target_timestamp": timestamp.isoformat(),
            **(metadata or {}),
        }

        # Record rollback as update or restore
        if current_state is None:
            # Entity was deleted, restore it
            return self.track_restore(
                entity_type=entity_type,
                entity_id=entity_id,
                new_value=target_state,
                user=user,
                metadata=rollback_metadata,
            )
        else:
            # Entity exists, update it
            return self.track_update(
                entity_type=entity_type,
                entity_id=entity_id,
                old_value=current_state,
                new_value=target_state,
                user=user,
                metadata=rollback_metadata,
            )

    def aggregate_daily_activity(
        self, start_date: datetime, end_date: datetime
    ) -> dict[str, dict[str, int]]:
        """Aggregate change activity by day.

        Args:
            start_date: Start of date range
            end_date: End of date range

        Returns:
            Dict mapping date strings to activity counts by change type
        """
        daily_activity: dict[str, dict[str, int]] = defaultdict(
            lambda: defaultdict(int)
        )

        events = self.query_timeline(start_date=start_date, end_date=end_date)

        for event in events:
            date_key = event.timestamp.date().isoformat()
            daily_activity[date_key][event.change_type] += 1

        return dict(daily_activity)

    def aggregate_weekly_report(
        self, start_date: datetime, end_date: datetime
    ) -> dict[str, Any]:
        """Generate weekly activity report.

        Args:
            start_date: Start of week
            end_date: End of week

        Returns:
            Dict with total changes, changes by type, changes by user, most active entities
        """
        events = self.query_timeline(start_date=start_date, end_date=end_date)

        by_type: dict[str, int] = defaultdict(int)
        by_user: dict[str, int] = defaultdict(int)
        by_entity: dict[str, int] = defaultdict(int)

        for event in events:
            by_type[event.change_type] += 1
            by_user[event.user] += 1
            by_entity[event.entity_id] += 1

        # Find most active entities
        most_active = sorted(by_entity.items(), key=lambda x: x[1], reverse=True)[:10]

        return {
            "period": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
            },
            "total_changes": len(events),
            "changes_by_type": dict(by_type),
            "changes_by_user": dict(by_user),
            "most_active_entities": [
                {"entity_id": entity_id, "changes": count}
                for entity_id, count in most_active
            ],
        }

    def export_audit_log(
        self,
        format: Literal["json", "csv"],
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        entity_type: EntityType | None = None,
    ) -> str:
        """Export change history to audit log format.

        Args:
            format: Export format (json or csv)
            start_date: Optional start date filter
            end_date: Optional end date filter
            entity_type: Optional entity type filter

        Returns:
            Formatted audit log string
        """
        events = self.query_timeline(
            start_date=start_date, end_date=end_date, entity_type=entity_type
        )

        audit_entries = [event.to_audit_log_entry() for event in events]

        if format == "json":
            return json.dumps(audit_entries, indent=2, default=str)
        elif format == "csv":
            if not audit_entries:
                return ""

            # Flatten nested changes for CSV
            flattened = []
            for entry in audit_entries:
                flat_entry = {
                    "event_id": entry["event_id"],
                    "timestamp": entry["timestamp"],
                    "user": entry["user"],
                    "action": entry["action"],
                    "entity_type": entry["entity_type"],
                    "entity_id": entry["entity_id"],
                }

                # Add change details as JSON strings
                if "changes" in entry:
                    flat_entry["changes"] = json.dumps(entry["changes"])
                if "created_data" in entry:
                    flat_entry["created_data"] = json.dumps(entry["created_data"])
                if "deleted_data" in entry:
                    flat_entry["deleted_data"] = json.dumps(entry["deleted_data"])
                if "metadata" in entry:
                    flat_entry["metadata"] = json.dumps(entry["metadata"])

                flattened.append(flat_entry)

            # Write CSV
            from io import StringIO

            output = StringIO()
            if flattened:
                fieldnames = list(flattened[0].keys())
                writer = csv.DictWriter(output, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(flattened)

            return output.getvalue()

        # Should not reach here given Literal type, but satisfy type checker
        return ""

    def search_events(
        self,
        search_term: str,
        search_in: Literal["entity_id", "user", "metadata", "all"] = "all",
    ) -> list[ChangeEvent]:
        """Search for events matching a term.

        Args:
            search_term: Term to search for
            search_in: Where to search (entity_id, user, metadata, or all)

        Returns:
            List of matching events
        """
        results = []
        search_lower = search_term.lower()

        for event in self._events:
            if search_in in ("entity_id", "all"):
                if search_lower in event.entity_id.lower():
                    results.append(event)
                    continue

            if search_in in ("user", "all"):
                if search_lower in event.user.lower():
                    results.append(event)
                    continue

            if search_in in ("metadata", "all"):
                metadata_str = json.dumps(event.metadata).lower()
                if search_lower in metadata_str:
                    results.append(event)
                    continue

        return results

    def get_entity_history(self, entity_id: str) -> list[ChangeEvent]:
        """Get complete change history for a specific entity.

        Args:
            entity_id: Entity ID

        Returns:
            List of all events for this entity, sorted by timestamp
        """
        events = [e for e in self._events if e.entity_id == entity_id]
        return sorted(events, key=lambda e: e.timestamp)
