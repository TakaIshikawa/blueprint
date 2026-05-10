"""Tests for change tracking system."""

import json
from datetime import datetime, timedelta, timezone

from blueprint.history.change_event import ChangeEvent
from blueprint.history.change_tracker import ChangeTracker


class TestChangeEvent:
    """Test ChangeEvent model."""

    def test_create_event(self):
        """Test creating a change event."""
        timestamp = datetime.now(timezone.utc)
        event = ChangeEvent(
            id="evt_001",
            timestamp=timestamp,
            user="test_user",
            change_type="create",
            entity_type="plan",
            entity_id="plan_123",
            new_value={"title": "Test Plan", "status": "draft"},
            metadata={"source": "api"},
        )

        assert event.id == "evt_001"
        assert event.timestamp == timestamp
        assert event.user == "test_user"
        assert event.change_type == "create"
        assert event.entity_type == "plan"
        assert event.entity_id == "plan_123"
        assert event.new_value == {"title": "Test Plan", "status": "draft"}
        assert event.metadata == {"source": "api"}

    def test_get_field_changes(self):
        """Test extracting field-level changes."""
        event = ChangeEvent(
            id="evt_002",
            timestamp=datetime.now(timezone.utc),
            user="test_user",
            change_type="update",
            entity_type="task",
            entity_id="task_456",
            old_value={"title": "Old Title", "status": "pending", "priority": "low"},
            new_value={"title": "New Title", "status": "in_progress", "priority": "low"},
        )

        changes = event.get_field_changes()
        assert len(changes) == 2
        assert changes["title"] == ("Old Title", "New Title")
        assert changes["status"] == ("pending", "in_progress")
        assert "priority" not in changes

    def test_get_field_changes_non_update(self):
        """Test get_field_changes returns empty dict for non-update events."""
        event = ChangeEvent(
            id="evt_003",
            timestamp=datetime.now(timezone.utc),
            user="test_user",
            change_type="create",
            entity_type="plan",
            entity_id="plan_789",
            new_value={"title": "Test"},
        )

        assert event.get_field_changes() == {}

    def test_to_audit_log_entry(self):
        """Test converting to audit log format."""
        timestamp = datetime.now(timezone.utc)
        event = ChangeEvent(
            id="evt_004",
            timestamp=timestamp,
            user="admin",
            change_type="update",
            entity_type="plan",
            entity_id="plan_001",
            old_value={"status": "draft"},
            new_value={"status": "ready"},
            metadata={"reason": "approval"},
        )

        entry = event.to_audit_log_entry()

        assert entry["event_id"] == "evt_004"
        assert entry["timestamp"] == timestamp.isoformat()
        assert entry["user"] == "admin"
        assert entry["action"] == "update_plan"
        assert entry["entity_type"] == "plan"
        assert entry["entity_id"] == "plan_001"
        assert "changes" in entry
        assert entry["metadata"] == {"reason": "approval"}

    def test_get_summary(self):
        """Test generating human-readable summary."""
        event = ChangeEvent(
            id="evt_005",
            timestamp=datetime.now(timezone.utc),
            user="alice",
            change_type="update",
            entity_type="task",
            entity_id="task_123",
            old_value={"status": "pending"},
            new_value={"status": "completed"},
        )

        summary = event.get_summary()
        assert "alice" in summary
        assert "updated" in summary
        assert "task" in summary
        assert "task_123" in summary
        assert "status" in summary


class TestChangeTracker:
    """Test ChangeTracker functionality."""

    def test_track_create(self):
        """Test tracking entity creation."""
        tracker = ChangeTracker()

        event = tracker.track_create(
            entity_type="plan",
            entity_id="plan_001",
            new_value={"title": "Test Plan", "status": "draft"},
            user="alice",
            metadata={"source": "api"},
        )

        assert event.change_type == "create"
        assert event.entity_type == "plan"
        assert event.entity_id == "plan_001"
        assert event.user == "alice"
        assert event.new_value == {"title": "Test Plan", "status": "draft"}
        assert event.old_value is None

    def test_track_update(self):
        """Test tracking entity updates."""
        tracker = ChangeTracker()

        event = tracker.track_update(
            entity_type="task",
            entity_id="task_001",
            old_value={"status": "pending"},
            new_value={"status": "in_progress"},
            user="bob",
        )

        assert event.change_type == "update"
        assert event.entity_type == "task"
        assert event.entity_id == "task_001"
        assert event.user == "bob"
        assert event.old_value == {"status": "pending"}
        assert event.new_value == {"status": "in_progress"}

    def test_track_delete(self):
        """Test tracking entity deletion."""
        tracker = ChangeTracker()

        event = tracker.track_delete(
            entity_type="milestone",
            entity_id="milestone_001",
            old_value={"name": "Phase 1", "date": "2024-01-01"},
            user="admin",
            metadata={"reason": "cancelled"},
        )

        assert event.change_type == "delete"
        assert event.entity_type == "milestone"
        assert event.entity_id == "milestone_001"
        assert event.old_value == {"name": "Phase 1", "date": "2024-01-01"}
        assert event.new_value is None
        assert event.metadata["reason"] == "cancelled"

    def test_track_restore(self):
        """Test tracking entity restoration."""
        tracker = ChangeTracker()

        event = tracker.track_restore(
            entity_type="task",
            entity_id="task_002",
            new_value={"title": "Restored Task", "status": "pending"},
            user="alice",
        )

        assert event.change_type == "restore"
        assert event.entity_type == "task"
        assert event.entity_id == "task_002"
        assert event.new_value == {"title": "Restored Task", "status": "pending"}
        assert event.old_value is None

    def test_observer_pattern(self):
        """Test observer registration and notification."""
        tracker = ChangeTracker()
        notifications = []

        def observer(event: ChangeEvent) -> None:
            notifications.append(event)

        # Register observer
        tracker.register_observer("create", "plan", observer)

        # Trigger event
        event = tracker.track_create(
            entity_type="plan",
            entity_id="plan_002",
            new_value={"title": "Observed Plan"},
            user="alice",
        )

        # Verify notification
        assert len(notifications) == 1
        assert notifications[0] == event

    def test_unregister_observer(self):
        """Test unregistering observers."""
        tracker = ChangeTracker()
        notifications = []

        def observer(event: ChangeEvent) -> None:
            notifications.append(event)

        tracker.register_observer("create", "plan", observer)
        tracker.unregister_observer("create", "plan", observer)

        # Trigger event
        tracker.track_create(
            entity_type="plan",
            entity_id="plan_003",
            new_value={"title": "Not Observed"},
            user="alice",
        )

        # Verify no notification
        assert len(notifications) == 0

    def test_query_timeline_all(self):
        """Test querying all events."""
        tracker = ChangeTracker()

        tracker.track_create("plan", "plan_001", {"title": "Plan 1"}, "alice")
        tracker.track_create("task", "task_001", {"title": "Task 1"}, "bob")
        tracker.track_update("plan", "plan_001", {"status": "draft"}, {"status": "ready"}, "alice")

        events = tracker.query_timeline()
        assert len(events) == 3

    def test_query_timeline_by_entity_type(self):
        """Test filtering by entity type."""
        tracker = ChangeTracker()

        tracker.track_create("plan", "plan_001", {"title": "Plan 1"}, "alice")
        tracker.track_create("task", "task_001", {"title": "Task 1"}, "bob")
        tracker.track_create("plan", "plan_002", {"title": "Plan 2"}, "alice")

        events = tracker.query_timeline(entity_type="plan")
        assert len(events) == 2
        assert all(e.entity_type == "plan" for e in events)

    def test_query_timeline_by_entity_id(self):
        """Test filtering by entity ID."""
        tracker = ChangeTracker()

        tracker.track_create("plan", "plan_001", {"title": "Plan 1"}, "alice")
        tracker.track_update("plan", "plan_001", {"status": "draft"}, {"status": "ready"}, "alice")
        tracker.track_create("plan", "plan_002", {"title": "Plan 2"}, "bob")

        events = tracker.query_timeline(entity_id="plan_001")
        assert len(events) == 2
        assert all(e.entity_id == "plan_001" for e in events)

    def test_query_timeline_by_user(self):
        """Test filtering by user."""
        tracker = ChangeTracker()

        tracker.track_create("plan", "plan_001", {"title": "Plan 1"}, "alice")
        tracker.track_create("task", "task_001", {"title": "Task 1"}, "bob")
        tracker.track_create("plan", "plan_002", {"title": "Plan 2"}, "alice")

        events = tracker.query_timeline(user="alice")
        assert len(events) == 2
        assert all(e.user == "alice" for e in events)

    def test_query_timeline_by_change_type(self):
        """Test filtering by change type."""
        tracker = ChangeTracker()

        tracker.track_create("plan", "plan_001", {"title": "Plan 1"}, "alice")
        tracker.track_update("plan", "plan_001", {"status": "draft"}, {"status": "ready"}, "alice")
        tracker.track_delete("plan", "plan_001", {"title": "Plan 1"}, "alice")

        events = tracker.query_timeline(change_type="update")
        assert len(events) == 1
        assert events[0].change_type == "update"

    def test_query_timeline_by_date_range(self):
        """Test filtering by date range."""
        tracker = ChangeTracker()

        now = datetime.now(timezone.utc)
        yesterday = now - timedelta(days=1)

        # Manually create events with specific timestamps
        event1 = ChangeEvent(
            id="evt_001",
            timestamp=yesterday,
            user="alice",
            change_type="create",
            entity_type="plan",
            entity_id="plan_001",
            new_value={"title": "Old Plan"},
        )
        event2 = ChangeEvent(
            id="evt_002",
            timestamp=now,
            user="alice",
            change_type="create",
            entity_type="plan",
            entity_id="plan_002",
            new_value={"title": "Current Plan"},
        )
        tracker._events = [event1, event2]

        # Query events from today onwards
        events = tracker.query_timeline(start_date=now - timedelta(hours=1))
        assert len(events) == 1
        assert events[0].entity_id == "plan_002"

        # Query events up to yesterday
        events = tracker.query_timeline(end_date=now - timedelta(hours=1))
        assert len(events) == 1
        assert events[0].entity_id == "plan_001"

    def test_query_timeline_with_limit(self):
        """Test limiting result count."""
        tracker = ChangeTracker()

        for i in range(10):
            tracker.track_create("plan", f"plan_{i:03d}", {"title": f"Plan {i}"}, "alice")

        events = tracker.query_timeline(limit=5)
        assert len(events) == 5

    def test_generate_diff(self):
        """Test generating diff between two time points."""
        tracker = ChangeTracker()

        now = datetime.now(timezone.utc)
        time1 = now - timedelta(hours=2)
        time2 = now - timedelta(hours=1)

        # Create entity
        event1 = ChangeEvent(
            id="evt_001",
            timestamp=time1,
            user="alice",
            change_type="create",
            entity_type="plan",
            entity_id="plan_001",
            new_value={"title": "Original Title", "status": "draft", "priority": "low"},
        )

        # Update entity
        event2 = ChangeEvent(
            id="evt_002",
            timestamp=time2,
            user="alice",
            change_type="update",
            entity_type="plan",
            entity_id="plan_001",
            old_value={"title": "Original Title", "status": "draft", "priority": "low"},
            new_value={"title": "Updated Title", "status": "ready", "priority": "low"},
        )

        tracker._events = [event1, event2]

        diff = tracker.generate_diff("plan_001", time1, time2)

        assert diff["entity_id"] == "plan_001"
        assert "changes" in diff
        assert "title" in diff["changes"]
        assert diff["changes"]["title"]["from"] == "Original Title"
        assert diff["changes"]["title"]["to"] == "Updated Title"
        assert "status" in diff["changes"]
        assert diff["changes"]["status"]["from"] == "draft"
        assert diff["changes"]["status"]["to"] == "ready"
        assert "priority" not in diff["changes"]

    def test_rollback_to_previous_state(self):
        """Test rolling back entity to previous state."""
        tracker = ChangeTracker()

        now = datetime.now(timezone.utc)
        time1 = now - timedelta(hours=2)
        time2 = now - timedelta(hours=1)

        # Create entity
        event1 = ChangeEvent(
            id="evt_001",
            timestamp=time1,
            user="alice",
            change_type="create",
            entity_type="plan",
            entity_id="plan_001",
            new_value={"title": "Original", "status": "draft"},
        )

        # Update entity
        event2 = ChangeEvent(
            id="evt_002",
            timestamp=time2,
            user="alice",
            change_type="update",
            entity_type="plan",
            entity_id="plan_001",
            old_value={"title": "Original", "status": "draft"},
            new_value={"title": "Modified", "status": "ready"},
        )

        tracker._events = [event1, event2]

        # Rollback to time1
        rollback_event = tracker.rollback_to("plan_001", time1, "admin")

        assert rollback_event is not None
        assert rollback_event.change_type == "update"
        assert rollback_event.new_value == {"title": "Original", "status": "draft"}
        assert rollback_event.metadata["reason"] == "rollback"

    def test_rollback_deleted_entity(self):
        """Test rolling back a deleted entity."""
        tracker = ChangeTracker()

        now = datetime.now(timezone.utc)
        time1 = now - timedelta(hours=2)
        time2 = now - timedelta(hours=1)

        # Create entity
        event1 = ChangeEvent(
            id="evt_001",
            timestamp=time1,
            user="alice",
            change_type="create",
            entity_type="task",
            entity_id="task_001",
            new_value={"title": "Task 1", "status": "pending"},
        )

        # Delete entity
        event2 = ChangeEvent(
            id="evt_002",
            timestamp=time2,
            user="alice",
            change_type="delete",
            entity_type="task",
            entity_id="task_001",
            old_value={"title": "Task 1", "status": "pending"},
        )

        tracker._events = [event1, event2]

        # Rollback to before deletion
        rollback_event = tracker.rollback_to("task_001", time1, "admin")

        assert rollback_event is not None
        assert rollback_event.change_type == "restore"
        assert rollback_event.new_value == {"title": "Task 1", "status": "pending"}

    def test_rollback_nonexistent_entity(self):
        """Test rollback returns None for entity that didn't exist."""
        tracker = ChangeTracker()

        now = datetime.now(timezone.utc)
        result = tracker.rollback_to("nonexistent", now, "admin")

        assert result is None

    def test_aggregate_daily_activity(self):
        """Test aggregating activity by day."""
        tracker = ChangeTracker()

        now = datetime.now(timezone.utc)
        today = now.replace(hour=12, minute=0, second=0, microsecond=0)
        yesterday = today - timedelta(days=1)

        # Create events on different days
        event1 = ChangeEvent(
            id="evt_001",
            timestamp=yesterday,
            user="alice",
            change_type="create",
            entity_type="plan",
            entity_id="plan_001",
            new_value={},
        )
        event2 = ChangeEvent(
            id="evt_002",
            timestamp=yesterday,
            user="alice",
            change_type="update",
            entity_type="plan",
            entity_id="plan_001",
            old_value={},
            new_value={},
        )
        event3 = ChangeEvent(
            id="evt_003",
            timestamp=today,
            user="bob",
            change_type="create",
            entity_type="task",
            entity_id="task_001",
            new_value={},
        )

        tracker._events = [event1, event2, event3]

        # Aggregate activity
        start = yesterday - timedelta(hours=1)
        end = today + timedelta(hours=1)
        activity = tracker.aggregate_daily_activity(start, end)

        yesterday_key = yesterday.date().isoformat()
        today_key = today.date().isoformat()

        assert yesterday_key in activity
        assert activity[yesterday_key]["create"] == 1
        assert activity[yesterday_key]["update"] == 1
        assert today_key in activity
        assert activity[today_key]["create"] == 1

    def test_aggregate_weekly_report(self):
        """Test generating weekly activity report."""
        tracker = ChangeTracker()

        # Create various events
        tracker.track_create("plan", "plan_001", {}, "alice")
        tracker.track_create("task", "task_001", {}, "bob")
        tracker.track_update("plan", "plan_001", {}, {}, "alice")
        tracker.track_update("plan", "plan_001", {}, {}, "alice")

        now = datetime.now(timezone.utc)
        start = now - timedelta(days=7)
        end = now + timedelta(hours=1)  # Include events just created

        report = tracker.aggregate_weekly_report(start, end)

        assert report["total_changes"] == 4
        assert report["changes_by_type"]["create"] == 2
        assert report["changes_by_type"]["update"] == 2
        assert report["changes_by_user"]["alice"] == 3
        assert report["changes_by_user"]["bob"] == 1
        assert len(report["most_active_entities"]) > 0

    def test_export_audit_log_json(self):
        """Test exporting audit log as JSON."""
        tracker = ChangeTracker()

        tracker.track_create("plan", "plan_001", {"title": "Test Plan"}, "alice")
        tracker.track_update(
            "plan", "plan_001", {"status": "draft"}, {"status": "ready"}, "alice"
        )

        json_export = tracker.export_audit_log(format="json")

        # Verify it's valid JSON
        data = json.loads(json_export)
        assert isinstance(data, list)
        assert len(data) == 2

        # Verify content (most recent first)
        assert data[0]["action"] == "update_plan"
        assert data[1]["action"] == "create_plan"

    def test_export_audit_log_csv(self):
        """Test exporting audit log as CSV."""
        tracker = ChangeTracker()

        tracker.track_create("plan", "plan_001", {"title": "Test Plan"}, "alice")

        csv_export = tracker.export_audit_log(format="csv")

        # Verify CSV structure
        lines = csv_export.strip().split("\n")
        assert len(lines) >= 2  # Header + at least one row
        assert "event_id" in lines[0]
        assert "timestamp" in lines[0]
        assert "user" in lines[0]

    def test_export_audit_log_with_filters(self):
        """Test exporting audit log with filters."""
        tracker = ChangeTracker()

        tracker.track_create("plan", "plan_001", {}, "alice")
        tracker.track_create("task", "task_001", {}, "bob")

        # Export only plan events
        json_export = tracker.export_audit_log(format="json", entity_type="plan")

        data = json.loads(json_export)
        assert len(data) == 1
        assert data[0]["entity_type"] == "plan"

    def test_search_events_by_entity_id(self):
        """Test searching events by entity ID."""
        tracker = ChangeTracker()

        tracker.track_create("plan", "plan_special_001", {}, "alice")
        tracker.track_create("plan", "plan_002", {}, "bob")
        tracker.track_create("task", "task_special_003", {}, "alice")

        results = tracker.search_events("special", search_in="entity_id")
        assert len(results) == 2
        assert all("special" in e.entity_id for e in results)

    def test_search_events_by_user(self):
        """Test searching events by user."""
        tracker = ChangeTracker()

        tracker.track_create("plan", "plan_001", {}, "alice_admin")
        tracker.track_create("task", "task_001", {}, "bob")
        tracker.track_create("plan", "plan_002", {}, "alice_dev")

        results = tracker.search_events("alice", search_in="user")
        assert len(results) == 2
        assert all("alice" in e.user for e in results)

    def test_search_events_by_metadata(self):
        """Test searching events by metadata."""
        tracker = ChangeTracker()

        tracker.track_create("plan", "plan_001", {}, "alice", metadata={"source": "api"})
        tracker.track_create("task", "task_001", {}, "bob", metadata={"source": "ui"})
        tracker.track_create("plan", "plan_002", {}, "alice", metadata={"source": "api"})

        results = tracker.search_events("api", search_in="metadata")
        assert len(results) == 2

    def test_search_events_all(self):
        """Test searching across all fields."""
        tracker = ChangeTracker()

        tracker.track_create("plan", "plan_findme", {}, "alice")
        tracker.track_create("task", "task_001", {}, "findme_user")
        tracker.track_create("plan", "plan_002", {}, "bob", metadata={"tag": "findme"})

        results = tracker.search_events("findme", search_in="all")
        assert len(results) == 3

    def test_get_entity_history(self):
        """Test getting complete history for an entity."""
        tracker = ChangeTracker()

        tracker.track_create("plan", "plan_001", {"status": "draft"}, "alice")
        tracker.track_create("task", "task_001", {}, "bob")  # Different entity
        tracker.track_update(
            "plan", "plan_001", {"status": "draft"}, {"status": "ready"}, "alice"
        )
        tracker.track_delete("plan", "plan_001", {"status": "ready"}, "admin")

        history = tracker.get_entity_history("plan_001")

        assert len(history) == 3
        assert history[0].change_type == "create"
        assert history[1].change_type == "update"
        assert history[2].change_type == "delete"

    def test_event_id_generation(self):
        """Test that event IDs are generated sequentially."""
        tracker = ChangeTracker()

        event1 = tracker.track_create("plan", "plan_001", {}, "alice")
        event2 = tracker.track_create("plan", "plan_002", {}, "alice")
        event3 = tracker.track_create("plan", "plan_003", {}, "alice")

        assert event1.id == "evt_00000001"
        assert event2.id == "evt_00000002"
        assert event3.id == "evt_00000003"
