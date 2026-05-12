from dataclasses import replace

from blueprint.workspace.team_workspace import TeamWorkspace
from blueprint.workspace.workspace_model import WorkspaceActivityDigest


def test_workspace_activity_digest_counts_recent_workspace_activity():
    manager = TeamWorkspace()
    workspace = manager.create_workspace("Engineering")
    other = manager.create_workspace("Support")

    manager.add_member(workspace.workspace_id, "alice", "Alice")
    manager.add_member(workspace.workspace_id, "bob", "Bob")
    manager.add_member(other.workspace_id, "carol", "Carol")
    manager._record_activity(workspace.workspace_id, "alice", "plan_added", "plan", "plan-1")

    digest = manager.build_activity_digest(workspace.workspace_id)

    assert isinstance(digest, WorkspaceActivityDigest)
    assert digest.workspace_id == workspace.workspace_id
    assert digest.total_event_count == 3
    assert digest.counts_by_action == {"member_added": 2, "plan_added": 1}
    assert digest.counts_by_actor == {"alice": 2, "bob": 1}
    assert digest.counts_by_target_type == {"member": 2, "plan": 1}
    assert digest.latest_activity_timestamp is not None


def test_all_workspace_activity_digest_aggregates_manager_feed():
    manager = TeamWorkspace()
    one = manager.create_workspace("One")
    two = manager.create_workspace("Two")

    manager.add_member(one.workspace_id, "alice", "Alice")
    manager.add_member(two.workspace_id, "bob", "Bob")

    digest = manager.build_all_workspace_activity_digest()

    assert digest.workspace_id is None
    assert digest.total_event_count == 2
    assert digest.counts_by_action == {"member_added": 2}
    assert digest.counts_by_actor == {"alice": 1, "bob": 1}
    assert digest.counts_by_target_type == {"member": 2}
    assert digest.to_dict()["total_event_count"] == 2


def test_activity_digest_time_window_filters_deterministically():
    manager = TeamWorkspace()
    workspace = manager.create_workspace("Windowed")
    manager._record_activity(workspace.workspace_id, "alice", "old", "plan", "old")
    manager._record_activity(workspace.workspace_id, "bob", "new", "plan", "new")
    manager._activity_feed[-2] = replace(manager._activity_feed[-2], timestamp="2026-01-01T00:00:00+00:00")
    manager._activity_feed[-1] = replace(manager._activity_feed[-1], timestamp="2026-02-01T00:00:00+00:00")

    digest = manager.build_activity_digest(
        workspace.workspace_id,
        window_start="2026-01-15T00:00:00+00:00",
        window_end="2026-02-15T00:00:00+00:00",
    )

    assert digest.total_event_count == 1
    assert digest.counts_by_action == {"new": 1}
    assert digest.latest_activity_timestamp == "2026-02-01T00:00:00+00:00"
