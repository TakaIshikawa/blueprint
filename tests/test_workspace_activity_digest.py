from blueprint.workspace.team_workspace import TeamWorkspace


def test_workspace_activity_digest_counts_recent_events_by_dimensions():
    manager = TeamWorkspace()
    workspace = manager.create_workspace("Platform")
    other = manager.create_workspace("Other")

    manager._record_activity(workspace.workspace_id, "alice", "plan_added", "plan", "plan-1")
    manager._record_activity(workspace.workspace_id, "alice", "member_added", "member", "mem-1")
    manager._record_activity(workspace.workspace_id, "bob", "plan_added", "plan", "plan-2")
    manager._record_activity(other.workspace_id, "carol", "plan_added", "plan", "plan-3")

    digest = manager.build_activity_digest(workspace.workspace_id)

    assert digest.workspace_id == workspace.workspace_id
    assert digest.total_event_count == 3
    assert digest.counts_by_action == {"member_added": 1, "plan_added": 2}
    assert digest.counts_by_actor == {"alice": 2, "bob": 1}
    assert digest.counts_by_target_type == {"member": 1, "plan": 2}
    assert digest.latest_activity_timestamp is not None


def test_all_workspace_activity_digest_aggregates_activity_feed():
    manager = TeamWorkspace()
    first = manager.create_workspace("First")
    second = manager.create_workspace("Second")

    manager._record_activity(first.workspace_id, "alice", "plan_added", "plan", "plan-1")
    manager._record_activity(second.workspace_id, "alice", "resource_added", "resource", "res-1")

    digest = manager.build_all_workspace_activity_digest()

    assert digest.workspace_id is None
    assert digest.total_event_count == 2
    assert digest.counts_by_action == {"plan_added": 1, "resource_added": 1}
    assert digest.counts_by_actor == {"alice": 2}
    assert digest.counts_by_target_type == {"plan": 1, "resource": 1}


def test_activity_digest_applies_time_window():
    manager = TeamWorkspace()
    workspace = manager.create_workspace("Window")
    manager._record_activity(workspace.workspace_id, "alice", "old", "plan", "plan-1")
    manager._activity_feed[-1] = manager._activity_feed[-1].__class__(
        event_id=manager._activity_feed[-1].event_id,
        workspace_id=workspace.workspace_id,
        user_id="alice",
        action="old",
        entity_type="plan",
        entity_id="plan-1",
        timestamp="2026-01-01T00:00:00+00:00",
    )
    manager._record_activity(workspace.workspace_id, "bob", "new", "member", "mem-1")

    digest = manager.build_activity_digest(workspace.workspace_id, window_start="2026-02-01T00:00:00+00:00")

    assert digest.total_event_count == 1
    assert digest.counts_by_action == {"new": 1}
    assert digest.counts_by_actor == {"bob": 1}
    assert digest.window_start == "2026-02-01T00:00:00+00:00"
