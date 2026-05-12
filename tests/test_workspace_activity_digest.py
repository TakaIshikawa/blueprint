from blueprint.workspace import TeamWorkspace, WorkspaceActivityDigest


def test_workspace_activity_digest_counts_events_for_one_workspace_only():
    manager = TeamWorkspace()
    first = manager.create_workspace("Platform")
    second = manager.create_workspace("Growth")

    manager._record_activity(first.workspace_id, "alice", "plan_added", "plan", "plan-1", "Plan added")
    manager._record_activity(first.workspace_id, "alice", "plan_updated", "plan", "plan-1", "Plan updated")
    manager._record_activity(first.workspace_id, "bob", "resource_added", "resource", "res-1", "Resource added")
    manager._record_activity(second.workspace_id, "carol", "plan_added", "plan", "plan-2", "Other")

    digest = manager.build_activity_digest(first.workspace_id)

    assert isinstance(digest, WorkspaceActivityDigest)
    assert digest.workspace_id == first.workspace_id
    assert digest.total_event_count == 3
    assert digest.counts_by_action == {"plan_added": 1, "plan_updated": 1, "resource_added": 1}
    assert digest.counts_by_actor == {"alice": 2, "bob": 1}
    assert digest.counts_by_target_type == {"plan": 2, "resource": 1}
    assert digest.latest_activity_timestamp is not None


def test_all_workspace_activity_digest_aggregates_manager_feed():
    manager = TeamWorkspace()
    first = manager.create_workspace("Platform")
    second = manager.create_workspace("Growth")

    manager._record_activity(first.workspace_id, "alice", "plan_added", "plan", "plan-1")
    manager._record_activity(second.workspace_id, "alice", "member_added", "member", "member-1")

    digest = manager.build_all_workspace_activity_digest()

    assert digest.workspace_id is None
    assert digest.total_event_count == 2
    assert digest.counts_by_action == {"member_added": 1, "plan_added": 1}
    assert digest.counts_by_actor == {"alice": 2}
    assert digest.counts_by_target_type == {"member": 1, "plan": 1}


def test_activity_digest_time_window_is_deterministic():
    manager = TeamWorkspace()
    workspace = manager.create_workspace("Platform")
    manager._activity_feed.extend(
        [
            _event(workspace.workspace_id, "a", "old", "plan", "2026-05-01T00:00:00+00:00"),
            _event(workspace.workspace_id, "b", "current", "task", "2026-05-02T00:00:00+00:00"),
            _event(workspace.workspace_id, "b", "future", "task", "2026-05-03T00:00:00+00:00"),
        ]
    )

    digest = manager.build_activity_digest(
        workspace.workspace_id,
        since="2026-05-02T00:00:00+00:00",
        until="2026-05-02T23:59:59+00:00",
    )

    assert digest.total_event_count == 1
    assert digest.counts_by_action == {"current": 1}
    assert digest.latest_activity_timestamp == "2026-05-02T00:00:00+00:00"


def _event(workspace_id, user_id, action, entity_type, timestamp):
    from blueprint.workspace import ActivityEvent

    return ActivityEvent(
        event_id=f"act-{action}",
        workspace_id=workspace_id,
        user_id=user_id,
        action=action,
        entity_type=entity_type,
        entity_id=f"{entity_type}-1",
        timestamp=timestamp,
    )
