from dataclasses import replace

from blueprint.workspace.team_workspace import TeamWorkspace


def _manager_with_activity() -> tuple[TeamWorkspace, str]:
    manager = TeamWorkspace()
    workspace = manager.create_workspace("Engineering")
    other = manager.create_workspace("Support")

    manager.add_member(workspace.workspace_id, "alice", "Alice")
    manager.add_member(workspace.workspace_id, "bob", "Bob")
    invitation = manager.invite_member(
        workspace.workspace_id,
        "carol@example.com",
        invited_by="alice",
    )
    assert invitation is not None
    manager.accept_invitation(invitation.invitation_id)
    manager.add_member(other.workspace_id, "dana", "Dana")

    manager._activity_feed[-4] = replace(
        manager._activity_feed[-4],
        timestamp="2026-01-01T00:00:00+00:00",
    )
    manager._activity_feed[-3] = replace(
        manager._activity_feed[-3],
        timestamp="2026-01-02T00:00:00+00:00",
    )
    manager._activity_feed[-2] = replace(
        manager._activity_feed[-2],
        timestamp="2026-01-03T00:00:00+00:00",
    )
    manager._activity_feed[-1] = replace(
        manager._activity_feed[-1],
        timestamp="2026-01-04T00:00:00+00:00",
    )
    return manager, workspace.workspace_id


def test_get_activity_feed_default_behavior_returns_latest_workspace_events():
    manager, workspace_id = _manager_with_activity()

    events = manager.get_activity_feed(workspace_id, limit=2)

    assert [event.action for event in events] == ["member_added", "invitation_accepted"]


def test_get_activity_feed_filters_by_user_action_and_entity_type():
    manager, workspace_id = _manager_with_activity()

    events = manager.get_activity_feed(
        workspace_id,
        user_id="carol@example.com",
        action="invitation_accepted",
        entity_type="member",
    )

    assert len(events) == 1
    assert events[0].user_id == "carol@example.com"
    assert events[0].action == "invitation_accepted"


def test_get_activity_feed_filters_by_inclusive_timestamp_bounds():
    manager, workspace_id = _manager_with_activity()

    events = manager.get_activity_feed(
        workspace_id,
        since="2026-01-02T00:00:00+00:00",
        until="2026-01-03T00:00:00+00:00",
    )

    assert [event.user_id for event in events] == ["bob", "carol@example.com"]

