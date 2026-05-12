from dataclasses import replace

from blueprint.workspace.team_workspace import TeamWorkspace
from blueprint.workspace.workspace_model import SharedResource, WorkspaceRole


def test_member_capacity_report_lists_members_with_capacity_and_calendar_counts():
    manager = TeamWorkspace()
    workspace = manager.create_workspace("Platform")
    workspace = manager.add_member(workspace.workspace_id, "alice", "Alice", email="alice@example.com", role=WorkspaceRole.ADMIN)
    member = replace(workspace.members[0], metadata={"assigned_capacity": {"hours_per_week": 32, "focus": "api"}})
    manager._workspaces[workspace.workspace_id] = replace(workspace, members=[member], plan_ids=["plan-1"])

    report = manager.build_member_capacity_report(
        workspace.workspace_id,
        {"plan-1": [{"title": "Alice API review", "start_date": "2026-05-01"}]},
    )

    assert report is not None
    assert report.workspace_id == workspace.workspace_id
    assert report.members[0].role == WorkspaceRole.ADMIN
    assert report.members[0].email == "alice@example.com"
    assert report.members[0].assigned_capacity_metadata == {"focus": "api", "hours_per_week": 32}
    assert report.members[0].calendar_event_count == 1


def test_capacity_report_lists_resources_with_utilization_metadata():
    manager = TeamWorkspace()
    workspace = manager.create_workspace("Platform")
    resource = SharedResource(
        resource_id="res-1",
        name="Build minutes",
        resource_type="ci",
        total_capacity=100.0,
        allocated=25.0,
        unit="minutes",
        metadata={"utilization": {"owner": "platform"}},
    )
    manager._workspaces[workspace.workspace_id] = replace(workspace, resources=[resource])

    report = manager.build_member_capacity_report(workspace.workspace_id)

    assert report is not None
    assert report.resources[0].total_capacity == 100.0
    assert report.resources[0].unit == "minutes"
    assert report.resources[0].utilization_metadata == {"allocated_ratio": 0.25, "owner": "platform"}


def test_capacity_report_handles_missing_optional_metadata_deterministically():
    manager = TeamWorkspace()
    workspace = manager.create_workspace("Platform")
    workspace = manager.add_member(workspace.workspace_id, "bob", "Bob")
    workspace = manager.add_resource(workspace.workspace_id, "Seats", "license", total_capacity=0.0, unit="seat")

    report = manager.build_member_capacity_report(workspace.workspace_id)

    assert report is not None
    assert report.members[0].assigned_capacity_metadata == {}
    assert report.members[0].calendar_event_count == 0
    assert report.resources[0].utilization_metadata == {}
    assert report.to_dict()["members"][0]["role"] == "member"
