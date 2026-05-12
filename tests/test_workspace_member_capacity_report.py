from blueprint.workspace.team_workspace import TeamWorkspace
from blueprint.workspace.workspace_model import WorkspaceMemberCapacityReport, WorkspaceRole


def test_member_capacity_report_lists_members_with_capacity_and_calendar_counts():
    manager = TeamWorkspace()
    workspace = manager.create_workspace("Engineering")
    updated = manager.add_member(
        workspace.workspace_id,
        "alice",
        "Alice",
        email="alice@example.com",
        role=WorkspaceRole.ADMIN,
        metadata={"weekly_capacity": 32, "team": "platform"},
    )
    assert updated is not None
    manager.add_member(workspace.workspace_id, "bob", "Bob", email="bob@example.com")
    manager.add_plan(workspace.workspace_id, "plan-1")

    report = manager.build_member_capacity_report(
        workspace.workspace_id,
        {
            "plan-1": [
                {"title": "Design", "start_date": "2026-06-01", "user_id": "alice"},
                {"title": "Review", "start_date": "2026-06-02", "attendees": ["alice", "bob"]},
            ]
        },
    )

    assert isinstance(report, WorkspaceMemberCapacityReport)
    assert report is not None
    assert [member.display_name for member in report.members] == ["Alice", "Bob"]
    alice = report.members[0]
    assert alice.email == "alice@example.com"
    assert alice.role == "admin"
    assert alice.capacity_metadata == {"weekly_capacity": 32}
    assert alice.calendar_event_count == 2
    assert report.members[1].capacity_metadata == {}
    assert report.members[1].calendar_event_count == 1


def test_member_capacity_report_lists_resources_with_utilization_metadata():
    manager = TeamWorkspace()
    workspace = manager.create_workspace("Engineering")
    manager.add_resource(
        workspace.workspace_id,
        "GPU Cluster",
        "compute",
        total_capacity=8.0,
        unit="gpu",
        metadata={"utilization": 0.5, "owner": "ml"},
    )
    resource = manager.get_resources(workspace.workspace_id)[0]
    manager.allocate_resource(workspace.workspace_id, resource.resource_id, "plan-1", 3.0)

    report = manager.build_member_capacity_report(workspace.workspace_id)

    assert report is not None
    assert len(report.resources) == 1
    capacity = report.resources[0]
    assert capacity.name == "GPU Cluster"
    assert capacity.total_capacity == 8.0
    assert capacity.allocated == 3.0
    assert capacity.unit == "gpu"
    assert capacity.utilization_metadata == {"utilization": 0.5}
    assert report.to_dict()["resources"][0]["resource_type"] == "compute"


def test_member_capacity_report_handles_missing_optional_metadata():
    manager = TeamWorkspace()
    workspace = manager.create_workspace("Empty")
    manager.add_member(workspace.workspace_id, "u1", "User")
    manager.add_resource(workspace.workspace_id, "Budget", "money")

    report = manager.build_member_capacity_report(workspace.workspace_id)

    assert report is not None
    assert report.members[0].capacity_metadata == {}
    assert report.members[0].calendar_event_count == 0
    assert report.resources[0].utilization_metadata == {}
    assert manager.build_member_capacity_report("missing") is None
