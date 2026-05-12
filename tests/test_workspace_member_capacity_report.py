from blueprint.workspace import TeamWorkspace, WorkspaceMemberCapacityReport, WorkspaceRole


def test_member_capacity_report_lists_members_capacity_and_calendar_counts():
    manager = TeamWorkspace()
    workspace = manager.create_workspace("Engineering")
    updated = manager.add_member(
        workspace.workspace_id,
        "alice",
        "Alice",
        email="alice@example.com",
        role=WorkspaceRole.ADMIN,
        metadata={"assigned_capacity": 32},
    )
    assert updated is not None
    member = updated.members[0]
    manager.add_plan(workspace.workspace_id, "plan-1")

    report = manager.build_member_capacity_report(
        workspace.workspace_id,
        {
            "plan-1": [
                {"title": "Design", "start_date": "2026-05-01", "member_id": member.member_id},
                {"title": "Build", "start_date": "2026-05-02", "user_id": "alice"},
                {"title": "Review", "start_date": "2026-05-03", "email": "alice@example.com"},
            ]
        },
    )

    assert isinstance(report, WorkspaceMemberCapacityReport)
    assert report is not None
    assert report.workspace_id == workspace.workspace_id
    assert report.members[0].role == "admin"
    assert report.members[0].email == "alice@example.com"
    assert report.members[0].assigned_capacity == 32
    assert report.members[0].calendar_event_count == 3


def test_member_capacity_report_lists_resources_with_utilization_metadata():
    manager = TeamWorkspace()
    workspace = manager.create_workspace("Engineering")
    updated = manager.add_resource(
        workspace.workspace_id,
        "GPU Cluster",
        "compute",
        total_capacity=10,
        unit="gpu",
        metadata={"utilization": "reserved"},
    )
    assert updated is not None

    report = manager.build_member_capacity_report(workspace.workspace_id)

    assert report is not None
    assert report.resources[0].resource_id == updated.resources[0].resource_id
    assert report.resources[0].total_capacity == 10
    assert report.resources[0].unit == "gpu"
    assert report.resources[0].utilization == "reserved"


def test_member_capacity_report_handles_missing_optional_metadata():
    manager = TeamWorkspace()
    workspace = manager.create_workspace("Engineering")
    manager.add_member(workspace.workspace_id, "bob", "Bob")
    manager.add_resource(workspace.workspace_id, "QA Lab", "environment", total_capacity=0)

    report = manager.build_member_capacity_report(workspace.workspace_id)

    assert report is not None
    assert report.members[0].assigned_capacity is None
    assert report.members[0].calendar_event_count == 0
    assert report.resources[0].utilization is None
    assert manager.build_member_capacity_report("missing") is None
