"""Tests for team workspace covering member management and settings."""

import pytest

from blueprint.workspace.team_workspace import TeamWorkspace
from blueprint.workspace.workspace_model import (
    ApprovalWorkflow,
    WorkspaceRole,
)


@pytest.fixture
def ws() -> TeamWorkspace:
    return TeamWorkspace()


class TestWorkspaceCRUD:
    def test_create_workspace(self, ws: TeamWorkspace):
        w = ws.create_workspace("Engineering", owner_id="alice")
        assert w.name == "Engineering"
        assert w.owner_id == "alice"

    def test_get_workspace(self, ws: TeamWorkspace):
        w = ws.create_workspace("Find")
        assert ws.get_workspace(w.workspace_id) is not None

    def test_delete_workspace(self, ws: TeamWorkspace):
        w = ws.create_workspace("Del")
        assert ws.delete_workspace(w.workspace_id) is True
        assert ws.get_workspace(w.workspace_id) is None

    def test_list_workspaces(self, ws: TeamWorkspace):
        ws.create_workspace("A")
        ws.create_workspace("B")
        assert len(ws.list_workspaces()) == 2


class TestMemberManagement:
    def test_add_member(self, ws: TeamWorkspace):
        w = ws.create_workspace("Team")
        result = ws.add_member(w.workspace_id, "u1", "Bob", role=WorkspaceRole.ADMIN)
        assert result is not None
        assert len(result.members) == 1
        assert result.members[0].role == WorkspaceRole.ADMIN

    def test_remove_member(self, ws: TeamWorkspace):
        w = ws.create_workspace("Team")
        ws.add_member(w.workspace_id, "u1", "Bob")
        result = ws.remove_member(w.workspace_id, "u1")
        assert result is not None
        assert len(result.members) == 0

    def test_update_member_role(self, ws: TeamWorkspace):
        w = ws.create_workspace("Team")
        ws.add_member(w.workspace_id, "u1", "Bob")
        result = ws.update_member_role(w.workspace_id, "u1", WorkspaceRole.ADMIN)
        assert result is not None
        assert result.members[0].role == WorkspaceRole.ADMIN

    def test_get_members(self, ws: TeamWorkspace):
        w = ws.create_workspace("Team")
        ws.add_member(w.workspace_id, "u1", "Alice")
        ws.add_member(w.workspace_id, "u2", "Bob")
        members = ws.get_members(w.workspace_id)
        assert len(members) == 2

    def test_add_member_missing_workspace(self, ws: TeamWorkspace):
        assert ws.add_member("missing", "u1", "Bob") is None


class TestSharedResources:
    def test_add_resource(self, ws: TeamWorkspace):
        w = ws.create_workspace("Team")
        result = ws.add_resource(w.workspace_id, "GPU Cluster", "compute", total_capacity=8.0)
        assert result is not None
        assert len(result.resources) == 1
        assert result.resources[0].total_capacity == 8.0

    def test_get_resources(self, ws: TeamWorkspace):
        w = ws.create_workspace("Team")
        ws.add_resource(w.workspace_id, "R1", "compute")
        ws.add_resource(w.workspace_id, "R2", "budget")
        assert len(ws.get_resources(w.workspace_id)) == 2


class TestTemplatesAndFields:
    def test_add_template(self, ws: TeamWorkspace):
        w = ws.create_workspace("Team")
        result = ws.add_template(w.workspace_id, "Sprint Template")
        assert result is not None
        assert len(result.templates) == 1

    def test_add_custom_field(self, ws: TeamWorkspace):
        w = ws.create_workspace("Team")
        result = ws.add_custom_field(
            w.workspace_id, "Priority", field_type="select", options=["P0", "P1", "P2"]
        )
        assert result is not None
        assert len(result.custom_fields) == 1
        assert result.custom_fields[0].options == ["P0", "P1", "P2"]


class TestSettings:
    def test_default_settings(self, ws: TeamWorkspace):
        w = ws.create_workspace("Team")
        assert w.settings.working_hours_start == "09:00"
        assert w.settings.approval_workflow == ApprovalWorkflow.NONE

    def test_update_settings(self, ws: TeamWorkspace):
        w = ws.create_workspace("Team")
        result = ws.update_settings(
            w.workspace_id,
            working_hours_start="08:00",
            holidays=["2025-12-25"],
            approval_workflow=ApprovalWorkflow.SINGLE_APPROVER,
        )
        assert result is not None
        assert result.settings.working_hours_start == "08:00"
        assert result.settings.holidays == ["2025-12-25"]
        assert result.settings.approval_workflow == ApprovalWorkflow.SINGLE_APPROVER

    def test_custom_settings_on_create(self, ws: TeamWorkspace):
        w = ws.create_workspace(
            "Team",
            settings={"timezone": "US/Pacific", "slack_channel": "#eng"},
        )
        assert w.settings.timezone == "US/Pacific"
        assert w.settings.slack_channel == "#eng"


class TestActivityAndCalendar:
    def test_activity_feed(self, ws: TeamWorkspace):
        w = ws.create_workspace("Team")
        ws.add_member(w.workspace_id, "u1", "Bob")
        feed = ws.get_activity_feed(w.workspace_id)
        assert len(feed) == 1
        assert feed[0].action == "member_added"

    def test_team_calendar(self, ws: TeamWorkspace):
        w = ws.create_workspace("Team")
        ws.add_plan(w.workspace_id, "plan-1")
        events = ws.build_team_calendar(
            w.workspace_id,
            {"plan-1": [{"title": "Launch", "start_date": "2025-06-01"}]},
        )
        assert len(events) == 1
        assert events[0].title == "Launch"


class TestExportImport:
    def test_export_workspace(self, ws: TeamWorkspace):
        w = ws.create_workspace("Export Me", owner_id="alice")
        data = ws.export_workspace(w.workspace_id)
        assert data is not None
        assert data["name"] == "Export Me"

    def test_import_workspace(self, ws: TeamWorkspace):
        w = ws.import_workspace({"name": "Imported", "owner_id": "bob", "plan_ids": ["p1"]})
        assert w.name == "Imported"
        assert ws.get_workspace(w.workspace_id) is not None
