"""Tests for workspace configuration snapshots."""

import json

from blueprint.workspace import (
    ApprovalWorkflow,
    TeamWorkspace,
    WorkspaceRole,
    workspace_configuration_snapshot,
)


def test_configuration_snapshot_includes_aggregates_and_identifiers_without_member_details():
    ws = TeamWorkspace()
    workspace = ws.create_workspace(
        "Platform",
        description="Core delivery workspace",
        owner_id="owner-1",
        settings={
            "timezone": "US/Pacific",
            "approval_workflow": ApprovalWorkflow.SINGLE_APPROVER,
            "metadata": {
                "approvers": ["lead-1"],
                "feature_flags": {"calendar_sync": True},
            },
        },
        metadata={"feature_flags": {"capacity_reports": True}},
    )
    ws.add_member(
        workspace.workspace_id,
        "alice",
        "Alice",
        email="alice@example.com",
        role=WorkspaceRole.ADMIN,
        metadata={"capacity": 32},
    )
    ws.add_member(
        workspace.workspace_id,
        "bob",
        "Bob",
        email="bob@example.com",
        role=WorkspaceRole.MEMBER,
    )
    updated = ws.add_template(workspace.workspace_id, "Launch", template_data={"phase": "ga"})
    assert updated is not None
    updated = ws.add_custom_field(workspace.workspace_id, "Risk", field_type="select")
    assert updated is not None
    updated = ws.add_resource(workspace.workspace_id, "GPU Cluster", "compute")
    assert updated is not None
    updated = ws.add_plan(workspace.workspace_id, "plan-1")
    assert updated is not None

    snapshot = ws.build_configuration_snapshot(workspace.workspace_id)

    assert snapshot is not None
    json.dumps(snapshot)
    assert snapshot["snapshot_type"] == "workspace_configuration"
    assert snapshot["workspace"]["workspace_id"] == workspace.workspace_id
    assert snapshot["workspace"]["name"] == "Platform"
    assert snapshot["member_counts"] == {
        "total": 2,
        "by_role": {"admin": 1, "member": 1},
    }
    assert snapshot["settings"]["timezone"] == "US/Pacific"
    assert snapshot["settings"]["approval_workflow"] == "single_approver"
    assert snapshot["feature_flags"] == {
        "calendar_sync": True,
        "capacity_reports": True,
    }
    assert snapshot["configuration_identifiers"]["plan_ids"] == ["plan-1"]
    assert len(snapshot["configuration_identifiers"]["template_ids"]) == 1
    assert len(snapshot["configuration_identifiers"]["custom_field_ids"]) == 1
    assert len(snapshot["configuration_identifiers"]["resource_ids"]) == 1
    assert snapshot["counts"] == {
        "templates": 1,
        "custom_fields": 1,
        "resources": 1,
        "plans": 1,
    }

    serialized = json.dumps(snapshot)
    assert "alice" not in serialized
    assert "bob" not in serialized
    assert "alice@example.com" not in serialized
    assert "bob@example.com" not in serialized
    assert "Alice" not in serialized
    assert "Bob" not in serialized


def test_configuration_snapshot_handles_empty_member_lists():
    ws = TeamWorkspace()
    workspace = ws.create_workspace("Empty")

    snapshot = ws.build_configuration_snapshot(workspace.workspace_id)

    assert snapshot is not None
    assert snapshot["member_counts"] == {"total": 0, "by_role": {}}
    assert snapshot["configuration_identifiers"] == {
        "template_ids": [],
        "custom_field_ids": [],
        "resource_ids": [],
        "plan_ids": [],
    }
    assert snapshot["feature_flags"] == {}
    assert snapshot["settings"]["working_hours_start"] == "09:00"


def test_configuration_snapshot_preserves_optional_configuration_fields():
    ws = TeamWorkspace()
    workspace = ws.create_workspace(
        "Configured",
        settings={
            "working_hours_start": "07:30",
            "working_hours_end": "15:30",
            "holidays": ["2026-01-01"],
            "slack_channel": "#ops",
            "email_domain": "example.com",
            "metadata": {
                "feature_flags": ["sso_enforcement", "audit_exports"],
                "retention_days": 90,
            },
        },
    )

    snapshot = workspace_configuration_snapshot(workspace, generated_at="2026-05-13T00:00:00+00:00")

    assert snapshot["generated_at"] == "2026-05-13T00:00:00+00:00"
    assert snapshot["settings"]["working_hours_start"] == "07:30"
    assert snapshot["settings"]["working_hours_end"] == "15:30"
    assert snapshot["settings"]["holidays"] == ["2026-01-01"]
    assert snapshot["settings"]["slack_channel"] == "#ops"
    assert snapshot["settings"]["email_domain"] == "example.com"
    assert snapshot["settings"]["metadata"]["retention_days"] == 90
    assert snapshot["feature_flags"] == {
        "audit_exports": True,
        "sso_enforcement": True,
    }


def test_configuration_snapshot_returns_none_for_missing_workspace():
    assert TeamWorkspace().build_configuration_snapshot("missing") is None
