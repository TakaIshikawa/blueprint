from blueprint.workspace import ApprovalWorkflow, TeamWorkspace, WorkspacePolicyFinding, WorkspaceRole


def test_compliant_workspace_has_no_policy_findings():
    manager = TeamWorkspace()
    workspace = manager.create_workspace(
        "Engineering",
        settings={
            "email_domain": "example.com",
            "approval_workflow": ApprovalWorkflow.SINGLE_APPROVER,
            "metadata": {"approvers": ["owner@example.com"]},
        },
    )
    manager.add_member(
        workspace.workspace_id,
        "owner",
        "Owner",
        email="owner@example.com",
        role=WorkspaceRole.OWNER,
    )

    assert manager.evaluate_workspace_policies(workspace.workspace_id) == []


def test_workspace_policy_evaluator_reports_multiple_failures():
    manager = TeamWorkspace()
    workspace = manager.create_workspace(
        "Engineering",
        settings={
            "email_domain": "example.com",
            "working_hours_start": "18:00",
            "working_hours_end": "09:00",
            "approval_workflow": ApprovalWorkflow.MULTI_APPROVER,
        },
    )
    manager.add_member(
        workspace.workspace_id,
        "member",
        "Member",
        email="member@other.com",
        role=WorkspaceRole.MEMBER,
    )

    findings = manager.evaluate_workspace_policies(workspace.workspace_id)

    assert all(isinstance(finding, WorkspacePolicyFinding) for finding in findings)
    assert {finding.code for finding in findings} == {
        "member_email_domain_mismatch",
        "missing_owner_or_admin",
        "invalid_working_hours",
        "missing_approval_workflow_metadata",
    }


def test_missing_workspace_returns_not_found_finding():
    manager = TeamWorkspace()

    findings = manager.evaluate_workspace_policies("missing")

    assert len(findings) == 1
    assert findings[0].code == "workspace_not_found"
    assert findings[0].severity == "error"

