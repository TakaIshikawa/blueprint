from blueprint.workspace import TeamWorkspace, WorkspaceInvitation, WorkspaceRole


def test_invite_member_creates_pending_invitation():
    manager = TeamWorkspace()
    workspace = manager.create_workspace("Engineering")

    invitation = manager.invite_member(
        workspace.workspace_id,
        "dev@example.com",
        WorkspaceRole.ADMIN,
        invited_by="owner",
    )

    assert isinstance(invitation, WorkspaceInvitation)
    assert invitation.status == "pending"
    assert invitation.role == WorkspaceRole.ADMIN
    assert manager.list_invitations(workspace.workspace_id) == [invitation]


def test_accept_invitation_adds_member_once():
    manager = TeamWorkspace()
    workspace = manager.create_workspace("Engineering")
    invitation = manager.invite_member(workspace.workspace_id, "dev@example.com", WorkspaceRole.MEMBER)

    assert invitation is not None
    accepted = manager.accept_invitation(invitation.invitation_id)
    accepted_again = manager.accept_invitation(invitation.invitation_id)

    members = manager.get_members(workspace.workspace_id)
    assert accepted is not None
    assert accepted.status == "accepted"
    assert accepted_again == accepted
    assert len(members) == 1
    assert members[0].user_id == "dev@example.com"
    assert members[0].role == WorkspaceRole.MEMBER


def test_decline_invitation_does_not_add_member():
    manager = TeamWorkspace()
    workspace = manager.create_workspace("Engineering")
    invitation = manager.invite_member(workspace.workspace_id, "dev@example.com")

    assert invitation is not None
    declined = manager.decline_invitation(invitation.invitation_id)

    assert declined is not None
    assert declined.status == "declined"
    assert manager.get_members(workspace.workspace_id) == []


def test_invite_missing_workspace_returns_none():
    manager = TeamWorkspace()

    assert manager.invite_member("missing", "dev@example.com") is None


def test_declined_invitation_cannot_be_accepted_later():
    manager = TeamWorkspace()
    workspace = manager.create_workspace("Engineering")
    invitation = manager.invite_member(workspace.workspace_id, "dev@example.com")

    assert invitation is not None
    manager.decline_invitation(invitation.invitation_id)
    result = manager.accept_invitation(invitation.invitation_id)

    assert result is not None
    assert result.status == "declined"
    assert manager.get_members(workspace.workspace_id) == []

