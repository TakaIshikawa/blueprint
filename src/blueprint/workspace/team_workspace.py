"""Team workspace manager for organizing plans by team.

Provides member management, shared resource pools, workspace-level
templates, default settings, activity feeds, and team calendars.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from blueprint.workspace.workspace_model import (
    ActivityEvent,
    ApprovalWorkflow,
    CalendarEvent,
    CustomField,
    ResourceAllocation,
    SharedResource,
    TeamMember,
    Workspace,
    WorkspaceInvitation,
    WorkspacePolicyFinding,
    WorkspaceRole,
    WorkspaceSettings,
    WorkspaceTemplate,
    _gen_id,
    _now_iso,
)


class TeamWorkspace:
    """Manages team workspaces with shared resources and settings."""

    def __init__(self) -> None:
        self._workspaces: dict[str, Workspace] = {}
        self._activity_feed: list[ActivityEvent] = []
        self._invitations: dict[str, WorkspaceInvitation] = {}
        self._resource_allocations: dict[str, ResourceAllocation] = {}

    # ------------------------------------------------------------------
    # Workspace CRUD
    # ------------------------------------------------------------------

    def create_workspace(
        self,
        name: str,
        *,
        description: str = "",
        owner_id: str = "",
        settings: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Workspace:
        wid = _gen_id("ws")
        ws_settings = WorkspaceSettings(**(settings or {}))
        ws = Workspace(
            workspace_id=wid,
            name=name,
            description=description,
            owner_id=owner_id,
            settings=ws_settings,
            metadata=metadata or {},
        )
        self._workspaces[wid] = ws
        return ws

    def get_workspace(self, workspace_id: str) -> Workspace | None:
        return self._workspaces.get(workspace_id)

    def list_workspaces(self) -> list[Workspace]:
        return list(self._workspaces.values())

    def search_workspaces(
        self,
        *,
        query: str = "",
        owner_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        member_user_id: str | None = None,
    ) -> list[Workspace]:
        matches = list(self._workspaces.values())

        if query:
            needle = query.casefold()
            matches = [
                ws
                for ws in matches
                if needle in ws.name.casefold() or needle in ws.description.casefold()
            ]
        if owner_id is not None:
            matches = [ws for ws in matches if ws.owner_id == owner_id]
        if metadata:
            matches = [
                ws
                for ws in matches
                if all(ws.metadata.get(key) == value for key, value in metadata.items())
            ]
        if member_user_id is not None:
            matches = [
                ws
                for ws in matches
                if any(member.user_id == member_user_id for member in ws.members)
            ]

        return matches

    def delete_workspace(self, workspace_id: str) -> bool:
        return self._workspaces.pop(workspace_id, None) is not None

    # ------------------------------------------------------------------
    # Member management
    # ------------------------------------------------------------------

    def add_member(
        self,
        workspace_id: str,
        user_id: str,
        display_name: str,
        *,
        email: str = "",
        role: WorkspaceRole = WorkspaceRole.MEMBER,
    ) -> Workspace | None:
        ws = self._workspaces.get(workspace_id)
        if ws is None:
            return None
        member = TeamMember(
            member_id=_gen_id("mem"),
            user_id=user_id,
            display_name=display_name,
            email=email,
            role=role,
        )
        updated = replace(
            ws,
            members=[*ws.members, member],
            updated_at=_now_iso(),
        )
        self._workspaces[workspace_id] = updated
        self._record_activity(workspace_id, user_id, "member_added", "member", member.member_id)
        return updated

    def remove_member(self, workspace_id: str, user_id: str) -> Workspace | None:
        ws = self._workspaces.get(workspace_id)
        if ws is None:
            return None
        new_members = [m for m in ws.members if m.user_id != user_id]
        updated = replace(ws, members=new_members, updated_at=_now_iso())
        self._workspaces[workspace_id] = updated
        return updated

    def update_member_role(
        self, workspace_id: str, user_id: str, role: WorkspaceRole
    ) -> Workspace | None:
        ws = self._workspaces.get(workspace_id)
        if ws is None:
            return None
        new_members = []
        for m in ws.members:
            if m.user_id == user_id:
                new_members.append(replace(m, role=role))
            else:
                new_members.append(m)
        updated = replace(ws, members=new_members, updated_at=_now_iso())
        self._workspaces[workspace_id] = updated
        return updated

    def get_members(self, workspace_id: str) -> list[TeamMember]:
        ws = self._workspaces.get(workspace_id)
        return list(ws.members) if ws else []

    # ------------------------------------------------------------------
    # Invitations
    # ------------------------------------------------------------------

    def invite_member(
        self,
        workspace_id: str,
        email: str,
        role: WorkspaceRole = WorkspaceRole.MEMBER,
        *,
        invited_by: str = "",
    ) -> WorkspaceInvitation | None:
        if workspace_id not in self._workspaces:
            return None
        invitation = WorkspaceInvitation(
            invitation_id=_gen_id("inv"),
            workspace_id=workspace_id,
            email=email,
            role=role,
            invited_by=invited_by,
        )
        self._invitations[invitation.invitation_id] = invitation
        return invitation

    def accept_invitation(self, invitation_id: str) -> WorkspaceInvitation | None:
        invitation = self._invitations.get(invitation_id)
        if invitation is None:
            return None
        if invitation.status != "pending":
            return invitation

        ws = self._workspaces.get(invitation.workspace_id)
        if ws is None:
            return None

        if not any(m.user_id == invitation.email for m in ws.members):
            member = TeamMember(
                member_id=_gen_id("mem"),
                user_id=invitation.email,
                display_name=invitation.email,
                email=invitation.email,
                role=invitation.role,
            )
            updated_ws = replace(
                ws,
                members=[*ws.members, member],
                updated_at=_now_iso(),
            )
            self._workspaces[ws.workspace_id] = updated_ws
            self._record_activity(
                ws.workspace_id,
                invitation.email,
                "invitation_accepted",
                "member",
                member.member_id,
            )

        accepted = replace(invitation, status="accepted", accepted_at=_now_iso())
        self._invitations[invitation_id] = accepted
        return accepted

    def decline_invitation(self, invitation_id: str) -> WorkspaceInvitation | None:
        invitation = self._invitations.get(invitation_id)
        if invitation is None:
            return None
        if invitation.status != "pending":
            return invitation
        declined = replace(invitation, status="declined", declined_at=_now_iso())
        self._invitations[invitation_id] = declined
        return declined

    def list_invitations(
        self,
        workspace_id: str | None = None,
        *,
        status: str | None = None,
    ) -> list[WorkspaceInvitation]:
        invitations = list(self._invitations.values())
        if workspace_id is not None:
            invitations = [i for i in invitations if i.workspace_id == workspace_id]
        if status is not None:
            invitations = [i for i in invitations if i.status == status]
        return invitations

    # ------------------------------------------------------------------
    # Shared resources
    # ------------------------------------------------------------------

    def add_resource(
        self,
        workspace_id: str,
        name: str,
        resource_type: str,
        *,
        total_capacity: float = 0.0,
        unit: str = "",
    ) -> Workspace | None:
        ws = self._workspaces.get(workspace_id)
        if ws is None:
            return None
        resource = SharedResource(
            resource_id=_gen_id("res"),
            name=name,
            resource_type=resource_type,
            total_capacity=total_capacity,
            unit=unit,
        )
        updated = replace(ws, resources=[*ws.resources, resource], updated_at=_now_iso())
        self._workspaces[workspace_id] = updated
        return updated

    def get_resources(self, workspace_id: str) -> list[SharedResource]:
        ws = self._workspaces.get(workspace_id)
        return list(ws.resources) if ws else []

    def allocate_resource(
        self,
        workspace_id: str,
        resource_id: str,
        plan_id: str,
        amount: float,
        *,
        reason: str = "",
        created_by: str = "",
    ) -> ResourceAllocation | None:
        ws = self._workspaces.get(workspace_id)
        if ws is None or amount <= 0:
            return None

        resource = next((r for r in ws.resources if r.resource_id == resource_id), None)
        if resource is None or resource.allocated + amount > resource.total_capacity:
            return None

        allocation = ResourceAllocation(
            allocation_id=_gen_id("alloc"),
            resource_id=resource_id,
            plan_id=plan_id,
            amount=amount,
            reason=reason,
            created_by=created_by,
        )
        resources = [
            replace(r, allocated=r.allocated + amount) if r.resource_id == resource_id else r
            for r in ws.resources
        ]
        self._workspaces[workspace_id] = replace(ws, resources=resources, updated_at=_now_iso())
        self._resource_allocations[allocation.allocation_id] = allocation
        return allocation

    def release_resource(self, workspace_id: str, allocation_id: str | None = None) -> bool:
        if allocation_id is None:
            allocation_id = workspace_id
            allocation = self._resource_allocations.get(allocation_id)
            if allocation is None:
                return False
            workspace_id = self._workspace_id_for_resource(allocation.resource_id) or ""

        ws = self._workspaces.get(workspace_id)
        allocation = self._resource_allocations.get(allocation_id)
        if ws is None or allocation is None:
            return False
        if not any(r.resource_id == allocation.resource_id for r in ws.resources):
            return False

        resources = [
            replace(r, allocated=max(0.0, r.allocated - allocation.amount))
            if r.resource_id == allocation.resource_id
            else r
            for r in ws.resources
        ]
        self._workspaces[workspace_id] = replace(ws, resources=resources, updated_at=_now_iso())
        del self._resource_allocations[allocation_id]
        return True

    def _workspace_id_for_resource(self, resource_id: str) -> str | None:
        for workspace_id, ws in self._workspaces.items():
            if any(resource.resource_id == resource_id for resource in ws.resources):
                return workspace_id
        return None

    def list_resource_allocations(
        self,
        workspace_id: str | None = None,
        *,
        resource_id: str | None = None,
    ) -> list[ResourceAllocation]:
        allocations = list(self._resource_allocations.values())
        if workspace_id is not None:
            ws = self._workspaces.get(workspace_id)
            if ws is None:
                return []
            resource_ids = {r.resource_id for r in ws.resources}
            allocations = [a for a in allocations if a.resource_id in resource_ids]
        if resource_id is not None:
            allocations = [a for a in allocations if a.resource_id == resource_id]
        return allocations

    # ------------------------------------------------------------------
    # Templates & custom fields
    # ------------------------------------------------------------------

    def add_template(
        self,
        workspace_id: str,
        name: str,
        *,
        description: str = "",
        template_data: dict[str, Any] | None = None,
    ) -> Workspace | None:
        ws = self._workspaces.get(workspace_id)
        if ws is None:
            return None
        tmpl = WorkspaceTemplate(
            template_id=_gen_id("tmpl"),
            name=name,
            description=description,
            template_data=template_data or {},
        )
        updated = replace(ws, templates=[*ws.templates, tmpl], updated_at=_now_iso())
        self._workspaces[workspace_id] = updated
        return updated

    def add_custom_field(
        self,
        workspace_id: str,
        name: str,
        *,
        field_type: str = "text",
        required: bool = False,
        options: list[str] | None = None,
    ) -> Workspace | None:
        ws = self._workspaces.get(workspace_id)
        if ws is None:
            return None
        cf = CustomField(
            field_id=_gen_id("cf"),
            name=name,
            field_type=field_type,
            required=required,
            options=options or [],
        )
        updated = replace(ws, custom_fields=[*ws.custom_fields, cf], updated_at=_now_iso())
        self._workspaces[workspace_id] = updated
        return updated

    # ------------------------------------------------------------------
    # Settings
    # ------------------------------------------------------------------

    def update_settings(
        self, workspace_id: str, **kwargs: Any
    ) -> Workspace | None:
        ws = self._workspaces.get(workspace_id)
        if ws is None:
            return None
        new_settings = replace(ws.settings, **kwargs)
        updated = replace(ws, settings=new_settings, updated_at=_now_iso())
        self._workspaces[workspace_id] = updated
        return updated

    def evaluate_workspace_policies(self, workspace_id: str) -> list[WorkspacePolicyFinding]:
        ws = self._workspaces.get(workspace_id)
        if ws is None:
            return [
                WorkspacePolicyFinding(
                    code="workspace_not_found",
                    severity="error",
                    message="Workspace was not found.",
                    entity_id=workspace_id,
                )
            ]

        findings: list[WorkspacePolicyFinding] = []
        domain = ws.settings.email_domain.strip().lower()
        if domain:
            domain = domain if domain.startswith("@") else f"@{domain}"
            for member in ws.members:
                if not member.email.lower().endswith(domain):
                    findings.append(
                        WorkspacePolicyFinding(
                            code="member_email_domain_mismatch",
                            severity="error",
                            message="Member email does not match the configured domain.",
                            entity_id=member.member_id,
                        )
                    )

        if not any(m.role in {WorkspaceRole.OWNER, WorkspaceRole.ADMIN} for m in ws.members):
            findings.append(
                WorkspacePolicyFinding(
                    code="missing_owner_or_admin",
                    severity="error",
                    message="Workspace must have at least one owner or admin member.",
                    entity_id=workspace_id,
                )
            )

        if ws.settings.working_hours_start >= ws.settings.working_hours_end:
            findings.append(
                WorkspacePolicyFinding(
                    code="invalid_working_hours",
                    severity="error",
                    message="Working hours start must be before working hours end.",
                    entity_id=workspace_id,
                )
            )

        if ws.settings.approval_workflow != ApprovalWorkflow.NONE:
            approvers = (
                ws.settings.metadata.get("approvers")
                or ws.settings.metadata.get("approval_approvers")
                or ws.settings.metadata.get("required_approvers")
            )
            if not approvers:
                findings.append(
                    WorkspacePolicyFinding(
                        code="missing_approval_workflow_metadata",
                        severity="error",
                        message="Approval workflow metadata must include approvers.",
                        entity_id=workspace_id,
                    )
                )

        return findings

    # ------------------------------------------------------------------
    # Plans
    # ------------------------------------------------------------------

    def add_plan(self, workspace_id: str, plan_id: str) -> Workspace | None:
        ws = self._workspaces.get(workspace_id)
        if ws is None:
            return None
        if plan_id in ws.plan_ids:
            return ws
        updated = replace(ws, plan_ids=[*ws.plan_ids, plan_id], updated_at=_now_iso())
        self._workspaces[workspace_id] = updated
        return updated

    # ------------------------------------------------------------------
    # Activity feed
    # ------------------------------------------------------------------

    def _record_activity(
        self,
        workspace_id: str,
        user_id: str,
        action: str,
        entity_type: str,
        entity_id: str,
        description: str = "",
    ) -> None:
        event = ActivityEvent(
            event_id=_gen_id("act"),
            workspace_id=workspace_id,
            user_id=user_id,
            action=action,
            entity_type=entity_type,
            entity_id=entity_id,
            description=description,
        )
        self._activity_feed.append(event)

    def get_activity_feed(
        self, workspace_id: str, *, limit: int = 50
    ) -> list[ActivityEvent]:
        events = [e for e in self._activity_feed if e.workspace_id == workspace_id]
        return events[-limit:]

    # ------------------------------------------------------------------
    # Team calendar
    # ------------------------------------------------------------------

    def build_team_calendar(
        self,
        workspace_id: str,
        plan_events: dict[str, list[dict[str, Any]]],
    ) -> list[CalendarEvent]:
        ws = self._workspaces.get(workspace_id)
        if ws is None:
            return []
        events: list[CalendarEvent] = []
        for plan_id in ws.plan_ids:
            for ev in plan_events.get(plan_id, []):
                events.append(
                    CalendarEvent(
                        event_id=ev.get("event_id", _gen_id("cal")),
                        plan_id=plan_id,
                        title=ev.get("title", ""),
                        start_date=ev.get("start_date", ""),
                        end_date=ev.get("end_date", ""),
                        event_type=ev.get("event_type", "milestone"),
                    )
                )
        events.sort(key=lambda e: e.start_date)
        return events

    # ------------------------------------------------------------------
    # Export / import
    # ------------------------------------------------------------------

    def export_workspace(self, workspace_id: str) -> dict[str, Any] | None:
        ws = self._workspaces.get(workspace_id)
        if ws is None:
            return None
        return ws.to_dict()

    def import_workspace(self, data: dict[str, Any]) -> Workspace:
        ws = Workspace(
            workspace_id=data.get("workspace_id", _gen_id("ws")),
            name=data.get("name", "Imported"),
            description=data.get("description", ""),
            owner_id=data.get("owner_id", ""),
            plan_ids=data.get("plan_ids", []),
        )
        self._workspaces[ws.workspace_id] = ws
        return ws


__all__ = ["TeamWorkspace"]
