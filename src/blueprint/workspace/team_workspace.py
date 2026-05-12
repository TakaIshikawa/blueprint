"""Team workspace manager for organizing plans by team.

Provides member management, shared resource pools, workspace-level
templates, default settings, activity feeds, and team calendars.
"""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import replace
from enum import Enum
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
    WorkspaceActivityDigest,
    WorkspaceMemberCapacity,
    WorkspaceMemberCapacityReport,
    WorkspaceResourceCapacity,
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
        metadata: dict[str, Any] | None = None,
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
            metadata=metadata or {},
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
        metadata: dict[str, Any] | None = None,
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
            metadata=metadata or {},
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

    def instantiate_template(
        self,
        workspace_id: str,
        template_id: str,
        *,
        overrides: dict[str, Any] | None = None,
        plan_id: str | None = None,
        add_to_workspace: bool = False,
    ) -> dict[str, Any] | None:
        """Create a plan scaffold from a workspace template."""
        ws = self._workspaces.get(workspace_id)
        if ws is None:
            return None

        template = next(
            (
                item
                for item in ws.templates
                if item.template_id == template_id or item.name == template_id
            ),
            None,
        )
        if template is None:
            return None

        generated_plan_id = plan_id or _gen_id("plan")
        scaffold = deepcopy(template.template_data)
        scaffold.update(overrides or {})
        scaffold["id"] = generated_plan_id
        scaffold["workspace_id"] = workspace_id
        scaffold["template_id"] = template.template_id
        scaffold.setdefault("template_name", template.name)

        if add_to_workspace and generated_plan_id not in ws.plan_ids:
            updated = replace(
                ws,
                plan_ids=[*ws.plan_ids, generated_plan_id],
                updated_at=_now_iso(),
            )
            self._workspaces[workspace_id] = updated

        return scaffold

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
        self,
        workspace_id: str,
        *,
        limit: int = 50,
        user_id: str | None = None,
        action: str | None = None,
        entity_type: str | None = None,
        since: str | None = None,
        until: str | None = None,
    ) -> list[ActivityEvent]:
        events = [e for e in self._activity_feed if e.workspace_id == workspace_id]
        if user_id is not None:
            events = [e for e in events if e.user_id == user_id]
        if action is not None:
            events = [e for e in events if e.action == action]
        if entity_type is not None:
            events = [e for e in events if e.entity_type == entity_type]
        if since is not None or until is not None:
            events = [e for e in events if _within_window(e.timestamp, since, until)]
        return events[-limit:]

    def build_activity_digest(
        self,
        workspace_id: str,
        *,
        window_start: str | None = None,
        window_end: str | None = None,
        since: str | None = None,
        until: str | None = None,
    ) -> WorkspaceActivityDigest:
        window_start = window_start or since
        window_end = window_end or until
        events = [
            event
            for event in self._activity_feed
            if event.workspace_id == workspace_id and _within_window(event.timestamp, window_start, window_end)
        ]
        return _activity_digest(workspace_id, events, window_start=window_start, window_end=window_end)

    def build_all_workspace_activity_digest(
        self,
        *,
        window_start: str | None = None,
        window_end: str | None = None,
        since: str | None = None,
        until: str | None = None,
    ) -> WorkspaceActivityDigest:
        window_start = window_start or since
        window_end = window_end or until
        events = [
            event
            for event in self._activity_feed
            if _within_window(event.timestamp, window_start, window_end)
        ]
        return _activity_digest(None, events, window_start=window_start, window_end=window_end)

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

    def build_member_capacity_report(
        self,
        workspace_id: str,
        plan_events: dict[str, list[dict[str, Any]]] | None = None,
    ) -> WorkspaceMemberCapacityReport | None:
        ws = self._workspaces.get(workspace_id)
        if ws is None:
            return None
        events = self.build_team_calendar(workspace_id, plan_events or {})
        raw_events = [event for plan_id in ws.plan_ids for event in (plan_events or {}).get(plan_id, [])]
        members = tuple(
            WorkspaceMemberCapacity(
                member_id=member.member_id,
                user_id=member.user_id,
                display_name=member.display_name,
                email=member.email,
                role=member.role.value,
                capacity_metadata=_capacity_metadata(member.metadata),
                calendar_event_count=_member_event_count(member, raw_events, events),
            )
            for member in sorted(ws.members, key=lambda item: (item.display_name, item.user_id, item.member_id))
        )
        resources = tuple(
            WorkspaceResourceCapacity(
                resource_id=resource.resource_id,
                name=resource.name,
                resource_type=resource.resource_type,
                total_capacity=resource.total_capacity,
                allocated=resource.allocated,
                unit=resource.unit,
                utilization_metadata=_utilization_metadata(resource.metadata),
            )
            for resource in sorted(ws.resources, key=lambda item: (item.name, item.resource_id))
        )
        return WorkspaceMemberCapacityReport(workspace_id=workspace_id, members=members, resources=resources)

    # ------------------------------------------------------------------
    # Export / import
    # ------------------------------------------------------------------

    def export_workspace(self, workspace_id: str) -> dict[str, Any] | None:
        ws = self._workspaces.get(workspace_id)
        if ws is None:
            return None
        return ws.to_dict()

    def build_configuration_snapshot(self, workspace_id: str) -> dict[str, Any] | None:
        """Return a portable workspace configuration summary without member details."""
        ws = self._workspaces.get(workspace_id)
        if ws is None:
            return None
        return workspace_configuration_snapshot(ws)

    def build_integration_inventory(self, workspace_id: str) -> dict[str, Any] | None:
        """Return configured integration readiness without exposing secret values."""
        ws = self._workspaces.get(workspace_id)
        if ws is None:
            return None
        return workspace_integration_inventory(ws)

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


def workspace_configuration_snapshot(
    workspace: Workspace,
    *,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Build a JSON-ready workspace configuration snapshot.

    The snapshot intentionally avoids per-member identifiers, emails, names, and
    metadata while retaining aggregate role counts for diagnostics.
    """
    settings = workspace.settings
    feature_flags = _feature_flags(workspace)
    configuration_identifiers = {
        "template_ids": [template.template_id for template in workspace.templates],
        "custom_field_ids": [field.field_id for field in workspace.custom_fields],
        "resource_ids": [resource.resource_id for resource in workspace.resources],
        "plan_ids": list(workspace.plan_ids),
    }

    return _json_safe(
        {
            "snapshot_type": "workspace_configuration",
            "generated_at": generated_at or _now_iso(),
            "workspace": {
                "workspace_id": workspace.workspace_id,
                "name": workspace.name,
                "description": workspace.description,
                "owner_id": workspace.owner_id,
                "created_at": workspace.created_at,
                "updated_at": workspace.updated_at,
            },
            "member_counts": {
                "total": len(workspace.members),
                "by_role": _counts(member.role.value for member in workspace.members),
            },
            "configuration_identifiers": configuration_identifiers,
            "settings": {
                "working_hours_start": settings.working_hours_start,
                "working_hours_end": settings.working_hours_end,
                "timezone": settings.timezone,
                "holidays": list(settings.holidays),
                "approval_workflow": settings.approval_workflow.value,
                "slack_channel": settings.slack_channel,
                "email_domain": settings.email_domain,
                "metadata": deepcopy(settings.metadata),
            },
            "feature_flags": feature_flags,
            "counts": {
                "templates": len(workspace.templates),
                "custom_fields": len(workspace.custom_fields),
                "resources": len(workspace.resources),
                "plans": len(workspace.plan_ids),
            },
        }
    )


def workspace_integration_inventory(
    workspace: Workspace,
    *,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Build a deterministic inventory of configured workspace integrations."""
    entries = [
        _integration_inventory_entry(record, index)
        for index, record in enumerate(_workspace_integration_records(workspace))
    ]
    entries.sort(
        key=lambda entry: (
            entry["integration_type"],
            entry["integration_id"],
            entry["owner"],
        )
    )
    return _json_safe(
        {
            "inventory_type": "workspace_integration_inventory",
            "generated_at": generated_at or _now_iso(),
            "workspace_id": workspace.workspace_id,
            "integration_count": len(entries),
            "readiness_counts": {
                status: sum(1 for entry in entries if entry["readiness_status"] == status)
                for status in ("disabled", "missing_required_settings", "ready")
            },
            "integrations": entries,
        }
    )


def _workspace_integration_records(workspace: Workspace) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for source in (workspace.metadata, workspace.settings.metadata):
        raw_records = source.get("integrations") or source.get("workspace_integrations")
        if isinstance(raw_records, Mapping):
            for key in sorted(raw_records, key=str):
                raw_record = raw_records[key]
                if isinstance(raw_record, Mapping):
                    record = dict(raw_record)
                    record.setdefault("integration_id", str(key))
                    records.append(record)
        elif isinstance(raw_records, (list, tuple)):
            records.extend(dict(item) for item in raw_records if isinstance(item, Mapping))
    return records


def _integration_inventory_entry(record: Mapping[str, Any], index: int) -> dict[str, Any]:
    integration_id = _integration_value(
        record,
        ("integration_id", "id", "name", "slug"),
        f"integration-{index + 1}",
    )
    integration_type = _integration_value(
        record,
        ("integration_type", "type", "provider", "service"),
        "unknown",
    )
    owner = _integration_value(record, ("owner", "owner_id", "configured_by"), "")
    enabled = _integration_enabled(record)
    missing_required_settings = _missing_required_integration_settings(record)
    if not enabled:
        readiness_status = "disabled"
    elif missing_required_settings:
        readiness_status = "missing_required_settings"
    else:
        readiness_status = "ready"

    return {
        "integration_id": integration_id,
        "integration_type": integration_type,
        "enabled": enabled,
        "owner": owner,
        "missing_required_settings": missing_required_settings,
        "readiness_status": readiness_status,
    }


def _integration_value(
    record: Mapping[str, Any],
    keys: tuple[str, ...],
    default: str,
) -> str:
    for key in keys:
        value = record.get(key)
        if value not in (None, ""):
            return str(value)
    return default


def _integration_enabled(record: Mapping[str, Any]) -> bool:
    value = record.get("enabled", True)
    if isinstance(value, str):
        return value.strip().lower() not in {"0", "false", "no", "off", "disabled"}
    return bool(value)


def _missing_required_integration_settings(record: Mapping[str, Any]) -> list[str]:
    required = record.get("required_settings") or record.get("required") or []
    if isinstance(required, str):
        required_names = [required]
    elif isinstance(required, (list, tuple, set, frozenset)):
        required_names = [str(item) for item in required if str(item)]
    else:
        required_names = []

    settings = record.get("settings")
    if not isinstance(settings, Mapping):
        settings = record.get("config")
    configured = settings if isinstance(settings, Mapping) else record
    missing = [
        name
        for name in required_names
        if configured.get(name) in (None, "")
    ]
    return sorted(set(missing))


def _activity_digest(
    workspace_id: str | None,
    events: list[ActivityEvent],
    *,
    window_start: str | None = None,
    window_end: str | None = None,
) -> WorkspaceActivityDigest:
    sorted_events = sorted(events, key=lambda event: (event.timestamp, event.event_id))
    return WorkspaceActivityDigest(
        workspace_id=workspace_id,
        total_event_count=len(sorted_events),
        counts_by_action=_counts(event.action for event in sorted_events),
        counts_by_actor=_counts(event.user_id for event in sorted_events),
        counts_by_target_type=_counts(event.entity_type for event in sorted_events),
        latest_activity_timestamp=sorted_events[-1].timestamp if sorted_events else None,
        window_start=window_start,
        window_end=window_end,
    )


def _counts(values: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        key = str(value)
        counts[key] = counts.get(key, 0) + 1
    return {key: counts[key] for key in sorted(counts)}


def _within_window(timestamp: str, window_start: str | None, window_end: str | None) -> bool:
    if window_start is not None and timestamp < window_start:
        return False
    if window_end is not None and timestamp > window_end:
        return False
    return True


def _capacity_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    keys = ("capacity", "capacity_hours", "weekly_capacity", "assigned_capacity", "availability", "fte")
    return {key: metadata[key] for key in sorted(keys) if key in metadata}


def _utilization_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    keys = ("utilization", "utilization_percent", "used_capacity", "utilized", "allocated_percent", "reserved", "available", "notes")
    return {key: metadata[key] for key in sorted(keys) if key in metadata}


def _feature_flags(workspace: Workspace) -> dict[str, Any]:
    flags: dict[str, Any] = {}
    for source in (workspace.metadata, workspace.settings.metadata):
        raw_flags = source.get("feature_flags", {})
        if isinstance(raw_flags, dict):
            flags.update(raw_flags)
        elif isinstance(raw_flags, (list, tuple, set)):
            flags.update({str(flag): True for flag in raw_flags})
    return {key: flags[key] for key in sorted(flags)}


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, set):
        return sorted((_json_safe(item) for item in value), key=str)
    return str(value)


def _member_event_count(
    member: TeamMember,
    raw_events: list[dict[str, Any]],
    calendar_events: list[CalendarEvent],
) -> int:
    matched = 0
    identifiers = {member.member_id, member.user_id, member.email, member.display_name}
    for event in raw_events:
        candidates = (
            event.get("member_id"),
            event.get("user_id"),
            event.get("assignee_id"),
            event.get("owner_id"),
            event.get("assignee"),
            event.get("owner"),
            event.get("actor"),
            event.get("email"),
        )
        attendees = event.get("attendees") or event.get("members") or []
        if isinstance(attendees, str):
            attendees = [attendees]
        if any(candidate in identifiers for candidate in candidates if candidate):
            matched += 1
        elif any(attendee in identifiers for attendee in attendees):
            matched += 1
    return matched if raw_events else 0


__all__ = ["TeamWorkspace", "workspace_configuration_snapshot", "workspace_integration_inventory"]
