"""Team workspace manager for organizing plans by team.

Provides member management, shared resource pools, workspace-level
templates, default settings, activity feeds, and team calendars.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from blueprint.workspace.workspace_model import (
    ActivityEvent,
    CalendarEvent,
    CustomField,
    SharedResource,
    TeamMember,
    Workspace,
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
