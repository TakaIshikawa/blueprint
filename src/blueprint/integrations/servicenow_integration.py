"""ServiceNow integration for ITSM workflow sync."""

from __future__ import annotations

import base64
import json
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Literal
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

HttpOpen = Callable[..., Any]

# ---------------------------------------------------------------------------
# Approval workflow states
# ---------------------------------------------------------------------------

CHANGE_STATES: dict[str, str] = {
    "-5": "draft",
    "-4": "requested",
    "-3": "authorize",
    "-2": "authorized",
    "-1": "scheduled",
    "0": "implementing",
    "1": "review",
    "2": "closed",
}

# Map ServiceNow change states to blueprint plan statuses
CHANGE_STATE_TO_PLAN_STATUS: dict[str, str] = {
    "draft": "draft",
    "requested": "draft",
    "authorize": "draft",
    "authorized": "ready",
    "scheduled": "queued",
    "implementing": "in_progress",
    "review": "in_progress",
    "closed": "completed",
}

# Map blueprint plan statuses to ServiceNow change states
PLAN_STATUS_TO_CHANGE_STATE: dict[str, str] = {
    "draft": "-5",
    "ready": "-2",
    "queued": "-1",
    "in_progress": "0",
    "completed": "2",
    "failed": "2",
}

# Map blueprint task statuses to ServiceNow task states
TASK_STATUS_TO_SN_STATE: dict[str, str] = {
    "pending": "1",  # Open
    "in_progress": "2",  # Work in Progress
    "completed": "3",  # Closed Complete
    "blocked": "-5",  # Pending
    "skipped": "4",  # Closed Skipped
}

ApprovalStatus = Literal[
    "draft",
    "requested",
    "authorize",
    "authorized",
    "scheduled",
    "implementing",
    "review",
    "closed",
]


# ---------------------------------------------------------------------------
# Data containers
# ---------------------------------------------------------------------------

@dataclass
class ServiceNowClient:
    """Lightweight client for ServiceNow REST API."""

    instance: str
    username: str
    password: str
    http_open: HttpOpen = urlopen

    @property
    def base_url(self) -> str:
        instance = self.instance.rstrip("/")
        if not instance.startswith("http"):
            instance = f"https://{instance}"
        return instance

    def get(self, table: str, sys_id: str) -> dict[str, Any]:
        """Fetch a single record from a table."""
        url = f"{self.base_url}/api/now/table/{quote(table)}/{quote(sys_id)}"
        return self._request("GET", url)

    def query(
        self,
        table: str,
        params: dict[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        """Query records from a table."""
        url = f"{self.base_url}/api/now/table/{quote(table)}"
        if params:
            url = f"{url}?{urlencode(params)}"
        result = self._request("GET", url)
        return result.get("result", []) if isinstance(result, dict) else []

    def create(self, table: str, data: dict[str, Any]) -> dict[str, Any]:
        """Create a record in a table."""
        url = f"{self.base_url}/api/now/table/{quote(table)}"
        return self._request("POST", url, body=data)

    def update(self, table: str, sys_id: str, data: dict[str, Any]) -> dict[str, Any]:
        """Update a record in a table."""
        url = f"{self.base_url}/api/now/table/{quote(table)}/{quote(sys_id)}"
        return self._request("PATCH", url, body=data)

    def _request(
        self,
        method: str,
        url: str,
        body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        headers = self._headers()
        data = None
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"

        request = Request(url, data=data, headers=headers, method=method)

        try:
            with self.http_open(request, timeout=30) as response:
                raw = response.read()
        except HTTPError as exc:
            raise ImportError(
                f"ServiceNow API request failed with HTTP {exc.code}: {url}"
            ) from exc
        except URLError as exc:
            raise ImportError(
                f"ServiceNow API request failed: {exc.reason}"
            ) from exc

        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ImportError("ServiceNow API returned invalid JSON") from exc

    def _headers(self) -> dict[str, str]:
        credentials = base64.b64encode(
            f"{self.username}:{self.password}".encode()
        ).decode()
        return {
            "Authorization": f"Basic {credentials}",
            "Accept": "application/json",
            "User-Agent": "blueprint-servicenow-integration",
        }


@dataclass
class ChangeRequest:
    """Representation of a ServiceNow change request."""

    sys_id: str
    number: str
    short_description: str
    description: str
    state: str
    assignment_group: str
    scheduled_start_date: str | None = None
    scheduled_end_date: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class SyncResult:
    """Result of a bidirectional sync operation."""

    change_request_id: str
    plan_id: str
    tasks_synced: int
    tasks_created: int
    tasks_updated: int
    status_updated: bool
    direction: str
    errors: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Main integration class
# ---------------------------------------------------------------------------

class ServiceNowIntegration:
    """Sync execution plans with ServiceNow change requests and project tasks."""

    def __init__(
        self,
        *,
        instance: str | None = None,
        username_env: str = "SERVICENOW_USERNAME",
        password_env: str = "SERVICENOW_PASSWORD",
        instance_env: str = "SERVICENOW_INSTANCE",
        http_open: HttpOpen = urlopen,
    ):
        self.instance = instance
        self.username_env = username_env
        self.password_env = password_env
        self.instance_env = instance_env
        self.http_open = http_open

    def authenticate(
        self,
        instance: str,
        username: str,
        password: str,
    ) -> ServiceNowClient:
        """Create an authenticated ServiceNow client and validate credentials."""
        client = ServiceNowClient(
            instance=instance,
            username=username,
            password=password,
            http_open=self.http_open,
        )
        # Validate by querying the sys_user table
        client.query("sys_user", {"sysparm_limit": "1"})
        return client

    def map_plan_to_change_request(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Map an ExecutionPlan to a ServiceNow change request payload."""
        brief = brief or {}
        tasks = plan.get("tasks") or []
        milestones = plan.get("milestones") or []

        # Build implementation plan from tasks
        impl_plan_lines = []
        for i, task in enumerate(tasks, 1):
            impl_plan_lines.append(
                f"{i}. {task.get('title', 'Untitled')} "
                f"[{task.get('status', 'pending')}]"
            )
        implementation_plan = "\n".join(impl_plan_lines)

        # Risk analysis from tasks
        risk_lines = []
        for task in tasks:
            risk = task.get("risk_level")
            if risk and risk in ("high", "medium"):
                risk_lines.append(f"- {task.get('title', '?')}: {risk} risk")
        risk_impact = "\n".join(risk_lines) if risk_lines else "Low risk change"

        # Schedule from metadata or milestones
        scheduled_start = None
        scheduled_end = None
        for ms in milestones:
            start = ms.get("start_date")
            end = ms.get("due_date") or ms.get("end_date")
            if start and not scheduled_start:
                scheduled_start = start
            if end:
                scheduled_end = end

        description_parts = [
            brief.get("problem_statement") or plan.get("id") or "",
            "",
            f"Plan ID: {plan.get('id', '')}",
            f"Target: {plan.get('target_repo') or plan.get('target_engine') or 'N/A'}",
            f"Tasks: {len(tasks)}",
        ]

        change_data: dict[str, Any] = {
            "short_description": brief.get("title") or f"Blueprint Plan: {plan.get('id', '')}",
            "description": "\n".join(description_parts),
            "implementation_plan": implementation_plan,
            "risk_impact_analysis": risk_impact,
            "type": "normal",
            "category": "Software",
            "state": PLAN_STATUS_TO_CHANGE_STATE.get(
                plan.get("status", "draft"), "-5"
            ),
        }

        if scheduled_start:
            change_data["start_date"] = scheduled_start
        if scheduled_end:
            change_data["end_date"] = scheduled_end

        assignment_group = (plan.get("metadata") or {}).get("assignment_group")
        if assignment_group:
            change_data["assignment_group"] = assignment_group

        return change_data

    def create_change_request(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any] | None = None,
    ) -> ChangeRequest:
        """Create a ServiceNow change request from a plan."""
        client = self._build_client()
        payload = self.map_plan_to_change_request(plan, brief)
        result = client.create("change_request", payload)
        record = result.get("result", result)

        return ChangeRequest(
            sys_id=record.get("sys_id", ""),
            number=record.get("number", ""),
            short_description=record.get("short_description", ""),
            description=record.get("description", ""),
            state=record.get("state", ""),
            assignment_group=record.get("assignment_group", ""),
            raw=record,
        )

    def sync_tasks_to_project_tasks(
        self,
        plan: dict[str, Any],
        change_sys_id: str,
    ) -> list[dict[str, Any]]:
        """Sync blueprint tasks as ServiceNow project tasks linked to a change request."""
        client = self._build_client()
        tasks = plan.get("tasks") or []
        created: list[dict[str, Any]] = []

        for task in tasks:
            sn_task = self._map_task_to_project_task(task, change_sys_id)
            result = client.create("pm_project_task", sn_task)
            record = result.get("result", result)
            created.append(record)

        return created

    def track_change_approval(self, change_sys_id: str) -> ApprovalStatus:
        """Track the approval workflow state of a change request."""
        client = self._build_client()
        result = client.get("change_request", change_sys_id)
        record = result.get("result", result)

        state_value = str(record.get("state", "-5"))
        return CHANGE_STATES.get(state_value, "draft")  # type: ignore[return-value]

    def update_plan_status_from_change(
        self,
        change_sys_id: str,
    ) -> dict[str, str]:
        """Get the blueprint plan status corresponding to a change request's state."""
        approval_status = self.track_change_approval(change_sys_id)
        plan_status = CHANGE_STATE_TO_PLAN_STATUS.get(approval_status, "draft")
        return {
            "change_state": approval_status,
            "plan_status": plan_status,
        }

    def sync_task_completion(
        self,
        task: dict[str, Any],
        sn_task_sys_id: str,
    ) -> dict[str, Any]:
        """Sync task completion back to ServiceNow."""
        client = self._build_client()
        status = task.get("status", "pending")
        sn_state = TASK_STATUS_TO_SN_STATE.get(status, "1")

        update_data: dict[str, Any] = {
            "state": sn_state,
        }

        if status == "completed":
            update_data["percent_complete"] = "100"
        elif status == "in_progress":
            update_data["percent_complete"] = "50"

        result = client.update("pm_project_task", sn_task_sys_id, update_data)
        return result.get("result", result)

    def bidirectional_sync(
        self,
        plan: dict[str, Any],
        change_sys_id: str,
    ) -> SyncResult:
        """Perform bidirectional sync between plan and change request."""
        client = self._build_client()
        errors: list[str] = []

        # 1. Update plan status from change request
        status_info = self.update_plan_status_from_change(change_sys_id)
        status_updated = status_info["plan_status"] != plan.get("status", "draft")

        # 2. Get existing ServiceNow tasks for this change
        existing_sn_tasks = client.query("pm_project_task", {
            "sysparm_query": f"change_request={change_sys_id}",
        })
        existing_map: dict[str, dict[str, Any]] = {}
        for sn_task in existing_sn_tasks:
            desc = sn_task.get("description") or ""
            # Extract blueprint task ID from description
            if "Blueprint:" in desc:
                bp_id = desc.split("Blueprint:")[-1].strip().split()[0]
                existing_map[bp_id] = sn_task

        # 3. Sync blueprint tasks to ServiceNow
        tasks = plan.get("tasks") or []
        tasks_created = 0
        tasks_updated = 0

        for task in tasks:
            task_id = task.get("id", "")
            if task_id in existing_map:
                # Update existing
                sn_task = existing_map[task_id]
                sn_sys_id = sn_task.get("sys_id", "")
                try:
                    self.sync_task_completion(task, sn_sys_id)
                    tasks_updated += 1
                except Exception as exc:
                    errors.append(f"Failed to update {task_id}: {exc}")
            else:
                # Create new
                try:
                    sn_data = self._map_task_to_project_task(task, change_sys_id)
                    client.create("pm_project_task", sn_data)
                    tasks_created += 1
                except Exception as exc:
                    errors.append(f"Failed to create {task_id}: {exc}")

        return SyncResult(
            change_request_id=change_sys_id,
            plan_id=plan.get("id", ""),
            tasks_synced=len(tasks),
            tasks_created=tasks_created,
            tasks_updated=tasks_updated,
            status_updated=status_updated,
            direction="bidirectional",
            errors=errors,
        )

    def generate_implementation_plan(
        self,
        plan: dict[str, Any],
    ) -> str:
        """Generate a formatted implementation plan for ServiceNow."""
        tasks = plan.get("tasks") or []
        milestones = plan.get("milestones") or []

        lines = [
            f"Implementation Plan: {plan.get('id', '')}",
            f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}",
            "",
        ]

        if milestones:
            lines.append("Milestones:")
            for ms in milestones:
                lines.append(f"  - {ms.get('name', 'Unnamed')}")
            lines.append("")

        lines.append("Tasks:")
        for i, task in enumerate(tasks, 1):
            status = task.get("status", "pending")
            title = task.get("title", "Untitled")
            complexity = task.get("estimated_complexity") or "unknown"
            hours = task.get("estimated_hours")
            hours_str = f" ({hours:.1f}h)" if hours else ""
            deps = task.get("depends_on") or []
            deps_str = f" [depends on: {', '.join(str(d) for d in deps)}]" if deps else ""

            lines.append(f"  {i}. [{status}] {title} ({complexity}){hours_str}{deps_str}")

        return "\n".join(lines)

    # -- Private helpers ---------------------------------------------------

    def _build_client(self) -> ServiceNowClient:
        instance = self.instance or os.getenv(self.instance_env) or ""
        username = os.getenv(self.username_env) or ""
        password = os.getenv(self.password_env) or ""

        if not instance:
            raise ValueError("ServiceNow instance URL is required")

        return ServiceNowClient(
            instance=instance,
            username=username,
            password=password,
            http_open=self.http_open,
        )

    def _map_task_to_project_task(
        self,
        task: dict[str, Any],
        change_sys_id: str,
    ) -> dict[str, Any]:
        """Map a blueprint task to a ServiceNow project task payload."""
        status = task.get("status", "pending")
        sn_state = TASK_STATUS_TO_SN_STATE.get(status, "1")

        # Build description with blueprint reference
        desc_parts = [
            task.get("description") or task.get("title") or "",
            "",
            f"Blueprint: {task.get('id', '')}",
        ]

        criteria = task.get("acceptance_criteria") or []
        if criteria:
            desc_parts.append("")
            desc_parts.append("Acceptance Criteria:")
            for c in criteria:
                desc_parts.append(f"  - {c}")

        priority_map = {
            "high": "1",
            "medium": "2",
            "low": "3",
        }
        risk = task.get("risk_level") or "medium"
        priority = priority_map.get(risk, "2")

        percent = "0"
        if status == "completed":
            percent = "100"
        elif status == "in_progress":
            percent = "50"

        return {
            "short_description": task.get("title") or "Untitled Task",
            "description": "\n".join(desc_parts),
            "state": sn_state,
            "priority": priority,
            "percent_complete": percent,
            "change_request": change_sys_id,
        }
