"""n8n workflow node integration for self-hosted automation."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _gen_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


# ---------------------------------------------------------------------------
# Node types and models
# ---------------------------------------------------------------------------


class NodeType(str, Enum):
    """Types of n8n nodes."""

    TRIGGER = "trigger"
    REGULAR = "regular"


class TriggerMode(str, Enum):
    """How a trigger node receives events."""

    POLLING = "polling"
    WEBHOOK = "webhook"


class ResourceType(str, Enum):
    """Resource types for dynamic dropdowns."""

    PROJECT = "project"
    TASK = "task"
    MILESTONE = "milestone"
    USER = "user"
    STATUS = "status"


class Operation(str, Enum):
    """CRUD operations for regular nodes."""

    CREATE = "create"
    READ = "read"
    UPDATE = "update"
    DELETE = "delete"
    LIST = "list"


@dataclass(frozen=True, slots=True)
class N8nCredential:
    """Credential type for n8n API authentication."""

    credential_id: str
    api_key: str
    base_url: str = ""
    label: str = "Blueprint API"
    created_at: str = field(default_factory=_now_iso)


@dataclass(frozen=True, slots=True)
class N8nWebhook:
    """Webhook registration for trigger nodes."""

    webhook_id: str
    url: str
    event_types: list[str] = field(default_factory=list)
    credential_id: str = ""
    active: bool = True
    created_at: str = field(default_factory=_now_iso)


@dataclass(frozen=True, slots=True)
class NodeDefinition:
    """n8n node definition following their node structure."""

    name: str
    display_name: str
    description: str
    node_type: NodeType
    icon: str = "file:blueprint.svg"
    group: str = "transform"
    version: int = 1
    properties: list[dict[str, Any]] = field(default_factory=list)
    credentials: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Node definitions
# ---------------------------------------------------------------------------


TRIGGER_NODE = NodeDefinition(
    name="blueprintTrigger",
    display_name="Blueprint Trigger",
    description="Triggers when events occur in Blueprint plans",
    node_type=NodeType.TRIGGER,
    group="trigger",
    properties=[
        {
            "displayName": "Trigger Mode",
            "name": "triggerMode",
            "type": "options",
            "options": [
                {"name": "Webhook", "value": "webhook"},
                {"name": "Polling", "value": "polling"},
            ],
            "default": "webhook",
        },
        {
            "displayName": "Event",
            "name": "event",
            "type": "options",
            "options": [
                {"name": "Task Created", "value": "task_created"},
                {"name": "Task Updated", "value": "task_updated"},
                {"name": "Task Completed", "value": "task_completed"},
                {"name": "Milestone Reached", "value": "milestone_reached"},
                {"name": "Plan Updated", "value": "plan_updated"},
            ],
            "default": "task_created",
        },
        {
            "displayName": "Plan ID",
            "name": "planId",
            "type": "string",
            "default": "",
            "description": "Filter events to a specific plan (optional)",
        },
        {
            "displayName": "Poll Interval",
            "name": "pollInterval",
            "type": "number",
            "default": 60,
            "description": "Polling interval in seconds (polling mode only)",
            "displayOptions": {"show": {"triggerMode": ["polling"]}},
        },
    ],
    credentials=[
        {"name": "blueprintApi", "required": True},
    ],
)

REGULAR_NODE = NodeDefinition(
    name="blueprint",
    display_name="Blueprint",
    description="Perform operations on Blueprint plans and tasks",
    node_type=NodeType.REGULAR,
    properties=[
        {
            "displayName": "Resource",
            "name": "resource",
            "type": "options",
            "options": [
                {"name": "Task", "value": "task"},
                {"name": "Milestone", "value": "milestone"},
                {"name": "Plan", "value": "plan"},
            ],
            "default": "task",
        },
        {
            "displayName": "Operation",
            "name": "operation",
            "type": "options",
            "options": [
                {"name": "Create", "value": "create"},
                {"name": "Read", "value": "read"},
                {"name": "Update", "value": "update"},
                {"name": "Delete", "value": "delete"},
                {"name": "List", "value": "list"},
            ],
            "default": "read",
        },
        {
            "displayName": "ID",
            "name": "itemId",
            "type": "string",
            "default": "",
            "description": "The ID of the item",
            "displayOptions": {
                "show": {"operation": ["read", "update", "delete"]},
            },
        },
        {
            "displayName": "Title",
            "name": "title",
            "type": "string",
            "default": "",
            "displayOptions": {"show": {"operation": ["create", "update"]}},
        },
        {
            "displayName": "Description",
            "name": "description",
            "type": "string",
            "default": "",
            "displayOptions": {"show": {"operation": ["create", "update"]}},
        },
        {
            "displayName": "Status",
            "name": "status",
            "type": "options",
            "options": [
                {"name": "Pending", "value": "pending"},
                {"name": "In Progress", "value": "in_progress"},
                {"name": "Completed", "value": "completed"},
                {"name": "Blocked", "value": "blocked"},
            ],
            "default": "pending",
            "displayOptions": {"show": {"operation": ["create", "update"]}},
        },
        {
            "displayName": "Plan ID",
            "name": "planId",
            "type": "string",
            "default": "",
            "displayOptions": {"show": {"operation": ["create", "list"]}},
        },
        {
            "displayName": "Limit",
            "name": "limit",
            "type": "number",
            "default": 50,
            "displayOptions": {"show": {"operation": ["list"]}},
        },
        {
            "displayName": "Offset",
            "name": "offset",
            "type": "number",
            "default": 0,
            "displayOptions": {"show": {"operation": ["list"]}},
        },
    ],
    credentials=[
        {"name": "blueprintApi", "required": True},
    ],
)

CREDENTIAL_DEFINITION: dict[str, Any] = {
    "name": "blueprintApi",
    "displayName": "Blueprint API",
    "properties": [
        {
            "displayName": "API Key",
            "name": "apiKey",
            "type": "string",
            "typeOptions": {"password": True},
            "default": "",
        },
        {
            "displayName": "Base URL",
            "name": "baseUrl",
            "type": "string",
            "default": "",
            "description": "Blueprint API base URL",
        },
    ],
    "authenticate": {
        "type": "generic",
        "properties": {
            "headers": {"Authorization": "Bearer={{$credentials.apiKey}}"},
        },
    },
}


# ---------------------------------------------------------------------------
# Main integration class
# ---------------------------------------------------------------------------

HttpSender = Callable[..., Any]


class N8nIntegration:
    """Manages n8n node execution, webhooks, and resource mapping."""

    def __init__(
        self,
        *,
        http_sender: HttpSender | None = None,
        max_retries: int = 3,
    ):
        self._credentials: dict[str, N8nCredential] = {}
        self._webhooks: dict[str, N8nWebhook] = {}
        self._tasks: dict[str, dict[str, Any]] = {}
        self._milestones: dict[str, dict[str, Any]] = {}
        self._plans: dict[str, dict[str, Any]] = {}
        self._http_sender = http_sender
        self._max_retries = max_retries

    # -- Credential management ----------------------------------------------

    def create_credential(
        self,
        api_key: str,
        *,
        base_url: str = "",
        label: str = "Blueprint API",
    ) -> N8nCredential:
        """Create a new credential for n8n authentication."""
        cred = N8nCredential(
            credential_id=_gen_id("cred"),
            api_key=api_key,
            base_url=base_url,
            label=label,
        )
        self._credentials[cred.credential_id] = cred
        return cred

    def verify_credential(self, credential_id: str) -> bool:
        """Verify that a credential is valid."""
        return credential_id in self._credentials

    # -- Trigger node -------------------------------------------------------

    def register_webhook(
        self,
        url: str,
        event_types: list[str],
        credential_id: str,
    ) -> N8nWebhook | None:
        """Register a webhook for the trigger node."""
        if credential_id not in self._credentials:
            return None

        webhook = N8nWebhook(
            webhook_id=_gen_id("n8n-wh"),
            url=url,
            event_types=event_types,
            credential_id=credential_id,
        )
        self._webhooks[webhook.webhook_id] = webhook
        return webhook

    def unregister_webhook(self, webhook_id: str) -> bool:
        """Unregister a webhook."""
        return self._webhooks.pop(webhook_id, None) is not None

    def poll_events(
        self,
        event_type: str,
        credential_id: str,
        *,
        plan_id: str | None = None,
        since: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]] | None:
        """Poll for events (polling trigger mode)."""
        if credential_id not in self._credentials:
            return None

        items = list(self._tasks.values())
        if plan_id:
            items = [i for i in items if i.get("plan_id") == plan_id]
        if since:
            items = [i for i in items if i.get("created_at", "") > since]
        return items[:limit]

    def fire_webhook_event(
        self,
        event_type: str,
        data: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Fire an event to all matching webhooks."""
        results: list[dict[str, Any]] = []
        for webhook in self._webhooks.values():
            if event_type in webhook.event_types and webhook.active:
                result = self._deliver_with_retry(webhook, event_type, data)
                results.append(result)
        return results

    # -- Regular node (CRUD) ------------------------------------------------

    def execute_operation(
        self,
        resource: str,
        operation: str,
        params: dict[str, Any],
        credential_id: str,
    ) -> dict[str, Any] | list[dict[str, Any]] | None:
        """Execute a regular node operation."""
        if credential_id not in self._credentials:
            return None

        store_map: dict[str, dict[str, dict[str, Any]]] = {
            "task": self._tasks,
            "milestone": self._milestones,
            "plan": self._plans,
        }

        store = store_map.get(resource)
        if store is None:
            return {"error": f"Unknown resource: {resource}"}

        if operation == "create":
            return self._op_create(store, resource, params)
        elif operation == "read":
            return self._op_read(store, params)
        elif operation == "update":
            return self._op_update(store, params)
        elif operation == "delete":
            return self._op_delete(store, params)
        elif operation == "list":
            return self._op_list(store, params)
        else:
            return {"error": f"Unknown operation: {operation}"}

    # -- Resource mapping for dropdowns -------------------------------------

    def get_resource_options(
        self,
        resource_type: ResourceType,
        credential_id: str,
    ) -> list[dict[str, str]]:
        """Get options for dynamic dropdowns in n8n UI."""
        if credential_id not in self._credentials:
            return []

        if resource_type == ResourceType.PROJECT:
            return [
                {"name": p.get("title", p_id), "value": p_id}
                for p_id, p in self._plans.items()
            ]
        elif resource_type == ResourceType.TASK:
            return [
                {"name": t.get("title", t_id), "value": t_id}
                for t_id, t in self._tasks.items()
            ]
        elif resource_type == ResourceType.MILESTONE:
            return [
                {"name": m.get("name", m_id), "value": m_id}
                for m_id, m in self._milestones.items()
            ]
        elif resource_type == ResourceType.STATUS:
            return [
                {"name": "Pending", "value": "pending"},
                {"name": "In Progress", "value": "in_progress"},
                {"name": "Completed", "value": "completed"},
                {"name": "Blocked", "value": "blocked"},
            ]
        elif resource_type == ResourceType.USER:
            # Collect unique assignees from tasks
            users = {
                t.get("assignee", "")
                for t in self._tasks.values()
                if t.get("assignee")
            }
            return [{"name": u, "value": u} for u in sorted(users)]

        return []

    # -- Node export --------------------------------------------------------

    def export_node_definitions(self) -> dict[str, Any]:
        """Export node definitions for n8n installation."""
        return {
            "nodes": [
                self._node_to_dict(TRIGGER_NODE),
                self._node_to_dict(REGULAR_NODE),
            ],
            "credentials": [CREDENTIAL_DEFINITION],
        }

    def get_node_documentation(self) -> dict[str, str]:
        """Get documentation for all nodes."""
        return {
            "blueprintTrigger": (
                "Blueprint Trigger node watches for events in Blueprint plans. "
                "Supports webhook mode for instant triggers and polling mode "
                "for periodic checks. Configure the event type and optional "
                "plan ID filter."
            ),
            "blueprint": (
                "Blueprint node performs CRUD operations on plans, tasks, and "
                "milestones. Select a resource type and operation, then provide "
                "the required parameters. Supports pagination for list operations."
            ),
        }

    # -- Private CRUD helpers -----------------------------------------------

    def _op_create(
        self,
        store: dict[str, dict[str, Any]],
        resource: str,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        item_id = _gen_id(resource[:4])
        item = {
            "id": item_id,
            "title": params.get("title", ""),
            "description": params.get("description", ""),
            "status": params.get("status", "pending"),
            "plan_id": params.get("planId", params.get("plan_id", "")),
            "created_at": _now_iso(),
            "created_via": "n8n",
        }
        if resource == "milestone":
            item["name"] = params.get("title", "")
        store[item_id] = item
        return item

    def _op_read(
        self,
        store: dict[str, dict[str, Any]],
        params: dict[str, Any],
    ) -> dict[str, Any] | None:
        item_id = params.get("itemId", params.get("id", ""))
        return store.get(item_id)

    def _op_update(
        self,
        store: dict[str, dict[str, Any]],
        params: dict[str, Any],
    ) -> dict[str, Any] | None:
        item_id = params.get("itemId", params.get("id", ""))
        item = store.get(item_id)
        if item is None:
            return None

        for key in ("title", "description", "status"):
            if params.get(key):
                item[key] = params[key]
        item["updated_at"] = _now_iso()
        return item

    def _op_delete(
        self,
        store: dict[str, dict[str, Any]],
        params: dict[str, Any],
    ) -> dict[str, Any]:
        item_id = params.get("itemId", params.get("id", ""))
        removed = store.pop(item_id, None)
        return {"deleted": removed is not None, "id": item_id}

    def _op_list(
        self,
        store: dict[str, dict[str, Any]],
        params: dict[str, Any],
    ) -> list[dict[str, Any]]:
        items = list(store.values())
        plan_id = params.get("planId", params.get("plan_id"))
        if plan_id:
            items = [i for i in items if i.get("plan_id") == plan_id]

        limit = params.get("limit", 50)
        offset_val = params.get("offset", 0)
        return items[offset_val : offset_val + limit]

    # -- Private helpers ----------------------------------------------------

    def _deliver_with_retry(
        self,
        webhook: N8nWebhook,
        event_type: str,
        data: dict[str, Any],
    ) -> dict[str, Any]:
        payload = {
            "webhook_id": webhook.webhook_id,
            "event_type": event_type,
            "timestamp": _now_iso(),
            "data": data,
        }

        if self._http_sender is None:
            return {"webhook_id": webhook.webhook_id, "success": True}

        last_error: str | None = None
        for attempt in range(self._max_retries):
            try:
                self._http_sender(url=webhook.url, payload=payload)
                return {"webhook_id": webhook.webhook_id, "success": True, "attempts": attempt + 1}
            except Exception as exc:
                last_error = str(exc)

        return {
            "webhook_id": webhook.webhook_id,
            "success": False,
            "error": last_error,
            "attempts": self._max_retries,
        }

    @staticmethod
    def _node_to_dict(node: NodeDefinition) -> dict[str, Any]:
        return {
            "name": node.name,
            "displayName": node.display_name,
            "description": node.description,
            "type": node.node_type.value,
            "icon": node.icon,
            "group": [node.group],
            "version": node.version,
            "properties": node.properties,
            "credentials": node.credentials,
        }


__all__ = [
    "NodeType",
    "TriggerMode",
    "ResourceType",
    "Operation",
    "N8nCredential",
    "N8nWebhook",
    "NodeDefinition",
    "N8nIntegration",
    "TRIGGER_NODE",
    "REGULAR_NODE",
    "CREDENTIAL_DEFINITION",
]
