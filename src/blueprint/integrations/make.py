"""Make (Integromat) integration for visual workflow automation."""

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
# Module types and models
# ---------------------------------------------------------------------------


class ModuleType(str, Enum):
    """Types of Make modules."""

    TRIGGER = "trigger"
    ACTION = "action"
    SEARCH = "search"
    INSTANT_TRIGGER = "instant_trigger"


class MakeTriggerEvent(str, Enum):
    """Events that can trigger Make scenarios."""

    NEW_TASK = "new_task"
    STATUS_CHANGE = "status_change"
    TASK_OVERDUE = "task_overdue"


class MakeErrorCode(str, Enum):
    """Make-compatible error codes."""

    INVALID_API_KEY = "InvalidApiKeyError"
    NOT_FOUND = "DataError"
    RATE_LIMIT = "RateLimitError"
    VALIDATION = "InvalidDataError"
    INTERNAL = "RuntimeError"


@dataclass(frozen=True, slots=True)
class MakeConnection:
    """API connection configuration for Make."""

    connection_id: str
    api_key: str
    base_url: str = ""
    label: str = "Blueprint Connection"
    created_at: str = field(default_factory=_now_iso)


@dataclass(frozen=True, slots=True)
class WebhookRegistration:
    """Instant trigger webhook registration."""

    webhook_id: str
    url: str
    event_type: MakeTriggerEvent
    connection_id: str
    active: bool = True
    created_at: str = field(default_factory=_now_iso)


@dataclass(frozen=True, slots=True)
class ModuleDefinition:
    """Definition of a Make module for the visual interface."""

    module_id: str
    name: str
    description: str
    module_type: ModuleType
    parameters: list[dict[str, Any]] = field(default_factory=list)
    output_fields: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class MakeError:
    """Make-compatible error response."""

    code: MakeErrorCode
    message: str
    detail: str = ""


# ---------------------------------------------------------------------------
# Module metadata for Make's visual interface
# ---------------------------------------------------------------------------

TRIGGER_MODULES: list[ModuleDefinition] = [
    ModuleDefinition(
        module_id="trigger_new_task",
        name="Watch New Tasks",
        description="Triggers when a new task is created in a plan",
        module_type=ModuleType.TRIGGER,
        parameters=[
            {"name": "plan_id", "type": "text", "label": "Plan ID", "required": False},
        ],
        output_fields=[
            {"name": "task_id", "type": "text"},
            {"name": "title", "type": "text"},
            {"name": "status", "type": "text"},
            {"name": "assignee", "type": "text"},
            {"name": "priority", "type": "text"},
            {"name": "created_at", "type": "date"},
        ],
    ),
    ModuleDefinition(
        module_id="trigger_status_change",
        name="Watch Status Changes",
        description="Triggers when a task status changes",
        module_type=ModuleType.TRIGGER,
        parameters=[
            {"name": "plan_id", "type": "text", "label": "Plan ID", "required": False},
            {"name": "status", "type": "select", "label": "New Status", "required": False},
        ],
        output_fields=[
            {"name": "task_id", "type": "text"},
            {"name": "title", "type": "text"},
            {"name": "old_status", "type": "text"},
            {"name": "new_status", "type": "text"},
            {"name": "changed_at", "type": "date"},
        ],
    ),
    ModuleDefinition(
        module_id="trigger_overdue",
        name="Watch Overdue Tasks",
        description="Triggers when a task becomes overdue",
        module_type=ModuleType.TRIGGER,
        parameters=[
            {"name": "plan_id", "type": "text", "label": "Plan ID", "required": False},
        ],
        output_fields=[
            {"name": "task_id", "type": "text"},
            {"name": "title", "type": "text"},
            {"name": "due_date", "type": "date"},
            {"name": "overdue_by_hours", "type": "number"},
        ],
    ),
]

ACTION_MODULES: list[ModuleDefinition] = [
    ModuleDefinition(
        module_id="action_create_task",
        name="Create a Task",
        description="Creates a new task in a plan",
        module_type=ModuleType.ACTION,
        parameters=[
            {"name": "plan_id", "type": "text", "label": "Plan ID", "required": True},
            {"name": "title", "type": "text", "label": "Title", "required": True},
            {"name": "description", "type": "text", "label": "Description", "required": False},
            {"name": "assignee", "type": "text", "label": "Assignee", "required": False},
            {"name": "priority", "type": "select", "label": "Priority", "required": False},
        ],
        output_fields=[
            {"name": "task_id", "type": "text"},
            {"name": "title", "type": "text"},
            {"name": "status", "type": "text"},
        ],
    ),
    ModuleDefinition(
        module_id="action_update_status",
        name="Update Task Status",
        description="Updates the status of an existing task",
        module_type=ModuleType.ACTION,
        parameters=[
            {"name": "task_id", "type": "text", "label": "Task ID", "required": True},
            {"name": "status", "type": "select", "label": "New Status", "required": True},
            {"name": "comment", "type": "text", "label": "Comment", "required": False},
        ],
        output_fields=[
            {"name": "task_id", "type": "text"},
            {"name": "status", "type": "text"},
            {"name": "updated_at", "type": "date"},
        ],
    ),
    ModuleDefinition(
        module_id="action_add_comment",
        name="Add Comment",
        description="Adds a comment to a task",
        module_type=ModuleType.ACTION,
        parameters=[
            {"name": "task_id", "type": "text", "label": "Task ID", "required": True},
            {"name": "comment", "type": "text", "label": "Comment", "required": True},
            {"name": "author", "type": "text", "label": "Author", "required": False},
        ],
        output_fields=[
            {"name": "comment_id", "type": "text"},
            {"name": "task_id", "type": "text"},
            {"name": "created_at", "type": "date"},
        ],
    ),
]

SEARCH_MODULES: list[ModuleDefinition] = [
    ModuleDefinition(
        module_id="search_tasks",
        name="Search Tasks",
        description="Find tasks matching specific criteria",
        module_type=ModuleType.SEARCH,
        parameters=[
            {"name": "plan_id", "type": "text", "label": "Plan ID", "required": False},
            {"name": "status", "type": "select", "label": "Status", "required": False},
            {"name": "assignee", "type": "text", "label": "Assignee", "required": False},
            {"name": "query", "type": "text", "label": "Search Query", "required": False},
            {"name": "limit", "type": "number", "label": "Max Results", "required": False},
            {"name": "offset", "type": "number", "label": "Offset", "required": False},
        ],
        output_fields=[
            {"name": "task_id", "type": "text"},
            {"name": "title", "type": "text"},
            {"name": "status", "type": "text"},
            {"name": "assignee", "type": "text"},
            {"name": "plan_id", "type": "text"},
        ],
    ),
]


# ---------------------------------------------------------------------------
# Main integration class
# ---------------------------------------------------------------------------

HttpSender = Callable[..., Any]


class MakeIntegration:
    """Manages Make (Integromat) modules and webhook integrations."""

    def __init__(
        self,
        *,
        http_sender: HttpSender | None = None,
    ):
        self._connections: dict[str, MakeConnection] = {}
        self._webhooks: dict[str, WebhookRegistration] = {}
        self._tasks: dict[str, dict[str, Any]] = {}
        self._comments: dict[str, list[dict[str, Any]]] = {}
        self._http_sender = http_sender
        self._last_poll: dict[str, str] = {}  # webhook_id -> last poll timestamp

    # -- Connection management ----------------------------------------------

    def create_connection(self, api_key: str, *, label: str = "Blueprint Connection") -> MakeConnection:
        """Create a new API connection."""
        conn = MakeConnection(
            connection_id=_gen_id("conn"),
            api_key=api_key,
            label=label,
        )
        self._connections[conn.connection_id] = conn
        return conn

    def verify_connection(self, connection_id: str) -> dict[str, Any]:
        """Verify a connection is valid."""
        conn = self._connections.get(connection_id)
        if conn is None:
            return {"valid": False, "error": MakeErrorCode.INVALID_API_KEY.value}
        return {"valid": True, "connection_id": conn.connection_id, "label": conn.label}

    # -- Trigger modules ----------------------------------------------------

    def register_instant_trigger(
        self,
        url: str,
        event_type: MakeTriggerEvent,
        connection_id: str,
    ) -> WebhookRegistration | MakeError:
        """Register an instant trigger webhook."""
        if connection_id not in self._connections:
            return MakeError(
                code=MakeErrorCode.INVALID_API_KEY,
                message="Invalid connection",
            )

        webhook = WebhookRegistration(
            webhook_id=_gen_id("whk"),
            url=url,
            event_type=event_type,
            connection_id=connection_id,
        )
        self._webhooks[webhook.webhook_id] = webhook
        return webhook

    def unregister_instant_trigger(self, webhook_id: str) -> bool:
        """Unregister an instant trigger webhook."""
        return self._webhooks.pop(webhook_id, None) is not None

    def poll_trigger(
        self,
        event_type: MakeTriggerEvent,
        connection_id: str,
        *,
        plan_id: str | None = None,
        since: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict[str, Any]] | MakeError:
        """Poll for new events (used for non-instant triggers)."""
        if connection_id not in self._connections:
            return MakeError(
                code=MakeErrorCode.INVALID_API_KEY,
                message="Invalid connection",
            )

        tasks = list(self._tasks.values())
        if plan_id:
            tasks = [t for t in tasks if t.get("plan_id") == plan_id]
        if since:
            tasks = [t for t in tasks if t.get("created_at", "") > since]

        # Apply pagination
        tasks = tasks[offset : offset + limit]
        return tasks

    def fire_instant_trigger(
        self,
        event_type: MakeTriggerEvent,
        data: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Fire an instant trigger to all registered webhooks."""
        results: list[dict[str, Any]] = []
        for webhook in self._webhooks.values():
            if webhook.event_type == event_type and webhook.active:
                result = self._deliver_webhook(webhook, data)
                results.append(result)
        return results

    # -- Action modules -----------------------------------------------------

    def execute_action(
        self,
        action_id: str,
        params: dict[str, Any],
        connection_id: str,
    ) -> dict[str, Any] | MakeError:
        """Execute a Make action module."""
        if connection_id not in self._connections:
            return MakeError(
                code=MakeErrorCode.INVALID_API_KEY,
                message="Invalid connection",
            )

        handlers: dict[str, Callable[..., dict[str, Any] | MakeError]] = {
            "action_create_task": self._action_create_task,
            "action_update_status": self._action_update_status,
            "action_add_comment": self._action_add_comment,
        }

        handler = handlers.get(action_id)
        if handler is None:
            return MakeError(
                code=MakeErrorCode.NOT_FOUND,
                message=f"Unknown action: {action_id}",
            )

        return handler(params)

    # -- Search modules -----------------------------------------------------

    def execute_search(
        self,
        search_id: str,
        params: dict[str, Any],
        connection_id: str,
    ) -> list[dict[str, Any]] | MakeError:
        """Execute a Make search module."""
        if connection_id not in self._connections:
            return MakeError(
                code=MakeErrorCode.INVALID_API_KEY,
                message="Invalid connection",
            )

        if search_id != "search_tasks":
            return MakeError(
                code=MakeErrorCode.NOT_FOUND,
                message=f"Unknown search: {search_id}",
            )

        return self._search_tasks(params)

    # -- Module metadata ----------------------------------------------------

    def get_module_definitions(self) -> list[ModuleDefinition]:
        """Get all module definitions for Make's visual interface."""
        return TRIGGER_MODULES + ACTION_MODULES + SEARCH_MODULES

    def get_module(self, module_id: str) -> ModuleDefinition | None:
        """Get a specific module definition."""
        for m in self.get_module_definitions():
            if m.module_id == module_id:
                return m
        return None

    # -- Private action handlers --------------------------------------------

    def _action_create_task(self, params: dict[str, Any]) -> dict[str, Any] | MakeError:
        title = params.get("title")
        if not title:
            return MakeError(
                code=MakeErrorCode.VALIDATION,
                message="Title is required",
            )

        task_id = _gen_id("task")
        task = {
            "task_id": task_id,
            "title": title,
            "description": params.get("description", ""),
            "status": "pending",
            "assignee": params.get("assignee", ""),
            "priority": params.get("priority", "medium"),
            "plan_id": params.get("plan_id", ""),
            "created_at": _now_iso(),
            "created_via": "make",
        }
        self._tasks[task_id] = task
        return task

    def _action_update_status(self, params: dict[str, Any]) -> dict[str, Any] | MakeError:
        task_id = params.get("task_id")
        if not task_id:
            return MakeError(
                code=MakeErrorCode.VALIDATION,
                message="Task ID is required",
            )

        task = self._tasks.get(task_id)
        if task is None:
            return MakeError(
                code=MakeErrorCode.NOT_FOUND,
                message=f"Task not found: {task_id}",
            )

        new_status = params.get("status", task.get("status"))
        task["status"] = new_status
        task["updated_at"] = _now_iso()
        if params.get("comment"):
            self._add_comment(task_id, params["comment"])

        return {
            "task_id": task_id,
            "status": new_status,
            "updated_at": task["updated_at"],
        }

    def _action_add_comment(self, params: dict[str, Any]) -> dict[str, Any] | MakeError:
        task_id = params.get("task_id")
        comment_text = params.get("comment")
        if not task_id or not comment_text:
            return MakeError(
                code=MakeErrorCode.VALIDATION,
                message="Task ID and comment are required",
            )

        if task_id not in self._tasks:
            return MakeError(
                code=MakeErrorCode.NOT_FOUND,
                message=f"Task not found: {task_id}",
            )

        return self._add_comment(task_id, comment_text, author=params.get("author", ""))

    def _add_comment(
        self, task_id: str, text: str, *, author: str = ""
    ) -> dict[str, Any]:
        comment = {
            "comment_id": _gen_id("cmt"),
            "task_id": task_id,
            "comment": text,
            "author": author,
            "created_at": _now_iso(),
        }
        self._comments.setdefault(task_id, []).append(comment)
        return comment

    # -- Private search handlers --------------------------------------------

    def _search_tasks(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        results = list(self._tasks.values())

        plan_id = params.get("plan_id")
        if plan_id:
            results = [t for t in results if t.get("plan_id") == plan_id]

        status = params.get("status")
        if status:
            results = [t for t in results if t.get("status") == status]

        assignee = params.get("assignee")
        if assignee:
            results = [t for t in results if t.get("assignee") == assignee]

        query = params.get("query")
        if query:
            q = query.lower()
            results = [
                t for t in results
                if q in t.get("title", "").lower() or q in t.get("description", "").lower()
            ]

        limit = params.get("limit", 50)
        offset_val = params.get("offset", 0)
        return results[offset_val : offset_val + limit]

    # -- Private webhook delivery -------------------------------------------

    def _deliver_webhook(
        self,
        webhook: WebhookRegistration,
        data: dict[str, Any],
    ) -> dict[str, Any]:
        payload = {
            "webhook_id": webhook.webhook_id,
            "event_type": webhook.event_type.value,
            "timestamp": _now_iso(),
            "data": data,
        }

        if self._http_sender is not None:
            try:
                self._http_sender(url=webhook.url, payload=payload)
                return {"webhook_id": webhook.webhook_id, "success": True}
            except Exception as exc:
                return {"webhook_id": webhook.webhook_id, "success": False, "error": str(exc)}

        return {"webhook_id": webhook.webhook_id, "success": True}


__all__ = [
    "ModuleType",
    "MakeTriggerEvent",
    "MakeErrorCode",
    "MakeConnection",
    "WebhookRegistration",
    "ModuleDefinition",
    "MakeError",
    "MakeIntegration",
    "TRIGGER_MODULES",
    "ACTION_MODULES",
    "SEARCH_MODULES",
]
