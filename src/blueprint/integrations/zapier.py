"""Zapier webhook integration for plan automation."""

from __future__ import annotations

import hashlib
import hmac
import json
import time
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
# Event types and models
# ---------------------------------------------------------------------------


class TriggerEvent(str, Enum):
    """Events that can trigger Zapier webhooks."""

    TASK_CREATED = "task_created"
    TASK_COMPLETED = "task_completed"
    MILESTONE_REACHED = "milestone_reached"
    PLAN_UPDATED = "plan_updated"


class SubscriptionStatus(str, Enum):
    ACTIVE = "active"
    PAUSED = "paused"
    DISABLED = "disabled"


@dataclass(frozen=True, slots=True)
class WebhookSubscription:
    """A Zapier webhook subscription for a specific event type."""

    subscription_id: str
    target_url: str
    event_type: TriggerEvent
    api_key: str
    status: SubscriptionStatus = SubscriptionStatus.ACTIVE
    created_at: str = field(default_factory=_now_iso)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class WebhookDelivery:
    """Record of a webhook delivery attempt."""

    delivery_id: str
    subscription_id: str
    event_type: TriggerEvent
    payload: dict[str, Any]
    status_code: int | None = None
    success: bool = False
    delivered_at: str = field(default_factory=_now_iso)
    error: str | None = None


@dataclass(frozen=True, slots=True)
class ZapTemplate:
    """A sample Zap template for common workflows."""

    template_id: str
    name: str
    description: str
    trigger_event: TriggerEvent
    action_type: str
    sample_config: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class RateLimitEntry:
    """Tracks rate limit state for a subscription."""

    subscription_id: str
    window_start: float
    request_count: int


# ---------------------------------------------------------------------------
# Payload formatting
# ---------------------------------------------------------------------------


def format_task_payload(task: dict[str, Any], event: TriggerEvent) -> dict[str, Any]:
    """Format a task into Zapier-compatible payload structure."""
    return {
        "id": task.get("id", ""),
        "event": event.value,
        "timestamp": _now_iso(),
        "data": {
            "task_id": task.get("id", ""),
            "title": task.get("title", ""),
            "description": task.get("description", ""),
            "status": task.get("status", "pending"),
            "assignee": task.get("assignee", ""),
            "priority": task.get("priority", "medium"),
            "estimated_hours": task.get("estimated_hours"),
            "tags": task.get("tags", []),
            "plan_id": task.get("plan_id", ""),
        },
    }


def format_milestone_payload(milestone: dict[str, Any]) -> dict[str, Any]:
    """Format a milestone into Zapier-compatible payload structure."""
    return {
        "id": milestone.get("id", ""),
        "event": TriggerEvent.MILESTONE_REACHED.value,
        "timestamp": _now_iso(),
        "data": {
            "milestone_id": milestone.get("id", ""),
            "name": milestone.get("name", ""),
            "description": milestone.get("description", ""),
            "due_date": milestone.get("due_date", ""),
            "completed_at": milestone.get("completed_at", ""),
            "plan_id": milestone.get("plan_id", ""),
        },
    }


def format_plan_payload(plan: dict[str, Any]) -> dict[str, Any]:
    """Format a plan update into Zapier-compatible payload structure."""
    return {
        "id": plan.get("id", ""),
        "event": TriggerEvent.PLAN_UPDATED.value,
        "timestamp": _now_iso(),
        "data": {
            "plan_id": plan.get("id", ""),
            "title": plan.get("title", ""),
            "status": plan.get("status", ""),
            "task_count": len(plan.get("tasks", [])),
            "milestone_count": len(plan.get("milestones", [])),
            "updated_fields": plan.get("updated_fields", []),
        },
    }


# ---------------------------------------------------------------------------
# Sample Zap templates
# ---------------------------------------------------------------------------

SAMPLE_TEMPLATES: list[ZapTemplate] = [
    ZapTemplate(
        template_id="tpl-task-slack",
        name="New Task to Slack",
        description="Post a Slack message when a new task is created",
        trigger_event=TriggerEvent.TASK_CREATED,
        action_type="slack_message",
        sample_config={
            "channel": "#project-updates",
            "message_template": "New task: {{title}} assigned to {{assignee}}",
        },
    ),
    ZapTemplate(
        template_id="tpl-complete-jira",
        name="Task Complete to Jira",
        description="Update Jira ticket when a blueprint task is completed",
        trigger_event=TriggerEvent.TASK_COMPLETED,
        action_type="jira_update",
        sample_config={
            "transition": "Done",
            "comment": "Completed in blueprint: {{title}}",
        },
    ),
    ZapTemplate(
        template_id="tpl-milestone-email",
        name="Milestone Reached Email",
        description="Send an email notification when a milestone is reached",
        trigger_event=TriggerEvent.MILESTONE_REACHED,
        action_type="email_send",
        sample_config={
            "subject": "Milestone reached: {{name}}",
            "body": "The milestone '{{name}}' has been completed.",
        },
    ),
    ZapTemplate(
        template_id="tpl-plan-sheet",
        name="Plan Update to Google Sheet",
        description="Log plan updates to a Google Sheet for tracking",
        trigger_event=TriggerEvent.PLAN_UPDATED,
        action_type="google_sheet_row",
        sample_config={
            "spreadsheet": "Plan Tracking",
            "row_data": ["{{plan_id}}", "{{status}}", "{{task_count}}"],
        },
    ),
]


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------


def verify_api_key(provided_key: str, expected_key: str) -> bool:
    """Verify an API key using constant-time comparison."""
    return hmac.compare_digest(provided_key, expected_key)


def sign_payload(payload: dict[str, Any], secret: str) -> str:
    """Create HMAC-SHA256 signature for webhook payload."""
    body = json.dumps(payload, sort_keys=True).encode("utf-8")
    return hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()


def verify_signature(payload: dict[str, Any], signature: str, secret: str) -> bool:
    """Verify webhook payload signature."""
    expected = sign_payload(payload, secret)
    return hmac.compare_digest(signature, expected)


# ---------------------------------------------------------------------------
# Main integration class
# ---------------------------------------------------------------------------

# Default: 100 requests per 60-second window
DEFAULT_RATE_LIMIT = 100
DEFAULT_RATE_WINDOW = 60.0

HttpSender = Callable[..., Any]


class ZapierIntegration:
    """Manages Zapier webhook subscriptions and event delivery."""

    def __init__(
        self,
        *,
        rate_limit: int = DEFAULT_RATE_LIMIT,
        rate_window: float = DEFAULT_RATE_WINDOW,
        http_sender: HttpSender | None = None,
    ):
        self._subscriptions: dict[str, WebhookSubscription] = {}
        self._deliveries: list[WebhookDelivery] = []
        self._rate_limits: dict[str, RateLimitEntry] = {}
        self._rate_limit = rate_limit
        self._rate_window = rate_window
        self._http_sender = http_sender

    # -- Subscription management --------------------------------------------

    def register_webhook(
        self,
        target_url: str,
        event_type: TriggerEvent,
        api_key: str,
        *,
        metadata: dict[str, Any] | None = None,
    ) -> WebhookSubscription:
        """Register a new webhook subscription."""
        sub = WebhookSubscription(
            subscription_id=_gen_id("sub"),
            target_url=target_url,
            event_type=event_type,
            api_key=api_key,
            metadata=metadata or {},
        )
        self._subscriptions[sub.subscription_id] = sub
        return sub

    def unregister_webhook(self, subscription_id: str) -> bool:
        """Remove a webhook subscription."""
        return self._subscriptions.pop(subscription_id, None) is not None

    def get_subscription(self, subscription_id: str) -> WebhookSubscription | None:
        """Get a subscription by ID."""
        return self._subscriptions.get(subscription_id)

    def list_subscriptions(
        self,
        *,
        event_type: TriggerEvent | None = None,
        status: SubscriptionStatus | None = None,
    ) -> list[WebhookSubscription]:
        """List subscriptions with optional filtering."""
        results = list(self._subscriptions.values())
        if event_type is not None:
            results = [s for s in results if s.event_type == event_type]
        if status is not None:
            results = [s for s in results if s.status == status]
        return results

    def pause_subscription(self, subscription_id: str) -> WebhookSubscription | None:
        """Pause a subscription."""
        return self._update_subscription_status(subscription_id, SubscriptionStatus.PAUSED)

    def resume_subscription(self, subscription_id: str) -> WebhookSubscription | None:
        """Resume a paused subscription."""
        return self._update_subscription_status(subscription_id, SubscriptionStatus.ACTIVE)

    def _update_subscription_status(
        self, subscription_id: str, status: SubscriptionStatus
    ) -> WebhookSubscription | None:
        sub = self._subscriptions.get(subscription_id)
        if sub is None:
            return None
        from dataclasses import replace
        updated = replace(sub, status=status)
        self._subscriptions[subscription_id] = updated
        return updated

    # -- Event firing -------------------------------------------------------

    def fire_event(
        self,
        event_type: TriggerEvent,
        data: dict[str, Any],
    ) -> list[WebhookDelivery]:
        """Fire an event to all active subscriptions for the event type."""
        subs = [
            s
            for s in self._subscriptions.values()
            if s.event_type == event_type and s.status == SubscriptionStatus.ACTIVE
        ]

        deliveries: list[WebhookDelivery] = []
        for sub in subs:
            delivery = self._deliver(sub, event_type, data)
            deliveries.append(delivery)

        return deliveries

    def fire_task_created(self, task: dict[str, Any]) -> list[WebhookDelivery]:
        """Fire a task_created event."""
        payload = format_task_payload(task, TriggerEvent.TASK_CREATED)
        return self.fire_event(TriggerEvent.TASK_CREATED, payload)

    def fire_task_completed(self, task: dict[str, Any]) -> list[WebhookDelivery]:
        """Fire a task_completed event."""
        payload = format_task_payload(task, TriggerEvent.TASK_COMPLETED)
        return self.fire_event(TriggerEvent.TASK_COMPLETED, payload)

    def fire_milestone_reached(self, milestone: dict[str, Any]) -> list[WebhookDelivery]:
        """Fire a milestone_reached event."""
        payload = format_milestone_payload(milestone)
        return self.fire_event(TriggerEvent.MILESTONE_REACHED, payload)

    def fire_plan_updated(self, plan: dict[str, Any]) -> list[WebhookDelivery]:
        """Fire a plan_updated event."""
        payload = format_plan_payload(plan)
        return self.fire_event(TriggerEvent.PLAN_UPDATED, payload)

    # -- Action endpoints ---------------------------------------------------

    def create_task_action(self, data: dict[str, Any], api_key: str) -> dict[str, Any]:
        """Action endpoint: create a task from Zapier."""
        if not self._validate_action_key(api_key):
            return {"error": "Invalid API key", "success": False}

        task_id = data.get("id") or _gen_id("task")
        task = {
            "id": task_id,
            "title": data.get("title", "Untitled"),
            "description": data.get("description", ""),
            "status": data.get("status", "pending"),
            "assignee": data.get("assignee", ""),
            "priority": data.get("priority", "medium"),
            "plan_id": data.get("plan_id", ""),
            "created_via": "zapier",
            "created_at": _now_iso(),
        }
        return {"success": True, "task": task}

    def update_task_action(
        self,
        task_id: str,
        updates: dict[str, Any],
        api_key: str,
    ) -> dict[str, Any]:
        """Action endpoint: update a task from Zapier."""
        if not self._validate_action_key(api_key):
            return {"error": "Invalid API key", "success": False}

        result = {
            "id": task_id,
            **updates,
            "updated_via": "zapier",
            "updated_at": _now_iso(),
        }
        return {"success": True, "task": result}

    # -- Webhook testing ----------------------------------------------------

    def test_webhook(self, subscription_id: str) -> WebhookDelivery:
        """Send a test payload to a webhook subscription to validate configuration."""
        sub = self._subscriptions.get(subscription_id)
        if sub is None:
            return WebhookDelivery(
                delivery_id=_gen_id("dlv"),
                subscription_id=subscription_id,
                event_type=TriggerEvent.TASK_CREATED,
                payload={},
                success=False,
                error="Subscription not found",
            )

        test_payload = {
            "id": "test-event-001",
            "event": sub.event_type.value,
            "timestamp": _now_iso(),
            "test": True,
            "data": {
                "message": "This is a test webhook from Blueprint",
                "subscription_id": sub.subscription_id,
            },
        }

        return self._deliver(sub, sub.event_type, test_payload)

    # -- Rate limiting ------------------------------------------------------

    def _check_rate_limit(self, subscription_id: str) -> bool:
        """Check if a subscription is within its rate limit. Returns True if allowed."""
        now = time.monotonic()
        entry = self._rate_limits.get(subscription_id)

        if entry is None or (now - entry.window_start) > self._rate_window:
            self._rate_limits[subscription_id] = RateLimitEntry(
                subscription_id=subscription_id,
                window_start=now,
                request_count=1,
            )
            return True

        if entry.request_count >= self._rate_limit:
            return False

        self._rate_limits[subscription_id] = RateLimitEntry(
            subscription_id=subscription_id,
            window_start=entry.window_start,
            request_count=entry.request_count + 1,
        )
        return True

    # -- Delivery history ---------------------------------------------------

    def get_deliveries(
        self,
        *,
        subscription_id: str | None = None,
        event_type: TriggerEvent | None = None,
    ) -> list[WebhookDelivery]:
        """Get delivery history with optional filtering."""
        results = list(self._deliveries)
        if subscription_id is not None:
            results = [d for d in results if d.subscription_id == subscription_id]
        if event_type is not None:
            results = [d for d in results if d.event_type == event_type]
        return results

    # -- Templates ----------------------------------------------------------

    def get_templates(self) -> list[ZapTemplate]:
        """Get all available Zap templates."""
        return list(SAMPLE_TEMPLATES)

    def get_template(self, template_id: str) -> ZapTemplate | None:
        """Get a specific template by ID."""
        for t in SAMPLE_TEMPLATES:
            if t.template_id == template_id:
                return t
        return None

    # -- Private helpers ----------------------------------------------------

    def _deliver(
        self,
        sub: WebhookSubscription,
        event_type: TriggerEvent,
        payload: dict[str, Any],
    ) -> WebhookDelivery:
        """Deliver a payload to a webhook subscription."""
        if not self._check_rate_limit(sub.subscription_id):
            delivery = WebhookDelivery(
                delivery_id=_gen_id("dlv"),
                subscription_id=sub.subscription_id,
                event_type=event_type,
                payload=payload,
                success=False,
                error="Rate limit exceeded",
            )
            self._deliveries.append(delivery)
            return delivery

        signature = sign_payload(payload, sub.api_key)
        status_code: int | None = None
        success = False
        error: str | None = None

        if self._http_sender is not None:
            try:
                response = self._http_sender(
                    url=sub.target_url,
                    payload=payload,
                    headers={
                        "Content-Type": "application/json",
                        "X-Blueprint-Signature": signature,
                        "X-Blueprint-Event": event_type.value,
                    },
                )
                status_code = response.get("status_code", 200) if isinstance(response, dict) else 200
                success = 200 <= (status_code or 0) < 300
            except Exception as exc:
                error = str(exc)
        else:
            # No sender configured; simulate success for testing
            status_code = 200
            success = True

        delivery = WebhookDelivery(
            delivery_id=_gen_id("dlv"),
            subscription_id=sub.subscription_id,
            event_type=event_type,
            payload=payload,
            status_code=status_code,
            success=success,
            error=error,
        )
        self._deliveries.append(delivery)
        return delivery

    def _validate_action_key(self, api_key: str) -> bool:
        """Validate that the API key belongs to at least one active subscription."""
        return any(
            s.api_key == api_key and s.status == SubscriptionStatus.ACTIVE
            for s in self._subscriptions.values()
        )


__all__ = [
    "TriggerEvent",
    "SubscriptionStatus",
    "WebhookSubscription",
    "WebhookDelivery",
    "ZapTemplate",
    "RateLimitEntry",
    "ZapierIntegration",
    "format_task_payload",
    "format_milestone_payload",
    "format_plan_payload",
    "verify_api_key",
    "sign_payload",
    "verify_signature",
    "SAMPLE_TEMPLATES",
]
