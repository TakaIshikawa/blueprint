"""Discord webhook exporter for execution plan notifications."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any, Literal
from urllib.parse import urlparse

from blueprint.exporters.base import TargetExporter


NotificationTrigger = Literal[
    "plan_creation",
    "task_completion",
    "milestone_reached",
    "blocking_issue",
    "status_change",
]


@dataclass(frozen=True, slots=True)
class DiscordWebhookConfig:
    """Configuration for Discord webhook routing and rate limiting."""

    webhooks: dict[str, str] = field(default_factory=dict)
    routing_rules: dict[str, str] = field(default_factory=dict)
    rate_limit_delay: float = 0.5  # Discord allows 5 requests per 2 seconds = 0.4s between requests
    enabled_triggers: tuple[NotificationTrigger, ...] = (
        "plan_creation",
        "task_completion",
        "milestone_reached",
        "blocking_issue",
    )

    def get_webhook_url(self, route_key: str) -> str | None:
        """Get webhook URL for a given route key (server, channel, or project)."""
        # First check direct webhook mapping
        if route_key in self.webhooks:
            return self.webhooks[route_key]
        # Then check routing rules
        if route_key in self.routing_rules:
            target_key = self.routing_rules[route_key]
            return self.webhooks.get(target_key)
        # Default webhook
        return self.webhooks.get("default")


class DiscordExporter(TargetExporter):
    """Export execution plan summaries to Discord channels via webhooks."""

    # Discord embed limits
    MAX_EMBED_TITLE = 256
    MAX_EMBED_DESCRIPTION = 4096
    MAX_EMBED_FIELDS = 25
    MAX_FIELD_NAME = 256
    MAX_FIELD_VALUE = 1024

    # Status colors
    STATUS_COLORS = {
        "pending": 0x808080,  # Gray
        "in_progress": 0x3498DB,  # Blue
        "completed": 0x2ECC71,  # Green
        "blocked": 0xE74C3C,  # Red
        "skipped": 0x95A5A6,  # Light gray
    }

    def __init__(self, config: DiscordWebhookConfig | None = None):
        """Initialize Discord exporter with configuration."""
        self.config = config or DiscordWebhookConfig()
        self._last_request_time = 0.0

    def get_format(self) -> str:
        """Get export format."""
        return "json"

    def get_extension(self) -> str:
        """Get file extension."""
        return ".json"

    def export(
        self,
        execution_plan: dict[str, Any],
        implementation_brief: dict[str, Any],
        output_path: str,
    ) -> str:
        """Export execution plan summary to Discord webhook."""
        plan, brief = self.validate_export_payload(execution_plan, implementation_brief)
        self.ensure_output_dir(output_path)

        # Build Discord embed payload
        payload = self.build_embed_payload(plan, brief)

        # Write payload to output file for inspection
        with open(output_path, "w") as f:
            json.dump(payload, f, indent=2)

        return output_path

    def build_embed_payload(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
    ) -> dict[str, Any]:
        """Build Discord embed payload for execution plan summary."""
        tasks = plan.get("tasks", [])
        status_summary = self._status_summary(tasks)
        color = self._determine_color(tasks)

        embed = {
            "title": self._truncate(f"Plan: {plan['id']}", self.MAX_EMBED_TITLE),
            "description": self._truncate(
                brief.get("title", "Execution Plan"), self.MAX_EMBED_DESCRIPTION
            ),
            "color": color,
            "fields": self._build_fields(plan, brief, tasks, status_summary),
            "footer": {"text": f"Plan ID: {plan['id']}"},
            "timestamp": self._get_timestamp(),
        }

        return {"embeds": [embed]}

    def send_to_webhook(
        self,
        webhook_url: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Send payload to Discord webhook with rate limiting.

        Note: This is a placeholder implementation. In production, you would use
        requests library or httpx to actually send the payload.

        Args:
            webhook_url: Discord webhook URL
            payload: Discord embed payload

        Returns:
            Response data (simulated in this implementation)
        """
        # Rate limiting
        self._apply_rate_limit()

        # Validate webhook URL
        if not self._is_valid_webhook_url(webhook_url):
            return {"error": "Invalid webhook URL"}

        # In production, you would do:
        # import requests
        # response = requests.post(webhook_url, json=payload)
        # response.raise_for_status()
        # return response.json()

        # For now, return simulated success
        return {"success": True, "webhook_url": webhook_url, "payload": payload}

    def notify(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        trigger: NotificationTrigger,
        route_key: str = "default",
    ) -> dict[str, Any]:
        """
        Send notification to Discord channel based on trigger.

        Args:
            plan: Execution plan data
            brief: Implementation brief data
            trigger: Notification trigger type
            route_key: Routing key (server, channel, or project identifier)

        Returns:
            Response data from webhook
        """
        if trigger not in self.config.enabled_triggers:
            return {"skipped": True, "reason": f"Trigger {trigger} not enabled"}

        webhook_url = self.config.get_webhook_url(route_key)
        if not webhook_url:
            return {"error": f"No webhook URL found for route key: {route_key}"}

        payload = self._build_notification_payload(plan, brief, trigger)
        return self.send_to_webhook(webhook_url, payload)

    def _build_notification_payload(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        trigger: NotificationTrigger,
    ) -> dict[str, Any]:
        """Build notification-specific payload based on trigger."""
        tasks = plan.get("tasks", [])

        if trigger == "plan_creation":
            return self._plan_creation_payload(plan, brief, tasks)
        elif trigger == "task_completion":
            return self._task_completion_payload(plan, brief, tasks)
        elif trigger == "milestone_reached":
            return self._milestone_reached_payload(plan, brief, tasks)
        elif trigger == "blocking_issue":
            return self._blocking_issue_payload(plan, brief, tasks)
        else:
            return self.build_embed_payload(plan, brief)

    def _plan_creation_payload(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        tasks: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Build payload for plan creation notification."""
        embed = {
            "title": "New Execution Plan Created",
            "description": self._truncate(brief.get("title", ""), self.MAX_EMBED_DESCRIPTION),
            "color": 0x3498DB,  # Blue
            "fields": [
                {"name": "Plan ID", "value": plan["id"], "inline": True},
                {"name": "Total Tasks", "value": str(len(tasks)), "inline": True},
                {
                    "name": "Status",
                    "value": self._status_summary(tasks),
                    "inline": False,
                },
            ],
            "timestamp": self._get_timestamp(),
        }
        return {"embeds": [embed]}

    def _task_completion_payload(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        tasks: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Build payload for task completion notification."""
        completed_tasks = [t for t in tasks if t.get("status") == "completed"]
        total_tasks = len(tasks)
        progress = (len(completed_tasks) / total_tasks * 100) if total_tasks > 0 else 0

        embed = {
            "title": "Task Completed",
            "description": f"Progress: {len(completed_tasks)}/{total_tasks} ({progress:.1f}%)",
            "color": 0x2ECC71,  # Green
            "fields": [
                {"name": "Plan ID", "value": plan["id"], "inline": True},
                {"name": "Completed", "value": str(len(completed_tasks)), "inline": True},
            ],
            "timestamp": self._get_timestamp(),
        }
        return {"embeds": [embed]}

    def _milestone_reached_payload(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        tasks: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Build payload for milestone reached notification."""
        milestones = plan.get("milestones", [])
        milestone_name = milestones[0] if milestones else "Milestone"

        embed = {
            "title": "Milestone Reached",
            "description": str(milestone_name),
            "color": 0xF39C12,  # Orange
            "fields": [
                {"name": "Plan ID", "value": plan["id"], "inline": True},
                {"name": "Total Milestones", "value": str(len(milestones)), "inline": True},
            ],
            "timestamp": self._get_timestamp(),
        }
        return {"embeds": [embed]}

    def _blocking_issue_payload(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        tasks: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Build payload for blocking issue notification."""
        blocked_tasks = [t for t in tasks if t.get("status") == "blocked"]

        fields = [
            {"name": "Plan ID", "value": plan["id"], "inline": True},
            {"name": "Blocked Tasks", "value": str(len(blocked_tasks)), "inline": True},
        ]

        # Add first few blocked tasks
        for task in blocked_tasks[:3]:
            task_title = self._truncate(task.get("title", "Unnamed Task"), 50)
            reason = task.get("blocked_reason", "No reason provided")
            fields.append(
                {
                    "name": f"Task: {task_title}",
                    "value": self._truncate(reason, self.MAX_FIELD_VALUE),
                    "inline": False,
                }
            )

        embed = {
            "title": "Blocking Issues Detected",
            "description": f"{len(blocked_tasks)} task(s) are currently blocked",
            "color": 0xE74C3C,  # Red
            "fields": fields[: self.MAX_EMBED_FIELDS],
            "timestamp": self._get_timestamp(),
        }
        return {"embeds": [embed]}

    def _build_fields(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        tasks: list[dict[str, Any]],
        status_summary: str,
    ) -> list[dict[str, Any]]:
        """Build embed fields for plan summary."""
        fields = [
            {
                "name": "Total Tasks",
                "value": str(len(tasks)),
                "inline": True,
            },
            {
                "name": "Status",
                "value": status_summary,
                "inline": True,
            },
        ]

        # Add milestone info if available
        milestones = plan.get("milestones", [])
        if milestones:
            fields.append(
                {
                    "name": "Milestones",
                    "value": str(len(milestones)),
                    "inline": True,
                }
            )

        # Add priority tasks if any
        high_priority_tasks = [
            t
            for t in tasks
            if str(t.get("priority", "")).lower() in {"p0", "p1", "critical", "high"}
        ]
        if high_priority_tasks:
            fields.append(
                {
                    "name": "High Priority Tasks",
                    "value": str(len(high_priority_tasks)),
                    "inline": True,
                }
            )

        # Add blocked tasks if any
        blocked_tasks = [t for t in tasks if t.get("status") == "blocked"]
        if blocked_tasks:
            fields.append(
                {
                    "name": "Blocked Tasks",
                    "value": str(len(blocked_tasks)),
                    "inline": True,
                }
            )

        return fields[: self.MAX_EMBED_FIELDS]

    def _status_summary(self, tasks: list[dict[str, Any]]) -> str:
        """Generate status summary text."""
        status_counts: dict[str, int] = {}
        for task in tasks:
            status = task.get("status", "pending")
            status_counts[status] = status_counts.get(status, 0) + 1

        parts = []
        for status in ["completed", "in_progress", "pending", "blocked", "skipped"]:
            count = status_counts.get(status, 0)
            if count > 0:
                parts.append(f"{count} {status}")

        return ", ".join(parts) or "No tasks"

    def _determine_color(self, tasks: list[dict[str, Any]]) -> int:
        """Determine embed color based on task statuses."""
        if not tasks:
            return 0x808080  # Gray

        # Check for blocked tasks first
        if any(t.get("status") == "blocked" for t in tasks):
            return self.STATUS_COLORS["blocked"]

        # Check overall status
        completed_count = sum(1 for t in tasks if t.get("status") == "completed")
        total_count = len(tasks)

        if completed_count == total_count:
            return self.STATUS_COLORS["completed"]
        elif any(t.get("status") == "in_progress" for t in tasks):
            return self.STATUS_COLORS["in_progress"]
        else:
            return self.STATUS_COLORS["pending"]

    def _truncate(self, text: str, max_length: int) -> str:
        """Truncate text to fit Discord limits."""
        if len(text) <= max_length:
            return text
        return text[: max_length - 3] + "..."

    def _get_timestamp(self) -> str:
        """Get current timestamp in ISO 8601 format."""
        from datetime import datetime, timezone

        return datetime.now(timezone.utc).isoformat()

    def _is_valid_webhook_url(self, url: str) -> bool:
        """Validate Discord webhook URL format."""
        try:
            parsed = urlparse(url)
            return (
                parsed.scheme in ("http", "https")
                and "discord.com" in parsed.netloc
                and "/api/webhooks/" in parsed.path
            )
        except Exception:
            return False

    def _apply_rate_limit(self) -> None:
        """Apply rate limiting between requests."""
        current_time = time.time()
        elapsed = current_time - self._last_request_time
        if elapsed < self.config.rate_limit_delay:
            time.sleep(self.config.rate_limit_delay - elapsed)
        self._last_request_time = time.time()


__all__ = [
    "DiscordExporter",
    "DiscordWebhookConfig",
    "NotificationTrigger",
]
