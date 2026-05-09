"""Slack webhook and bot API exporter for execution plan notifications."""

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
    "risk_detected",
    "status_change",
]


@dataclass(frozen=True, slots=True)
class SlackWebhookConfig:
    """Configuration for Slack webhook and bot API routing."""

    webhooks: dict[str, str] = field(default_factory=dict)
    routing_rules: dict[str, str] = field(default_factory=dict)
    rate_limit_delay: float = 1.0  # Slack Tier 1: 1 message per second
    enabled_triggers: tuple[NotificationTrigger, ...] = (
        "plan_creation",
        "task_completion",
        "milestone_reached",
        "risk_detected",
    )
    use_blocks: bool = True  # Use Block Kit instead of plain text

    def get_webhook_url(self, route_key: str) -> str | None:
        """Get webhook URL for a given route key (channel, priority, or project)."""
        if route_key in self.webhooks:
            return self.webhooks[route_key]
        if route_key in self.routing_rules:
            target_key = self.routing_rules[route_key]
            return self.webhooks.get(target_key)
        return self.webhooks.get("default")


class SlackExporter(TargetExporter):
    """Export execution plan summaries to Slack channels via webhooks or bot API."""

    # Slack Block Kit limits
    MAX_BLOCKS = 50
    MAX_TEXT_LENGTH = 3000
    MAX_FIELDS = 10
    MAX_FIELD_TEXT = 2000

    # Status colors for attachments (fallback)
    STATUS_COLORS = {
        "pending": "#808080",  # Gray
        "in_progress": "#3498DB",  # Blue
        "completed": "#2ECC71",  # Green
        "blocked": "#E74C3C",  # Red
        "skipped": "#95A5A6",  # Light gray
    }

    def __init__(self, config: SlackWebhookConfig | None = None):
        """Initialize Slack exporter with configuration."""
        self.config = config or SlackWebhookConfig()
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
        """Export execution plan summary to Slack webhook format."""
        plan, brief = self.validate_export_payload(execution_plan, implementation_brief)
        self.ensure_output_dir(output_path)

        # Build Slack payload
        if self.config.use_blocks:
            payload = self.build_block_kit_payload(plan, brief)
        else:
            payload = self.build_attachment_payload(plan, brief)

        # Write payload to output file
        with open(output_path, "w") as f:
            json.dump(payload, f, indent=2)

        return output_path

    def build_block_kit_payload(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
    ) -> dict[str, Any]:
        """Build Slack Block Kit payload for execution plan summary."""
        tasks = plan.get("tasks", [])
        status_summary = self._status_summary(tasks)

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": self._truncate(f"Plan: {plan['id']}", 150),
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": self._truncate(
                        f"*{brief.get('title', 'Execution Plan')}*", self.MAX_TEXT_LENGTH
                    ),
                },
            },
            {"type": "divider"},
            {
                "type": "section",
                "fields": self._build_fields(plan, brief, tasks, status_summary),
            },
        ]

        # Add action buttons if applicable
        actions = self._build_actions(plan)
        if actions:
            blocks.append(actions)

        # Add context footer
        blocks.append(
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"Plan ID: `{plan['id']}` | Status: {status_summary}",
                    }
                ],
            }
        )

        return {"blocks": blocks[: self.MAX_BLOCKS]}

    def build_attachment_payload(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
    ) -> dict[str, Any]:
        """Build legacy Slack attachment payload for execution plan summary."""
        tasks = plan.get("tasks", [])
        status_summary = self._status_summary(tasks)
        color = self._determine_color(tasks)

        attachment = {
            "color": color,
            "title": self._truncate(f"Plan: {plan['id']}", 150),
            "text": brief.get("title", "Execution Plan"),
            "fields": self._build_attachment_fields(plan, brief, tasks, status_summary),
            "footer": f"Plan ID: {plan['id']}",
            "ts": int(time.time()),
        }

        return {"attachments": [attachment]}

    def send_to_webhook(
        self,
        webhook_url: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Send payload to Slack webhook with rate limiting.

        Note: This is a placeholder implementation. In production, you would use
        requests library or httpx to actually send the payload.

        Args:
            webhook_url: Slack webhook URL
            payload: Slack block kit or attachment payload

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
        Send notification to Slack channel based on trigger.

        Args:
            plan: Execution plan data
            brief: Implementation brief data
            trigger: Notification trigger type
            route_key: Routing key (channel, priority, or project identifier)

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
        elif trigger == "risk_detected":
            return self._risk_detected_payload(plan, brief, tasks)
        else:
            if self.config.use_blocks:
                return self.build_block_kit_payload(plan, brief)
            else:
                return self.build_attachment_payload(plan, brief)

    def _plan_creation_payload(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        tasks: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Build payload for plan creation notification."""
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": ":rocket: New Execution Plan Created",
                    "emoji": True,
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*{brief.get('title', '')}*",
                },
            },
            {"type": "divider"},
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Plan ID:*\n`{plan['id']}`"},
                    {"type": "mrkdwn", "text": f"*Total Tasks:*\n{len(tasks)}"},
                    {
                        "type": "mrkdwn",
                        "text": f"*Status:*\n{self._status_summary(tasks)}",
                    },
                ],
            },
        ]

        return {"blocks": blocks}

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

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": ":white_check_mark: Task Completed",
                    "emoji": True,
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Progress:* {len(completed_tasks)}/{total_tasks} ({progress:.1f}%)",
                },
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Plan ID:*\n`{plan['id']}`"},
                    {
                        "type": "mrkdwn",
                        "text": f"*Completed:*\n{len(completed_tasks)} tasks",
                    },
                ],
            },
        ]

        return {"blocks": blocks}

    def _milestone_reached_payload(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        tasks: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Build payload for milestone reached notification."""
        milestones = plan.get("milestones", [])
        milestone_name = (
            milestones[0].get("name", "Milestone") if milestones else "Milestone"
        )

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": ":trophy: Milestone Reached",
                    "emoji": True,
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*{milestone_name}*",
                },
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Plan ID:*\n`{plan['id']}`"},
                    {
                        "type": "mrkdwn",
                        "text": f"*Total Milestones:*\n{len(milestones)}",
                    },
                ],
            },
        ]

        return {"blocks": blocks}

    def _risk_detected_payload(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        tasks: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Build payload for risk detected notification."""
        high_risk_tasks = [
            t
            for t in tasks
            if str(t.get("risk_level", "")).lower() in {"critical", "high", "blocker"}
        ]

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": ":warning: Risks Detected",
                    "emoji": True,
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*{len(high_risk_tasks)} high-risk task(s) detected*",
                },
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Plan ID:*\n`{plan['id']}`"},
                    {
                        "type": "mrkdwn",
                        "text": f"*High Risk Tasks:*\n{len(high_risk_tasks)}",
                    },
                ],
            },
        ]

        # Add first few high-risk tasks
        if high_risk_tasks:
            task_list = []
            for task in high_risk_tasks[:3]:
                task_title = self._truncate(task.get("title", "Unnamed Task"), 50)
                task_list.append(f"• `{task.get('id')}` - {task_title}")

            blocks.append(
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "*High-Risk Tasks:*\n" + "\n".join(task_list),
                    },
                }
            )

        return {"blocks": blocks}

    def _build_fields(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        tasks: list[dict[str, Any]],
        status_summary: str,
    ) -> list[dict[str, str]]:
        """Build fields for Slack Block Kit sections."""
        fields = [
            {"type": "mrkdwn", "text": f"*Total Tasks:*\n{len(tasks)}"},
            {
                "type": "mrkdwn",
                "text": f"*Status:*\n{self._truncate(status_summary, self.MAX_FIELD_TEXT)}",
            },
        ]

        # Add milestone info if available
        milestones = plan.get("milestones", [])
        if milestones:
            fields.append({"type": "mrkdwn", "text": f"*Milestones:*\n{len(milestones)}"})

        # Add high-risk tasks if any
        high_risk_tasks = [
            t
            for t in tasks
            if str(t.get("risk_level", "")).lower() in {"critical", "high", "blocker"}
        ]
        if high_risk_tasks:
            fields.append(
                {"type": "mrkdwn", "text": f"*High Risk Tasks:*\n{len(high_risk_tasks)}"}
            )

        # Add blocked tasks if any
        blocked_tasks = [t for t in tasks if t.get("status") == "blocked"]
        if blocked_tasks:
            fields.append(
                {"type": "mrkdwn", "text": f"*Blocked Tasks:*\n{len(blocked_tasks)}"}
            )

        return fields[: self.MAX_FIELDS]

    def _build_attachment_fields(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        tasks: list[dict[str, Any]],
        status_summary: str,
    ) -> list[dict[str, Any]]:
        """Build fields for legacy Slack attachments."""
        fields = [
            {"title": "Total Tasks", "value": str(len(tasks)), "short": True},
            {"title": "Status", "value": status_summary, "short": True},
        ]

        # Add milestone info if available
        milestones = plan.get("milestones", [])
        if milestones:
            fields.append(
                {"title": "Milestones", "value": str(len(milestones)), "short": True}
            )

        # Add blocked tasks if any
        blocked_tasks = [t for t in tasks if t.get("status") == "blocked"]
        if blocked_tasks:
            fields.append(
                {"title": "Blocked Tasks", "value": str(len(blocked_tasks)), "short": True}
            )

        return fields

    def _build_actions(self, plan: dict[str, Any]) -> dict[str, Any] | None:
        """Build action buttons for Slack Block Kit."""
        # Placeholder for action buttons (e.g., link to plan details)
        # In production, these would link to your plan management system
        # Example:
        # return {
        #     "type": "actions",
        #     "elements": [
        #         {
        #             "type": "button",
        #             "text": {"type": "plain_text", "text": "View Plan"},
        #             "url": f"https://example.com/plans/{plan['id']}",
        #         }
        #     ],
        # }
        return None

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

    def _determine_color(self, tasks: list[dict[str, Any]]) -> str:
        """Determine attachment color based on task statuses."""
        if not tasks:
            return self.STATUS_COLORS["pending"]

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
        """Truncate text to fit Slack limits."""
        if len(text) <= max_length:
            return text
        return text[: max_length - 3] + "..."

    def _is_valid_webhook_url(self, url: str) -> bool:
        """Validate Slack webhook URL format."""
        try:
            parsed = urlparse(url)
            # Slack webhooks are typically on hooks.slack.com
            return (
                parsed.scheme in ("http", "https")
                and "slack.com" in parsed.netloc
                and "/services/" in parsed.path
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
    "SlackExporter",
    "SlackWebhookConfig",
    "NotificationTrigger",
]
