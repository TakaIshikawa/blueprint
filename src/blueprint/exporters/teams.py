"""Microsoft Teams webhook and bot API exporter for execution plan notifications."""

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
class TeamsWebhookConfig:
    """Configuration for Teams webhook and bot API routing."""

    webhooks: dict[str, str] = field(default_factory=dict)
    routing_rules: dict[str, str] = field(default_factory=dict)
    rate_limit_delay: float = 0.5  # Conservative rate limiting
    enabled_triggers: tuple[NotificationTrigger, ...] = (
        "plan_creation",
        "task_completion",
        "milestone_reached",
        "risk_detected",
    )
    use_adaptive_cards: bool = True  # Use Adaptive Cards instead of message cards

    def get_webhook_url(self, route_key: str) -> str | None:
        """Get webhook URL for a given route key (team, priority, or project)."""
        if route_key in self.webhooks:
            return self.webhooks[route_key]
        if route_key in self.routing_rules:
            target_key = self.routing_rules[route_key]
            return self.webhooks.get(target_key)
        return self.webhooks.get("default")


class TeamsExporter(TargetExporter):
    """Export execution plan summaries to Microsoft Teams channels."""

    # Adaptive Card limits
    MAX_CARD_TITLE = 150
    MAX_TEXT_BLOCK = 2000
    MAX_FACT_VALUE = 500

    # Status colors (theme colors)
    STATUS_COLORS = {
        "pending": "default",
        "in_progress": "accent",
        "completed": "good",
        "blocked": "attention",
        "skipped": "default",
    }

    def __init__(self, config: TeamsWebhookConfig | None = None):
        """Initialize Teams exporter with configuration."""
        self.config = config or TeamsWebhookConfig()
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
        """Export execution plan summary to Teams webhook format."""
        plan, brief = self.validate_export_payload(execution_plan, implementation_brief)
        self.ensure_output_dir(output_path)

        # Build Teams payload
        if self.config.use_adaptive_cards:
            payload = self.build_adaptive_card_payload(plan, brief)
        else:
            payload = self.build_message_card_payload(plan, brief)

        # Write payload to output file
        with open(output_path, "w") as f:
            json.dump(payload, f, indent=2)

        return output_path

    def build_adaptive_card_payload(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
    ) -> dict[str, Any]:
        """Build Adaptive Card payload for execution plan summary."""
        tasks = plan.get("tasks", [])
        status_summary = self._status_summary(tasks)

        card = {
            "type": "AdaptiveCard",
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "version": "1.4",
            "body": [
                {
                    "type": "TextBlock",
                    "text": self._truncate(f"Plan: {plan['id']}", self.MAX_CARD_TITLE),
                    "weight": "bolder",
                    "size": "large",
                },
                {
                    "type": "TextBlock",
                    "text": self._truncate(brief.get("title", ""), self.MAX_TEXT_BLOCK),
                    "wrap": True,
                },
                {
                    "type": "FactSet",
                    "facts": self._build_facts(plan, brief, tasks, status_summary),
                },
            ],
        }

        # Add actions if applicable
        actions = self._build_actions(plan)
        if actions:
            card["actions"] = actions

        return {
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "content": card,
                }
            ],
        }

    def build_message_card_payload(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
    ) -> dict[str, Any]:
        """Build legacy Message Card payload for execution plan summary."""
        tasks = plan.get("tasks", [])
        status_summary = self._status_summary(tasks)
        color = self._determine_color(tasks)

        return {
            "@type": "MessageCard",
            "@context": "https://schema.org/extensions",
            "summary": f"Plan: {plan['id']}",
            "themeColor": color,
            "title": self._truncate(f"Plan: {plan['id']}", self.MAX_CARD_TITLE),
            "sections": [
                {
                    "activityTitle": brief.get("title", "Execution Plan"),
                    "facts": self._build_facts(plan, brief, tasks, status_summary),
                }
            ],
        }

    def send_to_webhook(
        self,
        webhook_url: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Send payload to Teams webhook with rate limiting.

        Note: This is a placeholder implementation. In production, you would use
        requests library or httpx to actually send the payload.

        Args:
            webhook_url: Teams webhook URL
            payload: Teams card payload

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
        Send notification to Teams channel based on trigger.

        Args:
            plan: Execution plan data
            brief: Implementation brief data
            trigger: Notification trigger type
            route_key: Routing key (team, priority, or project identifier)

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
            if self.config.use_adaptive_cards:
                return self.build_adaptive_card_payload(plan, brief)
            else:
                return self.build_message_card_payload(plan, brief)

    def _plan_creation_payload(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        tasks: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Build payload for plan creation notification."""
        card = {
            "type": "AdaptiveCard",
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "version": "1.4",
            "body": [
                {
                    "type": "TextBlock",
                    "text": "New Execution Plan Created",
                    "weight": "bolder",
                    "size": "large",
                    "color": "accent",
                },
                {
                    "type": "TextBlock",
                    "text": brief.get("title", ""),
                    "wrap": True,
                },
                {
                    "type": "FactSet",
                    "facts": [
                        {"title": "Plan ID", "value": plan["id"]},
                        {"title": "Total Tasks", "value": str(len(tasks))},
                        {"title": "Status", "value": self._status_summary(tasks)},
                    ],
                },
            ],
        }
        return {
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "content": card,
                }
            ],
        }

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

        card = {
            "type": "AdaptiveCard",
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "version": "1.4",
            "body": [
                {
                    "type": "TextBlock",
                    "text": "Task Completed",
                    "weight": "bolder",
                    "size": "large",
                    "color": "good",
                },
                {
                    "type": "TextBlock",
                    "text": f"Progress: {len(completed_tasks)}/{total_tasks} ({progress:.1f}%)",
                    "wrap": True,
                },
                {
                    "type": "FactSet",
                    "facts": [
                        {"title": "Plan ID", "value": plan["id"]},
                        {"title": "Completed", "value": str(len(completed_tasks))},
                    ],
                },
            ],
        }
        return {
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "content": card,
                }
            ],
        }

    def _milestone_reached_payload(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        tasks: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Build payload for milestone reached notification."""
        milestones = plan.get("milestones", [])
        milestone_name = milestones[0].get("name", "Milestone") if milestones else "Milestone"

        card = {
            "type": "AdaptiveCard",
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "version": "1.4",
            "body": [
                {
                    "type": "TextBlock",
                    "text": "Milestone Reached",
                    "weight": "bolder",
                    "size": "large",
                    "color": "accent",
                },
                {
                    "type": "TextBlock",
                    "text": str(milestone_name),
                    "wrap": True,
                },
                {
                    "type": "FactSet",
                    "facts": [
                        {"title": "Plan ID", "value": plan["id"]},
                        {"title": "Total Milestones", "value": str(len(milestones))},
                    ],
                },
            ],
        }
        return {
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "content": card,
                }
            ],
        }

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

        card = {
            "type": "AdaptiveCard",
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "version": "1.4",
            "body": [
                {
                    "type": "TextBlock",
                    "text": "Risks Detected",
                    "weight": "bolder",
                    "size": "large",
                    "color": "attention",
                },
                {
                    "type": "TextBlock",
                    "text": f"{len(high_risk_tasks)} high-risk task(s) detected",
                    "wrap": True,
                },
                {
                    "type": "FactSet",
                    "facts": [
                        {"title": "Plan ID", "value": plan["id"]},
                        {"title": "High Risk Tasks", "value": str(len(high_risk_tasks))},
                    ],
                },
            ],
        }

        # Add first few high-risk tasks
        for task in high_risk_tasks[:3]:
            card["body"].append(
                {
                    "type": "TextBlock",
                    "text": f"**{self._truncate(task.get('title', 'Unnamed Task'), 50)}**",
                    "wrap": True,
                    "spacing": "small",
                }
            )

        return {
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "content": card,
                }
            ],
        }

    def _build_facts(
        self,
        plan: dict[str, Any],
        brief: dict[str, Any],
        tasks: list[dict[str, Any]],
        status_summary: str,
    ) -> list[dict[str, str]]:
        """Build fact set for adaptive cards."""
        facts = [
            {"title": "Total Tasks", "value": str(len(tasks))},
            {"title": "Status", "value": self._truncate(status_summary, self.MAX_FACT_VALUE)},
        ]

        # Add milestone info if available
        milestones = plan.get("milestones", [])
        if milestones:
            facts.append({"title": "Milestones", "value": str(len(milestones))})

        # Add high-risk tasks if any
        high_risk_tasks = [
            t
            for t in tasks
            if str(t.get("risk_level", "")).lower() in {"critical", "high", "blocker"}
        ]
        if high_risk_tasks:
            facts.append({"title": "High Risk Tasks", "value": str(len(high_risk_tasks))})

        # Add blocked tasks if any
        blocked_tasks = [t for t in tasks if t.get("status") == "blocked"]
        if blocked_tasks:
            facts.append({"title": "Blocked Tasks", "value": str(len(blocked_tasks))})

        return facts

    def _build_actions(self, plan: dict[str, Any]) -> list[dict[str, Any]]:
        """Build action buttons for adaptive cards."""
        # Placeholder for action buttons (e.g., link to plan details)
        # In production, these would link to your plan management system
        return []

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
        """Determine theme color based on task statuses (for message cards)."""
        if not tasks:
            return "808080"  # Gray

        # Check for blocked tasks first
        if any(t.get("status") == "blocked" for t in tasks):
            return "E74C3C"  # Red

        # Check overall status
        completed_count = sum(1 for t in tasks if t.get("status") == "completed")
        total_count = len(tasks)

        if completed_count == total_count:
            return "2ECC71"  # Green
        elif any(t.get("status") == "in_progress" for t in tasks):
            return "3498DB"  # Blue
        else:
            return "808080"  # Gray

    def _truncate(self, text: str, max_length: int) -> str:
        """Truncate text to fit Teams limits."""
        if len(text) <= max_length:
            return text
        return text[: max_length - 3] + "..."

    def _is_valid_webhook_url(self, url: str) -> bool:
        """Validate Teams webhook URL format."""
        try:
            parsed = urlparse(url)
            # Teams webhooks are typically on outlook.office.com or specific regional endpoints
            # Webhooks can have either /webhookb2/ or /IncomingWebhook/ in path
            return (
                parsed.scheme in ("http", "https")
                and any(
                    domain in parsed.netloc
                    for domain in ["office.com", "office365.com", "outlook.com"]
                )
                and ("/webhookb2/" in parsed.path or "/IncomingWebhook/" in parsed.path)
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
    "TeamsExporter",
    "TeamsWebhookConfig",
    "NotificationTrigger",
]
