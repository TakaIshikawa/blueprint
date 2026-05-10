"""Tests for Zapier webhook integration."""

from __future__ import annotations

from typing import Any

import pytest

from blueprint.integrations.zapier import (
    SAMPLE_TEMPLATES,
    SubscriptionStatus,
    TriggerEvent,
    ZapierIntegration,
    format_milestone_payload,
    format_plan_payload,
    format_task_payload,
    sign_payload,
    verify_api_key,
    verify_signature,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def integration() -> ZapierIntegration:
    return ZapierIntegration()


@pytest.fixture
def integration_low_rate() -> ZapierIntegration:
    return ZapierIntegration(rate_limit=3, rate_window=60.0)


SAMPLE_TASK: dict = {
    "id": "task-001",
    "title": "Write tests",
    "description": "Write integration tests",
    "status": "pending",
    "assignee": "alice",
    "priority": "high",
    "plan_id": "plan-001",
}

SAMPLE_MILESTONE: dict = {
    "id": "ms-001",
    "name": "MVP Release",
    "description": "First milestone",
    "due_date": "2025-03-01",
    "plan_id": "plan-001",
}

SAMPLE_PLAN: dict = {
    "id": "plan-001",
    "title": "Project Alpha",
    "status": "in_progress",
    "tasks": [{"id": "t1"}, {"id": "t2"}],
    "milestones": [{"id": "ms1"}],
    "updated_fields": ["status"],
}


# ---------------------------------------------------------------------------
# Webhook registration
# ---------------------------------------------------------------------------


class TestWebhookRegistration:
    def test_register_webhook(self, integration: ZapierIntegration) -> None:
        sub = integration.register_webhook(
            "https://hooks.zapier.com/123",
            TriggerEvent.TASK_CREATED,
            "api-key-abc",
        )
        assert sub.target_url == "https://hooks.zapier.com/123"
        assert sub.event_type == TriggerEvent.TASK_CREATED
        assert sub.api_key == "api-key-abc"
        assert sub.status == SubscriptionStatus.ACTIVE
        assert sub.subscription_id.startswith("sub-")

    def test_unregister_webhook(self, integration: ZapierIntegration) -> None:
        sub = integration.register_webhook(
            "https://hooks.zapier.com/123",
            TriggerEvent.TASK_CREATED,
            "key",
        )
        assert integration.unregister_webhook(sub.subscription_id) is True
        assert integration.get_subscription(sub.subscription_id) is None

    def test_unregister_nonexistent(self, integration: ZapierIntegration) -> None:
        assert integration.unregister_webhook("nonexistent") is False

    def test_list_subscriptions(self, integration: ZapierIntegration) -> None:
        integration.register_webhook("https://a.com", TriggerEvent.TASK_CREATED, "k1")
        integration.register_webhook("https://b.com", TriggerEvent.TASK_COMPLETED, "k2")
        integration.register_webhook("https://c.com", TriggerEvent.TASK_CREATED, "k3")

        all_subs = integration.list_subscriptions()
        assert len(all_subs) == 3

        created_only = integration.list_subscriptions(event_type=TriggerEvent.TASK_CREATED)
        assert len(created_only) == 2

    def test_pause_resume(self, integration: ZapierIntegration) -> None:
        sub = integration.register_webhook("https://a.com", TriggerEvent.TASK_CREATED, "k")
        paused = integration.pause_subscription(sub.subscription_id)
        assert paused is not None
        assert paused.status == SubscriptionStatus.PAUSED

        resumed = integration.resume_subscription(sub.subscription_id)
        assert resumed is not None
        assert resumed.status == SubscriptionStatus.ACTIVE

    def test_pause_nonexistent(self, integration: ZapierIntegration) -> None:
        assert integration.pause_subscription("nope") is None


# ---------------------------------------------------------------------------
# Event subscription model
# ---------------------------------------------------------------------------


class TestEventSubscription:
    def test_subscription_metadata(self, integration: ZapierIntegration) -> None:
        sub = integration.register_webhook(
            "https://hooks.zapier.com/456",
            TriggerEvent.MILESTONE_REACHED,
            "key-xyz",
            metadata={"plan_id": "plan-001"},
        )
        assert sub.metadata["plan_id"] == "plan-001"

    def test_filter_by_status(self, integration: ZapierIntegration) -> None:
        integration.register_webhook("https://a.com", TriggerEvent.TASK_CREATED, "k1")
        sub2 = integration.register_webhook("https://b.com", TriggerEvent.TASK_CREATED, "k2")
        integration.pause_subscription(sub2.subscription_id)

        active = integration.list_subscriptions(status=SubscriptionStatus.ACTIVE)
        paused = integration.list_subscriptions(status=SubscriptionStatus.PAUSED)
        assert len(active) == 1
        assert len(paused) == 1


# ---------------------------------------------------------------------------
# Trigger events
# ---------------------------------------------------------------------------


class TestTriggerEvents:
    def test_fire_task_created(self, integration: ZapierIntegration) -> None:
        integration.register_webhook("https://a.com", TriggerEvent.TASK_CREATED, "k1")
        deliveries = integration.fire_task_created(SAMPLE_TASK)
        assert len(deliveries) == 1
        assert deliveries[0].success is True
        assert deliveries[0].event_type == TriggerEvent.TASK_CREATED

    def test_fire_task_completed(self, integration: ZapierIntegration) -> None:
        integration.register_webhook("https://a.com", TriggerEvent.TASK_COMPLETED, "k1")
        deliveries = integration.fire_task_completed(SAMPLE_TASK)
        assert len(deliveries) == 1
        assert deliveries[0].success is True

    def test_fire_milestone_reached(self, integration: ZapierIntegration) -> None:
        integration.register_webhook("https://a.com", TriggerEvent.MILESTONE_REACHED, "k1")
        deliveries = integration.fire_milestone_reached(SAMPLE_MILESTONE)
        assert len(deliveries) == 1

    def test_fire_plan_updated(self, integration: ZapierIntegration) -> None:
        integration.register_webhook("https://a.com", TriggerEvent.PLAN_UPDATED, "k1")
        deliveries = integration.fire_plan_updated(SAMPLE_PLAN)
        assert len(deliveries) == 1

    def test_no_matching_subscriptions(self, integration: ZapierIntegration) -> None:
        integration.register_webhook("https://a.com", TriggerEvent.TASK_CREATED, "k1")
        deliveries = integration.fire_task_completed(SAMPLE_TASK)
        assert len(deliveries) == 0

    def test_paused_subscription_not_fired(self, integration: ZapierIntegration) -> None:
        sub = integration.register_webhook("https://a.com", TriggerEvent.TASK_CREATED, "k1")
        integration.pause_subscription(sub.subscription_id)
        deliveries = integration.fire_task_created(SAMPLE_TASK)
        assert len(deliveries) == 0


# ---------------------------------------------------------------------------
# Payload formatting
# ---------------------------------------------------------------------------


class TestPayloadFormatting:
    def test_task_payload(self) -> None:
        payload = format_task_payload(SAMPLE_TASK, TriggerEvent.TASK_CREATED)
        assert payload["event"] == "task_created"
        assert payload["data"]["task_id"] == "task-001"
        assert payload["data"]["title"] == "Write tests"
        assert "timestamp" in payload

    def test_milestone_payload(self) -> None:
        payload = format_milestone_payload(SAMPLE_MILESTONE)
        assert payload["event"] == "milestone_reached"
        assert payload["data"]["milestone_id"] == "ms-001"
        assert payload["data"]["name"] == "MVP Release"

    def test_plan_payload(self) -> None:
        payload = format_plan_payload(SAMPLE_PLAN)
        assert payload["event"] == "plan_updated"
        assert payload["data"]["plan_id"] == "plan-001"
        assert payload["data"]["task_count"] == 2
        assert payload["data"]["milestone_count"] == 1


# ---------------------------------------------------------------------------
# API key authentication
# ---------------------------------------------------------------------------


class TestAuthentication:
    def test_verify_api_key_valid(self) -> None:
        assert verify_api_key("secret-key", "secret-key") is True

    def test_verify_api_key_invalid(self) -> None:
        assert verify_api_key("wrong-key", "secret-key") is False

    def test_sign_and_verify_payload(self) -> None:
        payload = {"event": "test", "data": {"id": "1"}}
        sig = sign_payload(payload, "my-secret")
        assert verify_signature(payload, sig, "my-secret") is True
        assert verify_signature(payload, "bad-sig", "my-secret") is False


# ---------------------------------------------------------------------------
# Action endpoints
# ---------------------------------------------------------------------------


class TestActionEndpoints:
    def test_create_task(self, integration: ZapierIntegration) -> None:
        # Register a subscription so the API key is valid
        integration.register_webhook("https://a.com", TriggerEvent.TASK_CREATED, "api-key")
        result = integration.create_task_action(
            {"title": "New Task", "assignee": "bob", "plan_id": "p1"},
            "api-key",
        )
        assert result["success"] is True
        assert result["task"]["title"] == "New Task"
        assert result["task"]["created_via"] == "zapier"

    def test_create_task_invalid_key(self, integration: ZapierIntegration) -> None:
        result = integration.create_task_action({"title": "x"}, "bad-key")
        assert result["success"] is False
        assert "Invalid API key" in result["error"]

    def test_update_task(self, integration: ZapierIntegration) -> None:
        integration.register_webhook("https://a.com", TriggerEvent.TASK_CREATED, "api-key")
        result = integration.update_task_action(
            "task-001",
            {"status": "completed"},
            "api-key",
        )
        assert result["success"] is True
        assert result["task"]["status"] == "completed"
        assert result["task"]["updated_via"] == "zapier"


# ---------------------------------------------------------------------------
# Webhook testing interface
# ---------------------------------------------------------------------------


class TestWebhookTesting:
    def test_test_webhook_success(self, integration: ZapierIntegration) -> None:
        sub = integration.register_webhook("https://a.com", TriggerEvent.TASK_CREATED, "k")
        delivery = integration.test_webhook(sub.subscription_id)
        assert delivery.success is True
        assert delivery.payload.get("test") is True

    def test_test_webhook_not_found(self, integration: ZapierIntegration) -> None:
        delivery = integration.test_webhook("nonexistent")
        assert delivery.success is False
        assert "not found" in (delivery.error or "").lower()


# ---------------------------------------------------------------------------
# Rate limiting
# ---------------------------------------------------------------------------


class TestRateLimiting:
    def test_rate_limit_exceeded(self, integration_low_rate: ZapierIntegration) -> None:
        integration = integration_low_rate
        integration.register_webhook("https://a.com", TriggerEvent.TASK_CREATED, "k")

        # Fire events up to the limit (3)
        for _ in range(3):
            deliveries = integration.fire_task_created(SAMPLE_TASK)
            assert deliveries[0].success is True

        # 4th should be rate limited
        deliveries = integration.fire_task_created(SAMPLE_TASK)
        assert deliveries[0].success is False
        assert "rate limit" in (deliveries[0].error or "").lower()


# ---------------------------------------------------------------------------
# Delivery history
# ---------------------------------------------------------------------------


class TestDeliveryHistory:
    def test_get_deliveries(self, integration: ZapierIntegration) -> None:
        integration.register_webhook("https://a.com", TriggerEvent.TASK_CREATED, "k1")
        integration.register_webhook("https://b.com", TriggerEvent.TASK_COMPLETED, "k2")

        integration.fire_task_created(SAMPLE_TASK)
        integration.fire_task_completed(SAMPLE_TASK)

        all_deliveries = integration.get_deliveries()
        assert len(all_deliveries) == 2

        created_only = integration.get_deliveries(event_type=TriggerEvent.TASK_CREATED)
        assert len(created_only) == 1


# ---------------------------------------------------------------------------
# Templates
# ---------------------------------------------------------------------------


class TestTemplates:
    def test_get_templates(self, integration: ZapierIntegration) -> None:
        templates = integration.get_templates()
        assert len(templates) == len(SAMPLE_TEMPLATES)

    def test_get_template_by_id(self, integration: ZapierIntegration) -> None:
        t = integration.get_template("tpl-task-slack")
        assert t is not None
        assert t.name == "New Task to Slack"

    def test_get_template_not_found(self, integration: ZapierIntegration) -> None:
        assert integration.get_template("nonexistent") is None


# ---------------------------------------------------------------------------
# HTTP sender callback
# ---------------------------------------------------------------------------


class TestHttpSender:
    def test_sender_called_with_payload(self) -> None:
        calls: list[dict] = []

        def mock_sender(**kwargs: Any) -> dict:
            calls.append(kwargs)
            return {"status_code": 200}

        integration = ZapierIntegration(http_sender=mock_sender)
        integration.register_webhook("https://a.com", TriggerEvent.TASK_CREATED, "k1")
        integration.fire_task_created(SAMPLE_TASK)

        assert len(calls) == 1
        assert calls[0]["url"] == "https://a.com"
        assert "X-Blueprint-Signature" in calls[0]["headers"]

    def test_sender_failure(self) -> None:
        def failing_sender(**kwargs: Any) -> dict:
            raise ConnectionError("Connection refused")

        integration = ZapierIntegration(http_sender=failing_sender)
        integration.register_webhook("https://a.com", TriggerEvent.TASK_CREATED, "k1")
        deliveries = integration.fire_task_created(SAMPLE_TASK)

        assert len(deliveries) == 1
        assert deliveries[0].success is False
        assert "Connection refused" in (deliveries[0].error or "")
