"""Tests for n8n workflow node integration."""

from __future__ import annotations

from typing import Any

import pytest

from blueprint.integrations.n8n import (
    CREDENTIAL_DEFINITION,
    REGULAR_NODE,
    TRIGGER_NODE,
    N8nIntegration,
    NodeType,
    ResourceType,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def integration() -> N8nIntegration:
    return N8nIntegration()


@pytest.fixture
def authed(integration: N8nIntegration) -> tuple[N8nIntegration, str]:
    """Integration with a valid credential."""
    cred = integration.create_credential("test-key", base_url="https://bp.example.com")
    return integration, cred.credential_id


# ---------------------------------------------------------------------------
# Credential management
# ---------------------------------------------------------------------------


class TestCredentials:
    def test_create_credential(self, integration: N8nIntegration) -> None:
        cred = integration.create_credential("my-key", label="My API")
        assert cred.api_key == "my-key"
        assert cred.label == "My API"
        assert cred.credential_id.startswith("cred-")

    def test_verify_valid(self, authed: tuple[N8nIntegration, str]) -> None:
        n, cred_id = authed
        assert n.verify_credential(cred_id) is True

    def test_verify_invalid(self, integration: N8nIntegration) -> None:
        assert integration.verify_credential("bad") is False


# ---------------------------------------------------------------------------
# Trigger node (webhook mode)
# ---------------------------------------------------------------------------


class TestTriggerWebhook:
    def test_register_webhook(self, authed: tuple[N8nIntegration, str]) -> None:
        n, cred_id = authed
        wh = n.register_webhook("https://n8n.local/webhook/1", ["task_created"], cred_id)
        assert wh is not None
        assert wh.webhook_id.startswith("n8n-wh-")

    def test_register_invalid_credential(self, integration: N8nIntegration) -> None:
        wh = integration.register_webhook("https://n8n.local/wh", ["task_created"], "bad")
        assert wh is None

    def test_unregister_webhook(self, authed: tuple[N8nIntegration, str]) -> None:
        n, cred_id = authed
        wh = n.register_webhook("https://n8n.local/wh", ["task_created"], cred_id)
        assert wh is not None
        assert n.unregister_webhook(wh.webhook_id) is True
        assert n.unregister_webhook(wh.webhook_id) is False

    def test_fire_webhook_event(self, authed: tuple[N8nIntegration, str]) -> None:
        n, cred_id = authed
        n.register_webhook("https://n8n.local/wh", ["task_created", "task_completed"], cred_id)
        results = n.fire_webhook_event("task_created", {"task_id": "t1"})
        assert len(results) == 1
        assert results[0]["success"] is True

    def test_fire_unmatched_event(self, authed: tuple[N8nIntegration, str]) -> None:
        n, cred_id = authed
        n.register_webhook("https://n8n.local/wh", ["task_created"], cred_id)
        results = n.fire_webhook_event("plan_updated", {"plan_id": "p1"})
        assert len(results) == 0


# ---------------------------------------------------------------------------
# Trigger node (polling mode)
# ---------------------------------------------------------------------------


class TestTriggerPolling:
    def test_poll_events(self, authed: tuple[N8nIntegration, str]) -> None:
        n, cred_id = authed
        # Create tasks via regular node
        n.execute_operation("task", "create", {"title": "A", "plan_id": "p1"}, cred_id)
        n.execute_operation("task", "create", {"title": "B", "plan_id": "p2"}, cred_id)

        results = n.poll_events("task_created", cred_id)
        assert results is not None
        assert len(results) == 2

    def test_poll_with_filter(self, authed: tuple[N8nIntegration, str]) -> None:
        n, cred_id = authed
        n.execute_operation("task", "create", {"title": "A", "plan_id": "p1"}, cred_id)
        n.execute_operation("task", "create", {"title": "B", "plan_id": "p2"}, cred_id)

        results = n.poll_events("task_created", cred_id, plan_id="p1")
        assert results is not None
        assert len(results) == 1

    def test_poll_invalid_credential(self, integration: N8nIntegration) -> None:
        assert integration.poll_events("task_created", "bad") is None


# ---------------------------------------------------------------------------
# Regular node CRUD
# ---------------------------------------------------------------------------


class TestRegularNode:
    def test_create_task(self, authed: tuple[N8nIntegration, str]) -> None:
        n, cred_id = authed
        result = n.execute_operation("task", "create", {"title": "New Task"}, cred_id)
        assert isinstance(result, dict)
        assert result["title"] == "New Task"
        assert result["created_via"] == "n8n"

    def test_read_task(self, authed: tuple[N8nIntegration, str]) -> None:
        n, cred_id = authed
        created = n.execute_operation("task", "create", {"title": "T"}, cred_id)
        assert isinstance(created, dict)

        read = n.execute_operation("task", "read", {"itemId": created["id"]}, cred_id)
        assert isinstance(read, dict)
        assert read["title"] == "T"

    def test_read_nonexistent(self, authed: tuple[N8nIntegration, str]) -> None:
        n, cred_id = authed
        result = n.execute_operation("task", "read", {"itemId": "nope"}, cred_id)
        assert result is None

    def test_update_task(self, authed: tuple[N8nIntegration, str]) -> None:
        n, cred_id = authed
        created = n.execute_operation("task", "create", {"title": "T"}, cred_id)
        assert isinstance(created, dict)

        updated = n.execute_operation(
            "task", "update", {"itemId": created["id"], "status": "completed"}, cred_id
        )
        assert isinstance(updated, dict)
        assert updated["status"] == "completed"

    def test_delete_task(self, authed: tuple[N8nIntegration, str]) -> None:
        n, cred_id = authed
        created = n.execute_operation("task", "create", {"title": "T"}, cred_id)
        assert isinstance(created, dict)

        deleted = n.execute_operation("task", "delete", {"itemId": created["id"]}, cred_id)
        assert isinstance(deleted, dict)
        assert deleted["deleted"] is True

        # Should be gone
        read = n.execute_operation("task", "read", {"itemId": created["id"]}, cred_id)
        assert read is None

    def test_list_tasks(self, authed: tuple[N8nIntegration, str]) -> None:
        n, cred_id = authed
        n.execute_operation("task", "create", {"title": "A", "plan_id": "p1"}, cred_id)
        n.execute_operation("task", "create", {"title": "B", "plan_id": "p1"}, cred_id)
        n.execute_operation("task", "create", {"title": "C", "plan_id": "p2"}, cred_id)

        all_tasks = n.execute_operation("task", "list", {}, cred_id)
        assert isinstance(all_tasks, list)
        assert len(all_tasks) == 3

        filtered = n.execute_operation("task", "list", {"planId": "p1"}, cred_id)
        assert isinstance(filtered, list)
        assert len(filtered) == 2

    def test_list_with_pagination(self, authed: tuple[N8nIntegration, str]) -> None:
        n, cred_id = authed
        for i in range(5):
            n.execute_operation("task", "create", {"title": f"T{i}"}, cred_id)

        page = n.execute_operation("task", "list", {"limit": 2, "offset": 0}, cred_id)
        assert isinstance(page, list)
        assert len(page) == 2

    def test_milestone_crud(self, authed: tuple[N8nIntegration, str]) -> None:
        n, cred_id = authed
        ms = n.execute_operation("milestone", "create", {"title": "MVP"}, cred_id)
        assert isinstance(ms, dict)
        assert ms["name"] == "MVP"

    def test_invalid_credential(self, integration: N8nIntegration) -> None:
        result = integration.execute_operation("task", "create", {"title": "T"}, "bad")
        assert result is None

    def test_unknown_resource(self, authed: tuple[N8nIntegration, str]) -> None:
        n, cred_id = authed
        result = n.execute_operation("unknown", "create", {}, cred_id)
        assert isinstance(result, dict)
        assert "error" in result


# ---------------------------------------------------------------------------
# Resource mapping for dropdowns
# ---------------------------------------------------------------------------


class TestResourceMapping:
    def test_status_options(self, authed: tuple[N8nIntegration, str]) -> None:
        n, cred_id = authed
        options = n.get_resource_options(ResourceType.STATUS, cred_id)
        assert len(options) == 4
        values = [o["value"] for o in options]
        assert "pending" in values
        assert "completed" in values

    def test_task_options(self, authed: tuple[N8nIntegration, str]) -> None:
        n, cred_id = authed
        n.execute_operation("task", "create", {"title": "Alpha"}, cred_id)
        options = n.get_resource_options(ResourceType.TASK, cred_id)
        assert len(options) == 1

    def test_invalid_credential_returns_empty(self, integration: N8nIntegration) -> None:
        options = integration.get_resource_options(ResourceType.STATUS, "bad")
        assert options == []


# ---------------------------------------------------------------------------
# Node export
# ---------------------------------------------------------------------------


class TestNodeExport:
    def test_export_definitions(self, integration: N8nIntegration) -> None:
        export = integration.export_node_definitions()
        assert "nodes" in export
        assert "credentials" in export
        assert len(export["nodes"]) == 2
        assert len(export["credentials"]) == 1

        names = [n["name"] for n in export["nodes"]]
        assert "blueprintTrigger" in names
        assert "blueprint" in names

    def test_trigger_node_definition(self) -> None:
        assert TRIGGER_NODE.node_type == NodeType.TRIGGER
        assert TRIGGER_NODE.name == "blueprintTrigger"
        assert len(TRIGGER_NODE.properties) >= 3

    def test_regular_node_definition(self) -> None:
        assert REGULAR_NODE.node_type == NodeType.REGULAR
        assert REGULAR_NODE.name == "blueprint"

    def test_credential_definition(self) -> None:
        assert CREDENTIAL_DEFINITION["name"] == "blueprintApi"
        assert "properties" in CREDENTIAL_DEFINITION

    def test_node_documentation(self, integration: N8nIntegration) -> None:
        docs = integration.get_node_documentation()
        assert "blueprintTrigger" in docs
        assert "blueprint" in docs


# ---------------------------------------------------------------------------
# Retry logic
# ---------------------------------------------------------------------------


class TestRetryLogic:
    def test_retry_on_failure(self) -> None:
        attempt_count = 0

        def failing_sender(**kwargs: Any) -> None:
            nonlocal attempt_count
            attempt_count += 1
            raise ConnectionError("fail")

        n = N8nIntegration(http_sender=failing_sender, max_retries=3)
        cred = n.create_credential("k")
        n.register_webhook("https://n8n.local/wh", ["task_created"], cred.credential_id)

        results = n.fire_webhook_event("task_created", {"id": "1"})
        assert results[0]["success"] is False
        assert attempt_count == 3

    def test_success_on_first_try(self) -> None:
        calls: list[dict] = []

        def ok_sender(**kwargs: Any) -> None:
            calls.append(kwargs)

        n = N8nIntegration(http_sender=ok_sender, max_retries=3)
        cred = n.create_credential("k")
        n.register_webhook("https://n8n.local/wh", ["task_created"], cred.credential_id)

        results = n.fire_webhook_event("task_created", {"id": "1"})
        assert results[0]["success"] is True
        assert len(calls) == 1
