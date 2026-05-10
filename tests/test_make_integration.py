"""Tests for Make (Integromat) integration."""

from __future__ import annotations

from typing import Any

import pytest

from blueprint.integrations.make import (
    ACTION_MODULES,
    SEARCH_MODULES,
    TRIGGER_MODULES,
    MakeError,
    MakeErrorCode,
    MakeIntegration,
    MakeTriggerEvent,
    WebhookRegistration,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def integration() -> MakeIntegration:
    return MakeIntegration()


@pytest.fixture
def connected(integration: MakeIntegration) -> tuple[MakeIntegration, str]:
    """Integration with a valid connection."""
    conn = integration.create_connection("test-api-key")
    return integration, conn.connection_id


# ---------------------------------------------------------------------------
# Connection management
# ---------------------------------------------------------------------------


class TestConnection:
    def test_create_connection(self, integration: MakeIntegration) -> None:
        conn = integration.create_connection("my-key", label="Test")
        assert conn.api_key == "my-key"
        assert conn.label == "Test"
        assert conn.connection_id.startswith("conn-")

    def test_verify_valid_connection(self, connected: tuple[MakeIntegration, str]) -> None:
        m, conn_id = connected
        result = m.verify_connection(conn_id)
        assert result["valid"] is True

    def test_verify_invalid_connection(self, integration: MakeIntegration) -> None:
        result = integration.verify_connection("bad-id")
        assert result["valid"] is False


# ---------------------------------------------------------------------------
# Trigger modules
# ---------------------------------------------------------------------------


class TestTriggerModules:
    def test_register_instant_trigger(self, connected: tuple[MakeIntegration, str]) -> None:
        m, conn_id = connected
        result = m.register_instant_trigger(
            "https://hook.make.com/abc",
            MakeTriggerEvent.NEW_TASK,
            conn_id,
        )
        assert isinstance(result, WebhookRegistration)
        assert result.url == "https://hook.make.com/abc"

    def test_register_invalid_connection(self, integration: MakeIntegration) -> None:
        result = integration.register_instant_trigger(
            "https://hook.make.com/abc",
            MakeTriggerEvent.NEW_TASK,
            "bad-conn",
        )
        assert isinstance(result, MakeError)

    def test_unregister_trigger(self, connected: tuple[MakeIntegration, str]) -> None:
        m, conn_id = connected
        wh = m.register_instant_trigger("https://h.com", MakeTriggerEvent.NEW_TASK, conn_id)
        assert isinstance(wh, WebhookRegistration)
        assert m.unregister_instant_trigger(wh.webhook_id) is True
        assert m.unregister_instant_trigger(wh.webhook_id) is False

    def test_poll_trigger(self, connected: tuple[MakeIntegration, str]) -> None:
        m, conn_id = connected
        # Add some tasks via actions
        m.execute_action("action_create_task", {"title": "A", "plan_id": "p1"}, conn_id)
        m.execute_action("action_create_task", {"title": "B", "plan_id": "p2"}, conn_id)

        results = m.poll_trigger(MakeTriggerEvent.NEW_TASK, conn_id)
        assert isinstance(results, list)
        assert len(results) == 2

        filtered = m.poll_trigger(MakeTriggerEvent.NEW_TASK, conn_id, plan_id="p1")
        assert isinstance(filtered, list)
        assert len(filtered) == 1

    def test_poll_invalid_connection(self, integration: MakeIntegration) -> None:
        result = integration.poll_trigger(MakeTriggerEvent.NEW_TASK, "bad")
        assert isinstance(result, MakeError)

    def test_fire_instant_trigger(self, connected: tuple[MakeIntegration, str]) -> None:
        m, conn_id = connected
        m.register_instant_trigger("https://h.com", MakeTriggerEvent.NEW_TASK, conn_id)
        results = m.fire_instant_trigger(MakeTriggerEvent.NEW_TASK, {"task_id": "t1"})
        assert len(results) == 1
        assert results[0]["success"] is True


# ---------------------------------------------------------------------------
# Action modules
# ---------------------------------------------------------------------------


class TestActionModules:
    def test_create_task(self, connected: tuple[MakeIntegration, str]) -> None:
        m, conn_id = connected
        result = m.execute_action(
            "action_create_task",
            {"title": "My Task", "plan_id": "p1", "assignee": "alice"},
            conn_id,
        )
        assert isinstance(result, dict)
        assert result["title"] == "My Task"
        assert result["created_via"] == "make"

    def test_create_task_no_title(self, connected: tuple[MakeIntegration, str]) -> None:
        m, conn_id = connected
        result = m.execute_action("action_create_task", {}, conn_id)
        assert isinstance(result, MakeError)
        assert result.code == MakeErrorCode.VALIDATION

    def test_update_status(self, connected: tuple[MakeIntegration, str]) -> None:
        m, conn_id = connected
        task = m.execute_action("action_create_task", {"title": "T"}, conn_id)
        assert isinstance(task, dict)

        updated = m.execute_action(
            "action_update_status",
            {"task_id": task["task_id"], "status": "completed"},
            conn_id,
        )
        assert isinstance(updated, dict)
        assert updated["status"] == "completed"

    def test_update_nonexistent(self, connected: tuple[MakeIntegration, str]) -> None:
        m, conn_id = connected
        result = m.execute_action(
            "action_update_status",
            {"task_id": "nonexistent", "status": "done"},
            conn_id,
        )
        assert isinstance(result, MakeError)
        assert result.code == MakeErrorCode.NOT_FOUND

    def test_add_comment(self, connected: tuple[MakeIntegration, str]) -> None:
        m, conn_id = connected
        task = m.execute_action("action_create_task", {"title": "T"}, conn_id)
        assert isinstance(task, dict)

        comment = m.execute_action(
            "action_add_comment",
            {"task_id": task["task_id"], "comment": "Hello", "author": "bob"},
            conn_id,
        )
        assert isinstance(comment, dict)
        assert comment["comment"] == "Hello"

    def test_unknown_action(self, connected: tuple[MakeIntegration, str]) -> None:
        m, conn_id = connected
        result = m.execute_action("unknown_action", {}, conn_id)
        assert isinstance(result, MakeError)

    def test_invalid_connection_action(self, integration: MakeIntegration) -> None:
        result = integration.execute_action("action_create_task", {"title": "T"}, "bad")
        assert isinstance(result, MakeError)


# ---------------------------------------------------------------------------
# Search modules
# ---------------------------------------------------------------------------


class TestSearchModules:
    def test_search_tasks(self, connected: tuple[MakeIntegration, str]) -> None:
        m, conn_id = connected
        m.execute_action("action_create_task", {"title": "Alpha", "plan_id": "p1"}, conn_id)
        m.execute_action("action_create_task", {"title": "Beta", "plan_id": "p2"}, conn_id)
        m.execute_action("action_create_task", {"title": "Alpha Two", "plan_id": "p1"}, conn_id)

        # Search by plan_id
        results = m.execute_search("search_tasks", {"plan_id": "p1"}, conn_id)
        assert isinstance(results, list)
        assert len(results) == 2

        # Search by query
        results = m.execute_search("search_tasks", {"query": "beta"}, conn_id)
        assert isinstance(results, list)
        assert len(results) == 1

    def test_search_with_pagination(self, connected: tuple[MakeIntegration, str]) -> None:
        m, conn_id = connected
        for i in range(5):
            m.execute_action("action_create_task", {"title": f"Task {i}"}, conn_id)

        results = m.execute_search("search_tasks", {"limit": 2, "offset": 0}, conn_id)
        assert isinstance(results, list)
        assert len(results) == 2

    def test_unknown_search(self, connected: tuple[MakeIntegration, str]) -> None:
        m, conn_id = connected
        result = m.execute_search("unknown_search", {}, conn_id)
        assert isinstance(result, MakeError)

    def test_search_invalid_connection(self, integration: MakeIntegration) -> None:
        result = integration.execute_search("search_tasks", {}, "bad")
        assert isinstance(result, MakeError)


# ---------------------------------------------------------------------------
# Module metadata
# ---------------------------------------------------------------------------


class TestModuleMetadata:
    def test_get_all_definitions(self, integration: MakeIntegration) -> None:
        defs = integration.get_module_definitions()
        expected_count = len(TRIGGER_MODULES) + len(ACTION_MODULES) + len(SEARCH_MODULES)
        assert len(defs) == expected_count

    def test_get_module_by_id(self, integration: MakeIntegration) -> None:
        m = integration.get_module("action_create_task")
        assert m is not None
        assert m.name == "Create a Task"

    def test_get_module_not_found(self, integration: MakeIntegration) -> None:
        assert integration.get_module("nonexistent") is None

    def test_trigger_module_has_params(self) -> None:
        assert len(TRIGGER_MODULES) >= 3
        for t in TRIGGER_MODULES:
            assert t.module_type.value in ("trigger", "instant_trigger")


# ---------------------------------------------------------------------------
# HTTP sender
# ---------------------------------------------------------------------------


class TestHttpSender:
    def test_webhook_delivery_with_sender(self) -> None:
        calls: list[dict] = []

        def mock_sender(**kwargs: Any) -> None:
            calls.append(kwargs)

        m = MakeIntegration(http_sender=mock_sender)
        conn = m.create_connection("k")
        wh = m.register_instant_trigger("https://h.com", MakeTriggerEvent.NEW_TASK, conn.connection_id)
        assert isinstance(wh, WebhookRegistration)

        m.fire_instant_trigger(MakeTriggerEvent.NEW_TASK, {"id": "1"})
        assert len(calls) == 1

    def test_webhook_delivery_failure(self) -> None:
        def failing(**kwargs: Any) -> None:
            raise ConnectionError("fail")

        m = MakeIntegration(http_sender=failing)
        conn = m.create_connection("k")
        m.register_instant_trigger("https://h.com", MakeTriggerEvent.NEW_TASK, conn.connection_id)

        results = m.fire_instant_trigger(MakeTriggerEvent.NEW_TASK, {"id": "1"})
        assert results[0]["success"] is False
