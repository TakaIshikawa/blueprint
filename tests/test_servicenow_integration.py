"""Tests for the ServiceNow integration."""

import json

from blueprint.integrations.servicenow_integration import (
    CHANGE_STATES,
    CHANGE_STATE_TO_PLAN_STATUS,
    PLAN_STATUS_TO_CHANGE_STATE,
    TASK_STATUS_TO_SN_STATE,
    ChangeRequest,
    ServiceNowClient,
    ServiceNowIntegration,
    SyncResult,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


class _Response:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self):
        return json.dumps(self.payload).encode("utf-8")


def _sample_plan() -> dict:
    return {
        "id": "plan-001",
        "implementation_brief_id": "ib-001",
        "target_engine": "relay",
        "target_repo": "acme/widgets",
        "project_type": "web",
        "milestones": [
            {"name": "Sprint 1", "description": "First sprint", "start_date": "2026-05-01", "due_date": "2026-05-14"},
        ],
        "test_strategy": "pytest",
        "handoff_prompt": None,
        "status": "in_progress",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
        "metadata": {},
        "tasks": [
            {
                "id": "task-1",
                "execution_plan_id": "plan-001",
                "title": "Implement auth",
                "description": "Build OAuth2 login",
                "milestone": "Sprint 1",
                "owner_type": "developer",
                "suggested_engine": None,
                "depends_on": [],
                "files_or_modules": None,
                "acceptance_criteria": ["Users can log in"],
                "estimated_complexity": "high",
                "estimated_hours": 16.0,
                "risk_level": "high",
                "test_command": "pytest",
                "status": "completed",
                "metadata": {},
                "blocked_reason": None,
                "created_at": None,
                "updated_at": None,
            },
            {
                "id": "task-2",
                "execution_plan_id": "plan-001",
                "title": "Add dashboard",
                "description": "Create user dashboard",
                "milestone": "Sprint 1",
                "owner_type": "developer",
                "suggested_engine": None,
                "depends_on": ["task-1"],
                "files_or_modules": None,
                "acceptance_criteria": ["Dashboard shows metrics"],
                "estimated_complexity": "medium",
                "estimated_hours": 8.0,
                "risk_level": "medium",
                "test_command": None,
                "status": "in_progress",
                "metadata": {},
                "blocked_reason": None,
                "created_at": None,
                "updated_at": None,
            },
        ],
    }


def _sample_brief() -> dict:
    return {
        "id": "ib-001",
        "source_brief_id": "sb-001",
        "title": "Widget Authentication System",
        "domain": "web",
        "target_user": None,
        "buyer": None,
        "workflow_context": None,
        "problem_statement": "Users need secure authentication",
        "mvp_goal": "Working OAuth2 login",
        "product_surface": None,
        "scope": ["auth"],
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Tests",
        "definition_of_done": ["Tests pass"],
        "status": "planned",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }


def _change_request_response(
    sys_id: str = "cr-001",
    number: str = "CHG0001234",
    state: str = "-5",
) -> dict:
    return {
        "result": {
            "sys_id": sys_id,
            "number": number,
            "short_description": "Blueprint Plan: plan-001",
            "description": "Test change request",
            "state": state,
            "assignment_group": "Dev Team",
            "type": "normal",
        }
    }


def _project_tasks_response(tasks: list[dict] | None = None) -> dict:
    if tasks is None:
        tasks = [
            {
                "sys_id": "pt-001",
                "short_description": "Implement auth",
                "description": "Build OAuth2 login\n\nBlueprint: task-1",
                "state": "3",
                "change_request": "cr-001",
            }
        ]
    return {"result": tasks}


# ---------------------------------------------------------------------------
# ServiceNowClient tests
# ---------------------------------------------------------------------------


def test_client_get():
    seen = {}

    def fake_open(request, timeout):
        seen["url"] = request.full_url
        seen["method"] = request.method
        seen["auth"] = request.headers.get("Authorization")
        return _Response({"result": {"sys_id": "abc", "state": "-5"}})

    client = ServiceNowClient(
        instance="https://test.service-now.com",
        username="admin",
        password="password123",
        http_open=fake_open,
    )

    result = client.get("change_request", "abc")

    assert "test.service-now.com/api/now/table/change_request/abc" in seen["url"]
    assert seen["method"] == "GET"
    assert seen["auth"].startswith("Basic ")
    assert result["result"]["sys_id"] == "abc"


def test_client_query():
    seen = {}

    def fake_open(request, timeout):
        seen["url"] = request.full_url
        return _Response({"result": [{"sys_id": "1"}, {"sys_id": "2"}]})

    client = ServiceNowClient(
        instance="test.service-now.com",
        username="admin",
        password="pwd",
        http_open=fake_open,
    )

    results = client.query("incident", {"sysparm_limit": "10"})
    assert "sysparm_limit=10" in seen["url"]
    assert len(results) == 2


def test_client_create():
    seen = {}

    def fake_open(request, timeout):
        seen["method"] = request.method
        seen["body"] = json.loads(request.data.decode("utf-8"))
        seen["content_type"] = request.headers.get("Content-type")
        return _Response({"result": {"sys_id": "new-001", "number": "CHG001"}})

    client = ServiceNowClient(
        instance="https://test.service-now.com",
        username="admin",
        password="pwd",
        http_open=fake_open,
    )

    result = client.create("change_request", {"short_description": "Test"})
    assert seen["method"] == "POST"
    assert seen["body"]["short_description"] == "Test"
    assert seen["content_type"] == "application/json"
    assert result["result"]["sys_id"] == "new-001"


def test_client_update():
    seen = {}

    def fake_open(request, timeout):
        seen["method"] = request.method
        seen["url"] = request.full_url
        seen["body"] = json.loads(request.data.decode("utf-8"))
        return _Response({"result": {"sys_id": "abc", "state": "0"}})

    client = ServiceNowClient(
        instance="https://test.service-now.com",
        username="admin",
        password="pwd",
        http_open=fake_open,
    )

    result = client.update("change_request", "abc", {"state": "0"})
    assert seen["method"] == "PATCH"
    assert "change_request/abc" in seen["url"]
    assert seen["body"]["state"] == "0"


def test_client_auto_adds_https():
    seen = {}

    def fake_open(request, timeout):
        seen["url"] = request.full_url
        return _Response({"result": {}})

    client = ServiceNowClient(
        instance="test.service-now.com",
        username="u",
        password="p",
        http_open=fake_open,
    )
    client.get("incident", "x")
    assert seen["url"].startswith("https://test.service-now.com")


# ---------------------------------------------------------------------------
# ServiceNowIntegration tests
# ---------------------------------------------------------------------------


def test_authenticate(monkeypatch):
    seen = {}

    def fake_open(request, timeout):
        seen["url"] = request.full_url
        return _Response({"result": [{"sys_id": "user-1"}]})

    integration = ServiceNowIntegration(http_open=fake_open)
    client = integration.authenticate(
        "https://test.service-now.com",
        "admin",
        "password",
    )

    assert isinstance(client, ServiceNowClient)
    assert "sys_user" in seen["url"]


def test_map_plan_to_change_request():
    integration = ServiceNowIntegration(http_open=lambda r, t: None)
    payload = integration.map_plan_to_change_request(_sample_plan(), _sample_brief())

    assert payload["short_description"] == "Widget Authentication System"
    assert "plan-001" in payload["description"]
    assert "implementation_plan" in payload
    assert "Implement auth" in payload["implementation_plan"]
    assert "Add dashboard" in payload["implementation_plan"]
    assert "risk_impact_analysis" in payload
    assert "high risk" in payload["risk_impact_analysis"].lower()
    assert payload["state"] == "0"  # in_progress -> implementing
    assert payload["start_date"] == "2026-05-01"
    assert payload["end_date"] == "2026-05-14"


def test_map_plan_to_change_request_without_brief():
    integration = ServiceNowIntegration(http_open=lambda r, t: None)
    payload = integration.map_plan_to_change_request(_sample_plan())

    assert "Blueprint Plan: plan-001" in payload["short_description"]


def test_create_change_request(monkeypatch):
    def fake_open(request, timeout):
        return _Response(_change_request_response())

    monkeypatch.setenv("SERVICENOW_INSTANCE", "https://test.service-now.com")
    monkeypatch.setenv("SERVICENOW_USERNAME", "admin")
    monkeypatch.setenv("SERVICENOW_PASSWORD", "pwd")

    integration = ServiceNowIntegration(http_open=fake_open)
    cr = integration.create_change_request(_sample_plan(), _sample_brief())

    assert isinstance(cr, ChangeRequest)
    assert cr.sys_id == "cr-001"
    assert cr.number == "CHG0001234"


def test_sync_tasks_to_project_tasks(monkeypatch):
    created = []

    def fake_open(request, timeout):
        if request.method == "POST":
            body = json.loads(request.data.decode("utf-8"))
            created.append(body)
        return _Response({"result": {"sys_id": f"pt-{len(created)}", "short_description": "task"}})

    monkeypatch.setenv("SERVICENOW_INSTANCE", "https://test.service-now.com")
    monkeypatch.setenv("SERVICENOW_USERNAME", "admin")
    monkeypatch.setenv("SERVICENOW_PASSWORD", "pwd")

    integration = ServiceNowIntegration(http_open=fake_open)
    results = integration.sync_tasks_to_project_tasks(_sample_plan(), "cr-001")

    assert len(results) == 2
    assert len(created) == 2
    assert created[0]["short_description"] == "Implement auth"
    assert created[0]["change_request"] == "cr-001"
    assert created[0]["state"] == "3"  # completed
    assert created[0]["percent_complete"] == "100"
    assert created[1]["short_description"] == "Add dashboard"
    assert created[1]["state"] == "2"  # in_progress
    assert "Blueprint: task-1" in created[0]["description"]


def test_track_change_approval(monkeypatch):
    def fake_open(request, timeout):
        return _Response({"result": {"sys_id": "cr-001", "state": "-2"}})

    monkeypatch.setenv("SERVICENOW_INSTANCE", "https://test.service-now.com")
    monkeypatch.setenv("SERVICENOW_USERNAME", "admin")
    monkeypatch.setenv("SERVICENOW_PASSWORD", "pwd")

    integration = ServiceNowIntegration(http_open=fake_open)
    status = integration.track_change_approval("cr-001")

    assert status == "authorized"


def test_track_change_approval_implementing(monkeypatch):
    def fake_open(request, timeout):
        return _Response({"result": {"sys_id": "cr-001", "state": "0"}})

    monkeypatch.setenv("SERVICENOW_INSTANCE", "https://test.service-now.com")
    monkeypatch.setenv("SERVICENOW_USERNAME", "admin")
    monkeypatch.setenv("SERVICENOW_PASSWORD", "pwd")

    integration = ServiceNowIntegration(http_open=fake_open)
    status = integration.track_change_approval("cr-001")

    assert status == "implementing"


def test_update_plan_status_from_change(monkeypatch):
    def fake_open(request, timeout):
        return _Response({"result": {"sys_id": "cr-001", "state": "-1"}})

    monkeypatch.setenv("SERVICENOW_INSTANCE", "https://test.service-now.com")
    monkeypatch.setenv("SERVICENOW_USERNAME", "admin")
    monkeypatch.setenv("SERVICENOW_PASSWORD", "pwd")

    integration = ServiceNowIntegration(http_open=fake_open)
    result = integration.update_plan_status_from_change("cr-001")

    assert result["change_state"] == "scheduled"
    assert result["plan_status"] == "queued"


def test_sync_task_completion(monkeypatch):
    seen = {}

    def fake_open(request, timeout):
        if request.method == "PATCH":
            seen["body"] = json.loads(request.data.decode("utf-8"))
        return _Response({"result": {"sys_id": "pt-001", "state": "3"}})

    monkeypatch.setenv("SERVICENOW_INSTANCE", "https://test.service-now.com")
    monkeypatch.setenv("SERVICENOW_USERNAME", "admin")
    monkeypatch.setenv("SERVICENOW_PASSWORD", "pwd")

    integration = ServiceNowIntegration(http_open=fake_open)
    task = {"id": "task-1", "status": "completed"}
    result = integration.sync_task_completion(task, "pt-001")

    assert seen["body"]["state"] == "3"  # Closed Complete
    assert seen["body"]["percent_complete"] == "100"


def test_bidirectional_sync(monkeypatch):
    call_count = {"n": 0}

    def fake_open(request, timeout):
        call_count["n"] += 1

        # First call: track_change_approval (GET change_request)
        if request.method == "GET" and "change_request" in request.full_url and call_count["n"] <= 1:
            return _Response({"result": {"sys_id": "cr-001", "state": "0"}})

        # Second call: query existing SN tasks
        if request.method == "GET" and "pm_project_task" in request.full_url:
            return _Response(_project_tasks_response())

        # Create/update calls
        if request.method in ("POST", "PATCH"):
            return _Response({"result": {"sys_id": f"pt-{call_count['n']}"}})

        return _Response({"result": {"sys_id": "cr-001", "state": "0"}})

    monkeypatch.setenv("SERVICENOW_INSTANCE", "https://test.service-now.com")
    monkeypatch.setenv("SERVICENOW_USERNAME", "admin")
    monkeypatch.setenv("SERVICENOW_PASSWORD", "pwd")

    integration = ServiceNowIntegration(http_open=fake_open)
    result = integration.bidirectional_sync(_sample_plan(), "cr-001")

    assert isinstance(result, SyncResult)
    assert result.change_request_id == "cr-001"
    assert result.plan_id == "plan-001"
    assert result.tasks_synced == 2
    assert result.direction == "bidirectional"
    assert result.errors == []


def test_generate_implementation_plan():
    integration = ServiceNowIntegration(http_open=lambda r, t: None)
    plan_text = integration.generate_implementation_plan(_sample_plan())

    assert "Implementation Plan: plan-001" in plan_text
    assert "Implement auth" in plan_text
    assert "Add dashboard" in plan_text
    assert "[completed]" in plan_text
    assert "[in_progress]" in plan_text
    assert "Sprint 1" in plan_text
    assert "depends on: task-1" in plan_text


def test_missing_instance_raises():
    integration = ServiceNowIntegration(http_open=lambda r, t: None)
    try:
        integration.create_change_request(_sample_plan())
        assert False, "Expected ValueError"
    except ValueError as exc:
        assert "instance" in str(exc).lower()


# ---------------------------------------------------------------------------
# State mapping tests
# ---------------------------------------------------------------------------


def test_change_states_cover_workflow():
    assert CHANGE_STATES["-5"] == "draft"
    assert CHANGE_STATES["-4"] == "requested"
    assert CHANGE_STATES["-2"] == "authorized"
    assert CHANGE_STATES["-1"] == "scheduled"
    assert CHANGE_STATES["0"] == "implementing"
    assert CHANGE_STATES["1"] == "review"
    assert CHANGE_STATES["2"] == "closed"


def test_change_state_to_plan_status():
    assert CHANGE_STATE_TO_PLAN_STATUS["draft"] == "draft"
    assert CHANGE_STATE_TO_PLAN_STATUS["authorized"] == "ready"
    assert CHANGE_STATE_TO_PLAN_STATUS["scheduled"] == "queued"
    assert CHANGE_STATE_TO_PLAN_STATUS["implementing"] == "in_progress"
    assert CHANGE_STATE_TO_PLAN_STATUS["closed"] == "completed"


def test_plan_status_to_change_state():
    assert PLAN_STATUS_TO_CHANGE_STATE["draft"] == "-5"
    assert PLAN_STATUS_TO_CHANGE_STATE["ready"] == "-2"
    assert PLAN_STATUS_TO_CHANGE_STATE["queued"] == "-1"
    assert PLAN_STATUS_TO_CHANGE_STATE["in_progress"] == "0"
    assert PLAN_STATUS_TO_CHANGE_STATE["completed"] == "2"


def test_task_status_to_sn_state():
    assert TASK_STATUS_TO_SN_STATE["pending"] == "1"
    assert TASK_STATUS_TO_SN_STATE["in_progress"] == "2"
    assert TASK_STATUS_TO_SN_STATE["completed"] == "3"
    assert TASK_STATUS_TO_SN_STATE["blocked"] == "-5"
    assert TASK_STATUS_TO_SN_STATE["skipped"] == "4"
