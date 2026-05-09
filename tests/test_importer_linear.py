"""Tests for the Linear GraphQL API importer."""

import json

from blueprint.importers.linear import (
    DEFAULT_STATE_MAP,
    DEFAULT_TYPE_MAP,
    LinearClient,
    LinearImporter,
    import_project_to_plan,
    map_issue_to_task,
    parse_issue_json,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


class _Response:
    """Mock HTTP response context manager."""

    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def read(self):
        return json.dumps(self.payload).encode("utf-8")


def _issue_payload(
    *,
    issue_id: str = "issue-001",
    identifier: str = "ENG-101",
    title: str = "Implement login page",
    description: str = "Build the login page with OAuth support.",
    priority: int = 3,
    priority_label: str = "Medium",
    estimate: float | None = 5.0,
    state_name: str = "In Progress",
    state_type: str = "started",
    assignee: str | None = "Alice Smith",
    creator: str | None = "Bob Jones",
    team_name: str | None = "Engineering",
    team_key: str | None = "ENG",
    project_name: str | None = "Login Feature",
    cycle_name: str | None = "Cycle 1",
    parent_id: str | None = None,
    parent_identifier: str | None = None,
    labels: list[dict] | None = None,
    children: list[dict] | None = None,
    relations: list[dict] | None = None,
    url: str | None = None,
) -> dict:
    """Build a realistic Linear issue payload."""
    issue: dict = {
        "id": issue_id,
        "identifier": identifier,
        "title": title,
        "description": description,
        "priority": priority,
        "priorityLabel": priority_label,
        "estimate": estimate,
        "url": url or f"https://linear.app/eng/issue/{identifier}",
        "createdAt": "2026-04-15T08:30:00.000Z",
        "updatedAt": "2026-05-01T10:00:00.000Z",
        "state": {
            "id": "state-001",
            "name": state_name,
            "type": state_type,
        },
    }

    if assignee:
        issue["assignee"] = {
            "id": "user-001",
            "name": assignee,
            "email": "alice@example.com",
        }
    else:
        issue["assignee"] = None

    if creator:
        issue["creator"] = {"id": "user-002", "name": creator}
    else:
        issue["creator"] = None

    if team_name:
        issue["team"] = {"id": "team-001", "name": team_name, "key": team_key}
    else:
        issue["team"] = None

    if project_name:
        issue["project"] = {"id": "proj-001", "name": project_name}
    else:
        issue["project"] = None

    if cycle_name:
        issue["cycle"] = {"id": "cycle-001", "name": cycle_name, "number": 1}
    else:
        issue["cycle"] = None

    if parent_id or parent_identifier:
        issue["parent"] = {
            "id": parent_id or "parent-001",
            "identifier": parent_identifier or "ENG-100",
            "title": "Parent Issue",
        }
    else:
        issue["parent"] = None

    issue["labels"] = {"nodes": labels or []}
    issue["children"] = {"nodes": children or []}
    issue["relations"] = {"nodes": relations or []}
    issue["attachments"] = {"nodes": []}

    return issue


def _graphql_response(data: dict) -> dict:
    """Wrap data in a standard GraphQL response."""
    return {"data": data}


def _issues_list_response(issues: list[dict]) -> dict:
    return _graphql_response({
        "issues": {"nodes": issues},
    })


def _viewer_response() -> dict:
    return _graphql_response({
        "viewer": {"id": "user-001", "name": "Alice Smith"},
    })


# ---------------------------------------------------------------------------
# parse_issue_json tests
# ---------------------------------------------------------------------------


def test_parse_issue_json_normalizes_source_brief():
    source_brief = parse_issue_json(_issue_payload())

    assert source_brief["title"] == "Implement login page"
    assert source_brief["domain"] == "linear"
    assert source_brief["source_project"] == "linear"
    assert source_brief["source_entity_type"] == "task"
    assert source_brief["source_id"] == "ENG-101"
    assert "Linear Issue ENG-101" in source_brief["summary"]
    assert "State: In Progress" in source_brief["summary"]
    assert "Assignee: Alice Smith" in source_brief["summary"]
    assert "Team: Engineering" in source_brief["summary"]
    assert "Project: Login Feature" in source_brief["summary"]

    normalized = source_brief["source_payload"]["normalized"]
    assert normalized["identifier"] == "ENG-101"
    assert normalized["state"] == "In Progress"
    assert normalized["state_type"] == "started"
    assert normalized["mapped_state"] == "in_progress"
    assert normalized["assignee"] == "Alice Smith"
    assert normalized["creator"] == "Bob Jones"
    assert normalized["team"] == "Engineering"
    assert normalized["team_key"] == "ENG"
    assert normalized["project"] == "Login Feature"
    assert normalized["cycle"] == "Cycle 1"
    assert normalized["estimate"] == 5.0
    assert normalized["priority"] == 3
    assert normalized["priority_label"] == "Medium"

    assert source_brief["source_links"]["html_url"] == (
        "https://linear.app/eng/issue/ENG-101"
    )


def test_parse_issue_json_with_labels():
    labels = [
        {"id": "l1", "name": "feature", "color": "#FF0000"},
        {"id": "l2", "name": "frontend", "color": "#00FF00"},
    ]
    sb = parse_issue_json(_issue_payload(labels=labels))
    assert sb["source_payload"]["normalized"]["labels"] == ["feature", "frontend"]
    assert "Labels: feature, frontend" in sb["summary"]


def test_parse_issue_json_epic_label_maps_to_phase():
    labels = [{"id": "l1", "name": "epic", "color": "#FF0000"}]
    sb = parse_issue_json(_issue_payload(labels=labels))
    assert sb["source_entity_type"] == "phase"


def test_parse_issue_json_custom_type_map():
    labels = [{"id": "l1", "name": "bugfix", "color": "#FF0000"}]
    sb = parse_issue_json(
        _issue_payload(labels=labels),
        type_map={"bugfix": "phase"},
    )
    assert sb["source_entity_type"] == "phase"


def test_parse_issue_json_custom_state_map():
    sb = parse_issue_json(
        _issue_payload(state_type="custom_state"),
        state_map={"custom_state": "blocked"},
    )
    assert sb["source_payload"]["normalized"]["mapped_state"] == "blocked"


def test_parse_issue_json_with_children():
    children = [
        {
            "id": "child-001",
            "identifier": "ENG-102",
            "title": "Subtask 1",
            "state": {"name": "Todo", "type": "unstarted"},
        },
        {
            "id": "child-002",
            "identifier": "ENG-103",
            "title": "Subtask 2",
            "state": {"name": "Done", "type": "completed"},
        },
    ]
    sb = parse_issue_json(_issue_payload(children=children))
    child_data = sb["source_payload"]["normalized"]["children"]
    assert len(child_data) == 2
    assert child_data[0]["identifier"] == "ENG-102"
    assert child_data[0]["state"] == "Todo"
    assert child_data[1]["identifier"] == "ENG-103"
    assert child_data[1]["state_type"] == "completed"


def test_parse_issue_json_with_relations():
    relations = [
        {
            "id": "rel-001",
            "type": "blocks",
            "relatedIssue": {
                "id": "issue-002",
                "identifier": "ENG-104",
                "title": "Related Issue",
            },
        },
        {
            "id": "rel-002",
            "type": "related",
            "relatedIssue": {
                "id": "issue-003",
                "identifier": "ENG-105",
                "title": "Another Issue",
            },
        },
    ]
    sb = parse_issue_json(_issue_payload(relations=relations))
    rel_data = sb["source_payload"]["normalized"]["relations"]
    assert len(rel_data) == 2
    assert rel_data[0]["type"] == "blocks"
    assert rel_data[0]["related_identifier"] == "ENG-104"
    assert rel_data[1]["type"] == "related"
    assert rel_data[1]["related_identifier"] == "ENG-105"


def test_parse_issue_json_with_parent():
    sb = parse_issue_json(
        _issue_payload(parent_identifier="ENG-50"),
    )
    assert sb["source_payload"]["normalized"]["parent"] == "ENG-50"


def test_parse_issue_json_missing_identifier_raises():
    payload = _issue_payload()
    del payload["identifier"]
    try:
        parse_issue_json(payload)
        assert False, "Expected ValueError"
    except ValueError as exc:
        assert "identifier" in str(exc)


def test_parse_issue_json_missing_title_raises():
    payload = _issue_payload()
    payload["title"] = ""
    try:
        parse_issue_json(payload)
        assert False, "Expected ValueError"
    except ValueError as exc:
        assert "title" in str(exc)


def test_parse_issue_json_empty_description():
    sb = parse_issue_json(_issue_payload(description=""))
    assert sb["source_payload"]["normalized"]["description"] == ""


def test_parse_issue_json_none_assignee():
    sb = parse_issue_json(_issue_payload(assignee=None))
    assert sb["source_payload"]["normalized"]["assignee"] is None
    assert "Assignee:" not in sb["summary"]


def test_parse_issue_json_none_project():
    sb = parse_issue_json(_issue_payload(project_name=None))
    assert sb["source_payload"]["normalized"]["project"] is None
    assert "Project:" not in sb["summary"]


def test_parse_issue_json_none_cycle():
    sb = parse_issue_json(_issue_payload(cycle_name=None))
    assert sb["source_payload"]["normalized"]["cycle"] is None
    assert "Cycle:" not in sb["summary"]


# ---------------------------------------------------------------------------
# State mapping tests
# ---------------------------------------------------------------------------


def test_backlog_state_maps_to_pending():
    sb = parse_issue_json(_issue_payload(state_type="backlog"))
    assert sb["source_payload"]["normalized"]["mapped_state"] == "pending"


def test_unstarted_state_maps_to_pending():
    sb = parse_issue_json(_issue_payload(state_type="unstarted"))
    assert sb["source_payload"]["normalized"]["mapped_state"] == "pending"


def test_started_state_maps_to_in_progress():
    sb = parse_issue_json(_issue_payload(state_type="started"))
    assert sb["source_payload"]["normalized"]["mapped_state"] == "in_progress"


def test_completed_state_maps_to_completed():
    sb = parse_issue_json(_issue_payload(state_type="completed"))
    assert sb["source_payload"]["normalized"]["mapped_state"] == "completed"


def test_cancelled_state_maps_to_skipped():
    sb = parse_issue_json(_issue_payload(state_type="cancelled"))
    assert sb["source_payload"]["normalized"]["mapped_state"] == "skipped"


def test_unknown_state_defaults_to_pending():
    sb = parse_issue_json(_issue_payload(state_type="unknown_state"))
    assert sb["source_payload"]["normalized"]["mapped_state"] == "pending"


def test_default_state_map_covers_standard_states():
    assert DEFAULT_STATE_MAP["backlog"] == "pending"
    assert DEFAULT_STATE_MAP["unstarted"] == "pending"
    assert DEFAULT_STATE_MAP["started"] == "in_progress"
    assert DEFAULT_STATE_MAP["completed"] == "completed"
    assert DEFAULT_STATE_MAP["cancelled"] == "skipped"


def test_default_type_map_covers_standard_types():
    assert DEFAULT_TYPE_MAP["feature"] == "task"
    assert DEFAULT_TYPE_MAP["bug"] == "task"
    assert DEFAULT_TYPE_MAP["epic"] == "phase"
    assert DEFAULT_TYPE_MAP["task"] == "task"


# ---------------------------------------------------------------------------
# map_issue_to_task tests
# ---------------------------------------------------------------------------


def test_map_issue_to_task_basic():
    task = map_issue_to_task(
        _issue_payload(),
        plan_id="plan-001",
    )

    assert task["id"] == "linear-ENG-101"
    assert task["execution_plan_id"] == "plan-001"
    assert task["title"] == "Implement login page"
    assert "OAuth" in task["description"]
    assert task["milestone"] == "Cycle 1"
    assert task["owner_type"] == "Alice Smith"
    assert task["status"] == "in_progress"
    assert task["estimated_hours"] == 20.0  # 5 * 4
    assert task["estimated_complexity"] == "medium"  # estimate = 5
    assert task["risk_level"] == "medium"  # priority = 3
    assert task["metadata"]["linear_identifier"] == "ENG-101"
    assert task["metadata"]["linear_state"] == "In Progress"
    assert task["metadata"]["linear_cycle"] == "Cycle 1"


def test_map_issue_to_task_dependencies():
    relations = [
        {
            "id": "rel-001",
            "type": "blocks",
            "relatedIssue": {
                "id": "issue-002",
                "identifier": "ENG-102",
                "title": "Blocked issue",
            },
        },
    ]
    task = map_issue_to_task(_issue_payload(relations=relations))
    assert "ENG-102" in task["depends_on"]


def test_map_issue_to_task_high_estimate():
    task = map_issue_to_task(_issue_payload(estimate=13))
    assert task["estimated_complexity"] == "high"
    assert task["estimated_hours"] == 52.0


def test_map_issue_to_task_low_estimate():
    task = map_issue_to_task(_issue_payload(estimate=1))
    assert task["estimated_complexity"] == "low"
    assert task["estimated_hours"] == 4.0


def test_map_issue_to_task_no_estimate():
    task = map_issue_to_task(_issue_payload(estimate=None))
    assert task["estimated_hours"] is None
    assert task["estimated_complexity"] == "medium"


def test_map_issue_to_task_urgent_priority():
    task = map_issue_to_task(_issue_payload(priority=1))
    assert task["risk_level"] == "high"


def test_map_issue_to_task_high_priority():
    task = map_issue_to_task(_issue_payload(priority=2))
    assert task["risk_level"] == "high"


def test_map_issue_to_task_low_priority():
    task = map_issue_to_task(_issue_payload(priority=4))
    assert task["risk_level"] == "low"


def test_map_issue_to_task_no_priority():
    task = map_issue_to_task(_issue_payload(priority=0))
    assert task["risk_level"] is None


# ---------------------------------------------------------------------------
# import_project_to_plan tests
# ---------------------------------------------------------------------------


def test_import_project_to_plan_basic():
    issues = [
        _issue_payload(identifier="ENG-101"),
        _issue_payload(identifier="ENG-102", title="Write tests"),
    ]

    plan = import_project_to_plan(
        issues,
        project_name="Login Feature",
    )

    assert plan["project_type"] == "linear"
    assert plan["metadata"]["project_name"] == "Login Feature"
    assert len(plan["tasks"]) == 2
    assert plan["tasks"][0]["id"] == "linear-ENG-101"
    assert plan["tasks"][1]["id"] == "linear-ENG-102"


def test_import_project_to_plan_with_epic_labels():
    issues = [
        _issue_payload(
            identifier="ENG-100",
            title="Auth Epic",
            labels=[{"id": "l1", "name": "epic", "color": "#FF0000"}],
        ),
        _issue_payload(identifier="ENG-101"),
        _issue_payload(identifier="ENG-102", title="Write tests"),
    ]

    plan = import_project_to_plan(
        issues,
        project_name="Login Feature",
    )

    assert len(plan["milestones"]) == 1
    assert plan["milestones"][0]["name"] == "Auth Epic"
    assert len(plan["tasks"]) == 2


def test_import_project_to_plan_derives_milestones_from_cycles():
    issues = [
        _issue_payload(identifier="ENG-101", cycle_name="Cycle 1"),
        _issue_payload(identifier="ENG-102", cycle_name="Cycle 2"),
    ]

    plan = import_project_to_plan(
        issues,
        project_name="Project",
    )

    assert len(plan["milestones"]) == 2
    names = {m["name"] for m in plan["milestones"]}
    assert "Cycle 1" in names
    assert "Cycle 2" in names


def test_import_project_to_plan_empty():
    plan = import_project_to_plan(
        [],
        project_name="Empty Project",
    )

    assert plan["tasks"] == []
    assert plan["milestones"] == []
    assert plan["status"] == "draft"


# ---------------------------------------------------------------------------
# LinearClient tests
# ---------------------------------------------------------------------------


def test_client_execute_sends_graphql():
    seen = {}

    def fake_open(request, timeout):
        seen["url"] = request.full_url
        seen["method"] = request.method
        seen["body"] = json.loads(request.data.decode("utf-8"))
        seen["auth"] = request.headers.get("Authorization")
        return _Response(_graphql_response({"viewer": {"id": "1"}}))

    client = LinearClient(
        api_key="lin_api_test123",
        http_open=fake_open,
    )

    result = client.execute("query { viewer { id } }")

    assert seen["url"] == "https://api.linear.app/graphql"
    assert seen["method"] == "POST"
    assert "query" in seen["body"]
    assert seen["auth"] == "lin_api_test123"
    assert result == {"viewer": {"id": "1"}}


def test_client_execute_with_variables():
    seen = {}

    def fake_open(request, timeout):
        seen["body"] = json.loads(request.data.decode("utf-8"))
        return _Response(_graphql_response({"issue": _issue_payload()}))

    client = LinearClient(api_key="key", http_open=fake_open)
    client.execute("query($id: String!) { issue(id: $id) { id } }", {"id": "123"})

    assert seen["body"]["variables"] == {"id": "123"}


def test_client_execute_graphql_error():
    def fake_open(request, timeout):
        return _Response({
            "errors": [{"message": "Issue not found"}],
            "data": None,
        })

    client = LinearClient(api_key="key", http_open=fake_open)
    try:
        client.execute("query { issue(id: \"bad\") { id } }")
        assert False, "Expected ImportError"
    except ImportError as exc:
        assert "Issue not found" in str(exc)


def test_client_http_error():
    from urllib.error import HTTPError

    def fake_open(request, timeout):
        raise HTTPError(request.full_url, 401, "Unauthorized", {}, None)

    client = LinearClient(api_key="bad-key", http_open=fake_open)
    try:
        client.execute("query { viewer { id } }")
        assert False, "Expected ImportError"
    except ImportError as exc:
        assert "401" in str(exc)


# ---------------------------------------------------------------------------
# LinearImporter tests
# ---------------------------------------------------------------------------


def test_importer_import_from_source(monkeypatch):
    seen = {}

    def fake_open(request, timeout):
        body = json.loads(request.data.decode("utf-8"))
        seen["variables"] = body.get("variables")
        return _Response(_graphql_response({"issue": _issue_payload()}))

    monkeypatch.setenv("LINEAR_API_KEY", "lin_api_test123")

    importer = LinearImporter(http_open=fake_open)
    sb = importer.import_from_source("issue-001")

    assert seen["variables"]["id"] == "issue-001"
    assert sb["source_id"] == "ENG-101"
    assert sb["title"] == "Implement login page"


def test_importer_import_from_source_not_found(monkeypatch):
    def fake_open(request, timeout):
        return _Response(_graphql_response({"issue": None}))

    monkeypatch.setenv("LINEAR_API_KEY", "lin_api_test123")

    importer = LinearImporter(http_open=fake_open)
    try:
        importer.import_from_source("bad-id")
        assert False, "Expected ImportError"
    except ImportError as exc:
        assert "not found" in str(exc)


def test_importer_validate_source_returns_true(monkeypatch):
    def fake_open(request, timeout):
        return _Response(_graphql_response({"issue": _issue_payload()}))

    monkeypatch.setenv("LINEAR_API_KEY", "lin_api_test123")

    importer = LinearImporter(http_open=fake_open)
    assert importer.validate_source("issue-001") is True


def test_importer_validate_source_returns_false_when_not_found(monkeypatch):
    def fake_open(request, timeout):
        return _Response(_graphql_response({"issue": None}))

    monkeypatch.setenv("LINEAR_API_KEY", "lin_api_test123")

    importer = LinearImporter(http_open=fake_open)
    assert importer.validate_source("bad-id") is False


def test_importer_validate_source_returns_false_on_error(monkeypatch):
    from urllib.error import HTTPError

    def fake_open(request, timeout):
        raise HTTPError(request.full_url, 401, "Unauthorized", {}, None)

    monkeypatch.setenv("LINEAR_API_KEY", "bad-key")

    importer = LinearImporter(http_open=fake_open)
    assert importer.validate_source("issue-001") is False


def test_importer_list_available_by_project(monkeypatch):
    def fake_open(request, timeout):
        return _Response(_issues_list_response([
            _issue_payload(identifier="ENG-101"),
            _issue_payload(identifier="ENG-102", title="Second issue"),
        ]))

    monkeypatch.setenv("LINEAR_API_KEY", "lin_api_test123")

    importer = LinearImporter(
        project_id="proj-001",
        http_open=fake_open,
    )

    items = importer.list_available(limit=10)
    assert len(items) == 2
    assert items[0]["identifier"] == "ENG-101"
    assert items[0]["title"] == "Implement login page"
    assert items[0]["status"] == "In Progress"
    assert items[0]["assignee"] == "Alice Smith"
    assert items[1]["identifier"] == "ENG-102"


def test_importer_list_available_by_team(monkeypatch):
    def fake_open(request, timeout):
        return _Response(_issues_list_response([_issue_payload()]))

    monkeypatch.setenv("LINEAR_API_KEY", "lin_api_test123")

    importer = LinearImporter(
        team_id="team-001",
        http_open=fake_open,
    )

    items = importer.list_available()
    assert len(items) == 1


def test_importer_list_available_requires_team_or_project():
    importer = LinearImporter(http_open=lambda r, t: None)
    try:
        importer.list_available()
        assert False, "Expected ValueError"
    except ValueError as exc:
        assert "team_id or project_id" in str(exc)


def test_importer_authenticate(monkeypatch):
    def fake_open(request, timeout):
        return _Response(_viewer_response())

    importer = LinearImporter(http_open=fake_open)
    client = importer.authenticate("lin_api_test123")

    assert isinstance(client, LinearClient)
    assert client.api_key == "lin_api_test123"


def test_importer_fetch_by_project(monkeypatch):
    def fake_open(request, timeout):
        return _Response(_issues_list_response([
            _issue_payload(identifier="ENG-101"),
            _issue_payload(identifier="ENG-102"),
        ]))

    monkeypatch.setenv("LINEAR_API_KEY", "lin_api_test123")

    importer = LinearImporter(http_open=fake_open)
    briefs = importer.fetch_by_project("proj-001")

    assert len(briefs) == 2
    assert briefs[0]["source_id"] == "ENG-101"
    assert briefs[1]["source_id"] == "ENG-102"


def test_importer_fetch_by_team(monkeypatch):
    def fake_open(request, timeout):
        return _Response(_issues_list_response([_issue_payload()]))

    monkeypatch.setenv("LINEAR_API_KEY", "lin_api_test123")

    importer = LinearImporter(http_open=fake_open)
    briefs = importer.fetch_by_team("team-001")

    assert len(briefs) == 1
    assert briefs[0]["source_id"] == "ENG-101"


def test_importer_fetch_by_filter(monkeypatch):
    def fake_open(request, timeout):
        body = json.loads(request.data.decode("utf-8"))
        assert "filter" in body["variables"]
        return _Response(_issues_list_response([_issue_payload()]))

    monkeypatch.setenv("LINEAR_API_KEY", "lin_api_test123")

    importer = LinearImporter(http_open=fake_open)
    gql_filter = {"state": {"type": {"eq": "started"}}}
    briefs = importer.fetch_by_filter(gql_filter)

    assert len(briefs) == 1


def test_importer_import_project(monkeypatch):
    def fake_open(request, timeout):
        return _Response(_issues_list_response([
            _issue_payload(identifier="ENG-101"),
            _issue_payload(identifier="ENG-102", title="Write tests"),
        ]))

    monkeypatch.setenv("LINEAR_API_KEY", "lin_api_test123")

    importer = LinearImporter(
        project_id="proj-001",
        http_open=fake_open,
    )

    plan = importer.import_project(project_name="My Project")

    assert plan["project_type"] == "linear"
    assert plan["metadata"]["project_name"] == "My Project"
    assert len(plan["tasks"]) == 2


def test_importer_import_project_requires_project_id():
    importer = LinearImporter(http_open=lambda r, t: None)
    try:
        importer.import_project()
        assert False, "Expected ValueError"
    except ValueError as exc:
        assert "project_id" in str(exc)


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_issue_without_labels():
    sb = parse_issue_json(_issue_payload(labels=[]))
    assert sb["source_payload"]["normalized"]["labels"] == []


def test_issue_without_team():
    sb = parse_issue_json(_issue_payload(team_name=None))
    assert sb["source_payload"]["normalized"]["team"] is None
    assert "Team:" not in sb["summary"]


def test_issue_without_parent():
    sb = parse_issue_json(_issue_payload())
    assert sb["source_payload"]["normalized"]["parent"] is None


def test_issue_with_no_children_or_relations():
    sb = parse_issue_json(_issue_payload())
    assert sb["source_payload"]["normalized"]["children"] == []
    assert sb["source_payload"]["normalized"]["relations"] == []


def test_issue_archived_state():
    """Archived issues with unknown state type default to pending."""
    sb = parse_issue_json(_issue_payload(state_type="archived"))
    assert sb["source_payload"]["normalized"]["mapped_state"] == "pending"


def test_issue_cancelled_vs_canceled_spelling():
    sb1 = parse_issue_json(_issue_payload(state_type="cancelled"))
    sb2 = parse_issue_json(_issue_payload(state_type="canceled"))
    assert sb1["source_payload"]["normalized"]["mapped_state"] == "skipped"
    assert sb2["source_payload"]["normalized"]["mapped_state"] == "skipped"


def test_issue_complex_label_hierarchy():
    labels = [
        {"id": "l1", "name": "team:platform", "color": "#FF0000"},
        {"id": "l2", "name": "priority:high", "color": "#00FF00"},
        {"id": "l3", "name": "feature", "color": "#0000FF"},
    ]
    sb = parse_issue_json(_issue_payload(labels=labels))
    assert sb["source_payload"]["normalized"]["labels"] == [
        "team:platform", "priority:high", "feature"
    ]
    assert sb["source_entity_type"] == "task"
