"""Tests for the Jira Cloud REST API importer."""

import json

from blueprint.importers.jira import (
    DEFAULT_LINK_TYPE_MAP,
    DEFAULT_STATE_MAP,
    DEFAULT_TYPE_MAP,
    JiraClient,
    JiraImporter,
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


_SENTINEL = object()


def _issue_payload(
    *,
    key: str = "PROJ-101",
    summary: str = "Implement login page",
    issue_type: str = "Story",
    status: str = "In Progress",
    priority: str = "Medium",
    description: dict | str | None | object = _SENTINEL,
    labels: list[str] | None = None,
    components: list[dict] | None = None,
    assignee: str | None = "Alice Smith",
    reporter: str | None = "Bob Jones",
    sprint: dict | None = None,
    epic_link: str | None = None,
    story_points: float | None = 5.0,
    acceptance_criteria: str | None = None,
    issue_links: list[dict] | None = None,
    attachments: list[dict] | None = None,
    project_key: str = "PROJ",
    resolution: str | None = None,
    parent: dict | None = None,
) -> dict:
    """Build a realistic Jira Cloud issue payload."""
    if description is _SENTINEL:
        description = {
            "type": "doc",
            "version": 1,
            "content": [
                {
                    "type": "paragraph",
                    "content": [
                        {"type": "text", "text": "Build the login page with OAuth support."}
                    ],
                }
            ],
        }

    effective_labels = labels if labels is not None else ["frontend", "auth"]
    effective_components = components if components is not None else [{"name": "web-app"}, {"name": "auth"}]

    fields: dict = {
        "summary": summary,
        "issuetype": {"id": "10001", "name": issue_type},
        "status": {"id": "3", "name": status},
        "priority": {"id": "3", "name": priority},
        "description": description,
        "labels": effective_labels,
        "components": effective_components,
        "project": {"key": project_key, "name": "My Project"},
        "updated": "2026-05-01T10:00:00.000+0000",
        "created": "2026-04-15T08:30:00.000+0000",
    }

    if assignee:
        fields["assignee"] = {
            "accountId": "abc123",
            "displayName": assignee,
            "emailAddress": "alice@example.com",
        }
    else:
        fields["assignee"] = None

    if reporter:
        fields["reporter"] = {
            "accountId": "def456",
            "displayName": reporter,
            "emailAddress": "bob@example.com",
        }
    else:
        fields["reporter"] = None

    if sprint is not None:
        fields["customfield_10020"] = [sprint]
    else:
        fields["customfield_10020"] = [
            {"id": 1, "name": "Sprint 1", "state": "active"}
        ]

    if epic_link:
        fields["customfield_10014"] = epic_link

    if story_points is not None:
        fields["customfield_10028"] = story_points

    if acceptance_criteria:
        fields["customfield_10035"] = acceptance_criteria

    if issue_links is not None:
        fields["issuelinks"] = issue_links

    if attachments is not None:
        fields["attachment"] = attachments

    if resolution:
        fields["resolution"] = {"name": resolution}

    if parent:
        fields["parent"] = parent

    payload = {
        "id": "10001",
        "key": key,
        "self": f"https://mysite.atlassian.net/rest/api/3/issue/{key}",
        "fields": fields,
    }

    return payload


def _epic_payload(*, key: str = "PROJ-200", summary: str = "Authentication Epic") -> dict:
    return _issue_payload(
        key=key,
        summary=summary,
        issue_type="Epic",
        status="In Progress",
        description={
            "type": "doc",
            "version": 1,
            "content": [
                {
                    "type": "paragraph",
                    "content": [{"type": "text", "text": "Epic for all auth work."}],
                }
            ],
        },
        labels=["epic", "auth"],
        components=[],
        story_points=None,
        priority="High",
    )


def _search_response(issues: list[dict]) -> dict:
    return {
        "startAt": 0,
        "maxResults": 50,
        "total": len(issues),
        "issues": issues,
    }


def _server_info_response() -> dict:
    return {
        "baseUrl": "https://mysite.atlassian.net",
        "version": "1001.0.0",
        "versionNumbers": [1001, 0, 0],
        "deploymentType": "Cloud",
        "buildNumber": 100000,
        "serverTitle": "My Jira Site",
    }


# ---------------------------------------------------------------------------
# parse_issue_json tests
# ---------------------------------------------------------------------------


def test_parse_issue_json_normalizes_source_brief():
    source_brief = parse_issue_json(
        _issue_payload(),
        instance_url="https://mysite.atlassian.net",
    )

    assert source_brief["title"] == "Implement login page"
    assert source_brief["domain"] == "jira"
    assert source_brief["source_project"] == "jira"
    assert source_brief["source_entity_type"] == "task"
    assert source_brief["source_id"] == "PROJ-101"
    assert "Jira Story PROJ-101" in source_brief["summary"]
    assert "Status: In Progress" in source_brief["summary"]
    assert "Assignee: Alice Smith" in source_brief["summary"]
    assert "Labels: frontend, auth" in source_brief["summary"]
    assert "Components: web-app, auth" in source_brief["summary"]
    assert "Sprint: Sprint 1" in source_brief["summary"]

    normalized = source_brief["source_payload"]["normalized"]
    assert normalized["key"] == "PROJ-101"
    assert normalized["issue_type"] == "Story"
    assert normalized["status"] == "In Progress"
    assert normalized["mapped_state"] == "in_progress"
    assert normalized["assignee"] == "Alice Smith"
    assert normalized["reporter"] == "Bob Jones"
    assert normalized["labels"] == ["frontend", "auth"]
    assert normalized["components"] == ["web-app", "auth"]
    assert normalized["sprint"] == "Sprint 1"
    assert normalized["story_points"] == 5.0
    assert normalized["project_key"] == "PROJ"

    assert source_brief["source_links"]["html_url"] == (
        "https://mysite.atlassian.net/browse/PROJ-101"
    )


def test_parse_issue_json_epic_maps_to_phase():
    source_brief = parse_issue_json(
        _epic_payload(),
        instance_url="https://mysite.atlassian.net",
    )
    assert source_brief["source_entity_type"] == "phase"


def test_parse_issue_json_custom_type_map():
    source_brief = parse_issue_json(
        _issue_payload(issue_type="Bug"),
        instance_url="https://mysite.atlassian.net",
        type_map={"Bug": "phase"},
    )
    assert source_brief["source_entity_type"] == "phase"


def test_parse_issue_json_custom_state_map():
    source_brief = parse_issue_json(
        _issue_payload(status="Code Review"),
        instance_url="https://mysite.atlassian.net",
        state_map={"Code Review": "blocked"},
    )
    assert source_brief["source_payload"]["normalized"]["mapped_state"] == "blocked"


def test_parse_issue_json_with_issue_links():
    links = [
        {
            "id": "1",
            "type": {
                "id": "10000",
                "name": "Blocks",
                "inward": "is blocked by",
                "outward": "blocks",
            },
            "outwardIssue": {
                "key": "PROJ-102",
                "fields": {
                    "summary": "Setup CI",
                    "status": {"name": "To Do"},
                },
            },
        },
        {
            "id": "2",
            "type": {
                "id": "10003",
                "name": "Relates",
                "inward": "relates to",
                "outward": "relates to",
            },
            "inwardIssue": {
                "key": "PROJ-103",
                "fields": {
                    "summary": "Design mockups",
                    "status": {"name": "Done"},
                },
            },
        },
    ]
    source_brief = parse_issue_json(
        _issue_payload(issue_links=links),
        instance_url="https://mysite.atlassian.net",
    )
    parsed_links = source_brief["source_payload"]["normalized"]["links"]
    assert len(parsed_links) == 2
    assert parsed_links[0]["direction"] == "outward"
    assert parsed_links[0]["type"] == "blocks"
    assert parsed_links[0]["target_key"] == "PROJ-102"
    assert parsed_links[0]["target_summary"] == "Setup CI"
    assert parsed_links[1]["direction"] == "inward"
    assert parsed_links[1]["target_key"] == "PROJ-103"


def test_parse_issue_json_with_attachments():
    attachments = [
        {
            "id": "att-1",
            "filename": "screenshot.png",
            "mimeType": "image/png",
            "size": 12345,
            "content": "https://mysite.atlassian.net/secure/attachment/att-1/screenshot.png",
            "created": "2026-04-20T12:00:00.000+0000",
            "author": {"displayName": "Alice Smith"},
        },
    ]
    source_brief = parse_issue_json(
        _issue_payload(attachments=attachments),
        instance_url="https://mysite.atlassian.net",
    )
    parsed_att = source_brief["source_payload"]["normalized"]["attachments"]
    assert len(parsed_att) == 1
    assert parsed_att[0]["filename"] == "screenshot.png"
    assert parsed_att[0]["mime_type"] == "image/png"
    assert parsed_att[0]["size"] == 12345
    assert parsed_att[0]["author"] == "Alice Smith"


def test_parse_issue_json_missing_key_raises():
    payload = _issue_payload()
    del payload["key"]
    try:
        parse_issue_json(payload, instance_url="https://x.atlassian.net")
        assert False, "Expected ValueError"
    except ValueError as exc:
        assert "key" in str(exc)


def test_parse_issue_json_missing_summary_raises():
    payload = _issue_payload()
    payload["fields"]["summary"] = ""
    try:
        parse_issue_json(payload, instance_url="https://x.atlassian.net")
        assert False, "Expected ValueError"
    except ValueError as exc:
        assert "summary" in str(exc)


def test_parse_issue_json_adf_description_extraction():
    adf = {
        "type": "doc",
        "version": 1,
        "content": [
            {
                "type": "paragraph",
                "content": [
                    {"type": "text", "text": "First paragraph."},
                ],
            },
            {
                "type": "paragraph",
                "content": [
                    {"type": "text", "text": "Second paragraph."},
                ],
            },
        ],
    }
    source_brief = parse_issue_json(
        _issue_payload(description=adf),
        instance_url="https://mysite.atlassian.net",
    )
    desc = source_brief["source_payload"]["normalized"]["description"]
    assert "First paragraph." in desc
    assert "Second paragraph." in desc


def test_parse_issue_json_string_description():
    source_brief = parse_issue_json(
        _issue_payload(description="Plain text description"),
        instance_url="https://mysite.atlassian.net",
    )
    desc = source_brief["source_payload"]["normalized"]["description"]
    assert desc == "Plain text description"


def test_parse_issue_json_none_description():
    source_brief = parse_issue_json(
        _issue_payload(description=None),
        instance_url="https://mysite.atlassian.net",
    )
    desc = source_brief["source_payload"]["normalized"]["description"]
    assert desc == ""


def test_parse_issue_json_with_epic_link():
    source_brief = parse_issue_json(
        _issue_payload(epic_link="PROJ-100"),
        instance_url="https://mysite.atlassian.net",
    )
    assert source_brief["source_payload"]["normalized"]["epic_link"] == "PROJ-100"


def test_parse_issue_json_with_parent_epic():
    parent = {
        "key": "PROJ-50",
        "fields": {
            "issuetype": {"name": "Epic"},
            "summary": "Parent Epic",
        },
    }
    source_brief = parse_issue_json(
        _issue_payload(parent=parent),
        instance_url="https://mysite.atlassian.net",
    )
    assert source_brief["source_payload"]["normalized"]["epic_link"] == "PROJ-50"


def test_parse_issue_json_with_resolution():
    source_brief = parse_issue_json(
        _issue_payload(resolution="Fixed"),
        instance_url="https://mysite.atlassian.net",
    )
    assert source_brief["source_payload"]["normalized"]["resolution"] == "Fixed"


def test_parse_issue_json_custom_field_mapping():
    payload = _issue_payload()
    payload["fields"]["customfield_99999"] = "custom value"
    source_brief = parse_issue_json(
        payload,
        instance_url="https://mysite.atlassian.net",
        custom_field_map={"my_custom": "customfield_99999"},
    )
    custom = source_brief["source_payload"]["normalized"]["custom_fields"]
    assert custom["my_custom"] == "custom value"


def test_parse_issue_json_acceptance_criteria_from_custom_map():
    payload = _issue_payload()
    payload["fields"]["customfield_11111"] = "AC: Login validates email format"
    source_brief = parse_issue_json(
        payload,
        instance_url="https://mysite.atlassian.net",
        custom_field_map={"acceptance_criteria": "customfield_11111"},
    )
    ac = source_brief["source_payload"]["normalized"]["acceptance_criteria"]
    assert ac == "AC: Login validates email format"


# ---------------------------------------------------------------------------
# map_issue_to_task tests
# ---------------------------------------------------------------------------


def test_map_issue_to_task_basic():
    task = map_issue_to_task(
        _issue_payload(),
        instance_url="https://mysite.atlassian.net",
        plan_id="plan-001",
    )

    assert task["id"] == "jira-PROJ-101"
    assert task["execution_plan_id"] == "plan-001"
    assert task["title"] == "Implement login page"
    assert "OAuth" in task["description"]
    assert task["milestone"] == "Sprint 1"
    assert task["owner_type"] == "Alice Smith"
    assert task["status"] == "in_progress"
    assert task["estimated_hours"] == 20.0  # 5 story points * 4
    assert task["estimated_complexity"] == "medium"  # 5 story points
    assert task["risk_level"] == "medium"  # Medium priority
    assert task["metadata"]["jira_key"] == "PROJ-101"
    assert task["metadata"]["jira_issue_type"] == "Story"
    assert task["metadata"]["jira_sprint"] == "Sprint 1"
    assert task["metadata"]["labels"] == ["frontend", "auth"]


def test_map_issue_to_task_dependencies():
    links = [
        {
            "id": "1",
            "type": {
                "inward": "is blocked by",
                "outward": "blocks",
            },
            "inwardIssue": {
                "key": "PROJ-50",
                "fields": {"summary": "Blocker", "status": {"name": "Open"}},
            },
        },
    ]
    task = map_issue_to_task(
        _issue_payload(issue_links=links),
        instance_url="https://mysite.atlassian.net",
    )
    assert "PROJ-50" in task["depends_on"]


def test_map_issue_to_task_high_story_points():
    task = map_issue_to_task(
        _issue_payload(story_points=13),
        instance_url="https://mysite.atlassian.net",
    )
    assert task["estimated_complexity"] == "high"
    assert task["estimated_hours"] == 52.0  # 13 * 4


def test_map_issue_to_task_low_story_points():
    task = map_issue_to_task(
        _issue_payload(story_points=1),
        instance_url="https://mysite.atlassian.net",
    )
    assert task["estimated_complexity"] == "low"
    assert task["estimated_hours"] == 4.0


def test_map_issue_to_task_no_story_points():
    task = map_issue_to_task(
        _issue_payload(story_points=None),
        instance_url="https://mysite.atlassian.net",
    )
    assert task["estimated_hours"] is None
    assert task["estimated_complexity"] == "medium"


def test_map_issue_to_task_priority_highest_is_high_risk():
    task = map_issue_to_task(
        _issue_payload(priority="Highest"),
        instance_url="https://mysite.atlassian.net",
    )
    assert task["risk_level"] == "high"


def test_map_issue_to_task_priority_low_is_low_risk():
    task = map_issue_to_task(
        _issue_payload(priority="Low"),
        instance_url="https://mysite.atlassian.net",
    )
    assert task["risk_level"] == "low"


def test_map_issue_to_task_acceptance_criteria_from_field():
    task = map_issue_to_task(
        _issue_payload(acceptance_criteria="User can log in with Google"),
        instance_url="https://mysite.atlassian.net",
    )
    assert task["acceptance_criteria"] == ["User can log in with Google"]


def test_map_issue_to_task_default_acceptance_criteria():
    task = map_issue_to_task(
        _issue_payload(acceptance_criteria=None),
        instance_url="https://mysite.atlassian.net",
    )
    assert task["acceptance_criteria"] == ["Complete: Implement login page"]


# ---------------------------------------------------------------------------
# import_project_to_plan tests
# ---------------------------------------------------------------------------


def test_import_project_to_plan_separates_epics_and_tasks():
    issues = [
        _epic_payload(key="PROJ-200", summary="Auth Epic"),
        _issue_payload(key="PROJ-101", issue_type="Story"),
        _issue_payload(key="PROJ-102", issue_type="Task", summary="Write tests"),
    ]

    plan = import_project_to_plan(
        issues,
        instance_url="https://mysite.atlassian.net",
        project_key="PROJ",
    )

    assert plan["project_type"] == "jira"
    assert plan["metadata"]["project_key"] == "PROJ"

    # Epic should become a milestone
    assert len(plan["milestones"]) == 1
    assert plan["milestones"][0]["name"] == "Auth Epic"
    assert plan["milestones"][0]["jira_issue_type"] == "Epic"

    # Story and Task should become tasks
    assert len(plan["tasks"]) == 2
    assert plan["tasks"][0]["id"] == "jira-PROJ-101"
    assert plan["tasks"][1]["id"] == "jira-PROJ-102"


def test_import_project_to_plan_derives_milestones_from_sprints():
    issues = [
        _issue_payload(
            key="PROJ-101",
            sprint={"id": 1, "name": "Sprint 1", "state": "active"},
        ),
        _issue_payload(
            key="PROJ-102",
            sprint={"id": 2, "name": "Sprint 2", "state": "future"},
        ),
    ]

    plan = import_project_to_plan(
        issues,
        instance_url="https://mysite.atlassian.net",
        project_key="PROJ",
    )

    # No epics, so milestones should come from unique sprints
    assert len(plan["milestones"]) == 2
    names = {m["name"] for m in plan["milestones"]}
    assert "Sprint 1" in names
    assert "Sprint 2" in names


def test_import_project_to_plan_empty_issues():
    plan = import_project_to_plan(
        [],
        instance_url="https://mysite.atlassian.net",
        project_key="PROJ",
    )

    assert plan["tasks"] == []
    assert plan["milestones"] == []
    assert plan["status"] == "draft"


# ---------------------------------------------------------------------------
# State and type mapping tests
# ---------------------------------------------------------------------------


def test_default_state_map_covers_standard_statuses():
    assert DEFAULT_STATE_MAP["To Do"] == "pending"
    assert DEFAULT_STATE_MAP["Open"] == "pending"
    assert DEFAULT_STATE_MAP["In Progress"] == "in_progress"
    assert DEFAULT_STATE_MAP["Done"] == "completed"
    assert DEFAULT_STATE_MAP["Closed"] == "completed"
    assert DEFAULT_STATE_MAP["Resolved"] == "completed"


def test_default_type_map_covers_standard_types():
    assert DEFAULT_TYPE_MAP["Epic"] == "phase"
    assert DEFAULT_TYPE_MAP["Story"] == "task"
    assert DEFAULT_TYPE_MAP["Task"] == "task"
    assert DEFAULT_TYPE_MAP["Bug"] == "task"
    assert DEFAULT_TYPE_MAP["Sub-task"] == "task"


def test_link_type_map_covers_common_link_types():
    assert DEFAULT_LINK_TYPE_MAP["Blocks"] == "blocks"
    assert DEFAULT_LINK_TYPE_MAP["is blocked by"] == "blocked_by"
    assert DEFAULT_LINK_TYPE_MAP["Relates"] == "relates_to"
    assert DEFAULT_LINK_TYPE_MAP["Duplicate"] == "duplicates"


# ---------------------------------------------------------------------------
# JiraClient tests
# ---------------------------------------------------------------------------


def test_client_request_json_builds_correct_url():
    seen = {}

    def fake_open(request, timeout):
        seen["url"] = request.full_url
        seen["auth"] = request.headers.get("Authorization")
        seen["timeout"] = timeout
        return _Response({"values": []})

    client = JiraClient(
        base_url="https://mysite.atlassian.net",
        email="user@example.com",
        api_token="test-token",
        http_open=fake_open,
    )

    client.request_json("serverInfo")

    assert "mysite.atlassian.net/rest/api/3/serverInfo" in seen["url"]
    assert seen["auth"].startswith("Basic ")
    assert seen["timeout"] == 30


def test_client_search_issues():
    seen = {}

    def fake_open(request, timeout):
        seen["url"] = request.full_url
        return _Response(_search_response([_issue_payload()]))

    client = JiraClient(
        base_url="https://mysite.atlassian.net",
        email="user@example.com",
        api_token="test-token",
        http_open=fake_open,
    )

    result = client.search_issues("project = PROJ", max_results=10)

    assert "search?" in seen["url"]
    assert "jql=" in seen["url"]
    assert "maxResults=10" in seen["url"]
    assert len(result["issues"]) == 1


def test_client_http_error_raises_import_error():
    from urllib.error import HTTPError

    def fake_open(request, timeout):
        raise HTTPError(request.full_url, 401, "Unauthorized", {}, None)

    client = JiraClient(
        base_url="https://mysite.atlassian.net",
        email="user@example.com",
        api_token="bad-token",
        http_open=fake_open,
    )

    try:
        client.request_json("serverInfo")
        assert False, "Expected ImportError"
    except ImportError as exc:
        assert "401" in str(exc)


# ---------------------------------------------------------------------------
# JiraImporter tests
# ---------------------------------------------------------------------------


def test_importer_import_from_source(monkeypatch):
    seen = {}

    def fake_open(request, timeout):
        seen["url"] = request.full_url
        return _Response(_issue_payload())

    monkeypatch.setenv("JIRA_EMAIL", "user@example.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "test-token")

    importer = JiraImporter(
        instance_url="https://mysite.atlassian.net",
        http_open=fake_open,
    )

    source_brief = importer.import_from_source("PROJ-101")

    assert "issue/PROJ-101" in seen["url"]
    assert source_brief["source_id"] == "PROJ-101"
    assert source_brief["title"] == "Implement login page"


def test_importer_validate_source_returns_true(monkeypatch):
    def fake_open(request, timeout):
        return _Response(_issue_payload())

    monkeypatch.setenv("JIRA_EMAIL", "user@example.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "test-token")

    importer = JiraImporter(
        instance_url="https://mysite.atlassian.net",
        http_open=fake_open,
    )

    assert importer.validate_source("PROJ-101") is True


def test_importer_validate_source_returns_false_on_error(monkeypatch):
    from urllib.error import HTTPError

    def fake_open(request, timeout):
        raise HTTPError(request.full_url, 404, "Not Found", {}, None)

    monkeypatch.setenv("JIRA_EMAIL", "user@example.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "test-token")

    importer = JiraImporter(
        instance_url="https://mysite.atlassian.net",
        http_open=fake_open,
    )

    assert importer.validate_source("PROJ-999") is False


def test_importer_list_available(monkeypatch):
    def fake_open(request, timeout):
        return _Response(
            _search_response([
                _issue_payload(key="PROJ-101"),
                _issue_payload(key="PROJ-102", summary="Second issue"),
            ])
        )

    monkeypatch.setenv("JIRA_EMAIL", "user@example.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "test-token")

    importer = JiraImporter(
        instance_url="https://mysite.atlassian.net",
        project_key="PROJ",
        http_open=fake_open,
    )

    items = importer.list_available(limit=10)
    assert len(items) == 2
    assert items[0]["id"] == "PROJ-101"
    assert items[0]["title"] == "Implement login page"
    assert items[0]["status"] == "In Progress"
    assert items[0]["type"] == "Story"
    assert items[0]["assignee"] == "Alice Smith"
    assert "atlassian.net" in items[0]["html_url"]
    assert items[1]["id"] == "PROJ-102"


def test_importer_list_available_requires_project_key():
    importer = JiraImporter(
        instance_url="https://mysite.atlassian.net",
        http_open=lambda r, t: None,
    )
    try:
        importer.list_available()
        assert False, "Expected ValueError"
    except ValueError as exc:
        assert "project_key" in str(exc)


def test_importer_authenticate(monkeypatch):
    seen = {}

    def fake_open(request, timeout):
        seen["url"] = request.full_url
        return _Response(_server_info_response())

    importer = JiraImporter(http_open=fake_open)
    client = importer.authenticate(
        "https://mysite.atlassian.net",
        "user@example.com",
        "my-token",
    )

    assert isinstance(client, JiraClient)
    assert client.base_url == "https://mysite.atlassian.net"
    assert client.email == "user@example.com"
    assert "serverInfo" in seen["url"]


def test_importer_fetch_by_jql(monkeypatch):
    def fake_open(request, timeout):
        return _Response(
            _search_response([
                _issue_payload(key="PROJ-101"),
                _issue_payload(key="PROJ-102", summary="Another issue"),
            ])
        )

    monkeypatch.setenv("JIRA_EMAIL", "user@example.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "test-token")

    importer = JiraImporter(
        instance_url="https://mysite.atlassian.net",
        http_open=fake_open,
    )

    briefs = importer.fetch_by_jql("project = PROJ AND status = 'In Progress'")
    assert len(briefs) == 2
    assert briefs[0]["source_id"] == "PROJ-101"
    assert briefs[1]["source_id"] == "PROJ-102"


def test_importer_import_project(monkeypatch):
    call_count = {"n": 0}

    def fake_open(request, timeout):
        call_count["n"] += 1
        return _Response(
            _search_response([
                _epic_payload(key="PROJ-200"),
                _issue_payload(key="PROJ-101"),
            ])
        )

    monkeypatch.setenv("JIRA_EMAIL", "user@example.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "test-token")

    importer = JiraImporter(
        instance_url="https://mysite.atlassian.net",
        project_key="PROJ",
        http_open=fake_open,
    )

    plan = importer.import_project()

    assert plan["project_type"] == "jira"
    assert plan["metadata"]["project_key"] == "PROJ"
    assert len(plan["milestones"]) == 1
    assert plan["milestones"][0]["name"] == "Authentication Epic"
    assert len(plan["tasks"]) == 1
    assert plan["tasks"][0]["id"] == "jira-PROJ-101"


def test_importer_import_epic(monkeypatch):
    call_count = {"n": 0}

    def fake_open(request, timeout):
        call_count["n"] += 1
        if call_count["n"] == 1:
            # Fetching the epic itself
            return _Response(_epic_payload(key="PROJ-200"))
        else:
            # Fetching child issues
            return _Response(
                _search_response([
                    _issue_payload(key="PROJ-101"),
                    _issue_payload(key="PROJ-102", summary="Write tests"),
                ])
            )

    monkeypatch.setenv("JIRA_EMAIL", "user@example.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "test-token")

    importer = JiraImporter(
        instance_url="https://mysite.atlassian.net",
        http_open=fake_open,
    )

    plan = importer.import_epic("PROJ-200")

    assert plan["project_type"] == "jira"
    # Epic is a milestone, two stories are tasks
    assert len(plan["milestones"]) == 1
    assert plan["milestones"][0]["name"] == "Authentication Epic"
    assert len(plan["tasks"]) == 2


def test_importer_requires_instance_url():
    importer = JiraImporter(http_open=lambda r, t: None)
    try:
        importer.import_from_source("PROJ-1")
        assert False, "Expected ValueError"
    except ValueError as exc:
        assert "instance_url" in str(exc)


# ---------------------------------------------------------------------------
# Various issue type handling
# ---------------------------------------------------------------------------


def test_issue_type_story():
    sb = parse_issue_json(
        _issue_payload(issue_type="Story"),
        instance_url="https://x.atlassian.net",
    )
    assert sb["source_entity_type"] == "task"


def test_issue_type_task():
    sb = parse_issue_json(
        _issue_payload(issue_type="Task"),
        instance_url="https://x.atlassian.net",
    )
    assert sb["source_entity_type"] == "task"


def test_issue_type_bug():
    sb = parse_issue_json(
        _issue_payload(issue_type="Bug"),
        instance_url="https://x.atlassian.net",
    )
    assert sb["source_entity_type"] == "task"


def test_issue_type_subtask():
    sb = parse_issue_json(
        _issue_payload(issue_type="Sub-task"),
        instance_url="https://x.atlassian.net",
    )
    assert sb["source_entity_type"] == "task"


def test_issue_type_custom_defaults_to_task():
    sb = parse_issue_json(
        _issue_payload(issue_type="Custom Type"),
        instance_url="https://x.atlassian.net",
    )
    assert sb["source_entity_type"] == "task"


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_issue_without_labels():
    sb = parse_issue_json(
        _issue_payload(labels=[]),
        instance_url="https://x.atlassian.net",
    )
    assert sb["source_payload"]["normalized"]["labels"] == []
    assert "Labels:" not in sb["summary"]


def test_issue_without_components():
    sb = parse_issue_json(
        _issue_payload(components=[]),
        instance_url="https://x.atlassian.net",
    )
    assert sb["source_payload"]["normalized"]["components"] == []
    assert "Components:" not in sb["summary"]


def test_issue_without_assignee():
    sb = parse_issue_json(
        _issue_payload(assignee=None),
        instance_url="https://x.atlassian.net",
    )
    assert sb["source_payload"]["normalized"]["assignee"] is None
    assert "Assignee:" not in sb["summary"]


def test_issue_without_sprint():
    payload = _issue_payload()
    payload["fields"]["customfield_10020"] = []
    sb = parse_issue_json(
        payload,
        instance_url="https://x.atlassian.net",
    )
    assert sb["source_payload"]["normalized"]["sprint"] is None
    assert "Sprint:" not in sb["summary"]


def test_issue_without_story_points():
    sb = parse_issue_json(
        _issue_payload(story_points=None),
        instance_url="https://x.atlassian.net",
    )
    assert sb["source_payload"]["normalized"]["story_points"] is None


def test_issue_no_links():
    sb = parse_issue_json(
        _issue_payload(issue_links=[]),
        instance_url="https://x.atlassian.net",
    )
    assert sb["source_payload"]["normalized"]["links"] == []


def test_issue_no_attachments():
    sb = parse_issue_json(
        _issue_payload(attachments=[]),
        instance_url="https://x.atlassian.net",
    )
    assert sb["source_payload"]["normalized"]["attachments"] == []


def test_complex_jql_with_special_characters(monkeypatch):
    seen = {}

    def fake_open(request, timeout):
        seen["url"] = request.full_url
        return _Response(_search_response([]))

    monkeypatch.setenv("JIRA_EMAIL", "user@example.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "test-token")

    importer = JiraImporter(
        instance_url="https://mysite.atlassian.net",
        http_open=fake_open,
    )

    jql = "project = PROJ AND labels in ('frontend', 'auth') AND status != Done"
    briefs = importer.fetch_by_jql(jql)
    assert briefs == []
    assert "search?" in seen["url"]


def test_sprint_string_field():
    """Sprint field is sometimes a plain string in server instances."""
    payload = _issue_payload()
    payload["fields"]["customfield_10020"] = "Sprint 5"
    sb = parse_issue_json(
        payload,
        instance_url="https://x.atlassian.net",
    )
    assert sb["source_payload"]["normalized"]["sprint"] == "Sprint 5"


def test_sprint_list_with_string_entries():
    """Sprint field can be a list of strings in some configurations."""
    payload = _issue_payload()
    payload["fields"]["customfield_10020"] = ["Sprint 3", "Sprint 4"]
    sb = parse_issue_json(
        payload,
        instance_url="https://x.atlassian.net",
    )
    assert sb["source_payload"]["normalized"]["sprint"] == "Sprint 4"
