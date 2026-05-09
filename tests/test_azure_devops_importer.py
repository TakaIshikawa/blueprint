"""Tests for the Azure DevOps work item importer."""

import json
from datetime import datetime

from blueprint.importers.azure_devops_importer import (
    AzureDevOpsClient,
    AzureDevOpsImporter,
    DEFAULT_STATE_MAP,
    DEFAULT_TYPE_MAP,
    PROCESS_TEMPLATE_STATE_MAPS,
    import_area_path_to_plan,
    map_work_item_to_task,
    parse_work_item_json,
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


def _work_item_payload(
    *,
    wi_id: int = 101,
    wi_type: str = "User Story",
    title: str = "Implement login page",
    state: str = "Active",
    description: str = "Build the login page with OAuth support.",
    tags: str = "frontend;auth",
    area_path: str = "MyProject\\Web",
    iteration_path: str = "MyProject\\Sprint 1",
    assigned_to: str | None = "Alice Smith",
    created_by: str | None = "Bob Jones",
    story_points: float | None = 5.0,
    remaining_work: float | None = 12.0,
    priority: int | None = 2,
    severity: str | None = None,
    acceptance_criteria: str = "Login form validates credentials",
    relations: list | None = None,
) -> dict:
    """Build a realistic Azure DevOps work item payload."""
    fields = {
        "System.WorkItemType": wi_type,
        "System.Title": title,
        "System.State": state,
        "System.Description": description,
        "System.Tags": tags,
        "System.AreaPath": area_path,
        "System.IterationPath": iteration_path,
        "System.ChangedDate": "2026-05-01T10:00:00Z",
        "Microsoft.VSTS.Common.AcceptanceCriteria": acceptance_criteria,
    }
    if assigned_to:
        fields["System.AssignedTo"] = {"displayName": assigned_to, "uniqueName": "alice@example.com"}
    if created_by:
        fields["System.CreatedBy"] = {"displayName": created_by, "uniqueName": "bob@example.com"}
    if story_points is not None:
        fields["Microsoft.VSTS.Scheduling.StoryPoints"] = story_points
    if remaining_work is not None:
        fields["Microsoft.VSTS.Scheduling.RemainingWork"] = remaining_work
    if priority is not None:
        fields["Microsoft.VSTS.Common.Priority"] = priority
    if severity is not None:
        fields["Microsoft.VSTS.Common.Severity"] = severity

    payload = {
        "id": wi_id,
        "rev": 3,
        "fields": fields,
        "url": f"https://dev.azure.com/myorg/myproj/_apis/wit/workitems/{wi_id}",
    }
    if relations is not None:
        payload["relations"] = relations

    return payload


def _epic_payload(*, wi_id: int = 200, title: str = "Authentication Epic") -> dict:
    return _work_item_payload(
        wi_id=wi_id,
        wi_type="Epic",
        title=title,
        state="Active",
        description="Epic for all auth work.",
        tags="epic;auth",
        story_points=None,
        remaining_work=None,
        priority=1,
    )


def _wiql_response(ids: list[int]) -> dict:
    return {"workItems": [{"id": wi_id, "url": f"https://example.com/{wi_id}"} for wi_id in ids]}


def _batch_response(work_items: list[dict]) -> dict:
    return {"count": len(work_items), "value": work_items}


def _project_response() -> dict:
    return {
        "id": "proj-001",
        "name": "MyProject",
        "description": "A test project",
        "url": "https://dev.azure.com/myorg/_apis/projects/proj-001",
        "state": "wellFormed",
        "capabilities": {
            "processTemplate": {
                "templateName": "Agile",
                "templateTypeId": "abc-123",
            }
        },
    }


# ---------------------------------------------------------------------------
# parse_work_item_json tests
# ---------------------------------------------------------------------------


def test_parse_work_item_json_normalizes_source_brief():
    source_brief = parse_work_item_json(
        _work_item_payload(),
        organization="myorg",
        project="myproj",
    )

    assert source_brief["title"] == "Implement login page"
    assert source_brief["domain"] == "azure-devops"
    assert source_brief["source_project"] == "azure-devops"
    assert source_brief["source_entity_type"] == "task"
    assert source_brief["source_id"] == "myorg/myproj/101"
    assert "Azure DevOps User Story myorg/myproj#101" in source_brief["summary"]
    assert "State: Active" in source_brief["summary"]
    assert "Assigned to: Alice Smith" in source_brief["summary"]
    assert "Tags: frontend, auth" in source_brief["summary"]

    normalized = source_brief["source_payload"]["normalized"]
    assert normalized["id"] == 101
    assert normalized["type"] == "User Story"
    assert normalized["state"] == "Active"
    assert normalized["mapped_state"] == "in_progress"
    assert normalized["assigned_to"] == "Alice Smith"
    assert normalized["created_by"] == "Bob Jones"
    assert normalized["tags"] == ["frontend", "auth"]
    assert normalized["story_points"] == 5.0
    assert normalized["remaining_work"] == 12.0
    assert normalized["priority"] == 2

    assert source_brief["source_links"]["html_url"] == (
        "https://dev.azure.com/myorg/myproj/_workitems/edit/101"
    )


def test_parse_work_item_json_epic_maps_to_phase():
    source_brief = parse_work_item_json(
        _epic_payload(),
        organization="myorg",
        project="myproj",
    )
    assert source_brief["source_entity_type"] == "phase"


def test_parse_work_item_json_custom_type_map():
    source_brief = parse_work_item_json(
        _work_item_payload(wi_type="Bug"),
        organization="myorg",
        project="myproj",
        type_map={"Bug": "phase"},
    )
    assert source_brief["source_entity_type"] == "phase"


def test_parse_work_item_json_custom_state_map():
    source_brief = parse_work_item_json(
        _work_item_payload(state="In Review"),
        organization="myorg",
        project="myproj",
        state_map={"In Review": "blocked"},
    )
    assert source_brief["source_payload"]["normalized"]["mapped_state"] == "blocked"


def test_parse_work_item_json_with_relations():
    relations = [
        {
            "rel": "System.LinkTypes.Hierarchy-Reverse",
            "url": "https://dev.azure.com/myorg/myproj/_apis/wit/workitems/200",
            "attributes": {"comment": "Parent epic"},
        },
        {
            "rel": "System.LinkTypes.Dependency-Reverse",
            "url": "https://dev.azure.com/myorg/myproj/_apis/wit/workitems/100",
            "attributes": {},
        },
        {
            "rel": "System.LinkTypes.Related",
            "url": "https://dev.azure.com/myorg/myproj/_apis/wit/workitems/102",
            "attributes": {},
        },
    ]
    source_brief = parse_work_item_json(
        _work_item_payload(relations=relations),
        organization="myorg",
        project="myproj",
    )
    links = source_brief["source_payload"]["normalized"]["links"]
    assert len(links) == 3
    assert links[0]["type"] == "Parent"
    assert links[0]["target_id"] == 200
    assert links[0]["comment"] == "Parent epic"
    assert links[1]["type"] == "Predecessor"
    assert links[1]["target_id"] == 100
    assert links[2]["type"] == "Related"
    assert links[2]["target_id"] == 102


def test_parse_work_item_json_missing_title_raises():
    payload = _work_item_payload()
    payload["fields"]["System.Title"] = ""
    try:
        parse_work_item_json(payload, organization="o", project="p")
        assert False, "Expected ValueError"
    except ValueError as exc:
        assert "System.Title" in str(exc)


def test_parse_work_item_json_missing_id_raises():
    payload = _work_item_payload()
    del payload["id"]
    try:
        parse_work_item_json(payload, organization="o", project="p")
        assert False, "Expected ValueError"
    except ValueError as exc:
        assert "id" in str(exc)


# ---------------------------------------------------------------------------
# map_work_item_to_task tests
# ---------------------------------------------------------------------------


def test_map_work_item_to_task_basic():
    task = map_work_item_to_task(
        _work_item_payload(),
        organization="myorg",
        project="myproj",
        plan_id="plan-001",
    )

    assert task["id"] == "ado-101"
    assert task["execution_plan_id"] == "plan-001"
    assert task["title"] == "Implement login page"
    assert task["description"] == "Build the login page with OAuth support."
    assert task["milestone"] == "MyProject\\Sprint 1"
    assert task["owner_type"] == "Alice Smith"
    assert task["status"] == "in_progress"
    assert task["estimated_hours"] == 12.0  # from remaining_work
    assert task["estimated_complexity"] == "medium"  # 5 story points
    assert task["risk_level"] == "medium"  # priority 2
    assert task["acceptance_criteria"] == ["Login form validates credentials"]
    assert task["metadata"]["azure_work_item_type"] == "User Story"
    assert task["metadata"]["azure_area_path"] == "MyProject\\Web"
    assert task["metadata"]["tags"] == ["frontend", "auth"]
    assert task["metadata"]["azure_organization"] == "myorg"
    assert task["metadata"]["azure_project"] == "myproj"


def test_map_work_item_to_task_dependencies():
    relations = [
        {
            "rel": "System.LinkTypes.Dependency-Reverse",
            "url": "https://dev.azure.com/myorg/myproj/_apis/wit/workitems/50",
            "attributes": {},
        },
        {
            "rel": "System.LinkTypes.Hierarchy-Reverse",
            "url": "https://dev.azure.com/myorg/myproj/_apis/wit/workitems/200",
            "attributes": {},
        },
    ]
    task = map_work_item_to_task(
        _work_item_payload(relations=relations),
        organization="myorg",
        project="myproj",
    )
    assert "50" in task["depends_on"]
    assert "200" in task["depends_on"]


def test_map_work_item_to_task_high_story_points():
    task = map_work_item_to_task(
        _work_item_payload(story_points=13, remaining_work=None),
        organization="myorg",
        project="myproj",
    )
    assert task["estimated_complexity"] == "high"
    assert task["estimated_hours"] == 52.0  # 13 * 4


def test_map_work_item_to_task_low_story_points():
    task = map_work_item_to_task(
        _work_item_payload(story_points=1, remaining_work=None),
        organization="myorg",
        project="myproj",
    )
    assert task["estimated_complexity"] == "low"
    assert task["estimated_hours"] == 4.0


def test_map_work_item_to_task_priority_1_is_high_risk():
    task = map_work_item_to_task(
        _work_item_payload(priority=1),
        organization="myorg",
        project="myproj",
    )
    assert task["risk_level"] == "high"


def test_map_work_item_to_task_scrum_state_mapping():
    task = map_work_item_to_task(
        _work_item_payload(state="Committed"),
        organization="myorg",
        project="myproj",
        state_map=PROCESS_TEMPLATE_STATE_MAPS["scrum"],
    )
    assert task["status"] == "in_progress"


def test_map_work_item_to_task_cmmi_state_mapping():
    task = map_work_item_to_task(
        _work_item_payload(state="Proposed"),
        organization="myorg",
        project="myproj",
        state_map=PROCESS_TEMPLATE_STATE_MAPS["cmmi"],
    )
    assert task["status"] == "pending"


# ---------------------------------------------------------------------------
# import_area_path_to_plan tests
# ---------------------------------------------------------------------------


def test_import_area_path_to_plan_separates_epics_and_tasks():
    work_items = [
        _epic_payload(wi_id=200, title="Auth Epic"),
        _work_item_payload(wi_id=101, wi_type="User Story"),
        _work_item_payload(wi_id=102, wi_type="Task", title="Write tests"),
    ]

    plan = import_area_path_to_plan(
        work_items,
        organization="myorg",
        project="myproj",
        area_path="MyProject\\Web",
    )

    assert plan["project_type"] == "azure-devops"
    assert plan["target_repo"] == "myorg/myproj"
    assert plan["metadata"]["area_path"] == "MyProject\\Web"

    # Epic should become a milestone
    assert len(plan["milestones"]) == 1
    assert plan["milestones"][0]["name"] == "Auth Epic"
    assert plan["milestones"][0]["azure_work_item_type"] == "Epic"

    # User Story and Task should become tasks
    assert len(plan["tasks"]) == 2
    assert plan["tasks"][0]["id"] == "ado-101"
    assert plan["tasks"][1]["id"] == "ado-102"


def test_import_area_path_to_plan_derives_milestones_from_iterations():
    work_items = [
        _work_item_payload(wi_id=101),
        _work_item_payload(wi_id=102, iteration_path="MyProject\\Sprint 2"),
    ]

    plan = import_area_path_to_plan(
        work_items,
        organization="myorg",
        project="myproj",
        area_path="MyProject",
    )

    # No epics, so milestones should come from unique iteration paths
    assert len(plan["milestones"]) == 2
    iteration_names = {m["name"] for m in plan["milestones"]}
    assert "MyProject\\Sprint 1" in iteration_names
    assert "MyProject\\Sprint 2" in iteration_names


def test_import_area_path_to_plan_empty_work_items():
    plan = import_area_path_to_plan(
        [],
        organization="myorg",
        project="myproj",
        area_path="MyProject\\Empty",
    )

    assert plan["tasks"] == []
    assert plan["milestones"] == []
    assert plan["status"] == "draft"


# ---------------------------------------------------------------------------
# Process template state mapping tests
# ---------------------------------------------------------------------------


def test_agile_state_map():
    state_map = PROCESS_TEMPLATE_STATE_MAPS["agile"]
    assert state_map["New"] == "pending"
    assert state_map["Active"] == "in_progress"
    assert state_map["Resolved"] == "completed"
    assert state_map["Closed"] == "completed"
    assert state_map["Removed"] == "skipped"


def test_scrum_state_map():
    state_map = PROCESS_TEMPLATE_STATE_MAPS["scrum"]
    assert state_map["New"] == "pending"
    assert state_map["Approved"] == "pending"
    assert state_map["Committed"] == "in_progress"
    assert state_map["Done"] == "completed"


def test_cmmi_state_map():
    state_map = PROCESS_TEMPLATE_STATE_MAPS["cmmi"]
    assert state_map["Proposed"] == "pending"
    assert state_map["Active"] == "in_progress"
    assert state_map["Resolved"] == "completed"
    assert state_map["Closed"] == "completed"


def test_default_type_map_covers_standard_types():
    assert DEFAULT_TYPE_MAP["Epic"] == "phase"
    assert DEFAULT_TYPE_MAP["Feature"] == "phase"
    assert DEFAULT_TYPE_MAP["User Story"] == "task"
    assert DEFAULT_TYPE_MAP["Task"] == "task"
    assert DEFAULT_TYPE_MAP["Bug"] == "task"


# ---------------------------------------------------------------------------
# AzureDevOpsClient tests
# ---------------------------------------------------------------------------


def test_client_request_json_builds_correct_url():
    seen = {}

    def fake_open(request, timeout):
        seen["url"] = request.full_url
        seen["auth"] = request.headers.get("Authorization")
        seen["timeout"] = timeout
        return _Response({"value": []})

    client = AzureDevOpsClient(
        organization="myorg",
        project="myproj",
        pat="test-pat",
        http_open=fake_open,
    )

    result = client.request_json("wit/workitems/101")

    assert "dev.azure.com/myorg/myproj/_apis/wit/workitems/101" in seen["url"]
    assert "api-version=7.0" in seen["url"]
    assert seen["auth"].startswith("Basic ")
    assert seen["timeout"] == 30


def test_client_post_json_sends_body():
    seen = {}

    def fake_open(request, timeout):
        seen["url"] = request.full_url
        seen["method"] = request.method
        seen["body"] = json.loads(request.data.decode("utf-8"))
        seen["content_type"] = request.headers.get("Content-type")
        return _Response({"workItems": [{"id": 1}]})

    client = AzureDevOpsClient(
        organization="myorg",
        project="myproj",
        pat="test-pat",
        http_open=fake_open,
    )

    body = {"query": "SELECT [System.Id] FROM WorkItems"}
    result = client.post_json("wit/wiql", body)

    assert seen["method"] == "POST"
    assert seen["body"]["query"] == "SELECT [System.Id] FROM WorkItems"
    assert seen["content_type"] == "application/json"
    assert result["workItems"] == [{"id": 1}]


# ---------------------------------------------------------------------------
# AzureDevOpsImporter tests
# ---------------------------------------------------------------------------


def test_importer_import_from_source_with_full_source_id(monkeypatch):
    seen = {}

    def fake_open(request, timeout):
        seen["url"] = request.full_url
        seen["auth"] = request.headers.get("Authorization")
        return _Response(_work_item_payload())

    monkeypatch.setenv("ADO_TEST_PAT", "my-test-pat")
    importer = AzureDevOpsImporter(
        token_env="ADO_TEST_PAT",
        http_open=fake_open,
    )

    source_brief = importer.import_from_source("myorg/myproj/101")

    assert "dev.azure.com/myorg/myproj/_apis/wit/workitems/101" in seen["url"]
    assert seen["auth"].startswith("Basic ")
    assert source_brief["source_id"] == "myorg/myproj/101"
    assert source_brief["title"] == "Implement login page"


def test_importer_import_from_source_with_short_id(monkeypatch):
    def fake_open(request, timeout):
        return _Response(_work_item_payload())

    monkeypatch.setenv("ADO_PAT", "pat")
    importer = AzureDevOpsImporter(
        organization="myorg",
        project="myproj",
        token_env="ADO_PAT",
        http_open=fake_open,
    )

    source_brief = importer.import_from_source("101")
    assert source_brief["source_id"] == "myorg/myproj/101"


def test_importer_validate_source_returns_true(monkeypatch):
    def fake_open(request, timeout):
        return _Response(_work_item_payload())

    monkeypatch.setenv("ADO_PAT", "pat")
    importer = AzureDevOpsImporter(
        organization="myorg",
        project="myproj",
        token_env="ADO_PAT",
        http_open=fake_open,
    )

    assert importer.validate_source("101") is True


def test_importer_validate_source_returns_false_on_error(monkeypatch):
    from urllib.error import HTTPError

    def fake_open(request, timeout):
        raise HTTPError(request.full_url, 404, "Not Found", {}, None)

    monkeypatch.setenv("ADO_PAT", "pat")
    importer = AzureDevOpsImporter(
        organization="myorg",
        project="myproj",
        token_env="ADO_PAT",
        http_open=fake_open,
    )

    assert importer.validate_source("999") is False


def test_importer_list_available(monkeypatch):
    call_count = {"n": 0}

    def fake_open(request, timeout):
        call_count["n"] += 1
        if call_count["n"] == 1:
            # WIQL response
            return _Response(_wiql_response([101, 102]))
        else:
            # Batch response
            return _Response(
                _batch_response([
                    _work_item_payload(wi_id=101),
                    _work_item_payload(wi_id=102, title="Second item"),
                ])
            )

    monkeypatch.setenv("ADO_PAT", "pat")
    importer = AzureDevOpsImporter(
        organization="myorg",
        project="myproj",
        token_env="ADO_PAT",
        http_open=fake_open,
    )

    items = importer.list_available(limit=10)
    assert len(items) == 2
    assert items[0]["id"] == "myorg/myproj/101"
    assert items[0]["title"] == "Implement login page"
    assert items[0]["state"] == "Active"
    assert items[0]["type"] == "User Story"
    assert items[0]["assigned_to"] == "Alice Smith"
    assert "dev.azure.com" in items[0]["html_url"]
    assert items[1]["work_item_id"] == 102


def test_importer_list_available_requires_org_and_project():
    importer = AzureDevOpsImporter(http_open=lambda r, t: None)
    try:
        importer.list_available()
        assert False, "Expected ValueError"
    except ValueError as exc:
        assert "organization and project are required" in str(exc)


def test_importer_authenticate(monkeypatch):
    seen = {}

    def fake_open(request, timeout):
        seen["url"] = request.full_url
        return _Response({"count": 1, "value": [_project_response()]})

    importer = AzureDevOpsImporter(http_open=fake_open)
    client = importer.authenticate("myorg", "myproj", "my-pat")

    assert isinstance(client, AzureDevOpsClient)
    assert client.organization == "myorg"
    assert client.project == "myproj"
    assert client.pat == "my-pat"
    assert "projects" in seen["url"]


def test_importer_fetch_project(monkeypatch):
    def fake_open(request, timeout):
        return _Response(_project_response())

    monkeypatch.setenv("ADO_PAT", "pat")
    importer = AzureDevOpsImporter(
        organization="myorg",
        token_env="ADO_PAT",
        http_open=fake_open,
    )

    project = importer.fetch_project("MyProject")

    assert project.id == "proj-001"
    assert project.name == "MyProject"
    assert project.description == "A test project"
    assert project.state == "wellFormed"
    assert project.process_template == "Agile"


def test_importer_import_area_path(monkeypatch):
    call_count = {"n": 0}

    def fake_open(request, timeout):
        call_count["n"] += 1
        if call_count["n"] == 1:
            return _Response(_wiql_response([101, 200]))
        else:
            return _Response(
                _batch_response([
                    _work_item_payload(wi_id=101),
                    _epic_payload(wi_id=200),
                ])
            )

    monkeypatch.setenv("ADO_PAT", "pat")
    importer = AzureDevOpsImporter(
        organization="myorg",
        project="myproj",
        token_env="ADO_PAT",
        http_open=fake_open,
    )

    plan = importer.import_area_path("MyProject\\Web")

    assert plan["project_type"] == "azure-devops"
    assert plan["metadata"]["area_path"] == "MyProject\\Web"
    assert len(plan["milestones"]) == 1
    assert plan["milestones"][0]["name"] == "Authentication Epic"
    assert len(plan["tasks"]) == 1
    assert plan["tasks"][0]["id"] == "ado-101"


def test_importer_sync_updates(monkeypatch):
    call_count = {"n": 0}

    def fake_open(request, timeout):
        call_count["n"] += 1
        if call_count["n"] == 1:
            return _Response(_wiql_response([101]))
        else:
            return _Response(_batch_response([_work_item_payload(wi_id=101)]))

    monkeypatch.setenv("ADO_PAT", "pat")
    importer = AzureDevOpsImporter(
        organization="myorg",
        token_env="ADO_PAT",
        http_open=fake_open,
    )

    since = datetime(2026, 5, 1)
    tasks = importer.sync_updates("myproj", since)

    assert len(tasks) == 1
    assert tasks[0]["id"] == "ado-101"
    assert tasks[0]["title"] == "Implement login page"


def test_importer_sync_updates_empty(monkeypatch):
    def fake_open(request, timeout):
        return _Response({"workItems": []})

    monkeypatch.setenv("ADO_PAT", "pat")
    importer = AzureDevOpsImporter(
        organization="myorg",
        token_env="ADO_PAT",
        http_open=fake_open,
    )

    tasks = importer.sync_updates("myproj", datetime(2026, 5, 1))
    assert tasks == []


def test_importer_process_template_scrum(monkeypatch):
    def fake_open(request, timeout):
        return _Response(_work_item_payload(state="Committed"))

    monkeypatch.setenv("ADO_PAT", "pat")
    importer = AzureDevOpsImporter(
        organization="myorg",
        project="myproj",
        token_env="ADO_PAT",
        process_template="scrum",
        http_open=fake_open,
    )

    source_brief = importer.import_from_source("101")
    assert source_brief["source_payload"]["normalized"]["mapped_state"] == "in_progress"


# ---------------------------------------------------------------------------
# Sprint / iteration mapping tests
# ---------------------------------------------------------------------------


def test_sprint_hierarchy_maps_to_milestones():
    work_items = [
        _work_item_payload(wi_id=1, iteration_path="Project\\Release 1\\Sprint 1"),
        _work_item_payload(wi_id=2, iteration_path="Project\\Release 1\\Sprint 2"),
        _work_item_payload(wi_id=3, iteration_path="Project\\Release 2\\Sprint 3"),
    ]

    plan = import_area_path_to_plan(
        work_items,
        organization="myorg",
        project="myproj",
        area_path="Project",
    )

    assert len(plan["milestones"]) == 3
    names = {m["name"] for m in plan["milestones"]}
    assert "Project\\Release 1\\Sprint 1" in names
    assert "Project\\Release 1\\Sprint 2" in names
    assert "Project\\Release 2\\Sprint 3" in names


# ---------------------------------------------------------------------------
# Webhook integration test (service hooks payload)
# ---------------------------------------------------------------------------


def test_parse_webhook_work_item_updated():
    """Verify that a work item from a webhook-style payload can be parsed."""
    webhook_payload = _work_item_payload(
        wi_id=555,
        state="Resolved",
        title="Webhook-triggered update",
    )

    source_brief = parse_work_item_json(
        webhook_payload,
        organization="myorg",
        project="myproj",
    )
    assert source_brief["source_id"] == "myorg/myproj/555"
    assert source_brief["source_payload"]["normalized"]["state"] == "Resolved"
    assert source_brief["source_payload"]["normalized"]["mapped_state"] == "completed"


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_work_item_without_tags():
    wi = _work_item_payload(tags="")
    source_brief = parse_work_item_json(wi, organization="o", project="p")
    assert source_brief["source_payload"]["normalized"]["tags"] == []


def test_work_item_without_assigned_to():
    wi = _work_item_payload(assigned_to=None)
    source_brief = parse_work_item_json(wi, organization="o", project="p")
    assert source_brief["source_payload"]["normalized"]["assigned_to"] is None
    assert "Assigned to:" not in source_brief["summary"]


def test_work_item_no_relations():
    wi = _work_item_payload()
    source_brief = parse_work_item_json(wi, organization="o", project="p")
    assert source_brief["source_payload"]["normalized"]["links"] == []


def test_task_without_remaining_work_or_story_points():
    task = map_work_item_to_task(
        _work_item_payload(remaining_work=None, story_points=None),
        organization="o",
        project="p",
    )
    assert task["estimated_hours"] is None
    assert task["estimated_complexity"] == "medium"


def test_source_id_parsing_invalid_format():
    importer = AzureDevOpsImporter(http_open=lambda r, t: None)
    try:
        importer.import_from_source("org/proj")
        assert False, "Expected ValueError"
    except ValueError as exc:
        assert "Invalid" in str(exc)


def test_source_id_parsing_non_numeric_id():
    importer = AzureDevOpsImporter(
        organization="org",
        project="proj",
        http_open=lambda r, t: None,
    )
    try:
        importer.import_from_source("abc")
        assert False, "Expected ValueError"
    except ValueError as exc:
        assert "work item ID" in str(exc)
