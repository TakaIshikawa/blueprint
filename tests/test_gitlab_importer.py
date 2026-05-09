"""Tests for the GitLab importer."""

import json

from blueprint.importers.gitlab_importer import (
    DEFAULT_STATE_MAP,
    Dependency,
    GitLabClient,
    GitLabImporter,
    LABEL_STATE_OVERRIDES,
    import_epic_to_plan,
    import_milestone_to_plan,
    map_gitlab_issue_to_task,
    parse_gitlab_issue_json,
    parse_issue_relations,
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


def _issue_payload(
    *,
    iid: int = 10,
    title: str = "Add user authentication",
    description: str = "Implement OAuth2 login flow.\n\n- [ ] Add login form\n- [x] Setup OAuth provider",
    state: str = "opened",
    weight: int | None = 5,
    time_estimate: int | None = 7200,
    total_time_spent: int | None = 3600,
    labels: list[str] | None = None,
    assignees: list[str] | None = None,
    milestone_title: str | None = "v1.0",
    related_issues: list | None = None,
    blocked_by: list | None = None,
) -> dict:
    issue = {
        "iid": iid,
        "title": title,
        "description": description,
        "state": state,
        "webUrl": f"https://gitlab.com/mygroup/myproject/-/issues/{iid}",
        "weight": weight,
        "timeEstimate": time_estimate,
        "totalTimeSpent": total_time_spent,
        "author": {"username": "developer1", "name": "Dev One"},
        "createdAt": "2026-05-01T10:00:00Z",
        "updatedAt": "2026-05-02T12:00:00Z",
        "closedAt": None,
        "taskCompletionStatus": {"completedCount": 1, "count": 2},
    }

    if labels is not None:
        issue["labels"] = {"nodes": [{"title": l} for l in labels]}
    else:
        issue["labels"] = {"nodes": [{"title": "feature"}, {"title": "auth"}]}

    if assignees is not None:
        issue["assignees"] = {"nodes": [{"username": a, "name": a} for a in assignees]}
    else:
        issue["assignees"] = {"nodes": [{"username": "alice", "name": "Alice"}]}

    if milestone_title:
        issue["milestone"] = {
            "title": milestone_title,
            "description": "First release",
            "startDate": "2026-05-01",
            "dueDate": "2026-06-01",
        }
    else:
        issue["milestone"] = None

    issue["relatedIssues"] = {"nodes": related_issues or []}
    issue["blockedByIssues"] = {"nodes": blocked_by or []}

    return issue


def _epic_payload(
    *,
    iid: int = 1,
    title: str = "Authentication Epic",
    issues: list | None = None,
) -> dict:
    return {
        "iid": iid,
        "title": title,
        "description": "Epic for auth features",
        "state": "opened",
        "webUrl": "https://gitlab.com/groups/mygroup/-/epics/1",
        "author": {"username": "manager", "name": "Manager"},
        "labels": {"nodes": [{"title": "epic"}]},
        "createdAt": "2026-04-01T00:00:00Z",
        "updatedAt": "2026-05-01T00:00:00Z",
        "closedAt": None,
        "startDate": "2026-04-01",
        "dueDate": "2026-07-01",
        "children": {"nodes": []},
        "issues": {"nodes": issues or [_issue_payload()]},
    }


def _graphql_response(data: dict) -> dict:
    return {"data": data}


# ---------------------------------------------------------------------------
# parse_gitlab_issue_json tests
# ---------------------------------------------------------------------------


def test_parse_gitlab_issue_json_normalizes_source_brief():
    brief = parse_gitlab_issue_json(
        _issue_payload(),
        project_path="mygroup/myproject",
    )

    assert brief["title"] == "Add user authentication"
    assert brief["domain"] == "gitlab"
    assert brief["source_project"] == "gitlab"
    assert brief["source_entity_type"] == "issue"
    assert brief["source_id"] == "mygroup/myproject#10"
    assert "GitLab issue mygroup/myproject#10" in brief["summary"]
    assert "State: opened" in brief["summary"]
    assert "Author: developer1" in brief["summary"]
    assert "Labels: feature, auth" in brief["summary"]
    assert "Assignees: alice" in brief["summary"]

    normalized = brief["source_payload"]["normalized"]
    assert normalized["iid"] == 10
    assert normalized["state"] == "opened"
    assert normalized["mapped_state"] == "in_progress"
    assert normalized["weight"] == 5
    assert normalized["time_estimate"] == 7200
    assert normalized["total_time_spent"] == 3600
    assert normalized["milestone"] == "v1.0"

    assert brief["source_links"]["html_url"] == (
        "https://gitlab.com/mygroup/myproject/-/issues/10"
    )


def test_parse_gitlab_issue_json_subtasks_parsed():
    brief = parse_gitlab_issue_json(
        _issue_payload(),
        project_path="mygroup/myproject",
    )
    subtasks = brief["source_payload"]["normalized"]["subtasks"]
    assert len(subtasks) == 2
    assert subtasks[0] == {"title": "Add login form", "completed": False}
    assert subtasks[1] == {"title": "Setup OAuth provider", "completed": True}


def test_parse_gitlab_issue_json_closed_maps_to_completed():
    brief = parse_gitlab_issue_json(
        _issue_payload(state="closed"),
        project_path="p/p",
    )
    assert brief["source_payload"]["normalized"]["mapped_state"] == "completed"


def test_parse_gitlab_issue_json_blocked_label_overrides_state():
    brief = parse_gitlab_issue_json(
        _issue_payload(state="opened", labels=["blocked", "feature"]),
        project_path="p/p",
    )
    assert brief["source_payload"]["normalized"]["mapped_state"] == "blocked"


def test_parse_gitlab_issue_json_review_label():
    brief = parse_gitlab_issue_json(
        _issue_payload(state="opened", labels=["review"]),
        project_path="p/p",
    )
    assert brief["source_payload"]["normalized"]["mapped_state"] == "in_progress"


def test_parse_gitlab_issue_json_with_relations():
    related = [{"iid": 11, "title": "Related issue", "linkType": "relates_to"}]
    blocked_by = [{"iid": 12, "title": "Blocker"}]

    brief = parse_gitlab_issue_json(
        _issue_payload(related_issues=related, blocked_by=blocked_by),
        project_path="p/p",
    )

    relations = brief["source_payload"]["normalized"]["relations"]
    assert len(relations) == 2
    assert relations[0]["target_iid"] == 11
    assert relations[0]["link_type"] == "relates_to"
    assert relations[1]["target_iid"] == 12
    assert relations[1]["link_type"] == "is_blocked_by"


def test_parse_gitlab_issue_json_missing_iid_raises():
    issue = _issue_payload()
    del issue["iid"]
    try:
        parse_gitlab_issue_json(issue, project_path="p/p")
        assert False, "Expected ValueError"
    except ValueError as exc:
        assert "iid" in str(exc)


def test_parse_gitlab_issue_json_missing_title_raises():
    issue = _issue_payload(title="")
    try:
        parse_gitlab_issue_json(issue, project_path="p/p")
        assert False, "Expected ValueError"
    except ValueError as exc:
        assert "title" in str(exc)


# ---------------------------------------------------------------------------
# parse_issue_relations tests
# ---------------------------------------------------------------------------


def test_parse_issue_relations():
    issue = _issue_payload(
        related_issues=[
            {"iid": 20, "title": "Related", "linkType": "blocks"},
        ],
        blocked_by=[
            {"iid": 30, "title": "Blocker"},
        ],
    )

    deps = parse_issue_relations(issue)
    assert len(deps) == 2
    assert isinstance(deps[0], Dependency)
    assert deps[0].source_iid == 10
    assert deps[0].target_iid == 20
    assert deps[0].link_type == "blocks"
    assert deps[1].target_iid == 30
    assert deps[1].link_type == "is_blocked_by"


# ---------------------------------------------------------------------------
# map_gitlab_issue_to_task tests
# ---------------------------------------------------------------------------


def test_map_gitlab_issue_to_task_basic():
    task = map_gitlab_issue_to_task(
        _issue_payload(),
        project_path="mygroup/myproject",
        plan_id="plan-001",
    )

    assert task["id"] == "gl-10"
    assert task["execution_plan_id"] == "plan-001"
    assert task["title"] == "Add user authentication"
    assert task["milestone"] == "v1.0"
    assert task["owner_type"] == "alice"
    assert task["status"] == "in_progress"
    assert task["estimated_hours"] == 2.0  # 7200s / 3600
    assert task["estimated_complexity"] == "medium"
    assert task["metadata"]["gitlab_iid"] == 10
    assert task["metadata"]["labels"] == ["feature", "auth"]
    assert task["metadata"]["weight"] == 5


def test_map_gitlab_issue_to_task_weight_fallback():
    task = map_gitlab_issue_to_task(
        _issue_payload(time_estimate=None, weight=3),
        project_path="p/p",
    )
    assert task["estimated_hours"] == 12.0  # weight * 4


def test_map_gitlab_issue_to_task_high_weight():
    task = map_gitlab_issue_to_task(
        _issue_payload(weight=10),
        project_path="p/p",
    )
    assert task["estimated_complexity"] == "high"


def test_map_gitlab_issue_to_task_low_weight():
    task = map_gitlab_issue_to_task(
        _issue_payload(weight=1),
        project_path="p/p",
    )
    assert task["estimated_complexity"] == "low"


def test_map_gitlab_issue_to_task_dependencies():
    blocked_by = [{"iid": 12, "title": "Blocker"}]
    task = map_gitlab_issue_to_task(
        _issue_payload(blocked_by=blocked_by),
        project_path="p/p",
    )
    assert "12" in task["depends_on"]


# ---------------------------------------------------------------------------
# import_epic_to_plan tests
# ---------------------------------------------------------------------------


def test_import_epic_to_plan():
    issues = [
        _issue_payload(iid=10, milestone_title="v1.0"),
        _issue_payload(iid=11, title="Add logout", milestone_title="v1.0"),
        _issue_payload(iid=12, title="Dashboard", milestone_title="v2.0"),
    ]
    epic = _epic_payload(issues=issues)

    plan = import_epic_to_plan(epic, group_path="mygroup")

    assert plan["project_type"] == "gitlab"
    assert plan["target_repo"] == "mygroup"
    assert plan["metadata"]["epic_iid"] == 1

    # Two unique milestones from issues
    assert len(plan["milestones"]) == 2
    ms_names = {m["name"] for m in plan["milestones"]}
    assert "v1.0" in ms_names
    assert "v2.0" in ms_names

    assert len(plan["tasks"]) == 3
    assert plan["tasks"][0]["id"] == "gl-10"


def test_import_epic_to_plan_no_milestones_uses_epic_title():
    epic = _epic_payload(issues=[_issue_payload(milestone_title=None)])

    plan = import_epic_to_plan(epic, group_path="mygroup")

    assert len(plan["milestones"]) == 1
    assert plan["milestones"][0]["name"] == "Authentication Epic"


# ---------------------------------------------------------------------------
# import_milestone_to_plan tests
# ---------------------------------------------------------------------------


def test_import_milestone_to_plan():
    milestone = {
        "title": "Sprint 1",
        "description": "First sprint",
        "startDate": "2026-05-01",
        "dueDate": "2026-05-14",
        "state": "active",
    }
    issues = [
        _issue_payload(iid=10),
        _issue_payload(iid=11, title="Second task"),
    ]

    plan = import_milestone_to_plan(
        milestone,
        issues,
        project_path="mygroup/myproject",
    )

    assert plan["project_type"] == "gitlab"
    assert plan["milestones"][0]["name"] == "Sprint 1"
    assert plan["milestones"][0]["start_date"] == "2026-05-01"
    assert len(plan["tasks"]) == 2


# ---------------------------------------------------------------------------
# GitLabClient tests
# ---------------------------------------------------------------------------


def test_client_query_sends_graphql():
    seen = {}

    def fake_open(request, timeout):
        seen["url"] = request.full_url
        seen["method"] = request.method
        seen["body"] = json.loads(request.data.decode("utf-8"))
        seen["auth"] = request.headers.get("Authorization")
        return _Response({"data": {"currentUser": {"username": "alice"}}})

    client = GitLabClient(
        token="test-token",
        instance_url="https://gitlab.example.com",
        http_open=fake_open,
    )

    result = client.query("query { currentUser { username } }")

    assert seen["url"] == "https://gitlab.example.com/api/graphql"
    assert seen["method"] == "POST"
    assert "query" in seen["body"]
    assert seen["auth"] == "Bearer test-token"
    assert result["currentUser"]["username"] == "alice"


def test_client_query_raises_on_graphql_errors():
    def fake_open(request, timeout):
        return _Response({"errors": [{"message": "Field not found"}]})

    client = GitLabClient(token="t", http_open=fake_open)

    try:
        client.query("query { bad }")
        assert False, "Expected ImportError"
    except ImportError as exc:
        assert "Field not found" in str(exc)


def test_client_self_hosted():
    seen = {}

    def fake_open(request, timeout):
        seen["url"] = request.full_url
        return _Response({"data": {}})

    client = GitLabClient(
        token="t",
        instance_url="https://git.corp.example.com",
        http_open=fake_open,
    )
    client.query("query { test }")
    assert seen["url"] == "https://git.corp.example.com/api/graphql"


# ---------------------------------------------------------------------------
# GitLabImporter tests
# ---------------------------------------------------------------------------


def test_importer_import_from_source(monkeypatch):
    def fake_open(request, timeout):
        return _Response(_graphql_response({
            "project": {
                "issues": {
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                    "nodes": [_issue_payload(iid=10)],
                }
            }
        }))

    monkeypatch.setenv("GL_TOKEN", "token")
    importer = GitLabImporter(
        token_env="GL_TOKEN",
        http_open=fake_open,
    )

    brief = importer.import_from_source("mygroup/myproject#10")
    assert brief["source_id"] == "mygroup/myproject#10"
    assert brief["title"] == "Add user authentication"


def test_importer_import_from_source_short_id(monkeypatch):
    def fake_open(request, timeout):
        return _Response(_graphql_response({
            "project": {
                "issues": {
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                    "nodes": [_issue_payload(iid=10)],
                }
            }
        }))

    monkeypatch.setenv("GL_TOKEN", "token")
    importer = GitLabImporter(
        token_env="GL_TOKEN",
        default_project="mygroup/myproject",
        http_open=fake_open,
    )

    brief = importer.import_from_source("10")
    assert brief["source_id"] == "mygroup/myproject#10"


def test_importer_validate_source_true(monkeypatch):
    def fake_open(request, timeout):
        return _Response(_graphql_response({
            "project": {
                "issues": {
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                    "nodes": [_issue_payload(iid=10)],
                }
            }
        }))

    monkeypatch.setenv("GL_TOKEN", "t")
    importer = GitLabImporter(
        token_env="GL_TOKEN",
        default_project="p/p",
        http_open=fake_open,
    )
    assert importer.validate_source("10") is True


def test_importer_validate_source_false(monkeypatch):
    def fake_open(request, timeout):
        return _Response(_graphql_response({
            "project": {
                "issues": {
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                    "nodes": [],
                }
            }
        }))

    monkeypatch.setenv("GL_TOKEN", "t")
    importer = GitLabImporter(
        token_env="GL_TOKEN",
        default_project="p/p",
        http_open=fake_open,
    )
    assert importer.validate_source("999") is False


def test_importer_list_available(monkeypatch):
    def fake_open(request, timeout):
        return _Response(_graphql_response({
            "project": {
                "issues": {
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                    "nodes": [
                        _issue_payload(iid=10),
                        _issue_payload(iid=11, title="Second"),
                    ],
                }
            }
        }))

    monkeypatch.setenv("GL_TOKEN", "t")
    importer = GitLabImporter(
        token_env="GL_TOKEN",
        default_project="mygroup/myproject",
        http_open=fake_open,
    )

    items = importer.list_available(limit=10)
    assert len(items) == 2
    assert items[0]["id"] == "mygroup/myproject#10"
    assert items[0]["title"] == "Add user authentication"
    assert items[0]["labels"] == ["feature", "auth"]
    assert items[0]["assignees"] == ["alice"]


def test_importer_list_available_requires_project():
    importer = GitLabImporter(http_open=lambda r, t: None)
    try:
        importer.list_available()
        assert False, "Expected ValueError"
    except ValueError as exc:
        assert "default_project" in str(exc)


def test_importer_authenticate(monkeypatch):
    seen = {}

    def fake_open(request, timeout):
        seen["auth"] = request.headers.get("Authorization")
        return _Response({"data": {"currentUser": {"username": "alice"}}})

    importer = GitLabImporter(http_open=fake_open)
    client = importer.authenticate("my-token", "https://gitlab.example.com")

    assert isinstance(client, GitLabClient)
    assert seen["auth"] == "Bearer my-token"


def test_importer_fetch_group(monkeypatch):
    def fake_open(request, timeout):
        return _Response(_graphql_response({
            "group": {
                "id": "gid://gitlab/Group/1",
                "name": "MyGroup",
                "fullPath": "mygroup",
                "description": "Test group",
                "webUrl": "https://gitlab.com/groups/mygroup",
                "projects": {"nodes": [
                    {"fullPath": "mygroup/proj1", "name": "proj1", "description": ""},
                ]},
                "epics": {"nodes": [
                    {"iid": 1, "title": "Epic 1", "state": "opened", "webUrl": ""},
                ]},
            }
        }))

    monkeypatch.setenv("GL_TOKEN", "t")
    importer = GitLabImporter(token_env="GL_TOKEN", http_open=fake_open)

    group = importer.fetch_group("mygroup")
    assert group.name == "MyGroup"
    assert group.full_path == "mygroup"
    assert len(group.projects) == 1
    assert len(group.epics) == 1


def test_importer_import_epic(monkeypatch):
    def fake_open(request, timeout):
        return _Response(_graphql_response({
            "group": {
                "epic": _epic_payload(issues=[
                    _issue_payload(iid=10),
                    _issue_payload(iid=11, title="Logout"),
                ]),
            }
        }))

    monkeypatch.setenv("GL_TOKEN", "t")
    importer = GitLabImporter(token_env="GL_TOKEN", http_open=fake_open)

    plan = importer.import_epic("mygroup", 1)
    assert plan["project_type"] == "gitlab"
    assert plan["metadata"]["epic_iid"] == 1
    assert len(plan["tasks"]) == 2


def test_importer_import_milestone(monkeypatch):
    def fake_open(request, timeout):
        return _Response(_graphql_response({
            "project": {
                "milestone": {
                    "title": "Sprint 1",
                    "description": "First sprint",
                    "startDate": "2026-05-01",
                    "dueDate": "2026-05-14",
                    "state": "active",
                },
                "issues": {
                    "nodes": [_issue_payload(iid=10)],
                },
            }
        }))

    monkeypatch.setenv("GL_TOKEN", "t")
    importer = GitLabImporter(token_env="GL_TOKEN", http_open=fake_open)

    plan = importer.import_milestone("mygroup/myproject", "Sprint 1")
    assert plan["milestones"][0]["name"] == "Sprint 1"
    assert len(plan["tasks"]) == 1


def test_importer_self_hosted(monkeypatch):
    seen = {}

    def fake_open(request, timeout):
        seen["url"] = request.full_url
        return _Response(_graphql_response({
            "project": {
                "issues": {
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                    "nodes": [_issue_payload(iid=10)],
                }
            }
        }))

    monkeypatch.setenv("GL_TOKEN", "t")
    importer = GitLabImporter(
        token_env="GL_TOKEN",
        instance_url="https://git.corp.example.com",
        http_open=fake_open,
    )

    brief = importer.import_from_source("corp/project#10")
    assert seen["url"] == "https://git.corp.example.com/api/graphql"
    assert brief["source_id"] == "corp/project#10"


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_issue_without_milestone():
    brief = parse_gitlab_issue_json(
        _issue_payload(milestone_title=None),
        project_path="p/p",
    )
    assert brief["source_payload"]["normalized"]["milestone"] is None


def test_issue_without_weight_or_time():
    task = map_gitlab_issue_to_task(
        _issue_payload(weight=None, time_estimate=None),
        project_path="p/p",
    )
    assert task["estimated_hours"] is None
    assert task["estimated_complexity"] == "medium"


def test_issue_empty_description():
    brief = parse_gitlab_issue_json(
        _issue_payload(description=""),
        project_path="p/p",
    )
    assert brief["source_payload"]["normalized"]["subtasks"] == []


def test_source_id_parsing_invalid():
    importer = GitLabImporter(http_open=lambda r, t: None)
    try:
        importer._parse_source_id("abc")
        assert False, "Expected ValueError"
    except ValueError as exc:
        assert "project/path#iid" in str(exc) or "IID" in str(exc)


def test_state_map_defaults():
    assert DEFAULT_STATE_MAP["opened"] == "in_progress"
    assert DEFAULT_STATE_MAP["closed"] == "completed"
    assert DEFAULT_STATE_MAP["merged"] == "completed"


def test_label_state_overrides():
    assert LABEL_STATE_OVERRIDES["blocked"] == "blocked"
    assert LABEL_STATE_OVERRIDES["in progress"] == "in_progress"
    assert LABEL_STATE_OVERRIDES["review"] == "in_progress"
    assert LABEL_STATE_OVERRIDES["done"] == "completed"
