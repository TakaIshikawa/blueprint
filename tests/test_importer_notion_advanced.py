"""Tests for the Notion advanced importer."""

import json

from blueprint.importers.notion_advanced import (
    DEFAULT_STATUS_MAP,
    NotionAdvancedImporter,
    NotionClient,
    SyncState,
    build_database_filter,
    extract_all_properties,
    extract_block_content,
    extract_page_blocks,
    extract_property_value,
    import_database_to_plan,
    map_notion_page_to_task,
    parse_notion_page_json,
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


def _page_payload(
    *,
    page_id: str = "page-001",
    title: str = "Implement OAuth",
    status: str = "In progress",
    extra_properties: dict | None = None,
    url: str = "https://notion.so/page-001",
    created_time: str = "2026-05-01T10:00:00.000Z",
    last_edited_time: str = "2026-05-02T12:00:00.000Z",
) -> dict:
    properties = {
        "Name": {
            "type": "title",
            "title": [{"type": "text", "plain_text": title}],
        },
        "Status": {
            "type": "status",
            "status": {"name": status, "color": "blue"},
        },
    }
    if extra_properties:
        properties.update(extra_properties)

    return {
        "id": page_id,
        "object": "page",
        "url": url,
        "created_time": created_time,
        "last_edited_time": last_edited_time,
        "parent": {"type": "database_id", "database_id": "db-001"},
        "properties": properties,
    }


def _database_meta(
    *,
    database_id: str = "db-001",
    title: str = "Sprint Board",
) -> dict:
    return {
        "id": database_id,
        "object": "database",
        "title": [{"type": "text", "plain_text": title}],
        "properties": {
            "Name": {"type": "title", "title": {}},
            "Status": {"type": "status", "status": {}},
        },
    }


# ---------------------------------------------------------------------------
# extract_property_value tests
# ---------------------------------------------------------------------------


def test_extract_title():
    prop = {"type": "title", "title": [{"plain_text": "Hello"}, {"plain_text": " World"}]}
    assert extract_property_value(prop) == "Hello World"


def test_extract_rich_text():
    prop = {"type": "rich_text", "rich_text": [{"plain_text": "Some text"}]}
    assert extract_property_value(prop) == "Some text"


def test_extract_number():
    assert extract_property_value({"type": "number", "number": 42}) == 42
    assert extract_property_value({"type": "number", "number": None}) is None


def test_extract_select():
    prop = {"type": "select", "select": {"name": "High"}}
    assert extract_property_value(prop) == "High"

    prop_none = {"type": "select", "select": None}
    assert extract_property_value(prop_none) is None


def test_extract_multi_select():
    prop = {
        "type": "multi_select",
        "multi_select": [{"name": "tag1"}, {"name": "tag2"}],
    }
    assert extract_property_value(prop) == ["tag1", "tag2"]


def test_extract_date():
    prop = {"type": "date", "date": {"start": "2026-05-01", "end": "2026-05-10"}}
    result = extract_property_value(prop)
    assert result == {"start": "2026-05-01", "end": "2026-05-10"}


def test_extract_date_none():
    prop = {"type": "date", "date": None}
    assert extract_property_value(prop) is None


def test_extract_people():
    prop = {
        "type": "people",
        "people": [
            {"name": "Alice", "id": "user-1"},
            {"name": "Bob", "id": "user-2"},
        ],
    }
    assert extract_property_value(prop) == ["Alice", "Bob"]


def test_extract_files():
    prop = {
        "type": "files",
        "files": [
            {"name": "doc.pdf", "type": "external", "external": {"url": "https://example.com/doc.pdf"}},
            {"name": "img.png", "type": "file", "file": {"url": "https://s3.example.com/img.png"}},
        ],
    }
    result = extract_property_value(prop)
    assert len(result) == 2
    assert result[0] == {"name": "doc.pdf", "url": "https://example.com/doc.pdf"}
    assert result[1] == {"name": "img.png", "url": "https://s3.example.com/img.png"}


def test_extract_checkbox():
    assert extract_property_value({"type": "checkbox", "checkbox": True}) is True
    assert extract_property_value({"type": "checkbox", "checkbox": False}) is False


def test_extract_url():
    prop = {"type": "url", "url": "https://example.com"}
    assert extract_property_value(prop) == "https://example.com"


def test_extract_email():
    prop = {"type": "email", "email": "alice@example.com"}
    assert extract_property_value(prop) == "alice@example.com"


def test_extract_phone():
    prop = {"type": "phone_number", "phone_number": "+1-555-0100"}
    assert extract_property_value(prop) == "+1-555-0100"


def test_extract_formula_number():
    prop = {"type": "formula", "formula": {"type": "number", "number": 95.5}}
    assert extract_property_value(prop) == 95.5


def test_extract_formula_string():
    prop = {"type": "formula", "formula": {"type": "string", "string": "High Priority"}}
    assert extract_property_value(prop) == "High Priority"


def test_extract_formula_boolean():
    prop = {"type": "formula", "formula": {"type": "boolean", "boolean": True}}
    assert extract_property_value(prop) is True


def test_extract_relation():
    prop = {
        "type": "relation",
        "relation": [{"id": "page-101"}, {"id": "page-102"}],
    }
    assert extract_property_value(prop) == ["page-101", "page-102"]


def test_extract_rollup_number():
    prop = {"type": "rollup", "rollup": {"type": "number", "number": 10}}
    assert extract_property_value(prop) == 10


def test_extract_rollup_array():
    prop = {
        "type": "rollup",
        "rollup": {
            "type": "array",
            "array": [
                {"type": "number", "number": 5},
                {"type": "number", "number": 10},
            ],
        },
    }
    assert extract_property_value(prop) == [5, 10]


def test_extract_status():
    prop = {"type": "status", "status": {"name": "In progress", "color": "blue"}}
    assert extract_property_value(prop) == "In progress"


def test_extract_created_time():
    prop = {"type": "created_time", "created_time": "2026-05-01T10:00:00.000Z"}
    assert extract_property_value(prop) == "2026-05-01T10:00:00.000Z"


def test_extract_created_by():
    prop = {"type": "created_by", "created_by": {"name": "Alice", "id": "user-1"}}
    assert extract_property_value(prop) == "Alice"


def test_extract_last_edited_time():
    prop = {"type": "last_edited_time", "last_edited_time": "2026-05-02T12:00:00.000Z"}
    assert extract_property_value(prop) == "2026-05-02T12:00:00.000Z"


def test_extract_last_edited_by():
    prop = {"type": "last_edited_by", "last_edited_by": {"name": "Bob", "id": "user-2"}}
    assert extract_property_value(prop) == "Bob"


def test_extract_unique_id_with_prefix():
    prop = {"type": "unique_id", "unique_id": {"prefix": "TASK", "number": 42}}
    assert extract_property_value(prop) == "TASK-42"


def test_extract_unique_id_without_prefix():
    prop = {"type": "unique_id", "unique_id": {"prefix": "", "number": 7}}
    assert extract_property_value(prop) == "7"


def test_extract_unknown_type():
    prop = {"type": "unsupported_new_type", "unsupported_new_type": "value"}
    assert extract_property_value(prop) is None


# ---------------------------------------------------------------------------
# extract_all_properties tests
# ---------------------------------------------------------------------------


def test_extract_all_properties_basic():
    properties = {
        "Name": {"type": "title", "title": [{"plain_text": "Task A"}]},
        "Priority": {"type": "select", "select": {"name": "High"}},
        "Tags": {"type": "multi_select", "multi_select": [{"name": "frontend"}]},
    }
    result = extract_all_properties(properties)
    assert result["Name"] == "Task A"
    assert result["Priority"] == "High"
    assert result["Tags"] == ["frontend"]


def test_extract_all_properties_with_map():
    properties = {
        "Name": {"type": "title", "title": [{"plain_text": "Task A"}]},
        "Priority": {"type": "select", "select": {"name": "High"}},
    }
    result = extract_all_properties(
        properties,
        property_map={"Name": "title", "Priority": "priority"},
    )
    assert result["title"] == "Task A"
    assert result["priority"] == "High"


def test_extract_all_properties_skips_non_dict():
    properties = {"Name": {"type": "title", "title": [{"plain_text": "X"}]}, "bad": "not a dict"}
    result = extract_all_properties(properties)
    assert "Name" in result
    assert "bad" not in result


# ---------------------------------------------------------------------------
# build_database_filter tests
# ---------------------------------------------------------------------------


def test_build_filter_empty():
    assert build_database_filter({}) == {}


def test_build_filter_simple_property():
    config = {"property": "Status", "status": {"equals": "Done"}}
    result = build_database_filter(config)
    assert result == {"property": "Status", "status": {"equals": "Done"}}


def test_build_filter_compound_and():
    config = {
        "and": [
            {"property": "Status", "status": {"equals": "In progress"}},
            {"property": "Priority", "select": {"equals": "High"}},
        ]
    }
    result = build_database_filter(config)
    assert "and" in result
    assert len(result["and"]) == 2
    assert result["and"][0]["property"] == "Status"
    assert result["and"][1]["property"] == "Priority"


def test_build_filter_compound_or():
    config = {
        "or": [
            {"property": "Status", "status": {"equals": "Done"}},
            {"property": "Status", "status": {"equals": "In progress"}},
        ]
    }
    result = build_database_filter(config)
    assert "or" in result
    assert len(result["or"]) == 2


def test_build_filter_formula():
    config = {"property": "Score", "formula": {"number": {"greater_than": 80}}}
    result = build_database_filter(config)
    assert result["property"] == "Score"
    assert result["formula"]["number"]["greater_than"] == 80


def test_build_filter_rollup():
    config = {"property": "TaskCount", "rollup": {"number": {"greater_than": 0}}}
    result = build_database_filter(config)
    assert result["property"] == "TaskCount"
    assert result["rollup"]["number"]["greater_than"] == 0


def test_build_filter_nested_compound():
    config = {
        "and": [
            {
                "or": [
                    {"property": "Status", "status": {"equals": "In progress"}},
                    {"property": "Status", "status": {"equals": "Not started"}},
                ]
            },
            {"property": "Priority", "select": {"equals": "High"}},
        ]
    }
    result = build_database_filter(config)
    assert "and" in result
    assert "or" in result["and"][0]


# ---------------------------------------------------------------------------
# extract_block_content tests
# ---------------------------------------------------------------------------


def test_extract_paragraph_block():
    block = {
        "id": "blk-1",
        "type": "paragraph",
        "has_children": False,
        "paragraph": {
            "rich_text": [{"plain_text": "Hello "}, {"plain_text": "world"}],
        },
    }
    result = extract_block_content(block)
    assert result["type"] == "paragraph"
    assert result["text"] == "Hello world"
    assert result["has_children"] is False


def test_extract_heading_block():
    block = {
        "id": "blk-2",
        "type": "heading_1",
        "has_children": False,
        "heading_1": {
            "rich_text": [{"plain_text": "Introduction"}],
        },
    }
    result = extract_block_content(block)
    assert result["type"] == "heading_1"
    assert result["text"] == "Introduction"


def test_extract_code_block():
    block = {
        "id": "blk-3",
        "type": "code",
        "has_children": False,
        "code": {
            "rich_text": [{"plain_text": "print('hello')"}],
            "language": "python",
        },
    }
    result = extract_block_content(block)
    assert result["type"] == "code"
    assert result["text"] == "print('hello')"
    assert result["language"] == "python"


def test_extract_equation_block():
    block = {
        "id": "blk-4",
        "type": "equation",
        "has_children": False,
        "equation": {"expression": "E = mc^2"},
    }
    result = extract_block_content(block)
    assert result["type"] == "equation"
    assert result["expression"] == "E = mc^2"


def test_extract_callout_block():
    block = {
        "id": "blk-5",
        "type": "callout",
        "has_children": False,
        "callout": {
            "rich_text": [{"plain_text": "Important note"}],
            "icon": {"type": "emoji", "emoji": "💡"},
        },
    }
    result = extract_block_content(block)
    assert result["type"] == "callout"
    assert result["text"] == "Important note"
    assert result["icon"] == "💡"


def test_extract_toggle_block():
    block = {
        "id": "blk-6",
        "type": "toggle",
        "has_children": True,
        "toggle": {
            "rich_text": [{"plain_text": "Click to expand"}],
        },
    }
    result = extract_block_content(block)
    assert result["type"] == "toggle"
    assert result["text"] == "Click to expand"
    assert result["has_children"] is True


def test_extract_todo_block():
    block = {
        "id": "blk-7",
        "type": "to_do",
        "has_children": False,
        "to_do": {
            "rich_text": [{"plain_text": "Buy groceries"}],
            "checked": True,
        },
    }
    result = extract_block_content(block)
    assert result["type"] == "to_do"
    assert result["text"] == "Buy groceries"
    assert result["checked"] is True


def test_extract_image_block():
    block = {
        "id": "blk-8",
        "type": "image",
        "has_children": False,
        "image": {
            "type": "external",
            "external": {"url": "https://example.com/img.png"},
            "caption": [{"plain_text": "A diagram"}],
        },
    }
    result = extract_block_content(block)
    assert result["type"] == "image"
    assert result["url"] == "https://example.com/img.png"
    assert result["caption"] == "A diagram"


def test_extract_divider_block():
    block = {"id": "blk-9", "type": "divider", "has_children": False}
    result = extract_block_content(block)
    assert result["type"] == "divider"


def test_extract_bulleted_list_block():
    block = {
        "id": "blk-10",
        "type": "bulleted_list_item",
        "has_children": False,
        "bulleted_list_item": {
            "rich_text": [{"plain_text": "Item one"}],
        },
    }
    result = extract_block_content(block)
    assert result["type"] == "bulleted_list_item"
    assert result["text"] == "Item one"


def test_extract_page_blocks_filters_non_dicts():
    blocks = [
        {
            "id": "blk-1",
            "type": "paragraph",
            "has_children": False,
            "paragraph": {"rich_text": [{"plain_text": "text"}]},
        },
        "not a dict",
        None,
    ]
    result = extract_page_blocks(blocks)
    assert len(result) == 1


# ---------------------------------------------------------------------------
# parse_notion_page_json tests
# ---------------------------------------------------------------------------


def test_parse_page_normalizes_source_brief():
    page = _page_payload()
    brief = parse_notion_page_json(page, database_id="db-001")

    assert brief["title"] == "Implement OAuth"
    assert brief["domain"] == "notion"
    assert brief["source_project"] == "notion"
    assert brief["source_entity_type"] == "page"
    assert brief["source_id"] == "db-001/page-001"
    assert "Notion page db-001/page-001" in brief["summary"]
    assert "Status: In progress" in brief["summary"]

    normalized = brief["source_payload"]["normalized"]
    assert normalized["page_id"] == "page-001"
    assert normalized["title"] == "Implement OAuth"
    assert normalized["status"] == "In progress"
    assert normalized["mapped_state"] == "in_progress"

    assert brief["source_links"]["html_url"] == "https://notion.so/page-001"


def test_parse_page_with_property_map():
    page = _page_payload(
        extra_properties={
            "Priority": {"type": "select", "select": {"name": "High"}},
        }
    )
    brief = parse_notion_page_json(
        page,
        database_id="db-001",
        property_map={"Priority": "priority"},
    )
    props = brief["source_payload"]["normalized"]["properties"]
    assert props["priority"] == "High"


def test_parse_page_status_done_maps_to_completed():
    page = _page_payload(status="Done")
    brief = parse_notion_page_json(page, database_id="db-001")
    assert brief["source_payload"]["normalized"]["mapped_state"] == "completed"


def test_parse_page_custom_status_map():
    page = _page_payload(status="Blocked")
    brief = parse_notion_page_json(
        page,
        database_id="db-001",
        status_map={"Blocked": "blocked"},
    )
    assert brief["source_payload"]["normalized"]["mapped_state"] == "blocked"


def test_parse_page_missing_id_raises():
    page = _page_payload()
    page["id"] = ""
    try:
        parse_notion_page_json(page, database_id="db-001")
        assert False, "Expected ValueError"
    except ValueError as exc:
        assert "id" in str(exc)


def test_parse_page_missing_title_raises():
    page = _page_payload(title="")
    try:
        parse_notion_page_json(page, database_id="db-001")
        assert False, "Expected ValueError"
    except ValueError as exc:
        assert "title" in str(exc)


# ---------------------------------------------------------------------------
# map_notion_page_to_task tests
# ---------------------------------------------------------------------------


def test_map_page_to_task_basic():
    page = _page_payload()
    task = map_notion_page_to_task(page, database_id="db-001", plan_id="plan-001")

    assert task["id"] == "notion-page-001"
    assert task["execution_plan_id"] == "plan-001"
    assert task["title"] == "Implement OAuth"
    assert task["status"] == "in_progress"
    assert task["metadata"]["notion_page_id"] == "page-001"
    assert task["metadata"]["notion_database_id"] == "db-001"
    assert task["metadata"]["notion_status"] == "In progress"


def test_map_page_to_task_with_assignee():
    page = _page_payload(
        extra_properties={
            "Assignee": {
                "type": "people",
                "people": [{"name": "Alice", "id": "user-1"}],
            },
        }
    )
    task = map_notion_page_to_task(page, database_id="db-001")
    assert task["owner_type"] == "Alice"


def test_map_page_to_task_with_relations():
    page = _page_payload(
        extra_properties={
            "Depends On": {
                "type": "relation",
                "relation": [{"id": "page-101"}, {"id": "page-102"}],
            },
        }
    )
    task = map_notion_page_to_task(page, database_id="db-001")
    assert "page-101" in task["depends_on"]
    assert "page-102" in task["depends_on"]


def test_map_page_to_task_no_assignee():
    page = _page_payload()
    task = map_notion_page_to_task(page, database_id="db-001")
    assert task["owner_type"] is None


# ---------------------------------------------------------------------------
# import_database_to_plan tests
# ---------------------------------------------------------------------------


def test_import_database_to_plan():
    pages = [
        _page_payload(page_id="p1", title="Task 1"),
        _page_payload(page_id="p2", title="Task 2", status="Done"),
    ]
    plan = import_database_to_plan(
        pages,
        database_id="db-001",
        database_title="Sprint Board",
    )

    assert plan["project_type"] == "notion"
    assert plan["metadata"]["database_id"] == "db-001"
    assert plan["metadata"]["database_title"] == "Sprint Board"
    assert plan["metadata"]["page_count"] == 2
    assert len(plan["tasks"]) == 2
    assert plan["tasks"][0]["title"] == "Task 1"
    assert plan["tasks"][1]["title"] == "Task 2"
    assert plan["tasks"][1]["status"] == "completed"
    assert plan["milestones"][0]["name"] == "Sprint Board"


def test_import_database_to_plan_no_title():
    plan = import_database_to_plan(
        [_page_payload()],
        database_id="db-001",
    )
    assert plan["milestones"][0]["name"] == "Notion Database"


# ---------------------------------------------------------------------------
# SyncState tests
# ---------------------------------------------------------------------------


def test_sync_state_get_set():
    state = SyncState()
    assert state.get_last_synced("db-001") is None

    state.update("db-001", "2026-05-01T00:00:00Z")
    assert state.get_last_synced("db-001") == "2026-05-01T00:00:00Z"


def test_sync_state_roundtrip():
    state = SyncState()
    state.update("db-001", "2026-05-01T00:00:00Z")
    state.update("db-002", "2026-05-02T00:00:00Z")

    data = state.to_dict()
    restored = SyncState.from_dict(data)
    assert restored.get_last_synced("db-001") == "2026-05-01T00:00:00Z"
    assert restored.get_last_synced("db-002") == "2026-05-02T00:00:00Z"


def test_sync_state_from_empty_dict():
    state = SyncState.from_dict({})
    assert state.get_last_synced("any") is None


# ---------------------------------------------------------------------------
# NotionClient tests
# ---------------------------------------------------------------------------


def test_client_query_database():
    seen = {}

    def fake_open(request, timeout):
        seen["url"] = request.full_url
        seen["method"] = request.method
        seen["body"] = json.loads(request.data.decode("utf-8"))
        seen["auth"] = request.headers.get("Authorization")
        seen["version"] = request.headers.get("Notion-version")
        return _Response({
            "results": [_page_payload()],
            "has_more": False,
            "next_cursor": None,
        })

    client = NotionClient(token="test-token", http_open=fake_open)
    result = client.query_database("db-001")

    assert seen["url"] == "https://api.notion.com/v1/databases/db-001/query"
    assert seen["method"] == "POST"
    assert seen["auth"] == "Bearer test-token"
    assert seen["version"] == "2022-06-28"
    assert seen["body"]["page_size"] == 100
    assert len(result["results"]) == 1


def test_client_query_database_with_filter():
    seen = {}

    def fake_open(request, timeout):
        seen["body"] = json.loads(request.data.decode("utf-8"))
        return _Response({"results": [], "has_more": False})

    client = NotionClient(token="t", http_open=fake_open)
    client.query_database(
        "db-001",
        filter_obj={"property": "Status", "status": {"equals": "Done"}},
        sorts=[{"property": "Name", "direction": "ascending"}],
    )

    assert seen["body"]["filter"]["property"] == "Status"
    assert seen["body"]["sorts"][0]["property"] == "Name"


def test_client_get_database():
    seen = {}

    def fake_open(request, timeout):
        seen["url"] = request.full_url
        seen["method"] = request.method
        return _Response(_database_meta())

    client = NotionClient(token="t", http_open=fake_open)
    result = client.get_database("db-001")

    assert seen["url"] == "https://api.notion.com/v1/databases/db-001"
    assert seen["method"] == "GET"
    assert result["id"] == "db-001"


def test_client_get_page():
    seen = {}

    def fake_open(request, timeout):
        seen["url"] = request.full_url
        return _Response(_page_payload())

    client = NotionClient(token="t", http_open=fake_open)
    result = client.get_page("page-001")

    assert seen["url"] == "https://api.notion.com/v1/pages/page-001"
    assert result["id"] == "page-001"


def test_client_get_block_children():
    seen = {}

    def fake_open(request, timeout):
        seen["url"] = request.full_url
        return _Response({
            "results": [
                {"id": "blk-1", "type": "paragraph", "has_children": False,
                 "paragraph": {"rich_text": [{"plain_text": "text"}]}},
            ],
            "has_more": False,
        })

    client = NotionClient(token="t", http_open=fake_open)
    result = client.get_block_children("page-001")

    assert "blocks/page-001/children" in seen["url"]
    assert len(result["results"]) == 1


def test_client_http_error_raises_import_error():
    from urllib.error import HTTPError

    def fake_open(request, timeout):
        raise HTTPError(request.full_url, 404, "Not Found", {}, None)

    client = NotionClient(token="t", http_open=fake_open)
    try:
        client.request_json("GET", "pages/missing")
        assert False, "Expected ImportError"
    except ImportError as exc:
        assert "404" in str(exc)


# ---------------------------------------------------------------------------
# NotionAdvancedImporter tests
# ---------------------------------------------------------------------------


def test_importer_import_from_source(monkeypatch):
    def fake_open(request, timeout):
        return _Response(_page_payload())

    monkeypatch.setenv("NOTION_TEST", "test-token")
    importer = NotionAdvancedImporter(
        token_env="NOTION_TEST",
        default_database_id="db-001",
        http_open=fake_open,
    )

    brief = importer.import_from_source("page-001")
    assert brief["source_id"] == "db-001/page-001"
    assert brief["title"] == "Implement OAuth"


def test_importer_import_from_source_full_id(monkeypatch):
    def fake_open(request, timeout):
        return _Response(_page_payload())

    monkeypatch.setenv("NOTION_TEST", "t")
    importer = NotionAdvancedImporter(
        token_env="NOTION_TEST",
        http_open=fake_open,
    )

    brief = importer.import_from_source("db-001/page-001")
    assert brief["source_id"] == "db-001/page-001"


def test_importer_validate_source_true(monkeypatch):
    def fake_open(request, timeout):
        return _Response(_page_payload())

    monkeypatch.setenv("NOTION_TEST", "t")
    importer = NotionAdvancedImporter(
        token_env="NOTION_TEST",
        default_database_id="db-001",
        http_open=fake_open,
    )
    assert importer.validate_source("page-001") is True


def test_importer_validate_source_false(monkeypatch):
    from urllib.error import HTTPError

    def fake_open(request, timeout):
        raise HTTPError(request.full_url, 404, "Not Found", {}, None)

    monkeypatch.setenv("NOTION_TEST", "t")
    importer = NotionAdvancedImporter(
        token_env="NOTION_TEST",
        default_database_id="db-001",
        http_open=fake_open,
    )
    assert importer.validate_source("missing") is False


def test_importer_list_available(monkeypatch):
    def fake_open(request, timeout):
        return _Response({
            "results": [
                _page_payload(page_id="p1", title="Task 1"),
                _page_payload(page_id="p2", title="Task 2"),
            ],
            "has_more": False,
        })

    monkeypatch.setenv("NOTION_TEST", "t")
    importer = NotionAdvancedImporter(
        token_env="NOTION_TEST",
        default_database_id="db-001",
        http_open=fake_open,
    )

    items = importer.list_available(limit=10)
    assert len(items) == 2
    assert items[0]["id"] == "db-001/p1"
    assert items[0]["title"] == "Task 1"
    assert items[0]["status"] == "In progress"


def test_importer_list_available_requires_database():
    importer = NotionAdvancedImporter(http_open=lambda r, t: None)
    try:
        importer.list_available()
        assert False, "Expected ValueError"
    except ValueError as exc:
        assert "default_database_id" in str(exc)


def test_importer_list_available_with_filter(monkeypatch):
    seen = {}

    def fake_open(request, timeout):
        seen["body"] = json.loads(request.data.decode("utf-8"))
        return _Response({"results": [], "has_more": False})

    monkeypatch.setenv("NOTION_TEST", "t")
    importer = NotionAdvancedImporter(
        token_env="NOTION_TEST",
        default_database_id="db-001",
        filter_config={"property": "Status", "status": {"equals": "In progress"}},
        http_open=fake_open,
    )

    importer.list_available()
    assert seen["body"]["filter"]["property"] == "Status"


def test_importer_import_database(monkeypatch):
    call_count = {"n": 0}

    def fake_open(request, timeout):
        call_count["n"] += 1
        url = request.full_url
        if "databases/db-001/query" in url:
            return _Response({
                "results": [
                    _page_payload(page_id="p1", title="Task 1"),
                    _page_payload(page_id="p2", title="Task 2", status="Done"),
                ],
                "has_more": False,
            })
        elif "databases/db-001" in url:
            return _Response(_database_meta())
        return _Response({})

    monkeypatch.setenv("NOTION_TEST", "t")
    importer = NotionAdvancedImporter(
        token_env="NOTION_TEST",
        default_database_id="db-001",
        http_open=fake_open,
    )

    plan = importer.import_database()
    assert plan["project_type"] == "notion"
    assert plan["metadata"]["database_title"] == "Sprint Board"
    assert len(plan["tasks"]) == 2
    assert plan["tasks"][0]["title"] == "Task 1"
    assert plan["tasks"][1]["status"] == "completed"


def test_importer_import_database_requires_id():
    importer = NotionAdvancedImporter(http_open=lambda r, t: None)
    try:
        importer.import_database()
        assert False, "Expected ValueError"
    except ValueError as exc:
        assert "database_id" in str(exc)


def test_importer_fetch_page_blocks(monkeypatch):
    def fake_open(request, timeout):
        return _Response({
            "results": [
                {
                    "id": "blk-1",
                    "type": "paragraph",
                    "has_children": False,
                    "paragraph": {"rich_text": [{"plain_text": "Hello"}]},
                },
                {
                    "id": "blk-2",
                    "type": "code",
                    "has_children": False,
                    "code": {
                        "rich_text": [{"plain_text": "x = 1"}],
                        "language": "python",
                    },
                },
            ],
            "has_more": False,
        })

    monkeypatch.setenv("NOTION_TEST", "t")
    importer = NotionAdvancedImporter(
        token_env="NOTION_TEST",
        http_open=fake_open,
    )

    blocks = importer.fetch_page_blocks("page-001")
    assert len(blocks) == 2
    assert blocks[0]["type"] == "paragraph"
    assert blocks[0]["text"] == "Hello"
    assert blocks[1]["type"] == "code"
    assert blocks[1]["language"] == "python"


# ---------------------------------------------------------------------------
# Incremental sync tests
# ---------------------------------------------------------------------------


def test_importer_sync_updates_first_time(monkeypatch):
    def fake_open(request, timeout):
        return _Response({
            "results": [_page_payload(page_id="p1", title="Updated Task")],
            "has_more": False,
        })

    monkeypatch.setenv("NOTION_TEST", "t")
    sync_state = SyncState()
    importer = NotionAdvancedImporter(
        token_env="NOTION_TEST",
        default_database_id="db-001",
        sync_state=sync_state,
        http_open=fake_open,
    )

    tasks = importer.sync_updates()
    assert len(tasks) == 1
    assert tasks[0]["title"] == "Updated Task"
    assert sync_state.get_last_synced("db-001") is not None


def test_importer_sync_updates_incremental(monkeypatch):
    seen = {}

    def fake_open(request, timeout):
        if request.data:
            seen["body"] = json.loads(request.data.decode("utf-8"))
        return _Response({
            "results": [_page_payload(page_id="p2", title="New Update")],
            "has_more": False,
        })

    monkeypatch.setenv("NOTION_TEST", "t")
    sync_state = SyncState()
    sync_state.update("db-001", "2026-05-01T00:00:00Z")

    importer = NotionAdvancedImporter(
        token_env="NOTION_TEST",
        default_database_id="db-001",
        sync_state=sync_state,
        http_open=fake_open,
    )

    tasks = importer.sync_updates()
    assert len(tasks) == 1

    # Verify the filter includes the last_edited_time constraint
    body_filter = seen["body"]["filter"]
    assert body_filter["property"] == "last_edited_time"
    assert body_filter["last_edited_time"]["after"] == "2026-05-01T00:00:00Z"


def test_importer_sync_updates_incremental_with_base_filter(monkeypatch):
    seen = {}

    def fake_open(request, timeout):
        if request.data:
            seen["body"] = json.loads(request.data.decode("utf-8"))
        return _Response({"results": [], "has_more": False})

    monkeypatch.setenv("NOTION_TEST", "t")
    sync_state = SyncState()
    sync_state.update("db-001", "2026-05-01T00:00:00Z")

    importer = NotionAdvancedImporter(
        token_env="NOTION_TEST",
        default_database_id="db-001",
        sync_state=sync_state,
        filter_config={"property": "Priority", "select": {"equals": "High"}},
        http_open=fake_open,
    )

    importer.sync_updates()

    # Should combine base filter with sync filter using 'and'
    body_filter = seen["body"]["filter"]
    assert "and" in body_filter
    assert len(body_filter["and"]) == 2


def test_importer_sync_requires_database():
    importer = NotionAdvancedImporter(http_open=lambda r, t: None)
    try:
        importer.sync_updates()
        assert False, "Expected ValueError"
    except ValueError as exc:
        assert "database_id" in str(exc)


# ---------------------------------------------------------------------------
# Pagination tests
# ---------------------------------------------------------------------------


def test_importer_import_database_handles_pagination(monkeypatch):
    call_count = {"n": 0}

    def fake_open(request, timeout):
        call_count["n"] += 1
        url = request.full_url

        if "databases/db-001/query" in url:
            body = json.loads(request.data.decode("utf-8")) if request.data else {}
            if body.get("start_cursor") == "cursor-page-2":
                return _Response({
                    "results": [_page_payload(page_id="p2", title="Task 2")],
                    "has_more": False,
                })
            return _Response({
                "results": [_page_payload(page_id="p1", title="Task 1")],
                "has_more": True,
                "next_cursor": "cursor-page-2",
            })
        elif "databases/db-001" in url:
            return _Response(_database_meta())

        return _Response({})

    monkeypatch.setenv("NOTION_TEST", "t")
    importer = NotionAdvancedImporter(
        token_env="NOTION_TEST",
        default_database_id="db-001",
        http_open=fake_open,
    )

    plan = importer.import_database()
    assert len(plan["tasks"]) == 2
    assert plan["tasks"][0]["title"] == "Task 1"
    assert plan["tasks"][1]["title"] == "Task 2"


# ---------------------------------------------------------------------------
# Source ID parsing edge cases
# ---------------------------------------------------------------------------


def test_source_id_parsing_no_default():
    importer = NotionAdvancedImporter(http_open=lambda r, t: None)
    try:
        importer._parse_source_id("page-only")
        assert False, "Expected ValueError"
    except ValueError as exc:
        assert "default_database_id" in str(exc)


def test_source_id_parsing_too_many_parts():
    importer = NotionAdvancedImporter(http_open=lambda r, t: None)
    try:
        importer._parse_source_id("a/b/c")
        assert False, "Expected ValueError"
    except ValueError as exc:
        assert "Invalid" in str(exc)


# ---------------------------------------------------------------------------
# Edge cases: circular relations, complex formulas, large databases
# ---------------------------------------------------------------------------


def test_circular_relations():
    """Pages that reference each other should not cause issues."""
    page_a = _page_payload(
        page_id="page-a",
        title="Task A",
        extra_properties={
            "Depends On": {
                "type": "relation",
                "relation": [{"id": "page-b"}],
            },
        },
    )
    page_b = _page_payload(
        page_id="page-b",
        title="Task B",
        extra_properties={
            "Depends On": {
                "type": "relation",
                "relation": [{"id": "page-a"}],
            },
        },
    )

    plan = import_database_to_plan(
        [page_a, page_b],
        database_id="db-001",
    )

    assert len(plan["tasks"]) == 2
    assert "page-b" in plan["tasks"][0]["depends_on"]
    assert "page-a" in plan["tasks"][1]["depends_on"]


def test_complex_formula_property():
    """Formula returning different types should extract correctly."""
    page = _page_payload(
        extra_properties={
            "Score": {"type": "formula", "formula": {"type": "number", "number": 95.5}},
            "Label": {"type": "formula", "formula": {"type": "string", "string": "Critical"}},
            "Active": {"type": "formula", "formula": {"type": "boolean", "boolean": False}},
        }
    )

    brief = parse_notion_page_json(page, database_id="db-001")
    props = brief["source_payload"]["normalized"]["properties"]
    assert props["Score"] == 95.5
    assert props["Label"] == "Critical"
    assert props["Active"] is False


def test_empty_database_returns_empty_plan():
    plan = import_database_to_plan([], database_id="db-001", database_title="Empty")
    assert len(plan["tasks"]) == 0
    assert plan["metadata"]["page_count"] == 0


def test_page_with_all_property_types():
    """Verify a page with many property types extracts without error."""
    page = _page_payload(
        extra_properties={
            "Priority": {"type": "select", "select": {"name": "High"}},
            "Tags": {"type": "multi_select", "multi_select": [{"name": "backend"}, {"name": "api"}]},
            "Due Date": {"type": "date", "date": {"start": "2026-06-01", "end": None}},
            "Assignee": {"type": "people", "people": [{"name": "Alice", "id": "u1"}]},
            "Done": {"type": "checkbox", "checkbox": True},
            "Link": {"type": "url", "url": "https://example.com"},
            "Contact": {"type": "email", "email": "dev@example.com"},
            "Phone": {"type": "phone_number", "phone_number": "+1-555-0100"},
            "Score": {"type": "formula", "formula": {"type": "number", "number": 85}},
            "Related": {"type": "relation", "relation": [{"id": "page-x"}]},
            "Sum": {"type": "rollup", "rollup": {"type": "number", "number": 42}},
            "ID": {"type": "unique_id", "unique_id": {"prefix": "TSK", "number": 7}},
            "Created": {"type": "created_time", "created_time": "2026-05-01T00:00:00.000Z"},
            "Created By": {"type": "created_by", "created_by": {"name": "System", "id": "sys"}},
            "Edited": {"type": "last_edited_time", "last_edited_time": "2026-05-02T00:00:00.000Z"},
            "Edited By": {"type": "last_edited_by", "last_edited_by": {"name": "Alice", "id": "u1"}},
            "Attachment": {
                "type": "files",
                "files": [{"name": "spec.pdf", "type": "external", "external": {"url": "https://example.com/spec.pdf"}}],
            },
        }
    )

    brief = parse_notion_page_json(page, database_id="db-001")
    props = brief["source_payload"]["normalized"]["properties"]

    assert props["Priority"] == "High"
    assert props["Tags"] == ["backend", "api"]
    assert props["Due Date"] == {"start": "2026-06-01", "end": None}
    assert props["Assignee"] == ["Alice"]
    assert props["Done"] is True
    assert props["Link"] == "https://example.com"
    assert props["Contact"] == "dev@example.com"
    assert props["Phone"] == "+1-555-0100"
    assert props["Score"] == 85
    assert props["Related"] == ["page-x"]
    assert props["Sum"] == 42
    assert props["ID"] == "TSK-7"
    assert props["Created"] == "2026-05-01T00:00:00.000Z"
    assert props["Created By"] == "System"
    assert props["Edited"] == "2026-05-02T00:00:00.000Z"
    assert props["Edited By"] == "Alice"
    assert props["Attachment"][0]["name"] == "spec.pdf"


# ---------------------------------------------------------------------------
# Status mapping defaults
# ---------------------------------------------------------------------------


def test_default_status_map():
    assert DEFAULT_STATUS_MAP["Not started"] == "pending"
    assert DEFAULT_STATUS_MAP["In progress"] == "in_progress"
    assert DEFAULT_STATUS_MAP["Done"] == "completed"
    assert DEFAULT_STATUS_MAP["Archived"] == "skipped"
