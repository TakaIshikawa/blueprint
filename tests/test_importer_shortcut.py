"""Tests for Shortcut story importer."""

import json
import pytest
from unittest.mock import Mock
from urllib.error import HTTPError

from blueprint.importers.shortcut import (
    ShortcutImporter,
    ShortcutStoryRef,
    parse_story_ref,
    parse_shortcut_story_json,
)


def test_parse_story_ref_with_plain_id():
    ref = parse_story_ref("12345")
    assert ref.story_id == 12345
    assert ref.source_id == "shortcut-12345"


def test_parse_story_ref_with_prefix():
    ref = parse_story_ref("shortcut-98765")
    assert ref.story_id == 98765
    assert ref.source_id == "shortcut-98765"


def test_parse_story_ref_invalid_id_raises_error():
    with pytest.raises(ValueError, match="Invalid Shortcut story ID"):
        parse_story_ref("not-a-number")


def test_parse_story_ref_negative_id_raises_error():
    with pytest.raises(ValueError, match="Invalid Shortcut story ID"):
        parse_story_ref("-123")


def test_parse_shortcut_story_json_extracts_core_fields():
    story = {
        "id": 54321,
        "name": "Implement user authentication",
        "description": "Add OAuth2 authentication flow for users",
        "story_type": "feature",
        "workflow_state_id": 500123456,
        "estimate": 5,
        "labels": [
            {"id": 1, "name": "backend"},
            {"id": 2, "name": "security"},
        ],
        "owners": [
            {"id": "user-123", "profile": {"name": "Jane Doe"}},
        ],
        "epic_id": 999,
        "iteration_id": 888,
        "project_id": 777,
        "blocked": False,
        "blocker": False,
        "tasks": [
            {"description": "Design auth flow", "complete": True},
            {"description": "Implement OAuth provider", "complete": False},
        ],
        "comments": [
            {"text": "LGTM", "author_id": "user-456"},
        ],
        "app_url": "https://app.shortcut.com/workspace/story/54321",
    }

    result = parse_shortcut_story_json(story)

    assert result["title"] == "Implement user authentication"
    assert result["domain"] == "shortcut"
    assert result["source_project"] == "shortcut"
    assert result["source_entity_type"] == "feature"
    assert result["source_id"] == "shortcut-54321"
    assert result["source_payload"]["normalized"]["story_id"] == 54321
    assert result["source_payload"]["normalized"]["story_type"] == "feature"
    assert result["source_payload"]["normalized"]["estimate"] == 5
    assert result["source_payload"]["normalized"]["labels"] == ["backend", "security"]
    assert result["source_payload"]["normalized"]["owners"] == ["user-123"]
    assert result["source_payload"]["normalized"]["epic_id"] == 999
    assert result["source_payload"]["normalized"]["iteration_id"] == 888
    assert result["source_payload"]["normalized"]["project_id"] == 777
    assert result["source_payload"]["normalized"]["blocked"] is False
    assert result["source_payload"]["normalized"]["blocker"] is False
    assert len(result["source_payload"]["normalized"]["tasks"]) == 2
    assert result["source_payload"]["normalized"]["comment_count"] == 1
    assert result["source_links"]["app_url"] == "https://app.shortcut.com/workspace/story/54321"


def test_parse_shortcut_story_json_handles_different_story_types():
    for story_type in ["feature", "bug", "chore"]:
        story = {
            "id": 111,
            "name": f"Test {story_type}",
            "story_type": story_type,
            "workflow_state_id": 500123456,
        }
        result = parse_shortcut_story_json(story)
        assert result["source_entity_type"] == story_type


def test_parse_shortcut_story_json_missing_required_field_raises_error():
    with pytest.raises(ValueError, match="Missing or empty required field: name"):
        parse_shortcut_story_json({"id": 123})

    with pytest.raises(ValueError, match="Missing or invalid required field: id"):
        parse_shortcut_story_json({"name": "Test story"})


def test_shortcut_importer_import_from_source_success(monkeypatch):
    story_json = {
        "id": 12345,
        "name": "Test Story",
        "description": "Test description",
        "story_type": "feature",
        "workflow_state_id": 500123456,
        "labels": [],
        "owners": [],
    }

    mock_response = Mock()
    mock_response.read.return_value = json.dumps(story_json).encode("utf-8")

    def mock_http_open(request):
        # Headers are normalized to lowercase in urllib.request.Request
        assert "Shortcut-token" in request.headers or "Shortcut-Token" in request.headers
        assert "/stories/12345" in request.full_url
        return mock_response

    monkeypatch.setenv("SHORTCUT_API_TOKEN", "test-token-12345")
    importer = ShortcutImporter(http_open=mock_http_open)

    result = importer.import_from_source("12345")

    assert result["title"] == "Test Story"
    assert result["source_id"] == "shortcut-12345"
    assert result["domain"] == "shortcut"


def test_shortcut_importer_import_from_source_missing_token():
    importer = ShortcutImporter()

    with pytest.raises(ImportError, match="Missing Shortcut API token"):
        importer.import_from_source("12345")


def test_shortcut_importer_import_from_source_not_found(monkeypatch):
    def mock_http_open(request):
        raise HTTPError(request.full_url, 404, "Not Found", {}, None)

    monkeypatch.setenv("SHORTCUT_API_TOKEN", "test-token")
    importer = ShortcutImporter(http_open=mock_http_open)

    with pytest.raises(ImportError, match="Shortcut story not found"):
        importer.import_from_source("99999")


def test_shortcut_importer_validate_source_returns_true_when_exists(monkeypatch):
    story_json = {
        "id": 12345,
        "name": "Test Story",
        "story_type": "feature",
        "workflow_state_id": 500123456,
    }

    mock_response = Mock()
    mock_response.read.return_value = json.dumps(story_json).encode("utf-8")

    monkeypatch.setenv("SHORTCUT_API_TOKEN", "test-token")
    importer = ShortcutImporter(http_open=lambda req: mock_response)

    assert importer.validate_source("12345") is True


def test_shortcut_importer_validate_source_returns_false_when_not_found(monkeypatch):
    def mock_http_open(request):
        raise HTTPError(request.full_url, 404, "Not Found", {}, None)

    monkeypatch.setenv("SHORTCUT_API_TOKEN", "test-token")
    importer = ShortcutImporter(http_open=mock_http_open)

    assert importer.validate_source("99999") is False


def test_shortcut_importer_list_available_returns_stories(monkeypatch):
    stories_json = {
        "data": [
            {
                "id": 111,
                "name": "Story 1",
                "story_type": "feature",
                "workflow_state_id": 500123456,
                "epic_id": 1,
                "iteration_id": 2,
                "project_id": 3,
            },
            {
                "id": 222,
                "name": "Story 2",
                "story_type": "bug",
                "workflow_state_id": 500123457,
                "project_id": 3,
            },
        ]
    }

    mock_response = Mock()
    mock_response.read.return_value = json.dumps(stories_json).encode("utf-8")

    def mock_http_open(request):
        assert "/search/stories" in request.full_url
        assert "page_size=50" in request.full_url
        return mock_response

    monkeypatch.setenv("SHORTCUT_API_TOKEN", "test-token")
    importer = ShortcutImporter(http_open=mock_http_open)

    stories = importer.list_available(limit=50)

    assert len(stories) == 2
    assert stories[0]["id"] == "shortcut-111"
    assert stories[0]["title"] == "Story 1"
    assert stories[0]["metadata"]["story_type"] == "feature"
    assert stories[1]["id"] == "shortcut-222"
    assert stories[1]["title"] == "Story 2"


def test_shortcut_importer_list_by_project_returns_project_stories(monkeypatch):
    stories_json = [
        {
            "id": 333,
            "name": "Project Story 1",
            "story_type": "feature",
            "workflow_state_id": 500123456,
            "project_id": 777,
        },
        {
            "id": 444,
            "name": "Project Story 2",
            "story_type": "chore",
            "workflow_state_id": 500123457,
            "project_id": 777,
        },
    ]

    mock_response = Mock()
    mock_response.read.return_value = json.dumps(stories_json).encode("utf-8")

    def mock_http_open(request):
        assert "/projects/777/stories" in request.full_url
        return mock_response

    monkeypatch.setenv("SHORTCUT_API_TOKEN", "test-token")
    importer = ShortcutImporter(http_open=mock_http_open)

    stories = importer.list_by_project(777)

    assert len(stories) == 2
    assert stories[0]["metadata"]["project_id"] == 777
    assert stories[1]["metadata"]["project_id"] == 777


def test_shortcut_importer_list_by_epic_returns_epic_stories(monkeypatch):
    stories_json = [
        {
            "id": 555,
            "name": "Epic Story 1",
            "story_type": "feature",
            "workflow_state_id": 500123456,
            "epic_id": 999,
        },
    ]

    mock_response = Mock()
    mock_response.read.return_value = json.dumps(stories_json).encode("utf-8")

    def mock_http_open(request):
        assert "/epics/999/stories" in request.full_url
        return mock_response

    monkeypatch.setenv("SHORTCUT_API_TOKEN", "test-token")
    importer = ShortcutImporter(http_open=mock_http_open)

    stories = importer.list_by_epic(999)

    assert len(stories) == 1
    assert stories[0]["metadata"]["epic_id"] == 999


def test_shortcut_importer_list_by_iteration_returns_iteration_stories(monkeypatch):
    stories_json = [
        {
            "id": 666,
            "name": "Iteration Story 1",
            "story_type": "feature",
            "workflow_state_id": 500123456,
            "iteration_id": 888,
        },
    ]

    mock_response = Mock()
    mock_response.read.return_value = json.dumps(stories_json).encode("utf-8")

    def mock_http_open(request):
        assert "/iterations/888/stories" in request.full_url
        return mock_response

    monkeypatch.setenv("SHORTCUT_API_TOKEN", "test-token")
    importer = ShortcutImporter(http_open=mock_http_open)

    stories = importer.list_by_iteration(888)

    assert len(stories) == 1
    assert stories[0]["metadata"]["iteration_id"] == 888


def test_shortcut_importer_handles_blocked_and_blocker_stories():
    blocked_story = {
        "id": 777,
        "name": "Blocked Story",
        "story_type": "feature",
        "workflow_state_id": 500123456,
        "blocked": True,
        "blocker": False,
    }

    result = parse_shortcut_story_json(blocked_story)
    assert result["source_payload"]["normalized"]["blocked"] is True
    assert result["source_payload"]["normalized"]["blocker"] is False

    blocker_story = {
        "id": 888,
        "name": "Blocker Story",
        "story_type": "bug",
        "workflow_state_id": 500123456,
        "blocked": False,
        "blocker": True,
    }

    result = parse_shortcut_story_json(blocker_story)
    assert result["source_payload"]["normalized"]["blocked"] is False
    assert result["source_payload"]["normalized"]["blocker"] is True
