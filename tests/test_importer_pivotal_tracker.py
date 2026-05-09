"""Tests for Pivotal Tracker story importer."""

import json
import pytest
from unittest.mock import Mock
from urllib.error import HTTPError

from blueprint.importers.pivotal_tracker import (
    PivotalTrackerImporter,
    PivotalTrackerStoryRef,
    parse_story_ref,
    parse_pivotal_tracker_story_json,
)


def test_parse_story_ref_with_project_and_story():
    ref = parse_story_ref("123456-789")
    assert ref.project_id == 123456
    assert ref.story_id == 789
    assert ref.source_id == "pivotal-123456-789"


def test_parse_story_ref_with_prefix():
    ref = parse_story_ref("pivotal-999-888")
    assert ref.project_id == 999
    assert ref.story_id == 888
    assert ref.source_id == "pivotal-999-888"


def test_parse_story_ref_with_default_project():
    ref = parse_story_ref("555", default_project_id=777)
    assert ref.project_id == 777
    assert ref.story_id == 555
    assert ref.source_id == "pivotal-777-555"


def test_parse_story_ref_without_project_raises_error():
    with pytest.raises(ValueError, match="requires project ID"):
        parse_story_ref("12345")


def test_parse_story_ref_invalid_format_raises_error():
    with pytest.raises(ValueError, match="Invalid Pivotal Tracker story"):
        parse_story_ref("abc-def")


def test_parse_pivotal_tracker_story_json_extracts_core_fields():
    story = {
        "id": 98765,
        "project_id": 12345,
        "name": "Implement payment processing",
        "description": "Add Stripe integration for payment processing",
        "story_type": "feature",
        "current_state": "started",
        "estimate": 3,
        "labels": [
            {"id": 1, "name": "backend"},
            {"id": 2, "name": "payments"},
        ],
        "owner_ids": [111, 222],
        "blockers": [
            {"description": "Waiting for API keys", "resolved": False},
        ],
        "tasks": [
            {"description": "Set up Stripe account", "complete": True},
            {"description": "Implement webhook handler", "complete": False},
        ],
        "comments": [
            {"text": "Ready for review", "person_id": 333},
        ],
        "url": "https://www.pivotaltracker.com/story/show/98765",
    }

    result = parse_pivotal_tracker_story_json(story)

    assert result["title"] == "Implement payment processing"
    assert result["domain"] == "pivotal_tracker"
    assert result["source_project"] == "pivotal_tracker"
    assert result["source_entity_type"] == "feature"
    assert result["source_id"] == "pivotal-12345-98765"
    assert result["source_payload"]["normalized"]["project_id"] == 12345
    assert result["source_payload"]["normalized"]["story_id"] == 98765
    assert result["source_payload"]["normalized"]["story_type"] == "feature"
    assert result["source_payload"]["normalized"]["current_state"] == "started"
    assert result["source_payload"]["normalized"]["estimate"] == 3
    assert result["source_payload"]["normalized"]["labels"] == ["backend", "payments"]
    assert result["source_payload"]["normalized"]["owner_ids"] == [111, 222]
    assert len(result["source_payload"]["normalized"]["blockers"]) == 1
    assert result["source_payload"]["normalized"]["blockers"][0]["resolved"] is False
    assert len(result["source_payload"]["normalized"]["tasks"]) == 2
    assert result["source_payload"]["normalized"]["comment_count"] == 1
    assert result["source_links"]["url"] == "https://www.pivotaltracker.com/story/show/98765"


def test_parse_pivotal_tracker_story_json_handles_all_story_types():
    for story_type in ["feature", "bug", "chore", "release"]:
        story = {
            "id": 111,
            "project_id": 222,
            "name": f"Test {story_type}",
            "story_type": story_type,
            "current_state": "unstarted",
        }
        result = parse_pivotal_tracker_story_json(story)
        assert result["source_entity_type"] == story_type


def test_parse_pivotal_tracker_story_json_missing_project_id_raises_error():
    with pytest.raises(ValueError, match="must include project_id"):
        parse_pivotal_tracker_story_json({"id": 123, "name": "Test"})


def test_parse_pivotal_tracker_story_json_missing_required_fields_raises_error():
    with pytest.raises(ValueError, match="Missing or empty required field: name"):
        parse_pivotal_tracker_story_json({"id": 123, "project_id": 456})

    with pytest.raises(ValueError, match="Missing or invalid required field: id"):
        parse_pivotal_tracker_story_json({"name": "Test story", "project_id": 456})


def test_pivotal_tracker_importer_import_from_source_success(monkeypatch):
    story_json = {
        "id": 54321,
        "project_id": 12345,
        "name": "Test Story",
        "description": "Test description",
        "story_type": "feature",
        "current_state": "started",
        "labels": [],
        "owner_ids": [],
    }

    mock_response = Mock()
    mock_response.read.return_value = json.dumps(story_json).encode("utf-8")

    def mock_http_open(request):
        # Headers are normalized in urllib.request.Request
        assert "X-trackertoken" in request.headers or "X-TrackerToken" in request.headers
        assert "/projects/12345/stories/54321" in request.full_url
        return mock_response

    monkeypatch.setenv("PIVOTAL_TRACKER_API_TOKEN", "test-token-xyz")
    importer = PivotalTrackerImporter(http_open=mock_http_open)

    result = importer.import_from_source("12345-54321")

    assert result["title"] == "Test Story"
    assert result["source_id"] == "pivotal-12345-54321"
    assert result["domain"] == "pivotal_tracker"


def test_pivotal_tracker_importer_import_with_default_project(monkeypatch):
    story_json = {
        "id": 999,
        "project_id": 777,
        "name": "Story with default project",
        "story_type": "bug",
        "current_state": "finished",
    }

    mock_response = Mock()
    mock_response.read.return_value = json.dumps(story_json).encode("utf-8")

    monkeypatch.setenv("PIVOTAL_TRACKER_API_TOKEN", "test-token")
    importer = PivotalTrackerImporter(http_open=lambda req: mock_response, project_id=777)

    result = importer.import_from_source("999")

    assert result["source_id"] == "pivotal-777-999"


def test_pivotal_tracker_importer_import_missing_token():
    importer = PivotalTrackerImporter()

    with pytest.raises(ImportError, match="Missing Pivotal Tracker API token"):
        importer.import_from_source("12345-67890")


def test_pivotal_tracker_importer_import_not_found(monkeypatch):
    def mock_http_open(request):
        raise HTTPError(request.full_url, 404, "Not Found", {}, None)

    monkeypatch.setenv("PIVOTAL_TRACKER_API_TOKEN", "test-token")
    importer = PivotalTrackerImporter(http_open=mock_http_open)

    with pytest.raises(ImportError, match="Pivotal Tracker story not found"):
        importer.import_from_source("999-888")


def test_pivotal_tracker_importer_validate_source_returns_true_when_exists(monkeypatch):
    story_json = {
        "id": 123,
        "project_id": 456,
        "name": "Valid Story",
        "story_type": "feature",
        "current_state": "started",
    }

    mock_response = Mock()
    mock_response.read.return_value = json.dumps(story_json).encode("utf-8")

    monkeypatch.setenv("PIVOTAL_TRACKER_API_TOKEN", "test-token")
    importer = PivotalTrackerImporter(http_open=lambda req: mock_response)

    assert importer.validate_source("456-123") is True


def test_pivotal_tracker_importer_validate_source_returns_false_when_not_found(monkeypatch):
    def mock_http_open(request):
        raise HTTPError(request.full_url, 404, "Not Found", {}, None)

    monkeypatch.setenv("PIVOTAL_TRACKER_API_TOKEN", "test-token")
    importer = PivotalTrackerImporter(http_open=mock_http_open)

    assert importer.validate_source("999-888") is False


def test_pivotal_tracker_importer_list_by_project_returns_stories(monkeypatch):
    stories_json = [
        {
            "id": 111,
            "name": "Story 1",
            "story_type": "feature",
            "current_state": "started",
            "estimate": 2,
        },
        {
            "id": 222,
            "name": "Story 2",
            "story_type": "bug",
            "current_state": "finished",
        },
    ]

    mock_response = Mock()
    mock_response.read.return_value = json.dumps(stories_json).encode("utf-8")

    def mock_http_open(request):
        assert "/projects/777/stories" in request.full_url
        assert "limit=50" in request.full_url
        return mock_response

    monkeypatch.setenv("PIVOTAL_TRACKER_API_TOKEN", "test-token")
    importer = PivotalTrackerImporter(http_open=mock_http_open)

    stories = importer.list_by_project(777, limit=50)

    assert len(stories) == 2
    assert stories[0]["id"] == "pivotal-777-111"
    assert stories[0]["title"] == "Story 1"
    assert stories[0]["metadata"]["story_type"] == "feature"
    assert stories[1]["id"] == "pivotal-777-222"
    assert stories[1]["title"] == "Story 2"


def test_pivotal_tracker_importer_list_available_with_configured_project(monkeypatch):
    stories_json = [
        {
            "id": 333,
            "name": "Available Story",
            "story_type": "chore",
            "current_state": "unstarted",
        },
    ]

    mock_response = Mock()
    mock_response.read.return_value = json.dumps(stories_json).encode("utf-8")

    monkeypatch.setenv("PIVOTAL_TRACKER_API_TOKEN", "test-token")
    importer = PivotalTrackerImporter(http_open=lambda req: mock_response, project_id=888)

    stories = importer.list_available()

    assert len(stories) == 1
    assert stories[0]["id"] == "pivotal-888-333"


def test_pivotal_tracker_importer_list_available_without_project_raises_error():
    importer = PivotalTrackerImporter()

    with pytest.raises(ImportError, match="Project ID must be configured"):
        importer.list_available()


def test_pivotal_tracker_importer_list_by_filter_returns_filtered_stories(monkeypatch):
    stories_json = [
        {
            "id": 444,
            "name": "Backend Bug",
            "story_type": "bug",
            "current_state": "started",
            "estimate": 1,
        },
    ]

    mock_response = Mock()
    mock_response.read.return_value = json.dumps(stories_json).encode("utf-8")

    def mock_http_open(request):
        assert "/projects/555/stories" in request.full_url
        assert "filter=label%3Abackend" in request.full_url
        return mock_response

    monkeypatch.setenv("PIVOTAL_TRACKER_API_TOKEN", "test-token")
    importer = PivotalTrackerImporter(http_open=mock_http_open)

    stories = importer.list_by_filter(555, "label:backend")

    assert len(stories) == 1
    assert stories[0]["metadata"]["story_type"] == "bug"


def test_pivotal_tracker_story_handles_blockers_and_tasks():
    story = {
        "id": 666,
        "project_id": 777,
        "name": "Story with blockers",
        "story_type": "feature",
        "current_state": "started",
        "blockers": [
            {"description": "Waiting for design", "resolved": False},
            {"description": "API approval needed", "resolved": True},
        ],
        "tasks": [
            {"description": "Write tests", "complete": False},
            {"description": "Update docs", "complete": True},
        ],
    }

    result = parse_pivotal_tracker_story_json(story)

    assert len(result["source_payload"]["normalized"]["blockers"]) == 2
    assert result["source_payload"]["normalized"]["blockers"][0]["resolved"] is False
    assert result["source_payload"]["normalized"]["blockers"][1]["resolved"] is True
    assert len(result["source_payload"]["normalized"]["tasks"]) == 2
    assert result["source_payload"]["normalized"]["tasks"][0]["complete"] is False
    assert result["source_payload"]["normalized"]["tasks"][1]["complete"] is True


def test_pivotal_tracker_story_handles_release_type():
    story = {
        "id": 888,
        "project_id": 999,
        "name": "Release v2.0",
        "story_type": "release",
        "current_state": "accepted",
    }

    result = parse_pivotal_tracker_story_json(story)

    assert result["source_entity_type"] == "release"
    assert result["source_payload"]["normalized"]["story_type"] == "release"
