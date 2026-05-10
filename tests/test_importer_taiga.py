"""Tests for Taiga importer with mocked API responses."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from blueprint.importers.taiga import (
    DEFAULT_STATUS_MAP,
    TaigaClient,
    TaigaImporter,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _mock_project() -> dict:
    return {
        "id": 42,
        "name": "My Taiga Project",
        "description": "A test project",
    }


def _mock_stories() -> list[dict]:
    return [
        {
            "id": 101,
            "subject": "User login",
            "description": "Implement user login flow",
            "status_extra_info": {"name": "In progress"},
            "assigned_to_extra_info": {"full_name_display": "Alice", "username": "alice"},
            "total_points": 5.0,
            "finish_date": "2025-06-15",
            "milestone": 10,
            "tags": [["urgent", None], ["frontend", None]],
            "modified_date": "2025-05-01T12:00:00Z",
        },
        {
            "id": 102,
            "subject": "Dashboard page",
            "description": "Build dashboard",
            "status_extra_info": {"name": "New"},
            "assigned_to_extra_info": None,
            "total_points": 3.0,
            "finish_date": None,
            "milestone": None,
            "tags": [],
            "modified_date": "2025-04-20T09:00:00Z",
        },
    ]


def _mock_epics() -> list[dict]:
    return [
        {
            "id": 201,
            "subject": "Authentication Epic",
            "description": "All auth-related stories",
            "status_extra_info": {"name": "In progress"},
            "assigned_to_extra_info": {"full_name_display": "Bob"},
            "tags": [],
        },
    ]


def _mock_epic_related() -> list[dict]:
    return [
        {
            "user_story": {
                "id": 101,
                "subject": "User login",
                "description": "Implement user login flow",
                "status_extra_info": {"name": "In progress"},
                "assigned_to_extra_info": {"full_name_display": "Alice"},
                "total_points": 5.0,
                "finish_date": "2025-06-15",
                "milestone": 10,
                "tags": [],
            }
        }
    ]


def _mock_milestones() -> list[dict]:
    return [
        {
            "id": 10,
            "name": "Sprint 1",
            "estimated_start": "2025-05-01",
            "estimated_finish": "2025-05-14",
            "closed": False,
        },
    ]


def _make_importer() -> TaigaImporter:
    return TaigaImporter(
        base_url="https://taiga.example.com",
        auth_token="test-token",
    )


# ---------------------------------------------------------------------------
# TaigaImporter class
# ---------------------------------------------------------------------------


class TestTaigaImporter:
    def test_requires_auth_token(self) -> None:
        with pytest.raises(ValueError, match="Auth token required"):
            TaigaImporter(auth_token="")

    @patch.object(TaigaClient, "get_project", return_value=_mock_project())
    @patch.object(TaigaClient, "get_user_stories", return_value=_mock_stories())
    def test_import_from_source(self, mock_stories: MagicMock, mock_proj: MagicMock) -> None:
        importer = _make_importer()
        result = importer.import_from_source("42")

        assert result["source_type"] == "taiga"
        assert result["source_id"] == "42"
        assert result["title"] == "My Taiga Project"
        assert len(result["tasks"]) == 2

    @patch.object(TaigaClient, "get_project", return_value=_mock_project())
    @patch.object(TaigaClient, "get_user_stories", return_value=_mock_stories())
    def test_story_attributes_mapped(self, mock_stories: MagicMock, mock_proj: MagicMock) -> None:
        importer = _make_importer()
        result = importer.import_from_source("42")
        task = result["tasks"][0]

        assert task["task_id"] == "101"
        assert task["title"] == "User login"
        assert task["status"] == "in_progress"
        assert task["assignee"] == "Alice"
        assert task["effort"] == 5.0
        assert task["due_date"] == "2025-06-15"
        assert task["milestone_id"] == "10"
        assert "urgent" in task["tags"]

    @patch.object(TaigaClient, "get_project", return_value=_mock_project())
    @patch.object(TaigaClient, "get_user_stories", return_value=_mock_stories())
    def test_unassigned_story(self, mock_stories: MagicMock, mock_proj: MagicMock) -> None:
        importer = _make_importer()
        result = importer.import_from_source("42")
        task = result["tasks"][1]

        assert task["assignee"] is None
        assert task["milestone_id"] is None


class TestImportStories:
    @patch.object(TaigaClient, "get_user_stories", return_value=_mock_stories())
    def test_import_stories_basic(self, mock_stories: MagicMock) -> None:
        importer = _make_importer()
        tasks = importer.import_stories("42")
        assert len(tasks) == 2

    @patch.object(TaigaClient, "get_user_stories", return_value=_mock_stories())
    def test_import_stories_modified_since_filter(self, mock_stories: MagicMock) -> None:
        importer = _make_importer()
        tasks = importer.import_stories("42", modified_since="2025-04-25")
        assert len(tasks) == 1
        assert tasks[0]["title"] == "User login"

    @patch.object(TaigaClient, "get_user_stories", return_value=_mock_stories())
    def test_import_stories_status_filter(self, mock_stories: MagicMock) -> None:
        importer = _make_importer()
        importer.import_stories("42", status_filter="In progress")
        mock_stories.assert_called_once_with("42", status="In progress")


class TestImportEpicHierarchy:
    @patch.object(TaigaClient, "get_epics", return_value=_mock_epics())
    @patch.object(TaigaClient, "get_epic_related_stories", return_value=_mock_epic_related())
    def test_epic_with_subtasks(self, mock_related: MagicMock, mock_epics: MagicMock) -> None:
        importer = _make_importer()
        epics = importer.import_epic_hierarchy("42")

        assert len(epics) == 1
        assert epics[0]["title"] == "Authentication Epic"
        assert epics[0]["type"] == "epic"
        assert len(epics[0]["subtasks"]) == 1
        assert epics[0]["subtasks"][0]["title"] == "User login"


class TestImportMilestones:
    @patch.object(TaigaClient, "get_milestones", return_value=_mock_milestones())
    def test_milestones_as_phases(self, mock_ms: MagicMock) -> None:
        importer = _make_importer()
        phases = importer.import_milestones("42")

        assert len(phases) == 1
        assert phases[0]["name"] == "Sprint 1"
        assert phases[0]["start_date"] == "2025-05-01"
        assert phases[0]["end_date"] == "2025-05-14"
        assert phases[0]["closed"] is False


class TestStatusMapping:
    def test_default_status_map(self) -> None:
        assert DEFAULT_STATUS_MAP["New"] == "pending"
        assert DEFAULT_STATUS_MAP["In progress"] == "in_progress"
        assert DEFAULT_STATUS_MAP["Done"] == "completed"

    @patch.object(TaigaClient, "get_project", return_value=_mock_project())
    @patch.object(TaigaClient, "get_user_stories")
    def test_custom_status_map(self, mock_stories: MagicMock, mock_proj: MagicMock) -> None:
        mock_stories.return_value = [
            {
                "id": 1,
                "subject": "Test",
                "description": "",
                "status_extra_info": {"name": "Custom Status"},
                "assigned_to_extra_info": None,
                "total_points": 0,
                "finish_date": None,
                "milestone": None,
                "tags": [],
            }
        ]
        importer = TaigaImporter(
            base_url="https://taiga.example.com",
            auth_token="tok",
            status_map={"Custom Status": "blocked"},
        )
        result = importer.import_from_source("1")
        assert result["tasks"][0]["status"] == "blocked"


class TestValidateSource:
    @patch.object(TaigaClient, "get_project", return_value=_mock_project())
    def test_valid_project(self, mock_proj: MagicMock) -> None:
        importer = _make_importer()
        assert importer.validate_source("42") is True

    @patch.object(TaigaClient, "get_project", side_effect=ImportError("not found"))
    def test_invalid_project(self, mock_proj: MagicMock) -> None:
        importer = _make_importer()
        assert importer.validate_source("999") is False
