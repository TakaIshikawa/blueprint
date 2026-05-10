"""Tests for Plane.so importer with mocked API responses."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from blueprint.importers.plane import (
    DEFAULT_STATE_MAP,
    PRIORITY_MAP,
    PlaneClient,
    PlaneImporter,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _mock_project() -> dict:
    return {
        "id": "proj-001",
        "name": "My Plane Project",
        "description": "A test project on Plane",
    }


def _mock_issues() -> list[dict]:
    return [
        {
            "id": "issue-1",
            "name": "Fix login bug",
            "description": "Users cannot log in",
            "description_stripped": "Users cannot log in",
            "state_detail": {"name": "Started"},
            "priority": "high",
            "assignees": ["user-a"],
            "estimate_point": 3.0,
            "target_date": "2025-07-01",
            "parent": "issue-0",
            "labels": [{"name": "bug"}, {"name": "auth"}],
            "updated_at": "2025-05-10T12:00:00Z",
        },
        {
            "id": "issue-2",
            "name": "Add dashboard",
            "description": "Create dashboard page",
            "state_detail": {"name": "Backlog"},
            "priority": "medium",
            "assignees": [],
            "estimate_point": None,
            "target_date": None,
            "parent": None,
            "labels": [],
            "updated_at": "2025-04-20T09:00:00Z",
        },
    ]


def _mock_cycles() -> list[dict]:
    return [
        {
            "id": "cycle-1",
            "name": "Sprint 1",
            "start_date": "2025-05-01",
            "end_date": "2025-05-14",
            "status": "current",
        },
    ]


def _mock_modules() -> list[dict]:
    return [
        {
            "id": "mod-1",
            "name": "Auth Module",
            "description": "Authentication milestone",
            "start_date": "2025-05-01",
            "target_date": "2025-06-01",
            "status": "in_progress",
        },
    ]


def _make_importer() -> PlaneImporter:
    return PlaneImporter(
        api_key="test-key",
        workspace_slug="my-workspace",
    )


# ---------------------------------------------------------------------------
# PlaneImporter class
# ---------------------------------------------------------------------------


class TestPlaneImporter:
    def test_requires_api_key(self) -> None:
        with pytest.raises(ValueError, match="API key required"):
            PlaneImporter(api_key="")

    @patch.object(PlaneClient, "get_project", return_value=_mock_project())
    @patch.object(PlaneClient, "get_issues", return_value=_mock_issues())
    def test_import_from_source(self, mock_issues: MagicMock, mock_proj: MagicMock) -> None:
        importer = _make_importer()
        result = importer.import_from_source("proj-001")

        assert result["source_type"] == "plane"
        assert result["source_id"] == "proj-001"
        assert result["title"] == "My Plane Project"
        assert len(result["tasks"]) == 2

    @patch.object(PlaneClient, "get_project", return_value=_mock_project())
    @patch.object(PlaneClient, "get_issues", return_value=_mock_issues())
    def test_issue_attributes_mapped(self, mock_issues: MagicMock, mock_proj: MagicMock) -> None:
        importer = _make_importer()
        result = importer.import_from_source("proj-001")
        task = result["tasks"][0]

        assert task["task_id"] == "issue-1"
        assert task["title"] == "Fix login bug"
        assert task["status"] == "in_progress"
        assert task["priority"] == "high"
        assert task["effort"] == 3.0
        assert task["target_date"] == "2025-07-01"
        assert task["parent_id"] == "issue-0"
        assert "bug" in task["tags"]
        assert "auth" in task["tags"]

    @patch.object(PlaneClient, "get_project", return_value=_mock_project())
    @patch.object(PlaneClient, "get_issues", return_value=_mock_issues())
    def test_issue_no_parent(self, mock_issues: MagicMock, mock_proj: MagicMock) -> None:
        importer = _make_importer()
        result = importer.import_from_source("proj-001")
        task = result["tasks"][1]

        assert task["parent_id"] is None
        assert task["effort"] == 0.0


class TestImportIssues:
    @patch.object(PlaneClient, "get_issues", return_value=_mock_issues())
    def test_import_issues_basic(self, mock_issues: MagicMock) -> None:
        importer = _make_importer()
        tasks = importer.import_issues("proj-001")
        assert len(tasks) == 2

    @patch.object(PlaneClient, "get_issues", return_value=_mock_issues())
    def test_import_issues_updated_since(self, mock_issues: MagicMock) -> None:
        importer = _make_importer()
        tasks = importer.import_issues("proj-001", updated_since="2025-05-01")
        assert len(tasks) == 1
        assert tasks[0]["title"] == "Fix login bug"


class TestImportCycles:
    @patch.object(PlaneClient, "get_cycles", return_value=_mock_cycles())
    def test_cycles_as_phases(self, mock_cycles: MagicMock) -> None:
        importer = _make_importer()
        phases = importer.import_cycles("proj-001")

        assert len(phases) == 1
        assert phases[0]["name"] == "Sprint 1"
        assert phases[0]["start_date"] == "2025-05-01"
        assert phases[0]["end_date"] == "2025-05-14"


class TestImportModules:
    @patch.object(PlaneClient, "get_modules", return_value=_mock_modules())
    def test_modules_as_milestones(self, mock_modules: MagicMock) -> None:
        importer = _make_importer()
        milestones = importer.import_modules("proj-001")

        assert len(milestones) == 1
        assert milestones[0]["name"] == "Auth Module"
        assert milestones[0]["target_date"] == "2025-06-01"


class TestStatusMapping:
    def test_default_state_map(self) -> None:
        assert DEFAULT_STATE_MAP["Backlog"] == "pending"
        assert DEFAULT_STATE_MAP["Started"] == "in_progress"
        assert DEFAULT_STATE_MAP["Done"] == "completed"

    def test_priority_map(self) -> None:
        assert PRIORITY_MAP["urgent"] == "critical"
        assert PRIORITY_MAP["high"] == "high"
        assert PRIORITY_MAP["medium"] == "medium"


class TestValidateSource:
    @patch.object(PlaneClient, "get_project", return_value=_mock_project())
    def test_valid_project(self, mock_proj: MagicMock) -> None:
        importer = _make_importer()
        assert importer.validate_source("proj-001") is True

    @patch.object(PlaneClient, "get_project", side_effect=ImportError("not found"))
    def test_invalid_project(self, mock_proj: MagicMock) -> None:
        importer = _make_importer()
        assert importer.validate_source("bad") is False

    def test_no_workspace_returns_false(self) -> None:
        importer = PlaneImporter(api_key="key", workspace_slug="")
        assert importer.validate_source("proj-001") is False
