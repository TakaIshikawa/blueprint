"""Tests for Wrike API importer."""

from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import Mock, patch

import pytest

from blueprint.importers.wrike import WrikeClient, WrikeImporter


# Mock Wrike API responses
MOCK_FOLDER_RESPONSE = {
    "data": [
        {
            "id": "IEAAA45KI4FOLDER",
            "title": "Product Development",
            "description": "Main product development folder",
            "createdDate": "2024-01-15T10:30:00Z",
        }
    ]
}

MOCK_TASKS_RESPONSE = {
    "data": [
        {
            "id": "IEAAA45KI4TASK001",
            "title": "Implement user authentication",
            "description": "Add JWT-based authentication to the API",
            "status": "Active",
            "importance": "High",
            "createdDate": "2024-01-20T09:00:00Z",
            "updatedDate": "2024-01-21T14:30:00Z",
            "dates": {
                "start": "2024-01-20",
                "due": "2024-01-27",
            },
            "responsibleIds": ["KUAAAAHC", "KUAAAAHD"],
            "superParentIds": [],
            "dependencyIds": [],
            "permalink": "https://www.wrike.com/open.htm?id=IEAAA45KI4TASK001",
            "customFields": [
                {"id": "CUSTOM001", "value": "Backend"},
                {"id": "CUSTOM002", "value": "3"},
            ],
        },
        {
            "id": "IEAAA45KI4TASK002",
            "title": "Add login UI",
            "description": "Create login form and connect to authentication API",
            "status": "Active",
            "importance": "Normal",
            "createdDate": "2024-01-22T10:00:00Z",
            "updatedDate": "2024-01-22T10:00:00Z",
            "dates": {
                "start": "2024-01-25",
                "due": "2024-01-30",
            },
            "responsibleIds": ["KUAAAAHD"],
            "superParentIds": [],
            "dependencyIds": ["IEAAA45KI4TASK001"],
            "permalink": "https://www.wrike.com/open.htm?id=IEAAA45KI4TASK002",
            "customFields": [],
        },
        {
            "id": "IEAAA45KI4TASK003",
            "title": "Deploy to staging",
            "description": "Deploy authentication feature to staging environment",
            "status": "Deferred",
            "importance": "Low",
            "createdDate": "2024-01-23T11:00:00Z",
            "updatedDate": "2024-01-24T15:00:00Z",
            "dates": {},
            "responsibleIds": [],
            "superParentIds": ["IEAAA45KI4TASK001"],
            "dependencyIds": ["IEAAA45KI4TASK001", "IEAAA45KI4TASK002"],
            "permalink": "https://www.wrike.com/open.htm?id=IEAAA45KI4TASK003",
            "customFields": [],
        },
    ]
}

MOCK_CUSTOM_FIELDS_RESPONSE = {
    "data": [
        {
            "id": "CUSTOM001",
            "title": "Team",
            "type": "DropDown",
        },
        {
            "id": "CUSTOM002",
            "title": "Story Points",
            "type": "Numeric",
        },
    ]
}

MOCK_TIME_LOGS_RESPONSE = {
    "data": [
        {
            "id": "TIMELOG001",
            "userId": "KUAAAAHC",
            "hours": 5.5,
            "trackedDate": "2024-01-20",
            "comment": "Initial authentication implementation",
            "createdDate": "2024-01-20T17:00:00Z",
        },
        {
            "id": "TIMELOG002",
            "userId": "KUAAAAHD",
            "hours": 3.0,
            "trackedDate": "2024-01-21",
            "comment": "Code review and testing",
            "createdDate": "2024-01-21T16:00:00Z",
        },
    ]
}


@pytest.fixture
def mock_urlopen():
    """Mock urlopen to simulate Wrike API responses."""
    with patch("blueprint.importers.wrike.urlopen") as mock:
        yield mock


@pytest.fixture
def wrike_importer(mock_urlopen):
    """Create a WrikeImporter with mocked API."""
    return WrikeImporter(api_token="test-api-token")


class TestWrikeClientInit:
    """Test WrikeClient initialization."""

    def test_init_with_api_token(self):
        """Test client initialization with API token."""
        client = WrikeClient(api_token="test-token")
        assert client.api_token == "test-token"
        assert client.base_url == "https://www.wrike.com/api/v4"

    def test_init_with_custom_base_url(self):
        """Test client initialization with custom base URL."""
        client = WrikeClient(
            api_token="test-token",
            base_url="https://custom.wrike.com/api/v4",
        )
        assert client.base_url == "https://custom.wrike.com/api/v4"

    def test_headers_include_bearer_token(self):
        """Test that headers include Bearer token."""
        client = WrikeClient(api_token="test-token")
        headers = client._headers()
        assert headers["Authorization"] == "Bearer test-token"
        assert headers["Content-Type"] == "application/json"


class TestWrikeImporterInit:
    """Test WrikeImporter initialization."""

    def test_init_with_api_token(self, mock_urlopen):
        """Test initialization with explicit API token."""
        importer = WrikeImporter(api_token="test-token")
        assert importer.api_token == "test-token"
        assert isinstance(importer.client, WrikeClient)

    def test_init_without_api_token_raises_error(self):
        """Test that missing API token raises ValueError."""
        with patch("blueprint.importers.wrike.os.environ.get", return_value=None):
            with pytest.raises(ValueError, match="API token required"):
                WrikeImporter()

    def test_init_with_env_var(self, mock_urlopen):
        """Test initialization from WRIKE_API_TOKEN environment variable."""
        with patch(
            "blueprint.importers.wrike.os.environ.get",
            return_value="env-api-token",
        ):
            importer = WrikeImporter()
            assert importer.api_token == "env-api-token"

    def test_init_with_custom_status_map(self, mock_urlopen):
        """Test initialization with custom status mapping."""
        custom_map = {"Active": "active", "Completed": "done"}
        importer = WrikeImporter(api_token="test-token", status_map=custom_map)
        assert importer.status_map == custom_map


class TestWrikeClientRequests:
    """Test WrikeClient API request methods."""

    def test_request_json_success(self, mock_urlopen):
        """Test successful API request."""
        mock_response = Mock()
        mock_response.read.return_value = json.dumps(MOCK_FOLDER_RESPONSE).encode()
        mock_urlopen.return_value.__enter__.return_value = mock_response

        client = WrikeClient(api_token="test-token")
        result = client.request_json("folders/IEAAA45KI4FOLDER")

        assert result == MOCK_FOLDER_RESPONSE
        mock_urlopen.assert_called_once()

    def test_request_json_with_params(self, mock_urlopen):
        """Test API request with query parameters."""
        mock_response = Mock()
        mock_response.read.return_value = json.dumps(MOCK_TASKS_RESPONSE).encode()
        mock_urlopen.return_value.__enter__.return_value = mock_response

        client = WrikeClient(api_token="test-token")
        result = client.request_json(
            "folders/IEAAA45KI4FOLDER/tasks",
            params={"descendants": "true", "pageSize": "100"},
        )

        assert result == MOCK_TASKS_RESPONSE
        call_args = mock_urlopen.call_args[0][0]
        assert "descendants=true" in call_args.full_url
        assert "pageSize=100" in call_args.full_url

    def test_get_folder(self, mock_urlopen):
        """Test getting folder details."""
        mock_response = Mock()
        mock_response.read.return_value = json.dumps(MOCK_FOLDER_RESPONSE).encode()
        mock_urlopen.return_value.__enter__.return_value = mock_response

        client = WrikeClient(api_token="test-token")
        folder = client.get_folder("IEAAA45KI4FOLDER")

        assert folder["id"] == "IEAAA45KI4FOLDER"
        assert folder["title"] == "Product Development"

    def test_get_tasks_in_folder(self, mock_urlopen):
        """Test getting tasks from folder."""
        mock_response = Mock()
        mock_response.read.return_value = json.dumps(MOCK_TASKS_RESPONSE).encode()
        mock_urlopen.return_value.__enter__.return_value = mock_response

        client = WrikeClient(api_token="test-token")
        tasks = client.get_tasks_in_folder("IEAAA45KI4FOLDER")

        assert len(tasks) == 3
        assert tasks[0]["id"] == "IEAAA45KI4TASK001"

    def test_get_custom_fields(self, mock_urlopen):
        """Test getting custom field definitions."""
        mock_response = Mock()
        mock_response.read.return_value = json.dumps(
            MOCK_CUSTOM_FIELDS_RESPONSE
        ).encode()
        mock_urlopen.return_value.__enter__.return_value = mock_response

        client = WrikeClient(api_token="test-token")
        fields = client.get_custom_fields()

        assert len(fields) == 2
        assert fields[0]["title"] == "Team"

    def test_get_time_logs(self, mock_urlopen):
        """Test getting time logs for a task."""
        mock_response = Mock()
        mock_response.read.return_value = json.dumps(MOCK_TIME_LOGS_RESPONSE).encode()
        mock_urlopen.return_value.__enter__.return_value = mock_response

        client = WrikeClient(api_token="test-token")
        logs = client.get_time_logs("IEAAA45KI4TASK001")

        assert len(logs) == 2
        assert logs[0]["hours"] == 5.5


class TestImportFromSource:
    """Test importing from a Wrike folder."""

    def test_import_folder_success(self, wrike_importer, mock_urlopen):
        """Test successful folder import."""
        responses = [
            json.dumps(MOCK_FOLDER_RESPONSE).encode(),
            json.dumps(MOCK_TASKS_RESPONSE).encode(),
            json.dumps(MOCK_CUSTOM_FIELDS_RESPONSE).encode(),  # For custom fields call
        ]
        mock_response = Mock()
        mock_response.read.side_effect = responses
        mock_urlopen.return_value.__enter__.return_value = mock_response

        result = wrike_importer.import_from_source("IEAAA45KI4FOLDER")

        assert result["source_type"] == "wrike"
        assert result["source_id"] == "IEAAA45KI4FOLDER"
        assert result["title"] == "Product Development"
        assert len(result["tasks"]) == 3
        assert result["metadata"]["task_count"] == 3

    def test_import_folder_not_found_raises_error(
        self, wrike_importer, mock_urlopen
    ):
        """Test that importing nonexistent folder raises ImportError."""
        mock_response = Mock()
        mock_response.read.return_value = json.dumps({"data": []}).encode()
        mock_urlopen.return_value.__enter__.return_value = mock_response

        with pytest.raises(ImportError, match="Wrike folder not found"):
            wrike_importer.import_from_source("NONEXISTENT")


class TestTaskMapping:
    """Test mapping of Wrike tasks to blueprint format."""

    def test_map_basic_task_attributes(self, wrike_importer, mock_urlopen):
        """Test mapping of basic task attributes."""
        mock_response = Mock()
        mock_response.read.side_effect = [
            json.dumps(MOCK_FOLDER_RESPONSE).encode(),
            json.dumps(MOCK_TASKS_RESPONSE).encode(),
            json.dumps(MOCK_CUSTOM_FIELDS_RESPONSE).encode(),
        ]
        mock_urlopen.return_value.__enter__.return_value = mock_response

        result = wrike_importer.import_from_source("IEAAA45KI4FOLDER")
        task = result["tasks"][0]

        assert task["id"] == "wrike-IEAAA45KI4TASK001"
        assert task["title"] == "Implement user authentication"
        assert task["description"] == "Add JWT-based authentication to the API"
        assert task["status"] == "in_progress"  # "Active" maps to "in_progress"
        assert task["priority"] == "high"  # "High" importance maps to "high"

    def test_map_task_dates(self, wrike_importer, mock_urlopen):
        """Test mapping of task dates."""
        mock_response = Mock()
        mock_response.read.side_effect = [
            json.dumps(MOCK_FOLDER_RESPONSE).encode(),
            json.dumps(MOCK_TASKS_RESPONSE).encode(),
            json.dumps(MOCK_CUSTOM_FIELDS_RESPONSE).encode(),
        ]
        mock_urlopen.return_value.__enter__.return_value = mock_response

        result = wrike_importer.import_from_source("IEAAA45KI4FOLDER")
        task = result["tasks"][0]

        assert task["start_date"] == "2024-01-20"
        assert task["due_date"] == "2024-01-27"
        assert task["created_at"] == "2024-01-20T09:00:00Z"
        assert task["updated_at"] == "2024-01-21T14:30:00Z"

    def test_map_task_assignees(self, wrike_importer, mock_urlopen):
        """Test mapping of task assignees."""
        mock_response = Mock()
        mock_response.read.side_effect = [
            json.dumps(MOCK_FOLDER_RESPONSE).encode(),
            json.dumps(MOCK_TASKS_RESPONSE).encode(),
            json.dumps(MOCK_CUSTOM_FIELDS_RESPONSE).encode(),
        ]
        mock_urlopen.return_value.__enter__.return_value = mock_response

        result = wrike_importer.import_from_source("IEAAA45KI4FOLDER")
        task = result["tasks"][0]

        assert len(task["assignees"]) == 2
        assert "KUAAAAHC" in task["assignees"]
        assert "KUAAAAHD" in task["assignees"]

    def test_map_task_dependencies(self, wrike_importer, mock_urlopen):
        """Test mapping of task dependencies."""
        mock_response = Mock()
        mock_response.read.side_effect = [
            json.dumps(MOCK_FOLDER_RESPONSE).encode(),
            json.dumps(MOCK_TASKS_RESPONSE).encode(),
            json.dumps(MOCK_CUSTOM_FIELDS_RESPONSE).encode(),
        ]
        mock_urlopen.return_value.__enter__.return_value = mock_response

        result = wrike_importer.import_from_source("IEAAA45KI4FOLDER")
        task_with_deps = result["tasks"][2]  # Third task has dependencies

        assert len(task_with_deps["depends_on"]) == 2
        assert "wrike-IEAAA45KI4TASK001" in task_with_deps["depends_on"]
        assert "wrike-IEAAA45KI4TASK002" in task_with_deps["depends_on"]

    def test_map_task_hierarchy(self, wrike_importer, mock_urlopen):
        """Test mapping of task parent hierarchy."""
        mock_response = Mock()
        mock_response.read.side_effect = [
            json.dumps(MOCK_FOLDER_RESPONSE).encode(),
            json.dumps(MOCK_TASKS_RESPONSE).encode(),
            json.dumps(MOCK_CUSTOM_FIELDS_RESPONSE).encode(),
        ]
        mock_urlopen.return_value.__enter__.return_value = mock_response

        result = wrike_importer.import_from_source("IEAAA45KI4FOLDER")
        subtask = result["tasks"][2]  # Third task has parent

        assert len(subtask["parent_ids"]) == 1
        assert "wrike-IEAAA45KI4TASK001" in subtask["parent_ids"]

    def test_map_custom_fields(self, wrike_importer, mock_urlopen):
        """Test mapping of custom fields."""
        mock_response = Mock()
        mock_response.read.side_effect = [
            json.dumps(MOCK_FOLDER_RESPONSE).encode(),
            json.dumps(MOCK_TASKS_RESPONSE).encode(),
            json.dumps(MOCK_CUSTOM_FIELDS_RESPONSE).encode(),
        ]
        mock_urlopen.return_value.__enter__.return_value = mock_response

        result = wrike_importer.import_from_source("IEAAA45KI4FOLDER")
        task = result["tasks"][0]

        custom_fields = task["metadata"]["custom_fields"]
        assert custom_fields["Team"] == "Backend"
        assert custom_fields["Story Points"] == "3"

    def test_map_task_metadata(self, wrike_importer, mock_urlopen):
        """Test mapping of task metadata."""
        mock_response = Mock()
        mock_response.read.side_effect = [
            json.dumps(MOCK_FOLDER_RESPONSE).encode(),
            json.dumps(MOCK_TASKS_RESPONSE).encode(),
            json.dumps(MOCK_CUSTOM_FIELDS_RESPONSE).encode(),
        ]
        mock_urlopen.return_value.__enter__.return_value = mock_response

        result = wrike_importer.import_from_source("IEAAA45KI4FOLDER")
        task = result["tasks"][0]

        assert task["metadata"]["wrike_id"] == "IEAAA45KI4TASK001"
        assert (
            task["metadata"]["wrike_permalink"]
            == "https://www.wrike.com/open.htm?id=IEAAA45KI4TASK001"
        )
        assert task["metadata"]["wrike_status"] == "Active"
        assert task["metadata"]["wrike_importance"] == "High"


class TestMultiFolderImport:
    """Test importing from multiple folders."""

    def test_import_multiple_folders(self, wrike_importer, mock_urlopen):
        """Test importing tasks from multiple folders."""
        folder2_response = {
            "data": [{"id": "FOLDER2", "title": "Testing", "description": ""}]
        }
        tasks2_response = {
            "data": [
                {
                    "id": "TASK_F2_001",
                    "title": "Write tests",
                    "description": "Unit tests",
                    "status": "Active",
                    "importance": "Normal",
                    "createdDate": "2024-01-25T10:00:00Z",
                    "updatedDate": "2024-01-25T10:00:00Z",
                    "dates": {},
                    "responsibleIds": [],
                    "superParentIds": [],
                    "dependencyIds": [],
                    "permalink": "https://www.wrike.com/open.htm?id=TASK_F2_001",
                    "customFields": [],
                }
            ]
        }

        mock_response = Mock()
        mock_response.read.side_effect = [
            json.dumps(MOCK_FOLDER_RESPONSE).encode(),
            json.dumps(MOCK_TASKS_RESPONSE).encode(),
            json.dumps(MOCK_CUSTOM_FIELDS_RESPONSE).encode(),  # For first folder
            json.dumps(folder2_response).encode(),
            json.dumps(tasks2_response).encode(),
        ]
        mock_urlopen.return_value.__enter__.return_value = mock_response

        result = wrike_importer.import_tasks(["IEAAA45KI4FOLDER", "FOLDER2"])

        assert result["source_type"] == "wrike"
        assert len(result["tasks"]) == 4  # 3 from first folder + 1 from second
        assert result["metadata"]["total_task_count"] == 4
        assert len(result["metadata"]["folders"]) == 2

    def test_import_with_time_logs(self, wrike_importer, mock_urlopen):
        """Test importing tasks with time logs."""
        mock_response = Mock()
        mock_response.read.side_effect = [
            json.dumps(MOCK_FOLDER_RESPONSE).encode(),
            json.dumps(MOCK_TASKS_RESPONSE).encode(),
            json.dumps(MOCK_CUSTOM_FIELDS_RESPONSE).encode(),
            json.dumps(MOCK_TIME_LOGS_RESPONSE).encode(),
            json.dumps(MOCK_TIME_LOGS_RESPONSE).encode(),
            json.dumps(MOCK_TIME_LOGS_RESPONSE).encode(),
        ]
        mock_urlopen.return_value.__enter__.return_value = mock_response

        result = wrike_importer.import_tasks(
            ["IEAAA45KI4FOLDER"], include_time_logs=True
        )

        task = result["tasks"][0]
        assert "time_logs" in task["metadata"]
        assert len(task["metadata"]["time_logs"]) == 2
        assert task["metadata"]["time_logs"][0]["hours"] == 5.5


class TestIncrementalSync:
    """Test incremental sync functionality."""

    def test_sync_since_date(self, wrike_importer, mock_urlopen):
        """Test syncing only tasks updated after a specific date."""
        from datetime import timezone

        mock_response = Mock()
        mock_response.read.side_effect = [
            json.dumps(MOCK_TASKS_RESPONSE).encode(),
            json.dumps(MOCK_CUSTOM_FIELDS_RESPONSE).encode(),
            json.dumps(MOCK_TASKS_RESPONSE).encode(),
            json.dumps(MOCK_CUSTOM_FIELDS_RESPONSE).encode(),
        ]
        mock_urlopen.return_value.__enter__.return_value = mock_response

        since = datetime(2024, 1, 21, 0, 0, 0, tzinfo=timezone.utc)
        result = wrike_importer.sync_incremental("IEAAA45KI4FOLDER", since)

        # Only tasks updated after 2024-01-21 should be included
        # Task 1: updated 2024-01-21T14:30:00Z - INCLUDED
        # Task 2: updated 2024-01-22T10:00:00Z - INCLUDED
        # Task 3: updated 2024-01-24T15:00:00Z - INCLUDED
        assert len(result["tasks"]) == 3

        # Test with later date
        since = datetime(2024, 1, 23, 0, 0, 0, tzinfo=timezone.utc)
        result = wrike_importer.sync_incremental("IEAAA45KI4FOLDER", since)

        # Only task 3 should be included (updated 2024-01-24)
        assert len(result["tasks"]) == 1
        assert result["metadata"]["sync_since"] == since.isoformat()


class TestValidateSource:
    """Test source validation."""

    def test_validate_existing_folder(self, wrike_importer, mock_urlopen):
        """Test validating an existing folder."""
        mock_response = Mock()
        mock_response.read.return_value = json.dumps(MOCK_FOLDER_RESPONSE).encode()
        mock_urlopen.return_value.__enter__.return_value = mock_response

        is_valid = wrike_importer.validate_source("IEAAA45KI4FOLDER")
        assert is_valid is True

    def test_validate_nonexistent_folder(self, wrike_importer, mock_urlopen):
        """Test validating a nonexistent folder."""
        mock_response = Mock()
        mock_response.read.return_value = json.dumps({"data": []}).encode()
        mock_urlopen.return_value.__enter__.return_value = mock_response

        is_valid = wrike_importer.validate_source("NONEXISTENT")
        assert is_valid is False


class TestStatusMapping:
    """Test status mapping from Wrike to blueprint."""

    def test_default_status_mapping(self, wrike_importer, mock_urlopen):
        """Test that Wrike statuses map correctly to blueprint statuses."""
        mock_response = Mock()
        mock_response.read.side_effect = [
            json.dumps(MOCK_FOLDER_RESPONSE).encode(),
            json.dumps(MOCK_TASKS_RESPONSE).encode(),
            json.dumps(MOCK_CUSTOM_FIELDS_RESPONSE).encode(),
        ]
        mock_urlopen.return_value.__enter__.return_value = mock_response

        result = wrike_importer.import_from_source("IEAAA45KI4FOLDER")

        # Task 1: Active -> in_progress
        assert result["tasks"][0]["status"] == "in_progress"

        # Task 3: Deferred -> pending
        assert result["tasks"][2]["status"] == "pending"

    def test_custom_status_mapping(self, mock_urlopen):
        """Test using custom status mapping."""
        custom_map = {
            "Active": "active",
            "Completed": "done",
            "Deferred": "backlog",
        }
        importer = WrikeImporter(api_token="test-token", status_map=custom_map)

        mock_response = Mock()
        mock_response.read.side_effect = [
            json.dumps(MOCK_FOLDER_RESPONSE).encode(),
            json.dumps(MOCK_TASKS_RESPONSE).encode(),
            json.dumps(MOCK_CUSTOM_FIELDS_RESPONSE).encode(),
        ]
        mock_urlopen.return_value.__enter__.return_value = mock_response

        result = importer.import_from_source("IEAAA45KI4FOLDER")

        # Task 1: Active -> active (custom mapping)
        assert result["tasks"][0]["status"] == "active"

        # Task 3: Deferred -> backlog (custom mapping)
        assert result["tasks"][2]["status"] == "backlog"
