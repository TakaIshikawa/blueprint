import pytest
from pydantic import ValidationError

from blueprint.api.export_jobs import CreateExportRequest, ExportJobFacade
from blueprint.export.data_exporter import InMemoryDataStore


def _store() -> InMemoryDataStore:
    store = InMemoryDataStore()
    store.workspaces["ws-1"] = {"id": "ws-1", "plan_ids": ["p1"]}
    store.plans["p1"] = {"id": "p1", "tasks": [{"id": "t1"}]}
    store.tasks["t1"] = {"id": "t1", "status": "todo"}
    return store


def test_create_export_request_validates_format_scope_destination_filters_and_options():
    request = CreateExportRequest(
        format="json",
        scope="filtered",
        destination="memory://export",
        filters={"status": ["todo"]},
        options={"include_metadata": False},
    )

    assert request.format == "json"
    assert request.scope == "filtered"
    assert request.filters.status == ["todo"]
    assert request.options.include_metadata is False


def test_facade_creates_immediate_export_result_and_manifest_summary():
    facade = ExportJobFacade(store=_store())
    response = facade.create_export({"format": "json", "scope": "all", "destination": "memory://export"})

    assert response.status == "completed"
    assert response.manifest.export_id == response.export_id
    assert response.manifest.format == "json"
    assert response.manifest.scope == "all"
    assert response.manifest.record_counts["plans"] == 1
    assert "sha256" in response.manifest.checksums
    assert facade.get_status(response.export_id)["status"] == "completed"


def test_invalid_format_or_scope_produce_api_facing_validation_errors():
    with pytest.raises(ValidationError):
        CreateExportRequest(format="xml")
    with pytest.raises(ValidationError):
        CreateExportRequest(scope="tenant")

