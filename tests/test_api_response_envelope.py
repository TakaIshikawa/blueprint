import pytest
from pydantic import ValidationError

from blueprint.api.response_envelope import (
    APISuccessResponse,
    PaginationMetadata,
    error_response,
    success_response,
)


def test_success_response_includes_standard_fields():
    response = success_response({"id": "task-1"}, request_id="req-1", timestamp="2026-01-01T00:00:00Z")

    assert response.status == "success"
    assert response.data == {"id": "task-1"}
    assert response.request_id == "req-1"
    assert response.metadata.request_id == "req-1"
    assert response.timestamp == "2026-01-01T00:00:00Z"


def test_error_response_includes_structured_error_object():
    response = error_response(
        code="not_found",
        message="Task not found",
        details={"task_id": "missing"},
        request_id="req-2",
    )

    assert response.status == "error"
    assert response.error.code == "not_found"
    assert response.error.details == {"task_id": "missing"}
    assert response.error.request_id == "req-2"


def test_pagination_metadata_does_not_change_data_payload():
    data = [{"id": "a"}]
    response = success_response(
        data,
        request_id="req-3",
        pagination=PaginationMetadata(has_next=True, next_cursor="cursor", limit=1),
    )

    assert response.data is data
    assert response.metadata.pagination.next_cursor == "cursor"
    assert response.metadata.pagination.has_next is True


def test_success_response_rejects_unexpected_top_level_fields():
    with pytest.raises(ValidationError):
        APISuccessResponse(
            data={},
            metadata={"request_id": "req-4"},
            request_id="req-4",
            timestamp="now",
            unexpected=True,
        )

