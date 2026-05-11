import pytest
from pydantic import ValidationError

from blueprint.api.bulk_operations import (
    BulkOperationItem,
    BulkOperationRequest,
    build_bulk_result,
    item_failure,
    item_skipped,
    item_success,
)


def test_bulk_request_validates_required_item_fields():
    request = BulkOperationRequest(
        items=[{"operation": "create", "resource_type": "tasks", "client_id": "c1", "payload": {}}]
    )

    assert request.items[0].client_id == "c1"

    with pytest.raises(ValidationError):
        BulkOperationRequest(items=[{"operation": "merge", "resource_type": "tasks", "client_id": "c2"}])


def test_result_builders_produce_summary_counts_and_preserve_client_ids():
    item = BulkOperationItem(operation="create", resource_type="tasks", client_id="c1", payload={})
    result = build_bulk_result(
        [
            item_success(item, resource_id="t1"),
            item_failure(item, code="invalid", message="Invalid"),
            item_skipped(item, reason="Blocked"),
        ]
    )

    assert result.summary.total == 3
    assert result.summary.succeeded == 1
    assert result.summary.failed == 1
    assert result.summary.skipped == 1
    assert result.partial_success is True
    assert {item.client_id for item in result.results} == {"c1"}

