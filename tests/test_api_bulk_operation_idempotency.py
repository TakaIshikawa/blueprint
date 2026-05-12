from datetime import datetime, timezone

from blueprint.api.bulk_operations import (
    BulkIdempotencyStore,
    BulkOperationRequest,
    bulk_request_fingerprint,
    classify_bulk_idempotency,
)


def _request(title: str = "Launch") -> BulkOperationRequest:
    return BulkOperationRequest(
        items=[
            {
                "operation": "create",
                "resource_type": "plans",
                "client_id": "plan-1",
                "payload": {"title": title, "labels": ["release", "priority"]},
            }
        ],
        continue_on_error=True,
    )


def test_bulk_request_fingerprint_is_stable_for_semantically_identical_requests():
    request = _request()
    equivalent = BulkOperationRequest(**request.model_dump(mode="json"))

    assert bulk_request_fingerprint(request) == bulk_request_fingerprint(equivalent)


def test_first_idempotency_key_use_stores_new_receipt():
    receipts = {}
    request = _request()

    check = classify_bulk_idempotency(
        receipts,
        "bulk-key-1",
        request,
        metadata={"status_code": 202},
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )

    assert check.classification == "new"
    assert receipts["bulk-key-1"] == check.receipt
    assert check.receipt.request_fingerprint == bulk_request_fingerprint(request)
    assert check.receipt.created_at == "2026-01-01T00:00:00Z"
    assert check.receipt.metadata == {"status_code": 202}


def test_reusing_same_key_and_fingerprint_replays_original_receipt_metadata():
    store = BulkIdempotencyStore()
    request = _request()

    first = store.classify(
        "bulk-key-1",
        request,
        metadata={"result_id": "result-1"},
        created_at="2026-01-01T00:00:00Z",
    )
    replay = store.classify(
        "bulk-key-1",
        BulkOperationRequest(**request.model_dump(mode="json")),
        metadata={"result_id": "ignored"},
        created_at="2026-01-02T00:00:00Z",
    )

    assert replay.classification == "replay"
    assert replay.receipt == first.receipt
    assert replay.receipt.metadata == {"result_id": "result-1"}
    assert replay.receipt.created_at == "2026-01-01T00:00:00Z"


def test_reusing_same_key_with_different_fingerprint_conflicts_without_overwrite():
    store = BulkIdempotencyStore()
    first = store.classify("bulk-key-1", _request(), metadata={"result_id": "result-1"})

    conflict = store.classify(
        "bulk-key-1", _request(title="Different"), metadata={"result_id": "result-2"}
    )

    assert conflict.classification == "conflict"
    assert conflict.receipt == first.receipt
    assert store.receipts["bulk-key-1"] == first.receipt
