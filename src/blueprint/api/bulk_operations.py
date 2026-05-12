"""Storage-agnostic bulk operation request and result models."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Literal, MutableMapping

from pydantic import BaseModel, ConfigDict, Field

OperationType = Literal["create", "update", "delete"]
IdempotencyStatus = Literal["new", "replay", "conflict"]
DependencyDiagnosticType = Literal["missing_dependency", "cycle"]


class BulkOperationItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    operation: OperationType
    resource_type: str = Field(min_length=1)
    client_id: str = Field(min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)


class BulkOperationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[BulkOperationItem] = Field(min_length=1)
    continue_on_error: bool = True


class BulkItemResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    client_id: str = Field(min_length=1)
    operation: OperationType
    resource_type: str = Field(min_length=1)
    status: Literal["succeeded", "failed", "skipped"]
    resource_id: str | None = None
    error: dict[str, Any] | None = None


class BulkOperationSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    total: int = Field(ge=0)
    succeeded: int = Field(ge=0)
    failed: int = Field(ge=0)
    skipped: int = Field(ge=0)


class BulkOperationResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    results: list[BulkItemResult]
    summary: BulkOperationSummary
    partial_success: bool


class BulkDependencyDiagnostic(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: DependencyDiagnosticType
    client_id: str = Field(min_length=1)
    message: str = Field(min_length=1)
    depends_on: list[str] = Field(default_factory=list)


class BulkExecutionPlan(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[BulkOperationItem]
    diagnostics: list[BulkDependencyDiagnostic] = Field(default_factory=list)
    executable: bool


class BulkIdempotencyReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid")

    idempotency_key: str = Field(min_length=1)
    request_fingerprint: str = Field(min_length=1)
    status: Literal["stored"] = "stored"
    created_at: str = Field(min_length=1)
    metadata: dict[str, Any] = Field(default_factory=dict)


class BulkIdempotencyCheck(BaseModel):
    model_config = ConfigDict(extra="forbid")

    classification: IdempotencyStatus
    receipt: BulkIdempotencyReceipt


class BulkIdempotencyStore:
    """Small dict-backed receipt store for adapters that do not have persistence yet."""

    def __init__(self, receipts: MutableMapping[str, BulkIdempotencyReceipt] | None = None) -> None:
        self.receipts: MutableMapping[str, BulkIdempotencyReceipt] = (
            receipts if receipts is not None else {}
        )

    def classify(
        self,
        idempotency_key: str,
        request: BulkOperationRequest,
        *,
        metadata: dict[str, Any] | None = None,
        created_at: datetime | str | None = None,
    ) -> BulkIdempotencyCheck:
        return classify_bulk_idempotency(
            self.receipts,
            idempotency_key,
            request,
            metadata=metadata,
            created_at=created_at,
        )


def bulk_request_fingerprint(request: BulkOperationRequest) -> str:
    payload = request.model_dump(mode="json")
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(raw).hexdigest()


def create_idempotency_receipt(
    idempotency_key: str,
    request: BulkOperationRequest,
    *,
    metadata: dict[str, Any] | None = None,
    created_at: datetime | str | None = None,
) -> BulkIdempotencyReceipt:
    if isinstance(created_at, datetime):
        timestamp = created_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    elif created_at is None:
        timestamp = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    else:
        timestamp = created_at
    return BulkIdempotencyReceipt(
        idempotency_key=idempotency_key,
        request_fingerprint=bulk_request_fingerprint(request),
        created_at=timestamp,
        metadata=dict(metadata or {}),
    )


def classify_bulk_idempotency(
    receipts: MutableMapping[str, BulkIdempotencyReceipt],
    idempotency_key: str,
    request: BulkOperationRequest,
    *,
    metadata: dict[str, Any] | None = None,
    created_at: datetime | str | None = None,
) -> BulkIdempotencyCheck:
    fingerprint = bulk_request_fingerprint(request)
    existing = receipts.get(idempotency_key)
    if existing is None:
        receipt = create_idempotency_receipt(
            idempotency_key,
            request,
            metadata=metadata,
            created_at=created_at,
        )
        receipts[idempotency_key] = receipt
        return BulkIdempotencyCheck(classification="new", receipt=receipt)
    if existing.request_fingerprint == fingerprint:
        return BulkIdempotencyCheck(classification="replay", receipt=existing)
    return BulkIdempotencyCheck(classification="conflict", receipt=existing)


def _item_dependencies(item: BulkOperationItem) -> list[str]:
    raw = item.payload.get("depends_on", [])
    if raw is None:
        return []
    if isinstance(raw, str):
        return [raw]
    if isinstance(raw, list):
        return [dependency for dependency in raw if isinstance(dependency, str) and dependency]
    return []


def plan_bulk_operation_dependencies(request: BulkOperationRequest) -> BulkExecutionPlan:
    items_by_id = {item.client_id: item for item in request.items}
    diagnostics: list[BulkDependencyDiagnostic] = []
    dependencies: dict[str, list[str]] = {}

    for item in request.items:
        item_dependencies = _item_dependencies(item)
        dependencies[item.client_id] = item_dependencies
        missing = [dependency for dependency in item_dependencies if dependency not in items_by_id]
        if missing:
            diagnostics.append(
                BulkDependencyDiagnostic(
                    type="missing_dependency",
                    client_id=item.client_id,
                    depends_on=missing,
                    message=f"Missing dependency for {item.client_id}: {', '.join(missing)}",
                )
            )

    blocked = {diagnostic.client_id for diagnostic in diagnostics}
    ordered: list[BulkOperationItem] = []
    ordered_ids: set[str] = set()
    remaining = [item.client_id for item in request.items if item.client_id not in blocked]

    while remaining:
        ready = [
            client_id
            for client_id in remaining
            if all(dependency in ordered_ids for dependency in dependencies[client_id])
        ]
        if not ready:
            for client_id in remaining:
                cycle_dependencies = [
                    dependency for dependency in dependencies[client_id] if dependency in remaining
                ]
                diagnostics.append(
                    BulkDependencyDiagnostic(
                        type="cycle",
                        client_id=client_id,
                        depends_on=cycle_dependencies,
                        message=f"Dependency cycle prevents ordering {client_id}",
                    )
                )
            break
        for client_id in ready:
            ordered.append(items_by_id[client_id])
            ordered_ids.add(client_id)
            remaining.remove(client_id)

    return BulkExecutionPlan(
        items=ordered,
        diagnostics=diagnostics,
        executable=not diagnostics,
    )


def item_success(item: BulkOperationItem, *, resource_id: str | None = None) -> BulkItemResult:
    return BulkItemResult(
        client_id=item.client_id,
        operation=item.operation,
        resource_type=item.resource_type,
        status="succeeded",
        resource_id=resource_id,
    )


def item_failure(
    item: BulkOperationItem, *, code: str, message: str, details: dict[str, Any] | None = None
) -> BulkItemResult:
    return BulkItemResult(
        client_id=item.client_id,
        operation=item.operation,
        resource_type=item.resource_type,
        status="failed",
        error={"code": code, "message": message, "details": details or {}},
    )


def item_skipped(item: BulkOperationItem, *, reason: str = "skipped") -> BulkItemResult:
    return BulkItemResult(
        client_id=item.client_id,
        operation=item.operation,
        resource_type=item.resource_type,
        status="skipped",
        error={"code": "skipped", "message": reason, "details": {}},
    )


def build_bulk_result(results: list[BulkItemResult]) -> BulkOperationResult:
    succeeded = sum(1 for result in results if result.status == "succeeded")
    failed = sum(1 for result in results if result.status == "failed")
    skipped = sum(1 for result in results if result.status == "skipped")
    return BulkOperationResult(
        results=results,
        summary=BulkOperationSummary(
            total=len(results),
            succeeded=succeeded,
            failed=failed,
            skipped=skipped,
        ),
        partial_success=succeeded > 0 and (failed > 0 or skipped > 0),
    )
