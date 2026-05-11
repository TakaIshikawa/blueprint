"""Storage-agnostic bulk operation request and result models."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

OperationType = Literal["create", "update", "delete"]


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


def item_success(item: BulkOperationItem, *, resource_id: str | None = None) -> BulkItemResult:
    return BulkItemResult(
        client_id=item.client_id,
        operation=item.operation,
        resource_type=item.resource_type,
        status="succeeded",
        resource_id=resource_id,
    )


def item_failure(item: BulkOperationItem, *, code: str, message: str, details: dict[str, Any] | None = None) -> BulkItemResult:
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

