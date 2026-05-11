"""API response envelope models and builders."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field


def new_request_id(prefix: str = "req") -> str:
    """Return an opaque request identifier suitable for response tracing."""
    return f"{prefix}_{uuid4().hex}"


def utc_timestamp() -> str:
    """Return the current UTC timestamp as an ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()


class PaginationMetadata(BaseModel):
    """Common pagination fields carried in response metadata."""

    model_config = ConfigDict(extra="forbid")

    next_cursor: str | None = None
    previous_cursor: str | None = None
    has_next: bool = False
    has_previous: bool = False
    limit: int | None = Field(default=None, ge=1)
    total_count: int | None = Field(default=None, ge=0)


class ResponseMetadata(BaseModel):
    """Envelope metadata that does not alter the response data payload."""

    model_config = ConfigDict(extra="forbid")

    request_id: str = Field(min_length=1)
    pagination: PaginationMetadata | None = None
    extra: dict[str, Any] = Field(default_factory=dict)


class APIErrorObject(BaseModel):
    """Structured API error payload."""

    model_config = ConfigDict(extra="forbid")

    code: str = Field(min_length=1)
    message: str = Field(min_length=1)
    details: dict[str, Any] = Field(default_factory=dict)
    request_id: str = Field(min_length=1)


class APISuccessResponse(BaseModel):
    """Standard success response envelope."""

    model_config = ConfigDict(extra="forbid")

    data: Any
    metadata: ResponseMetadata
    request_id: str = Field(min_length=1)
    status: Literal["success"] = "success"
    timestamp: str = Field(min_length=1)


class APIErrorResponse(BaseModel):
    """Standard error response envelope."""

    model_config = ConfigDict(extra="forbid")

    error: APIErrorObject
    metadata: ResponseMetadata
    request_id: str = Field(min_length=1)
    status: Literal["error"] = "error"
    timestamp: str = Field(min_length=1)


def build_metadata(
    *,
    request_id: str | None = None,
    pagination: PaginationMetadata | dict[str, Any] | None = None,
    extra: dict[str, Any] | None = None,
) -> ResponseMetadata:
    rid = request_id or new_request_id()
    page = (
        pagination
        if isinstance(pagination, PaginationMetadata) or pagination is None
        else PaginationMetadata(**pagination)
    )
    return ResponseMetadata(request_id=rid, pagination=page, extra=extra or {})


def success_response(
    data: Any,
    *,
    request_id: str | None = None,
    metadata: dict[str, Any] | ResponseMetadata | None = None,
    pagination: PaginationMetadata | dict[str, Any] | None = None,
    timestamp: str | None = None,
) -> APISuccessResponse:
    """Build a success envelope without mutating the data payload."""
    if isinstance(metadata, ResponseMetadata):
        meta = metadata
        rid = request_id or meta.request_id
    else:
        rid = request_id or new_request_id()
        meta = build_metadata(request_id=rid, pagination=pagination, extra=metadata)
    return APISuccessResponse(
        data=data,
        metadata=meta,
        request_id=rid,
        timestamp=timestamp or utc_timestamp(),
    )


def error_response(
    *,
    code: str,
    message: str,
    details: dict[str, Any] | None = None,
    request_id: str | None = None,
    metadata: dict[str, Any] | ResponseMetadata | None = None,
    timestamp: str | None = None,
) -> APIErrorResponse:
    """Build an error envelope with a structured error object."""
    if isinstance(metadata, ResponseMetadata):
        meta = metadata
        rid = request_id or meta.request_id
    else:
        rid = request_id or new_request_id()
        meta = build_metadata(request_id=rid, extra=metadata)
    return APIErrorResponse(
        error=APIErrorObject(
            code=code,
            message=message,
            details=details or {},
            request_id=rid,
        ),
        metadata=meta,
        request_id=rid,
        timestamp=timestamp or utc_timestamp(),
    )


__all__ = [
    "APIErrorObject",
    "APIErrorResponse",
    "APISuccessResponse",
    "PaginationMetadata",
    "ResponseMetadata",
    "build_metadata",
    "error_response",
    "new_request_id",
    "success_response",
    "utc_timestamp",
]

