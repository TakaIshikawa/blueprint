"""API error taxonomy and redaction helpers."""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError


class ErrorCategory(str, Enum):
    VALIDATION = "validation"
    AUTHENTICATION = "authentication"
    AUTHORIZATION = "authorization"
    NOT_FOUND = "not_found"
    CONFLICT = "conflict"
    RATE_LIMIT = "rate_limit"
    INTERNAL = "internal"


STATUS_BY_CATEGORY = {
    ErrorCategory.VALIDATION: 422,
    ErrorCategory.AUTHENTICATION: 401,
    ErrorCategory.AUTHORIZATION: 403,
    ErrorCategory.NOT_FOUND: 404,
    ErrorCategory.CONFLICT: 409,
    ErrorCategory.RATE_LIMIT: 429,
    ErrorCategory.INTERNAL: 500,
}
SENSITIVE_KEYS = {"token", "password", "secret", "api_key"}
REDACTED = "[REDACTED]"


class APIErrorPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: str = Field(min_length=1)
    category: ErrorCategory
    message: str = Field(min_length=1)
    status: int = Field(ge=100, le=599)
    details: dict[str, Any] = Field(default_factory=dict)


def redact_sensitive(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: REDACTED if key.lower() in SENSITIVE_KEYS else redact_sensitive(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact_sensitive(item) for item in value]
    if isinstance(value, tuple):
        return tuple(redact_sensitive(item) for item in value)
    return value


def api_error(
    code: str,
    message: str,
    *,
    category: ErrorCategory | str = ErrorCategory.INTERNAL,
    status: int | None = None,
    details: dict[str, Any] | None = None,
) -> APIErrorPayload:
    cat = ErrorCategory(category)
    return APIErrorPayload(
        code=code,
        category=cat,
        message=message,
        status=status or STATUS_BY_CATEGORY[cat],
        details=redact_sensitive(details or {}),
    )


def validation_error(details: dict[str, Any] | list[dict[str, Any]]) -> APIErrorPayload:
    safe_details = {"fields": details} if isinstance(details, list) else details
    return api_error(
        "validation_error",
        "Request validation failed",
        category=ErrorCategory.VALIDATION,
        details=safe_details,
    )


def error_from_exception(exc: Exception) -> APIErrorPayload:
    if isinstance(exc, ValidationError):
        return validation_error(exc.errors())
    return api_error(
        "internal_error",
        "An internal error occurred",
        category=ErrorCategory.INTERNAL,
        details={},
    )

