"""ETag and conditional request helpers."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class ConditionalDecision(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["ok", "not_modified", "precondition_failed"]
    status_code: int
    etag: str = Field(min_length=1)
    should_return_body: bool = True
    reason: str = ""


def generate_etag(resource: Any, *, weak: bool = False) -> str:
    raw = json.dumps(resource, sort_keys=True, separators=(",", ":"), default=str).encode()
    digest = hashlib.sha256(raw).hexdigest()
    tag = f'"{digest}"'
    return f"W/{tag}" if weak else tag


def _parse_etags(header: str | None) -> set[str]:
    if not header:
        return set()
    return {part.strip() for part in header.split(",") if part.strip()}


def _matches(header: str | None, current_etag: str) -> bool:
    tags = _parse_etags(header)
    return "*" in tags or current_etag in tags


def evaluate_if_none_match(header: str | None, current_etag: str) -> ConditionalDecision:
    if _matches(header, current_etag):
        return ConditionalDecision(
            status="not_modified",
            status_code=304,
            etag=current_etag,
            should_return_body=False,
            reason="If-None-Match matched current ETag",
        )
    return ConditionalDecision(status="ok", status_code=200, etag=current_etag)


def evaluate_if_match(header: str | None, current_etag: str) -> ConditionalDecision:
    if header and not _matches(header, current_etag):
        return ConditionalDecision(
            status="precondition_failed",
            status_code=412,
            etag=current_etag,
            should_return_body=True,
            reason="If-Match did not match current ETag",
        )
    return ConditionalDecision(status="ok", status_code=200, etag=current_etag)

