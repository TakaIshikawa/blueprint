"""Shared JSON extraction helpers for LLM responses."""

from __future__ import annotations

import json
import re
import tempfile
from dataclasses import dataclass
from typing import Any


class LLMJsonParseError(ValueError):
    """Raised when an LLM response cannot be parsed as JSON."""

    def __init__(
        self,
        message: str,
        *,
        original_error: Exception | None,
        debug_file: str | None,
    ) -> None:
        self.original_error = original_error
        self.debug_file = debug_file
        super().__init__(message)


@dataclass(frozen=True)
class JsonCandidate:
    """One candidate substring extracted from an LLM response."""

    stage: str
    content: str


def parse_json_response(
    content: str,
    *,
    context: str = "LLM response",
    write_debug_file: bool = True,
) -> dict[str, Any]:
    """
    Parse JSON from an LLM response using common response extraction strategies.

    The parser accepts raw JSON, fenced ``json`` blocks, generic fenced blocks,
    and JSON objects surrounded by commentary. It does not mutate malformed JSON.
    """
    last_error: json.JSONDecodeError | None = None
    last_stage = "no candidates"

    for candidate in build_json_candidates(content):
        try:
            parsed = json.loads(candidate.content)
        except json.JSONDecodeError as exc:
            last_error = exc
            last_stage = candidate.stage
            continue

        if not isinstance(parsed, dict):
            debug_file = _maybe_write_debug_response(content, write_debug_file)
            raise LLMJsonParseError(
                f"Failed to parse LLM response for {context} as a JSON object.\n"
                f"Last stage: {candidate.stage}\n"
                f"Last error: parsed JSON was {type(parsed).__name__}\n"
                f"{_debug_file_message(debug_file)}",
                original_error=None,
                debug_file=debug_file,
            )
        return parsed

    debug_file = _maybe_write_debug_response(content, write_debug_file)
    message = (
        f"Failed to parse LLM response for {context} as JSON after trying "
        "multiple strategies.\n"
        f"Last stage: {last_stage}\n"
        f"Last error: {last_error}\n"
        f"{_debug_file_message(debug_file)}"
    )
    raise LLMJsonParseError(
        message,
        original_error=last_error,
        debug_file=debug_file,
    )


def build_json_candidates(content: str) -> list[JsonCandidate]:
    """Build parse candidates from raw LLM response text."""
    candidates: list[JsonCandidate] = []
    seen: set[str] = set()

    def add_candidate(stage: str, candidate: str | None) -> None:
        if candidate is None:
            return
        normalized = candidate.strip()
        if not normalized or normalized in seen:
            return
        seen.add(normalized)
        candidates.append(JsonCandidate(stage=stage, content=normalized))

    add_candidate("raw response", content)

    for index, block in enumerate(_extract_json_fenced_blocks(content), start=1):
        add_candidate(f"fenced json block {index}", block)

    for index, block in enumerate(_extract_generic_fenced_blocks(content), start=1):
        add_candidate(f"generic fenced block {index}", block)

    add_candidate("object boundary extraction", _extract_balanced_object(content))

    return candidates


def write_debug_response(content: str) -> str:
    """Persist an unrecoverable LLM response for later inspection."""
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as handle:
        handle.write(content)
        return handle.name


def _maybe_write_debug_response(content: str, enabled: bool) -> str | None:
    """Persist debug content when requested."""
    if not enabled:
        return None
    return write_debug_response(content)


def _debug_file_message(debug_file: str | None) -> str:
    """Format the debug-file portion of an error message."""
    if debug_file is None:
        return "Response debug file was not written."
    return f"Response saved to: {debug_file}"


def _extract_json_fenced_blocks(content: str) -> list[str]:
    """Extract markdown code blocks explicitly tagged as JSON."""
    pattern = re.compile(r"```json\s*([\s\S]*?)\s*```", re.IGNORECASE | re.MULTILINE)
    return [match.group(1).strip() for match in pattern.finditer(content)]


def _extract_generic_fenced_blocks(content: str) -> list[str]:
    """Extract markdown code blocks that are not explicitly tagged."""
    pattern = re.compile(r"```\s*([\s\S]*?)\s*```", re.MULTILINE)
    return [match.group(1).strip() for match in pattern.finditer(content)]


def _extract_balanced_object(content: str) -> str | None:
    """Extract the first balanced JSON object from a larger response."""
    start = content.find("{")
    if start == -1:
        return None

    depth = 0
    in_string = False
    escape = False

    for index in range(start, len(content)):
        char = content[index]

        if in_string:
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == '"':
                in_string = False
            continue

        if char == '"':
            in_string = True
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return content[start : index + 1].strip()

    return None
