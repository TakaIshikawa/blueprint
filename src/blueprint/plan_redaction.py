"""Utilities for redacting sensitive values from plan handoff payloads."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
import re
from typing import Any, Pattern


PatternInput = str | Pattern[str]
RedactionReplacement = str | Callable[[re.Match[str]], str]


@dataclass(frozen=True)
class _Redactor:
    pattern: Pattern[str]
    replacement: RedactionReplacement


_EMAIL_RE = re.compile(
    r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b",
    re.IGNORECASE,
)
_TOKEN_ASSIGNMENT_RE = re.compile(
    r"(?P<prefix>(?<![\w-])[\"']?(?:"
    r"[A-Z0-9_]*(?:API[_-]?KEY|TOKEN|SECRET|PASSWORD)"
    r"|API[_-]?KEY|ACCESS[_-]?TOKEN|AUTH[_-]?TOKEN|REFRESH[_-]?TOKEN"
    r"|BEARER[_-]?TOKEN|CLIENT[_-]?SECRET|SECRET|PASSWORD|TOKEN"
    r")[\"']?\s*[:=]\s*)"
    r"(?P<quote>[\"']?)"
    r"(?P<value>[^\s\"',;}]+)"
    r"(?P=quote)",
    re.IGNORECASE,
)
_GITHUB_TOKEN_RE = re.compile(r"\b(?:gh[opusr]_[A-Za-z0-9_]{20,}|github_pat_[A-Za-z0-9_]{20,})\b")
_SLACK_WEBHOOK_RE = re.compile(r"https://hooks\.slack\.com/services/[A-Za-z0-9/_-]+")
_OPENAI_API_KEY_RE = re.compile(r"\bsk-(?:proj-)?[A-Za-z0-9_-]{20,}\b")
_ANTHROPIC_API_KEY_RE = re.compile(r"\bsk-ant-[A-Za-z0-9_-]{20,}\b")
_AWS_ACCESS_KEY_RE = re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b")
_GOOGLE_API_KEY_RE = re.compile(r"\bAIza[0-9A-Za-z_-]{35,}\b")


def redact_plan_payload(
    payload: Any,
    patterns: list[PatternInput] | tuple[PatternInput, ...] | None = None,
    replacement: str = "[REDACTED]",
) -> Any:
    """Return a redacted copy of a nested plan payload.

    String values are redacted inside dictionaries and lists. Mapping keys and
    non-string scalar values are preserved.
    """
    redactors = _redactors(patterns, replacement)
    return _redact_value(payload, redactors)


def redact_text(
    text: str,
    patterns: list[PatternInput] | tuple[PatternInput, ...] | None = None,
    replacement: str = "[REDACTED]",
) -> str:
    """Redact sensitive values from a text string."""
    redacted = text
    for redactor in _redactors(patterns, replacement):
        redacted = redactor.pattern.sub(redactor.replacement, redacted)
    return redacted


def _redact_value(value: Any, redactors: list[_Redactor]) -> Any:
    if isinstance(value, Mapping):
        return {
            item_key: _redact_value(item_value, redactors) for item_key, item_value in value.items()
        }
    if isinstance(value, list):
        return [_redact_value(item, redactors) for item in value]
    if isinstance(value, str):
        redacted = value
        for redactor in redactors:
            redacted = redactor.pattern.sub(redactor.replacement, redacted)
        return redacted
    return value


def _redactors(
    patterns: list[PatternInput] | tuple[PatternInput, ...] | None,
    replacement: str,
) -> list[_Redactor]:
    redactors = [
        _Redactor(_TOKEN_ASSIGNMENT_RE, _assignment_replacement(replacement)),
        _Redactor(_SLACK_WEBHOOK_RE, replacement),
        _Redactor(_EMAIL_RE, replacement),
        _Redactor(_GITHUB_TOKEN_RE, replacement),
        _Redactor(_OPENAI_API_KEY_RE, replacement),
        _Redactor(_ANTHROPIC_API_KEY_RE, replacement),
        _Redactor(_AWS_ACCESS_KEY_RE, replacement),
        _Redactor(_GOOGLE_API_KEY_RE, replacement),
    ]
    redactors.extend(
        _Redactor(_compile_pattern(pattern), replacement) for pattern in patterns or ()
    )
    return redactors


def _assignment_replacement(replacement: str) -> Callable[[re.Match[str]], str]:
    def replace(match: re.Match[str]) -> str:
        quote = match.group("quote")
        return f"{match.group('prefix')}{quote}{replacement}{quote}"

    return replace


def _compile_pattern(pattern: PatternInput) -> Pattern[str]:
    if isinstance(pattern, str):
        return re.compile(pattern)
    return pattern


__all__ = [
    "redact_plan_payload",
    "redact_text",
]
