"""Helpers for resolving task file paths against CODEOWNERS rules."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Iterable, Sequence


@dataclass(frozen=True)
class CodeownersRule:
    """A parsed CODEOWNERS rule."""

    pattern: str
    owners: tuple[str, ...]
    line_number: int


def parse_codeowners(text: str) -> list[CodeownersRule]:
    """Parse CODEOWNERS text into deterministic rules.

    Blank lines, leading whitespace, and full-line comments are ignored. Invalid
    ownerless lines are skipped because they cannot resolve a path to reviewers.
    """
    rules: list[CodeownersRule] = []
    for line_number, raw_line in enumerate(text.splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        parts = line.split()
        if len(parts) < 2:
            continue

        pattern, *owners = parts
        rules.append(
            CodeownersRule(
                pattern=pattern,
                owners=tuple(owners),
                line_number=line_number,
            )
        )
    return rules


def load_codeowners(path: str | Path) -> list[CodeownersRule]:
    """Load and parse a CODEOWNERS file."""
    return parse_codeowners(Path(path).read_text(encoding="utf-8"))


def resolve_codeowners(
    files: Sequence[str],
    rules: Iterable[CodeownersRule],
) -> dict[str, list[str]]:
    """Resolve owners for file paths using last matching rule precedence."""
    parsed_rules = list(rules)
    resolved: dict[str, list[str]] = {}

    for file_path in files:
        normalized_path = _normalize_path(file_path)
        owners: tuple[str, ...] = ()
        for rule in parsed_rules:
            if _matches(rule.pattern, normalized_path):
                owners = rule.owners
        resolved[normalized_path] = list(owners)

    return resolved


def _matches(pattern: str, path: str) -> bool:
    normalized_pattern = _normalize_pattern(pattern)
    if not normalized_pattern:
        return False

    if normalized_pattern.endswith("/"):
        directory = normalized_pattern.rstrip("/")
        return path == directory or path.startswith(f"{directory}/")

    if "/" not in normalized_pattern:
        return _glob_matches(normalized_pattern, Path(path).name)

    return _glob_matches(normalized_pattern, path)


def _normalize_path(path: str) -> str:
    normalized = path.strip().replace("\\", "/")
    while normalized.startswith("./"):
        normalized = normalized[2:]
    normalized = normalized.lstrip("/")
    normalized = re.sub(r"/+", "/", normalized)
    return normalized.rstrip("/")


def _normalize_pattern(pattern: str) -> str:
    normalized = pattern.strip().replace("\\", "/")
    while normalized.startswith("./"):
        normalized = normalized[2:]
    normalized = normalized.lstrip("/")
    normalized = re.sub(r"/+", "/", normalized)
    return normalized


def _glob_matches(pattern: str, path: str) -> bool:
    return re.fullmatch(_glob_to_regex(pattern), path) is not None


def _glob_to_regex(pattern: str) -> str:
    regex = ""
    index = 0
    while index < len(pattern):
        char = pattern[index]
        if char == "*":
            if index + 1 < len(pattern) and pattern[index + 1] == "*":
                if index + 2 < len(pattern) and pattern[index + 2] == "/":
                    regex += "(?:.*/)?"
                    index += 3
                else:
                    regex += ".*"
                    index += 2
            else:
                regex += "[^/]*"
                index += 1
        elif char == "?":
            regex += "[^/]"
            index += 1
        else:
            regex += re.escape(char)
            index += 1
    return regex


__all__ = [
    "CodeownersRule",
    "load_codeowners",
    "parse_codeowners",
    "resolve_codeowners",
]
