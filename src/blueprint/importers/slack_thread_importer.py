"""Slack thread transcript importer."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any
import re
import uuid

import yaml

from blueprint.importers.base import SourceImporter


TIMESTAMPED_MESSAGE_RE = re.compile(
    r"^\[(?P<timestamp>[^\]]+)\]\s*(?P<speaker>[^:]{1,80}):\s*(?P<message>.*)$"
)
SPEAKER_MESSAGE_RE = re.compile(r"^(?P<speaker>[A-Za-z][^:\n]{0,79}):\s*(?P<message>.*)$")
HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
ACTION_ITEM_RE = re.compile(
    r"(^|\b)(TODO\b|Action\s*:)|^\s*[-*+]\s*\[[ xX]\]\s+",
    re.IGNORECASE,
)
METADATA_KEYS = {
    "channel",
    "thread",
    "thread url",
    "thread_url",
    "url",
    "permalink",
    "team",
    "workspace",
    "date",
    "title",
    "domain",
    "source_id",
}


def generate_source_brief_id() -> str:
    """Generate a unique source brief ID."""
    return f"sb-{uuid.uuid4().hex[:12]}"


class SlackThreadImporter(SourceImporter):
    """Import Slack thread transcripts from markdown or plain text files."""

    def import_from_source(self, source_id: str) -> dict[str, Any]:
        """Read and normalize a Slack thread transcript file."""
        path = Path(source_id).expanduser()
        if path.suffix.lower() not in {".md", ".markdown", ".txt"}:
            raise ImportError(
                f"Slack thread transcript must be a .md, .markdown, or .txt file: {path}"
            )
        try:
            transcript_text = path.read_text(encoding="utf-8")
        except FileNotFoundError as exc:
            raise ImportError(f"Slack thread transcript not found: {path}") from exc
        except OSError as exc:
            raise ImportError(f"Could not read Slack thread transcript: {path}") from exc

        try:
            return parse_slack_thread_transcript(transcript_text, file_path=str(path))
        except Exception as exc:
            raise ImportError(str(exc)) from exc

    def validate_source(self, source_id: str) -> bool:
        """Check whether a Slack thread transcript can be imported."""
        try:
            self.import_from_source(source_id)
        except Exception:
            return False
        return True

    def list_available(self, limit: int = 50) -> list[dict[str, Any]]:
        """Slack thread imports are file-based and do not support discovery."""
        return []


def parse_slack_thread_transcript(
    transcript_text: str,
    *,
    file_path: str,
) -> dict[str, Any]:
    """Normalize a Slack thread transcript into a SourceBrief dictionary."""
    if not transcript_text.strip():
        raise ValueError("Slack thread transcript is empty")

    resolved_path = str(Path(file_path).expanduser().resolve())
    frontmatter, content = _parse_frontmatter(transcript_text)
    parsed = _parse_content(content)

    metadata = {**parsed["metadata"], **_string_metadata(frontmatter)}
    channel = _optional_string(metadata.get("channel"))
    thread = _optional_string(metadata.get("thread"))
    thread_url = _first_non_empty(
        _optional_string(metadata.get("thread_url")),
        _optional_string(metadata.get("thread url")),
        _optional_string(metadata.get("permalink")),
        _optional_string(metadata.get("url")),
    )
    title = (
        _optional_string(metadata.get("title"))
        or parsed["title"]
        or thread
        or (f"Slack thread in {channel}" if channel else None)
        or _title_from_path(resolved_path)
    )
    body = _build_context_body(parsed["messages"], parsed["body_lines"])
    summary = _first_paragraph(body) or title
    source_id = _optional_string(metadata.get("source_id")) or thread_url or resolved_path

    source_links: dict[str, Any] = {"file_path": resolved_path}
    if thread_url:
        source_links["thread_url"] = thread_url

    source_metadata = {
        "participants": parsed["participants"],
        "timestamps": parsed["timestamps"],
        "channel": channel,
        "thread": thread,
        "thread_url": thread_url,
        "action_items": parsed["action_items"],
        "transcript_metadata": parsed["metadata"],
        "frontmatter": frontmatter,
    }

    now = datetime.utcnow()
    return {
        "id": generate_source_brief_id(),
        "title": title,
        "domain": _optional_string(metadata.get("domain")),
        "summary": summary,
        "source_project": "slack",
        "source_entity_type": "thread_transcript",
        "source_id": source_id,
        "source_payload": _json_safe(
            {
                "file_path": resolved_path,
                "raw_text": transcript_text,
                "content": content,
                "frontmatter": frontmatter,
                "metadata": source_metadata,
                "participants": parsed["participants"],
                "timestamps": parsed["timestamps"],
                "messages": parsed["messages"],
                "action_items": parsed["action_items"],
                "body": body,
                "normalized": {
                    "title": title,
                    "domain": _optional_string(metadata.get("domain")),
                    "summary": summary,
                    "channel": channel,
                    "thread": thread,
                    "thread_url": thread_url,
                    "participants": parsed["participants"],
                    "timestamps": parsed["timestamps"],
                    "action_items": parsed["action_items"],
                    "source_metadata": source_metadata,
                    "source_links": source_links,
                },
            }
        ),
        "source_links": source_links,
        "created_at": now,
        "updated_at": now,
    }


def _parse_content(content: str) -> dict[str, Any]:
    metadata: dict[str, str] = {}
    title: str | None = None
    messages: list[dict[str, Any]] = []
    body_lines: list[str] = []
    participants: list[str] = []
    timestamps: list[str] = []
    action_items: list[dict[str, Any]] = []

    for raw_line in content.splitlines():
        line = raw_line.rstrip()
        stripped = line.strip()
        if not stripped:
            body_lines.append("")
            continue

        heading_match = HEADING_RE.match(stripped)
        if heading_match:
            heading = heading_match.group("title").strip()
            title = title or heading
            body_lines.append(heading)
            continue

        metadata_item = _parse_metadata_line(stripped)
        if metadata_item and not messages:
            key, value = metadata_item
            metadata[key] = value
            continue

        message = _parse_message_line(stripped)
        if message:
            messages.append(message)
            body_lines.append(_format_message(message))
            _append_unique(participants, message["speaker"])
            if message["timestamp"]:
                timestamps.append(message["timestamp"])
            _capture_action_item(action_items, stripped, message)
            continue

        if messages:
            messages[-1]["message"] = f"{messages[-1]['message']}\n{stripped}".strip()
        body_lines.append(stripped)
        _capture_action_item(action_items, stripped, None)

    return {
        "metadata": metadata,
        "title": title,
        "messages": messages,
        "body_lines": body_lines,
        "participants": participants,
        "timestamps": timestamps,
        "action_items": action_items,
    }


def _parse_message_line(line: str) -> dict[str, Any] | None:
    timestamped = TIMESTAMPED_MESSAGE_RE.match(line)
    if timestamped:
        return {
            "timestamp": timestamped.group("timestamp").strip(),
            "speaker": timestamped.group("speaker").strip(),
            "message": timestamped.group("message").strip(),
        }

    speaker = SPEAKER_MESSAGE_RE.match(line)
    if speaker:
        speaker_name = speaker.group("speaker").strip()
        if speaker_name.casefold() not in {*METADATA_KEYS, "action", "todo"}:
            return {
                "timestamp": None,
                "speaker": speaker_name,
                "message": speaker.group("message").strip(),
            }
    return None


def _parse_metadata_line(line: str) -> tuple[str, str] | None:
    cleaned = line.lstrip("-* ").replace("**", "")
    if ":" not in cleaned:
        return None
    key, value = cleaned.split(":", 1)
    normalized_key = key.strip().casefold()
    if normalized_key not in METADATA_KEYS:
        return None
    return normalized_key.replace(" ", "_"), value.strip()


def _capture_action_item(
    action_items: list[dict[str, Any]],
    line: str,
    message: dict[str, Any] | None,
) -> None:
    if not ACTION_ITEM_RE.search(line):
        return
    action_items.append(
        {
            "text": _strip_action_marker(message["message"] if message else line),
            "raw": line,
            "speaker": message["speaker"] if message else None,
            "timestamp": message["timestamp"] if message else None,
        }
    )


def _strip_action_marker(text: str) -> str:
    stripped = text.strip()
    stripped = re.sub(r"^\s*[-*+]\s*\[[ xX]\]\s+", "", stripped)
    stripped = re.sub(r"^\s*(TODO\b|Action\s*:)\s*", "", stripped, flags=re.IGNORECASE)
    return stripped.strip()


def _format_message(message: dict[str, Any]) -> str:
    timestamp = f"[{message['timestamp']}] " if message["timestamp"] else ""
    return f"{timestamp}{message['speaker']}: {message['message']}".strip()


def _build_context_body(messages: list[dict[str, Any]], body_lines: list[str]) -> str:
    if messages:
        return "\n".join(_format_message(message) for message in messages).strip()
    return "\n".join(body_lines).strip()


def _parse_frontmatter(transcript_text: str) -> tuple[dict[str, Any], str]:
    stripped = transcript_text.lstrip()
    if not stripped.startswith("---\n"):
        return {}, transcript_text.strip()

    closing = stripped.find("\n---\n", 4)
    if closing == -1:
        return {}, transcript_text.strip()

    raw_metadata = stripped[4:closing]
    content = stripped[closing + len("\n---\n") :].strip()
    try:
        metadata = yaml.safe_load(raw_metadata) or {}
    except yaml.YAMLError as exc:
        raise ValueError(f"Invalid YAML frontmatter: {exc}") from exc
    if not isinstance(metadata, dict):
        return {}, content
    return _json_safe(metadata), content


def _string_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    return {str(key).casefold().replace(" ", "_"): value for key, value in metadata.items()}


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        return value or None
    return str(value).strip() or None


def _first_non_empty(*values: str | None) -> str | None:
    for value in values:
        if value:
            return value
    return None


def _first_paragraph(text: str) -> str | None:
    for paragraph in re.split(r"\n\s*\n", text.strip()):
        normalized = " ".join(line.strip() for line in paragraph.splitlines()).strip()
        if normalized:
            return normalized
    return None


def _append_unique(values: list[str], value: str) -> None:
    if value not in values:
        values.append(value)


def _title_from_path(path: str) -> str:
    return (
        Path(path).stem.replace("_", " ").replace("-", " ").strip().title()
        or "Slack Thread"
    )


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value
