import json
from pathlib import Path

import pytest

from blueprint.llm.json_parser import LLMJsonParseError, parse_json_response


def test_parse_json_response_accepts_raw_json():
    assert parse_json_response('{"title": "Build parser", "count": 1}') == {
        "title": "Build parser",
        "count": 1,
    }


def test_parse_json_response_extracts_json_fenced_block():
    content = """Here is the payload:
```json
{"title": "Build parser", "count": 2}
```"""

    assert parse_json_response(content) == {
        "title": "Build parser",
        "count": 2,
    }


def test_parse_json_response_extracts_generic_fenced_block():
    content = """```
{"title": "Build parser", "count": 3}
```"""

    assert parse_json_response(content) == {
        "title": "Build parser",
        "count": 3,
    }


def test_parse_json_response_extracts_object_from_surrounding_commentary():
    content = """Sure, here is the requested JSON:
{"title": "Build parser", "nested": {"count": 4}}
This should meet the schema."""

    assert parse_json_response(content) == {
        "title": "Build parser",
        "nested": {"count": 4},
    }


def test_parse_json_response_failure_includes_debug_file_and_original_error():
    content = "This is not JSON."

    with pytest.raises(LLMJsonParseError) as exc_info:
        parse_json_response(content, context="brief generation")

    error = exc_info.value
    assert isinstance(error.original_error, json.JSONDecodeError)
    assert "brief generation" in str(error)
    assert "Last stage:" in str(error)
    assert "Last error:" in str(error)
    assert "Response saved to:" in str(error)
    assert error.debug_file
    assert Path(error.debug_file).read_text() == content


def test_parse_json_response_can_skip_debug_file_writing():
    content = "This is not JSON."

    with pytest.raises(LLMJsonParseError) as exc_info:
        parse_json_response(
            content,
            context="plan generation",
            write_debug_file=False,
        )

    error = exc_info.value
    assert isinstance(error.original_error, json.JSONDecodeError)
    assert "plan generation" in str(error)
    assert "Response debug file was not written." in str(error)
    assert error.debug_file is None
