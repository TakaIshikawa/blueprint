"""Tests for export checksum summaries."""

import hashlib

import pytest

from blueprint.export import build_export_checksum_summary


def test_checksum_summary_hashes_export_rows_with_sha256_by_default():
    rows = [
        {"id": "task-2", "title": "Second", "done": False},
        {"id": "task-1", "title": "First", "done": True},
    ]

    summary = build_export_checksum_summary(reversed(rows))

    assert summary["algorithm"] == "sha256"
    assert summary["output_count"] == 2
    assert [entry["output_id"] for entry in summary["checksums"]] == ["task-1", "task-2"]
    assert summary == build_export_checksum_summary(rows)
    first = summary["checksums"][0]
    assert first["name"] == "task-1"
    assert first["size_bytes"] > 0
    assert len(first["checksum"]) == 64
    assert len(summary["combined_checksum"]) == 64


def test_checksum_summary_supports_md5_for_file_descriptors():
    outputs = [
        {"path": "exports/users.json", "content": b'{"users":[]}'},
        {"path": "exports/tasks.json", "data": "task rows"},
    ]

    summary = build_export_checksum_summary(outputs, algorithm="md5")

    assert summary["algorithm"] == "md5"
    by_name = {entry["name"]: entry for entry in summary["checksums"]}
    assert by_name["exports/users.json"]["checksum"] == hashlib.md5(b'{"users":[]}').hexdigest()
    assert by_name["exports/users.json"]["size_bytes"] == len(b'{"users":[]}')
    assert by_name["exports/tasks.json"]["checksum"] == hashlib.md5(b"task rows").hexdigest()
    assert len(summary["combined_checksum"]) == 32


def test_checksum_summary_uses_precomputed_descriptor_checksum_without_secret_values():
    summary = build_export_checksum_summary(
        [
            {
                "filename": "bundle.zip",
                "size_bytes": "128",
                "checksums": {"sha256": "abc123"},
                "token": "not-read-when-checksum-present",
            }
        ]
    )

    assert summary["total_size_bytes"] == 128
    assert summary["checksums"] == [
        {
            "output_id": "bundle.zip",
            "name": "bundle.zip",
            "size_bytes": 128,
            "checksum": "abc123",
        }
    ]


def test_empty_checksum_summary_is_stable():
    summary = build_export_checksum_summary([])

    assert summary == {
        "algorithm": "sha256",
        "output_count": 0,
        "total_size_bytes": 0,
        "checksums": [],
        "combined_checksum": hashlib.sha256(b"[]").hexdigest(),
    }


def test_checksum_summary_rejects_unsupported_algorithms():
    with pytest.raises(ValueError, match="md5, sha256"):
        build_export_checksum_summary([], algorithm="sha1")
