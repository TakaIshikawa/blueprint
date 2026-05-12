"""Tests for export validation summaries."""

from dataclasses import dataclass

from blueprint.export import build_export_validation_summary


@dataclass(frozen=True)
class Finding:
    severity: str
    code: str
    message: str
    location: str


def test_validation_summary_counts_severities_and_codes_from_mixed_inputs():
    findings = [
        {"severity": "error", "code": "missing_required", "message": "Name is missing", "location": "rows[0].name"},
        Finding("warning", "extra_column", "Legacy column exported", "rows[0].legacy"),
        {"severity": "ERROR", "code": "missing_required"},
    ]

    summary = build_export_validation_summary(findings)

    assert summary["total"] == 3
    assert summary["passed"] is False
    assert summary["severity_counts"] == {"error": 2, "warning": 1}
    assert summary["code_counts"] == {"extra_column": 1, "missing_required": 2}


def test_validation_summary_includes_blocking_findings_in_deterministic_order():
    findings = [
        {"severity": "error", "code": "schema", "message": "Bad schema", "location": "manifest"},
        {"severity": "critical", "code": "checksum", "message": "Checksum mismatch", "location": "bundle"},
        {"severity": "warning", "code": "nullable", "message": "Optional field is empty", "location": "rows[1].note"},
        {"severity": "error", "code": "alpha", "message": "Earlier code"},
    ]

    summary = build_export_validation_summary(findings)

    assert summary["blocking_findings"] == [
        {
            "severity": "critical",
            "code": "checksum",
            "message": "Checksum mismatch",
            "location": "bundle",
        },
        {"severity": "error", "code": "alpha", "message": "Earlier code"},
        {
            "severity": "error",
            "code": "schema",
            "message": "Bad schema",
            "location": "manifest",
        },
    ]


def test_validation_summary_passes_when_no_blocking_severities_are_present():
    summary = build_export_validation_summary(
        [
            {"severity": "warning", "code": "extra_column"},
            {"severity": "info", "code": "row_count"},
        ]
    )

    assert summary == {
        "total": 2,
        "passed": True,
        "severity_counts": {"info": 1, "warning": 1},
        "code_counts": {"extra_column": 1, "row_count": 1},
        "blocking_findings": [],
    }


def test_validation_summary_empty_input_is_passing():
    assert build_export_validation_summary([]) == {
        "total": 0,
        "passed": True,
        "severity_counts": {},
        "code_counts": {},
        "blocking_findings": [],
    }
