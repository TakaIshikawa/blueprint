import json

from blueprint.domain.models import SourceBrief
from blueprint.source_assumption_volatility import (
    SourceAssumptionVolatilityRecord,
    SourceAssumptionVolatilityReport,
    build_source_assumption_volatility_report,
    source_assumption_volatility_report_to_dict,
    source_assumption_volatility_report_to_markdown,
)


def test_explicit_and_external_pending_assumptions_are_scored_with_evidence():
    report = build_source_assumption_volatility_report(
        _source_brief(
            source_id="sb-high",
            summary="Assume the vendor API limit will be raised before launch.",
            source_payload={
                "open_questions": [
                    "Is legal approval pending for pricing changes?",
                    "Can support update the help copy?",
                ],
                "metadata": {"last_reviewed_at": "2025-01-10"},
            },
        ),
        today="2026-05-01",
    )

    assert [record.assumption for record in report.records][:2] == [
        "Is legal approval pending for pricing changes?",
        "Assume the vendor API limit will be raised before launch.",
    ]
    assert [record.volatility for record in report.records][:2] == ["high", "high"]
    assert report.records[0].score > report.records[1].score
    assert "external dependency" in report.records[1].evidence
    assert any("stale" in evidence for evidence in report.records[0].evidence)
    assert report.records[1].follow_up_actions == (
        "Confirm or replace the assumption with dated source evidence before planning.",
        "Get written confirmation from the external owner or vendor.",
    )
    assert report.records[0].evidence_type == "unresolved_question"
    assert "Resolve the pending question and record the decision owner." in (
        report.records[0].follow_up_actions
    )
    assert all(isinstance(record, SourceAssumptionVolatilityRecord) for record in report.records)


def test_empty_brief_returns_empty_report_summary_and_markdown():
    report = build_source_assumption_volatility_report(
        _source_brief(
            summary="Checkout retry flow has concrete requirements.",
            source_payload={"acceptance_criteria": ["Retry records an audit event."]},
        )
    )

    assert report.records == ()
    assert report.summary_counts == {"high": 0, "medium": 0, "low": 0}
    assert report.to_dict() == {
        "source_count": 1,
        "assumption_count": 0,
        "summary_counts": {"high": 0, "medium": 0, "low": 0},
        "records": [],
    }
    assert report.to_markdown() == "\n".join(
        [
            "# Source Assumption Volatility",
            "",
            "Summary: 0 high, 0 medium, 0 low.",
            "",
            "No volatile source assumptions found.",
        ]
    )


def test_metadata_driven_assumptions_are_detected_from_dict_inputs():
    report = build_source_assumption_volatility_report(
        {
            **_source_brief(source_id="sb-meta"),
            "metadata": {
                "assumptions": [
                    "Staffing estimate assumes one backend engineer is available.",
                    "Release date is subject to vendor certification.",
                ],
            },
        }
    )

    assert [record.assumption for record in report.records] == [
        "Release date is subject to vendor certification.",
        "Staffing estimate assumes one backend engineer is available.",
    ]
    assert [record.volatility for record in report.records] == ["high", "medium"]
    assert report.records[0].source_fields == ("metadata.assumptions[1]",)
    assert report.records[0].evidence_type == "external_dependency"


def test_payload_driven_assumptions_and_sourcebrief_iterables_are_supported():
    source_model = SourceBrief.model_validate(
        _source_brief(
            source_id="sb-model",
            source_payload={
                "normalized": {
                    "assumptions": [
                        {
                            "assumption": (
                                "Assume pricing remains unchanged through launch."
                            )
                        }
                    ],
                    "delivery": "Implementation depends on staffing availability.",
                }
            },
        )
    )

    report = build_source_assumption_volatility_report([source_model])

    assert report.source_count == 1
    assert [record.assumption for record in report.records] == [
        "Assume pricing remains unchanged through launch.",
        "Implementation depends on staffing availability.",
    ]
    assert report.records[0].evidence_type == "external_dependency"
    assert source_assumption_volatility_report_to_dict(report) == report.to_dict()
    payload = report.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["source_count", "assumption_count", "summary_counts", "records"]
    assert list(payload["records"][0]) == [
        "source_brief_id",
        "assumption",
        "volatility",
        "score",
        "evidence_type",
        "evidence",
        "source_fields",
        "follow_up_actions",
    ]


def test_markdown_assumptions_are_extracted_and_rendered_stably():
    report = build_source_assumption_volatility_report(
        _source_brief(
            source_id="sb-md",
            summary="\n".join(
                [
                    "## Notes",
                    "- Assumption: deadline remains May 15.",
                    "- TBD: vendor sandbox credentials.",
                    "- Confirm copy after design review.",
                ]
            ),
        )
    )

    assert [record.assumption for record in report.records] == [
        "Assumption: deadline remains May 15.",
        "TBD: vendor sandbox credentials.",
    ]
    assert source_assumption_volatility_report_to_markdown(report) == "\n".join(
        [
            "# Source Assumption Volatility",
            "",
            "Summary: 1 high, 1 medium, 0 low.",
            "",
            "| Volatility | Score | Source | Assumption | Evidence | Follow-up |",
            "| --- | --- | --- | --- | --- | --- |",
            (
                "| high | 11 | sb-md | Assumption: deadline remains May 15. | "
                "explicit assumption; external dependency; pending signal; "
                "Source field: summary | Confirm or replace the assumption with dated source "
                "evidence before planning.; Get written confirmation from the external owner "
                "or vendor.; Resolve the pending question and record the decision owner.; "
                "Escalate commercial, legal, schedule, or staffing risk to the owner. |"
            ),
            (
                "| medium | 8 | sb-md | TBD: vendor sandbox credentials. | "
                "external dependency; pending signal; Source field: summary |  |"
            ),
        ]
    )


def test_summary_counts_and_deterministic_high_risk_ordering_across_iterables():
    report = build_source_assumption_volatility_report(
        [
            _source_brief(
                source_id="sb-low",
                summary="Assume the existing button copy is acceptable.",
            ),
            _source_brief(
                source_id="sb-high",
                source_payload={
                    "open_questions": [
                        "Is vendor pricing approval pending before the deadline?"
                    ],
                },
            ),
            _source_brief(
                source_id="sb-medium",
                source_payload={
                    "notes": ["Implementation depends on staffing availability."]
                },
            ),
        ]
    )

    assert isinstance(report, SourceAssumptionVolatilityReport)
    assert report.source_count == 3
    assert report.summary_counts == {"high": 1, "medium": 1, "low": 1}
    assert [record.source_brief_id for record in report.records] == [
        "sb-high",
        "sb-medium",
        "sb-low",
    ]
    assert [record.volatility for record in report.records] == ["high", "medium", "low"]


def _source_brief(
    *,
    source_id="sb-assumption-volatility",
    title="Checkout Retry",
    domain="payments",
    summary="Retry failed payment submissions.",
    source_payload=None,
    source_links=None,
):
    return {
        "id": source_id,
        "title": title,
        "domain": domain,
        "summary": summary,
        "source_project": "blueprint",
        "source_entity_type": "manual",
        "source_id": source_id,
        "source_payload": {} if source_payload is None else source_payload,
        "source_links": {} if source_links is None else source_links,
        "created_at": None,
        "updated_at": None,
    }
