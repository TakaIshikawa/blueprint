import json

from blueprint.domain.models import SourceBrief
from blueprint.source_citation_coverage import (
    SourceCitationClaim,
    analyze_source_citation_coverage,
    source_citation_coverage_to_dict,
)


def test_linked_source_brief_marks_claims_as_partial_with_record_level_evidence():
    result = analyze_source_citation_coverage(
        _source_brief(
            summary="Checkout retries must preserve the original payment id.",
            source_links={"html_url": "https://example.test/issues/123"},
        )
    )

    assert result.source_brief_ids == ("sb-checkout-retry",)
    assert result.status == "partial"
    assert result.coverage_score == 0.5
    assert [(claim.path, claim.coverage_status) for claim in result.claims] == [
        ("summary", "partial")
    ]
    assert result.claims[0].citation_evidence == ("https://example.test/issues/123",)
    assert result.uncited_claim_candidates[0].evidence_text == (
        "Checkout retries must preserve the original payment id."
    )
    assert result.uncited_claim_candidates[0].remediation_suggestion == (
        "Attach a claim-level citation at summary; the brief has only record-level source links."
    )


def test_markdown_links_and_footnotes_mark_claims_as_covered():
    result = analyze_source_citation_coverage(
        [
            _source_brief(
                source_id="sb-link",
                summary=(
                    "Retry copy must match the [support policy]"
                    "(https://example.test/policy)."
                ),
            ),
            _source_brief(
                source_id="sb-footnote",
                summary="Refund requests require manager approval.[^refund]",
                source_payload={
                    "raw_markdown": (
                        "Refund requests require manager approval.[^refund]\n\n"
                        "[^refund]: https://example.test/refund-policy"
                    )
                },
            ),
        ]
    )

    assert [(claim.source_brief_id, claim.coverage_status) for claim in result.claims] == [
        ("sb-footnote", "covered"),
        ("sb-link", "covered"),
    ]
    assert result.status == "covered"
    assert result.coverage_score == 1.0
    assert result.claims[0].citation_evidence == ("[^refund]",)
    assert result.claims[1].citation_evidence == (
        "[support policy](https://example.test/policy)",
    )


def test_payload_level_references_cover_sibling_claims():
    result = analyze_source_citation_coverage(
        _source_brief(
            summary="Fallback queue behavior is still being clarified.",
            source_payload={
                "normalized": {
                    "requirements": [
                        "Retries must stop after three failed attempts.",
                        "The failure banner must include the next retry time.",
                    ],
                    "references": ["https://example.test/spec#retry"],
                }
            },
            source_links={},
        )
    )

    requirement_claims = [
        claim for claim in result.claims if claim.path.startswith("source_payload.")
    ]

    assert [(claim.path, claim.coverage_status) for claim in requirement_claims] == [
        ("source_payload.normalized.requirements[0]", "covered"),
        ("source_payload.normalized.requirements[1]", "covered"),
    ]
    assert requirement_claims[0].citation_evidence == (
        "source_payload.normalized.references: https://example.test/spec#retry",
    )
    assert next(claim for claim in result.claims if claim.path == "summary").coverage_status == (
        "partial"
    )


def test_missing_citations_surface_uncited_claim_candidates_and_suggestions():
    result = analyze_source_citation_coverage(
        {
            "id": "sb-unsupported",
            "summary": (
                "Customers abandon checkout when a retry takes more than ten seconds."
            ),
            "assumptions": ["Support agents can manually replay failed payment attempts."],
            "source_payload": {
                "normalized": {
                    "requirements": ["The retry action must be visible on failed payments."]
                }
            },
            "source_links": {},
        }
    )

    assert result.status == "missing"
    assert result.coverage_score == 0.0
    assert result.summary == {
        "source_brief_count": 1,
        "claim_count": 3,
        "covered_count": 0,
        "partial_count": 0,
        "missing_count": 3,
    }
    assert [claim.path for claim in result.uncited_claim_candidates] == [
        "assumptions[0]",
        "source_payload.normalized.requirements[0]",
        "summary",
    ]
    assert all(claim.remediation_suggestion for claim in result.uncited_claim_candidates)


def test_model_inputs_and_stable_serialization_are_json_compatible():
    source_model = SourceBrief.model_validate(
        _source_brief(
            summary="Retry attempts must emit audit events https://example.test/audit.",
            source_payload={
                "normalized": {
                    "requirements": ["Audit exports must include retry attempt ids."],
                    "references": [{"url": "https://example.test/export-spec"}],
                }
            },
            source_links={"path": "briefs/retry.md"},
        )
    )

    first = analyze_source_citation_coverage(source_model)
    second = analyze_source_citation_coverage(source_model)
    payload = source_citation_coverage_to_dict(first)

    assert all(isinstance(claim, SourceCitationClaim) for claim in first.claims)
    assert payload == source_citation_coverage_to_dict(second)
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "source_brief_ids",
        "status",
        "coverage_score",
        "summary",
        "claims",
        "uncited_claim_candidates",
        "remediation_suggestions",
    ]
    assert list(payload["claims"][0]) == [
        "source_brief_id",
        "path",
        "evidence_text",
        "coverage_status",
        "citation_evidence",
        "remediation_suggestion",
    ]
    assert first.to_dicts() == payload["claims"]
    assert payload["summary"] == {
        "source_brief_count": 1,
        "claim_count": 2,
        "covered_count": 2,
        "partial_count": 0,
        "missing_count": 0,
    }


def _source_brief(
    *,
    source_id="sb-checkout-retry",
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
