from blueprint.domain.models import SourceBrief
from blueprint.source_nonfunctional_requirements import (
    SourceNonfunctionalRequirement,
    extract_source_nonfunctional_requirements,
    source_nonfunctional_requirements_to_dicts,
    summarize_source_nonfunctional_requirements,
)


def test_structured_fields_extract_requirements_with_evidence_and_follow_up():
    records = extract_source_nonfunctional_requirements(
        {
            "id": "checkout",
            "title": "Checkout hardening",
            "summary": "Checkout must stay under 250ms p95 latency for saved-card users.",
            "problem": "Support cannot diagnose failed payment retries today.",
            "goals": ["Add observability metrics and alerts for every retry failure."],
            "constraints": [
                "All payment tokens must be encrypted at rest.",
                "The checkout flow must support WCAG 2.2 AA keyboard navigation.",
            ],
            "acceptance_criteria": [
                "Done when dashboards show retry failure rate and alert within 5 minutes.",
            ],
            "source_payload": {},
        }
    )

    assert all(isinstance(record, SourceNonfunctionalRequirement) for record in records)
    assert [
        (record.category, record.requirement_text, record.source_field) for record in records
    ] == [
        (
            "performance",
            "Checkout must stay under 250ms p95 latency for saved-card users.",
            "summary",
        ),
        ("security", "All payment tokens must be encrypted at rest.", "constraints[0]"),
        (
            "accessibility",
            "The checkout flow must support WCAG 2.2 AA keyboard navigation.",
            "constraints[1]",
        ),
        (
            "observability",
            "Done when dashboards show retry failure rate and alert within 5 minutes.",
            "acceptance_criteria[0]",
        ),
        (
            "observability",
            "Add observability metrics and alerts for every retry failure.",
            "goals[0]",
        ),
    ]
    assert records[0].confidence == "high"
    assert records[0].evidence == records[0].requirement_text
    assert "latency" in records[0].suggested_follow_up


def test_markdown_bullets_from_source_payload_are_extracted():
    records = extract_source_nonfunctional_requirements(
        _source_brief(
            source_payload={
                "body": (
                    "## Nonfunctional requirements\n"
                    "- Must support Safari, Chrome, and mobile browsers.\n"
                    "- Should localize dates, currency, and timezone for EU users.\n"
                    "- Need to scale to 10,000 concurrent users during launches."
                )
            }
        )
    )

    assert [
        (record.category, record.requirement_text, record.source_field) for record in records
    ] == [
        (
            "compatibility",
            "Must support Safari, Chrome, and mobile browsers.",
            "source_payload.body",
        ),
        (
            "localization",
            "Should localize dates, currency, and timezone for EU users.",
            "source_payload.body",
        ),
        (
            "scalability",
            "Need to scale to 10,000 concurrent users during launches.",
            "source_payload.body",
        ),
    ]


def test_duplicate_requirement_text_across_fields_collapses_to_first_source():
    records = extract_source_nonfunctional_requirements(
        _source_brief(
            summary="API must stay under 200ms p95 latency.",
            source_payload={
                "requirements": [
                    "API must stay under 200ms p95 latency.",
                    "- API must stay under 200ms p95 latency.",
                    "Audit logs must capture admin changes.",
                ]
            },
        )
    )

    assert [(record.requirement_text, record.source_field) for record in records] == [
        ("API must stay under 200ms p95 latency.", "summary"),
        ("Audit logs must capture admin changes.", "source_payload.requirements[2]"),
    ]


def test_confidence_scoring_and_summary_helper_are_deterministic():
    records = extract_source_nonfunctional_requirements(
        _source_brief(
            summary="The profile page should be fast for mobile users.",
            source_payload={
                "constraints": ["Availability must be 99.9% for importer workers."],
            },
        )
    )
    payload = source_nonfunctional_requirements_to_dicts(records)
    summary = summarize_source_nonfunctional_requirements(records)

    assert [(record.category, record.confidence) for record in records] == [
        ("performance", "medium"),
        ("reliability", "high"),
    ]
    assert payload == [
        {
            "category": "performance",
            "requirement_text": "The profile page should be fast for mobile users.",
            "source_field": "summary",
            "confidence": "medium",
            "evidence": "The profile page should be fast for mobile users.",
            "suggested_follow_up": "Define measurable latency, throughput, and load-test acceptance criteria.",
        },
        {
            "category": "reliability",
            "requirement_text": "Availability must be 99.9% for importer workers.",
            "source_field": "source_payload.constraints[0]",
            "confidence": "high",
            "evidence": "Availability must be 99.9% for importer workers.",
            "suggested_follow_up": (
                "Confirm uptime targets, failure modes, retry behavior, and recovery objectives."
            ),
        },
    ]
    assert summary == {
        "requirement_count": 2,
        "category_counts": {"performance": 1, "reliability": 1},
        "confidence_counts": {"high": 1, "medium": 1, "low": 0},
        "follow_ups": [
            "Define measurable latency, throughput, and load-test acceptance criteria.",
            "Confirm uptime targets, failure modes, retry behavior, and recovery objectives.",
        ],
    }
    assert (
        summarize_source_nonfunctional_requirements(_source_brief(summary="No requirements."))[
            "requirement_count"
        ]
        == 0
    )


def test_source_brief_model_input_is_supported_without_mutation():
    source = SourceBrief.model_validate(
        _source_brief(
            summary="Account deletion must remove PII within 30 days.",
            source_payload={
                "description": "SOC 2 compliance requires audit evidence for access changes.",
                "metadata": {"note": "No user-facing copy changes."},
            },
        )
    )
    before = source.model_dump(mode="python")

    records = extract_source_nonfunctional_requirements(source)

    assert source.model_dump(mode="python") == before
    assert [(record.category, record.source_field) for record in records] == [
        ("privacy", "summary"),
        ("compliance", "source_payload.description"),
    ]


def test_empty_unrelated_or_malformed_sources_return_empty_tuple():
    assert (
        extract_source_nonfunctional_requirements(_source_brief(summary="Adjust onboarding copy."))
        == ()
    )
    assert (
        extract_source_nonfunctional_requirements(_source_brief(summary="", source_payload={}))
        == ()
    )
    assert extract_source_nonfunctional_requirements("not a source brief") == ()


def _source_brief(
    *,
    source_id="sb-nfr",
    title="Checkout requirements",
    domain="payments",
    summary="General checkout requirements.",
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
