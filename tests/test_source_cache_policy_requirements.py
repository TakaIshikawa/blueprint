import json

from blueprint.source_cache_policy_requirements import (
    SourceCachePolicyRequirement,
    SourceCachePolicyRequirementsReport,
    build_source_cache_policy_requirements,
    derive_source_cache_policy_requirements,
    extract_source_cache_policy_requirements,
    generate_source_cache_policy_requirements,
    source_cache_policy_requirements_to_dict,
    source_cache_policy_requirements_to_dicts,
    source_cache_policy_requirements_to_markdown,
    summarize_source_cache_policy_requirements,
)


def test_structured_performance_api_frontend_and_infrastructure_sections_extract_records():
    report = build_source_cache_policy_requirements(
        _source(
            {
                "performance": [
                    "Cacheable resources must include API responses in CDN cache keyed by locale vary header.",
                    "TTL must set CDN max-age to 10 minutes with configurable per route override.",
                    "Stale-while-revalidate can serve stale for 60 seconds with fallback refresh and acceptable user impact.",
                ],
                "api": [
                    "Cache scope must be per-tenant private cache keyed by tenant id and authorization boundary.",
                    "Invalidation trigger must purge resource URL tags on content update and propagate within 2 minutes.",
                ],
                "frontend": ["Browser cache privacy must use Cache-Control private and no-store for PII profile data with header enforcement."],
                "infrastructure": ["Cache owner must be platform owner with quarterly review cadence and Slack escalation."],
            }
        )
    )

    assert isinstance(report, SourceCachePolicyRequirementsReport)
    assert all(isinstance(record, SourceCachePolicyRequirement) for record in report.records)
    assert [record.requirement_type for record in report.records] == [
        "cacheable_resources",
        "ttl",
        "cache_scope",
        "invalidation_trigger",
        "staleness_tolerance",
        "privacy_constraint",
        "ownership",
    ]
    assert all(record.readiness == "ready" for record in report.records)
    assert report.summary["readiness_counts"] == {"ready": 7, "needs_detail": 0}
    assert report.summary["category_counts"]["ttl"] == 1


def test_vague_cache_mentions_need_ttl_scope_and_invalidation_detail():
    report = build_source_cache_policy_requirements("Caching is required. TTL, CDN, browser cache, privacy, and invalidation should be defined.")
    by_type = {record.requirement_type: record for record in report.records}

    assert by_type["ttl"].readiness == "needs_detail"
    assert by_type["invalidation_trigger"].missing_details == ("trigger", "propagation")
    assert report.summary["readiness_counts"]["needs_detail"] == len(report.records)
    assert report.summary["missing_detail_count"] > 0


def test_helpers_and_empty_inputs_are_stable():
    source = _source({"performance": ["TTL must set CDN max-age to 5 minutes with default override | note."]})
    report = build_source_cache_policy_requirements(source)
    payload = source_cache_policy_requirements_to_dict(report)
    empty = build_source_cache_policy_requirements(_source({"performance": ["No cache, caching, TTL, CDN, browser cache, invalidation, or stale work is required."]}))

    assert extract_source_cache_policy_requirements(source).to_dict() == payload
    assert generate_source_cache_policy_requirements(source).to_dict() == payload
    assert derive_source_cache_policy_requirements(source).to_dict() == payload
    assert json.loads(json.dumps(payload)) == payload
    assert report.records == report.findings
    assert source_cache_policy_requirements_to_dicts(report) == payload["requirements"]
    assert source_cache_policy_requirements_to_dicts(report.records) == payload["records"]
    assert summarize_source_cache_policy_requirements(report) == report.summary
    assert source_cache_policy_requirements_to_markdown(report) == report.to_markdown()
    assert "default override \\| note" in report.to_markdown()
    assert empty.records == ()
    assert build_source_cache_policy_requirements({"source_payload": {"notes": object()}}).records == ()
    assert build_source_cache_policy_requirements(42).records == ()


def _source(source_payload):
    return {
        "id": "sb-cache",
        "title": "Cache policy requirements",
        "domain": "performance",
        "summary": "Cache planning.",
        "source_project": "project",
        "source_entity_type": "issue",
        "source_id": "issue-cache",
        "source_payload": source_payload,
        "source_links": {},
    }
