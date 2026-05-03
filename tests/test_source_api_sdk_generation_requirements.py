import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_api_sdk_generation_requirements import (
    SourceApiSdkGenerationRequirement,
    SourceApiSdkGenerationRequirementsReport,
    build_source_api_sdk_generation_requirements,
    derive_source_api_sdk_generation_requirements,
    extract_source_api_sdk_generation_requirements,
    generate_source_api_sdk_generation_requirements,
    source_api_sdk_generation_requirements_to_dict,
    source_api_sdk_generation_requirements_to_dicts,
    source_api_sdk_generation_requirements_to_markdown,
    summarize_source_api_sdk_generation_requirements,
)


def test_comprehensive_sdk_generation_requirements_extracted():
    result = build_source_api_sdk_generation_requirements(
        _brief(
            "brief-sdk-gen",
            title="Generate API SDKs for multiple languages",
            body=(
                "Generate SDKs for Python, JavaScript, Go, Java, and Ruby using OpenAPI Generator. "
                "SDKs should follow semantic versioning aligned with API versions. "
                "Publish Python SDK to PyPI, JavaScript SDK to npm, Ruby to RubyGems, and Java to Maven Central. "
                "Generate SDK documentation from OpenAPI specs with JSDoc, PyDoc, JavaDoc, and RDoc. "
                "Include authentication helpers for API keys, bearer tokens, and OAuth flows in all SDKs. "
                "Implement retry logic with exponential backoff and jitter for transient errors. "
                "Provide example code and quickstart guides for common integration patterns."
            ),
        )
    )

    assert isinstance(result, SourceApiSdkGenerationRequirementsReport)
    assert result.source_id == "brief-sdk-gen"
    assert result.title == "Generate API SDKs for multiple languages"
    assert len(result.requirements) == 8
    assert result.records == result.requirements
    assert result.findings == result.requirements
    categories = [req.category for req in result.requirements]
    assert categories == [
        "sdk_language_targets",
        "sdk_generation_tooling",
        "sdk_versioning",
        "sdk_package_distribution",
        "sdk_documentation",
        "sdk_authentication_helpers",
        "sdk_retry_logic",
        "sdk_example_code",
    ]
    lang_req = next(req for req in result.requirements if req.category == "sdk_language_targets")
    assert lang_req.confidence in ("high", "medium")
    assert any(lang in " ".join(lang_req.evidence).lower() for lang in ["python", "javascript", "go", "java", "ruby"])
    assert len(lang_req.planning_note) > 0
    assert result.summary["requirement_count"] == 8
    assert result.summary["status"] == "ready_for_planning"


def test_negated_scope_produces_no_requirements():
    result = build_source_api_sdk_generation_requirements(
        _brief(
            "brief-no-sdk",
            title="Internal API endpoint",
            scope="This is an internal API. No SDKs or client libraries are required for this release.",
        )
    )

    assert result.requirements == ()
    assert result.records == ()
    assert result.summary["requirement_count"] == 0
    assert result.summary["status"] == "no_api_sdk_generation_requirements_found"


def test_partial_requirements_with_unresolved_questions():
    result = build_source_api_sdk_generation_requirements(
        _brief(
            "brief-partial-sdk",
            title="Generate API SDK",
            body=(
                "Generate SDK for the API. "
                "Publish SDK to package manager. "
                "Include retry logic in SDK client."
            ),
        )
    )

    categories = [req.category for req in result.requirements]
    assert "sdk_generation_tooling" in categories or "sdk_package_distribution" in categories or "sdk_retry_logic" in categories

    for req in result.requirements:
        if req.category == "sdk_language_targets" and req in result.requirements:
            assert any("language" in q.lower() for q in req.unresolved_questions) or len(req.unresolved_questions) == 0


def test_structured_metadata_fields_contribute_evidence():
    result = build_source_api_sdk_generation_requirements(
        _brief(
            "brief-sdk-metadata",
            title="API SDK Development",
            metadata={
                "sdk": {
                    "languages": "Python, JavaScript, Go",
                    "generator": "OpenAPI Generator with custom templates",
                    "versioning": "Semantic versioning aligned with API v2",
                    "distribution": "PyPI for Python, npm for JavaScript, GitHub releases for Go",
                },
                "documentation": "Generated from OpenAPI spec with JSDoc and PyDoc",
                "authentication": "Include OAuth and API key helpers",
                "retry": "Exponential backoff with max 3 retries",
                "examples": "Quickstart guide and integration patterns",
            },
        )
    )

    categories = [req.category for req in result.requirements]
    assert "sdk_language_targets" in categories
    assert "sdk_generation_tooling" in categories
    assert "sdk_versioning" in categories
    assert "sdk_package_distribution" in categories
    assert "sdk_documentation" in categories
    assert "sdk_authentication_helpers" in categories
    assert "sdk_retry_logic" in categories
    assert "sdk_example_code" in categories


def test_deduplication_and_stable_ordering():
    result = build_source_api_sdk_generation_requirements(
        _brief(
            "brief-sdk-dedup",
            title="SDK generation",
            body="Generate Python SDK. Generate JavaScript SDK. Use OpenAPI Generator.",
            requirements=[
                "Support Python and JavaScript SDKs",
                "Use OpenAPI Generator for code generation",
                "Publish to PyPI and npm",
            ],
        )
    )

    lang_reqs = [req for req in result.requirements if req.category == "sdk_language_targets"]
    assert len(lang_reqs) == 1
    assert len(lang_reqs[0].evidence) <= 6

    tool_reqs = [req for req in result.requirements if req.category == "sdk_generation_tooling"]
    if tool_reqs:
        assert len(tool_reqs) == 1


def test_implementation_brief_input():
    brief = {
        "id": "impl-brief-sdk",
        "source_brief_id": "source-sdk-1",
        "title": "SDK generation implementation",
        "problem_statement": "Generate multi-language SDKs for the public API",
        "mvp_goal": "Generate Python and JavaScript SDKs using OpenAPI Generator, publish to PyPI and npm, include auth helpers and retry logic.",
        "scope": ["SDK generation", "Package distribution"],
        "non_goals": [],
        "assumptions": [],
        "risks": [],
        "validation_plan": "Test SDK generation",
        "definition_of_done": ["SDKs published"],
        "status": "draft",
    }

    result = build_source_api_sdk_generation_requirements(brief)

    assert result.source_id == "impl-brief-sdk"
    assert len(result.requirements) > 0
    categories = [req.category for req in result.requirements]
    assert "sdk_language_targets" in categories or "sdk_generation_tooling" in categories
    assert "sdk_package_distribution" in categories


def test_model_validation_and_serialization():
    brief = _brief(
        "brief-sdk-serial",
        title="Multi-language SDK generation",
        body="Generate Python, JavaScript, and Go SDKs using OpenAPI Generator with semantic versioning, publish to PyPI, npm, and GitHub, include auth helpers, retry logic, and example code.",
    )
    original = copy.deepcopy(brief)

    result = build_source_api_sdk_generation_requirements(brief)
    payload = source_api_sdk_generation_requirements_to_dict(result)
    dicts = source_api_sdk_generation_requirements_to_dicts(result)
    markdown = source_api_sdk_generation_requirements_to_markdown(result)

    assert brief == original
    assert derive_source_api_sdk_generation_requirements(brief).to_dict() == result.to_dict()
    assert generate_source_api_sdk_generation_requirements(brief).to_dict() == result.to_dict()
    assert extract_source_api_sdk_generation_requirements(brief) == result.requirements
    assert summarize_source_api_sdk_generation_requirements(brief) == result.summary
    assert summarize_source_api_sdk_generation_requirements(result) == result.summary
    assert json.loads(json.dumps(payload)) == payload
    assert dicts == payload["requirements"]
    assert source_api_sdk_generation_requirements_to_dicts(result.requirements) == dicts
    assert "# Source API SDK Generation Requirements Report: brief-sdk-serial" in markdown
    assert list(payload) == ["source_id", "title", "requirements", "summary", "records", "findings"]
    assert list(payload["requirements"][0]) == [
        "category",
        "source_field",
        "evidence",
        "confidence",
        "planning_note",
        "unresolved_questions",
    ]


def test_string_input():
    result = build_source_api_sdk_generation_requirements(
        "Generate Python SDK using OpenAPI Generator. "
        "Use semantic versioning for SDK releases. "
        "Publish to PyPI package registry. "
        "Generate SDK documentation with PyDoc. "
        "Include authentication helpers for API keys and OAuth. "
        "Add retry logic with exponential backoff. "
        "Provide quickstart guide and code examples."
    )

    categories = [req.category for req in result.requirements]
    assert "sdk_language_targets" in categories
    assert "sdk_generation_tooling" in categories
    assert "sdk_versioning" in categories
    assert "sdk_package_distribution" in categories
    assert "sdk_documentation" in categories
    assert "sdk_authentication_helpers" in categories
    assert "sdk_retry_logic" in categories
    assert "sdk_example_code" in categories


def test_object_with_attributes():
    obj = SimpleNamespace(
        id="obj-sdk",
        title="API SDK generation",
        description="Generate client SDKs for Python and JavaScript",
        body="Use Swagger Codegen to generate SDKs. Publish to npm and PyPI. Include retry logic and auth helpers.",
        sdk="Python and JavaScript client libraries",
        distribution="npm for JavaScript, PyPI for Python",
    )

    result = build_source_api_sdk_generation_requirements(obj)

    assert result.source_id == "obj-sdk"
    assert len(result.requirements) > 0
    categories = [req.category for req in result.requirements]
    assert "sdk_language_targets" in categories or "sdk_generation_tooling" in categories
    assert "sdk_package_distribution" in categories


def test_multi_language_sdk_requirements():
    result = build_source_api_sdk_generation_requirements(
        _brief(
            "brief-multi-lang",
            title="Multi-language SDK support",
            body=(
                "Generate SDKs for Python, JavaScript, TypeScript, Go, Java, Ruby, PHP, and Swift. "
                "Use OpenAPI Generator with custom mustache templates for each language. "
                "Publish Python to PyPI, JavaScript/TypeScript to npm, Ruby to RubyGems, Java to Maven Central, PHP to Packagist, Go to GitHub releases, and Swift to CocoaPods."
            ),
        )
    )

    lang_req = next((req for req in result.requirements if req.category == "sdk_language_targets"), None)
    assert lang_req is not None
    evidence_text = " ".join(lang_req.evidence).lower()
    assert any(lang in evidence_text for lang in ["python", "javascript", "go", "java", "ruby"])

    dist_req = next((req for req in result.requirements if req.category == "sdk_package_distribution"), None)
    assert dist_req is not None
    dist_text = " ".join(dist_req.evidence).lower()
    assert any(pkg in dist_text for pkg in ["pypi", "npm", "rubygems", "maven"])


def test_custom_generator_and_versioning():
    result = build_source_api_sdk_generation_requirements(
        _brief(
            "brief-custom-gen",
            title="Custom SDK generator",
            body=(
                "Build custom SDK generator based on OpenAPI Generator with company-specific templates. "
                "SDK versions should follow semver and align with API major versions. "
                "Each SDK major version bump requires backward compatibility review."
            ),
        )
    )

    tool_req = next((req for req in result.requirements if req.category == "sdk_generation_tooling"), None)
    assert tool_req is not None
    assert any("custom" in ev.lower() or "openapi" in ev.lower() for ev in tool_req.evidence)

    version_req = next((req for req in result.requirements if req.category == "sdk_versioning"), None)
    assert version_req is not None
    assert any("semver" in ev.lower() or "semantic" in ev.lower() or "version" in ev.lower() for ev in version_req.evidence)


def test_no_sdk_when_unrelated_client_mentioned():
    result = build_source_api_sdk_generation_requirements(
        _brief(
            "brief-unrelated-client",
            title="Web client improvements",
            body=(
                "Improve the web client UI. "
                "Add client-side validation to forms. "
                "Update client-side routing with React Router."
            ),
        )
    )

    assert result.requirements == () or len(result.requirements) == 0


def _brief(brief_id, *, title=None, body=None, scope=None, requirements=None, metadata=None):
    brief = {
        "id": brief_id,
        "title": title or brief_id,
        "body": body or "",
        "status": "draft",
    }
    if scope is not None:
        brief["scope"] = scope
    if requirements is not None:
        brief["requirements"] = requirements
    if metadata is not None:
        brief["metadata"] = metadata
    return brief
