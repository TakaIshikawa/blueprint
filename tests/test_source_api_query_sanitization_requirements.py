import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_api_query_sanitization_requirements import (
    SourceApiQuerySanitizationRequirement,
    SourceApiQuerySanitizationRequirementsReport,
    build_source_api_query_sanitization_requirements,
    derive_source_api_query_sanitization_requirements,
    extract_source_api_query_sanitization_requirements,
    generate_source_api_query_sanitization_requirements,
    source_api_query_sanitization_requirements_to_dict,
    source_api_query_sanitization_requirements_to_dicts,
    source_api_query_sanitization_requirements_to_markdown,
    summarize_source_api_query_sanitization_requirements,
)


def test_comprehensive_query_sanitization_requirements_extracted():
    result = build_source_api_query_sanitization_requirements(
        _brief(
            "brief-query-sanitization",
            title="Add query parameter sanitization for API endpoints",
            body=(
                "Implement comprehensive query parameter sanitization to prevent injection attacks. "
                "Use parameterized queries to prevent SQL injection attacks on database queries. "
                "Sanitize MongoDB queries to prevent NoSQL injection via operator manipulation. "
                "Escape shell parameters to prevent command injection in subprocess calls. "
                "Escape LDAP special characters to prevent LDAP injection in directory queries. "
                "Validate XPath queries to prevent XPath injection attacks. "
                "Implement parameter whitelisting to only allow expected query parameters. "
                "Apply HTML encoding and URL encoding for query parameter values. "
                "Validate parameter types with joi, max length 255, and enforce string/integer/uuid validation."
                "Use context-specific escaping strategies for special characters in parameters. "
                "Add comprehensive tests for SQL injection, NoSQL injection, XSS, and fuzzing attacks."
            ),
        )
    )

    assert isinstance(result, SourceApiQuerySanitizationRequirementsReport)
    assert result.brief_id == "brief-query-sanitization"
    assert result.title == "Add query parameter sanitization for API endpoints"
    assert len(result.requirements) == 10
    assert result.records == result.requirements
    assert result.findings == result.requirements
    categories = [req.category for req in result.requirements]
    assert categories == [
        "sql_injection_prevention",
        "nosql_injection_prevention",
        "command_injection_prevention",
        "ldap_injection_prevention",
        "xpath_injection_prevention",
        "parameter_whitelisting",
        "input_encoding",
        "length_type_validation",
        "escaping_strategies",
        "test_coverage",
    ]
    sql_req = next(req for req in result.requirements if req.category == "sql_injection_prevention")
    assert sql_req.confidence in ("high", "medium")
    assert sql_req.value and ("parameterized" in sql_req.value or "sql" in sql_req.value)
    assert len(sql_req.planning_notes) > 0
    assert result.summary["requirement_count"] == 10
    assert result.summary["status"] == "ready_for_planning"
    assert result.summary["missing_detail_flags"] == []


def test_negated_scope_produces_no_requirements():
    result = build_source_api_query_sanitization_requirements(
        _brief(
            "brief-no-sanitization",
            title="Internal API with trusted input",
            scope="This API receives trusted input only. No query parameter sanitization or validation is required.",
        )
    )

    assert result.requirements == ()
    assert result.records == ()
    assert result.summary["requirement_count"] == 0
    assert result.summary["status"] == "no_sanitization_language"


def test_partial_requirements_report_missing_detail_flags():
    result = build_source_api_query_sanitization_requirements(
        _brief(
            "brief-partial",
            title="Add query parameter validation",
            body=(
                "Implement query parameter validation to prevent injection attacks. "
                "Validate parameter types and enforce length constraints."
            ),
        )
    )

    categories = [req.category for req in result.requirements]
    assert "length_type_validation" in categories
    assert result.summary["requirement_count"] > 0
    assert "missing_validation_library" in result.summary["missing_detail_flags"]
    assert result.summary["status"] == "needs_sanitization_details"


def test_structured_metadata_fields_contribute_evidence():
    result = build_source_api_query_sanitization_requirements(
        _brief(
            "brief-metadata",
            title="API query sanitization",
            metadata={
                "validation": "Use joi for parameter validation with whitelisting",
                "sql_injection": "Use parameterized queries with SQLAlchemy ORM",
                "nosql_injection": "Sanitize MongoDB queries with Mongoose",
                "encoding": "Apply HTML encoding and URL encoding for all parameters",
                "security": {
                    "length_validation": "Enforce max length 500 for all string parameters",
                    "type_validation": "Validate string, integer, uuid, and email types",
                },
            },
        )
    )

    categories = [req.category for req in result.requirements]
    assert "sql_injection_prevention" in categories
    assert "nosql_injection_prevention" in categories
    assert "parameter_whitelisting" in categories
    assert "input_encoding" in categories
    assert "length_type_validation" in categories

    sql_req = next(req for req in result.requirements if req.category == "sql_injection_prevention")
    assert sql_req.value and ("parameterized" in sql_req.value or "orm" in sql_req.value or "sqlalchemy" in sql_req.value)

    encoding_req = next(req for req in result.requirements if req.category == "input_encoding")
    assert encoding_req.value and ("html encoding" in encoding_req.value or "url encoding" in encoding_req.value)


def test_deduplication_and_stable_ordering():
    result = build_source_api_query_sanitization_requirements(
        _brief(
            "brief-dedup",
            title="Query parameter sanitization",
            body="Prevent SQL injection. Use parameterized queries. Validate SQL queries.",
            requirements=[
                "Support SQL injection prevention",
                "Use parameterized queries with ORM",
                "Validate query parameters with joi",
            ],
        )
    )

    sql_reqs = [req for req in result.requirements if req.category == "sql_injection_prevention"]
    assert len(sql_reqs) == 1
    assert len(sql_reqs[0].evidence) <= 5

    validation_reqs = [req for req in result.requirements if req.category == "length_type_validation"]
    if validation_reqs:
        assert len(validation_reqs) == 1


def test_implementation_brief_input():
    brief = {
        "id": "impl-brief-sanitization",
        "source_brief_id": "source-1",
        "title": "Query parameter sanitization implementation",
        "problem_statement": "Add SQL injection and NoSQL injection prevention",
        "mvp_goal": "Implement parameterized queries, parameter whitelisting, input encoding, length validation, and tests.",
        "scope": ["Query parameter sanitization"],
        "non_goals": [],
        "assumptions": [],
        "risks": [],
        "validation_plan": "Test injection attacks",
        "definition_of_done": ["Sanitization works"],
        "status": "draft",
    }

    result = build_source_api_query_sanitization_requirements(brief)

    assert result.brief_id == "impl-brief-sanitization"
    assert len(result.requirements) > 0
    categories = [req.category for req in result.requirements]
    assert "sql_injection_prevention" in categories
    assert "nosql_injection_prevention" in categories


def test_model_validation_and_serialization():
    brief = _brief(
        "brief-serial",
        title="Query sanitization",
        body="Implement SQL injection prevention with parameterized queries, parameter whitelisting, HTML encoding, length validation with joi, and injection tests.",
    )
    original = copy.deepcopy(brief)

    result = build_source_api_query_sanitization_requirements(brief)
    payload = source_api_query_sanitization_requirements_to_dict(result)
    dicts = source_api_query_sanitization_requirements_to_dicts(result)
    markdown = source_api_query_sanitization_requirements_to_markdown(result)

    assert brief == original
    assert derive_source_api_query_sanitization_requirements(brief).to_dict() == result.to_dict()
    assert generate_source_api_query_sanitization_requirements(brief).to_dict() == result.to_dict()
    assert extract_source_api_query_sanitization_requirements(brief) == result.requirements
    assert summarize_source_api_query_sanitization_requirements(brief) == result.summary
    assert summarize_source_api_query_sanitization_requirements(result) == result.summary
    assert json.loads(json.dumps(payload)) == payload
    assert dicts == payload["requirements"]
    assert source_api_query_sanitization_requirements_to_dicts(result.requirements) == dicts
    assert "# Source API Query Sanitization Requirements Report: brief-serial" in markdown
    assert "sql_injection_prevention" in markdown
    assert list(payload) == ["brief_id", "title", "summary", "requirements", "records", "findings"]
    assert list(payload["requirements"][0]) == [
        "category",
        "source_field",
        "evidence",
        "confidence",
        "value",
        "suggested_owners",
        "planning_notes",
        "gap_messages",
    ]


def test_string_input():
    result = build_source_api_query_sanitization_requirements(
        "Query parameters must be sanitized using parameterized queries to prevent SQL injection. "
        "Implement parameter whitelisting to only allow expected parameters. "
        "Apply URL encoding for all query parameter values. "
        "Validate parameter types with joi and enforce max length validation. "
        "Add SQL injection tests and XSS tests."
    )

    categories = [req.category for req in result.requirements]
    assert "sql_injection_prevention" in categories
    assert "parameter_whitelisting" in categories
    assert "input_encoding" in categories
    assert "length_type_validation" in categories


def test_object_with_attributes():
    obj = SimpleNamespace(
        id="obj-sanitization",
        title="Query sanitization",
        description="Add query parameter sanitization and validation",
        body="Implement parameterized queries for SQL injection prevention. Implement parameter whitelisting. Validate types with joi.",
        validation="Parameter validation with length and type checks",
        security="SQL injection prevention with escaping",
    )

    result = build_source_api_query_sanitization_requirements(obj)

    assert result.brief_id == "obj-sanitization"
    assert len(result.requirements) > 0
    categories = [req.category for req in result.requirements]
    assert "sql_injection_prevention" in categories
    assert "parameter_whitelisting" in categories


def test_unrelated_validation_not_detected():
    result = build_source_api_query_sanitization_requirements(
        _brief(
            "brief-unrelated",
            title="User registration form",
            body=(
                "Improve user registration form with client-side validation. "
                "Add frontend validation for email and password fields. "
                "Support browser-based form validation."
            ),
        )
    )

    assert result.requirements == ()
    assert result.summary["status"] == "no_sanitization_language"


def test_custom_validators_detected():
    result = build_source_api_query_sanitization_requirements(
        _brief(
            "brief-custom-validators",
            title="Custom query parameter validators",
            body=(
                "Implement custom validators for query parameters using Pydantic. "
                "Implement UUID validation, email validation, and URL validation. "
                "Enforce max length 1000 for all string parameters. "
                "Use custom regex patterns for parameter validation."
            ),
        )
    )

    categories = [req.category for req in result.requirements]
    assert "length_type_validation" in categories
    validation_req = next(req for req in result.requirements if req.category == "length_type_validation")
    assert validation_req.value and ("pydantic" in validation_req.value or "uuid" in validation_req.value or "email" in validation_req.value)


def test_context_specific_escaping_detected():
    result = build_source_api_query_sanitization_requirements(
        _brief(
            "brief-escaping",
            title="Context-specific parameter escaping",
            body=(
                "Apply context-specific escaping for special characters in query parameters. "
                "Implement HTML escaping for parameters rendered in HTML context. "
                "Prevent SQL injection with parameterized queries and SQL escaping. "
                "Prevent command injection with shell escaping for command-line parameters."
            ),
        )
    )

    categories = [req.category for req in result.requirements]
    assert "escaping_strategies" in categories
    assert "sql_injection_prevention" in categories
    assert "command_injection_prevention" in categories


def test_no_sanitization_explicit_language():
    result = build_source_api_query_sanitization_requirements(
        _brief(
            "brief-trusted",
            title="Internal API",
            scope="This API accepts trusted input only. No query sanitization is required.",
        )
    )

    assert result.requirements == ()
    assert result.summary["status"] == "no_sanitization_language"


def test_injection_prevention_categories_detected():
    result = build_source_api_query_sanitization_requirements(
        _brief(
            "brief-injections",
            title="Prevent multiple injection attacks",
            body=(
                "Prevent SQL injection with parameterized queries. "
                "Prevent NoSQL injection in MongoDB queries. "
                "Prevent command injection in shell executions. "
                "Prevent LDAP injection in directory queries. "
                "Prevent XPath injection in XML queries."
            ),
        )
    )

    categories = [req.category for req in result.requirements]
    assert "sql_injection_prevention" in categories
    assert "nosql_injection_prevention" in categories
    assert "command_injection_prevention" in categories
    assert "ldap_injection_prevention" in categories
    assert "xpath_injection_prevention" in categories


def test_acceptance_criteria_high_confidence():
    result = build_source_api_query_sanitization_requirements(
        _brief(
            "brief-acceptance",
            title="Query sanitization",
            acceptance_criteria=[
                "SQL injection prevention must use parameterized queries",
                "Parameter whitelisting must reject unknown parameters",
                "Input encoding must apply HTML and URL encoding",
            ],
        )
    )

    assert len(result.requirements) >= 3
    for req in result.requirements:
        if req.source_field.startswith("acceptance_criteria"):
            assert req.confidence == "high"


def test_validation_library_specified():
    result = build_source_api_query_sanitization_requirements(
        _brief(
            "brief-joi",
            title="Query validation with joi",
            body=(
                "Implement joi library for query parameter validation. "
                "Implement parameter whitelisting with joi schemas. "
                "Validate string types and number types and UUID types with joi. "
                "Prevent SQL injection with parameterized queries and ORM."
            ),
        )
    )

    categories = [req.category for req in result.requirements]
    assert "length_type_validation" in categories
    assert result.summary["missing_detail_flags"] == []


def test_sanitization_strategy_specified():
    result = build_source_api_query_sanitization_requirements(
        _brief(
            "brief-strategy",
            title="Query sanitization strategy",
            body=(
                "Implement parameter whitelisting with joi to only allow expected query parameters. "
                "Apply HTML encoding for all parameter values. "
                "Implement prepared statements to prevent SQL injection."
            ),
        )
    )

    assert len(result.requirements) > 0
    assert result.summary["missing_detail_flags"] == []


def test_evidence_truncation():
    long_text = "A" * 200
    result = build_source_api_query_sanitization_requirements(
        _brief(
            "brief-long",
            title="Query sanitization",
            body=f"Prevent SQL injection with parameterized queries {long_text} and validate parameters.",
        )
    )

    for req in result.requirements:
        for evidence in req.evidence:
            assert len(evidence) <= 200


def test_compatibility_properties():
    result = build_source_api_query_sanitization_requirements(
        _brief(
            "brief-compat",
            title="Query sanitization",
            body="Prevent SQL injection with parameterized queries and validate parameters.",
        )
    )

    for req in result.requirements:
        assert req.requirement_category == req.category
        assert req.concern == req.category
        assert req.suggested_plan_impacts == req.planning_notes


def _brief(brief_id, *, title=None, body=None, scope=None, requirements=None, metadata=None, acceptance_criteria=None):
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
    if acceptance_criteria is not None:
        brief["acceptance_criteria"] = acceptance_criteria
    return brief
