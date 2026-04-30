import pytest
from pydantic import ValidationError

from blueprint.audits.brief_ambiguity import audit_brief_ambiguity
from blueprint.domain import ImplementationBrief


def test_brief_ambiguity_accepts_clean_specific_brief():
    result = audit_brief_ambiguity(_specific_brief())

    assert result.passed is True
    assert result.score == 100
    assert result.issues == []
    assert result.to_dict()["summary"] == {"high": 0, "medium": 0, "issues": 0}


def test_brief_ambiguity_reports_multiple_ambiguous_fields():
    brief = _specific_brief()
    brief.update(
        {
            "problem_statement": "Owner TBD for tenant migration errors.",
            "mvp_goal": "Create a user-friendly migration summary.",
            "scope": ["Support various import paths", "Add fast validation"],
            "assumptions": ["Someone will provide production fixture exports"],
            "validation_plan": "Run checks as needed.",
            "definition_of_done": ["Appropriate team accepts the rollout report"],
        }
    )

    result = audit_brief_ambiguity(brief)

    assert result.passed is False
    assert result.score == 0
    assert {(issue.field, issue.phrase) for issue in result.issues} == {
        ("problem_statement", "Owner TBD"),
        ("mvp_goal", "user-friendly"),
        ("scope", "various"),
        ("scope", "fast"),
        ("assumptions", "Someone"),
        ("validation_plan", "as needed"),
        ("definition_of_done", "Appropriate team"),
    }
    assert all(issue.message for issue in result.issues)
    assert all(issue.recommendation for issue in result.issues)


def test_brief_ambiguity_matching_is_case_insensitive_inside_list_fields():
    brief = _specific_brief()
    brief["scope"] = ["Build SCALABLE queue metrics", "Mark migration status as tBd"]
    brief["definition_of_done"] = ["Admin review confirms USER FRIENDLY labels"]

    result = audit_brief_ambiguity(brief)

    assert [issue.field for issue in result.issues] == [
        "scope",
        "scope",
        "definition_of_done",
    ]
    assert [issue.phrase for issue in result.issues] == [
        "SCALABLE",
        "tBd",
        "USER FRIENDLY",
    ]


def test_brief_ambiguity_score_calculation_uses_severity_penalties():
    brief = _specific_brief()
    brief["scope"] = ["Use fast parsing", "Owner is TBD"]

    result = audit_brief_ambiguity(brief)

    assert [(issue.phrase, issue.severity) for issue in result.issues] == [
        ("fast", "medium"),
        ("Owner is TBD", "high"),
    ]
    assert result.score == 70
    assert result.to_dict()["summary"] == {"high": 1, "medium": 1, "issues": 2}


def test_brief_ambiguity_accepts_implementation_brief_model():
    brief = ImplementationBrief.model_validate(_specific_brief())

    result = audit_brief_ambiguity(brief)

    assert result.brief_id == "ib-specific"
    assert result.score == 100


def test_brief_ambiguity_surfaces_validation_errors_for_invalid_payloads():
    brief = _specific_brief()
    brief.pop("mvp_goal")

    with pytest.raises(ValidationError):
        audit_brief_ambiguity(brief)


def _specific_brief():
    return {
        "id": "ib-specific",
        "source_brief_id": "sb-specific",
        "title": "Tenant import diagnostics",
        "domain": "data",
        "target_user": "Support engineers",
        "buyer": "Customer operations",
        "workflow_context": "Investigating failed customer imports",
        "problem_statement": (
            "Support engineers need import diagnostics that identify the failed row "
            "and the violated schema rule."
        ),
        "mvp_goal": (
            "Return row-level import failure details in the admin import history view."
        ),
        "product_surface": "Admin web UI",
        "scope": [
            "Add row number and schema rule columns to import failure records",
            "Render import failure details in the admin import history table",
        ],
        "non_goals": ["Changing the import parser format"],
        "assumptions": [
            "Import failures already include stable row identifiers in stored metadata"
        ],
        "architecture_notes": "Use existing import history store methods.",
        "data_requirements": "Import job metadata and validation failure records",
        "integration_points": [],
        "risks": [
            "Large failure payloads could slow import history queries",
            "Older import jobs may not have row identifiers in metadata",
        ],
        "validation_plan": (
            "Run admin import history tests and verify row-level errors in JSON output."
        ),
        "definition_of_done": [
            "Import history displays row-level failure details",
            "Admin import history tests cover legacy and current failure payloads",
        ],
        "status": "ready_for_planning",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
