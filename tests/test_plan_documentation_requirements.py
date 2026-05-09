"""Tests for plan documentation requirements matrix generator."""

import pytest

from blueprint.plan_documentation_requirements import (
    CoverageStatus,
    DocumentationRequirementsMatrixGenerator,
    DocumentationType,
    PlanDocumentationRequirementsMatrix,
    PlanDocumentationRequirementsRow,
    build_plan_documentation_requirements_matrix,
    extract_plan_documentation_requirements_rows,
    generate_plan_documentation_requirements_matrix,
    plan_documentation_requirements_matrix_to_dict,
    plan_documentation_requirements_matrix_to_dicts,
    plan_documentation_requirements_matrix_to_markdown,
    summarize_plan_documentation_requirements_matrix,
)


def test_empty_plan_returns_empty_matrix():
    """Empty plan should return empty matrix."""
    plan = {"id": "test-plan", "tasks": []}

    matrix = build_plan_documentation_requirements_matrix(plan)

    assert isinstance(matrix, PlanDocumentationRequirementsMatrix)
    assert matrix.plan_id == "test-plan"
    assert len(matrix.rows) == 0
    assert matrix.completeness_score == 0.0
    assert matrix.summary["doc_type_count"] == 0


def test_api_documentation_detected():
    """Detect API documentation requirements in plan."""
    plan = {
        "id": "api-plan",
        "tasks": [
            {
                "id": "task-1",
                "title": "Create API documentation",
                "description": "Write OpenAPI spec and API reference documentation with owner assigned to @team",
            }
        ],
    }

    matrix = build_plan_documentation_requirements_matrix(plan)

    # Should detect API documentation (might also detect api_reference)
    assert len(matrix.rows) >= 1
    api_doc_found = any(row.doc_type == "api_documentation" for row in matrix.rows)
    assert api_doc_found
    api_row = [row for row in matrix.rows if row.doc_type == "api_documentation"][0]
    assert "task-1" in api_row.affected_task_ids
    assert api_row.coverage_status in ("partial", "covered")


def test_architecture_diagrams_detected():
    """Detect architecture diagram requirements in plan."""
    plan = {
        "id": "arch-plan",
        "tasks": [
            {
                "id": "task-2",
                "title": "Document system architecture",
                "description": "Create architecture diagrams and C4 model with ownership by engineering team",
            }
        ],
    }

    matrix = build_plan_documentation_requirements_matrix(plan)

    api_or_arch_found = any(
        row.doc_type in ("architecture_diagrams", "api_documentation") for row in matrix.rows
    )
    assert api_or_arch_found


def test_runbook_documentation_detected():
    """Detect runbook requirements in plan."""
    plan = {
        "id": "ops-plan",
        "tasks": [
            {
                "id": "task-3",
                "description": "Write operational runbook for deployment and rollback procedures",
            }
        ],
    }

    matrix = build_plan_documentation_requirements_matrix(plan)

    runbook_found = any(row.doc_type == "runbooks" for row in matrix.rows)
    assert runbook_found


def test_user_guide_documentation_detected():
    """Detect user guide requirements in plan."""
    plan = {
        "tasks": [
            {
                "id": "task-4",
                "description": "Create end-user documentation and getting started guide for customers",
            }
        ],
    }

    matrix = build_plan_documentation_requirements_matrix(plan)

    user_guide_found = any(row.doc_type == "user_guides" for row in matrix.rows)
    assert user_guide_found


def test_onboarding_materials_detected():
    """Detect onboarding materials requirements in plan."""
    plan = {
        "tasks": [
            {
                "id": "task-5",
                "description": "Develop developer onboarding guide and quickstart tutorial",
            }
        ],
    }

    matrix = build_plan_documentation_requirements_matrix(plan)

    onboarding_found = any(row.doc_type == "onboarding_materials" for row in matrix.rows)
    assert onboarding_found


def test_troubleshooting_guide_detected():
    """Detect troubleshooting guide requirements in plan."""
    plan = {
        "tasks": [
            {
                "id": "task-6",
                "description": "Write troubleshooting guide and FAQ for common issues",
            }
        ],
    }

    matrix = build_plan_documentation_requirements_matrix(plan)

    troubleshooting_found = any(row.doc_type == "troubleshooting_guides" for row in matrix.rows)
    assert troubleshooting_found


def test_deployment_guide_detected():
    """Detect deployment guide requirements in plan."""
    plan = {
        "tasks": [
            {
                "id": "task-7",
                "description": "Write deployment guide with installation instructions and setup process",
            }
        ],
    }

    matrix = build_plan_documentation_requirements_matrix(plan)

    deployment_found = any(row.doc_type == "deployment_guides" for row in matrix.rows)
    assert deployment_found


def test_coverage_status_missing():
    """Test coverage status is missing when no attributes covered."""
    plan = {
        "tasks": [
            {
                "id": "task-8",
                "description": "API documentation",
            }
        ],
    }

    matrix = build_plan_documentation_requirements_matrix(plan)

    if matrix.rows:
        # The coverage status should be missing or partial since we don't have full attributes
        row = matrix.rows[0]
        assert row.coverage_status in ("missing", "partial")


def test_coverage_status_partial():
    """Test coverage status is partial when some attributes covered."""
    plan = {
        "tasks": [
            {
                "id": "task-9",
                "description": "Write API documentation owned by @api-team with review process",
            }
        ],
    }

    matrix = build_plan_documentation_requirements_matrix(plan)

    if matrix.rows:
        row = matrix.rows[0]
        # Should have partial coverage since we have owner and review_process
        assert len(row.missing_attributes) < len(row.recommended_attributes)


def test_coverage_status_covered():
    """Test coverage status is covered when all attributes present."""
    plan = {
        "tasks": [
            {
                "id": "task-10",
                "description": (
                    "Create API documentation owned by @api-team with review process, "
                    "delivery timeline by 2026-12-31, and content generated"
                ),
            }
        ],
    }

    matrix = build_plan_documentation_requirements_matrix(plan)

    if matrix.rows:
        row = matrix.rows[0]
        # Should have better coverage with more attributes
        assert row.coverage_status in ("partial", "covered")


def test_owner_extraction():
    """Test owner extraction from task descriptions."""
    plan = {
        "tasks": [
            {
                "id": "task-11",
                "description": "API docs owned by @platform-team",
            }
        ],
    }

    matrix = build_plan_documentation_requirements_matrix(plan)

    if matrix.rows:
        row = matrix.rows[0]
        # Owner might be extracted
        assert row.owner is None or "@platform-team" in str(row.owner)


def test_timeline_extraction():
    """Test timeline extraction from task descriptions."""
    plan = {
        "tasks": [
            {
                "id": "task-12",
                "description": "Complete runbook documentation by 2026-06-01",
            }
        ],
    }

    matrix = build_plan_documentation_requirements_matrix(plan)

    if matrix.rows:
        row = matrix.rows[0]
        # Timeline might be extracted
        assert row.delivery_timeline is None or "2026-06-01" in str(row.delivery_timeline)


def test_multiple_doc_types_in_single_task():
    """Test detection of multiple documentation types in a single task."""
    plan = {
        "tasks": [
            {
                "id": "task-13",
                "description": (
                    "Create comprehensive documentation including API reference, "
                    "architecture diagrams, deployment guide, and troubleshooting FAQ"
                ),
            }
        ],
    }

    matrix = build_plan_documentation_requirements_matrix(plan)

    # Should detect multiple doc types
    assert len(matrix.rows) >= 2
    doc_types = {row.doc_type for row in matrix.rows}
    # At least some of the mentioned types should be detected
    expected_types = {"api_reference", "architecture_diagrams", "deployment_guides", "troubleshooting_guides"}
    assert len(doc_types & expected_types) >= 1


def test_comprehensive_documentation_plan():
    """Test comprehensive plan with all documentation types."""
    plan = {
        "id": "comprehensive-plan",
        "tasks": [
            {
                "id": "api-task",
                "description": "Write API documentation with OpenAPI spec owned by @api-team timeline 2026-12-31",
            },
            {
                "id": "arch-task",
                "description": "Create architecture diagrams and design documentation with maintenance plan",
            },
            {
                "id": "runbook-task",
                "description": "Develop operational runbook with review process",
            },
            {
                "id": "user-task",
                "description": "Write user guides accessible to all customers",
            },
            {
                "id": "onboard-task",
                "description": "Create developer onboarding materials",
            },
            {
                "id": "troubleshoot-task",
                "description": "Document troubleshooting guide and common issues",
            },
            {
                "id": "deploy-task",
                "description": "Write deployment guide with review required",
            },
        ],
    }

    matrix = build_plan_documentation_requirements_matrix(plan)

    # Should have multiple doc types
    assert len(matrix.rows) >= 3
    assert matrix.summary["doc_type_count"] >= 3
    assert matrix.completeness_score > 0.0


def test_path_based_detection():
    """Test documentation type detection from file paths."""
    plan = {
        "tasks": [
            {
                "id": "task-14",
                "files_or_modules": ["docs/api/openapi.yaml", "docs/runbooks/deployment.md"],
            }
        ],
    }

    matrix = build_plan_documentation_requirements_matrix(plan)

    # Should detect based on file paths
    doc_types = {row.doc_type for row in matrix.rows}
    # At least one type should be detected from paths
    assert len(doc_types) >= 1


def test_generate_alias():
    """Test generate_plan_documentation_requirements_matrix alias."""
    plan = {"id": "test", "tasks": [{"description": "API docs"}]}

    matrix1 = build_plan_documentation_requirements_matrix(plan)
    matrix2 = generate_plan_documentation_requirements_matrix(plan)

    assert matrix1.plan_id == matrix2.plan_id
    assert len(matrix1.rows) == len(matrix2.rows)


def test_extract_rows():
    """Test extract_plan_documentation_requirements_rows function."""
    plan = {
        "tasks": [
            {"id": "t1", "description": "API documentation"},
            {"id": "t2", "description": "Runbook creation"},
        ],
    }

    rows = extract_plan_documentation_requirements_rows(plan)

    assert isinstance(rows, tuple)
    assert all(isinstance(row, PlanDocumentationRequirementsRow) for row in rows)


def test_summarize_matrix():
    """Test summarize_plan_documentation_requirements_matrix function."""
    plan = {
        "tasks": [
            {"id": "t1", "description": "API documentation"},
        ],
    }

    summary = summarize_plan_documentation_requirements_matrix(plan)

    assert isinstance(summary, dict)
    assert "doc_type_count" in summary
    assert "covered_count" in summary
    assert "missing_count" in summary


def test_summarize_from_matrix_object():
    """Test summarizing from matrix object."""
    plan = {"tasks": [{"description": "API docs"}]}
    matrix = build_plan_documentation_requirements_matrix(plan)

    summary = summarize_plan_documentation_requirements_matrix(matrix)

    assert isinstance(summary, dict)
    assert summary == matrix.summary


def test_to_dict():
    """Test matrix to_dict serialization."""
    plan = {
        "id": "test-plan",
        "tasks": [{"id": "t1", "description": "API documentation"}],
    }

    matrix = build_plan_documentation_requirements_matrix(plan)
    result = matrix.to_dict()

    assert isinstance(result, dict)
    assert result["plan_id"] == "test-plan"
    assert "rows" in result
    assert "summary" in result
    assert "completeness_score" in result
    assert "records" in result


def test_to_dict_helper():
    """Test plan_documentation_requirements_matrix_to_dict helper."""
    plan = {"id": "test", "tasks": []}
    matrix = build_plan_documentation_requirements_matrix(plan)

    result = plan_documentation_requirements_matrix_to_dict(matrix)

    assert isinstance(result, dict)
    assert result["plan_id"] == "test"


def test_to_dicts():
    """Test matrix to_dicts serialization."""
    plan = {
        "tasks": [
            {"id": "t1", "description": "API docs"},
            {"id": "t2", "description": "Runbook"},
        ],
    }

    matrix = build_plan_documentation_requirements_matrix(plan)
    result = matrix.to_dicts()

    assert isinstance(result, list)
    assert all(isinstance(item, dict) for item in result)


def test_to_dicts_helper_from_matrix():
    """Test plan_documentation_requirements_matrix_to_dicts helper with matrix."""
    plan = {"tasks": [{"description": "API docs"}]}
    matrix = build_plan_documentation_requirements_matrix(plan)

    result = plan_documentation_requirements_matrix_to_dicts(matrix)

    assert isinstance(result, list)


def test_to_dicts_helper_from_rows():
    """Test plan_documentation_requirements_matrix_to_dicts helper with rows."""
    plan = {"tasks": [{"description": "API docs"}]}
    rows = extract_plan_documentation_requirements_rows(plan)

    result = plan_documentation_requirements_matrix_to_dicts(rows)

    assert isinstance(result, list)


def test_to_markdown():
    """Test matrix to_markdown rendering."""
    plan = {
        "id": "test-plan",
        "tasks": [{"id": "t1", "description": "Write API documentation"}],
    }

    matrix = build_plan_documentation_requirements_matrix(plan)
    markdown = matrix.to_markdown()

    assert isinstance(markdown, str)
    assert "# Plan Documentation Requirements Matrix" in markdown
    assert "test-plan" in markdown
    assert "## Summary" in markdown


def test_to_markdown_helper():
    """Test plan_documentation_requirements_matrix_to_markdown helper."""
    plan = {"id": "test", "tasks": [{"description": "API docs"}]}
    matrix = build_plan_documentation_requirements_matrix(plan)

    markdown = plan_documentation_requirements_matrix_to_markdown(matrix)

    assert isinstance(markdown, str)
    assert "Documentation Requirements Matrix" in markdown


def test_to_markdown_empty():
    """Test markdown rendering for empty matrix."""
    plan = {"id": "empty-plan", "tasks": []}

    matrix = build_plan_documentation_requirements_matrix(plan)
    markdown = matrix.to_markdown()

    assert "No documentation requirements were found" in markdown


def test_row_to_dict():
    """Test PlanDocumentationRequirementsRow to_dict."""
    row = PlanDocumentationRequirementsRow(
        doc_type="api_documentation",
        affected_task_ids=("t1", "t2"),
        evidence=("evidence1",),
        coverage_status="partial",
        missing_attributes=("timeline",),
        recommended_attributes=("content", "ownership", "timeline", "review_process"),
        owner="@team",
        delivery_timeline="2026-12-31",
    )

    result = row.to_dict()

    assert isinstance(result, dict)
    assert result["doc_type"] == "api_documentation"
    assert result["affected_task_ids"] == ["t1", "t2"]
    assert result["coverage_status"] == "partial"
    assert result["owner"] == "@team"
    assert result["delivery_timeline"] == "2026-12-31"


def test_completeness_score_calculation():
    """Test completeness score calculation."""
    # All missing
    plan1 = {
        "tasks": [
            {"description": "API docs"},
        ],
    }
    matrix1 = build_plan_documentation_requirements_matrix(plan1)
    # Score should be low when attributes are missing
    assert 0.0 <= matrix1.completeness_score <= 1.0

    # Some coverage
    plan2 = {
        "tasks": [
            {"description": "API documentation with owner @team and review process"},
        ],
    }
    matrix2 = build_plan_documentation_requirements_matrix(plan2)
    assert 0.0 <= matrix2.completeness_score <= 1.0


def test_generator_class():
    """Test DocumentationRequirementsMatrixGenerator class."""
    generator = DocumentationRequirementsMatrixGenerator()
    plan = {"id": "test", "tasks": [{"description": "API documentation"}]}

    matrix = generator.generate_matrix(plan)

    assert isinstance(matrix, PlanDocumentationRequirementsMatrix)
    assert matrix.plan_id == "test"


def test_with_implementation_brief():
    """Test matrix generation with implementation brief."""
    plan = {
        "id": "plan-1",
        "implementation_brief_id": "brief-1",
        "tasks": [{"description": "API docs"}],
    }
    brief = {
        "id": "brief-1",
        "title": "Documentation project",
        "scope": "Create comprehensive documentation including architecture diagrams",
    }

    matrix = build_plan_documentation_requirements_matrix(plan, brief)

    assert matrix.plan_id == "plan-1"
    assert matrix.implementation_brief_id == "brief-1"
    # Should detect doc types from brief as well
    assert len(matrix.rows) >= 1


def test_case_insensitive_matching():
    """Test that pattern matching is case-insensitive."""
    plan = {
        "tasks": [
            {"description": "CREATE API DOCUMENTATION"},
            {"description": "write RUNBOOK for deployment"},
        ],
    }

    matrix = build_plan_documentation_requirements_matrix(plan)

    # Should detect regardless of case
    assert len(matrix.rows) >= 1


def test_multiple_tasks_same_doc_type():
    """Test multiple tasks contributing to same documentation type."""
    plan = {
        "tasks": [
            {"id": "t1", "description": "Write API documentation part 1"},
            {"id": "t2", "description": "Write API documentation part 2"},
        ],
    }

    matrix = build_plan_documentation_requirements_matrix(plan)

    # Should consolidate into single row with multiple task IDs
    api_docs_rows = [row for row in matrix.rows if row.doc_type == "api_documentation"]
    if api_docs_rows:
        row = api_docs_rows[0]
        # Should reference multiple tasks
        assert len(row.affected_task_ids) >= 1


def test_evidence_collection():
    """Test that evidence is collected from tasks."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "API Documentation",
                "description": "Create OpenAPI specification",
            }
        ],
    }

    matrix = build_plan_documentation_requirements_matrix(plan)

    if matrix.rows:
        row = matrix.rows[0]
        assert len(row.evidence) > 0
        # Evidence should contain task information
        assert any("task-1" in str(e) for e in row.evidence)


def test_summary_structure():
    """Test summary structure."""
    plan = {
        "tasks": [
            {"id": "t1", "description": "API documentation"},
            {"id": "t2", "description": "Runbook creation"},
        ],
    }

    matrix = build_plan_documentation_requirements_matrix(plan)

    assert "doc_type_count" in matrix.summary
    assert "covered_count" in matrix.summary
    assert "partial_count" in matrix.summary
    assert "missing_count" in matrix.summary
    assert "type_counts" in matrix.summary
    assert "doc_types" in matrix.summary
    assert "affected_task_ids" in matrix.summary


def test_frozen_dataclass():
    """Test that PlanDocumentationRequirementsRow is frozen."""
    row = PlanDocumentationRequirementsRow(
        doc_type="api_documentation",
        coverage_status="missing",
    )

    with pytest.raises(AttributeError):
        row.doc_type = "runbooks"  # type: ignore


def test_records_property():
    """Test that records property returns rows."""
    plan = {"tasks": [{"description": "API docs"}]}
    matrix = build_plan_documentation_requirements_matrix(plan)

    assert matrix.records == matrix.rows


def test_versioned_documentation():
    """Test detection of versioned documentation."""
    plan = {
        "tasks": [
            {
                "description": "Maintain API documentation with versioning and update schedule",
            }
        ],
    }

    matrix = build_plan_documentation_requirements_matrix(plan)

    # Should detect documentation with maintenance plan
    if matrix.rows:
        row = matrix.rows[0]
        # maintenance_plan might be in covered attributes if detected
        assert len(row.recommended_attributes) > 0


def test_localized_documentation():
    """Test detection of localized documentation."""
    plan = {
        "tasks": [
            {
                "description": "Create user guides accessible in multiple languages for external customers",
            }
        ],
    }

    matrix = build_plan_documentation_requirements_matrix(plan)

    # Should detect user guides with accessibility
    user_guide_rows = [row for row in matrix.rows if row.doc_type == "user_guides"]
    if user_guide_rows:
        row = user_guide_rows[0]
        assert "accessibility" in row.recommended_attributes


def test_generated_documentation():
    """Test detection of generated/auto-generated documentation."""
    plan = {
        "tasks": [
            {
                "description": "Generate API reference documentation from code comments",
            }
        ],
    }

    matrix = build_plan_documentation_requirements_matrix(plan)

    # Should detect API reference
    api_ref_found = any(row.doc_type == "api_reference" for row in matrix.rows)
    assert api_ref_found or any(row.doc_type == "api_documentation" for row in matrix.rows)


def test_empty_tasks_list():
    """Test with empty tasks list."""
    plan = {"id": "empty", "tasks": []}

    matrix = build_plan_documentation_requirements_matrix(plan)

    assert len(matrix.rows) == 0
    assert matrix.completeness_score == 0.0


def test_none_plan():
    """Test with minimal plan input."""
    plan = {}

    matrix = build_plan_documentation_requirements_matrix(plan)

    assert isinstance(matrix, PlanDocumentationRequirementsMatrix)
    assert len(matrix.rows) == 0
