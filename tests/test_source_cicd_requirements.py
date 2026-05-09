"""Tests for source CI/CD requirements extractor."""

import pytest

from blueprint.source_cicd_requirements import (
    SourceCicdRequirement,
    SourceCicdRequirementsReport,
    extract_cicd_requirements,
    generate_cicd_requirements,
    analyze_cicd_requirements,
)


def test_empty_source_returns_empty_report():
    """Empty source should return report with no requirements."""
    result = extract_cicd_requirements({})

    assert isinstance(result, SourceCicdRequirementsReport)
    assert len(result.requirements) == 0
    assert result.summary["requirement_count"] == 0


def test_build_steps_detected():
    """Detect build steps in source brief."""
    source = {
        "title": "CI/CD Pipeline Setup",
        "description": "Configure build steps and compilation process",
    }

    result = extract_cicd_requirements(source)

    assert len(result.requirements) > 0
    assert any(r.requirement_type == "build_steps" for r in result.requirements)


def test_npm_build_detected():
    """Detect npm build as build steps."""
    source = {
        "description": "Add npm build to pipeline",
    }

    result = extract_cicd_requirements(source)

    assert any(r.requirement_type == "build_steps" for r in result.requirements)


def test_docker_build_detected():
    """Detect Docker build as build steps."""
    source = {
        "description": "Configure docker build for containers",
    }

    result = extract_cicd_requirements(source)

    assert any(r.requirement_type == "build_steps" for r in result.requirements)


def test_test_stages_detected():
    """Detect test stages in source brief."""
    source = {
        "title": "Testing Pipeline",
        "description": "Set up unit tests and integration tests",
    }

    result = extract_cicd_requirements(source)

    assert any(r.requirement_type == "test_stages" for r in result.requirements)


def test_e2e_tests_detected():
    """Detect end-to-end tests."""
    source = {
        "description": "Add e2e tests to pipeline",
    }

    result = extract_cicd_requirements(source)

    assert any(r.requirement_type == "test_stages" for r in result.requirements)


def test_test_automation_detected():
    """Detect test automation."""
    source = {
        "description": "Implement automated testing in CI",
    }

    result = extract_cicd_requirements(source)

    assert any(r.requirement_type == "test_stages" for r in result.requirements)


def test_deployment_strategy_detected():
    """Detect deployment strategy in source brief."""
    source = {
        "title": "Deployment Strategy",
        "description": "Implement blue-green deployment",
    }

    result = extract_cicd_requirements(source)

    assert any(r.requirement_type == "deployment_strategy" for r in result.requirements)


def test_canary_deployment_detected():
    """Detect canary deployment strategy."""
    source = {
        "description": "Use canary deployment for gradual rollout",
    }

    result = extract_cicd_requirements(source)

    assert any(r.requirement_type == "deployment_strategy" for r in result.requirements)


def test_rolling_deployment_detected():
    """Detect rolling deployment strategy."""
    source = {
        "description": "Configure rolling deployment for zero downtime",
    }

    result = extract_cicd_requirements(source)

    assert any(r.requirement_type == "deployment_strategy" for r in result.requirements)


def test_environment_promotion_detected():
    """Detect environment promotion in source brief."""
    source = {
        "title": "Environment Pipeline",
        "description": "Set up dev, staging, and production environments",
    }

    result = extract_cicd_requirements(source)

    assert any(r.requirement_type == "environment_promotion" for r in result.requirements)


def test_promote_to_production_detected():
    """Detect promotion to production."""
    source = {
        "description": "Promote to production after staging approval",
    }

    result = extract_cicd_requirements(source)

    assert any(r.requirement_type == "environment_promotion" for r in result.requirements)


def test_multi_environment_deployment_detected():
    """Detect multi-environment deployment."""
    source = {
        "description": "Configure multi-environment deployment pipeline",
    }

    result = extract_cicd_requirements(source)

    assert any(r.requirement_type == "environment_promotion" for r in result.requirements)


def test_approval_gates_detected():
    """Detect approval gates in source brief."""
    source = {
        "title": "Deployment Approvals",
        "description": "Add manual approval before production deployment",
    }

    result = extract_cicd_requirements(source)

    assert any(r.requirement_type == "approval_gates" for r in result.requirements)


def test_manual_approval_detected():
    """Detect manual approval requirement."""
    source = {
        "description": "Require approval for production releases",
    }

    result = extract_cicd_requirements(source)

    assert any(r.requirement_type == "approval_gates" for r in result.requirements)


def test_approval_workflow_detected():
    """Detect approval workflow."""
    source = {
        "description": "Implement approval workflow for deployments",
    }

    result = extract_cicd_requirements(source)

    assert any(r.requirement_type == "approval_gates" for r in result.requirements)


def test_build_optimization_detected():
    """Detect build optimization in source brief."""
    source = {
        "title": "Build Performance",
        "description": "Optimize build with caching for faster builds",
    }

    result = extract_cicd_requirements(source)

    assert any(r.requirement_type == "build_optimization" for r in result.requirements)


def test_parallel_build_detected():
    """Detect parallel build optimization."""
    source = {
        "description": "Enable parallel build for performance",
    }

    result = extract_cicd_requirements(source)

    assert any(r.requirement_type == "build_optimization" for r in result.requirements)


def test_incremental_build_detected():
    """Detect incremental build."""
    source = {
        "description": "Use incremental build to reduce build time",
    }

    result = extract_cicd_requirements(source)

    assert any(r.requirement_type == "build_optimization" for r in result.requirements)


def test_test_parallelization_detected():
    """Detect test parallelization in source brief."""
    source = {
        "title": "Test Performance",
        "description": "Implement parallel tests for faster execution",
    }

    result = extract_cicd_requirements(source)

    assert any(r.requirement_type == "test_parallelization" for r in result.requirements)


def test_test_matrix_detected():
    """Detect test matrix as parallelization."""
    source = {
        "description": "Use test matrix for concurrent testing",
    }

    result = extract_cicd_requirements(source)

    assert any(r.requirement_type == "test_parallelization" for r in result.requirements)


def test_test_sharding_detected():
    """Detect test sharding."""
    source = {
        "description": "Implement test sharding for parallel execution",
    }

    result = extract_cicd_requirements(source)

    assert any(r.requirement_type == "test_parallelization" for r in result.requirements)


def test_artifact_management_detected():
    """Detect artifact management in source brief."""
    source = {
        "title": "Artifact Storage",
        "description": "Configure artifact management and storage",
    }

    result = extract_cicd_requirements(source)

    assert any(r.requirement_type == "artifact_management" for r in result.requirements)


def test_docker_registry_detected():
    """Detect Docker registry as artifact management."""
    source = {
        "description": "Push to docker registry for artifact storage",
    }

    result = extract_cicd_requirements(source)

    assert any(r.requirement_type == "artifact_management" for r in result.requirements)


def test_npm_registry_detected():
    """Detect npm registry."""
    source = {
        "description": "Publish to npm registry",
    }

    result = extract_cicd_requirements(source)

    assert any(r.requirement_type == "artifact_management" for r in result.requirements)


def test_deployment_automation_detected():
    """Detect deployment automation in source brief."""
    source = {
        "title": "Automated Deployment",
        "description": "Implement automated deployment with CI/CD pipeline",
    }

    result = extract_cicd_requirements(source)

    assert any(r.requirement_type == "deployment_automation" for r in result.requirements)


def test_github_actions_detected():
    """Detect GitHub Actions as deployment automation."""
    source = {
        "description": "Use GitHub Actions pipeline for automation",
    }

    result = extract_cicd_requirements(source)

    assert any(r.requirement_type == "deployment_automation" for r in result.requirements)


def test_gitlab_ci_detected():
    """Detect GitLab CI."""
    source = {
        "description": "Configure GitLab CI pipeline",
    }

    result = extract_cicd_requirements(source)

    assert any(r.requirement_type == "deployment_automation" for r in result.requirements)


def test_rollback_procedures_detected():
    """Detect rollback procedures in source brief."""
    source = {
        "title": "Rollback Strategy",
        "description": "Implement automated rollback procedures",
    }

    result = extract_cicd_requirements(source)

    assert any(r.requirement_type == "rollback_procedures" for r in result.requirements)


def test_manual_rollback_detected():
    """Detect manual rollback."""
    source = {
        "description": "Support manual rollback for failed deployments",
    }

    result = extract_cicd_requirements(source)

    assert any(r.requirement_type == "rollback_procedures" for r in result.requirements)


def test_deployment_rollback_detected():
    """Detect deployment rollback capability."""
    source = {
        "description": "Enable deployment rollback support",
    }

    result = extract_cicd_requirements(source)

    assert any(r.requirement_type == "rollback_procedures" for r in result.requirements)


def test_comprehensive_cicd_all_aspects_detected():
    """Test comprehensive CI/CD pipeline with all aspects present."""
    source = {
        "title": "Complete CI/CD Pipeline",
        "description": (
            "Configure build steps with npm build and docker build. "
            "Set up test stages with unit tests and integration tests. "
            "Implement blue-green deployment strategy with environment promotion. "
            "Add manual approval gates before production. "
            "Optimize build with parallel build and test parallelization. "
            "Configure artifact management with docker registry. "
            "Implement deployment automation with GitHub Actions pipeline. "
            "Set up automated rollback procedures for failed deployments."
        ),
        "acceptance_criteria": [
            "Build steps configured",
            "Test stages implemented",
            "Deployment strategy defined",
            "Environment promotion enabled",
            "Approval gates added",
            "Build optimization done",
            "Test parallelization configured",
            "Artifact management set up",
            "Deployment automation working",
            "Rollback procedures tested",
        ],
    }

    result = extract_cicd_requirements(source)

    assert any(r.requirement_type == "build_steps" for r in result.requirements)
    assert any(r.requirement_type == "test_stages" for r in result.requirements)
    assert any(r.requirement_type == "deployment_strategy" for r in result.requirements)
    assert any(r.requirement_type == "environment_promotion" for r in result.requirements)
    assert any(r.requirement_type == "approval_gates" for r in result.requirements)
    assert any(r.requirement_type == "build_optimization" for r in result.requirements)
    assert any(r.requirement_type == "test_parallelization" for r in result.requirements)
    assert any(r.requirement_type == "artifact_management" for r in result.requirements)
    assert any(r.requirement_type == "deployment_automation" for r in result.requirements)
    assert any(r.requirement_type == "rollback_procedures" for r in result.requirements)


def test_matrix_builds_edge_case():
    """Test matrix builds detection (edge case)."""
    source = {
        "description": "Use test matrix for multiple configurations",
    }

    result = extract_cicd_requirements(source)

    assert any(r.requirement_type == "test_parallelization" for r in result.requirements)


def test_conditional_deployments_edge_case():
    """Test conditional deployment detection (edge case)."""
    source = {
        "description": "Configure conditional deployment based on branch",
    }

    result = extract_cicd_requirements(source)

    assert any(r.requirement_type == "deployment_automation" for r in result.requirements)


def test_multi_environment_pipeline_edge_case():
    """Test multi-environment pipeline (edge case)."""
    source = {
        "description": "Set up multi-environment pipeline for all stages",
    }

    result = extract_cicd_requirements(source)

    assert any(r.requirement_type == "environment_promotion" for r in result.requirements)


def test_invalid_source_non_mapping():
    """Test with invalid input (non-mapping type)."""
    result = extract_cicd_requirements("not a mapping")

    assert isinstance(result, SourceCicdRequirementsReport)
    assert len(result.requirements) == 0


def test_invalid_source_none():
    """Test with None input."""
    result = extract_cicd_requirements(None)

    assert isinstance(result, SourceCicdRequirementsReport)
    assert len(result.requirements) == 0


def test_case_insensitive_matching():
    """Test that pattern matching is case-insensitive."""
    source = {
        "description": "CONFIGURE BUILD STEPS and TEST STAGES",
        "acceptance_criteria": ["DEPLOYMENT STRATEGY", "APPROVAL GATES"],
    }

    result = extract_cicd_requirements(source)

    assert any(r.requirement_type == "build_steps" for r in result.requirements)
    assert any(r.requirement_type == "test_stages" for r in result.requirements)
    assert any(r.requirement_type == "deployment_strategy" for r in result.requirements)
    assert any(r.requirement_type == "approval_gates" for r in result.requirements)


def test_to_dict_method():
    """Test SourceCicdRequirementsReport.to_dict() serialization."""
    source = {
        "id": "test-source-1",
        "description": "Configure build steps and test stages",
    }

    result = extract_cicd_requirements(source)
    result_dict = result.to_dict()

    assert isinstance(result_dict, dict)
    assert "source_brief_id" in result_dict
    assert "requirements" in result_dict
    assert "summary" in result_dict
    assert "records" in result_dict


def test_to_dicts_method():
    """Test to_dicts() returns list of requirement dictionaries."""
    source = {
        "description": "Configure build steps",
    }

    result = extract_cicd_requirements(source)
    dicts = result.to_dicts()

    assert isinstance(dicts, list)
    if dicts:
        assert isinstance(dicts[0], dict)
        assert "requirement_type" in dicts[0]


def test_to_markdown_method():
    """Test to_markdown() renders Markdown report."""
    source = {
        "id": "test-source-1",
        "description": "Configure build steps and test stages",
    }

    result = extract_cicd_requirements(source)
    markdown = result.to_markdown()

    assert isinstance(markdown, str)
    assert "# Source CI/CD Requirements Report" in markdown
    assert "## Summary" in markdown


def test_records_property():
    """Test records property returns requirements."""
    source = {
        "description": "Configure build steps",
    }

    result = extract_cicd_requirements(source)

    assert result.records == result.requirements


def test_requirement_to_dict():
    """Test SourceCicdRequirement.to_dict() serialization."""
    requirement = SourceCicdRequirement(
        requirement_type="build_steps",
        evidence=("build steps", "npm build"),
        source_field_paths=("description",),
        matched_terms=("build steps",),
    )

    result = requirement.to_dict()

    assert isinstance(result, dict)
    assert result["requirement_type"] == "build_steps"
    assert isinstance(result["evidence"], list)
    assert isinstance(result["source_field_paths"], list)
    assert isinstance(result["matched_terms"], list)


def test_summary_statistics():
    """Test summary statistics calculation."""
    source = {
        "description": "Configure build steps and test stages with deployment automation",
    }

    result = extract_cicd_requirements(source)

    assert "requirement_count" in result.summary
    assert "source_count" in result.summary
    assert "pipeline_coverage" in result.summary
    assert "automation_coverage" in result.summary
    assert "type_counts" in result.summary


def test_compatibility_aliases():
    """Test compatibility alias functions."""
    source = {"description": "Configure build steps"}

    result1 = generate_cicd_requirements(source)
    result2 = analyze_cicd_requirements(source)

    assert isinstance(result1, SourceCicdRequirementsReport)
    assert isinstance(result2, SourceCicdRequirementsReport)


def test_jenkins_pipeline_detected():
    """Test Jenkins pipeline detection."""
    source = {
        "description": "Configure Jenkins pipeline for automation",
    }

    result = extract_cicd_requirements(source)

    assert any(r.requirement_type == "deployment_automation" for r in result.requirements)


def test_circleci_detected():
    """Test CircleCI detection."""
    source = {
        "description": "Use CircleCI pipeline for continuous integration",
    }

    result = extract_cicd_requirements(source)

    assert any(r.requirement_type == "deployment_automation" for r in result.requirements)


def test_build_cache_detected():
    """Test build cache as optimization."""
    source = {
        "description": "Enable build cache to speed up builds",
    }

    result = extract_cicd_requirements(source)

    assert any(r.requirement_type == "build_optimization" for r in result.requirements)


def test_maven_build_detected():
    """Test Maven build detection."""
    source = {
        "description": "Configure Maven build for Java project",
    }

    result = extract_cicd_requirements(source)

    assert any(r.requirement_type == "build_steps" for r in result.requirements)
