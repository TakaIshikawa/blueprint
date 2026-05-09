from blueprint.source_integration_testing_requirements import (
    SourceIntegrationTestingRequirement,
    SourceIntegrationTestingRequirementsReport,
    extract_source_integration_testing_requirements,
)


def test_extract_integration_points():
    result = extract_source_integration_testing_requirements(
        {
            "id": "brief-integration-points",
            "title": "Test API integrations",
            "description": (
                "Test integration points including REST API, GraphQL, gRPC services, "
                "third-party integrations, webhooks, message queues like Kafka and RabbitMQ, "
                "and microservice endpoints."
            ),
        }
    )

    assert isinstance(result, SourceIntegrationTestingRequirementsReport)
    assert result.source_brief_id == "brief-integration-points"
    assert len(result.requirements) > 0
    integration_req = next((r for r in result.requirements if r.requirement_type == "integration_points"), None)
    assert integration_req is not None
    assert any("api" in term.lower() or "integration" in term.lower() for term in integration_req.matched_terms)
    assert "integration_points" in result.summary["type_counts"]


def test_extract_test_scenarios():
    result = extract_source_integration_testing_requirements(
        {
            "id": "brief-test-scenarios",
            "description": (
                "Create integration test scenarios covering happy path, edge cases, error cases, "
                "failure scenarios, retry logic, timeout scenarios, and contract tests using Pact."
            ),
        }
    )

    scenario_req = next((r for r in result.requirements if r.requirement_type == "test_scenarios"), None)
    assert scenario_req is not None
    assert any("test" in term.lower() or "scenario" in term.lower() for term in scenario_req.matched_terms)


def test_extract_test_data_requirements():
    result = extract_source_integration_testing_requirements(
        {
            "id": "brief-test-data",
            "description": (
                "Set up test data including fixtures, mock data, seed data, sample data, "
                "test database, factories using Faker, and test payloads for integration tests."
            ),
        }
    )

    data_req = next((r for r in result.requirements if r.requirement_type == "test_data_requirements"), None)
    assert data_req is not None
    assert any("test data" in term.lower() or "fixture" in term.lower() for term in data_req.matched_terms)


def test_extract_environment_needs():
    result = extract_source_integration_testing_requirements(
        {
            "id": "brief-environment",
            "description": (
                "Configure test environment using Docker Compose, testcontainers, staging environment, "
                "QA environment, local environment setup with WireMock and LocalStack mock servers."
            ),
        }
    )

    env_req = next((r for r in result.requirements if r.requirement_type == "environment_needs"), None)
    assert env_req is not None
    assert any("environment" in term.lower() or "docker" in term.lower() for term in env_req.matched_terms)


def test_extract_external_service_mocking():
    result = extract_source_integration_testing_requirements(
        {
            "id": "brief-mocking",
            "description": (
                "Mock external services using mocking, stubbing, fakes, and test doubles. "
                "Set up service mocks, HTTP mocks, API mocks with tools like Nock, Sinon, MSW, "
                "Mock Service Worker, WireMock, and VCR for HTTP recordings."
            ),
        }
    )

    mocking_req = next((r for r in result.requirements if r.requirement_type == "external_service_mocking"), None)
    assert mocking_req is not None
    assert any("mock" in term.lower() or "stub" in term.lower() for term in mocking_req.matched_terms)


def test_extract_test_isolation():
    result = extract_source_integration_testing_requirements(
        {
            "id": "brief-isolation",
            "description": (
                "Ensure test isolation with independent tests, no test dependencies or coupling, "
                "parallel tests execution, database rollback and transactions, cleanup, teardown, "
                "and fresh state using test containers."
            ),
        }
    )

    isolation_req = next((r for r in result.requirements if r.requirement_type == "test_isolation"), None)
    assert isolation_req is not None
    assert any("isolation" in term.lower() or "independent" in term.lower() for term in isolation_req.matched_terms)


def test_extract_test_data_management():
    result = extract_source_integration_testing_requirements(
        {
            "id": "brief-data-management",
            "description": (
                "Manage test data lifecycle, data versioning, schema migrations, database migrations, "
                "test data generation and maintenance, data anonymization, synthetic data generation, "
                "and data masking for test data refresh."
            ),
        }
    )

    data_mgmt_req = next((r for r in result.requirements if r.requirement_type == "test_data_management"), None)
    assert data_mgmt_req is not None
    assert any("data" in term.lower() or "migration" in term.lower() for term in data_mgmt_req.matched_terms)


def test_extract_flaky_tests():
    result = extract_source_integration_testing_requirements(
        {
            "id": "brief-flaky",
            "description": (
                "Address flaky tests and flakiness issues, handle non-deterministic behavior, "
                "intermittent failures, race conditions, timing issues, retry logic, retry mechanisms, "
                "test stability, test reliability, and eventual consistency."
            ),
        }
    )

    flaky_req = next((r for r in result.requirements if r.requirement_type == "flaky_tests"), None)
    assert flaky_req is not None
    assert any("flak" in term.lower() or "race condition" in term.lower() for term in flaky_req.matched_terms)


def test_extract_environment_parity():
    result = extract_source_integration_testing_requirements(
        {
            "id": "brief-parity",
            "description": (
                "Maintain environment parity with production-like environment, prod-like setup, "
                "dev-prod parity, staging-production parity, environment consistency, "
                "prevent configuration drift using infrastructure as code (IaC) with Terraform."
            ),
        }
    )

    parity_req = next((r for r in result.requirements if r.requirement_type == "environment_parity"), None)
    assert parity_req is not None
    assert any("parity" in term.lower() or "production" in term.lower() for term in parity_req.matched_terms)


def test_extract_ci_cd_integration():
    result = extract_source_integration_testing_requirements(
        {
            "id": "brief-cicd",
            "description": (
                "Integrate with CI/CD pipeline including continuous integration, continuous deployment, "
                "GitHub Actions, GitLab CI, Jenkins, CircleCI, Travis, build pipelines, "
                "deployment pipelines, automated tests, test automation with Jest, Pytest, Mocha, and JUnit."
            ),
        }
    )

    cicd_req = next((r for r in result.requirements if r.requirement_type == "ci_cd_integration"), None)
    assert cicd_req is not None
    assert any("ci" in term.lower() or "pipeline" in term.lower() for term in cicd_req.matched_terms)


def test_comprehensive_integration_testing_requirements():
    result = extract_source_integration_testing_requirements(
        {
            "id": "brief-comprehensive",
            "description": (
                "Build integration tests for REST API and GraphQL integration points with comprehensive "
                "test scenarios including edge cases. Use Docker Compose for test environment, "
                "mock external services with WireMock, ensure test isolation with database rollback, "
                "manage test data with fixtures, prevent flaky tests with retry logic, "
                "maintain environment parity using Terraform, and run in CI/CD with GitHub Actions."
            ),
        }
    )

    assert len(result.requirements) >= 5
    assert result.summary["requirement_count"] >= 5
    assert result.summary["type_counts"]["integration_points"] >= 1
    assert result.summary["type_counts"]["test_scenarios"] >= 1


def test_follow_up_questions_present():
    result = extract_source_integration_testing_requirements(
        {
            "id": "brief-questions",
            "description": "Integration tests for API endpoints with mocking and CI/CD.",
        }
    )

    for requirement in result.requirements:
        assert len(requirement.follow_up_questions) > 0


def test_empty_source():
    result = extract_source_integration_testing_requirements(
        {"id": "brief-empty", "description": "Frontend UI work only."}
    )

    assert result.source_brief_id == "brief-empty"
    assert len(result.requirements) == 0
    assert result.summary["requirement_count"] == 0


def test_evidence_truncation():
    long_desc = (
        "Integration tests with comprehensive test scenarios and mock services " * 30
    )
    result = extract_source_integration_testing_requirements(
        {
            "id": "brief-long",
            "description": long_desc,
        }
    )

    for requirement in result.requirements:
        for evidence in requirement.evidence:
            assert len(evidence) <= 200


def test_to_dict_serialization():
    result = extract_source_integration_testing_requirements(
        {
            "id": "brief-dict",
            "description": "Integration tests with API mocking and test isolation in CI/CD.",
        }
    )

    result_dict = result.to_dict()
    assert result_dict["source_brief_id"] == "brief-dict"
    assert isinstance(result_dict["requirements"], list)
    assert isinstance(result_dict["summary"], dict)
    assert "records" in result_dict

    dicts = result.to_dicts()
    assert isinstance(dicts, list)


def test_records_property():
    result = extract_source_integration_testing_requirements(
        {
            "id": "brief-records",
            "description": "Integration tests for microservices with test scenarios.",
        }
    )

    assert result.records == result.requirements


def test_all_requirement_types_have_patterns():
    """Ensure all requirement types have corresponding patterns and questions."""
    result = extract_source_integration_testing_requirements(
        {
            "id": "brief-all-types",
            "description": (
                "Integration points with REST API, test scenarios with edge cases, "
                "test data requirements using fixtures, test environment with Docker Compose, "
                "external service mocking with WireMock, test isolation with database rollback, "
                "test data management with schema migrations, prevent flaky tests with retry logic, "
                "environment parity using Terraform IaC, and CI/CD integration with GitHub Actions."
            ),
        }
    )

    # Should detect all 10 requirement types
    assert len(result.requirements) == 10
    detected_types = {req.requirement_type for req in result.requirements}
    expected_types = {
        "integration_points",
        "test_scenarios",
        "test_data_requirements",
        "environment_needs",
        "external_service_mocking",
        "test_isolation",
        "test_data_management",
        "flaky_tests",
        "environment_parity",
        "ci_cd_integration",
    }
    assert detected_types == expected_types


def test_multiple_evidence_sources():
    result = extract_source_integration_testing_requirements(
        {
            "id": "brief-multi-evidence",
            "title": "API integration tests",
            "description": "Test REST API integration points",
            "requirements": "Include GraphQL integration testing",
            "acceptance_criteria": "Verify microservice endpoints work correctly",
        }
    )

    integration_req = next((r for r in result.requirements if r.requirement_type == "integration_points"), None)
    assert integration_req is not None
    assert len(integration_req.evidence) >= 2
    assert len(integration_req.source_field_paths) >= 2


def test_matched_terms_sorted_case_insensitive():
    result = extract_source_integration_testing_requirements(
        {
            "id": "brief-terms",
            "description": "REST API, GraphQL, API integration, gRPC services",
        }
    )

    integration_req = next((r for r in result.requirements if r.requirement_type == "integration_points"), None)
    assert integration_req is not None
    terms = list(integration_req.matched_terms)
    assert terms == sorted(terms, key=str.casefold)


def test_nested_field_extraction():
    result = extract_source_integration_testing_requirements(
        {
            "id": "brief-nested",
            "metadata": {
                "testing": {
                    "type": "integration tests",
                    "approach": "mock external services",
                }
            },
        }
    )

    assert len(result.requirements) >= 2
    scenario_req = next((r for r in result.requirements if r.requirement_type == "test_scenarios"), None)
    mocking_req = next((r for r in result.requirements if r.requirement_type == "external_service_mocking"), None)
    assert scenario_req is not None
    assert mocking_req is not None


def test_evidence_deduplication():
    result = extract_source_integration_testing_requirements(
        {
            "id": "brief-dedup",
            "title": "Integration test scenarios",
            "description": "Integration test scenarios for API",
            "summary": "Test scenarios for integration tests",
        }
    )

    scenario_req = next((r for r in result.requirements if r.requirement_type == "test_scenarios"), None)
    assert scenario_req is not None
    # Evidence should be deduplicated
    evidence_list = list(scenario_req.evidence)
    assert len(evidence_list) == len(set(e.casefold() for e in evidence_list))


def test_summary_type_counts():
    result = extract_source_integration_testing_requirements(
        {
            "id": "brief-summary",
            "description": (
                "Integration tests with REST API and GraphQL endpoints, "
                "mock services with WireMock, run in CI/CD pipeline."
            ),
        }
    )

    type_counts = result.summary["type_counts"]
    assert isinstance(type_counts, dict)
    assert len(type_counts) == 10  # All requirement types should be in type_counts
    assert type_counts["integration_points"] >= 1
    assert type_counts["external_service_mocking"] >= 1
    assert type_counts["ci_cd_integration"] >= 1
    # Types not found should have count of 0
    for req_type, count in type_counts.items():
        assert count >= 0
