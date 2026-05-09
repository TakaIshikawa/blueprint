"""Generate testing strategy matrix for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar, cast

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


TestType = Literal[
    "unit",
    "integration",
    "e2e",
    "property_based",
    "performance",
    "security",
    "chaos",
    "visual_regression",
    "accessibility",
    "canary",
    "smoke",
    "contract",
]
TestDataStrategy = Literal[
    "fixtures",
    "factories",
    "mocks",
    "real_data_snapshot",
    "generated",
    "property_based",
    "anonymized_production",
]
EnvironmentType = Literal[
    "local",
    "ci",
    "staging",
    "preview",
    "production",
    "isolated",
    "containerized",
]
AutomationLevel = Literal["fully_automated", "partially_automated", "manual"]
TestingGap = Literal[
    "missing_unit_tests",
    "missing_integration_tests",
    "missing_e2e_tests",
    "inadequate_coverage",
    "no_property_testing",
    "missing_performance_tests",
    "missing_security_tests",
    "flaky_tests",
    "environment_parity_issues",
    "missing_test_data_strategy",
    "no_chaos_testing",
    "missing_canary_validation",
    "manual_only_tests",
]

_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_UNIT_TEST_RE = re.compile(
    r"\b(?:unit\s+test|test\s+unit|unittest|isolated\s+test|component\s+test|"
    r"jest|vitest|pytest|mocha|jasmine|xunit|nunit|test\s+coverage)\b",
    re.IGNORECASE,
)
_INTEGRATION_TEST_RE = re.compile(
    r"\b(?:integration\s+test|api\s+test|service\s+test|database\s+test|"
    r"integration|middleware\s+test|integration\s+coverage)\b",
    re.IGNORECASE,
)
_E2E_TEST_RE = re.compile(
    r"\b(?:e2e|end[\s-]to[\s-]end|browser\s+test|ui\s+test|acceptance\s+test|"
    r"cypress|playwright|selenium|puppeteer|scenario\s+test)\b",
    re.IGNORECASE,
)
_PROPERTY_TEST_RE = re.compile(
    r"\b(?:property[\s-]based|property\s+test|quickcheck|hypothesis|fast[\s-]check|"
    r"generative\s+test|fuzz\s+test|fuzzing)\b",
    re.IGNORECASE,
)
_PERFORMANCE_TEST_RE = re.compile(
    r"\b(?:performance\s+test|load\s+test|stress\s+test|benchmark|latency\s+test|"
    r"throughput\s+test|k6|jmeter|gatling|locust)\b",
    re.IGNORECASE,
)
_SECURITY_TEST_RE = re.compile(
    r"\b(?:security\s+test|penetration\s+test|vulnerability\s+scan|sql\s+injection|"
    r"xss\s+test|csrf\s+test|auth\s+test|owasp|snyk|dependabot)\b",
    re.IGNORECASE,
)
_CHAOS_TEST_RE = re.compile(
    r"\b(?:chaos\s+engineer|chaos\s+test|fault\s+injection|resilience\s+test|"
    r"failure\s+test|chaos\s+monkey|gremlin|litmus|chaostoolkit)\b",
    re.IGNORECASE,
)
_CANARY_TEST_RE = re.compile(
    r"\b(?:canary|gradual\s+rollout|progressive\s+deploy|blue[\s-]green|"
    r"shadow\s+test|dark\s+launch|feature\s+toggle)\b",
    re.IGNORECASE,
)
_VISUAL_TEST_RE = re.compile(
    r"\b(?:visual\s+regression|screenshot\s+test|visual\s+test|percy|chromatic|"
    r"applitools|backstop)\b",
    re.IGNORECASE,
)
_ACCESSIBILITY_TEST_RE = re.compile(
    r"\b(?:accessibility\s+test|a11y|wcag|axe|aria|screen\s+reader|keyboard\s+nav)\b",
    re.IGNORECASE,
)
_SMOKE_TEST_RE = re.compile(
    r"\b(?:smoke\s+test|sanity\s+test|health\s+check|basic\s+validation)\b",
    re.IGNORECASE,
)
_CONTRACT_TEST_RE = re.compile(
    r"\b(?:contract\s+test|pact|consumer\s+driven|api\s+contract|schema\s+test)\b",
    re.IGNORECASE,
)
_COVERAGE_RE = re.compile(
    r"\b(?:coverage|code\s+coverage|test\s+coverage|coverage\s+threshold|"
    r"coverage\s+report|(?:80|90|95|100)%?\s+coverage)\b",
    re.IGNORECASE,
)
_FLAKY_RE = re.compile(
    r"\b(?:flaky|flakiness|intermittent\s+fail|unstable\s+test|retry\s+test|"
    r"non[\s-]deterministic)\b",
    re.IGNORECASE,
)
_MOCK_RE = re.compile(
    r"\b(?:mocks?|stubs?|spies?|spy|fakes?|fake|test\s+doubles?|sinon|jest\.mock|unittest\.mock)\b",
    re.IGNORECASE,
)
_FIXTURE_RE = re.compile(
    r"\b(?:fixtures?|test\s+data|seed\s+data|factories?|factory[\s_]?bot|faker)\b",
    re.IGNORECASE,
)
_CI_RE = re.compile(
    r"\b(?:ci|continuous\s+integration|github\s+actions|gitlab\s+ci|jenkins|"
    r"circleci|travis|buildkite|\.github/workflows)\b",
    re.IGNORECASE,
)
_ENV_PARITY_RE = re.compile(
    r"\b(?:environment\s+parity|dev[\s-]?prod\s+parity|staging|preview|"
    r"production[\s-]?like|docker|container|infrastructure\s+as\s+code)\b",
    re.IGNORECASE,
)

_TEST_TYPE_ORDER: dict[TestType, int] = {
    "unit": 0,
    "integration": 1,
    "contract": 2,
    "e2e": 3,
    "smoke": 4,
    "visual_regression": 5,
    "accessibility": 6,
    "property_based": 7,
    "performance": 8,
    "security": 9,
    "chaos": 10,
    "canary": 11,
}

_AUTOMATION_ORDER: dict[AutomationLevel, int] = {
    "fully_automated": 0,
    "partially_automated": 1,
    "manual": 2,
}


@dataclass(frozen=True, slots=True)
class TestingRequirement:
    """Individual testing requirement extracted from a task."""

    test_type: TestType
    coverage_target: str | None = None
    data_strategy: TestDataStrategy | None = None
    environment: EnvironmentType | None = None
    automation_level: AutomationLevel = "fully_automated"
    description: str | None = None
    tools_mentioned: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "test_type": self.test_type,
            "coverage_target": self.coverage_target,
            "data_strategy": self.data_strategy,
            "environment": self.environment,
            "automation_level": self.automation_level,
            "description": self.description,
            "tools_mentioned": list(self.tools_mentioned),
        }


@dataclass(frozen=True, slots=True)
class TaskTestingRecord:
    """Testing strategy assessment for a single execution task."""

    task_id: str
    title: str
    test_types: tuple[TestType, ...]
    requirements: tuple[TestingRequirement, ...]
    gaps: tuple[TestingGap, ...]
    completeness_score: float
    automation_level: AutomationLevel
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "test_types": list(self.test_types),
            "requirements": [req.to_dict() for req in self.requirements],
            "gaps": list(self.gaps),
            "completeness_score": self.completeness_score,
            "automation_level": self.automation_level,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TestingStrategyMatrix:
    """Complete testing strategy matrix for an execution plan."""

    plan_id: str | None = None
    records: tuple[TaskTestingRecord, ...] = field(default_factory=tuple)

    @property
    def summary(self) -> dict[str, Any]:
        """Return summary metrics."""
        if not self.records:
            return {
                "task_count": 0,
                "average_completeness_score": 0.0,
                "test_types_coverage": {},
                "total_gaps_count": 0,
                "automation_distribution": {},
                "recommendations": [],
            }

        test_types_count: dict[str, int] = {}
        for record in self.records:
            for test_type in record.test_types:
                test_types_count[test_type] = test_types_count.get(test_type, 0) + 1

        automation_dist: dict[str, int] = {}
        for record in self.records:
            automation_dist[record.automation_level] = (
                automation_dist.get(record.automation_level, 0) + 1
            )

        avg_score = sum(r.completeness_score for r in self.records) / len(self.records)
        total_gaps = sum(len(r.gaps) for r in self.records)

        recommendations = _generate_recommendations(
            self.records, avg_score, test_types_count, total_gaps
        )

        return {
            "task_count": len(self.records),
            "average_completeness_score": round(avg_score, 2),
            "test_types_coverage": dict(sorted(test_types_count.items())),
            "total_gaps_count": total_gaps,
            "automation_distribution": dict(sorted(automation_dist.items())),
            "recommendations": recommendations,
        }

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "plan_id": self.plan_id,
            "summary": self.summary,
            "records": [record.to_dict() for record in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return testing records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render the matrix as deterministic Markdown."""
        title = "# Testing Strategy Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]

        if not self.records:
            lines.extend(["", "No execution tasks were analyzed."])
            return "\n".join(lines)

        summary = self.summary
        lines.extend(
            [
                "",
                "## Summary",
                f"- **Tasks analyzed**: {summary['task_count']}",
                f"- **Average completeness**: {summary['average_completeness_score']:.0%}",
                f"- **Total gaps identified**: {summary['total_gaps_count']}",
                "",
            ]
        )

        if summary["recommendations"]:
            lines.append("### Recommendations")
            for rec in summary["recommendations"]:
                lines.append(f"- {rec}")
            lines.append("")

        lines.extend(
            [
                "## Testing Matrix",
                "| Task | Test Types | Completeness | Automation | Gaps |",
                "| --- | --- | --- | --- | --- |",
            ]
        )

        for record in self.records:
            test_types_str = ", ".join(record.test_types) if record.test_types else "none"
            gaps_str = f"{len(record.gaps)} gap(s)" if record.gaps else "none"
            lines.append(
                f"| {_markdown_cell(f'{record.task_id}: {record.title}')} | "
                f"{_markdown_cell(test_types_str)} | "
                f"{record.completeness_score:.0%} | "
                f"{record.automation_level} | "
                f"{gaps_str} |"
            )

        return "\n".join(lines)


def build_testing_strategy_matrix(
    source: Mapping[str, Any] | ExecutionPlan,
) -> TestingStrategyMatrix:
    """Generate testing strategy matrix from an execution plan."""
    plan = _plan_payload(source)
    tasks = _task_payloads(plan.get("tasks"))

    records = tuple(
        sorted(
            (
                _build_record(
                    task,
                    fallback_id=f"task-{index}",
                    plan_test_strategy=_optional_text(plan.get("test_strategy")),
                )
                for index, task in enumerate(tasks, start=1)
            ),
            key=_record_sort_key,
        )
    )

    return TestingStrategyMatrix(plan_id=_optional_text(plan.get("id")), records=records)


def testing_strategy_matrix_to_dict(matrix: TestingStrategyMatrix) -> dict[str, Any]:
    """Serialize a testing strategy matrix to a plain dictionary."""
    return matrix.to_dict()


testing_strategy_matrix_to_dict.__test__ = False


def testing_strategy_matrix_to_markdown(matrix: TestingStrategyMatrix) -> str:
    """Render a testing strategy matrix as Markdown."""
    return matrix.to_markdown()


testing_strategy_matrix_to_markdown.__test__ = False


def summarize_testing_strategy(
    source: Mapping[str, Any] | ExecutionPlan | TestingStrategyMatrix,
) -> dict[str, Any]:
    """Return testing strategy summary for a plan or matrix."""
    if isinstance(source, TestingStrategyMatrix):
        return source.summary
    return build_testing_strategy_matrix(source).summary


summarize_testing_strategy.__test__ = False


def _build_record(
    task: Mapping[str, Any],
    *,
    fallback_id: str,
    plan_test_strategy: str | None,
) -> TaskTestingRecord:
    """Build a testing record for a single task."""
    task_id = _optional_text(task.get("id")) or fallback_id
    title = _optional_text(task.get("title")) or task_id
    context = _task_context(task, plan_test_strategy)
    metadata = task.get("metadata") if isinstance(task.get("metadata"), Mapping) else {}

    # Extract testing signals
    test_types = _extract_test_types(context, metadata)
    requirements = _extract_requirements(context, metadata, test_types)
    gaps = _identify_gaps(context, metadata, test_types, requirements)
    completeness_score = _calculate_completeness(test_types, requirements, gaps)
    automation_level = _determine_automation_level(context, metadata, requirements)
    evidence = _build_evidence(context, test_types, requirements, gaps, completeness_score)

    return TaskTestingRecord(
        task_id=task_id,
        title=title,
        test_types=test_types,
        requirements=requirements,
        gaps=gaps,
        completeness_score=completeness_score,
        automation_level=automation_level,
        evidence=tuple(evidence),
    )


def _extract_test_types(context: str, metadata: Mapping[str, Any]) -> tuple[TestType, ...]:
    """Extract test types mentioned in the context."""
    types: list[TestType] = []

    # Check for explicit metadata
    explicit = _metadata_value(metadata, "test_types", "testing_types")
    if explicit:
        for test_type in explicit.lower().split(","):
            test_type = test_type.strip()
            if test_type in _TEST_TYPE_ORDER:
                types.append(cast(TestType, test_type))

    # Pattern matching
    if _UNIT_TEST_RE.search(context):
        types.append("unit")
    if _INTEGRATION_TEST_RE.search(context):
        types.append("integration")
    if _E2E_TEST_RE.search(context):
        types.append("e2e")
    if _PROPERTY_TEST_RE.search(context):
        types.append("property_based")
    if _PERFORMANCE_TEST_RE.search(context):
        types.append("performance")
    if _SECURITY_TEST_RE.search(context):
        types.append("security")
    if _CHAOS_TEST_RE.search(context):
        types.append("chaos")
    if _CANARY_TEST_RE.search(context):
        types.append("canary")
    if _VISUAL_TEST_RE.search(context):
        types.append("visual_regression")
    if _ACCESSIBILITY_TEST_RE.search(context):
        types.append("accessibility")
    if _SMOKE_TEST_RE.search(context):
        types.append("smoke")
    if _CONTRACT_TEST_RE.search(context):
        types.append("contract")

    return tuple(_dedupe(types))


def _extract_requirements(
    context: str,
    metadata: Mapping[str, Any],
    test_types: tuple[TestType, ...],
) -> tuple[TestingRequirement, ...]:
    """Extract detailed testing requirements."""
    requirements: list[TestingRequirement] = []

    # Extract coverage target - look for patterns like "90% coverage" or "Coverage >= 90%"
    coverage_match = re.search(r"(\d+)\s*%\s*(?:unit\s+test\s+)?(?:coverage|cov)", context, re.IGNORECASE)
    if not coverage_match:
        # Try "coverage >= 90%" or "Coverage: 90%"
        coverage_match = re.search(r"coverage\s*(?:>=|>|of|:|target|threshold)?\s*(\d+)\s*%", context, re.IGNORECASE)
    if not coverage_match:
        # Try just "90% unit test" or similar
        coverage_match = re.search(r"(\d+)\s*%(?:\s+(?:unit|test|code))?", context, re.IGNORECASE)
    coverage_target = f"{coverage_match.group(1)}%" if coverage_match else None

    # Determine data strategy
    data_strategy: TestDataStrategy | None = None
    if _MOCK_RE.search(context):
        data_strategy = "mocks"
    elif _FIXTURE_RE.search(context):
        data_strategy = "fixtures"
    elif _PROPERTY_TEST_RE.search(context):
        data_strategy = "property_based"
    elif "factory" in context.lower():
        data_strategy = "factories"

    # Determine environment (check all, not elif, to catch multiple)
    environments: list[EnvironmentType] = []
    if _CI_RE.search(context):
        environments.append("ci")
    if "docker" in context.lower() or "container" in context.lower():
        environments.append("containerized")
    if "staging" in context.lower():
        environments.append("staging")
    if "local" in context.lower() and not environments:
        environments.append("local")
    if "production" in context.lower():
        environments.append("production")

    environment = environments[0] if environments else None

    # Determine automation level
    automation: AutomationLevel = "fully_automated"
    if "manual" in context.lower():
        if "partially" in context.lower() or "semi" in context.lower():
            automation = "partially_automated"
        else:
            automation = "manual"

    # Extract tools
    tools: list[str] = []
    tool_patterns = [
        "jest", "vitest", "pytest", "mocha", "cypress", "playwright",
        "selenium", "k6", "jmeter", "hypothesis", "quickcheck", "percy",
    ]
    for tool in tool_patterns:
        if re.search(rf"\b{tool}\b", context, re.IGNORECASE):
            tools.append(tool)

    # Create requirements for each test type
    for test_type in test_types:
        requirements.append(
            TestingRequirement(
                test_type=test_type,
                coverage_target=coverage_target,
                data_strategy=data_strategy,
                environment=environment,
                automation_level=automation,
                tools_mentioned=tuple(tools),
            )
        )

    # If no test types but we have testing-related info, create a generic requirement
    if not test_types and (data_strategy or environment or tools or coverage_target):
        # Try to infer test type from context
        inferred_type: TestType = "unit"  # default
        if "integration" in context.lower() or "api" in context.lower():
            inferred_type = "integration"
        elif "e2e" in context.lower() or "end-to-end" in context.lower():
            inferred_type = "e2e"

        requirements.append(
            TestingRequirement(
                test_type=inferred_type,
                coverage_target=coverage_target,
                data_strategy=data_strategy,
                environment=environment,
                automation_level=automation,
                tools_mentioned=tuple(tools),
            )
        )

    return tuple(requirements)


def _identify_gaps(
    context: str,
    metadata: Mapping[str, Any],
    test_types: tuple[TestType, ...],
    requirements: tuple[TestingRequirement, ...],
) -> tuple[TestingGap, ...]:
    """Identify testing gaps."""
    gaps: list[TestingGap] = []

    # Check for missing test types
    if "unit" not in test_types and not _mentions_no_testing(context):
        gaps.append("missing_unit_tests")
    if "integration" not in test_types and _requires_integration(context):
        gaps.append("missing_integration_tests")
    if "e2e" not in test_types and _requires_e2e(context):
        gaps.append("missing_e2e_tests")

    # Check coverage - if no explicit coverage target or low coverage, flag it
    coverage_target = None
    if requirements:
        coverage_target = next((req.coverage_target for req in requirements if req.coverage_target), None)

    if coverage_target:
        # Extract numeric value
        coverage_match = re.search(r"(\d+)", coverage_target)
        if coverage_match and int(coverage_match.group(1)) < 80:
            gaps.append("inadequate_coverage")
    elif not test_types and not _mentions_no_testing(context):
        # No tests at all
        gaps.append("inadequate_coverage")

    # Check for advanced testing gaps
    if _should_have_property_tests(context) and "property_based" not in test_types:
        gaps.append("no_property_testing")
    if _should_have_performance_tests(context) and "performance" not in test_types:
        gaps.append("missing_performance_tests")
    if _should_have_security_tests(context) and "security" not in test_types:
        gaps.append("missing_security_tests")

    # Check for flakiness
    if _FLAKY_RE.search(context):
        gaps.append("flaky_tests")

    # Check environment parity
    if _ENV_PARITY_RE.search(context) and "dev-prod" not in context.lower():
        gaps.append("environment_parity_issues")

    # Check test data strategy
    if not any(req.data_strategy for req in requirements) and test_types:
        gaps.append("missing_test_data_strategy")

    # Check for chaos/canary testing in critical systems
    if _should_have_chaos_tests(context) and "chaos" not in test_types:
        gaps.append("no_chaos_testing")
    if _should_have_canary_tests(context) and "canary" not in test_types:
        gaps.append("missing_canary_validation")

    # Check automation - only add gap if there are requirements with manual automation
    if requirements and any(req.automation_level == "manual" for req in requirements):
        gaps.append("manual_only_tests")
    elif not requirements and "manual" in context.lower():
        # No requirements extracted, but manual testing mentioned
        gaps.append("manual_only_tests")

    return tuple(_dedupe(gaps))


def _calculate_completeness(
    test_types: tuple[TestType, ...],
    requirements: tuple[TestingRequirement, ...],
    gaps: tuple[TestingGap, ...],
) -> float:
    """Calculate testing completeness score (0.0 to 1.0)."""
    score = 0.0

    # Base score from test types (55%)
    type_weights = {
        "unit": 0.25,
        "integration": 0.15,
        "e2e": 0.10,
        "contract": 0.05,
    }
    for test_type in test_types:
        score += type_weights.get(test_type, 0.0)

    # Bonus for advanced testing (25%)
    advanced_weights = {
        "property_based": 0.08,
        "performance": 0.08,
        "security": 0.08,
        "chaos": 0.08,
        "canary": 0.08,
        "visual_regression": 0.05,
        "accessibility": 0.05,
        "smoke": 0.03,
    }
    for test_type in test_types:
        score += advanced_weights.get(test_type, 0.0)

    # Requirements quality (20%)
    if requirements:
        req_score = 0.0
        # Coverage target is important
        if any(req.coverage_target for req in requirements):
            coverage_targets = [req.coverage_target for req in requirements if req.coverage_target]
            if coverage_targets:
                # Extract numeric value from first target
                coverage_match = re.search(r"(\d+)", coverage_targets[0])
                if coverage_match:
                    coverage_value = int(coverage_match.group(1))
                    # Higher coverage = higher score
                    req_score += 0.10 if coverage_value >= 90 else 0.08
        if any(req.data_strategy for req in requirements):
            req_score += 0.04
        if any(req.environment for req in requirements):
            req_score += 0.04
        if all(req.automation_level == "fully_automated" for req in requirements):
            req_score += 0.04
        score += req_score

    # Penalties for gaps (deduct based on severity)
    # Less harsh penalties for specialized test types (chaos, canary, etc.) without unit tests
    has_specialized_tests = any(
        t in test_types for t in ["chaos", "canary", "property_based", "security", "performance"]
    )

    critical_gaps = {
        "missing_unit_tests": 0.10 if has_specialized_tests else 0.15,
        "inadequate_coverage": 0.05,
        "manual_only_tests": 0.05,
    }
    for gap in gaps:
        score -= critical_gaps.get(gap, 0.02)

    return max(0.0, min(1.0, score))


def _determine_automation_level(
    context: str,
    metadata: Mapping[str, Any],
    requirements: tuple[TestingRequirement, ...],
) -> AutomationLevel:
    """Determine overall automation level."""
    if not requirements:
        return "manual"

    automation_levels = [req.automation_level for req in requirements]
    if all(level == "fully_automated" for level in automation_levels):
        return "fully_automated"
    if any(level == "manual" for level in automation_levels):
        if any(level == "fully_automated" for level in automation_levels):
            return "partially_automated"
        return "manual"
    return "partially_automated"


def _build_evidence(
    context: str,
    test_types: tuple[TestType, ...],
    requirements: tuple[TestingRequirement, ...],
    gaps: tuple[TestingGap, ...],
    completeness_score: float,
) -> list[str]:
    """Build evidence list for the testing assessment."""
    evidence: list[str] = []

    if test_types:
        evidence.append(f"Test types identified: {', '.join(test_types)}.")
    else:
        evidence.append("No test types identified.")

    if requirements:
        evidence.append(f"{len(requirements)} testing requirement(s) extracted.")

    coverage_mentions = [req.coverage_target for req in requirements if req.coverage_target]
    if coverage_mentions:
        evidence.append(f"Coverage target: {coverage_mentions[0]}.")

    data_strategies = set(req.data_strategy for req in requirements if req.data_strategy)
    if data_strategies:
        evidence.append(f"Data strategies: {', '.join(data_strategies)}.")

    if gaps:
        evidence.append(f"{len(gaps)} gap(s) identified: {', '.join(gaps[:3])}.")

    evidence.append(f"Completeness score: {completeness_score:.0%}.")

    return evidence


def _generate_recommendations(
    records: tuple[TaskTestingRecord, ...],
    avg_score: float,
    test_types_count: dict[str, int],
    total_gaps: int,
) -> list[str]:
    """Generate testing recommendations based on analysis."""
    recommendations: list[str] = []

    if avg_score < 0.5:
        recommendations.append(
            "Low testing completeness detected. Consider adding comprehensive test coverage."
        )

    if test_types_count.get("unit", 0) < len(records) * 0.5:
        recommendations.append("Increase unit test coverage across tasks.")

    if test_types_count.get("integration", 0) < len(records) * 0.3:
        recommendations.append("Add integration tests for multi-component interactions.")

    if "e2e" not in test_types_count and len(records) > 3:
        recommendations.append("Consider adding end-to-end tests for critical user flows.")

    if total_gaps > len(records) * 2:
        recommendations.append("Address testing gaps to improve overall quality assurance.")

    manual_count = sum(1 for r in records if r.automation_level == "manual")
    if manual_count > len(records) * 0.3:
        recommendations.append("Automate manual tests to improve CI/CD reliability.")

    if "property_based" not in test_types_count and any(
        "algorithm" in r.title.lower() or "validation" in r.title.lower() for r in records
    ):
        recommendations.append("Consider property-based testing for algorithmic code.")

    return recommendations


def _mentions_no_testing(context: str) -> bool:
    """Check if context explicitly mentions no testing needed."""
    return bool(
        re.search(r"\b(?:no\s+test|skip\s+test|test\s+not\s+required)\b", context, re.IGNORECASE)
    )


def _requires_integration(context: str) -> bool:
    """Check if task requires integration testing."""
    return bool(
        re.search(
            r"\b(?:api|database|service|external|third[\s-]?party|integration)\b",
            context,
            re.IGNORECASE,
        )
    )


def _requires_e2e(context: str) -> bool:
    """Check if task requires e2e testing."""
    return bool(
        re.search(
            r"\b(?:user\s+flow|workflow|journey|checkout|onboard|ui|frontend|browser)\b",
            context,
            re.IGNORECASE,
        )
    )


def _should_have_property_tests(context: str) -> bool:
    """Check if task should have property-based tests."""
    return bool(
        re.search(
            r"\b(?:algorithm|parser|encoder|serializ|transform|calculation|property[\s-]?based)\b",
            context,
            re.IGNORECASE,
        )
    )


def _should_have_performance_tests(context: str) -> bool:
    """Check if task should have performance tests."""
    return bool(
        re.search(
            r"\b(?:performance|latency|throughput|scale|load|concurrent|optimization)\b",
            context,
            re.IGNORECASE,
        )
    )


def _should_have_security_tests(context: str) -> bool:
    """Check if task should have security tests."""
    return bool(
        re.search(
            r"\b(?:auth|security|permission|access\s+control|encrypt|credential|token|vulnerability)\b",
            context,
            re.IGNORECASE,
        )
    )


def _should_have_chaos_tests(context: str) -> bool:
    """Check if task should have chaos tests."""
    return bool(
        re.search(
            r"\b(?:critical|resilience|fault\s+toleran|high\s+availability|disaster\s+recovery)\b",
            context,
            re.IGNORECASE,
        )
    )


def _should_have_canary_tests(context: str) -> bool:
    """Check if task should have canary tests."""
    return bool(
        re.search(
            r"\b(?:deploy|release|rollout|production|gradual|phased|canary)\b",
            context,
            re.IGNORECASE,
        )
    )


def _record_sort_key(record: TaskTestingRecord) -> tuple[float, int, str]:
    """Sort records by completeness (desc), automation, then task_id."""
    return (
        -record.completeness_score,  # Higher completeness first
        _AUTOMATION_ORDER.get(record.automation_level, 999),
        record.task_id,
    )


def _task_context(task: Mapping[str, Any], plan_test_strategy: str | None) -> str:
    """Build context string from task fields."""
    metadata = task.get("metadata") if isinstance(task.get("metadata"), Mapping) else {}
    values = [
        _text(task.get("title")),
        _text(task.get("description")),
        *_strings(task.get("acceptance_criteria")),
        _text(task.get("test_command")),
        *_strings(task.get("files_or_modules")),
        *_strings(metadata),
        plan_test_strategy or "",
    ]
    return " ".join(value for value in values if value)


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    """Convert plan to dictionary."""
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        return dict(plan)


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    """Convert tasks to list of dictionaries."""
    if not isinstance(value, list):
        return []
    tasks: list[dict[str, Any]] = []
    for item in value:
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif hasattr(item, "model_dump"):
            dumped = item.model_dump(mode="python")
            if isinstance(dumped, Mapping):
                tasks.append(dict(dumped))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


def _metadata_value(metadata: Mapping[str, Any], *keys: str) -> str | None:
    """Extract first matching metadata value."""
    values = _metadata_values(metadata, *keys)
    return values[0] if values else None


def _metadata_values(metadata: Mapping[str, Any], *keys: str) -> list[str]:
    """Extract all matching metadata values."""
    values: list[str] = []
    wanted = {key.lower() for key in keys}
    for key, value in metadata.items():
        normalized = str(key).lower()
        if normalized in wanted:
            values.extend(_strings(value))
        elif isinstance(value, Mapping):
            values.extend(_metadata_values(value, *keys))
    return values


def _strings(value: Any) -> list[str]:
    """Extract strings from various data structures."""
    if value is None:
        return []
    if isinstance(value, str):
        text = _optional_text(value)
        return [text] if text else []
    if isinstance(value, Mapping):
        strings: list[str] = []
        for key in sorted(value, key=lambda item: str(item)):
            strings.extend(_strings(value[key]))
        return strings
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        strings: list[str] = []
        for item in items:
            strings.extend(_strings(item))
        return strings
    text = _optional_text(value)
    return [text] if text else []


def _optional_text(value: Any) -> str | None:
    """Convert value to optional text."""
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    """Convert value to normalized text."""
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _dedupe(values: Iterable[_T]) -> tuple[_T, ...]:
    """Remove duplicates while preserving order."""
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return tuple(deduped)


def _markdown_cell(value: str) -> str:
    """Escape markdown table cell content."""
    return value.replace("|", "\\|").replace("\n", " ")


__all__ = [
    "AutomationLevel",
    "EnvironmentType",
    "TaskTestingRecord",
    "TestDataStrategy",
    "TestingGap",
    "TestingRequirement",
    "TestingStrategyMatrix",
    "TestType",
    "build_testing_strategy_matrix",
    "summarize_testing_strategy",
    "testing_strategy_matrix_to_dict",
    "testing_strategy_matrix_to_markdown",
]
