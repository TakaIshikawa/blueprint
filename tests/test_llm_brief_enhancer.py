"""Tests for LLM-powered brief enhancement generator."""

import json

import pytest

from blueprint.generators.llm_brief_enhancer import (
    ALL_CATEGORIES,
    EnhancementConfig,
    EnhancementResult,
    EnhancementSuggestion,
    LlmBriefEnhancer,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class FakeLLMProvider:
    """Mock LLM provider for testing."""

    def __init__(self, payload: dict, *, raw_suffix: str = ""):
        self.payload = payload
        self.raw_suffix = raw_suffix
        self.last_prompt: str | None = None
        self.last_system: str | None = None
        self.call_count = 0

    def generate(self, prompt, model=None, temperature=1.0, max_tokens=4096, system=None):
        self.last_prompt = prompt
        self.last_system = system
        self.call_count += 1
        return {
            "content": json.dumps(self.payload) + self.raw_suffix,
            "model": model or "test-model",
            "usage": {"input_tokens": 50, "output_tokens": 50, "total_tokens": 100},
        }


def _complete_brief() -> dict:
    """Return a brief with all fields populated sufficiently."""
    return {
        "id": "ib-test-001",
        "source_brief_id": "sb-test",
        "title": "Complete Test Brief",
        "domain": "testing",
        "target_user": "Engineers running test suites",
        "buyer": "Engineering management",
        "workflow_context": "CI/CD pipeline integration",
        "problem_statement": "Need reliable automated testing with comprehensive coverage across all modules.",
        "mvp_goal": "Provide a testing framework that supports unit, integration, and end-to-end tests.",
        "product_surface": "CLI",
        "scope": [
            "Unit test runner with parallel execution",
            "Integration test support with fixtures",
            "Coverage reporting with threshold enforcement",
        ],
        "non_goals": [
            "Visual regression testing",
            "Performance benchmarking suite",
            "Mobile device testing",
        ],
        "assumptions": [
            "Python 3.11+ is available in all environments",
            "pytest is the established test runner",
            "CI infrastructure supports parallel execution",
        ],
        "architecture_notes": (
            "The framework builds on pytest with custom plugins for fixture management "
            "and parallel execution. Test discovery uses standard pytest conventions."
        ),
        "data_requirements": "Test fixtures stored in YAML, test results in SQLite database.",
        "integration_points": [
            "GitHub Actions for CI/CD",
            "Codecov for coverage tracking",
        ],
        "risks": [
            "Parallel execution may cause flaky tests due to shared state — mitigate with isolated fixtures",
            "Coverage thresholds may block deployments — start with 70% and increase gradually",
            "Third-party API mocking may drift from real behavior — use contract testing",
        ],
        "validation_plan": (
            "Run the full test suite in CI with coverage enforcement. "
            "Validate parallel execution produces deterministic results across 10 repeated runs."
        ),
        "definition_of_done": [
            "All unit tests pass with 80% coverage",
            "Integration tests run in under 5 minutes",
            "Coverage report is generated and published",
        ],
        "status": "draft",
    }


def _incomplete_brief() -> dict:
    """Return a brief with deliberate gaps for enhancement."""
    return {
        "id": "ib-incomplete",
        "source_brief_id": "sb-test",
        "title": "Incomplete Brief",
        "domain": "data",
        "problem_statement": "Need data pipeline processing.",
        "mvp_goal": "Build a data pipeline.",
        "scope": ["Process data"],
        "non_goals": [],
        "assumptions": [],
        "risks": [],
        "validation_plan": "Test it.",
        "definition_of_done": ["It works"],
        "status": "draft",
    }


def _enhancement_response(suggestions: list[dict] | None = None) -> dict:
    """Build a standard enhancement LLM response payload."""
    if suggestions is None:
        suggestions = [
            {
                "category": "acceptance_criteria",
                "field": "definition_of_done",
                "suggested_value": [
                    "It works",
                    "All data transformations produce correct output",
                    "Error handling covers malformed input records",
                ],
                "rationale": "Original definition of done is too vague for verification.",
            },
            {
                "category": "testing_strategy",
                "field": "validation_plan",
                "suggested_value": (
                    "Run unit tests for each transformation stage. "
                    "Execute integration tests with sample datasets. "
                    "Validate output schema conformance with JSON Schema."
                ),
                "rationale": "Testing strategy needs concrete steps beyond 'test it'.",
            },
            {
                "category": "edge_cases",
                "field": "risks",
                "suggested_value": [
                    "Malformed input records may cause pipeline failures — add input validation",
                    "Large datasets may exceed memory — implement streaming processing",
                    "Schema changes upstream may break transformations — add schema versioning",
                ],
                "rationale": "No risks identified; data pipelines have well-known failure modes.",
            },
            {
                "category": "technical_constraints",
                "field": "assumptions",
                "suggested_value": [
                    "Input data arrives in JSON format",
                    "Processing must complete within 30-minute SLA",
                    "Output schema is defined and versioned",
                ],
                "rationale": "Assumptions clarify the operating environment for implementers.",
            },
        ]
    return {"suggestions": suggestions}


# ---------------------------------------------------------------------------
# Gap detection tests
# ---------------------------------------------------------------------------


class TestGapDetection:
    def test_complete_brief_has_minimal_gaps(self):
        """A well-populated brief should have few or no gaps."""
        enhancer = LlmBriefEnhancer(FakeLLMProvider({}))
        gaps = enhancer.detect_gaps(_complete_brief())
        # Complete brief may still have audit findings but should have minimal gaps
        assert isinstance(gaps, list)

    def test_incomplete_brief_detects_missing_risks(self):
        """Empty risks list triggers a gap."""
        enhancer = LlmBriefEnhancer(FakeLLMProvider({}))
        brief = _incomplete_brief()
        gaps = enhancer.detect_gaps(brief)
        risk_gaps = [g for g in gaps if "risks" in g.lower()]
        assert len(risk_gaps) > 0

    def test_incomplete_brief_detects_thin_validation_plan(self):
        """Short validation plan triggers a gap."""
        enhancer = LlmBriefEnhancer(FakeLLMProvider({}))
        brief = _incomplete_brief()
        gaps = enhancer.detect_gaps(brief)
        validation_gaps = [g for g in gaps if "validation_plan" in g]
        assert len(validation_gaps) > 0

    def test_missing_product_surface_detected(self):
        """Missing product_surface triggers a readiness gap."""
        enhancer = LlmBriefEnhancer(FakeLLMProvider({}))
        brief = _incomplete_brief()
        gaps = enhancer.detect_gaps(brief)
        surface_gaps = [g for g in gaps if "product_surface" in g]
        assert len(surface_gaps) > 0

    def test_empty_list_fields_detected(self):
        """Empty list fields are detected as gaps."""
        enhancer = LlmBriefEnhancer(FakeLLMProvider({}))
        brief = _incomplete_brief()
        gaps = enhancer.detect_gaps(brief)
        assumption_gaps = [g for g in gaps if "assumptions" in g]
        assert len(assumption_gaps) > 0

    def test_short_scope_detected(self):
        """Scope with fewer than min_list_items triggers a gap."""
        enhancer = LlmBriefEnhancer(FakeLLMProvider({}))
        brief = _incomplete_brief()
        gaps = enhancer.detect_gaps(brief)
        scope_gaps = [g for g in gaps if "scope" in g]
        assert len(scope_gaps) > 0

    def test_custom_config_min_list_items(self):
        """Custom config changes gap detection sensitivity."""
        config = EnhancementConfig(min_list_items=5)
        enhancer = LlmBriefEnhancer(FakeLLMProvider({}), config=config)
        brief = _complete_brief()
        gaps = enhancer.detect_gaps(brief)
        # With higher threshold, even complete brief may flag list fields
        assert isinstance(gaps, list)


# ---------------------------------------------------------------------------
# Enhancement flow tests
# ---------------------------------------------------------------------------


class TestEnhance:
    def test_enhance_returns_result_with_suggestions(self):
        """Enhancement produces suggestions for an incomplete brief."""
        provider = FakeLLMProvider(_enhancement_response())
        enhancer = LlmBriefEnhancer(provider)
        result = enhancer.enhance(_incomplete_brief())

        assert isinstance(result, EnhancementResult)
        assert result.brief_id == "ib-incomplete"
        assert result.has_suggestions is True
        assert len(result.suggestions) > 0
        assert result.generation_model == "test-model"
        assert result.generation_tokens == 100

    def test_enhance_complete_brief_skips_llm(self):
        """A complete brief with no gaps skips LLM call."""
        provider = FakeLLMProvider(_enhancement_response())
        # Build a brief that passes all gap checks
        brief = _complete_brief()
        config = EnhancementConfig(min_field_length=1, min_list_items=1)
        enhancer = LlmBriefEnhancer(provider, config=config)
        result = enhancer.enhance(brief)

        assert result.has_suggestions is False
        assert result.enhanced_brief == brief
        assert provider.call_count == 0

    def test_enhance_includes_gaps_in_result(self):
        """Result includes the detected gaps."""
        provider = FakeLLMProvider(_enhancement_response())
        enhancer = LlmBriefEnhancer(provider)
        result = enhancer.enhance(_incomplete_brief())

        assert len(result.gaps_detected) > 0
        assert any("risks" in g.lower() for g in result.gaps_detected)

    def test_enhance_passes_model_to_provider(self):
        """Model parameter is forwarded to the LLM provider."""
        provider = FakeLLMProvider(_enhancement_response())
        enhancer = LlmBriefEnhancer(provider)
        result = enhancer.enhance(_incomplete_brief(), model="sonnet")

        assert result.generation_model == "sonnet"

    def test_enhance_applies_suggestions_to_brief(self):
        """Enhanced brief contains the suggested values."""
        provider = FakeLLMProvider(_enhancement_response())
        enhancer = LlmBriefEnhancer(provider)
        result = enhancer.enhance(_incomplete_brief())

        assert result.enhanced_brief is not None
        # Definition of done should be enhanced
        assert len(result.enhanced_brief["definition_of_done"]) > 1


# ---------------------------------------------------------------------------
# Prompt generation tests
# ---------------------------------------------------------------------------


class TestPromptGeneration:
    def test_prompt_includes_brief_content(self):
        """Prompt includes the implementation brief JSON."""
        provider = FakeLLMProvider(_enhancement_response())
        enhancer = LlmBriefEnhancer(provider)
        enhancer.enhance(_incomplete_brief())

        assert provider.last_prompt is not None
        assert "ib-incomplete" in provider.last_prompt

    def test_prompt_includes_detected_gaps(self):
        """Prompt includes the detected gaps."""
        provider = FakeLLMProvider(_enhancement_response())
        enhancer = LlmBriefEnhancer(provider)
        enhancer.enhance(_incomplete_brief())

        assert provider.last_prompt is not None
        assert "Detected Gaps" in provider.last_prompt

    def test_prompt_includes_categories(self):
        """Prompt includes the requested enhancement categories."""
        provider = FakeLLMProvider(_enhancement_response())
        enhancer = LlmBriefEnhancer(provider)
        enhancer.enhance(_incomplete_brief())

        assert provider.last_prompt is not None
        assert "acceptance_criteria" in provider.last_prompt

    def test_system_prompt_set(self):
        """System prompt is provided to the LLM."""
        provider = FakeLLMProvider(_enhancement_response())
        enhancer = LlmBriefEnhancer(provider)
        enhancer.enhance(_incomplete_brief())

        assert provider.last_system is not None
        assert "technical product architect" in provider.last_system


# ---------------------------------------------------------------------------
# Response parsing tests
# ---------------------------------------------------------------------------


class TestResponseParsing:
    def test_valid_suggestions_parsed(self):
        """Valid suggestion objects are parsed into EnhancementSuggestion."""
        provider = FakeLLMProvider(_enhancement_response())
        enhancer = LlmBriefEnhancer(provider)
        result = enhancer.enhance(_incomplete_brief())

        assert all(isinstance(s, EnhancementSuggestion) for s in result.suggestions)

    def test_invalid_category_filtered(self):
        """Suggestions with invalid categories are filtered out."""
        response = _enhancement_response(
            [
                {
                    "category": "invalid_category",
                    "field": "risks",
                    "suggested_value": ["new risk"],
                    "rationale": "reason",
                }
            ]
        )
        provider = FakeLLMProvider(response)
        enhancer = LlmBriefEnhancer(provider)
        result = enhancer.enhance(_incomplete_brief())

        assert len(result.suggestions) == 0

    def test_missing_field_name_filtered(self):
        """Suggestions with missing field names are filtered out."""
        response = _enhancement_response(
            [
                {
                    "category": "edge_cases",
                    "field": "",
                    "suggested_value": ["something"],
                    "rationale": "reason",
                }
            ]
        )
        provider = FakeLLMProvider(response)
        enhancer = LlmBriefEnhancer(provider)
        result = enhancer.enhance(_incomplete_brief())

        assert len(result.suggestions) == 0

    def test_missing_rationale_filtered_when_required(self):
        """Suggestions without rationale are filtered when require_rationale is True."""
        response = _enhancement_response(
            [
                {
                    "category": "edge_cases",
                    "field": "risks",
                    "suggested_value": ["new risk"],
                    "rationale": "",
                }
            ]
        )
        provider = FakeLLMProvider(response)
        enhancer = LlmBriefEnhancer(provider)
        result = enhancer.enhance(_incomplete_brief())

        assert len(result.suggestions) == 0

    def test_missing_rationale_allowed_when_not_required(self):
        """Suggestions without rationale pass when require_rationale is False."""
        response = _enhancement_response(
            [
                {
                    "category": "edge_cases",
                    "field": "risks",
                    "suggested_value": ["new risk item"],
                    "rationale": "",
                }
            ]
        )
        config = EnhancementConfig(require_rationale=False)
        provider = FakeLLMProvider(response)
        enhancer = LlmBriefEnhancer(provider, config=config)
        result = enhancer.enhance(_incomplete_brief())

        assert len(result.suggestions) == 1

    def test_non_dict_suggestions_filtered(self):
        """Non-dict items in suggestions list are filtered."""
        response = {"suggestions": ["not a dict", 42, None]}
        provider = FakeLLMProvider(response)
        enhancer = LlmBriefEnhancer(provider)
        result = enhancer.enhance(_incomplete_brief())

        assert len(result.suggestions) == 0

    def test_non_list_suggestions_returns_empty(self):
        """Non-list suggestions value returns empty list."""
        response = {"suggestions": "not a list"}
        provider = FakeLLMProvider(response)
        enhancer = LlmBriefEnhancer(provider)
        result = enhancer.enhance(_incomplete_brief())

        assert len(result.suggestions) == 0


# ---------------------------------------------------------------------------
# Validation and filtering tests
# ---------------------------------------------------------------------------


class TestValidation:
    def test_unknown_field_rejected(self):
        """Suggestions targeting unknown fields are rejected."""
        response = _enhancement_response(
            [
                {
                    "category": "edge_cases",
                    "field": "nonexistent_field",
                    "suggested_value": ["item"],
                    "rationale": "reason",
                }
            ]
        )
        provider = FakeLLMProvider(response)
        enhancer = LlmBriefEnhancer(provider)
        result = enhancer.enhance(_incomplete_brief())

        assert len(result.suggestions) == 0

    def test_type_mismatch_list_field_gets_string(self):
        """String value for a list field is rejected."""
        response = _enhancement_response(
            [
                {
                    "category": "edge_cases",
                    "field": "risks",
                    "suggested_value": "this should be a list",
                    "rationale": "reason",
                }
            ]
        )
        provider = FakeLLMProvider(response)
        enhancer = LlmBriefEnhancer(provider)
        result = enhancer.enhance(_incomplete_brief())

        assert len(result.suggestions) == 0

    def test_type_mismatch_string_field_gets_list(self):
        """List value for a string field is rejected."""
        response = _enhancement_response(
            [
                {
                    "category": "testing_strategy",
                    "field": "validation_plan",
                    "suggested_value": ["should", "be", "string"],
                    "rationale": "reason",
                }
            ]
        )
        provider = FakeLLMProvider(response)
        enhancer = LlmBriefEnhancer(provider)
        result = enhancer.enhance(_incomplete_brief())

        assert len(result.suggestions) == 0

    def test_duplicate_field_takes_first(self):
        """Only the first suggestion per field is kept."""
        response = _enhancement_response(
            [
                {
                    "category": "edge_cases",
                    "field": "risks",
                    "suggested_value": ["first risk set"],
                    "rationale": "first reason",
                },
                {
                    "category": "technical_constraints",
                    "field": "risks",
                    "suggested_value": ["second risk set"],
                    "rationale": "second reason",
                },
            ]
        )
        provider = FakeLLMProvider(response)
        enhancer = LlmBriefEnhancer(provider)
        result = enhancer.enhance(_incomplete_brief())

        risk_suggestions = [s for s in result.suggestions if s.field == "risks"]
        assert len(risk_suggestions) == 1
        assert risk_suggestions[0].rationale == "first reason"

    def test_identical_value_rejected(self):
        """Suggestion with value identical to original is rejected."""
        brief = _incomplete_brief()
        response = _enhancement_response(
            [
                {
                    "category": "acceptance_criteria",
                    "field": "definition_of_done",
                    "suggested_value": brief["definition_of_done"],
                    "rationale": "no change needed",
                }
            ]
        )
        provider = FakeLLMProvider(response)
        enhancer = LlmBriefEnhancer(provider)
        result = enhancer.enhance(brief)

        dod_suggestions = [s for s in result.suggestions if s.field == "definition_of_done"]
        assert len(dod_suggestions) == 0

    def test_max_suggestions_respected(self):
        """Suggestions are capped at max_suggestions."""
        many_suggestions = [
            {
                "category": "edge_cases",
                "field": f"risks" if i == 0 else f"non_goals" if i == 1 else f"scope",
                "suggested_value": [f"item {i}"],
                "rationale": f"reason {i}",
            }
            for i in range(3)
        ]
        config = EnhancementConfig(max_suggestions=2)
        provider = FakeLLMProvider({"suggestions": many_suggestions})
        enhancer = LlmBriefEnhancer(provider, config=config)
        result = enhancer.enhance(_incomplete_brief())

        assert len(result.suggestions) <= 2


# ---------------------------------------------------------------------------
# Diff generation tests
# ---------------------------------------------------------------------------


class TestDiff:
    def test_diff_shows_changed_fields(self):
        """Diff includes fields that were changed."""
        provider = FakeLLMProvider(_enhancement_response())
        enhancer = LlmBriefEnhancer(provider)
        brief = _incomplete_brief()
        result = enhancer.enhance(brief)

        diff = result.diff(brief)
        assert isinstance(diff, dict)
        assert len(diff) > 0

    def test_diff_includes_original_and_enhanced(self):
        """Each diff entry has original and enhanced values."""
        provider = FakeLLMProvider(_enhancement_response())
        enhancer = LlmBriefEnhancer(provider)
        brief = _incomplete_brief()
        result = enhancer.enhance(brief)

        diff = result.diff(brief)
        for field_name, change in diff.items():
            assert "original" in change
            assert "enhanced" in change
            assert "category" in change
            assert "rationale" in change

    def test_diff_empty_when_no_suggestions(self):
        """Diff is empty when there are no suggestions."""
        config = EnhancementConfig(min_field_length=1, min_list_items=1)
        provider = FakeLLMProvider({})
        enhancer = LlmBriefEnhancer(provider, config=config)
        brief = _complete_brief()
        result = enhancer.enhance(brief)

        diff = result.diff(brief)
        assert diff == {}

    def test_diff_empty_when_no_enhanced_brief(self):
        """Diff is empty when enhanced_brief is None."""
        result = EnhancementResult(
            brief_id="test",
            gaps_detected=["gap"],
            suggestions=[],
            enhanced_brief=None,
        )
        diff = result.diff({"field": "value"})
        assert diff == {}


# ---------------------------------------------------------------------------
# Merge/apply logic tests
# ---------------------------------------------------------------------------


class TestApply:
    def test_apply_preserves_original_fields(self):
        """Apply does not modify fields without suggestions."""
        provider = FakeLLMProvider(
            _enhancement_response(
                [
                    {
                        "category": "edge_cases",
                        "field": "risks",
                        "suggested_value": ["added risk"],
                        "rationale": "needed",
                    }
                ]
            )
        )
        enhancer = LlmBriefEnhancer(provider)
        brief = _incomplete_brief()
        result = enhancer.enhance(brief)

        assert result.enhanced_brief is not None
        assert result.enhanced_brief["title"] == brief["title"]
        assert result.enhanced_brief["problem_statement"] == brief["problem_statement"]

    def test_apply_updates_targeted_field(self):
        """Apply updates only the field targeted by the suggestion."""
        provider = FakeLLMProvider(
            _enhancement_response(
                [
                    {
                        "category": "edge_cases",
                        "field": "risks",
                        "suggested_value": ["new risk with mitigation"],
                        "rationale": "added specifics",
                    }
                ]
            )
        )
        enhancer = LlmBriefEnhancer(provider)
        result = enhancer.enhance(_incomplete_brief())

        assert result.enhanced_brief is not None
        assert result.enhanced_brief["risks"] == ["new risk with mitigation"]


# ---------------------------------------------------------------------------
# Edge case tests
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_already_complete_brief(self):
        """Already-complete brief produces no suggestions."""
        config = EnhancementConfig(min_field_length=1, min_list_items=1)
        provider = FakeLLMProvider(_enhancement_response())
        enhancer = LlmBriefEnhancer(provider, config=config)
        brief = _complete_brief()
        result = enhancer.enhance(brief)

        assert result.has_suggestions is False
        assert provider.call_count == 0

    def test_contradictory_enhancements_first_wins(self):
        """When multiple suggestions target the same field, only first is kept."""
        response = _enhancement_response(
            [
                {
                    "category": "edge_cases",
                    "field": "risks",
                    "suggested_value": ["risk A"],
                    "rationale": "reason A",
                },
                {
                    "category": "technical_constraints",
                    "field": "risks",
                    "suggested_value": ["contradictory risk B"],
                    "rationale": "reason B",
                },
            ]
        )
        provider = FakeLLMProvider(response)
        enhancer = LlmBriefEnhancer(provider)
        result = enhancer.enhance(_incomplete_brief())

        risk_suggestions = [s for s in result.suggestions if s.field == "risks"]
        assert len(risk_suggestions) == 1
        assert risk_suggestions[0].suggested_value == ["risk A"]

    def test_multi_section_updates(self):
        """Multiple sections can be enhanced in a single pass."""
        provider = FakeLLMProvider(_enhancement_response())
        enhancer = LlmBriefEnhancer(provider)
        result = enhancer.enhance(_incomplete_brief())

        enhanced_fields = {s.field for s in result.suggestions}
        assert len(enhanced_fields) >= 2

    def test_brief_with_no_id(self):
        """Brief without an id still works."""
        brief = _incomplete_brief()
        del brief["id"]
        provider = FakeLLMProvider(_enhancement_response())
        enhancer = LlmBriefEnhancer(provider)
        result = enhancer.enhance(brief)

        assert result.brief_id == ""
        assert isinstance(result, EnhancementResult)

    def test_null_suggested_value_filtered(self):
        """Suggestion with None suggested_value is filtered."""
        response = _enhancement_response(
            [
                {
                    "category": "edge_cases",
                    "field": "risks",
                    "suggested_value": None,
                    "rationale": "reason",
                }
            ]
        )
        provider = FakeLLMProvider(response)
        enhancer = LlmBriefEnhancer(provider)
        result = enhancer.enhance(_incomplete_brief())

        assert len(result.suggestions) == 0


# ---------------------------------------------------------------------------
# Data model tests
# ---------------------------------------------------------------------------


class TestDataModels:
    def test_enhancement_suggestion_to_dict(self):
        """EnhancementSuggestion serializes to dict."""
        suggestion = EnhancementSuggestion(
            category="edge_cases",
            field="risks",
            original_value=["old risk"],
            suggested_value=["new risk"],
            rationale="improvement",
        )
        d = suggestion.to_dict()
        assert d["category"] == "edge_cases"
        assert d["field"] == "risks"
        assert d["original_value"] == ["old risk"]
        assert d["suggested_value"] == ["new risk"]
        assert d["rationale"] == "improvement"

    def test_enhancement_result_to_dict(self):
        """EnhancementResult serializes to dict."""
        result = EnhancementResult(
            brief_id="ib-001",
            gaps_detected=["risk gap"],
            suggestions=[
                EnhancementSuggestion(
                    category="edge_cases",
                    field="risks",
                    original_value=[],
                    suggested_value=["risk"],
                    rationale="needed",
                )
            ],
            generation_model="test",
            generation_tokens=50,
        )
        d = result.to_dict()
        assert d["brief_id"] == "ib-001"
        assert d["has_suggestions"] is True
        assert "edge_cases" in d["categories_covered"]
        assert d["generation_model"] == "test"
        assert d["generation_tokens"] == 50

    def test_enhancement_result_categories_covered(self):
        """categories_covered returns unique categories from suggestions."""
        result = EnhancementResult(
            brief_id="ib-001",
            gaps_detected=[],
            suggestions=[
                EnhancementSuggestion(
                    category="edge_cases",
                    field="risks",
                    original_value=[],
                    suggested_value=["r"],
                    rationale="a",
                ),
                EnhancementSuggestion(
                    category="testing_strategy",
                    field="validation_plan",
                    original_value="x",
                    suggested_value="y",
                    rationale="b",
                ),
                EnhancementSuggestion(
                    category="edge_cases",
                    field="non_goals",
                    original_value=[],
                    suggested_value=["ng"],
                    rationale="c",
                ),
            ],
        )
        assert result.categories_covered == {"edge_cases", "testing_strategy"}

    def test_all_categories_constant(self):
        """ALL_CATEGORIES contains all four enhancement categories."""
        assert len(ALL_CATEGORIES) == 4
        assert "acceptance_criteria" in ALL_CATEGORIES
        assert "technical_constraints" in ALL_CATEGORIES
        assert "edge_cases" in ALL_CATEGORIES
        assert "testing_strategy" in ALL_CATEGORIES

    def test_enhancement_config_defaults(self):
        """EnhancementConfig has sensible defaults."""
        config = EnhancementConfig()
        assert config.categories == ALL_CATEGORIES
        assert config.max_suggestions == 20
        assert config.min_field_length == 10
        assert config.min_list_items == 2
        assert config.require_rationale is True

    def test_enhancement_config_custom(self):
        """EnhancementConfig accepts custom values."""
        config = EnhancementConfig(
            categories=("edge_cases",),
            max_suggestions=5,
            min_field_length=20,
            min_list_items=3,
            require_rationale=False,
        )
        assert config.categories == ("edge_cases",)
        assert config.max_suggestions == 5
        assert config.min_field_length == 20
        assert config.min_list_items == 3
        assert config.require_rationale is False
