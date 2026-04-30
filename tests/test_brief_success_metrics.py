import json

from blueprint.brief_success_metrics import (
    BriefSuccessMetricCandidate,
    brief_success_metrics_to_dicts,
    extract_brief_success_metrics,
)
from blueprint.domain.models import ImplementationBrief


def test_extracts_measurable_acceptance_text_from_brief_fields():
    metrics = extract_brief_success_metrics(
        _brief(
            definition_of_done=[
                "API p95 latency stays under 200ms for dashboard requests.",
                "At least 95% of imports complete successfully.",
            ],
            validation_plan="Run a 500 user load test before 2026-06-30.",
            risks=["Error rate must remain below 1% during rollout."],
        )
    )

    assert metrics[:4] == (
        BriefSuccessMetricCandidate(
            source_field="definition_of_done[0]",
            normalized_label="api p95 latency stays under 200ms dashboard request",
            metric_type="performance",
            measurement_hint="Threshold: under 200ms",
            confidence="high",
            vague_or_unverifiable=False,
        ),
        BriefSuccessMetricCandidate(
            source_field="definition_of_done[1]",
            normalized_label="at least 95 of imports complete successfully",
            metric_type="threshold",
            measurement_hint="Threshold: At least 95%",
            confidence="high",
            vague_or_unverifiable=False,
        ),
        BriefSuccessMetricCandidate(
            source_field="validation_plan",
            normalized_label="run 500 user load test before 2026 06 30",
            metric_type="date",
            measurement_hint="Target date: 2026-06-30",
            confidence="high",
            vague_or_unverifiable=False,
        ),
        BriefSuccessMetricCandidate(
            source_field="risks[0]",
            normalized_label="error rate must remain under 1 during rollout",
            metric_type="reliability",
            measurement_hint="Threshold: below 1%",
            confidence="high",
            vague_or_unverifiable=False,
        ),
    )


def test_vague_goals_are_preserved_as_low_confidence_unverifiable_candidates():
    metrics = extract_brief_success_metrics(
        _brief(
            mvp_goal="Make onboarding faster and more intuitive.",
            definition_of_done=["The experience is seamless for operators."],
        )
    )

    assert [(metric.metric_type, metric.confidence, metric.vague_or_unverifiable) for metric in metrics] == [
        ("vague", "low", True),
        ("vague", "low", True),
    ]
    assert metrics[0].measurement_hint == "Needs a concrete observable threshold."


def test_duplicate_and_near_identical_metrics_are_normalized_once():
    metrics = extract_brief_success_metrics(
        _brief(
            definition_of_done=[
                "API latency under 200ms.",
                "api latency below 200 milliseconds",
                "At least 25 records are imported.",
            ],
            validation_plan="At least 25 records are imported.",
        )
    )

    assert [(metric.source_field, metric.normalized_label) for metric in metrics] == [
        ("definition_of_done[0]", "api latency under 200ms"),
        ("definition_of_done[2]", "at least 25 records imported"),
    ]


def test_metadata_success_metrics_are_flattened_and_serialized_cleanly():
    metrics = extract_brief_success_metrics(
        _brief(
            metadata={
                "success_metrics": [
                    {
                        "label": "Activation completion",
                        "metric": "wizard completion",
                        "threshold": ">= 80%",
                    },
                    "Availability at least 99.9%",
                ]
            }
        )
    )
    payload = brief_success_metrics_to_dicts(metrics)

    assert payload == [metric.to_dict() for metric in metrics]
    assert payload == [
        {
            "source_field": "metadata.success_metrics[0]",
            "normalized_label": "activation completion wizard completion 80",
            "metric_type": "threshold",
            "measurement_hint": "Threshold: >= 80%",
            "confidence": "high",
            "vague_or_unverifiable": False,
        },
        {
            "source_field": "metadata.success_metrics[1]",
            "normalized_label": "availability at least 99 9",
            "metric_type": "reliability",
            "measurement_hint": "Threshold: at least 99.9%",
            "confidence": "high",
            "vague_or_unverifiable": False,
        },
    ]
    assert list(payload[0]) == [
        "source_field",
        "normalized_label",
        "metric_type",
        "measurement_hint",
        "confidence",
        "vague_or_unverifiable",
    ]
    assert json.loads(json.dumps(payload)) == payload


def test_accepts_implementation_brief_model_input():
    model = ImplementationBrief.model_validate(
        _brief(
            definition_of_done=["At most 3 manual review steps remain."],
            scope=["Support 10 pilot users."],
        )
    )

    metrics = extract_brief_success_metrics(model)

    assert brief_success_metrics_to_dicts(metrics) == [
        {
            "source_field": "definition_of_done[0]",
            "normalized_label": "at most 3 manual review steps remain",
            "metric_type": "threshold",
            "measurement_hint": "Threshold: At most 3",
            "confidence": "high",
            "vague_or_unverifiable": False,
        },
        {
            "source_field": "scope[0]",
            "normalized_label": "support 10 pilot user",
            "metric_type": "count",
            "measurement_hint": "Track value: 10 pilot users",
            "confidence": "medium",
            "vague_or_unverifiable": False,
        },
    ]


def _brief(
    *,
    definition_of_done=None,
    validation_plan="Run focused validation.",
    scope=None,
    risks=None,
    architecture_notes=None,
    mvp_goal="Extract success metrics for planning.",
    metadata=None,
):
    brief = {
        "id": "brief-success-metrics",
        "source_brief_id": "source-success-metrics",
        "title": "Success Metrics Brief",
        "problem_statement": "Execution plans need measurable success signals.",
        "mvp_goal": mvp_goal,
        "scope": ["Build the extractor"] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": architecture_notes,
        "data_requirements": None,
        "integration_points": [],
        "risks": [] if risks is None else risks,
        "validation_plan": validation_plan,
        "definition_of_done": (
            ["Success metrics are visible to execution agents"]
            if definition_of_done is None
            else definition_of_done
        ),
    }
    if metadata is not None:
        brief["metadata"] = metadata
    return brief
