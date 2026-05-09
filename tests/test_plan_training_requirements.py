"""Tests for training requirements matrix generator."""

import pytest

from blueprint.plan_training_requirements import (
    TrainingRequirements,
    generate_training_requirements,
)


def test_empty_plan_data_returns_all_false():
    result = generate_training_requirements({})
    assert isinstance(result, TrainingRequirements)
    assert result.completeness_score == 0.0


def test_target_audiences_detected():
    plan = {"description": "Training for developers and end users"}
    result = generate_training_requirements(plan)
    assert result.target_audiences_identified is True


def test_training_content_detected():
    plan = {"requirements": ["Training material required", "Training modules defined"]}
    result = generate_training_requirements(plan)
    assert result.training_content_defined is True


def test_delivery_methods_detected():
    plan = {"description": "Online training with hands-on workshops"}
    result = generate_training_requirements(plan)
    assert result.delivery_methods_specified is True


def test_certification_detected():
    plan = {"description": "Training certification required for completion"}
    result = generate_training_requirements(plan)
    assert result.certification_required is True


def test_knowledge_validation_detected():
    plan = {"requirements": ["Training assessment required", "Validate knowledge with quiz"]}
    result = generate_training_requirements(plan)
    assert result.knowledge_validation_planned is True


def test_hands_on_practice_detected():
    plan = {"description": "Hands-on practice with lab exercises"}
    result = generate_training_requirements(plan)
    assert result.hands_on_practice_included is True


def test_follow_up_detected():
    plan = {"description": "Follow-up training session and refresher course"}
    result = generate_training_requirements(plan)
    assert result.follow_up_planned is True


def test_training_schedule_detected():
    plan = {"description": "Training schedule and delivery timeline defined"}
    result = generate_training_requirements(plan)
    assert result.training_schedule_defined is True


def test_success_metrics_detected():
    plan = {"requirements": ["Track training completion", "Measure training effectiveness"]}
    result = generate_training_requirements(plan)
    assert result.success_metrics_established is True


def test_comprehensive_training_all_detected():
    plan = {
        "title": "Complete training program",
        "description": (
            "Training for developers and admins with defined training material. "
            "Online training delivery with hands-on labs. "
            "Certification required with knowledge validation quiz. "
            "Identify training gaps with follow-up sessions. "
            "Training schedule defined and measure training success metrics."
        ),
    }
    result = generate_training_requirements(plan)
    assert result.completeness_score == 1.0


def test_invalid_plan_data_none():
    result = generate_training_requirements(None)  # type: ignore
    assert isinstance(result, TrainingRequirements)
    assert result.completeness_score == 0.0


def test_dataclass_immutability():
    reqs = TrainingRequirements(target_audiences_identified=True)
    with pytest.raises(AttributeError):
        reqs.target_audiences_identified = False  # type: ignore


def test_to_dict_method():
    reqs = TrainingRequirements(
        target_audiences_identified=True,
        training_content_defined=True,
        delivery_methods_specified=False,
        certification_required=True,
        knowledge_validation_planned=False,
        training_gaps_identified=True,
        hands_on_practice_included=False,
        follow_up_planned=True,
        training_schedule_defined=False,
        success_metrics_established=True,
    )
    result = reqs.to_dict()
    assert isinstance(result, dict)
    assert result["target_audiences_identified"] is True
    assert result["completeness_score"] == 0.6
