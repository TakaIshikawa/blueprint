import json

from blueprint.task_ml_model_rollout_readiness import (
    build_task_ml_model_rollout_readiness_plan,
    task_ml_model_rollout_readiness_plan_to_dict,
    task_ml_model_rollout_readiness_plan_to_dicts,
    task_ml_model_rollout_readiness_plan_to_markdown,
)


def test_complete_ml_model_rollout_task_is_ready():
    result = build_task_ml_model_rollout_readiness_plan(
        _plan(
            [
                _task(
                    "ml-ready",
                    "Deploy fraud inference model",
                    (
                        "Model rollout for inference deployment and model serving. Model version v3 is in the "
                        "model registry. Evaluation gates require holdout AUC threshold. Canary and shadow plan "
                        "uses champion-challenger traffic split. Monitoring covers drift, metrics, latency, "
                        "and alerts. Rollback owner is the model owner."
                    ),
                    ["src/ml/model_registry/fraud_serving_deployment.py"],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.readiness == "ready"
    assert record.present_criteria == ("model_versioning", "evaluation_gates", "shadow_canary_plan", "monitoring", "rollback_owner")
    assert record.missing_criteria == ()


def test_detects_rollout_serving_champion_monitoring_and_rollback():
    result = build_task_ml_model_rollout_readiness_plan(
        _plan(
            [
                _task("serving", "Model serving", "Add inference service for recommendation model.", ["src/inference/serving.py"]),
                _task("challenger", "Champion challenger", "Run challenger model with model monitoring.", ["src/ml/champion_challenger.py"]),
                _task("policy", "AI policy", "Document AI policy and responsible AI review.", ["docs/ai_policy.md"]),
            ]
        )
    )

    by_id = {record.task_id: record for record in result.records}
    assert result.impacted_task_ids == ("serving", "challenger")
    assert result.ignored_task_ids == ("policy",)
    assert "model_serving" in by_id["serving"].detected_signals
    assert "champion_challenger" in by_id["challenger"].detected_signals
    assert "model_monitoring" in by_id["challenger"].detected_signals


def test_serialization_and_markdown_are_stable():
    result = build_task_ml_model_rollout_readiness_plan(_plan([_task("alias", "Deploy model", "Deploy model with model version.", ["ml/deploy.py"])]))
    payload = task_ml_model_rollout_readiness_plan_to_dict(result)

    assert task_ml_model_rollout_readiness_plan_to_dicts(result) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert "# Task ML Model Rollout Readiness: plan-ml-model-rollout" in task_ml_model_rollout_readiness_plan_to_markdown(result)


def _plan(tasks):
    return {"id": "plan-ml-model-rollout", "tasks": tasks}


def _task(task_id, title, description, files):
    return {"id": task_id, "title": title, "description": description, "files_or_modules": files}
