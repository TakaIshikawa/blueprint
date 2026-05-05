import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_consent_capture_readiness import (
    TaskConsentCaptureReadinessPlan,
    TaskConsentCaptureReadinessRecord,
    analyze_task_consent_capture_readiness,
    build_task_consent_capture_readiness_plan,
    derive_task_consent_capture_readiness_plan,
    summarize_task_consent_capture_readiness,
    task_consent_capture_readiness_plan_to_dict,
    task_consent_capture_readiness_plan_to_dicts,
    task_consent_capture_readiness_plan_to_markdown,
)


def test_detects_consent_tasks_and_classifies_impact_levels():
    result = build_task_consent_capture_readiness_plan(
        _plan(
            [
                _task(
                    "task-high",
                    title="Add user-facing consent capture for marketing sync",
                    description=(
                        "Capture explicit user consent in signup, store consent state, "
                        "and propagate consent changes to downstream CRM systems."
                    ),
                    files_or_modules=[
                        "src/ui/signup_consent_form.tsx",
                        "src/workers/consent_propagation.py",
                    ],
                    acceptance_criteria=[
                        "Explicit consent UI uses clear copy and no pre-checked default.",
                        "Consent timestamp stores consented_at and policy version.",
                    ],
                ),
                _task(
                    "task-medium",
                    title="Store consent preferences",
                    description="Persist consent record table and consent state for account settings.",
                    metadata={
                        "consent_safeguards": ["consent_timestamp_storage"],
                    },
                ),
                _task(
                    "task-low",
                    title="Backfill consent audit evidence",
                    description="Create consent audit history events for prior preference imports.",
                    acceptance_criteria=[
                        "Audit trail keeps compliance evidence for consent history."
                    ],
                ),
                _task(
                    "task-none",
                    title="Render profile settings",
                    description="Update copy for the settings page.",
                ),
            ]
        )
    )

    assert isinstance(result, TaskConsentCaptureReadinessPlan)
    assert result.plan_id == "plan-consent"
    assert result.impacted_task_ids == ("task-high", "task-medium", "task-low")
    assert result.no_impact_task_ids == ("task-none",)
    assert result.summary["impact_level_counts"] == {"high": 1, "medium": 1, "low": 1}
    assert result.summary["signal_counts"]["user_facing_capture"] == 1
    assert result.summary["signal_counts"]["consent_storage"] == 2
    assert result.summary["signal_counts"]["auditability"] == 1
    assert result.summary["signal_counts"]["downstream_propagation"] == 1

    high = _record(result, "task-high")
    assert high.impact_level == "high"
    assert {"user_facing_capture", "consent_storage", "downstream_propagation"} <= set(
        high.matched_signals
    )
    assert "explicit_consent_ui" not in high.missing_safeguards
    assert "consent_timestamp_storage" not in high.missing_safeguards
    assert "withdrawal_path" in high.missing_safeguards
    assert "downstream_propagation" not in high.missing_safeguards
    assert high.recommended_checks == (
        "Verify users can withdraw consent and that withdrawal behavior is covered before launch.",
        "Verify consent capture, changes, withdrawal, and propagation emit durable audit evidence.",
    )

    medium = _record(result, "task-medium")
    assert medium.impact_level == "medium"
    assert medium.present_safeguards == ("consent_timestamp_storage",)
    assert "consent_timestamp_storage" not in medium.missing_safeguards

    low = _record(result, "task-low")
    assert low.impact_level == "low"
    assert low.matched_signals == ("consent_change", "auditability")
    assert "audit_trail" not in low.missing_safeguards


def test_metadata_and_validation_commands_provide_evidence_and_safeguards():
    result = analyze_task_consent_capture_readiness(
        _task(
            "task-validation",
            title="Consent revocation propagation",
            description="Add opt-out path and consent sync for notification workers.",
            metadata={
                "consent_signals": ["withdrawal", "propagation"],
                "consent_safeguards": ["withdrawal_path"],
                "validation_commands": {
                    "test": [
                        "poetry run pytest tests/privacy/test_consent_audit_trail.py",
                        "poetry run pytest tests/privacy/test_downstream_consent_sync.py",
                    ]
                },
            },
        )
    )

    record = result.records[0]
    assert record.impact_level == "high"
    assert record.matched_signals == (
        "consent_change",
        "withdrawal_path",
        "auditability",
        "downstream_propagation",
    )
    assert record.present_safeguards == (
        "withdrawal_path",
        "audit_trail",
        "downstream_propagation",
    )
    assert record.missing_safeguards == (
        "explicit_consent_ui",
        "consent_timestamp_storage",
    )
    assert any("metadata.consent_signals: withdrawal_path" in item for item in record.evidence)
    assert any(
        "validation_commands:" in item and "test_consent_audit_trail.py" in item
        for item in record.evidence
    )
    assert any(
        "validation_commands:" in item and "test_downstream_consent_sync.py" in item
        for item in record.evidence
    )


def test_serialization_aliases_markdown_and_no_source_mutation():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Consent capture | checkout",
                description="Capture consent in checkout UI and store consent record.",
            ),
            _task(
                "task-a",
                title="Withdrawal path",
                description=(
                    "Build self-service withdrawal path, consent audit trail, "
                    "and downstream propagation for third-party marketing sync."
                ),
                acceptance_criteria=[
                    "Withdrawal path covers opt-out.",
                    "Audit trail records consent history.",
                    "Downstream propagation emits processing stop signal.",
                ],
            ),
            _task("task-none", title="Profile copy", description="Adjust help text."),
        ]
    )
    original = copy.deepcopy(plan)

    result = summarize_task_consent_capture_readiness(plan)
    payload = task_consent_capture_readiness_plan_to_dict(result)
    markdown = task_consent_capture_readiness_plan_to_markdown(result)

    assert plan == original
    assert json.loads(json.dumps(payload)) == payload
    assert result.to_dicts() == payload["records"]
    assert task_consent_capture_readiness_plan_to_dicts(result) == payload["records"]
    assert task_consent_capture_readiness_plan_to_dicts(result.records) == payload["records"]
    assert derive_task_consent_capture_readiness_plan(plan).to_dict() == result.to_dict()
    assert result.findings == result.records
    assert result.impacted_task_ids == ("task-a", "task-z")
    assert result.no_impact_task_ids == ("task-none",)
    assert list(payload) == [
        "plan_id",
        "records",
        "impacted_task_ids",
        "no_impact_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "matched_signals",
        "present_safeguards",
        "missing_safeguards",
        "impact_level",
        "evidence",
        "recommended_checks",
    ]
    assert markdown == result.to_markdown()
    assert markdown.startswith("# Task Consent Capture Readiness: plan-consent")
    assert "Consent capture \\| checkout" in markdown
    assert (
        "| Task | Title | Impact | Matched Signals | Present Safeguards | Missing Safeguards | Evidence |"
        in markdown
    )


def test_execution_models_object_like_empty_invalid_and_complete_safeguards():
    model_task = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Consent settings UI",
            description="Capture user consent in settings and persist consent record.",
            acceptance_criteria=[
                "Explicit consent UI uses affirmative action.",
                "Consent timestamp stores consented_at and policy version.",
                "Withdrawal path lets users revoke consent.",
                "Audit trail records consent history.",
                "Downstream propagation sends consent sync events.",
            ],
        )
    )
    plan_model = ExecutionPlan.model_validate(
        _plan([model_task.model_dump(mode="python")], plan_id="plan-model")
    )
    object_task = SimpleNamespace(
        id="task-object",
        title="Consent audit event",
        description="Emit consent audit log events.",
        acceptance_criteria=["Audit trail is queryable."],
        files_or_modules=[],
        metadata={},
        status="pending",
    )

    model_result = build_task_consent_capture_readiness_plan(plan_model)
    direct_result = build_task_consent_capture_readiness_plan(model_task)
    object_result = build_task_consent_capture_readiness_plan([object_task])
    empty = build_task_consent_capture_readiness_plan({"id": "plan-empty", "tasks": []})
    invalid = build_task_consent_capture_readiness_plan(13)
    no_impact = build_task_consent_capture_readiness_plan(
        _plan([_task("task-copy", title="Help copy", description="Update help text.")])
    )

    assert model_result.plan_id == "plan-model"
    assert model_result.records[0].missing_safeguards == ()
    assert model_result.records[0].impact_level == "high"
    assert direct_result.plan_id is None
    assert direct_result.records[0].task_id == "task-model"
    assert isinstance(object_result.records[0], TaskConsentCaptureReadinessRecord)
    assert object_result.records[0].task_id == "task-object"
    assert empty.records == ()
    assert empty.summary["task_count"] == 0
    assert invalid.records == ()
    assert no_impact.records == ()
    assert no_impact.no_impact_task_ids == ("task-copy",)
    assert "No consent capture readiness records were inferred." in no_impact.to_markdown()
    assert no_impact.summary == {
        "task_count": 1,
        "record_count": 0,
        "impacted_task_count": 0,
        "no_impact_task_count": 1,
        "missing_safeguard_count": 0,
        "impact_level_counts": {"high": 0, "medium": 0, "low": 0},
        "signal_counts": {
            "user_facing_capture": 0,
            "consent_change": 0,
            "consent_storage": 0,
            "withdrawal_path": 0,
            "auditability": 0,
            "downstream_propagation": 0,
        },
        "present_safeguard_counts": {
            "explicit_consent_ui": 0,
            "consent_timestamp_storage": 0,
            "withdrawal_path": 0,
            "audit_trail": 0,
            "downstream_propagation": 0,
        },
        "impacted_task_ids": [],
        "no_impact_task_ids": ["task-copy"],
    }


def _record(result, task_id):
    return next(record for record in result.records if record.task_id == task_id)


def _plan(tasks, plan_id="plan-consent"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-consent",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "service",
        "milestones": [],
        "test_strategy": "pytest",
        "status": "draft",
        "tasks": tasks,
    }


def test_malformed_inputs_with_missing_fields():
    result = build_task_consent_capture_readiness_plan(
        _plan(
            [
                {
                    "id": "task-minimal",
                    "title": "Task with minimal fields",
                    "description": "Minimal task.",
                },
            ]
        )
    )

    assert result.summary["task_count"] == 1


def test_boundary_conditions_empty_and_whitespace():
    result = build_task_consent_capture_readiness_plan(
        _plan(
            [
                _task(
                    "task-empty",
                    title="",
                    description="",
                    files_or_modules=[""],
                    acceptance_criteria=[""],
                ),
                _task(
                    "task-whitespace",
                    title="   ",
                    description="   \n\t  ",
                ),
            ]
        )
    )

    assert result.summary["task_count"] == 2


def test_complex_consent_flow_with_multiple_signals():
    result = build_task_consent_capture_readiness_plan(
        _plan(
            [
                _task(
                    "task-complex",
                    title="Full consent lifecycle implementation",
                    description=(
                        "Implement user-facing consent capture form with explicit opt-in, "
                        "store consent records with timestamps and policy versions, "
                        "enable withdrawal mechanisms with immediate propagation, "
                        "maintain comprehensive audit trail, support GDPR and CCPA compliance, "
                        "propagate consent changes to downstream marketing systems."
                    ),
                    files_or_modules=[
                        "src/ui/consent_form.tsx",
                        "src/models/consent_record.py",
                        "src/api/consent_withdrawal.py",
                        "src/audit/consent_events.py",
                        "src/workers/consent_propagation.py",
                    ],
                    acceptance_criteria=[
                        "Explicit consent UI with no pre-checked boxes.",
                        "Consent timestamp stores policy version.",
                        "Withdrawal path accessible and tested.",
                        "Audit trail captures all consent events.",
                        "GDPR and CCPA compliance verified.",
                        "Downstream propagation working.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]

    assert record.impact_level == "high"
    assert len(record.matched_signals) >= 4
    assert "user_facing_capture" in record.matched_signals
    assert "consent_storage" in record.matched_signals
    assert "withdrawal_path" in record.matched_signals
    assert "auditability" in record.matched_signals
    assert "downstream_propagation" in record.matched_signals
    assert len(record.present_safeguards) > 0


def test_gdpr_and_ccpa_compliance_requirements():
    result = build_task_consent_capture_readiness_plan(
        _plan(
            [
                _task(
                    "task-gdpr",
                    title="GDPR compliance for consent capture",
                    description=(
                        "Implement GDPR-compliant consent capture with explicit opt-in, "
                        "right to withdraw, and data portability support."
                    ),
                    acceptance_criteria=[
                        "GDPR Article 7 consent requirements met.",
                        "Right to withdraw consent implemented.",
                    ],
                ),
                _task(
                    "task-ccpa",
                    title="CCPA compliance for data collection",
                    description=(
                        "Add CCPA-compliant opt-out mechanisms and privacy notice "
                        "for California residents."
                    ),
                    acceptance_criteria=["CCPA opt-out link added to privacy policy."],
                ),
            ]
        )
    )

    assert result.summary["task_count"] == 2
    gdpr_record = _record(result, "task-gdpr")
    ccpa_record = _record(result, "task-ccpa")

    assert gdpr_record.impact_level in ("high", "medium")
    assert ccpa_record.impact_level in ("high", "medium", "low")


def test_withdrawal_mechanism_with_immediate_effect():
    result = build_task_consent_capture_readiness_plan(
        _plan(
            [
                _task(
                    "task-withdrawal",
                    title="Implement consent withdrawal with immediate effect",
                    description=(
                        "Add user-facing withdrawal UI, immediately revoke consent, "
                        "propagate withdrawal to all downstream systems."
                    ),
                    files_or_modules=[
                        "src/ui/consent_withdrawal.tsx",
                        "src/api/revoke_consent.py",
                        "src/workers/withdrawal_propagation.py",
                    ],
                    acceptance_criteria=[
                        "Withdrawal path is user-accessible.",
                        "Withdrawal takes immediate effect.",
                        "Downstream systems notified of withdrawal.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]

    assert record.impact_level in ("high", "medium")
    assert "withdrawal_path" in record.matched_signals
    assert "withdrawal_path" not in record.missing_safeguards


def test_audit_trail_for_consent_changes():
    result = build_task_consent_capture_readiness_plan(
        _plan(
            [
                _task(
                    "task-audit",
                    title="Comprehensive audit trail for consent events",
                    description=(
                        "Log all consent capture, changes, and withdrawals with "
                        "timestamps, user IDs, and policy versions for compliance."
                    ),
                    files_or_modules=["src/audit/consent_audit_log.py"],
                    acceptance_criteria=[
                        "Audit trail captures consent lifecycle events.",
                        "Audit logs immutable and tamper-evident.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]

    assert "auditability" in record.matched_signals
    assert "audit_trail" not in record.missing_safeguards


def test_cross_jurisdiction_consent_handling():
    result = build_task_consent_capture_readiness_plan(
        _plan(
            [
                _task(
                    "task-jurisdiction",
                    title="Multi-jurisdiction consent handling",
                    description=(
                        "Support consent requirements across GDPR (EU), CCPA (California), "
                        "LGPD (Brazil), and PIPEDA (Canada) with jurisdiction-specific flows."
                    ),
                    files_or_modules=["src/consent/jurisdiction_routing.py"],
                    acceptance_criteria=[
                        "Jurisdiction detected from user location.",
                        "Appropriate consent flow shown per jurisdiction.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]

    assert record.impact_level in ("high", "medium", "low")


def test_consent_version_tracking_and_migration():
    result = build_task_consent_capture_readiness_plan(
        _plan(
            [
                _task(
                    "task-versioning",
                    title="Consent policy versioning and migration",
                    description=(
                        "Track consent policy versions, require re-consent on policy updates, "
                        "migrate existing consent records to new policy version."
                    ),
                    files_or_modules=["src/models/consent_policy_version.py"],
                    acceptance_criteria=[
                        "Consent timestamp stores policy version.",
                        "Re-consent required after policy update.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]

    assert record.impact_level in ("high", "medium")
    assert "consent_storage" in record.matched_signals


def test_downstream_propagation_with_retry_logic():
    result = build_task_consent_capture_readiness_plan(
        _plan(
            [
                _task(
                    "task-propagation",
                    title="Reliable downstream consent propagation",
                    description=(
                        "Propagate consent changes to CRM, marketing platform, analytics, "
                        "with retry logic and dead-letter queue for failed propagations."
                    ),
                    files_or_modules=["src/workers/consent_propagation_worker.py"],
                    acceptance_criteria=["Downstream propagation tested with failures."],
                )
            ]
        )
    )

    record = result.records[0]

    assert "downstream_propagation" in record.matched_signals


def test_pre_checked_consent_detection():
    result = build_task_consent_capture_readiness_plan(
        _plan(
            [
                _task(
                    "task-prechecked",
                    title="Remove pre-checked consent boxes",
                    description=(
                        "Update signup form to ensure consent checkboxes are not pre-checked "
                        "per GDPR explicit consent requirements."
                    ),
                    acceptance_criteria=["No pre-checked consent boxes in UI."],
                )
            ]
        )
    )

    record = result.records[0]

    assert record.impact_level in ("high", "medium", "low")


def test_metadata_safeguards_override():
    result = build_task_consent_capture_readiness_plan(
        _plan(
            [
                _task(
                    "task-meta",
                    title="Task with metadata safeguards",
                    description="Consent capture with metadata-specified safeguards.",
                    metadata={
                        "consent_safeguards": [
                            "explicit_consent_ui",
                            "withdrawal_path",
                            "audit_trail",
                        ],
                    },
                )
            ]
        )
    )

    record = result.records[0]

    assert len(record.present_safeguards) >= 3
    assert "explicit_consent_ui" in record.present_safeguards
    assert "withdrawal_path" in record.present_safeguards
    assert "audit_trail" in record.present_safeguards


def test_sorting_by_impact_level_then_task_id():
    result = build_task_consent_capture_readiness_plan(
        _plan(
            [
                _task(
                    "task-z-high",
                    title="High impact Z",
                    description="User-facing consent capture.",
                ),
                _task(
                    "task-a-high",
                    title="High impact A",
                    description="Explicit consent UI implementation.",
                ),
                _task(
                    "task-m-medium",
                    title="Medium impact M",
                    description="Store consent preferences.",
                ),
                _task(
                    "task-b-low",
                    title="Low impact B",
                    description="Audit trail logging.",
                ),
            ]
        )
    )

    task_ids = [r.task_id for r in result.records]
    impact_levels = [r.impact_level for r in result.records]

    high_tasks = [tid for tid, level in zip(task_ids, impact_levels) if level == "high"]
    assert high_tasks == sorted(high_tasks)


def test_special_characters_in_fields():
    result = build_task_consent_capture_readiness_plan(
        _plan(
            [
                _task(
                    "task-special",
                    title="Consent with <special> & \"quoted\" characters",
                    description="Unicode: \u00e9\u00f1\u00fc and symbols: #$%^&*",
                )
            ]
        )
    )

    markdown = task_consent_capture_readiness_plan_to_markdown(result)
    assert "task-special" in markdown


def test_very_long_descriptions_and_file_lists():
    long_description = " ".join(
        ["consent capture withdrawal audit GDPR CCPA explicit opt-in timestamp"] * 20
    )
    many_files = [f"src/consent/module_{i}.py" for i in range(50)]

    result = build_task_consent_capture_readiness_plan(
        _plan(
            [
                _task(
                    "task-long",
                    title="Long task",
                    description=long_description,
                    files_or_modules=many_files,
                )
            ]
        )
    )

    assert len(result.records) == 1


def test_serialization_round_trip():
    result = build_task_consent_capture_readiness_plan(
        _plan(
            [
                _task(
                    "task-serialize",
                    title="Consent capture",
                    description="User-facing consent capture.",
                )
            ]
        )
    )

    payload = task_consent_capture_readiness_plan_to_dict(result)
    json_payload = json.loads(json.dumps(payload))

    assert payload == json_payload
    assert payload == result.to_dict()
    assert task_consent_capture_readiness_plan_to_dicts(result) == payload["records"]


def test_edge_case_file_paths():
    result = build_task_consent_capture_readiness_plan(
        _plan(
            [
                _task(
                    "task-paths",
                    title="Consent with various paths",
                    description="Test path detection.",
                    files_or_modules=[
                        "/absolute/consent/form.tsx",
                        "./relative/consent.py",
                        "../parent/withdrawal.py",
                        "consent_model.py",
                    ],
                )
            ]
        )
    )

    assert len(result.records) == 1


def test_acceptance_criteria_with_consent_keywords():
    result = build_task_consent_capture_readiness_plan(
        _plan(
            [
                _task(
                    "task-ac",
                    title="Feature with consent ACs",
                    description="Standard feature.",
                    acceptance_criteria=[
                        "Explicit consent obtained from users.",
                        "Withdrawal mechanism tested.",
                        "Audit trail verified.",
                        "GDPR compliance checked.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]

    assert len(record.matched_signals) > 0
    assert len(record.evidence) > 0


def _task(
    task_id,
    *,
    title,
    description,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
    test_command=None,
):
    return {
        "id": task_id,
        "execution_plan_id": "plan-consent",
        "title": title,
        "description": description,
        "milestone": "implementation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "acceptance_criteria": acceptance_criteria or ["Implemented."],
        "estimated_complexity": "medium",
        "estimated_hours": 2.0,
        "risk_level": "medium",
        "test_command": test_command,
        "status": "pending",
        "metadata": metadata or {},
    }
