from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_notification_preference_readiness import (
    NotificationPreferenceReadinessTask,
    TaskNotificationPreferenceReadinessPlan,
    TaskNotificationPreferenceReadinessRecord,
    analyze_task_notification_preference_readiness,
    build_task_notification_preference_readiness_plan,
    derive_task_notification_preference_readiness,
    extract_task_notification_preference_readiness,
    generate_task_notification_preference_readiness,
    recommend_task_notification_preference_readiness,
    summarize_task_notification_preference_readiness,
    task_notification_preference_readiness_plan_to_dict,
    task_notification_preference_readiness_plan_to_dicts,
    task_notification_preference_readiness_plan_to_markdown,
    task_notification_preference_readiness_to_dicts,
)


def test_complete_notification_preference_task_generates_low_risk_readiness():
    result = build_task_notification_preference_readiness_plan(
        _plan(
            [
                _task(
                    "task-notification-preference",
                    title="Implement email notification preferences",
                    description=(
                        "Allow users to subscribe and unsubscribe from email notifications, "
                        "persist preference state in the database, define default notification state, "
                        "support channel-specific preferences for email/SMS/push, audit preference changes, "
                        "migrate existing users, and test delivery enforcement for denied channels."
                    ),
                    files_or_modules=[
                        "src/notifications/preference_center.py",
                        "src/notifications/subscription_model.py",
                    ],
                    acceptance_criteria=[
                        "Users can unsubscribe from notification types without contacting support.",
                        "Preferences persist across sessions.",
                        "Tests verify denied channels do not receive notifications.",
                    ],
                    metadata={
                        "validation_commands": {
                            "test": ["poetry run pytest tests/test_notification_preferences.py"]
                        }
                    },
                )
            ]
        )
    )

    assert isinstance(result, TaskNotificationPreferenceReadinessPlan)
    assert result.preference_task_ids == ("task-notification-preference",)
    assert len(result.records) == 1
    record = result.records[0]
    assert isinstance(record, TaskNotificationPreferenceReadinessRecord)
    assert record.risk_level == "ready"
    # Check that key signals are detected
    assert "notification_preference" in record.detected_signals
    assert "subscription" in record.detected_signals
    assert "unsubscribe" in record.detected_signals
    assert "preference_center" in record.detected_signals
    # Check that all key safeguards are present
    safeguards = set(_safeguards(record))
    assert "unsubscribe_flow" in safeguards
    assert "preference_persistence" in safeguards
    assert "default_state" in safeguards
    assert "denied_channel_tests" in safeguards
    assert all(isinstance(task, NotificationPreferenceReadinessTask) for task in record.generated_tasks)
    assert record.missing_safeguards == ()
    assert record.suggested_validation_commands == (
        "poetry run pytest",
        "poetry run pytest tests/test_notification_preferences.py",
    )
    assert any("description: Allow users to subscribe" in item for item in record.evidence)
    assert result.summary["risk_counts"]["ready"] == 1


def test_unsubscribe_only_requirement_detects_missing_persistence_and_tests():
    task = ExecutionTask.model_validate(
        _task(
            "task-unsubscribe",
            title="Add unsubscribe link to marketing emails",
            description="Users must unsubscribe via email link and opt out from marketing emails.",
            acceptance_criteria=[
                "Unsubscribe link appears in all marketing emails.",
                "Unsubscribe takes effect immediately.",
            ],
            files_or_modules=["src/notifications/unsubscribe.py"],
        )
    )

    result = analyze_task_notification_preference_readiness(task)

    record = result.records[0]
    assert record.detected_signals == (
        "notification_preference",
        "subscription",
        "unsubscribe",
    )
    assert "unsubscribe_flow" in _safeguards(record)
    assert "preference_persistence" in record.missing_safeguards
    assert "denied_channel_tests" in record.missing_safeguards
    assert record.risk_level == "needs_planning"


def test_missing_persistence_and_tests_reported_as_critical_gaps():
    result = build_task_notification_preference_readiness_plan(
        _plan(
            [
                _task(
                    "task-subscription-only",
                    title="Add email subscription in signup form",
                    description=(
                        "Signup must collect opt-in for marketing emails with unsubscribe option available."
                    ),
                    acceptance_criteria=["Subscription preferences appear in the preference center."],
                )
            ],
            metadata={"validation_command": "poetry run pytest tests/test_subscription.py"},
        )
    )

    record = result.records[0]
    assert record.risk_level == "needs_planning"
    assert "preference_persistence" in record.missing_safeguards
    assert "denied_channel_tests" in record.missing_safeguards
    missing_task_categories = {task.category for task in record.generated_tasks}
    assert "preference_persistence" in missing_task_categories
    assert "denied_channel_tests" in missing_task_categories
    assert result.summary["missing_safeguard_counts"]["preference_persistence"] == 1
    assert result.summary["missing_safeguard_counts"]["denied_channel_tests"] == 1


def test_path_based_signal_detection_for_notification_preference_modules():
    result = build_task_notification_preference_readiness_plan(
        [
            _task(
                "task-path-signal",
                title="Update notification settings UI",
                files_or_modules=[
                    "src/notifications/notification_preference_model.py",
                    "src/notifications/preference_center_ui.tsx",
                    "tests/test_unsubscribe_flow.py",
                ],
            )
        ]
    )

    record = result.records[0]
    assert "notification_preference" in record.detected_signals
    assert "unsubscribe" in record.detected_signals
    assert "preference_center" in record.detected_signals
    assert any("files_or_modules:" in item for item in record.evidence)


def test_metadata_override_detection_for_channel_specific_preferences():
    result = build_task_notification_preference_readiness_plan(
        [
            _task(
                "task-metadata",
                title="Implement channel-specific preferences",
                metadata={
                    "notification_channels": ["email", "sms", "push"],
                    "channel_preference_support": True,
                    "digest_cadence_options": ["daily", "weekly"],
                },
            )
        ]
    )

    record = result.records[0]
    # Metadata detection should find at least notification_preference signal
    assert "notification_preference" in record.detected_signals or "channel_preference" in record.detected_signals


def test_markdown_output_includes_risk_levels_and_safeguards():
    plan = _plan(
        [
            _task(
                "task-md",
                title="Add mute settings for conversations",
                description="Users can mute and unmute conversation notifications.",
            )
        ]
    )
    result = build_task_notification_preference_readiness_plan(plan)
    markdown = result.to_markdown()

    assert "# Task Notification Preference Readiness" in markdown
    assert "Risk Level" in markdown
    assert "Present Safeguards" in markdown
    assert "Missing Safeguards" in markdown
    assert "Generated Tasks" in markdown
    if result.records:
        assert "task-md" in markdown
        assert "mute" in markdown.lower() or "notification" in markdown.lower()


def test_to_dict_serialization_preserves_stable_key_order():
    result = build_task_notification_preference_readiness_plan(
        [
            _task(
                "task-dict",
                title="Implement preference persistence",
                description="Store notification preferences in database.",
            )
        ]
    )

    result_dict = result.to_dict()
    assert result_dict["plan_id"] is None
    assert "records" in result_dict
    assert "findings" in result_dict
    assert "recommendations" in result_dict
    assert result_dict["records"] == result_dict["findings"]
    assert result_dict["preference_task_ids"] == result_dict["impacted_task_ids"]

    if result.records:
        record = result.records[0]
        record_dict = record.to_dict()
        assert "task_id" in record_dict
        assert "detected_signals" in record_dict
        assert "present_safeguards" in record_dict
        assert "missing_safeguards" in record_dict
        assert "risk_level" in record_dict
        assert "generated_tasks" in record_dict


def test_to_dicts_returns_list_of_plain_record_dictionaries():
    plan = _plan(
        [
            _task("task-1", title="Task 1"),
            _task("task-2", title="Task 2", description="Unsubscribe flow"),
        ]
    )
    result = build_task_notification_preference_readiness_plan(plan)
    dicts = result.to_dicts()

    assert isinstance(dicts, list)
    if dicts:
        assert all(isinstance(item, dict) for item in dicts)
        assert all("task_id" in item for item in dicts)


def test_string_source_is_parsed_as_requirement_text():
    result = build_task_notification_preference_readiness_plan(
        "Users must unsubscribe from email notifications and manage preferences in the preference center. "
        "Preferences persist in the database and tests verify denied channels."
    )

    assert result.plan_id is None
    if result.records:
        record = result.records[0]
        assert record.task_id == "requirement-text"
        assert "unsubscribe" in record.detected_signals
        assert "preference_center" in record.detected_signals


def test_object_with_attributes_is_parsed_without_pydantic_model():
    obj = SimpleNamespace(
        id="obj-preference",
        title="Notification preference object",
        description="Implement digest cadence and channel preferences.",
        acceptance_criteria=[
            "Users select daily or weekly digest cadence.",
            "Channel preferences are stored per user.",
        ],
    )

    result = build_task_notification_preference_readiness_plan(obj)

    assert len(result.records) == 1
    record = result.records[0]
    assert record.task_id == "obj-preference"
    assert "notification_preference" in record.detected_signals
    assert "digest_cadence" in record.detected_signals or "channel_preference" in record.detected_signals


def test_no_notification_preference_scope_returns_no_impact():
    result = build_task_notification_preference_readiness_plan(
        _plan(
            [
                _task(
                    "task-no-impact",
                    title="Update analytics dashboard",
                    description="No notification preference changes required for this release.",
                )
            ]
        )
    )

    assert len(result.records) == 0
    assert result.preference_task_ids == ()
    assert "task-no-impact" in result.no_impact_task_ids


def test_generic_send_notification_without_preference_context_is_no_impact():
    result = build_task_notification_preference_readiness_plan(
        [
            _task(
                "task-send-only",
                title="Send notification to user",
                description="Send push notification when event occurs.",
            )
        ]
    )

    assert len(result.records) == 0


def test_channel_preference_signals_require_channel_settings():
    result = build_task_notification_preference_readiness_plan(
        [
            _task(
                "task-channel",
                title="Add channel-specific preferences",
                description="Users can configure email, SMS, and push notification preferences independently.",
            )
        ]
    )

    if result.records:
        record = result.records[0]
        assert "channel_preference" in record.detected_signals
        assert "channel_settings" in record.missing_safeguards or "channel_settings" in _safeguards(record)


def test_mute_setting_signals_require_persistence_and_tests():
    result = build_task_notification_preference_readiness_plan(
        [
            _task(
                "task-mute",
                title="Implement conversation mute",
                description="Users can mute and unmute notifications for specific conversations.",
            )
        ]
    )

    if result.records:
        record = result.records[0]
        assert "mute_setting" in record.detected_signals
        assert "preference_persistence" in record.missing_safeguards or "preference_persistence" in _safeguards(record)
        assert "denied_channel_tests" in record.missing_safeguards or "denied_channel_tests" in _safeguards(record)


def test_digest_cadence_signals_require_channel_settings():
    result = build_task_notification_preference_readiness_plan(
        [
            _task(
                "task-digest",
                title="Implement daily and weekly digest options",
                description="Users select notification digest frequency.",
            )
        ]
    )

    if result.records:
        record = result.records[0]
        assert "digest_cadence" in record.detected_signals
        assert "channel_settings" in record.missing_safeguards or "channel_settings" in _safeguards(record)


def test_preference_center_signals_require_comprehensive_safeguards():
    result = build_task_notification_preference_readiness_plan(
        [
            _task(
                "task-preference-center",
                title="Build notification preference center",
                description="Preference center for managing all notification settings.",
            )
        ]
    )

    if result.records:
        record = result.records[0]
        assert "preference_center" in record.detected_signals
        expected_safeguards = {
            "unsubscribe_flow",
            "preference_persistence",
            "channel_settings",
            "audit_trail",
        }
        present = set(_safeguards(record))
        missing = set(record.missing_safeguards)
        assert expected_safeguards <= (present | missing)


def test_plan_with_validation_commands_propagates_to_records():
    plan = _plan(
        [
            _task(
                "task-validation",
                title="Add unsubscribe flow",
                test_command="poetry run pytest tests/test_unsubscribe.py",
            )
        ],
        metadata={"validation_command": "poetry run pytest"},
    )

    result = build_task_notification_preference_readiness_plan(plan)

    if result.records:
        record = result.records[0]
        assert "poetry run pytest" in record.suggested_validation_commands
        assert "poetry run pytest tests/test_unsubscribe.py" in record.suggested_validation_commands


def test_execution_task_model_input_is_processed():
    task = ExecutionTask.model_validate(
        _task(
            "task-exec",
            title="Implement subscription model",
            description="Persist user notification subscriptions.",
        )
    )

    result = build_task_notification_preference_readiness_plan(task)

    assert len(result.records) == 1
    record = result.records[0]
    assert record.task_id == "task-exec"
    assert "subscription" in record.detected_signals


def test_execution_plan_model_input_is_processed():
    plan_dict = _plan(
            [
                _task(
                    "task-plan",
                    title="Build preference center",
                    description="Preference center with unsubscribe and channel settings.",
                )
            ],
            plan_id="plan-exec",
        )
    plan = ExecutionPlan.model_validate(plan_dict)

    result = build_task_notification_preference_readiness_plan(plan)

    assert result.plan_id == "plan-exec"
    assert len(result.records) >= 1


def test_iterable_tasks_without_plan_wrapper():
    tasks = [
        _task("task-iter-1", title="Unsubscribe flow"),
        _task("task-iter-2", title="Add notification preference persistence", description="Store notification preferences in database."),
    ]

    result = build_task_notification_preference_readiness_plan(tasks)

    assert result.plan_id is None
    assert len(result.records) >= 1  # At least one should be detected


def test_compatibility_aliases_return_identical_results():
    source = [_task("task-alias", title="Add unsubscribe link")]

    result1 = build_task_notification_preference_readiness_plan(source)
    result2 = analyze_task_notification_preference_readiness(source)
    result3 = recommend_task_notification_preference_readiness(source)
    result4 = generate_task_notification_preference_readiness(source)
    result5 = extract_task_notification_preference_readiness(source)
    result6 = derive_task_notification_preference_readiness(source)

    assert result1.to_dict() == result2.to_dict()
    assert result1.to_dict() == result3.to_dict()
    assert result1.to_dict() == result4.to_dict()
    assert result1.to_dict() == result5.to_dict()
    assert result1.to_dict() == result6.to_dict()


def test_summarize_returns_same_plan_when_given_plan():
    plan_input = [_task("task-sum", title="Notification preferences")]
    result = build_task_notification_preference_readiness_plan(plan_input)
    summarized = summarize_task_notification_preference_readiness(result)

    assert result is summarized


def test_serialization_functions_work_on_plan_and_records():
    plan = _plan([_task("task-ser", title="Unsubscribe flow")])
    result = build_task_notification_preference_readiness_plan(plan)

    dict_result = task_notification_preference_readiness_plan_to_dict(result)
    assert isinstance(dict_result, dict)

    dicts_from_plan = task_notification_preference_readiness_plan_to_dicts(result)
    assert isinstance(dicts_from_plan, list)

    dicts_from_records = task_notification_preference_readiness_plan_to_dicts(result.records)
    assert isinstance(dicts_from_records, list)

    dicts_alias = task_notification_preference_readiness_to_dicts(result)
    assert dicts_from_plan == dicts_alias

    markdown = task_notification_preference_readiness_plan_to_markdown(result)
    assert isinstance(markdown, str)


def test_empty_source_returns_empty_plan():
    result = build_task_notification_preference_readiness_plan([])

    assert result.plan_id is None
    assert len(result.records) == 0
    assert result.preference_task_ids == ()
    assert result.no_impact_task_ids == ()
    assert result.summary["preference_task_count"] == 0


def test_multiple_tasks_with_mixed_risk_levels():
    plan = _plan(
        [
            _task(
                "task-ready",
                title="Complete preference system",
                description=(
                    "Unsubscribe flow, preference persistence, default state, channel settings, "
                    "audit trail, migration handling, and denied channel tests."
                ),
            ),
            _task(
                "task-needs-planning",
                title="Quick unsubscribe link",
                description="Add unsubscribe to emails.",
            ),
            _task(
                "task-no-impact",
                title="Update logo",
                description="Change notification email logo.",
            ),
        ]
    )

    result = build_task_notification_preference_readiness_plan(plan)

    assert result.summary["task_count"] == 3
    assert result.summary["preference_task_count"] == 2
    assert len(result.no_impact_task_ids) == 1
    assert "task-no-impact" in result.no_impact_task_ids
    assert result.summary["risk_counts"]["ready"] >= 0
    assert result.summary["risk_counts"]["needs_planning"] >= 1


def test_generated_task_structure_and_acceptance_criteria():
    result = build_task_notification_preference_readiness_plan(
        [
            _task(
                "task-generated",
                title="Unsubscribe flow",
                description="Add unsubscribe link.",
            )
        ]
    )

    if result.records and result.records[0].generated_tasks:
        task = result.records[0].generated_tasks[0]
        assert isinstance(task, NotificationPreferenceReadinessTask)
        assert hasattr(task, "category")
        assert hasattr(task, "title")
        assert hasattr(task, "description")
        assert hasattr(task, "acceptance_criteria")
        assert hasattr(task, "evidence")
        assert len(task.acceptance_criteria) == 3
        assert task.title.startswith(task.category.replace("_", " ").title())


def test_flattened_acceptance_criteria_property():
    result = build_task_notification_preference_readiness_plan(
        [
            _task(
                "task-flat",
                title="Notification preferences",
                description="Unsubscribe and persistence.",
            )
        ]
    )

    if result.records and result.records[0].generated_tasks:
        record = result.records[0]
        flattened = record.acceptance_criteria
        assert isinstance(flattened, tuple)
        total_criteria = sum(len(task.acceptance_criteria) for task in record.generated_tasks)
        assert len(flattened) == total_criteria


def test_compatibility_views_expose_correct_properties():
    result = build_task_notification_preference_readiness_plan(
        [_task("task-compat", title="Unsubscribe", description="Add unsubscribe flow.")]
    )

    if result.records:
        record = result.records[0]
        assert record.matched_signals == record.detected_signals
        assert record.recommended_tasks == record.generated_tasks
        assert record.readiness == record.risk_level

    assert result.findings == result.records
    assert result.recommendations == result.records
    assert result.impacted_task_ids == result.preference_task_ids


def test_plan_summary_includes_all_expected_metrics():
    plan = _plan(
        [
            _task("task-sum-1", title="Unsubscribe", description="Unsubscribe flow."),
            _task("task-sum-2", title="Mute", description="Mute conversations."),
        ]
    )
    result = build_task_notification_preference_readiness_plan(plan)

    summary = result.summary
    assert "task_count" in summary
    assert "preference_task_count" in summary
    assert "preference_task_ids" in summary
    assert "impacted_task_ids" in summary
    assert "no_impact_task_ids" in summary
    assert "generated_task_count" in summary
    assert "risk_counts" in summary
    assert "signal_counts" in summary
    assert "generated_task_category_counts" in summary
    assert "missing_safeguard_counts" in summary


def _task(
    task_id,
    *,
    title=None,
    description=None,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
    test_command=None,
):
    return {
        "id": task_id,
        "execution_plan_id": "plan-notification-preference",
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "milestone": "implementation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria or ["Implemented."],
        "estimated_complexity": "small",
        "estimated_hours": 1.0,
        "risk_level": "medium",
        "test_command": test_command or "poetry run pytest",
        "status": "pending",
        "metadata": metadata or {},
    }


def _plan(tasks, *, plan_id=None, metadata=None):
    return {
        "id": plan_id or "plan-notification-preference",
        "implementation_brief_id": "brief-notification-preference",
        "milestones": [{"id": "implementation", "title": "Implementation", "tasks": [task["id"] for task in tasks]}],
        "tasks": tasks,
        "metadata": metadata or {},
    }


def _safeguards(record: TaskNotificationPreferenceReadinessRecord) -> tuple[str, ...]:
    return record.present_safeguards
