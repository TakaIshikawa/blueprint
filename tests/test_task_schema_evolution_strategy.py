from blueprint.task_schema_evolution_strategy import (
    SchemaEvolutionStrategyAnalysis,
    analyze_schema_evolution_strategy,
)


def test_detects_field_additions():
    analysis = analyze_schema_evolution_strategy(
        _change_brief(
            description="Add column 'email' to users table",
            acceptance_criteria=["New field is nullable initially"],
        )
    )

    assert analysis.field_changes_identified is True


def test_detects_field_removals():
    analysis = analyze_schema_evolution_strategy(
        _change_brief(
            description="Remove column 'legacy_id' from accounts table",
        )
    )

    assert analysis.field_changes_identified is True


def test_detects_type_changes():
    analysis = analyze_schema_evolution_strategy(
        _change_brief(
            description="Change user_id type from integer to bigint",
            requirements=["Alter type to support larger values"],
        )
    )

    assert analysis.type_changes_identified is True


def test_detects_constraint_additions():
    analysis = analyze_schema_evolution_strategy(
        _change_brief(
            description="Add unique constraint on email field",
            acceptance_criteria=["Ensure no duplicate emails"],
        )
    )

    assert analysis.constraint_changes_identified is True


def test_detects_constraint_removals():
    analysis = analyze_schema_evolution_strategy(
        _change_brief(
            description="Remove not null constraint from phone_number",
        )
    )

    assert analysis.constraint_changes_identified is True


def test_detects_index_updates():
    analysis = analyze_schema_evolution_strategy(
        _change_brief(
            description="Add composite index on (tenant_id, created_at)",
            requirements=["Create index for query optimization"],
        )
    )

    assert analysis.index_updates_planned is True


def test_detects_backwards_compatibility_consideration():
    analysis = analyze_schema_evolution_strategy(
        _change_brief(
            description="Rename field while maintaining backwards compatibility",
            approach="Support both old and new field names during transition",
        )
    )

    assert analysis.backwards_compatibility_considered is True


def test_detects_migration_scripts():
    analysis = analyze_schema_evolution_strategy(
        _change_brief(
            description="Write migration script for schema changes",
            acceptance_criteria=["Create Alembic migration with up and down paths"],
        )
    )

    assert analysis.migration_scripts_planned is True


def test_detects_dual_write_strategy():
    analysis = analyze_schema_evolution_strategy(
        _change_brief(
            description="Implement dual-write to old and new columns during migration",
            approach="Write to both fields simultaneously during transition period",
        )
    )

    assert analysis.dual_write_strategy_defined is True


def test_detects_rollback_strategy():
    analysis = analyze_schema_evolution_strategy(
        _change_brief(
            description="Schema change with rollback plan",
            rollback_plan="Revert migration using down migration script",
        )
    )

    assert analysis.rollback_strategy_defined is True


def test_detects_zero_downtime_approach():
    analysis = analyze_schema_evolution_strategy(
        _change_brief(
            description="Zero-downtime schema migration",
            approach="Use online schema change with rolling deployment",
        )
    )

    assert analysis.zero_downtime_approach is True


def test_detects_testing_coverage():
    analysis = analyze_schema_evolution_strategy(
        _change_brief(
            description="Schema change with comprehensive testing",
            testing_strategy="Test migration forward and rollback scenarios",
        )
    )

    assert analysis.testing_coverage_planned is True


def test_detects_expand_contract_pattern():
    analysis = analyze_schema_evolution_strategy(
        _change_brief(
            description="Use expand-contract pattern for schema change",
            approach="Expand phase: add new column, Contract phase: remove old column",
        )
    )

    assert analysis.expand_contract_pattern is True


def test_comprehensive_schema_evolution_plan():
    analysis = analyze_schema_evolution_strategy(
        _change_brief(
            title="Migrate users table to support multi-tenancy",
            description=(
                "Add tenant_id column to users table using expand-contract pattern. "
                "Change user_id from integer to bigint. Add composite index on (tenant_id, user_id). "
                "Maintain backwards compatibility during migration."
            ),
            approach=(
                "Phase 1: Add nullable tenant_id column with migration script. "
                "Phase 2: Dual-write to both old and new schema. "
                "Phase 3: Backfill tenant_id for existing users. "
                "Phase 4: Add not null constraint after backfill. "
                "Use zero-downtime deployment with rolling update."
            ),
            acceptance_criteria=[
                "Migration scripts include up and down paths",
                "Test migration and rollback procedures",
                "No breaking changes to existing API",
            ],
            rollback_plan="Revert using down migration, preserve data integrity",
        )
    )

    assert analysis.field_changes_identified is True
    assert analysis.type_changes_identified is True
    assert analysis.index_updates_planned is True
    assert analysis.backwards_compatibility_considered is True
    assert analysis.migration_scripts_planned is True
    assert analysis.dual_write_strategy_defined is True
    assert analysis.rollback_strategy_defined is True
    assert analysis.zero_downtime_approach is True
    assert analysis.testing_coverage_planned is True
    assert analysis.expand_contract_pattern is True
    assert analysis.readiness_score > 0.9


def test_readiness_score_calculation():
    # High readiness: all aspects covered
    high_readiness = analyze_schema_evolution_strategy(
        _change_brief(
            description="Add column with migration script",
            approach="Backwards compatible, zero-downtime, dual-write during transition",
            rollback_plan="Down migration ready",
            testing_strategy="Test migration and rollback",
        )
    )
    assert high_readiness.readiness_score > 0.7

    # Low readiness: minimal planning
    low_readiness = analyze_schema_evolution_strategy(
        _change_brief(description="Add some field")
    )
    assert low_readiness.readiness_score < 0.3


def test_recommendations_for_incomplete_plan():
    analysis = analyze_schema_evolution_strategy(
        _change_brief(
            description="Add column to table",
        )
    )

    recs = analysis.recommendations
    assert len(recs) > 0
    assert any("migration script" in rec.lower() for rec in recs)
    assert any("backwards compatibility" in rec.lower() for rec in recs)
    assert any("rollback" in rec.lower() for rec in recs)
    assert any("test" in rec.lower() for rec in recs)


def test_recommendations_for_type_changes():
    analysis = analyze_schema_evolution_strategy(
        _change_brief(
            description="Change column type from varchar to integer",
        )
    )

    recs = analysis.recommendations
    assert any("dual-write" in rec.lower() for rec in recs)
    assert any("zero-downtime" in rec.lower() for rec in recs)


def test_recommendations_for_constraint_changes():
    analysis = analyze_schema_evolution_strategy(
        _change_brief(
            description="Add unique constraint on email field",
        )
    )

    recs = analysis.recommendations
    assert any("expand-contract" in rec.lower() for rec in recs)


def test_breaking_change_detection():
    analysis = analyze_schema_evolution_strategy(
        _change_brief(
            description="Schema change with breaking changes to API",
        )
    )

    assert analysis.backwards_compatibility_considered is True


def test_non_breaking_change_approach():
    analysis = analyze_schema_evolution_strategy(
        _change_brief(
            description="Non-breaking change preserving compatibility",
        )
    )

    assert analysis.backwards_compatibility_considered is True


def test_multi_step_migration():
    analysis = analyze_schema_evolution_strategy(
        _change_brief(
            description="Multi-phase migration with expand and contract phases",
        )
    )

    assert analysis.expand_contract_pattern is True


def test_large_table_modification():
    analysis = analyze_schema_evolution_strategy(
        _change_brief(
            description="Modify large users table with 100M rows",
            approach="Online schema change without downtime",
        )
    )

    assert analysis.zero_downtime_approach is True


def test_foreign_key_constraint():
    analysis = analyze_schema_evolution_strategy(
        _change_brief(
            description="Add foreign key constraint between tables",
        )
    )

    assert analysis.constraint_changes_identified is True


def test_primary_key_modification():
    analysis = analyze_schema_evolution_strategy(
        _change_brief(
            description="Change primary key from id to uuid",
        )
    )

    assert analysis.constraint_changes_identified is True


def test_alembic_migration():
    analysis = analyze_schema_evolution_strategy(
        _change_brief(
            description="Create Alembic migration for schema change",
        )
    )

    assert analysis.migration_scripts_planned is True


def test_flyway_migration():
    analysis = analyze_schema_evolution_strategy(
        _change_brief(
            description="Write Flyway migration script",
        )
    )

    assert analysis.migration_scripts_planned is True


def test_django_migration():
    analysis = analyze_schema_evolution_strategy(
        _change_brief(
            description="Generate Django migration",
        )
    )

    assert analysis.migration_scripts_planned is True


def test_blue_green_deployment():
    analysis = analyze_schema_evolution_strategy(
        _change_brief(
            description="Schema change using blue-green deployment",
        )
    )

    assert analysis.zero_downtime_approach is True


def test_rolling_deployment():
    analysis = analyze_schema_evolution_strategy(
        _change_brief(
            description="Deploy schema change with rolling update",
        )
    )

    assert analysis.zero_downtime_approach is True


def test_data_migration_testing():
    analysis = analyze_schema_evolution_strategy(
        _change_brief(
            description="Test data migration thoroughly",
            testing_strategy="Validate data integrity after migration",
        )
    )

    assert analysis.testing_coverage_planned is True


def test_empty_brief_returns_defaults():
    analysis = analyze_schema_evolution_strategy({})

    assert analysis.field_changes_identified is False
    assert analysis.type_changes_identified is False
    assert analysis.migration_scripts_planned is False
    assert analysis.readiness_score == 0.0


def test_non_dict_input_returns_defaults():
    analysis = analyze_schema_evolution_strategy("not a dict")

    assert analysis.field_changes_identified is False
    assert analysis.readiness_score == 0.0


def test_to_dict_serialization():
    analysis = analyze_schema_evolution_strategy(
        _change_brief(
            description="Add column with migration",
            approach="Backwards compatible approach",
        )
    )

    result = analysis.to_dict()
    assert isinstance(result, dict)
    assert "field_changes_identified" in result
    assert "backwards_compatibility_considered" in result
    assert isinstance(result["field_changes_identified"], bool)


def _change_brief(
    *,
    title="Schema evolution task",
    description="",
    summary="",
    body="",
    requirements=None,
    acceptance_criteria=None,
    approach="",
    implementation="",
    rollback_plan="",
    testing_strategy="",
    risks=None,
    constraints=None,
):
    brief = {
        "title": title,
        "description": description,
        "summary": summary,
        "body": body,
        "approach": approach,
        "implementation": implementation,
        "rollback_plan": rollback_plan,
        "testing_strategy": testing_strategy,
    }
    if requirements:
        brief["requirements"] = requirements
    if acceptance_criteria:
        brief["acceptance_criteria"] = acceptance_criteria
    if risks:
        brief["risks"] = risks
    if constraints:
        brief["constraints"] = constraints
    return brief
