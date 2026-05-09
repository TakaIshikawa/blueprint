from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_api_authentication_strategy import (
    TaskApiAuthenticationStrategyFinding,
    TaskApiAuthenticationStrategyPlan,
    analyze_task_api_authentication_strategy,
    build_task_api_authentication_strategy_plan,
    extract_task_api_authentication_strategy,
    generate_task_api_authentication_strategy,
    recommend_task_api_authentication_strategy,
    summarize_task_api_authentication_strategy,
    task_api_authentication_strategy_plan_to_dict,
    task_api_authentication_strategy_plan_to_dicts,
)


def test_ready_api_authentication_task_has_no_recommended_checks():
    result = analyze_task_api_authentication_strategy(
        _plan(
            [
                _task(
                    "task-oauth-complete",
                    title="Implement OAuth 2.0 with comprehensive security",
                    description=(
                        "Build OAuth 2.0 flow with authorization code and PKCE for secure authentication. "
                        "Implement bearer token auth with JWT validation and signature verification. "
                        "Configure API key mechanism with X-API-Key header validation. "
                        "Set up mutual TLS handshake with client certificate validation and X.509 verification. "
                        "Validate token expiration on each request and check exp claim in JWT. "
                        "Define credential rotation policy with automated rotation schedule and key versioning. "
                        "Apply rate limiting per credential with token bucket algorithm and quota tracking. "
                        "Set up audit logging for authentication events and failed login attempts. "
                        "Build token revocation mechanism with blacklist and immediate invalidation."
                    ),
                    files_or_modules=[
                        "src/auth/oauth_flow.py",
                        "src/auth/bearer_token.py",
                        "src/auth/api_key.py",
                        "src/auth/mtls_validation.py",
                    ],
                )
            ]
        )
    )

    assert isinstance(result, TaskApiAuthenticationStrategyPlan)
    assert result.plan_id == "plan-auth"
    assert result.impacted_task_ids == ("task-oauth-complete",)
    finding = result.findings[0]
    assert isinstance(finding, TaskApiAuthenticationStrategyFinding)
    assert finding.detected_signals == (
        "api_key",
        "bearer_token",
        "oauth",
        "mutual_tls",
        "token_lifecycle",
        "credential_rotation",
    )
    assert finding.present_strategies == (
        "api_key_mechanism",
        "bearer_token_auth",
        "oauth_flow",
        "mtls_handshake",
        "token_expiry_validation",
        "credential_rotation_policy",
        "rate_limiting_per_credential",
        "audit_logging",
        "revocation_mechanism",
    )
    assert finding.missing_strategies == ()
    assert finding.recommended_checks == ()
    assert finding.readiness == "ready"
    assert "files_or_modules: src/auth/oauth_flow.py" in finding.evidence
    assert result.summary["impacted_task_count"] == 1
    assert result.summary["missing_strategy_count"] == 0
    assert result.summary["readiness_counts"] == {"weak": 0, "partial": 0, "ready": 1}


def test_partial_authentication_task_reports_specific_recommended_checks():
    result = build_task_api_authentication_strategy_plan(
        _plan(
            [
                _task(
                    "task-partial-oauth",
                    title="Add OAuth authentication",
                    description=(
                        "Implement OAuth 2.0 flow with authorization code for user authentication. "
                        "Use bearer token authentication with JWT for API access."
                    ),
                    files_or_modules=["src/auth/oauth.py"],
                )
            ]
        )
    )

    finding = result.findings[0]
    assert finding.task_id == "task-partial-oauth"
    assert finding.detected_signals == ("bearer_token", "oauth")
    assert "oauth_flow" in finding.present_strategies
    assert "bearer_token_auth" in finding.present_strategies
    assert "token_expiry_validation" in finding.missing_strategies
    assert "audit_logging" in finding.missing_strategies
    assert finding.readiness == "weak"
    assert len(finding.recommended_checks) > 5
    assert any("token expir" in check.lower() for check in finding.recommended_checks)
    assert result.summary["present_strategy_counts"]["oauth_flow"] == 1
    assert result.summary["present_strategy_counts"]["bearer_token_auth"] == 1


def test_path_hints_contribute_to_detection():
    result = build_task_api_authentication_strategy_plan(
        _plan(
            [
                _task(
                    "task-paths",
                    title="Set up API authentication",
                    description="Configure authentication pipeline with security and audit logging.",
                    files_or_modules=[
                        "src/auth/api_key.py",
                        "src/auth/bearer_token.py",
                        "src/auth/oauth_endpoint.py",
                        "src/auth/mtls_handshake.py",
                        "src/auth/token_expiry.py",
                        "src/auth/credential_rotation.py",
                    ],
                )
            ]
        )
    )

    finding = result.findings[0]
    assert {"api_key", "bearer_token", "oauth", "mutual_tls", "token_lifecycle", "credential_rotation"} <= set(finding.detected_signals)
    assert "files_or_modules: src/auth/api_key.py" in finding.evidence
    assert "files_or_modules: src/auth/bearer_token.py" in finding.evidence
    assert "files_or_modules: src/auth/oauth_endpoint.py" in finding.evidence


def test_unrelated_and_explicit_no_impact_tasks_are_not_applicable():
    result = build_task_api_authentication_strategy_plan(
        _plan(
            [
                _task(
                    "task-docs",
                    title="Update documentation",
                    description="Improve documentation for API endpoints.",
                    files_or_modules=["docs/api.md"],
                ),
                _task(
                    "task-no-auth",
                    title="Data processing",
                    description="This task has no authentication or security scope. Backend processing only.",
                    files_or_modules=["src/services/processor.py"],
                ),
            ]
        )
    )

    assert result.impacted_task_ids == ()
    assert result.not_applicable_task_ids == ("task-docs", "task-no-auth")
    assert result.findings == ()
    assert result.summary["impacted_task_count"] == 0


def test_api_key_task_detects_api_key_mechanism():
    result = analyze_task_api_authentication_strategy(
        _task(
            "task-api-key",
            title="Implement API key authentication",
            description=(
                "Create API key mechanism with X-API-Key header validation. "
                "Store API keys securely and implement rate limiting per API key. "
                "Set up audit logging for API key usage."
            ),
            files_or_modules=["src/auth/api_key_validator.py"],
        )
    )

    finding = result.findings[0]
    assert "api_key" in finding.detected_signals
    assert "api_key_mechanism" in finding.present_strategies
    assert "rate_limiting_per_credential" in finding.present_strategies
    assert "audit_logging" in finding.present_strategies


def test_jwt_bearer_token_task_detects_bearer_auth():
    result = build_task_api_authentication_strategy_plan(
        [
            _task(
                "task-jwt",
                title="JWT bearer token authentication",
                description=(
                    "Use bearer token authentication with JWT validation. "
                    "Verify JWT signatures and validate token expiration. "
                    "Implement token revocation with blacklist."
                ),
                files_or_modules=["src/auth/jwt_validator.py"],
            )
        ]
    )

    finding = result.findings[0]
    assert "bearer_token" in finding.detected_signals
    assert "token_lifecycle" in finding.detected_signals
    assert "bearer_token_auth" in finding.present_strategies
    assert "token_expiry_validation" in finding.present_strategies
    assert "revocation_mechanism" in finding.present_strategies


def test_oauth_with_pkce():
    result = analyze_task_api_authentication_strategy(
        _task(
            "task-oauth-pkce",
            title="OAuth 2.0 with PKCE",
            description=(
                "Implement OAuth 2.0 authorization code flow with PKCE for mobile clients. "
                "Configure token endpoint and handle refresh tokens. "
                "Set up token expiration validation and audit logging."
            ),
            files_or_modules=["src/auth/oauth_pkce.py"],
        )
    )

    finding = result.findings[0]
    assert "oauth" in finding.detected_signals
    assert "token_lifecycle" in finding.detected_signals
    assert "oauth_flow" in finding.present_strategies
    assert "token_expiry_validation" in finding.present_strategies
    assert "audit_logging" in finding.present_strategies


def test_mutual_tls_authentication():
    result = build_task_api_authentication_strategy_plan(
        [
            _task(
                "task-mtls",
                title="Mutual TLS authentication",
                description=(
                    "Set up mutual TLS with client certificate validation. "
                    "Verify X.509 certificates and validate certificate authority. "
                    "Implement mtls handshake with certificate revocation checking."
                ),
                files_or_modules=["src/auth/client_cert_validation.py"],
            )
        ]
    )

    finding = result.findings[0]
    assert "mutual_tls" in finding.detected_signals
    assert "mtls_handshake" in finding.present_strategies


def test_credential_rotation_policy():
    result = analyze_task_api_authentication_strategy(
        _task(
            "task-rotation",
            title="Implement credential rotation",
            description=(
                "Define credential rotation policy with automated rotation schedule. "
                "Rotate API keys and secrets periodically with zero-downtime rotation. "
                "Set up key versioning and revocation mechanism for old credentials."
            ),
            files_or_modules=["src/auth/credential_rotation.py"],
        )
    )

    finding = result.findings[0]
    assert "credential_rotation" in finding.detected_signals
    assert "credential_rotation_policy" in finding.present_strategies
    assert "revocation_mechanism" in finding.present_strategies


def test_weak_readiness_for_missing_critical_strategies():
    result = build_task_api_authentication_strategy_plan(
        [
            _task(
                "task-weak",
                title="Add OAuth without strategy",
                description="Implement OAuth 2.0 authentication for API.",
                files_or_modules=["src/auth/oauth.py"],
            )
        ]
    )

    finding = result.findings[0]
    assert finding.readiness == "weak"
    assert "token_expiry_validation" in finding.missing_strategies
    assert "audit_logging" in finding.missing_strategies
    assert "revocation_mechanism" in finding.missing_strategies
    assert len(finding.missing_strategies) >= 6


def test_acceptance_criteria_contributes_to_detection():
    result = analyze_task_api_authentication_strategy(
        _task(
            "task-ac",
            title="API authentication",
            description="Build authentication system.",
            acceptance_criteria=[
                "OAuth 2.0 flow with authorization code",
                "Bearer token auth with JWT validation",
                "Validate token expiration on each request",
                "Rate limiting per API key credential",
                "Audit logging for authentication events",
            ],
            files_or_modules=["src/auth/main.py"],
        )
    )

    finding = result.findings[0]
    assert "bearer_token" in finding.detected_signals
    assert "oauth" in finding.detected_signals
    assert "token_lifecycle" in finding.detected_signals


def test_metadata_authentication_hints():
    result = build_task_api_authentication_strategy_plan(
        [
            _task(
                "task-meta",
                title="Authentication task",
                description="Auth work.",
                metadata={
                    "authentication_method": "OAuth 2.0 with JWT",
                    "token_lifecycle": "15 minute expiry",
                    "credential_rotation": "automated monthly",
                    "audit_logging": "enabled",
                },
                files_or_modules=["src/auth.py"],
            )
        ]
    )

    finding = result.findings[0]
    assert "bearer_token" in finding.detected_signals
    assert "oauth" in finding.detected_signals
    assert "token_lifecycle" in finding.detected_signals
    assert "credential_rotation" in finding.detected_signals


def test_pydantic_execution_task_source():
    task = ExecutionTask(
        id="task-pydantic-oauth",
        title="OAuth with JWT",
        description=(
            "Implement OAuth 2.0 flow with bearer token authentication. "
            "Validate token expiration and set up audit logging."
        ),
        files_or_modules=["src/auth/oauth.py"],
        acceptance_criteria=[],
    )
    result = analyze_task_api_authentication_strategy(task)

    finding = result.findings[0]
    assert finding.task_id == "task-pydantic-oauth"
    assert "bearer_token" in finding.detected_signals
    assert "oauth" in finding.detected_signals
    assert "token_lifecycle" in finding.detected_signals


def test_pydantic_execution_plan_source():
    plan = ExecutionPlan(
        id="plan-pydantic-auth",
        implementation_brief_id="brief-1",
        milestones=[],
        tasks=[
            ExecutionTask(
                id="task-1",
                title="OAuth setup",
                description="OAuth 2.0 flow with bearer token auth.",
                acceptance_criteria=[],
            ),
            ExecutionTask(
                id="task-2",
                title="Frontend work",
                description="No authentication impact.",
                acceptance_criteria=[],
            ),
        ],
    )
    result = build_task_api_authentication_strategy_plan(plan)

    assert result.plan_id == "plan-pydantic-auth"
    assert result.impacted_task_ids == ("task-1",)
    assert result.not_applicable_task_ids == ("task-2",)
    assert len(result.findings) == 1


def test_list_of_tasks_source():
    tasks = [
        {"id": "task-a", "title": "API key auth", "description": "API key mechanism with rate limiting."},
        {"id": "task-b", "title": "JWT auth", "description": "Bearer token authentication with JWT."},
    ]
    result = analyze_task_api_authentication_strategy(tasks)

    assert len(result.findings) == 2
    assert result.impacted_task_ids == ("task-a", "task-b")


def test_simple_namespace_task():
    task = SimpleNamespace(
        id="task-ns-oauth",
        title="Namespace OAuth",
        description="OAuth 2.0 flow with token expiry validation and audit logging.",
        files_or_modules=["src/auth/oauth.py"],
    )
    result = build_task_api_authentication_strategy_plan(task)

    finding = result.findings[0]
    assert finding.task_id == "task-ns-oauth"
    assert "oauth" in finding.detected_signals
    assert "token_lifecycle" in finding.detected_signals


def test_to_dict_serialization():
    result = analyze_task_api_authentication_strategy(
        _task(
            "task-dict",
            title="OAuth task",
            description="OAuth 2.0 flow with bearer token authentication.",
        )
    )

    result_dict = result.to_dict()
    assert result_dict["plan_id"] is None
    assert result_dict["impacted_task_ids"] == ["task-dict"]
    assert isinstance(result_dict["findings"], list)
    assert result_dict["findings"][0]["task_id"] == "task-dict"

    finding_dict = result.findings[0].to_dict()
    assert finding_dict["task_id"] == "task-dict"
    assert isinstance(finding_dict["detected_signals"], list)
    assert isinstance(finding_dict["recommended_checks"], list)


def test_plan_to_dict_helper():
    result = build_task_api_authentication_strategy_plan(
        _task("task-helper", title="OAuth", description="OAuth 2.0 authentication.")
    )

    result_dict = task_api_authentication_strategy_plan_to_dict(result)
    assert result_dict["impacted_task_ids"] == ["task-helper"]

    findings_dicts = task_api_authentication_strategy_plan_to_dicts(result)
    assert len(findings_dicts) == 1
    assert findings_dicts[0]["task_id"] == "task-helper"


def test_plan_to_dicts_from_iterable():
    findings = [
        TaskApiAuthenticationStrategyFinding(
            task_id="task-iter-1",
            title="OAuth task",
            detected_signals=("oauth",),
            present_strategies=("oauth_flow",),
            missing_strategies=(),
            readiness="partial",
        ),
        TaskApiAuthenticationStrategyFinding(
            task_id="task-iter-2",
            title="API key task",
            detected_signals=("api_key",),
            present_strategies=("api_key_mechanism",),
            missing_strategies=(),
            readiness="partial",
        ),
    ]

    findings_dicts = task_api_authentication_strategy_plan_to_dicts(findings)
    assert len(findings_dicts) == 2
    assert findings_dicts[0]["task_id"] == "task-iter-1"
    assert findings_dicts[1]["task_id"] == "task-iter-2"


def test_compatibility_aliases():
    task = _task("task-alias", title="OAuth", description="OAuth 2.0 flow with bearer token.")

    result1 = build_task_api_authentication_strategy_plan(task)
    result2 = analyze_task_api_authentication_strategy(task)
    result3 = summarize_task_api_authentication_strategy(task)
    result4 = extract_task_api_authentication_strategy(task)
    result5 = generate_task_api_authentication_strategy(task)
    result6 = recommend_task_api_authentication_strategy(task)

    assert result1.findings[0].task_id == "task-alias"
    assert result2.findings[0].task_id == "task-alias"
    assert result3.findings[0].task_id == "task-alias"
    assert result4.findings[0].task_id == "task-alias"
    assert result5.findings[0].task_id == "task-alias"
    assert result6.findings[0].task_id == "task-alias"


def test_summary_aggregates_correctly():
    result = build_task_api_authentication_strategy_plan(
        _plan(
            [
                _task("task-1", title="OAuth full", description="OAuth with all strategies implemented."),
                _task("task-2", title="OAuth partial", description="OAuth only."),
                _task("task-3", title="Unrelated", description="Frontend work."),
            ]
        )
    )

    summary = result.summary
    assert summary["total_task_count"] == 3
    assert summary["impacted_task_count"] == 2
    assert len(summary["not_applicable_task_ids"]) == 1
    assert isinstance(summary["missing_strategy_count"], int)
    assert "readiness_counts" in summary
    assert "signal_counts" in summary
    assert "present_strategy_counts" in summary
    assert "missing_strategy_counts" in summary


def test_records_property_compatibility():
    result = analyze_task_api_authentication_strategy(
        _task("task-rec", title="OAuth", description="OAuth 2.0 authentication.")
    )

    assert result.records == result.findings
    assert len(result.records) == 1
    assert result.records[0].task_id == "task-rec"


def test_task_id_fallback_to_index():
    result = build_task_api_authentication_strategy_plan(
        [
            {"title": "OAuth without ID", "description": "OAuth 2.0 authentication."},
            {"title": "API key without ID", "description": "API key authentication."},
        ]
    )

    assert result.findings[0].task_id == "task-1"
    assert result.findings[1].task_id == "task-2"


def test_openid_connect_detection():
    result = analyze_task_api_authentication_strategy(
        _task(
            "task-oidc",
            title="OpenID Connect authentication",
            description=(
                "Implement OpenID Connect with OAuth 2.0 for identity verification. "
                "Use bearer tokens and validate token expiration."
            ),
        )
    )

    finding = result.findings[0]
    assert "oauth" in finding.detected_signals
    assert "bearer_token" in finding.detected_signals
    assert "token_lifecycle" in finding.detected_signals
    assert "oauth_flow" in finding.present_strategies


def test_client_credentials_flow():
    result = build_task_api_authentication_strategy_plan(
        [
            _task(
                "task-client-creds",
                title="Client credentials flow",
                description=(
                    "Use OAuth 2.0 client credentials flow for machine-to-machine authentication. "
                    "Implement token expiry validation and credential rotation policy."
                ),
            )
        ]
    )

    finding = result.findings[0]
    assert "oauth" in finding.detected_signals
    assert "token_lifecycle" in finding.detected_signals
    assert "credential_rotation" in finding.detected_signals
    assert "oauth_flow" in finding.present_strategies


def test_refresh_token_mechanism():
    result = analyze_task_api_authentication_strategy(
        _task(
            "task-refresh",
            title="Refresh token handling",
            description=(
                "Implement OAuth refresh token mechanism with token lifecycle management. "
                "Validate token expiration and handle token renewal. "
                "Set up revocation mechanism for expired refresh tokens."
            ),
            files_or_modules=["src/auth/refresh_token.py"],
        )
    )

    finding = result.findings[0]
    assert "oauth" in finding.detected_signals
    assert "token_lifecycle" in finding.detected_signals
    assert "token_expiry_validation" in finding.present_strategies
    assert "revocation_mechanism" in finding.present_strategies


def test_certificate_authority_configuration():
    result = build_task_api_authentication_strategy_plan(
        [
            _task(
                "task-ca",
                title="Certificate authority setup",
                description=(
                    "Configure certificate authority for mutual TLS. "
                    "Validate client certificates with X.509 verification. "
                    "Implement certificate revocation checking."
                ),
            )
        ]
    )

    finding = result.findings[0]
    assert "mutual_tls" in finding.detected_signals
    assert "mtls_handshake" in finding.present_strategies


def test_rate_limiting_with_api_key():
    result = analyze_task_api_authentication_strategy(
        _task(
            "task-rate-limit",
            title="Rate limiting per API key",
            description=(
                "Implement rate limiting per credential with token bucket algorithm. "
                "Apply throttling per API key and track quota usage. "
                "Set up audit logging for rate limit violations."
            ),
            files_or_modules=["src/auth/rate_limiter.py"],
        )
    )

    finding = result.findings[0]
    assert "api_key" in finding.detected_signals
    assert "rate_limiting_per_credential" in finding.present_strategies
    assert "audit_logging" in finding.present_strategies


def test_security_audit_logging():
    result = build_task_api_authentication_strategy_plan(
        [
            _task(
                "task-audit",
                title="Authentication audit logging",
                description=(
                    "Set up audit logging for authentication events and failed login attempts. "
                    "Log credential usage with timestamp and source IP. "
                    "Monitor suspicious activity for bearer token authentication."
                ),
            )
        ]
    )

    finding = result.findings[0]
    assert "bearer_token" in finding.detected_signals
    assert "audit_logging" in finding.present_strategies


def test_token_blacklist_revocation():
    result = analyze_task_api_authentication_strategy(
        _task(
            "task-blacklist",
            title="Token revocation with blacklist",
            description=(
                "Build revocation mechanism with token blacklist for immediate invalidation. "
                "Implement database-backed revocation list and revocation API endpoint. "
                "Handle bearer token revocation for JWT tokens."
            ),
            files_or_modules=["src/auth/token_blacklist.py"],
        )
    )

    finding = result.findings[0]
    assert "bearer_token" in finding.detected_signals
    assert "revocation_mechanism" in finding.present_strategies


def test_evidence_truncation_for_long_text():
    long_description = (
        "Implement OAuth 2.0 authentication with authorization code flow for secure user authentication. "
        "This is a very long description that should be truncated in the evidence snippet to ensure "
        "that evidence entries remain concise and readable in the output. "
        "Additional context about bearer token validation and audit logging continues here."
    )
    result = analyze_task_api_authentication_strategy(
        _task("task-long", title="OAuth", description=long_description)
    )

    finding = result.findings[0]
    evidence_entries = [e for e in finding.evidence if e.startswith("description:")]
    assert len(evidence_entries) > 0
    for evidence in evidence_entries:
        assert len(evidence) <= 200


def _plan(tasks):
    return {"id": "plan-auth", "tasks": tasks}


def _task(task_id, **kwargs):
    return {"id": task_id, **kwargs}
