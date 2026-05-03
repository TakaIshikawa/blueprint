import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_api_security_headers_requirements import (
    SourceApiSecurityHeadersRequirement,
    SourceApiSecurityHeadersRequirementsReport,
    build_source_api_security_headers_requirements,
    derive_source_api_security_headers_requirements,
    extract_source_api_security_headers_requirements,
    generate_source_api_security_headers_requirements,
    source_api_security_headers_requirements_to_dict,
    source_api_security_headers_requirements_to_dicts,
    source_api_security_headers_requirements_to_markdown,
    summarize_source_api_security_headers_requirements,
)


def test_comprehensive_api_security_headers_requirements_extracted():
    result = build_source_api_security_headers_requirements(
        _brief(
            "brief-security-headers",
            title="Add API security headers for production deployment",
            body=(
                "Implement comprehensive security headers for all API endpoints. "
                "Configure HSTS with max-age 31536000, includeSubDomains, and preload directives. "
                "Set Content-Security-Policy with default-src 'self', script-src 'nonce-{random}', and upgrade-insecure-requests. "
                "Add X-Frame-Options DENY to prevent clickjacking attacks. "
                "Set X-Content-Type-Options nosniff to prevent MIME type sniffing. "
                "Configure Referrer-Policy strict-origin-when-cross-origin for privacy. "
                "Define Permissions-Policy to restrict geolocation, camera, and microphone access. "
                "Set CORS headers with Access-Control-Allow-Origin for trusted domains. "
                "Enforce HttpOnly, Secure, and SameSite=Strict attributes on all cookies. "
                "Add comprehensive tests for all security headers, CSP violations, and cookie attributes."
            ),
        )
    )

    assert isinstance(result, SourceApiSecurityHeadersRequirementsReport)
    assert result.brief_id == "brief-security-headers"
    assert result.title == "Add API security headers for production deployment"
    assert len(result.requirements) == 9
    assert result.records == result.requirements
    assert result.findings == result.requirements
    categories = [req.category for req in result.requirements]
    assert categories == [
        "hsts",
        "csp",
        "frame_options",
        "content_type_options",
        "referrer_policy",
        "permissions_policy",
        "cors_headers",
        "secure_cookies",
        "test_coverage",
    ]
    hsts_req = next(req for req in result.requirements if req.category == "hsts")
    assert hsts_req.confidence in ("high", "medium")
    assert hsts_req.value and ("31536000" in hsts_req.value or "preload" in hsts_req.value or "max-age" in hsts_req.value)
    assert len(hsts_req.planning_notes) > 0
    csp_req = next(req for req in result.requirements if req.category == "csp")
    assert csp_req.value and ("default-src" in csp_req.value or "script-src" in csp_req.value or "csp" in csp_req.value)
    assert result.summary["requirement_count"] == 9
    assert result.summary["status"] == "ready_for_planning"
    assert result.summary["missing_detail_flags"] == []


def test_negated_scope_produces_no_requirements():
    result = build_source_api_security_headers_requirements(
        _brief(
            "brief-no-security-headers",
            title="Basic API endpoint",
            scope="This is a minimal API. No security headers or HSTS are required for this release.",
        )
    )

    assert result.requirements == ()
    assert result.records == ()
    assert result.summary["requirement_count"] == 0
    assert result.summary["status"] == "no_security_headers_language"


def test_partial_requirements_report_missing_detail_flags():
    result = build_source_api_security_headers_requirements(
        _brief(
            "brief-partial",
            title="Add CSP and secure cookies",
            body=(
                "Add Content-Security-Policy header for XSS protection. "
                "Make cookies secure to prevent session hijacking."
            ),
        )
    )

    categories = [req.category for req in result.requirements]
    assert "csp" in categories
    assert "secure_cookies" in categories
    assert result.summary["requirement_count"] > 0
    assert "missing_csp_directives" in result.summary["missing_detail_flags"]
    assert "missing_cookie_security" in result.summary["missing_detail_flags"]
    assert result.summary["status"] == "needs_security_headers_details"


def test_structured_metadata_fields_contribute_evidence():
    result = build_source_api_security_headers_requirements(
        _brief(
            "brief-metadata",
            title="API Security Hardening",
            metadata={
                "security": {
                    "hsts": "Strict-Transport-Security with preload",
                    "csp": "Content-Security-Policy with script-src 'nonce-{random}' and default-src 'self'",
                    "frame_protection": "X-Frame-Options DENY",
                },
                "headers": {
                    "content_type_options": "nosniff",
                    "referrer_policy": "strict-origin-when-cross-origin",
                },
                "cookies": "HttpOnly, Secure, SameSite=Strict",
                "cors": "Access-Control-Allow-Origin for trusted domains",
            },
        )
    )

    categories = [req.category for req in result.requirements]
    assert "hsts" in categories
    assert "csp" in categories
    assert "frame_options" in categories
    assert "content_type_options" in categories
    assert "referrer_policy" in categories
    assert "secure_cookies" in categories
    assert "cors_headers" in categories

    hsts_req = next(req for req in result.requirements if req.category == "hsts")
    assert hsts_req.value and "preload" in hsts_req.value

    csp_req = next(req for req in result.requirements if req.category == "csp")
    assert csp_req.value and ("script-src" in csp_req.value or "default-src" in csp_req.value or "csp" in csp_req.value)


def test_deduplication_and_stable_ordering():
    result = build_source_api_security_headers_requirements(
        _brief(
            "brief-dedup",
            title="API security headers",
            body="Use HSTS. HSTS should be configured with preload. Enable HSTS for all subdomains.",
            requirements=[
                "Support HSTS with max-age 31536000",
                "Configure HSTS preload",
                "Include subdomains in HSTS policy",
            ],
        )
    )

    hsts_reqs = [req for req in result.requirements if req.category == "hsts"]
    assert len(hsts_reqs) == 1
    assert len(hsts_reqs[0].evidence) <= 5


def test_implementation_brief_input():
    brief = {
        "id": "impl-brief-security-headers",
        "source_brief_id": "source-1",
        "title": "API security headers implementation",
        "problem_statement": "Add security headers to protect against common attacks",
        "mvp_goal": "Implement HSTS, CSP, X-Frame-Options, secure cookies, CORS headers, and comprehensive tests.",
        "scope": ["API security headers"],
        "non_goals": [],
        "assumptions": [],
        "risks": [],
        "validation_plan": "Test security headers",
        "definition_of_done": ["Security headers work"],
        "status": "draft",
    }

    result = build_source_api_security_headers_requirements(brief)

    assert result.brief_id == "impl-brief-security-headers"
    assert len(result.requirements) > 0
    categories = [req.category for req in result.requirements]
    assert "hsts" in categories
    assert "csp" in categories
    assert "frame_options" in categories


def test_model_validation_and_serialization():
    brief = _brief(
        "brief-serial",
        title="HSTS and CSP implementation",
        body=(
            "Implement HSTS with max-age 31536000 and preload. "
            "Add CSP with default-src 'self' and script-src 'nonce-{random}'. "
            "Configure secure cookies with HttpOnly, Secure, and SameSite=Strict. "
            "Add security header tests."
        ),
    )
    original = copy.deepcopy(brief)

    result = build_source_api_security_headers_requirements(brief)
    payload = source_api_security_headers_requirements_to_dict(result)
    dicts = source_api_security_headers_requirements_to_dicts(result)
    markdown = source_api_security_headers_requirements_to_markdown(result)

    assert brief == original
    assert derive_source_api_security_headers_requirements(brief).to_dict() == result.to_dict()
    assert generate_source_api_security_headers_requirements(brief).to_dict() == result.to_dict()
    assert extract_source_api_security_headers_requirements(brief) == result.requirements
    assert summarize_source_api_security_headers_requirements(brief) == result.summary
    assert summarize_source_api_security_headers_requirements(result) == result.summary
    assert json.loads(json.dumps(payload)) == payload
    assert dicts == payload["requirements"]
    assert source_api_security_headers_requirements_to_dicts(result.requirements) == dicts
    assert "# Source API Security Headers Requirements Report: brief-serial" in markdown
    assert "hsts" in markdown
    assert list(payload) == ["brief_id", "title", "summary", "requirements", "records", "findings"]
    assert list(payload["requirements"][0]) == [
        "category",
        "source_field",
        "evidence",
        "confidence",
        "value",
        "suggested_owners",
        "planning_notes",
        "gap_messages",
    ]


def test_string_input():
    result = build_source_api_security_headers_requirements(
        "Configure HSTS with max-age 31536000, includeSubDomains, and preload. "
        "Set CSP with default-src 'self', script-src 'nonce-{random}', and style-src 'self'. "
        "Add X-Frame-Options DENY and X-Content-Type-Options nosniff. "
        "Configure Referrer-Policy strict-origin-when-cross-origin. "
        "Set Permissions-Policy to restrict geolocation and camera. "
        "Configure CORS with Access-Control-Allow-Origin. "
        "Enforce HttpOnly, Secure, and SameSite=Strict on cookies. "
        "Add security header tests."
    )

    categories = [req.category for req in result.requirements]
    assert "hsts" in categories
    assert "csp" in categories
    assert "frame_options" in categories
    assert "content_type_options" in categories
    assert "referrer_policy" in categories
    assert "permissions_policy" in categories
    assert "cors_headers" in categories
    assert "secure_cookies" in categories
    assert "test_coverage" in categories


def test_object_with_attributes():
    obj = SimpleNamespace(
        id="obj-security-headers",
        title="API security headers",
        description="Add security headers to API responses",
        body=(
            "Configure HSTS with preload. "
            "Set CSP with script-src restrictions. "
            "Add X-Frame-Options DENY. "
            "Set secure cookies with HttpOnly and SameSite=Strict."
        ),
        security="HSTS, CSP, and secure cookies required",
        headers="Strict-Transport-Security, Content-Security-Policy, X-Frame-Options",
    )

    result = build_source_api_security_headers_requirements(obj)

    assert result.brief_id == "obj-security-headers"
    assert len(result.requirements) > 0
    categories = [req.category for req in result.requirements]
    assert "hsts" in categories
    assert "csp" in categories
    assert "frame_options" in categories
    assert "secure_cookies" in categories


def test_unrelated_headers_not_detected():
    result = build_source_api_security_headers_requirements(
        _brief(
            "brief-unrelated",
            title="CSV export feature",
            body=(
                "Add CSV export with proper column headers. "
                "Include email headers for notification system. "
                "Display table headers in the UI."
            ),
        )
    )

    assert result.requirements == ()
    assert result.summary["status"] == "no_security_headers_language"


def test_conflicting_policies_detected():
    result = build_source_api_security_headers_requirements(
        _brief(
            "brief-conflicting",
            title="API security with mixed requirements",
            body=(
                "Set CSP with default-src 'self' and script-src 'unsafe-inline' for legacy support. "
                "Configure HSTS with max-age 31536000. "
                "Set SameSite=None for third-party cookie sharing."
            ),
        )
    )

    categories = [req.category for req in result.requirements]
    assert "hsts" in categories
    assert "csp" in categories
    assert "secure_cookies" in categories


def test_browser_compatibility_considerations():
    result = build_source_api_security_headers_requirements(
        _brief(
            "brief-browser-compat",
            title="Security headers with browser compatibility",
            body=(
                "Implement HSTS with fallback for older browsers. "
                "Use CSP Level 2 directives for broad browser support. "
                "Configure SameSite cookie attribute with Lax for compatibility."
            ),
        )
    )

    categories = [req.category for req in result.requirements]
    assert "hsts" in categories
    assert "csp" in categories
    assert "secure_cookies" in categories

    cookie_req = next(req for req in result.requirements if req.category == "secure_cookies")
    assert cookie_req.value and "lax" in cookie_req.value


def test_csp_directives_extraction():
    result = build_source_api_security_headers_requirements(
        _brief(
            "brief-csp-directives",
            title="Comprehensive CSP configuration",
            body=(
                "Configure CSP with default-src 'self'. "
                "Set script-src 'nonce-{random}' 'strict-dynamic'. "
                "Add style-src 'self' 'unsafe-inline'. "
                "Include img-src https: data:. "
                "Set frame-ancestors 'none'. "
                "Enable upgrade-insecure-requests. "
                "Block all mixed content."
            ),
        )
    )

    csp_req = next((req for req in result.requirements if req.category == "csp"), None)
    assert csp_req is not None
    assert csp_req.value and (
        "default-src" in csp_req.value
        or "script-src" in csp_req.value
        or "frame-ancestors" in csp_req.value
        or "upgrade-insecure-requests" in csp_req.value
    )


def test_cors_security_headers():
    result = build_source_api_security_headers_requirements(
        _brief(
            "brief-cors",
            title="CORS security configuration",
            body=(
                "Configure CORS headers for cross-origin requests. "
                "Set Access-Control-Allow-Origin to trusted domains only. "
                "Configure Access-Control-Allow-Methods for GET, POST, PUT. "
                "Set Access-Control-Allow-Headers for custom headers. "
                "Add Access-Control-Max-Age for preflight caching. "
                "Do not allow credentials for public endpoints."
            ),
        )
    )

    cors_req = next((req for req in result.requirements if req.category == "cors_headers"), None)
    assert cors_req is not None
    assert cors_req.value and (
        "access-control-allow-origin" in cors_req.value
        or "access-control-allow-methods" in cors_req.value
        or "cors" in cors_req.value
    )


def test_permissions_policy_extraction():
    result = build_source_api_security_headers_requirements(
        _brief(
            "brief-permissions-policy",
            title="Feature policy configuration",
            body=(
                "Define Permissions-Policy to restrict browser features. "
                "Disable geolocation access. "
                "Block camera and microphone permissions. "
                "Restrict payment and USB access. "
                "Disable accelerometer and gyroscope."
            ),
        )
    )

    permissions_req = next((req for req in result.requirements if req.category == "permissions_policy"), None)
    assert permissions_req is not None
    assert permissions_req.value and (
        "geolocation" in permissions_req.value or "camera" in permissions_req.value or "microphone" in permissions_req.value
    )


def _brief(brief_id, *, title=None, body=None, scope=None, requirements=None, metadata=None):
    brief = {
        "id": brief_id,
        "title": title or brief_id,
        "body": body or "",
        "status": "draft",
    }
    if scope is not None:
        brief["scope"] = scope
    if requirements is not None:
        brief["requirements"] = requirements
    if metadata is not None:
        brief["metadata"] = metadata
    return brief
