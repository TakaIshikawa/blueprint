"""Tests for SSO and identity provider integration."""

from blueprint.security.sso_integration import (
    DEFAULT_GROUP_ROLE_MAP,
    AuthRequest,
    AuthResult,
    IdentityProvider,
    IdentityProviderType,
    MFAMethod,
    SCIMOperation,
    SSOManager,
    SSOProtocol,
    SessionState,
    User,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _okta_config() -> dict:
    return {
        "provider_id": "idp-okta-001",
        "provider_type": "okta",
        "protocol": "saml",
        "display_name": "Okta Production",
        "entity_id": "https://okta.example.com",
        "sso_url": "https://okta.example.com/sso/saml",
        "certificate": "MIIC...base64cert...",
        "group_mappings": {"platform-admins": "admin", "dev-team": "editor"},
    }


def _auth0_config() -> dict:
    return {
        "provider_id": "idp-auth0-001",
        "provider_type": "auth0",
        "protocol": "oidc",
        "display_name": "Auth0 Tenant",
        "client_id": "auth0-client-id",
        "client_secret": "auth0-secret",
        "sso_url": "https://tenant.auth0.com/authorize",
        "scopes": ["openid", "profile", "email", "groups"],
    }


def _azure_ad_config() -> dict:
    return {
        "provider_id": "idp-azuread-001",
        "provider_type": "azure_ad",
        "protocol": "oidc",
        "display_name": "Azure AD",
        "client_id": "azure-client-id",
        "client_secret": "azure-secret",
        "sso_url": "https://login.microsoftonline.com/tenant/authorize",
    }


def _google_config() -> dict:
    return {
        "provider_id": "idp-google-001",
        "provider_type": "google_workspace",
        "protocol": "oidc",
        "display_name": "Google Workspace",
        "client_id": "google-client-id",
    }


def _saml_assertion(email: str = "alice@example.com", name: str = "Alice") -> dict:
    return {
        "issuer": "https://okta.example.com",
        "subject": email,
        "conditions": {"not_before": "2026-01-01", "not_after": "2026-12-31"},
        "attributes": {
            "email": email,
            "name": name,
            "groups": ["admins", "developers"],
            "roles": ["admin"],
        },
    }


def _oidc_token_response(
    email: str = "bob@example.com",
    name: str = "Bob",
    groups: list | None = None,
) -> dict:
    return {
        "access_token": "at-abc123",
        "refresh_token": "rt-xyz789",
        "user_info": {
            "email": email,
            "name": name,
            "groups": groups or ["engineers"],
        },
    }


def _make_manager(**kwargs) -> SSOManager:
    return SSOManager(**kwargs)


# ---------------------------------------------------------------------------
# Provider configuration tests
# ---------------------------------------------------------------------------


class TestProviderConfiguration:

    def test_configure_okta(self):
        mgr = _make_manager()
        idp = mgr.configure_idp(_okta_config())

        assert isinstance(idp, IdentityProvider)
        assert idp.provider_id == "idp-okta-001"
        assert idp.provider_type == IdentityProviderType.OKTA
        assert idp.protocol == SSOProtocol.SAML
        assert idp.display_name == "Okta Production"
        assert idp.enabled
        assert idp.created_at

    def test_configure_auth0(self):
        mgr = _make_manager()
        idp = mgr.configure_idp(_auth0_config())
        assert idp.provider_type == IdentityProviderType.AUTH0
        assert idp.protocol == SSOProtocol.OIDC
        assert "groups" in idp.scopes

    def test_configure_azure_ad(self):
        mgr = _make_manager()
        idp = mgr.configure_idp(_azure_ad_config())
        assert idp.provider_type == IdentityProviderType.AZURE_AD
        assert idp.client_id == "azure-client-id"

    def test_configure_google_workspace(self):
        mgr = _make_manager()
        idp = mgr.configure_idp(_google_config())
        assert idp.provider_type == IdentityProviderType.GOOGLE_WORKSPACE
        assert idp.display_name == "Google Workspace"

    def test_get_provider(self):
        mgr = _make_manager()
        mgr.configure_idp(_okta_config())
        idp = mgr.get_provider("idp-okta-001")
        assert idp.provider_id == "idp-okta-001"

    def test_get_unknown_provider_raises(self):
        mgr = _make_manager()
        try:
            mgr.get_provider("nonexistent")
            assert False, "Should have raised"
        except KeyError:
            pass

    def test_provider_to_dict(self):
        mgr = _make_manager()
        idp = mgr.configure_idp(_okta_config())
        data = idp.to_dict()
        assert data["provider_type"] == "okta"
        assert data["protocol"] == "saml"
        assert "group_mappings" in data

    def test_list_providers(self):
        mgr = _make_manager()
        mgr.configure_idp(_okta_config())
        mgr.configure_idp(_auth0_config())
        assert len(mgr.providers) == 2


# ---------------------------------------------------------------------------
# SAML flow tests
# ---------------------------------------------------------------------------


class TestSAMLFlow:

    def test_initiate_saml_flow(self):
        mgr = _make_manager()
        mgr.configure_idp(_okta_config())
        auth_req = mgr.initiate_sso_flow("idp-okta-001")

        assert isinstance(auth_req, AuthRequest)
        assert auth_req.provider_id == "idp-okta-001"
        assert auth_req.protocol == SSOProtocol.SAML
        assert "okta" in auth_req.redirect_url
        assert auth_req.state
        assert auth_req.created_at

    def test_saml_callback_success(self):
        mgr = _make_manager()
        mgr.configure_idp(_okta_config())
        auth_req = mgr.initiate_sso_flow("idp-okta-001")

        result = mgr.handle_sso_callback({
            "request_id": auth_req.request_id,
            "assertion": _saml_assertion(),
        })

        assert isinstance(result, AuthResult)
        assert result.success
        assert result.email == "alice@example.com"
        assert result.name == "Alice"
        assert result.session_id
        assert result.provider_id == "idp-okta-001"

    def test_saml_callback_invalid_assertion(self):
        mgr = _make_manager()
        mgr.configure_idp(_okta_config())
        auth_req = mgr.initiate_sso_flow("idp-okta-001")

        result = mgr.handle_sso_callback({
            "request_id": auth_req.request_id,
            "assertion": {"bad": "data"},
        })
        assert not result.success
        assert "validation failed" in result.error

    def test_saml_callback_unknown_request(self):
        mgr = _make_manager()
        result = mgr.handle_sso_callback({"request_id": "nonexistent"})
        assert not result.success
        assert "expired" in result.error.lower() or "unknown" in result.error.lower()

    def test_saml_groups_mapped_to_roles(self):
        mgr = _make_manager()
        mgr.configure_idp(_okta_config())
        auth_req = mgr.initiate_sso_flow("idp-okta-001")

        assertion = _saml_assertion()
        assertion["attributes"]["groups"] = ["platform-admins", "developers"]

        result = mgr.handle_sso_callback({
            "request_id": auth_req.request_id,
            "assertion": assertion,
        })
        assert result.success
        # platform-admins -> admin (from idp mapping), developers -> editor (default map)
        assert "admin" in result.roles
        assert "editor" in result.roles


# ---------------------------------------------------------------------------
# OAuth2/OIDC flow tests
# ---------------------------------------------------------------------------


class TestOIDCFlow:

    def test_initiate_oidc_flow_with_pkce(self):
        mgr = _make_manager()
        mgr.configure_idp(_auth0_config())
        auth_req = mgr.initiate_sso_flow("idp-auth0-001")

        assert auth_req.protocol == SSOProtocol.OIDC
        assert auth_req.code_challenge  # PKCE should be generated
        assert auth_req.nonce

    def test_oidc_callback_success(self):
        mgr = _make_manager()
        mgr.configure_idp(_auth0_config())
        auth_req = mgr.initiate_sso_flow("idp-auth0-001")

        result = mgr.handle_sso_callback({
            "request_id": auth_req.request_id,
            "token_response": _oidc_token_response(),
        })

        assert result.success
        assert result.email == "bob@example.com"
        assert result.name == "Bob"
        assert result.access_token == "at-abc123"
        assert result.refresh_token == "rt-xyz789"
        assert result.session_id

    def test_oidc_callback_missing_token(self):
        mgr = _make_manager()
        mgr.configure_idp(_auth0_config())
        auth_req = mgr.initiate_sso_flow("idp-auth0-001")

        result = mgr.handle_sso_callback({
            "request_id": auth_req.request_id,
        })
        assert not result.success
        assert "token" in result.error.lower()

    def test_oidc_with_azure_ad(self):
        mgr = _make_manager()
        mgr.configure_idp(_azure_ad_config())
        auth_req = mgr.initiate_sso_flow("idp-azuread-001")

        result = mgr.handle_sso_callback({
            "request_id": auth_req.request_id,
            "token_response": _oidc_token_response(
                email="dev@contoso.com",
                name="Developer",
                groups=["admins"],
            ),
        })
        assert result.success
        assert result.email == "dev@contoso.com"
        assert "admin" in result.roles


# ---------------------------------------------------------------------------
# User attribute sync tests
# ---------------------------------------------------------------------------


class TestAttributeSync:

    def _create_user(self, mgr: SSOManager) -> AuthResult:
        mgr.configure_idp(_auth0_config())
        auth_req = mgr.initiate_sso_flow("idp-auth0-001")
        return mgr.handle_sso_callback({
            "request_id": auth_req.request_id,
            "token_response": _oidc_token_response(),
        })

    def test_sync_user_attributes(self):
        mgr = _make_manager()
        result = self._create_user(mgr)
        user = mgr.sync_user_attributes(result.user_id)

        assert isinstance(user, User)
        assert user.email == "bob@example.com"
        assert user.updated_at  # should be refreshed

    def test_sync_unknown_user_raises(self):
        mgr = _make_manager()
        try:
            mgr.sync_user_attributes("nonexistent")
            assert False, "Should have raised"
        except KeyError:
            pass

    def test_attribute_mapping_name_email_groups_roles(self):
        mgr = _make_manager()
        result = self._create_user(mgr)
        assert result.email == "bob@example.com"
        assert result.name == "Bob"
        assert isinstance(result.groups, tuple)
        assert isinstance(result.roles, tuple)

    def test_user_to_dict(self):
        mgr = _make_manager()
        result = self._create_user(mgr)
        user = mgr.get_user(result.user_id)
        data = user.to_dict()
        assert data["email"] == "bob@example.com"
        assert "groups" in data
        assert "roles" in data


# ---------------------------------------------------------------------------
# JIT provisioning tests
# ---------------------------------------------------------------------------


class TestJITProvisioning:

    def test_first_login_provisions_user(self):
        mgr = _make_manager()
        mgr.configure_idp(_auth0_config())
        auth_req = mgr.initiate_sso_flow("idp-auth0-001")

        result = mgr.handle_sso_callback({
            "request_id": auth_req.request_id,
            "token_response": _oidc_token_response(email="new@example.com"),
        })

        assert result.success
        assert result.jit_provisioned
        assert len(mgr.users) == 1
        assert mgr.users[0].email == "new@example.com"

    def test_repeat_login_does_not_duplicate_user(self):
        mgr = _make_manager()
        mgr.configure_idp(_auth0_config())

        # First login
        req1 = mgr.initiate_sso_flow("idp-auth0-001")
        r1 = mgr.handle_sso_callback({
            "request_id": req1.request_id,
            "token_response": _oidc_token_response(email="repeat@example.com"),
        })
        assert r1.jit_provisioned

        # Second login
        req2 = mgr.initiate_sso_flow("idp-auth0-001")
        r2 = mgr.handle_sso_callback({
            "request_id": req2.request_id,
            "token_response": _oidc_token_response(email="repeat@example.com"),
        })
        assert not r2.jit_provisioned
        assert len(mgr.users) == 1


# ---------------------------------------------------------------------------
# SCIM operations tests
# ---------------------------------------------------------------------------


class TestSCIMOperations:

    def test_scim_provision_new_user(self):
        mgr = _make_manager()
        mgr.configure_idp(_okta_config())

        user = mgr.scim_provision_user("idp-okta-001", {
            "email": "scim-user@example.com",
            "name": "SCIM User",
            "groups": ["developers"],
        })

        assert isinstance(user, User)
        assert user.email == "scim-user@example.com"
        assert user.active
        assert len(mgr.scim_log) == 1
        assert mgr.scim_log[0].operation == SCIMOperation.CREATE

    def test_scim_update_existing_user(self):
        mgr = _make_manager()
        mgr.configure_idp(_okta_config())

        # Create
        mgr.scim_provision_user("idp-okta-001", {
            "email": "update@example.com",
            "name": "Original",
        })
        # Update
        user = mgr.scim_provision_user("idp-okta-001", {
            "email": "update@example.com",
            "name": "Updated Name",
            "groups": ["admins"],
        })

        assert user.name == "Updated Name"
        assert len(mgr.scim_log) == 2
        assert mgr.scim_log[1].operation == SCIMOperation.UPDATE

    def test_scim_deactivate_user(self):
        mgr = _make_manager()
        mgr.configure_idp(_okta_config())

        mgr.scim_provision_user("idp-okta-001", {
            "email": "deactivate@example.com",
            "name": "To Deactivate",
        })

        result = mgr.scim_deactivate_user("idp-okta-001", "deactivate@example.com")
        assert result is not None
        assert not result.active
        assert len(mgr.scim_log) == 2
        assert mgr.scim_log[1].operation == SCIMOperation.DEACTIVATE

    def test_scim_deactivate_nonexistent(self):
        mgr = _make_manager()
        mgr.configure_idp(_okta_config())
        result = mgr.scim_deactivate_user("idp-okta-001", "nobody@example.com")
        assert result is None


# ---------------------------------------------------------------------------
# Session management tests
# ---------------------------------------------------------------------------


class TestSessionManagement:

    def _authenticate(self, mgr: SSOManager) -> AuthResult:
        mgr.configure_idp(_auth0_config())
        auth_req = mgr.initiate_sso_flow("idp-auth0-001")
        return mgr.handle_sso_callback({
            "request_id": auth_req.request_id,
            "token_response": _oidc_token_response(),
        })

    def test_session_created_on_login(self):
        mgr = _make_manager()
        result = self._authenticate(mgr)
        assert result.session_id
        assert result.expires_at

        session = mgr.manage_sessions(result.user_id)
        assert session.state == SessionState.ACTIVE
        assert session.user_id == result.user_id

    def test_revoke_session(self):
        mgr = _make_manager()
        result = self._authenticate(mgr)
        session = mgr.revoke_session(result.session_id)
        assert session.state == SessionState.REVOKED

    def test_revoke_unknown_session_raises(self):
        mgr = _make_manager()
        try:
            mgr.revoke_session("nonexistent")
            assert False, "Should have raised"
        except KeyError:
            pass

    def test_session_info_to_dict(self):
        mgr = _make_manager()
        result = self._authenticate(mgr)
        session = mgr.manage_sessions(result.user_id)
        data = session.to_dict()
        assert data["state"] == "active"
        assert "session_id" in data
        assert "expires_at" in data

    def test_no_active_session_raises(self):
        mgr = _make_manager()
        result = self._authenticate(mgr)
        mgr.revoke_session(result.session_id)
        try:
            mgr.manage_sessions(result.user_id)
            assert False, "Should have raised"
        except KeyError:
            pass

    def test_list_sessions(self):
        mgr = _make_manager()
        self._authenticate(mgr)
        assert len(mgr.sessions) == 1


# ---------------------------------------------------------------------------
# MFA enforcement
# ---------------------------------------------------------------------------


class TestMFAEnforcement:

    def _create_user(self, mgr: SSOManager) -> str:
        mgr.configure_idp(_auth0_config())
        auth_req = mgr.initiate_sso_flow("idp-auth0-001")
        result = mgr.handle_sso_callback({
            "request_id": auth_req.request_id,
            "token_response": _oidc_token_response(),
        })
        return result.user_id

    def test_enforce_mfa_default_totp(self):
        mgr = _make_manager()
        user_id = self._create_user(mgr)
        user = mgr.enforce_mfa(user_id)
        assert user.mfa_enabled
        assert MFAMethod.TOTP.value in user.mfa_methods

    def test_enforce_mfa_custom_methods(self):
        mgr = _make_manager()
        user_id = self._create_user(mgr)
        user = mgr.enforce_mfa(user_id, methods=["totp", "webauthn"])
        assert user.mfa_enabled
        assert "totp" in user.mfa_methods
        assert "webauthn" in user.mfa_methods

    def test_enforce_mfa_unknown_user_raises(self):
        mgr = _make_manager()
        try:
            mgr.enforce_mfa("nonexistent")
            assert False, "Should have raised"
        except KeyError:
            pass


# ---------------------------------------------------------------------------
# Group → role mapping tests
# ---------------------------------------------------------------------------


class TestGroupRoleMapping:

    def test_default_mapping(self):
        assert DEFAULT_GROUP_ROLE_MAP["admins"] == "admin"
        assert DEFAULT_GROUP_ROLE_MAP["developers"] == "editor"
        assert DEFAULT_GROUP_ROLE_MAP["viewers"] == "viewer"
        assert DEFAULT_GROUP_ROLE_MAP["managers"] == "manager"

    def test_idp_specific_mapping_takes_precedence(self):
        mgr = _make_manager()
        config = _okta_config()
        config["group_mappings"] = {"developers": "admin"}  # override default
        mgr.configure_idp(config)

        auth_req = mgr.initiate_sso_flow("idp-okta-001")
        assertion = _saml_assertion()
        assertion["attributes"]["groups"] = ["developers"]

        result = mgr.handle_sso_callback({
            "request_id": auth_req.request_id,
            "assertion": assertion,
        })
        assert "admin" in result.roles

    def test_no_matching_groups_gets_viewer_default(self):
        mgr = _make_manager()
        mgr.configure_idp(_auth0_config())
        auth_req = mgr.initiate_sso_flow("idp-auth0-001")

        result = mgr.handle_sso_callback({
            "request_id": auth_req.request_id,
            "token_response": _oidc_token_response(groups=["unknown-group"]),
        })
        assert result.success
        assert "viewer" in result.roles

    def test_disabled_provider_raises(self):
        mgr = _make_manager()
        config = _okta_config()
        config["enabled"] = False
        mgr.configure_idp(config)
        try:
            mgr.initiate_sso_flow("idp-okta-001")
            assert False, "Should have raised"
        except ValueError as exc:
            assert "disabled" in str(exc)
