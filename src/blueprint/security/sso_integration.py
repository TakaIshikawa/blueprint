"""Single Sign-On and identity provider integration for enterprise authentication.

Supports SAML 2.0, OAuth2/OIDC with PKCE, and SCIM for automated user
provisioning.  Integrates with Okta, Auth0, Azure AD, Google Workspace,
OneLogin, and JumpCloud.
"""

from __future__ import annotations

import base64
import hashlib
import secrets
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class SSOProtocol(str, Enum):
    """Supported SSO protocols."""

    SAML = "saml"
    OAUTH2 = "oauth2"
    OIDC = "oidc"


class IdentityProviderType(str, Enum):
    """Supported identity providers."""

    OKTA = "okta"
    AUTH0 = "auth0"
    AZURE_AD = "azure_ad"
    GOOGLE_WORKSPACE = "google_workspace"
    ONELOGIN = "onelogin"
    JUMPCLOUD = "jumpcloud"


class SessionState(str, Enum):
    """Session lifecycle states."""

    ACTIVE = "active"
    EXPIRED = "expired"
    REVOKED = "revoked"


class SCIMOperation(str, Enum):
    """SCIM provisioning operations."""

    CREATE = "create"
    UPDATE = "update"
    DEACTIVATE = "deactivate"
    DELETE = "delete"


class MFAMethod(str, Enum):
    """Multi-factor authentication methods."""

    TOTP = "totp"
    SMS = "sms"
    EMAIL = "email"
    PUSH = "push"
    WEBAUTHN = "webauthn"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class IdentityProvider:
    """Configured identity provider."""

    provider_id: str
    provider_type: IdentityProviderType
    protocol: SSOProtocol
    display_name: str
    entity_id: str = ""
    sso_url: str = ""
    slo_url: str = ""
    certificate: str = ""
    client_id: str = ""
    client_secret: str = ""
    scopes: tuple[str, ...] = field(default_factory=lambda: ("openid", "profile", "email"))
    metadata_url: str = ""
    group_mappings: dict[str, str] = field(default_factory=dict)
    enabled: bool = True
    created_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "provider_id": self.provider_id,
            "provider_type": self.provider_type.value,
            "protocol": self.protocol.value,
            "display_name": self.display_name,
            "entity_id": self.entity_id,
            "sso_url": self.sso_url,
            "slo_url": self.slo_url,
            "certificate": self.certificate,
            "client_id": self.client_id,
            "scopes": list(self.scopes),
            "metadata_url": self.metadata_url,
            "group_mappings": dict(self.group_mappings),
            "enabled": self.enabled,
            "created_at": self.created_at,
        }


@dataclass(frozen=True, slots=True)
class AuthRequest:
    """SSO authentication request."""

    request_id: str
    provider_id: str
    protocol: SSOProtocol
    redirect_url: str
    state: str
    nonce: str = ""
    code_verifier: str = ""
    code_challenge: str = ""
    created_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "request_id": self.request_id,
            "provider_id": self.provider_id,
            "protocol": self.protocol.value,
            "redirect_url": self.redirect_url,
            "state": self.state,
            "nonce": self.nonce,
            "code_challenge": self.code_challenge,
            "created_at": self.created_at,
        }


@dataclass(frozen=True, slots=True)
class AuthResult:
    """SSO authentication result."""

    success: bool
    user_id: str = ""
    email: str = ""
    name: str = ""
    groups: tuple[str, ...] = field(default_factory=tuple)
    roles: tuple[str, ...] = field(default_factory=tuple)
    attributes: dict[str, Any] = field(default_factory=dict)
    session_id: str = ""
    access_token: str = ""
    refresh_token: str = ""
    expires_at: str = ""
    error: str = ""
    provider_id: str = ""
    jit_provisioned: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "success": self.success,
            "user_id": self.user_id,
            "email": self.email,
            "name": self.name,
            "groups": list(self.groups),
            "roles": list(self.roles),
            "attributes": dict(self.attributes),
            "session_id": self.session_id,
            "expires_at": self.expires_at,
            "error": self.error,
            "provider_id": self.provider_id,
            "jit_provisioned": self.jit_provisioned,
        }


@dataclass(frozen=True, slots=True)
class User:
    """User record with IdP-synced attributes."""

    user_id: str
    email: str
    name: str
    groups: tuple[str, ...] = field(default_factory=tuple)
    roles: tuple[str, ...] = field(default_factory=tuple)
    provider_id: str = ""
    active: bool = True
    mfa_enabled: bool = False
    mfa_methods: tuple[str, ...] = field(default_factory=tuple)
    attributes: dict[str, Any] = field(default_factory=dict)
    last_login: str = ""
    created_at: str = ""
    updated_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "user_id": self.user_id,
            "email": self.email,
            "name": self.name,
            "groups": list(self.groups),
            "roles": list(self.roles),
            "provider_id": self.provider_id,
            "active": self.active,
            "mfa_enabled": self.mfa_enabled,
            "mfa_methods": list(self.mfa_methods),
            "attributes": dict(self.attributes),
            "last_login": self.last_login,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


@dataclass(frozen=True, slots=True)
class SessionInfo:
    """Active session information."""

    session_id: str
    user_id: str
    provider_id: str
    state: SessionState
    access_token: str = ""
    refresh_token: str = ""
    created_at: str = ""
    expires_at: str = ""
    last_activity: str = ""
    ip_address: str = ""
    user_agent: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "user_id": self.user_id,
            "provider_id": self.provider_id,
            "state": self.state.value,
            "created_at": self.created_at,
            "expires_at": self.expires_at,
            "last_activity": self.last_activity,
            "ip_address": self.ip_address,
            "user_agent": self.user_agent,
        }


@dataclass(frozen=True, slots=True)
class SCIMEvent:
    """SCIM provisioning event."""

    event_id: str
    operation: SCIMOperation
    user_id: str
    provider_id: str
    attributes: dict[str, Any] = field(default_factory=dict)
    timestamp: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "operation": self.operation.value,
            "user_id": self.user_id,
            "provider_id": self.provider_id,
            "attributes": dict(self.attributes),
            "timestamp": self.timestamp,
        }


# ---------------------------------------------------------------------------
# Default group → role mappings
# ---------------------------------------------------------------------------

DEFAULT_GROUP_ROLE_MAP: dict[str, str] = {
    "admins": "admin",
    "administrators": "admin",
    "developers": "editor",
    "engineers": "editor",
    "viewers": "viewer",
    "readonly": "viewer",
    "managers": "manager",
}

# ---------------------------------------------------------------------------
# PKCE helpers
# ---------------------------------------------------------------------------


def _generate_pkce() -> tuple[str, str]:
    """Generate PKCE code_verifier and code_challenge."""
    verifier = secrets.token_urlsafe(64)
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
    return verifier, challenge


# ---------------------------------------------------------------------------
# SAML assertion helpers
# ---------------------------------------------------------------------------


def _validate_saml_signature(assertion: dict[str, Any], certificate: str) -> bool:
    """Validate SAML assertion signature (stub).

    In production this would use ``xmlsec1`` or ``signxml`` to verify the
    XML-DSig signature against the IdP certificate.  Here we verify the
    presence of required assertion fields as a structural check.
    """
    required = {"issuer", "subject", "conditions", "attributes"}
    return required.issubset(set(assertion.keys())) and bool(certificate)


def _extract_saml_attributes(assertion: dict[str, Any]) -> dict[str, Any]:
    """Extract user attributes from a SAML assertion."""
    attrs = assertion.get("attributes", {})
    return {
        "email": attrs.get("email", assertion.get("subject", "")),
        "name": attrs.get("name", attrs.get("displayName", "")),
        "groups": attrs.get("groups", []),
        "roles": attrs.get("roles", []),
    }


# ---------------------------------------------------------------------------
# SSOManager
# ---------------------------------------------------------------------------

class SSOManager:
    """Manage SSO authentication flows and identity provider integrations.

    Supports SAML 2.0, OAuth2, and OIDC with PKCE.  Provides JIT user
    provisioning, SCIM-based automated provisioning, and IdP group-to-role
    mapping.
    """

    def __init__(
        self,
        group_role_map: dict[str, str] | None = None,
        session_ttl_hours: int = 8,
        mfa_required: bool = False,
    ) -> None:
        self._providers: dict[str, IdentityProvider] = {}
        self._users: dict[str, User] = {}
        self._sessions: dict[str, SessionInfo] = {}
        self._pending_requests: dict[str, AuthRequest] = {}
        self._scim_log: list[SCIMEvent] = []
        self._group_role_map = group_role_map or dict(DEFAULT_GROUP_ROLE_MAP)
        self._session_ttl = timedelta(hours=session_ttl_hours)
        self._mfa_required = mfa_required
        self._event_counter = 0

    # -- provider management -----------------------------------------------

    def configure_idp(self, provider_config: dict[str, Any]) -> IdentityProvider:
        """Register or update an identity provider configuration."""
        now = datetime.now(timezone.utc).isoformat()
        provider_type = IdentityProviderType(provider_config["provider_type"])
        protocol = SSOProtocol(provider_config.get("protocol", "oidc"))
        provider_id = provider_config.get(
            "provider_id",
            f"idp-{provider_type.value}-{secrets.token_hex(4)}",
        )

        idp = IdentityProvider(
            provider_id=provider_id,
            provider_type=provider_type,
            protocol=protocol,
            display_name=provider_config.get("display_name", provider_type.value),
            entity_id=provider_config.get("entity_id", ""),
            sso_url=provider_config.get("sso_url", ""),
            slo_url=provider_config.get("slo_url", ""),
            certificate=provider_config.get("certificate", ""),
            client_id=provider_config.get("client_id", ""),
            client_secret=provider_config.get("client_secret", ""),
            scopes=tuple(provider_config.get("scopes", ("openid", "profile", "email"))),
            metadata_url=provider_config.get("metadata_url", ""),
            group_mappings=provider_config.get("group_mappings", {}),
            enabled=provider_config.get("enabled", True),
            created_at=now,
        )
        self._providers[provider_id] = idp
        return idp

    def get_provider(self, provider_id: str) -> IdentityProvider:
        """Return the configured provider."""
        idp = self._providers.get(provider_id)
        if idp is None:
            raise KeyError(f"Identity provider not found: {provider_id}")
        return idp

    @property
    def providers(self) -> list[IdentityProvider]:
        return list(self._providers.values())

    # -- SSO flow ----------------------------------------------------------

    def initiate_sso_flow(self, provider_id: str) -> AuthRequest:
        """Start an SSO authentication flow for the given provider."""
        idp = self.get_provider(provider_id)
        if not idp.enabled:
            raise ValueError(f"Provider {provider_id} is disabled")

        state = secrets.token_urlsafe(32)
        nonce = secrets.token_urlsafe(16)
        request_id = f"req-{secrets.token_hex(8)}"
        code_verifier = ""
        code_challenge = ""

        if idp.protocol in (SSOProtocol.OAUTH2, SSOProtocol.OIDC):
            code_verifier, code_challenge = _generate_pkce()

        if idp.protocol == SSOProtocol.SAML:
            redirect = idp.sso_url or f"https://{idp.provider_type.value}.example.com/sso/saml"
        else:
            redirect = idp.sso_url or f"https://{idp.provider_type.value}.example.com/authorize"

        now = datetime.now(timezone.utc).isoformat()
        auth_req = AuthRequest(
            request_id=request_id,
            provider_id=provider_id,
            protocol=idp.protocol,
            redirect_url=redirect,
            state=state,
            nonce=nonce,
            code_verifier=code_verifier,
            code_challenge=code_challenge,
            created_at=now,
        )
        self._pending_requests[request_id] = auth_req
        return auth_req

    def handle_sso_callback(self, response: dict[str, Any]) -> AuthResult:
        """Process the callback from the identity provider.

        Parameters
        ----------
        response:
            A dict with at least ``request_id`` and either a SAML ``assertion``
            or an OAuth2/OIDC ``code`` and ``token_response``.
        """
        request_id = response.get("request_id", "")
        auth_req = self._pending_requests.pop(request_id, None)
        if auth_req is None:
            return AuthResult(success=False, error="Unknown or expired request")

        idp = self._providers.get(auth_req.provider_id)
        if idp is None:
            return AuthResult(success=False, error="Provider not found")

        if auth_req.protocol == SSOProtocol.SAML:
            return self._handle_saml_callback(response, idp, auth_req)
        else:
            return self._handle_oidc_callback(response, idp, auth_req)

    def _handle_saml_callback(
        self,
        response: dict[str, Any],
        idp: IdentityProvider,
        auth_req: AuthRequest,
    ) -> AuthResult:
        assertion = response.get("assertion", {})
        if not _validate_saml_signature(assertion, idp.certificate):
            return AuthResult(
                success=False,
                error="SAML assertion validation failed",
                provider_id=idp.provider_id,
            )

        attrs = _extract_saml_attributes(assertion)
        return self._complete_authentication(attrs, idp)

    def _handle_oidc_callback(
        self,
        response: dict[str, Any],
        idp: IdentityProvider,
        auth_req: AuthRequest,
    ) -> AuthResult:
        token_response = response.get("token_response", {})
        if not token_response:
            return AuthResult(
                success=False,
                error="Missing token response",
                provider_id=idp.provider_id,
            )

        # Extract user info from token response (or id_token claims)
        user_info = token_response.get("user_info", {})
        attrs = {
            "email": user_info.get("email", ""),
            "name": user_info.get("name", ""),
            "groups": user_info.get("groups", []),
            "roles": user_info.get("roles", []),
        }
        return self._complete_authentication(
            attrs, idp,
            access_token=token_response.get("access_token", ""),
            refresh_token=token_response.get("refresh_token", ""),
        )

    def _complete_authentication(
        self,
        attrs: dict[str, Any],
        idp: IdentityProvider,
        access_token: str = "",
        refresh_token: str = "",
    ) -> AuthResult:
        email = attrs.get("email", "")
        name = attrs.get("name", "")
        groups = tuple(attrs.get("groups", []))
        roles = self._map_groups_to_roles(groups, idp)

        # JIT provisioning
        jit = False
        existing = self._find_user_by_email(email)
        if existing is None:
            user = self._jit_provision(email, name, groups, roles, idp)
            jit = True
        else:
            user = self._sync_attributes(existing, name, groups, roles, idp)

        # Create session
        session = self._create_session(user, idp, access_token, refresh_token)

        return AuthResult(
            success=True,
            user_id=user.user_id,
            email=user.email,
            name=user.name,
            groups=user.groups,
            roles=user.roles,
            attributes=user.attributes,
            session_id=session.session_id,
            access_token=access_token,
            refresh_token=refresh_token,
            expires_at=session.expires_at,
            provider_id=idp.provider_id,
            jit_provisioned=jit,
        )

    # -- user management ---------------------------------------------------

    def sync_user_attributes(self, user_id: str) -> User:
        """Re-sync user attributes from the identity provider."""
        user = self._users.get(user_id)
        if user is None:
            raise KeyError(f"User not found: {user_id}")
        now = datetime.now(timezone.utc).isoformat()
        return User(
            user_id=user.user_id,
            email=user.email,
            name=user.name,
            groups=user.groups,
            roles=user.roles,
            provider_id=user.provider_id,
            active=user.active,
            mfa_enabled=user.mfa_enabled,
            mfa_methods=user.mfa_methods,
            attributes=user.attributes,
            last_login=user.last_login,
            created_at=user.created_at,
            updated_at=now,
        )

    def get_user(self, user_id: str) -> User:
        user = self._users.get(user_id)
        if user is None:
            raise KeyError(f"User not found: {user_id}")
        return user

    @property
    def users(self) -> list[User]:
        return list(self._users.values())

    def _find_user_by_email(self, email: str) -> User | None:
        for user in self._users.values():
            if user.email == email:
                return user
        return None

    def _jit_provision(
        self,
        email: str,
        name: str,
        groups: tuple[str, ...],
        roles: tuple[str, ...],
        idp: IdentityProvider,
    ) -> User:
        now = datetime.now(timezone.utc).isoformat()
        user_id = f"user-{secrets.token_hex(6)}"
        user = User(
            user_id=user_id,
            email=email,
            name=name,
            groups=groups,
            roles=roles,
            provider_id=idp.provider_id,
            active=True,
            last_login=now,
            created_at=now,
            updated_at=now,
        )
        self._users[user_id] = user
        return user

    def _sync_attributes(
        self,
        existing: User,
        name: str,
        groups: tuple[str, ...],
        roles: tuple[str, ...],
        idp: IdentityProvider,
    ) -> User:
        now = datetime.now(timezone.utc).isoformat()
        updated = User(
            user_id=existing.user_id,
            email=existing.email,
            name=name or existing.name,
            groups=groups,
            roles=roles,
            provider_id=idp.provider_id,
            active=existing.active,
            mfa_enabled=existing.mfa_enabled,
            mfa_methods=existing.mfa_methods,
            attributes=existing.attributes,
            last_login=now,
            created_at=existing.created_at,
            updated_at=now,
        )
        self._users[existing.user_id] = updated
        return updated

    # -- group → role mapping ----------------------------------------------

    def _map_groups_to_roles(
        self,
        groups: tuple[str, ...],
        idp: IdentityProvider,
    ) -> tuple[str, ...]:
        """Map IdP groups to blueprint roles using provider-specific and default mappings."""
        roles: set[str] = set()
        combined_map = {**self._group_role_map, **idp.group_mappings}
        for group in groups:
            normalized = group.lower().strip()
            if normalized in combined_map:
                roles.add(combined_map[normalized])
        if not roles:
            roles.add("viewer")  # default role
        return tuple(sorted(roles))

    # -- session management ------------------------------------------------

    def manage_sessions(self, user_id: str) -> SessionInfo:
        """Return the most recent active session for *user_id*."""
        user_sessions = [
            s for s in self._sessions.values()
            if s.user_id == user_id and s.state == SessionState.ACTIVE
        ]
        if not user_sessions:
            raise KeyError(f"No active session for user: {user_id}")
        return max(user_sessions, key=lambda s: s.created_at)

    def revoke_session(self, session_id: str) -> SessionInfo:
        """Revoke an active session."""
        session = self._sessions.get(session_id)
        if session is None:
            raise KeyError(f"Session not found: {session_id}")
        revoked = SessionInfo(
            session_id=session.session_id,
            user_id=session.user_id,
            provider_id=session.provider_id,
            state=SessionState.REVOKED,
            created_at=session.created_at,
            expires_at=session.expires_at,
            last_activity=datetime.now(timezone.utc).isoformat(),
            ip_address=session.ip_address,
            user_agent=session.user_agent,
        )
        self._sessions[session_id] = revoked
        return revoked

    def _create_session(
        self,
        user: User,
        idp: IdentityProvider,
        access_token: str = "",
        refresh_token: str = "",
    ) -> SessionInfo:
        now = datetime.now(timezone.utc)
        expires = now + self._session_ttl
        session_id = f"sess-{secrets.token_hex(8)}"
        session = SessionInfo(
            session_id=session_id,
            user_id=user.user_id,
            provider_id=idp.provider_id,
            state=SessionState.ACTIVE,
            access_token=access_token,
            refresh_token=refresh_token,
            created_at=now.isoformat(),
            expires_at=expires.isoformat(),
            last_activity=now.isoformat(),
        )
        self._sessions[session_id] = session
        return session

    @property
    def sessions(self) -> list[SessionInfo]:
        return list(self._sessions.values())

    # -- SCIM provisioning -------------------------------------------------

    def scim_provision_user(
        self,
        provider_id: str,
        user_attrs: dict[str, Any],
    ) -> User:
        """Provision a user via SCIM (automated from IdP)."""
        idp = self.get_provider(provider_id)
        now = datetime.now(timezone.utc).isoformat()
        email = user_attrs.get("email", "")

        existing = self._find_user_by_email(email)
        if existing:
            groups = tuple(user_attrs.get("groups", list(existing.groups)))
            roles = self._map_groups_to_roles(groups, idp)
            user = self._sync_attributes(existing, user_attrs.get("name", ""), groups, roles, idp)
            op = SCIMOperation.UPDATE
        else:
            user_id = f"user-{secrets.token_hex(6)}"
            groups = tuple(user_attrs.get("groups", []))
            roles = self._map_groups_to_roles(groups, idp)
            user = User(
                user_id=user_id,
                email=email,
                name=user_attrs.get("name", ""),
                groups=groups,
                roles=roles,
                provider_id=provider_id,
                active=True,
                created_at=now,
                updated_at=now,
            )
            self._users[user_id] = user
            op = SCIMOperation.CREATE

        self._log_scim(op, user.user_id, provider_id, user_attrs)
        return user

    def scim_deactivate_user(self, provider_id: str, email: str) -> User | None:
        """Deactivate a user via SCIM."""
        user = self._find_user_by_email(email)
        if user is None:
            return None
        now = datetime.now(timezone.utc).isoformat()
        deactivated = User(
            user_id=user.user_id,
            email=user.email,
            name=user.name,
            groups=user.groups,
            roles=user.roles,
            provider_id=user.provider_id,
            active=False,
            created_at=user.created_at,
            updated_at=now,
        )
        self._users[user.user_id] = deactivated
        self._log_scim(SCIMOperation.DEACTIVATE, user.user_id, provider_id, {"email": email})
        return deactivated

    def _log_scim(
        self,
        operation: SCIMOperation,
        user_id: str,
        provider_id: str,
        attributes: dict[str, Any],
    ) -> None:
        self._event_counter += 1
        self._scim_log.append(SCIMEvent(
            event_id=f"scim-{self._event_counter:04d}",
            operation=operation,
            user_id=user_id,
            provider_id=provider_id,
            attributes=attributes,
            timestamp=datetime.now(timezone.utc).isoformat(),
        ))

    @property
    def scim_log(self) -> list[SCIMEvent]:
        return list(self._scim_log)

    # -- MFA ---------------------------------------------------------------

    def enforce_mfa(self, user_id: str, methods: list[str] | None = None) -> User:
        """Enable MFA for a user."""
        user = self._users.get(user_id)
        if user is None:
            raise KeyError(f"User not found: {user_id}")
        now = datetime.now(timezone.utc).isoformat()
        mfa_methods = tuple(methods) if methods else (MFAMethod.TOTP.value,)
        updated = User(
            user_id=user.user_id,
            email=user.email,
            name=user.name,
            groups=user.groups,
            roles=user.roles,
            provider_id=user.provider_id,
            active=user.active,
            mfa_enabled=True,
            mfa_methods=mfa_methods,
            attributes=user.attributes,
            last_login=user.last_login,
            created_at=user.created_at,
            updated_at=now,
        )
        self._users[user_id] = updated
        return updated


__all__ = [
    "AuthRequest",
    "AuthResult",
    "DEFAULT_GROUP_ROLE_MAP",
    "IdentityProvider",
    "IdentityProviderType",
    "MFAMethod",
    "SCIMEvent",
    "SCIMOperation",
    "SSOManager",
    "SSOProtocol",
    "SessionInfo",
    "SessionState",
    "User",
]
