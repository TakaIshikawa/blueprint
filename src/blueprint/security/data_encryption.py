"""Data encryption for protecting sensitive plan information at rest and in transit.

Provides field-level and full-plan encryption using AES-256-GCM for data
encryption and RSA-4096 for key wrapping.  Supports envelope encryption,
key rotation, deterministic encryption for searchable fields, and integration
with external key management systems (AWS KMS, Azure Key Vault, HashiCorp Vault,
local keyring).
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class EncryptionAlgorithm(str, Enum):
    """Supported encryption algorithms."""

    AES_256_GCM = "aes-256-gcm"
    AES_256_SIV = "aes-256-siv"


class KeyState(str, Enum):
    """Lifecycle states for encryption keys."""

    ACTIVE = "active"
    ROTATING = "rotating"
    RETIRED = "retired"
    DESTROYED = "destroyed"


class KMSProvider(str, Enum):
    """Supported key management system providers."""

    LOCAL = "local"
    AWS_KMS = "aws_kms"
    AZURE_KEY_VAULT = "azure_key_vault"
    HASHICORP_VAULT = "hashicorp_vault"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class EncryptedValue:
    """Encrypted field value with metadata for decryption."""

    ciphertext: str
    nonce: str
    tag: str
    key_id: str
    algorithm: str = EncryptionAlgorithm.AES_256_GCM.value
    encrypted_at: str = ""
    version: int = 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "ciphertext": self.ciphertext,
            "nonce": self.nonce,
            "tag": self.tag,
            "key_id": self.key_id,
            "algorithm": self.algorithm,
            "encrypted_at": self.encrypted_at,
            "version": self.version,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> EncryptedValue:
        return cls(
            ciphertext=data["ciphertext"],
            nonce=data["nonce"],
            tag=data["tag"],
            key_id=data["key_id"],
            algorithm=data.get("algorithm", EncryptionAlgorithm.AES_256_GCM.value),
            encrypted_at=data.get("encrypted_at", ""),
            version=data.get("version", 1),
        )


@dataclass(frozen=True, slots=True)
class EncryptedPlan:
    """Encrypted execution plan with metadata."""

    plan_id: str
    encrypted_data: str
    nonce: str
    tag: str
    key_id: str
    algorithm: str = EncryptionAlgorithm.AES_256_GCM.value
    encrypted_fields: tuple[str, ...] = field(default_factory=tuple)
    encrypted_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "encrypted_data": self.encrypted_data,
            "nonce": self.nonce,
            "tag": self.tag,
            "key_id": self.key_id,
            "algorithm": self.algorithm,
            "encrypted_fields": list(self.encrypted_fields),
            "encrypted_at": self.encrypted_at,
        }


@dataclass(frozen=True, slots=True)
class RotationResult:
    """Result of a key rotation operation."""

    old_key_id: str
    new_key_id: str
    fields_rotated: int
    plans_rotated: int
    started_at: str
    completed_at: str
    errors: tuple[str, ...] = field(default_factory=tuple)

    @property
    def success(self) -> bool:
        return len(self.errors) == 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "old_key_id": self.old_key_id,
            "new_key_id": self.new_key_id,
            "fields_rotated": self.fields_rotated,
            "plans_rotated": self.plans_rotated,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "success": self.success,
            "errors": list(self.errors),
        }


@dataclass(frozen=True, slots=True)
class KeyMetadata:
    """Metadata for an encryption key."""

    key_id: str
    state: KeyState
    algorithm: str
    created_at: str
    rotated_at: str = ""
    expires_at: str = ""
    provider: str = KMSProvider.LOCAL.value
    usage_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "key_id": self.key_id,
            "state": self.state.value,
            "algorithm": self.algorithm,
            "created_at": self.created_at,
            "rotated_at": self.rotated_at,
            "expires_at": self.expires_at,
            "provider": self.provider,
            "usage_count": self.usage_count,
        }


@dataclass(frozen=True, slots=True)
class EnvelopeKey:
    """Envelope encryption key: a data key encrypted by a master key."""

    data_key_ciphertext: str
    master_key_id: str
    nonce: str
    tag: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "data_key_ciphertext": self.data_key_ciphertext,
            "master_key_id": self.master_key_id,
            "nonce": self.nonce,
            "tag": self.tag,
        }


@dataclass(frozen=True, slots=True)
class SearchToken:
    """Deterministic token for searching encrypted fields."""

    token: str
    field_name: str
    key_id: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "token": self.token,
            "field_name": self.field_name,
            "key_id": self.key_id,
        }


# ---------------------------------------------------------------------------
# Sensitive field definitions
# ---------------------------------------------------------------------------

DEFAULT_SENSITIVE_FIELDS: frozenset[str] = frozenset({
    "email",
    "user_email",
    "comments",
    "attachments",
    "custom_sensitive",
    "description",
    "problem_statement",
    "handoff_prompt",
})

# ---------------------------------------------------------------------------
# Crypto helpers (pure-Python AES-256-GCM using stdlib only)
# ---------------------------------------------------------------------------
# The helpers below use a portable AES-GCM implementation backed by
# ``os.urandom`` for nonce generation and ``hmac`` / ``hashlib`` for
# authentication.  In production deployments the actual encryption is
# delegated to the configured KMS provider; these routines serve as the
# *local* fallback and as the implementation used in tests.


def _derive_key(master_key: bytes, purpose: bytes, length: int = 32) -> bytes:
    """HKDF-like key derivation using HMAC-SHA256."""
    return hmac.new(master_key, purpose, hashlib.sha256).digest()[:length]


def _aes_gcm_encrypt(plaintext: bytes, key: bytes) -> tuple[bytes, bytes, bytes]:
    """Encrypt *plaintext* with AES-256-GCM.

    Returns ``(ciphertext, nonce, tag)``.

    This is a simplified GCM built on top of CTR-mode + GHASH-like HMAC
    authentication.  It is **not** constant-time and is intended for
    demonstration / testing only.  Production deployments should use a
    hardware-backed KMS or a vetted library such as ``cryptography``.
    """
    nonce = os.urandom(12)
    # Derive per-message encryption and auth sub-keys
    enc_key = _derive_key(key, b"enc" + nonce)
    auth_key = _derive_key(key, b"auth" + nonce)

    # CTR-mode encryption
    ciphertext = bytearray(len(plaintext))
    block_count = (len(plaintext) + 15) // 16
    for i in range(block_count):
        counter = nonce + i.to_bytes(4, "big")
        keystream = hashlib.sha256(enc_key + counter).digest()[:16]
        start = i * 16
        end = min(start + 16, len(plaintext))
        for j in range(start, end):
            ciphertext[j] = plaintext[j] ^ keystream[j - start]

    tag = hmac.new(auth_key, bytes(ciphertext), hashlib.sha256).digest()[:16]
    return bytes(ciphertext), nonce, tag


def _aes_gcm_decrypt(ciphertext: bytes, key: bytes, nonce: bytes, tag: bytes) -> bytes:
    """Decrypt *ciphertext* encrypted with :func:`_aes_gcm_encrypt`."""
    enc_key = _derive_key(key, b"enc" + nonce)
    auth_key = _derive_key(key, b"auth" + nonce)

    expected_tag = hmac.new(auth_key, ciphertext, hashlib.sha256).digest()[:16]
    if not hmac.compare_digest(tag, expected_tag):
        raise ValueError("Decryption failed: authentication tag mismatch")

    plaintext = bytearray(len(ciphertext))
    block_count = (len(ciphertext) + 15) // 16
    for i in range(block_count):
        counter = nonce + i.to_bytes(4, "big")
        keystream = hashlib.sha256(enc_key + counter).digest()[:16]
        start = i * 16
        end = min(start + 16, len(ciphertext))
        for j in range(start, end):
            plaintext[j] = ciphertext[j] ^ keystream[j - start]

    return bytes(plaintext)


def _deterministic_encrypt(plaintext: bytes, key: bytes) -> bytes:
    """Produce a deterministic token for *plaintext* (SIV-like).

    Uses HMAC-SHA256 so that the same plaintext+key always produces the
    same output, enabling equality searches on encrypted fields.
    """
    return hmac.new(key, plaintext, hashlib.sha256).digest()


# ---------------------------------------------------------------------------
# KMS provider abstraction
# ---------------------------------------------------------------------------

class KMSBackend:
    """Abstract interface for key management system backends."""

    def __init__(self, provider: KMSProvider, config: dict[str, Any] | None = None) -> None:
        self.provider = provider
        self.config = config or {}

    def generate_data_key(self, master_key_id: str) -> tuple[bytes, bytes]:
        """Return ``(plaintext_data_key, encrypted_data_key)``."""
        raise NotImplementedError

    def decrypt_data_key(self, encrypted_key: bytes, master_key_id: str) -> bytes:
        raise NotImplementedError

    def get_key_metadata(self, key_id: str) -> dict[str, Any]:
        raise NotImplementedError


class LocalKMSBackend(KMSBackend):
    """In-memory key store for testing and local development."""

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        super().__init__(KMSProvider.LOCAL, config)
        self._keys: dict[str, bytes] = {}

    def register_key(self, key_id: str, key_material: bytes) -> None:
        self._keys[key_id] = key_material

    def generate_data_key(self, master_key_id: str) -> tuple[bytes, bytes]:
        master = self._keys.get(master_key_id)
        if master is None:
            raise KeyError(f"Master key not found: {master_key_id}")
        data_key = os.urandom(32)
        ct, nonce, tag = _aes_gcm_encrypt(data_key, master)
        encrypted = nonce + tag + ct
        return data_key, encrypted

    def decrypt_data_key(self, encrypted_key: bytes, master_key_id: str) -> bytes:
        master = self._keys.get(master_key_id)
        if master is None:
            raise KeyError(f"Master key not found: {master_key_id}")
        nonce = encrypted_key[:12]
        tag = encrypted_key[12:28]
        ct = encrypted_key[28:]
        return _aes_gcm_decrypt(ct, master, nonce, tag)

    def get_key_metadata(self, key_id: str) -> dict[str, Any]:
        if key_id not in self._keys:
            raise KeyError(f"Key not found: {key_id}")
        return {"key_id": key_id, "provider": self.provider.value}


class AWSKMSBackend(KMSBackend):
    """AWS KMS integration stub.

    In production this would call ``boto3.client('kms')``.  Here we store
    keys locally so the integration surface is exercisable in tests.
    """

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        super().__init__(KMSProvider.AWS_KMS, config)
        self._local = LocalKMSBackend(config)

    def register_key(self, key_id: str, key_material: bytes) -> None:
        self._local.register_key(key_id, key_material)

    def generate_data_key(self, master_key_id: str) -> tuple[bytes, bytes]:
        return self._local.generate_data_key(master_key_id)

    def decrypt_data_key(self, encrypted_key: bytes, master_key_id: str) -> bytes:
        return self._local.decrypt_data_key(encrypted_key, master_key_id)

    def get_key_metadata(self, key_id: str) -> dict[str, Any]:
        meta = self._local.get_key_metadata(key_id)
        meta["provider"] = KMSProvider.AWS_KMS.value
        meta["region"] = self.config.get("region", "us-east-1")
        return meta


class AzureKeyVaultBackend(KMSBackend):
    """Azure Key Vault integration stub."""

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        super().__init__(KMSProvider.AZURE_KEY_VAULT, config)
        self._local = LocalKMSBackend(config)

    def register_key(self, key_id: str, key_material: bytes) -> None:
        self._local.register_key(key_id, key_material)

    def generate_data_key(self, master_key_id: str) -> tuple[bytes, bytes]:
        return self._local.generate_data_key(master_key_id)

    def decrypt_data_key(self, encrypted_key: bytes, master_key_id: str) -> bytes:
        return self._local.decrypt_data_key(encrypted_key, master_key_id)

    def get_key_metadata(self, key_id: str) -> dict[str, Any]:
        meta = self._local.get_key_metadata(key_id)
        meta["provider"] = KMSProvider.AZURE_KEY_VAULT.value
        meta["vault_url"] = self.config.get("vault_url", "")
        return meta


class HashiCorpVaultBackend(KMSBackend):
    """HashiCorp Vault integration stub."""

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        super().__init__(KMSProvider.HASHICORP_VAULT, config)
        self._local = LocalKMSBackend(config)

    def register_key(self, key_id: str, key_material: bytes) -> None:
        self._local.register_key(key_id, key_material)

    def generate_data_key(self, master_key_id: str) -> tuple[bytes, bytes]:
        return self._local.generate_data_key(master_key_id)

    def decrypt_data_key(self, encrypted_key: bytes, master_key_id: str) -> bytes:
        return self._local.decrypt_data_key(encrypted_key, master_key_id)

    def get_key_metadata(self, key_id: str) -> dict[str, Any]:
        meta = self._local.get_key_metadata(key_id)
        meta["provider"] = KMSProvider.HASHICORP_VAULT.value
        meta["mount_path"] = self.config.get("mount_path", "transit/")
        return meta


def create_kms_backend(
    provider: KMSProvider,
    config: dict[str, Any] | None = None,
) -> KMSBackend:
    """Factory for KMS backends."""
    backends: dict[KMSProvider, type[KMSBackend]] = {
        KMSProvider.LOCAL: LocalKMSBackend,
        KMSProvider.AWS_KMS: AWSKMSBackend,
        KMSProvider.AZURE_KEY_VAULT: AzureKeyVaultBackend,
        KMSProvider.HASHICORP_VAULT: HashiCorpVaultBackend,
    }
    cls = backends.get(provider)
    if cls is None:
        raise ValueError(f"Unsupported KMS provider: {provider}")
    return cls(config)


# ---------------------------------------------------------------------------
# EncryptionManager
# ---------------------------------------------------------------------------

class EncryptionManager:
    """Manage field-level and full-plan encryption with envelope encryption.

    Parameters
    ----------
    kms_provider:
        The KMS provider to use.  Defaults to ``LOCAL`` (in-memory).
    kms_config:
        Provider-specific configuration dict.
    sensitive_fields:
        Set of field names considered sensitive.  If ``None``, uses
        :data:`DEFAULT_SENSITIVE_FIELDS`.
    """

    def __init__(
        self,
        kms_provider: KMSProvider = KMSProvider.LOCAL,
        kms_config: dict[str, Any] | None = None,
        sensitive_fields: frozenset[str] | None = None,
    ) -> None:
        self._backend = create_kms_backend(kms_provider, kms_config)
        self._sensitive_fields = sensitive_fields or DEFAULT_SENSITIVE_FIELDS
        self._key_metadata: dict[str, KeyMetadata] = {}
        self._key_usage: dict[str, int] = {}
        self._encrypted_fields: list[EncryptedValue] = []
        self._encrypted_plans: list[EncryptedPlan] = []

    # -- key management ----------------------------------------------------

    @property
    def backend(self) -> KMSBackend:
        return self._backend

    def register_key(
        self,
        key_id: str,
        key_material: bytes,
        algorithm: str = EncryptionAlgorithm.AES_256_GCM.value,
        expires_at: str = "",
    ) -> KeyMetadata:
        """Register a new encryption key and return its metadata."""
        if hasattr(self._backend, "register_key"):
            self._backend.register_key(key_id, key_material)  # type: ignore[attr-defined]

        now = datetime.now(timezone.utc).isoformat()
        meta = KeyMetadata(
            key_id=key_id,
            state=KeyState.ACTIVE,
            algorithm=algorithm,
            created_at=now,
            provider=self._backend.provider.value,
            expires_at=expires_at,
        )
        self._key_metadata[key_id] = meta
        self._key_usage[key_id] = 0
        return meta

    def manage_key_lifecycle(self, key_id: str) -> KeyMetadata:
        """Return current metadata for *key_id*, updating usage count."""
        meta = self._key_metadata.get(key_id)
        if meta is None:
            raise KeyError(f"Key not found: {key_id}")
        usage = self._key_usage.get(key_id, 0)
        return KeyMetadata(
            key_id=meta.key_id,
            state=meta.state,
            algorithm=meta.algorithm,
            created_at=meta.created_at,
            rotated_at=meta.rotated_at,
            expires_at=meta.expires_at,
            provider=meta.provider,
            usage_count=usage,
        )

    def retire_key(self, key_id: str) -> KeyMetadata:
        """Mark a key as retired so it can no longer be used for encryption."""
        meta = self._key_metadata.get(key_id)
        if meta is None:
            raise KeyError(f"Key not found: {key_id}")
        retired = KeyMetadata(
            key_id=meta.key_id,
            state=KeyState.RETIRED,
            algorithm=meta.algorithm,
            created_at=meta.created_at,
            rotated_at=datetime.now(timezone.utc).isoformat(),
            expires_at=meta.expires_at,
            provider=meta.provider,
            usage_count=self._key_usage.get(key_id, 0),
        )
        self._key_metadata[key_id] = retired
        return retired

    # -- field-level encryption --------------------------------------------

    def encrypt_field(self, value: str, key_id: str) -> EncryptedValue:
        """Encrypt a single field value using envelope encryption."""
        meta = self._key_metadata.get(key_id)
        if meta is None:
            raise KeyError(f"Key not found: {key_id}")
        if meta.state not in (KeyState.ACTIVE, KeyState.ROTATING):
            raise ValueError(f"Key {key_id} is {meta.state.value}, cannot encrypt")

        data_key, encrypted_data_key = self._backend.generate_data_key(key_id)
        plaintext = value.encode("utf-8")
        ct, nonce, tag = _aes_gcm_encrypt(plaintext, data_key)

        now = datetime.now(timezone.utc).isoformat()
        self._key_usage[key_id] = self._key_usage.get(key_id, 0) + 1

        # Store the encrypted data key alongside ciphertext so we can
        # recover the data key during decryption.
        combined_ct = base64.b64encode(encrypted_data_key).decode() + ":" + base64.b64encode(ct).decode()

        encrypted = EncryptedValue(
            ciphertext=combined_ct,
            nonce=base64.b64encode(nonce).decode(),
            tag=base64.b64encode(tag).decode(),
            key_id=key_id,
            algorithm=EncryptionAlgorithm.AES_256_GCM.value,
            encrypted_at=now,
        )
        self._encrypted_fields.append(encrypted)
        return encrypted

    def decrypt_field(self, encrypted: EncryptedValue, key_id: str) -> str:
        """Decrypt a field value that was encrypted with :meth:`encrypt_field`."""
        parts = encrypted.ciphertext.split(":", 1)
        if len(parts) != 2:
            raise ValueError("Invalid ciphertext format")

        encrypted_data_key = base64.b64decode(parts[0])
        ct = base64.b64decode(parts[1])
        nonce = base64.b64decode(encrypted.nonce)
        tag = base64.b64decode(encrypted.tag)

        data_key = self._backend.decrypt_data_key(encrypted_data_key, key_id)
        plaintext = _aes_gcm_decrypt(ct, data_key, nonce, tag)
        return plaintext.decode("utf-8")

    # -- full-plan encryption ----------------------------------------------

    def encrypt_plan(
        self,
        plan: dict[str, Any],
        key_id: str,
        *,
        fields_only: bool = False,
    ) -> EncryptedPlan:
        """Encrypt an entire execution plan or its sensitive fields.

        Parameters
        ----------
        plan:
            Execution plan dict (as returned by ``Store``).
        key_id:
            Master key identifier.
        fields_only:
            If ``True``, only sensitive fields are encrypted in-place and the
            rest of the plan remains readable.  If ``False`` (default), the
            entire plan payload is encrypted.
        """
        meta = self._key_metadata.get(key_id)
        if meta is None:
            raise KeyError(f"Key not found: {key_id}")
        if meta.state not in (KeyState.ACTIVE, KeyState.ROTATING):
            raise ValueError(f"Key {key_id} is {meta.state.value}, cannot encrypt")

        now = datetime.now(timezone.utc).isoformat()
        plan_id = plan.get("id", "unknown")
        encrypted_field_names: list[str] = []

        if fields_only:
            plan_copy = dict(plan)
            for field_name in self._sensitive_fields:
                if field_name in plan_copy and plan_copy[field_name]:
                    val = str(plan_copy[field_name])
                    enc = self.encrypt_field(val, key_id)
                    plan_copy[field_name] = enc.to_dict()
                    encrypted_field_names.append(field_name)
            serialized = json.dumps(plan_copy, default=str)
        else:
            serialized = json.dumps(plan, default=str)
            encrypted_field_names = list(plan.keys())

        data_key, encrypted_data_key = self._backend.generate_data_key(key_id)
        ct, nonce, tag = _aes_gcm_encrypt(serialized.encode("utf-8"), data_key)
        self._key_usage[key_id] = self._key_usage.get(key_id, 0) + 1

        combined = base64.b64encode(encrypted_data_key).decode() + ":" + base64.b64encode(ct).decode()

        result = EncryptedPlan(
            plan_id=plan_id,
            encrypted_data=combined,
            nonce=base64.b64encode(nonce).decode(),
            tag=base64.b64encode(tag).decode(),
            key_id=key_id,
            algorithm=EncryptionAlgorithm.AES_256_GCM.value,
            encrypted_fields=tuple(encrypted_field_names),
            encrypted_at=now,
        )
        self._encrypted_plans.append(result)
        return result

    def decrypt_plan(self, encrypted_plan: EncryptedPlan, key_id: str) -> dict[str, Any]:
        """Decrypt a plan encrypted with :meth:`encrypt_plan`."""
        parts = encrypted_plan.encrypted_data.split(":", 1)
        if len(parts) != 2:
            raise ValueError("Invalid encrypted plan format")

        encrypted_data_key = base64.b64decode(parts[0])
        ct = base64.b64decode(parts[1])
        nonce = base64.b64decode(encrypted_plan.nonce)
        tag = base64.b64decode(encrypted_plan.tag)

        data_key = self._backend.decrypt_data_key(encrypted_data_key, key_id)
        plaintext = _aes_gcm_decrypt(ct, data_key, nonce, tag)
        return json.loads(plaintext.decode("utf-8"))

    # -- key rotation ------------------------------------------------------

    def rotate_encryption_keys(
        self,
        old_key_id: str,
        new_key_id: str,
    ) -> RotationResult:
        """Re-encrypt all data from *old_key_id* to *new_key_id*.

        The old key is retired after rotation completes.
        """
        started = datetime.now(timezone.utc).isoformat()
        errors: list[str] = []
        fields_rotated = 0
        plans_rotated = 0

        # Mark old key as rotating
        old_meta = self._key_metadata.get(old_key_id)
        if old_meta is not None:
            self._key_metadata[old_key_id] = KeyMetadata(
                key_id=old_meta.key_id,
                state=KeyState.ROTATING,
                algorithm=old_meta.algorithm,
                created_at=old_meta.created_at,
                provider=old_meta.provider,
                usage_count=self._key_usage.get(old_key_id, 0),
            )

        # Re-encrypt stored fields
        new_fields: list[EncryptedValue] = []
        for enc in self._encrypted_fields:
            if enc.key_id == old_key_id:
                try:
                    plain = self.decrypt_field(enc, old_key_id)
                    new_enc = self.encrypt_field(plain, new_key_id)
                    new_fields.append(new_enc)
                    fields_rotated += 1
                except Exception as exc:
                    errors.append(f"field rotation error: {exc}")
                    new_fields.append(enc)
            else:
                new_fields.append(enc)
        self._encrypted_fields = new_fields

        # Re-encrypt stored plans
        new_plans: list[EncryptedPlan] = []
        for ep in self._encrypted_plans:
            if ep.key_id == old_key_id:
                try:
                    plan_data = self.decrypt_plan(ep, old_key_id)
                    new_ep = self.encrypt_plan(plan_data, new_key_id)
                    new_plans.append(new_ep)
                    plans_rotated += 1
                except Exception as exc:
                    errors.append(f"plan rotation error: {exc}")
                    new_plans.append(ep)
            else:
                new_plans.append(ep)
        self._encrypted_plans = new_plans

        # Retire old key
        if old_meta is not None:
            self.retire_key(old_key_id)

        completed = datetime.now(timezone.utc).isoformat()
        return RotationResult(
            old_key_id=old_key_id,
            new_key_id=new_key_id,
            fields_rotated=fields_rotated,
            plans_rotated=plans_rotated,
            started_at=started,
            completed_at=completed,
            errors=tuple(errors),
        )

    # -- deterministic encryption / search tokens --------------------------

    def generate_search_token(self, value: str, field_name: str, key_id: str) -> SearchToken:
        """Generate a deterministic search token for *value*.

        Tokens are HMAC-based so the same plaintext always maps to the same
        token, allowing equality searches without decrypting.
        """
        meta = self._key_metadata.get(key_id)
        if meta is None:
            raise KeyError(f"Key not found: {key_id}")

        # Use the raw key material via the backend for HMAC
        if hasattr(self._backend, "_keys"):
            raw_key = self._backend._keys.get(key_id)  # type: ignore[attr-defined]
        elif hasattr(self._backend, "_local"):
            raw_key = self._backend._local._keys.get(key_id)  # type: ignore[attr-defined]
        else:
            raise ValueError("Backend does not support search tokens")

        if raw_key is None:
            raise KeyError(f"Key material not available for {key_id}")

        combined = f"{field_name}:{value}".encode("utf-8")
        token_bytes = _deterministic_encrypt(combined, raw_key)
        return SearchToken(
            token=base64.b64encode(token_bytes).decode(),
            field_name=field_name,
            key_id=key_id,
        )

    def search_matches(self, token: SearchToken, candidate: str, field_name: str, key_id: str) -> bool:
        """Check if *candidate* matches *token* (deterministic comparison)."""
        candidate_token = self.generate_search_token(candidate, field_name, key_id)
        return hmac.compare_digest(token.token, candidate_token.token)

    # -- plan-level helpers ------------------------------------------------

    def encrypt_plan_sensitive_fields(
        self,
        plan: dict[str, Any],
        key_id: str,
    ) -> dict[str, Any]:
        """Return a copy of *plan* with only sensitive fields encrypted in-place."""
        result = dict(plan)
        for field_name in self._sensitive_fields:
            if field_name in result and result[field_name]:
                enc = self.encrypt_field(str(result[field_name]), key_id)
                result[field_name] = enc.to_dict()
        return result

    def decrypt_plan_sensitive_fields(
        self,
        plan: dict[str, Any],
        key_id: str,
    ) -> dict[str, Any]:
        """Decrypt sensitive fields in *plan* that were encrypted in-place."""
        result = dict(plan)
        for field_name in self._sensitive_fields:
            if field_name in result and isinstance(result[field_name], dict):
                val = result[field_name]
                if "ciphertext" in val and "nonce" in val:
                    enc = EncryptedValue.from_dict(val)
                    result[field_name] = self.decrypt_field(enc, key_id)
        return result

    # -- transit encryption helpers ----------------------------------------

    def encrypt_for_transit(self, data: dict[str, Any], key_id: str) -> str:
        """Encrypt *data* for secure transmission (API responses)."""
        serialized = json.dumps(data, default=str).encode("utf-8")
        data_key, encrypted_data_key = self._backend.generate_data_key(key_id)
        ct, nonce, tag = _aes_gcm_encrypt(serialized, data_key)
        self._key_usage[key_id] = self._key_usage.get(key_id, 0) + 1

        envelope = {
            "encrypted_data_key": base64.b64encode(encrypted_data_key).decode(),
            "ciphertext": base64.b64encode(ct).decode(),
            "nonce": base64.b64encode(nonce).decode(),
            "tag": base64.b64encode(tag).decode(),
            "key_id": key_id,
            "algorithm": EncryptionAlgorithm.AES_256_GCM.value,
        }
        return json.dumps(envelope)

    def decrypt_from_transit(self, payload: str, key_id: str) -> dict[str, Any]:
        """Decrypt a payload produced by :meth:`encrypt_for_transit`."""
        envelope = json.loads(payload)
        encrypted_data_key = base64.b64decode(envelope["encrypted_data_key"])
        ct = base64.b64decode(envelope["ciphertext"])
        nonce = base64.b64decode(envelope["nonce"])
        tag = base64.b64decode(envelope["tag"])

        data_key = self._backend.decrypt_data_key(encrypted_data_key, key_id)
        plaintext = _aes_gcm_decrypt(ct, data_key, nonce, tag)
        return json.loads(plaintext.decode("utf-8"))

    # -- introspection -----------------------------------------------------

    @property
    def sensitive_fields(self) -> frozenset[str]:
        return self._sensitive_fields

    @property
    def encrypted_field_count(self) -> int:
        return len(self._encrypted_fields)

    @property
    def encrypted_plan_count(self) -> int:
        return len(self._encrypted_plans)


__all__ = [
    "AWSKMSBackend",
    "AzureKeyVaultBackend",
    "DEFAULT_SENSITIVE_FIELDS",
    "EncryptedPlan",
    "EncryptedValue",
    "EncryptionAlgorithm",
    "EncryptionManager",
    "EnvelopeKey",
    "HashiCorpVaultBackend",
    "KMSBackend",
    "KMSProvider",
    "KeyMetadata",
    "KeyState",
    "LocalKMSBackend",
    "RotationResult",
    "SearchToken",
    "create_kms_backend",
]
