"""Tests for data encryption module."""

import json
import os

from blueprint.security.data_encryption import (
    DEFAULT_SENSITIVE_FIELDS,
    AWSKMSBackend,
    AzureKeyVaultBackend,
    EncryptedPlan,
    EncryptedValue,
    EncryptionAlgorithm,
    EncryptionManager,
    HashiCorpVaultBackend,
    KMSProvider,
    KeyMetadata,
    KeyState,
    LocalKMSBackend,
    RotationResult,
    SearchToken,
    create_kms_backend,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _master_key() -> bytes:
    """Return a deterministic 32-byte key for testing."""
    return b"test-master-key-32bytes-padding!"


def _sample_plan() -> dict:
    return {
        "id": "plan-001",
        "implementation_brief_id": "ib-001",
        "target_engine": "relay",
        "target_repo": "acme/widgets",
        "project_type": "web",
        "milestones": [{"name": "Sprint 1"}],
        "test_strategy": "pytest",
        "handoff_prompt": "Deploy the service to production",
        "status": "in_progress",
        "email": "user@example.com",
        "comments": "This is a sensitive comment about the project.",
        "description": "Build OAuth2 login flow for widget platform",
        "tasks": [
            {
                "id": "task-1",
                "title": "Implement auth",
                "description": "Build OAuth2 login",
                "status": "completed",
            },
        ],
    }


def _make_manager(
    provider: KMSProvider = KMSProvider.LOCAL,
    config: dict | None = None,
) -> tuple[EncryptionManager, str]:
    """Create an EncryptionManager with one registered key, return (manager, key_id)."""
    mgr = EncryptionManager(kms_provider=provider, kms_config=config)
    key_id = "test-key-001"
    mgr.register_key(key_id, _master_key())
    return mgr, key_id


# ---------------------------------------------------------------------------
# Field encryption tests
# ---------------------------------------------------------------------------


class TestFieldEncryption:

    def test_encrypt_decrypt_roundtrip(self):
        mgr, key_id = _make_manager()
        original = "user@example.com"
        encrypted = mgr.encrypt_field(original, key_id)

        assert isinstance(encrypted, EncryptedValue)
        assert encrypted.key_id == key_id
        assert encrypted.algorithm == EncryptionAlgorithm.AES_256_GCM.value
        assert encrypted.ciphertext != original
        assert encrypted.encrypted_at

        decrypted = mgr.decrypt_field(encrypted, key_id)
        assert decrypted == original

    def test_encrypt_empty_string(self):
        mgr, key_id = _make_manager()
        encrypted = mgr.encrypt_field("", key_id)
        decrypted = mgr.decrypt_field(encrypted, key_id)
        assert decrypted == ""

    def test_encrypt_unicode(self):
        mgr, key_id = _make_manager()
        original = "Hello world with unicode chars"
        encrypted = mgr.encrypt_field(original, key_id)
        decrypted = mgr.decrypt_field(encrypted, key_id)
        assert decrypted == original

    def test_encrypt_large_value(self):
        mgr, key_id = _make_manager()
        original = "A" * 10_000
        encrypted = mgr.encrypt_field(original, key_id)
        decrypted = mgr.decrypt_field(encrypted, key_id)
        assert decrypted == original

    def test_encrypted_value_serialization(self):
        mgr, key_id = _make_manager()
        encrypted = mgr.encrypt_field("test value", key_id)
        data = encrypted.to_dict()

        assert "ciphertext" in data
        assert "nonce" in data
        assert "tag" in data
        assert data["key_id"] == key_id

        restored = EncryptedValue.from_dict(data)
        assert restored.ciphertext == encrypted.ciphertext
        assert restored.nonce == encrypted.nonce
        assert restored.tag == encrypted.tag

    def test_encrypted_value_json(self):
        mgr, key_id = _make_manager()
        encrypted = mgr.encrypt_field("test", key_id)
        json_str = encrypted.to_json()
        parsed = json.loads(json_str)
        assert parsed["key_id"] == key_id

    def test_encrypt_with_unknown_key_raises(self):
        mgr, _ = _make_manager()
        try:
            mgr.encrypt_field("value", "nonexistent-key")
            assert False, "Should have raised KeyError"
        except KeyError:
            pass

    def test_encrypt_with_retired_key_raises(self):
        mgr, key_id = _make_manager()
        mgr.retire_key(key_id)
        try:
            mgr.encrypt_field("value", key_id)
            assert False, "Should have raised ValueError"
        except ValueError as exc:
            assert "retired" in str(exc)

    def test_decrypt_with_tampered_ciphertext_raises(self):
        mgr, key_id = _make_manager()
        encrypted = mgr.encrypt_field("secret", key_id)
        # Tamper with the ciphertext by changing a character
        tampered = EncryptedValue(
            ciphertext=encrypted.ciphertext[:-5] + "XXXXX",
            nonce=encrypted.nonce,
            tag=encrypted.tag,
            key_id=encrypted.key_id,
        )
        try:
            mgr.decrypt_field(tampered, key_id)
            assert False, "Should have raised"
        except (ValueError, Exception):
            pass

    def test_different_encryptions_produce_different_ciphertexts(self):
        mgr, key_id = _make_manager()
        enc1 = mgr.encrypt_field("same value", key_id)
        enc2 = mgr.encrypt_field("same value", key_id)
        # Non-deterministic encryption should produce different ciphertexts
        assert enc1.ciphertext != enc2.ciphertext

    def test_usage_count_increments(self):
        mgr, key_id = _make_manager()
        mgr.encrypt_field("a", key_id)
        mgr.encrypt_field("b", key_id)
        meta = mgr.manage_key_lifecycle(key_id)
        assert meta.usage_count == 2


# ---------------------------------------------------------------------------
# Plan encryption tests
# ---------------------------------------------------------------------------


class TestPlanEncryption:

    def test_full_plan_encrypt_decrypt(self):
        mgr, key_id = _make_manager()
        plan = _sample_plan()
        encrypted = mgr.encrypt_plan(plan, key_id)

        assert isinstance(encrypted, EncryptedPlan)
        assert encrypted.plan_id == "plan-001"
        assert encrypted.key_id == key_id
        assert encrypted.encrypted_at

        decrypted = mgr.decrypt_plan(encrypted, key_id)
        assert decrypted["id"] == plan["id"]
        assert decrypted["email"] == plan["email"]
        assert decrypted["tasks"] == plan["tasks"]

    def test_full_plan_encryption_hides_all_data(self):
        mgr, key_id = _make_manager()
        plan = _sample_plan()
        encrypted = mgr.encrypt_plan(plan, key_id)

        # The encrypted_data should not contain plaintext
        assert "user@example.com" not in encrypted.encrypted_data
        assert "Build OAuth2" not in encrypted.encrypted_data

    def test_plan_to_dict(self):
        mgr, key_id = _make_manager()
        encrypted = mgr.encrypt_plan(_sample_plan(), key_id)
        data = encrypted.to_dict()
        assert data["plan_id"] == "plan-001"
        assert "encrypted_data" in data
        assert "nonce" in data

    def test_encrypt_plan_fields_only(self):
        mgr, key_id = _make_manager()
        plan = _sample_plan()
        encrypted = mgr.encrypt_plan(plan, key_id, fields_only=True)

        assert encrypted.plan_id == "plan-001"
        # encrypted_fields should list only the sensitive ones present
        for f in encrypted.encrypted_fields:
            assert f in mgr.sensitive_fields

    def test_encrypt_plan_with_unknown_key_raises(self):
        mgr, _ = _make_manager()
        try:
            mgr.encrypt_plan(_sample_plan(), "bad-key")
            assert False, "Should have raised KeyError"
        except KeyError:
            pass


# ---------------------------------------------------------------------------
# Sensitive field encryption in-place
# ---------------------------------------------------------------------------


class TestSensitiveFieldEncryption:

    def test_encrypt_sensitive_fields_in_place(self):
        mgr, key_id = _make_manager()
        plan = _sample_plan()
        encrypted_plan = mgr.encrypt_plan_sensitive_fields(plan, key_id)

        # email, comments, description, handoff_prompt are sensitive
        for field_name in ("email", "comments", "description", "handoff_prompt"):
            assert isinstance(encrypted_plan[field_name], dict)
            assert "ciphertext" in encrypted_plan[field_name]

        # Non-sensitive fields should be unchanged
        assert encrypted_plan["id"] == "plan-001"
        assert encrypted_plan["target_engine"] == "relay"

    def test_decrypt_sensitive_fields_roundtrip(self):
        mgr, key_id = _make_manager()
        plan = _sample_plan()
        encrypted_plan = mgr.encrypt_plan_sensitive_fields(plan, key_id)
        decrypted_plan = mgr.decrypt_plan_sensitive_fields(encrypted_plan, key_id)

        assert decrypted_plan["email"] == "user@example.com"
        assert decrypted_plan["comments"] == plan["comments"]
        assert decrypted_plan["description"] == plan["description"]
        assert decrypted_plan["handoff_prompt"] == plan["handoff_prompt"]

    def test_default_sensitive_fields(self):
        assert "email" in DEFAULT_SENSITIVE_FIELDS
        assert "comments" in DEFAULT_SENSITIVE_FIELDS
        assert "attachments" in DEFAULT_SENSITIVE_FIELDS
        assert "custom_sensitive" in DEFAULT_SENSITIVE_FIELDS

    def test_custom_sensitive_fields(self):
        mgr = EncryptionManager(
            sensitive_fields=frozenset({"custom_field"}),
        )
        key_id = "k1"
        mgr.register_key(key_id, _master_key())

        plan = {"id": "p1", "custom_field": "secret", "email": "visible@example.com"}
        encrypted = mgr.encrypt_plan_sensitive_fields(plan, key_id)

        # custom_field should be encrypted
        assert isinstance(encrypted["custom_field"], dict)
        # email is NOT in custom fields set, so stays plaintext
        assert encrypted["email"] == "visible@example.com"


# ---------------------------------------------------------------------------
# Key rotation tests
# ---------------------------------------------------------------------------


class TestKeyRotation:

    def test_rotate_field_keys(self):
        mgr, old_key = _make_manager()
        new_key = "test-key-002"
        mgr.register_key(new_key, os.urandom(32))

        # Encrypt some fields with the old key
        mgr.encrypt_field("value-1", old_key)
        mgr.encrypt_field("value-2", old_key)

        result = mgr.rotate_encryption_keys(old_key, new_key)
        assert isinstance(result, RotationResult)
        assert result.success
        assert result.fields_rotated == 2
        assert result.old_key_id == old_key
        assert result.new_key_id == new_key

    def test_rotate_plan_keys(self):
        mgr, old_key = _make_manager()
        new_key = "test-key-002"
        mgr.register_key(new_key, os.urandom(32))

        mgr.encrypt_plan(_sample_plan(), old_key)

        result = mgr.rotate_encryption_keys(old_key, new_key)
        assert result.success
        assert result.plans_rotated == 1

    def test_old_key_retired_after_rotation(self):
        mgr, old_key = _make_manager()
        new_key = "test-key-002"
        mgr.register_key(new_key, os.urandom(32))

        mgr.encrypt_field("data", old_key)
        mgr.rotate_encryption_keys(old_key, new_key)

        meta = mgr.manage_key_lifecycle(old_key)
        assert meta.state == KeyState.RETIRED

    def test_rotation_result_serialization(self):
        mgr, old_key = _make_manager()
        new_key = "test-key-002"
        mgr.register_key(new_key, os.urandom(32))
        mgr.encrypt_field("data", old_key)

        result = mgr.rotate_encryption_keys(old_key, new_key)
        data = result.to_dict()
        assert data["success"] is True
        assert data["fields_rotated"] == 1
        assert "started_at" in data
        assert "completed_at" in data

    def test_data_accessible_after_rotation(self):
        mgr, old_key = _make_manager()
        new_key = "test-key-002"
        mgr.register_key(new_key, os.urandom(32))

        # Encrypt with old key
        plan = _sample_plan()
        mgr.encrypt_plan(plan, old_key)

        # Rotate
        result = mgr.rotate_encryption_keys(old_key, new_key)
        assert result.plans_rotated == 1
        assert result.success


# ---------------------------------------------------------------------------
# KMS backend tests
# ---------------------------------------------------------------------------


class TestKMSBackends:

    def test_local_backend(self):
        backend = LocalKMSBackend()
        backend.register_key("k1", _master_key())

        data_key, encrypted = backend.generate_data_key("k1")
        assert len(data_key) == 32
        assert len(encrypted) > 0

        recovered = backend.decrypt_data_key(encrypted, "k1")
        assert recovered == data_key

    def test_local_backend_unknown_key(self):
        backend = LocalKMSBackend()
        try:
            backend.generate_data_key("nonexistent")
            assert False, "Should have raised"
        except KeyError:
            pass

    def test_aws_kms_backend(self):
        backend = AWSKMSBackend(config={"region": "us-west-2"})
        backend.register_key("aws-key", _master_key())

        data_key, encrypted = backend.generate_data_key("aws-key")
        recovered = backend.decrypt_data_key(encrypted, "aws-key")
        assert recovered == data_key

        meta = backend.get_key_metadata("aws-key")
        assert meta["provider"] == KMSProvider.AWS_KMS.value
        assert meta["region"] == "us-west-2"

    def test_azure_key_vault_backend(self):
        backend = AzureKeyVaultBackend(
            config={"vault_url": "https://myvault.vault.azure.net"}
        )
        backend.register_key("az-key", _master_key())

        data_key, encrypted = backend.generate_data_key("az-key")
        recovered = backend.decrypt_data_key(encrypted, "az-key")
        assert recovered == data_key

        meta = backend.get_key_metadata("az-key")
        assert meta["provider"] == KMSProvider.AZURE_KEY_VAULT.value
        assert meta["vault_url"] == "https://myvault.vault.azure.net"

    def test_hashicorp_vault_backend(self):
        backend = HashiCorpVaultBackend(
            config={"mount_path": "transit/"}
        )
        backend.register_key("hc-key", _master_key())

        data_key, encrypted = backend.generate_data_key("hc-key")
        recovered = backend.decrypt_data_key(encrypted, "hc-key")
        assert recovered == data_key

        meta = backend.get_key_metadata("hc-key")
        assert meta["provider"] == KMSProvider.HASHICORP_VAULT.value
        assert meta["mount_path"] == "transit/"

    def test_create_kms_backend_factory(self):
        for provider in KMSProvider:
            backend = create_kms_backend(provider)
            assert backend.provider == provider

    def test_create_kms_backend_invalid(self):
        try:
            create_kms_backend("invalid_provider")  # type: ignore[arg-type]
            assert False, "Should have raised"
        except ValueError:
            pass

    def test_manager_with_aws_kms(self):
        mgr, key_id = _make_manager(
            provider=KMSProvider.AWS_KMS,
            config={"region": "eu-west-1"},
        )
        encrypted = mgr.encrypt_field("aws-secret", key_id)
        decrypted = mgr.decrypt_field(encrypted, key_id)
        assert decrypted == "aws-secret"

    def test_manager_with_azure(self):
        mgr, key_id = _make_manager(
            provider=KMSProvider.AZURE_KEY_VAULT,
            config={"vault_url": "https://test.vault.azure.net"},
        )
        encrypted = mgr.encrypt_field("azure-secret", key_id)
        decrypted = mgr.decrypt_field(encrypted, key_id)
        assert decrypted == "azure-secret"

    def test_manager_with_hashicorp(self):
        mgr, key_id = _make_manager(
            provider=KMSProvider.HASHICORP_VAULT,
            config={"mount_path": "secret/data/"},
        )
        encrypted = mgr.encrypt_field("vault-secret", key_id)
        decrypted = mgr.decrypt_field(encrypted, key_id)
        assert decrypted == "vault-secret"


# ---------------------------------------------------------------------------
# Key lifecycle tests
# ---------------------------------------------------------------------------


class TestKeyLifecycle:

    def test_register_key(self):
        mgr = EncryptionManager()
        meta = mgr.register_key("k1", _master_key())

        assert isinstance(meta, KeyMetadata)
        assert meta.key_id == "k1"
        assert meta.state == KeyState.ACTIVE
        assert meta.algorithm == EncryptionAlgorithm.AES_256_GCM.value
        assert meta.created_at

    def test_manage_key_lifecycle(self):
        mgr, key_id = _make_manager()
        meta = mgr.manage_key_lifecycle(key_id)
        assert meta.key_id == key_id
        assert meta.state == KeyState.ACTIVE
        assert meta.usage_count == 0

    def test_manage_key_lifecycle_unknown_key(self):
        mgr, _ = _make_manager()
        try:
            mgr.manage_key_lifecycle("nonexistent")
            assert False, "Should have raised"
        except KeyError:
            pass

    def test_retire_key(self):
        mgr, key_id = _make_manager()
        retired = mgr.retire_key(key_id)
        assert retired.state == KeyState.RETIRED
        assert retired.rotated_at  # timestamp set on retirement

    def test_key_metadata_serialization(self):
        mgr, key_id = _make_manager()
        meta = mgr.manage_key_lifecycle(key_id)
        data = meta.to_dict()
        assert data["key_id"] == key_id
        assert data["state"] == "active"
        assert "created_at" in data

    def test_register_key_with_expiry(self):
        mgr = EncryptionManager()
        meta = mgr.register_key("k1", _master_key(), expires_at="2026-12-31T00:00:00Z")
        assert meta.expires_at == "2026-12-31T00:00:00Z"


# ---------------------------------------------------------------------------
# Encrypted search tests
# ---------------------------------------------------------------------------


class TestEncryptedSearch:

    def test_generate_search_token(self):
        mgr, key_id = _make_manager()
        token = mgr.generate_search_token("user@example.com", "email", key_id)

        assert isinstance(token, SearchToken)
        assert token.field_name == "email"
        assert token.key_id == key_id
        assert len(token.token) > 0

    def test_deterministic_tokens(self):
        mgr, key_id = _make_manager()
        token1 = mgr.generate_search_token("user@example.com", "email", key_id)
        token2 = mgr.generate_search_token("user@example.com", "email", key_id)
        # Same input should produce same token
        assert token1.token == token2.token

    def test_different_values_different_tokens(self):
        mgr, key_id = _make_manager()
        t1 = mgr.generate_search_token("alice@example.com", "email", key_id)
        t2 = mgr.generate_search_token("bob@example.com", "email", key_id)
        assert t1.token != t2.token

    def test_search_matches(self):
        mgr, key_id = _make_manager()
        token = mgr.generate_search_token("user@example.com", "email", key_id)
        assert mgr.search_matches(token, "user@example.com", "email", key_id)
        assert not mgr.search_matches(token, "other@example.com", "email", key_id)

    def test_search_token_serialization(self):
        mgr, key_id = _make_manager()
        token = mgr.generate_search_token("test", "field", key_id)
        data = token.to_dict()
        assert data["field_name"] == "field"
        assert data["key_id"] == key_id
        assert "token" in data


# ---------------------------------------------------------------------------
# Transit encryption tests
# ---------------------------------------------------------------------------


class TestTransitEncryption:

    def test_encrypt_decrypt_for_transit(self):
        mgr, key_id = _make_manager()
        data = {"plan_id": "plan-001", "email": "user@example.com", "status": "active"}

        payload = mgr.encrypt_for_transit(data, key_id)
        assert isinstance(payload, str)

        # Payload should be valid JSON envelope
        envelope = json.loads(payload)
        assert "encrypted_data_key" in envelope
        assert "ciphertext" in envelope
        assert "nonce" in envelope
        assert "tag" in envelope
        assert envelope["key_id"] == key_id

        # Plaintext should not appear
        assert "user@example.com" not in payload

        decrypted = mgr.decrypt_from_transit(payload, key_id)
        assert decrypted == data

    def test_transit_roundtrip_with_nested_data(self):
        mgr, key_id = _make_manager()
        data = {
            "plan": _sample_plan(),
            "metadata": {"exported_by": "system", "timestamp": "2026-01-01"},
        }
        payload = mgr.encrypt_for_transit(data, key_id)
        decrypted = mgr.decrypt_from_transit(payload, key_id)
        assert decrypted["plan"]["id"] == "plan-001"
        assert decrypted["metadata"]["exported_by"] == "system"


# ---------------------------------------------------------------------------
# Envelope encryption tests
# ---------------------------------------------------------------------------


class TestEnvelopeEncryption:

    def test_envelope_pattern(self):
        """Verify that data is encrypted with a data key, and the data key
        is encrypted with the master key (envelope encryption)."""
        mgr, key_id = _make_manager()
        encrypted = mgr.encrypt_field("envelope-test", key_id)

        # The ciphertext contains two parts: encrypted_data_key:encrypted_value
        parts = encrypted.ciphertext.split(":")
        assert len(parts) == 2
        assert len(parts[0]) > 0  # encrypted data key
        assert len(parts[1]) > 0  # encrypted value

    def test_each_encryption_uses_unique_data_key(self):
        mgr, key_id = _make_manager()
        enc1 = mgr.encrypt_field("same", key_id)
        enc2 = mgr.encrypt_field("same", key_id)

        # Encrypted data keys should differ
        edk1 = enc1.ciphertext.split(":")[0]
        edk2 = enc2.ciphertext.split(":")[0]
        assert edk1 != edk2


# ---------------------------------------------------------------------------
# Introspection / counts
# ---------------------------------------------------------------------------


class TestIntrospection:

    def test_encrypted_counts(self):
        mgr, key_id = _make_manager()
        assert mgr.encrypted_field_count == 0
        assert mgr.encrypted_plan_count == 0

        mgr.encrypt_field("a", key_id)
        mgr.encrypt_field("b", key_id)
        assert mgr.encrypted_field_count == 2

        mgr.encrypt_plan(_sample_plan(), key_id)
        assert mgr.encrypted_plan_count == 1

    def test_sensitive_fields_property(self):
        mgr, _ = _make_manager()
        assert mgr.sensitive_fields == DEFAULT_SENSITIVE_FIELDS
