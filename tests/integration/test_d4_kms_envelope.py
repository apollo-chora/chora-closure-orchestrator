"""D4 gate — envelope encryption + crypto-shred lifecycle.

Per ADR-145 D4 gate (GDPR shred / CMEK) + ``feedback_d6_resilience_first_class``
+ ``feedback_no_inline_config``: the LocalKMSClient must implement
envelope encryption with per-user DEK wrapped by the master KEK
(``CHORA_LOCAL_KEK`` env), and crypto-shred semantics must render prior
ciphertexts unrecoverable.

This file covers:

* **Unit tests** (always run) — KMSClient adapter behaviours with an
  explicit 32-byte test KEK. Validates: AAD binding, envelope encryption
  shape, crypto-shred idempotency, DEKDeletedError after shred.

* **Live local round-trip** (always run) — exercises the full envelope
  lifecycle against the env-configured KEK when ``CHORA_LOCAL_KEK`` is
  set: create DEK → encrypt PII payload → decrypt to recover plaintext →
  crypto-shred → verify subsequent decrypt fails.
"""

from __future__ import annotations

import base64
import os

import pytest
from cryptography.exceptions import InvalidTag

from chora_closure_orchestrator.adapter.kms import (
    LOCAL_KEK_ENV,
    DEKDeletedError,
    LocalKMSClient,
    kek_from_env,
)

# Fixture identifiers — keep deterministic for trace correlation
TENANT_A = "01970000-0000-7000-8000-000000000001"
TENANT_B = "01970000-0000-7000-8000-000000000002"
GCID_X = "01970000-0000-7000-9000-00000000000a"
GCID_Y = "01970000-0000-7000-9000-00000000000b"

TEST_KEK = b"\x42" * 32
TEST_KEK_B64 = base64.b64encode(TEST_KEK).decode("ascii")


# -----------------------------------------------------------------------------
# Env-var discipline
# -----------------------------------------------------------------------------


def test_kek_from_env_raises_when_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(LOCAL_KEK_ENV, raising=False)
    with pytest.raises(RuntimeError) as exc:
        kek_from_env()
    assert LOCAL_KEK_ENV in str(exc.value)
    assert "feedback_no_inline_config" in str(exc.value)


def test_kek_from_env_passes_through(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(LOCAL_KEK_ENV, TEST_KEK_B64)
    assert kek_from_env() == TEST_KEK


def test_kek_from_env_rejects_wrong_length(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(LOCAL_KEK_ENV, base64.b64encode(b"\x01" * 24).decode("ascii"))
    with pytest.raises(RuntimeError, match="AES-256"):
        kek_from_env()


# -----------------------------------------------------------------------------
# Local-KMS unit tests
# -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_d4_create_user_dek_wraps_with_kek() -> None:
    """create_user_dek wraps a fresh 32-byte DEK under the master KEK +
    tenant/gcid AAD."""
    client = LocalKMSClient(kek=TEST_KEK)

    meta = await client.create_user_dek(tenant_id=TENANT_A, gcid=GCID_X)

    # DEK is 32 random bytes — the wrapped bundle is nonce(12) + tag(16) + 32
    persisted = await client.store.get(tenant_id=TENANT_A, gcid=GCID_X)
    assert persisted is not None
    assert len(persisted.wrapped_dek) == 12 + 16 + 32
    assert meta.tenant_id == TENANT_A
    assert meta.gcid == GCID_X
    assert meta.deleted is False


@pytest.mark.asyncio
async def test_d4_create_user_dek_is_idempotent() -> None:
    """Second create with same (tenant, gcid) returns existing without re-wrapping."""
    client = LocalKMSClient(kek=TEST_KEK)

    meta_a = await client.create_user_dek(tenant_id=TENANT_A, gcid=GCID_X)
    wrapped_rec_a = await client.store.get(tenant_id=TENANT_A, gcid=GCID_X)
    assert wrapped_rec_a is not None
    wrapped_a = wrapped_rec_a.wrapped_dek
    meta_b = await client.create_user_dek(tenant_id=TENANT_A, gcid=GCID_X)
    wrapped_rec_b = await client.store.get(tenant_id=TENANT_A, gcid=GCID_X)
    assert wrapped_rec_b is not None
    wrapped_b = wrapped_rec_b.wrapped_dek
    assert meta_a.dek_resource_name == meta_b.dek_resource_name
    # Only ONE wrap — the second create returned the cached entry
    assert wrapped_a == wrapped_b


@pytest.mark.asyncio
async def test_d4_encrypt_decrypt_round_trip() -> None:
    """encrypt + decrypt with the same DEK recovers the plaintext."""
    client = LocalKMSClient(kek=TEST_KEK)
    await client.create_user_dek(tenant_id=TENANT_A, gcid=GCID_X)

    plaintext = b"chora-closure-d4-roundtrip-payload-with-PII"
    ciphertext = await client.encrypt(tenant_id=TENANT_A, gcid=GCID_X, plaintext=plaintext)
    # Ciphertext bundle is nonce (12B) + AESGCM body (≥16B tag)
    assert len(ciphertext) >= 12 + 16 + len(plaintext)
    assert ciphertext != plaintext  # actually encrypted

    recovered = await client.decrypt(tenant_id=TENANT_A, gcid=GCID_X, ciphertext=ciphertext)
    assert recovered == plaintext


@pytest.mark.asyncio
async def test_d4_encrypt_fails_when_dek_not_provisioned() -> None:
    """Calling encrypt without first creating a DEK raises DEKDeletedError."""
    client = LocalKMSClient(kek=TEST_KEK)
    with pytest.raises(DEKDeletedError):
        await client.encrypt(tenant_id=TENANT_A, gcid=GCID_X, plaintext=b"x")


@pytest.mark.asyncio
async def test_d4_crypto_shred_makes_ciphertext_unrecoverable() -> None:
    """The D4 invariant — delete the wrapped DEK, prior ciphertext is gone.

    This is the load-bearing test for the GDPR shred gate.
    """
    client = LocalKMSClient(kek=TEST_KEK)
    await client.create_user_dek(tenant_id=TENANT_A, gcid=GCID_X)

    plaintext = b"PII-payload-that-must-become-unrecoverable"
    ciphertext = await client.encrypt(tenant_id=TENANT_A, gcid=GCID_X, plaintext=plaintext)

    # Pre-shred: round-trip works
    assert await client.decrypt(tenant_id=TENANT_A, gcid=GCID_X, ciphertext=ciphertext) == plaintext

    # Crypto-shred
    op_id = await client.delete_user_dek(tenant_id=TENANT_A, gcid=GCID_X)
    assert op_id.startswith("shred-")

    # Post-shred: decrypt fails. The DEK is gone; the ciphertext is opaque.
    with pytest.raises(DEKDeletedError) as exc:
        await client.decrypt(tenant_id=TENANT_A, gcid=GCID_X, ciphertext=ciphertext)
    assert "crypto-shredded" in str(exc.value).lower() or "UNRECOVERABLE" in str(exc.value)

    # Also can't re-encrypt — DEK is gone
    with pytest.raises(DEKDeletedError):
        await client.encrypt(tenant_id=TENANT_A, gcid=GCID_X, plaintext=b"new")


@pytest.mark.asyncio
async def test_d4_crypto_shred_is_idempotent() -> None:
    """Second shred call returns a fresh op_id without raising."""
    client = LocalKMSClient(kek=TEST_KEK)
    await client.create_user_dek(tenant_id=TENANT_A, gcid=GCID_X)

    op_a = await client.delete_user_dek(tenant_id=TENANT_A, gcid=GCID_X)
    op_b = await client.delete_user_dek(tenant_id=TENANT_A, gcid=GCID_X)
    assert op_a != op_b  # fresh op_id each call
    assert op_a.startswith("shred-")
    assert op_b.startswith("shred-")


@pytest.mark.asyncio
async def test_d4_crypto_shred_for_unknown_user_is_idempotent() -> None:
    """Shredding a DEK that was never created returns op_id without error."""
    client = LocalKMSClient(kek=TEST_KEK)
    op = await client.delete_user_dek(tenant_id=TENANT_A, gcid="ghost-gcid")
    assert op.startswith("shred-")


@pytest.mark.asyncio
async def test_d4_per_user_dek_isolation() -> None:
    """User X's ciphertext cannot be decrypted as user Y (AAD binding).

    Per-user DEK isolation is what makes individual-user GDPR shred valid
    in a multi-user tenant — closing user X does not affect user Y.
    """
    client = LocalKMSClient(kek=TEST_KEK)
    await client.create_user_dek(tenant_id=TENANT_A, gcid=GCID_X)
    await client.create_user_dek(tenant_id=TENANT_A, gcid=GCID_Y)

    plaintext = b"user-X-data"
    ct_x = await client.encrypt(tenant_id=TENANT_A, gcid=GCID_X, plaintext=plaintext)

    # X's own decrypt works
    assert await client.decrypt(tenant_id=TENANT_A, gcid=GCID_X, ciphertext=ct_x) == plaintext

    # Y CANNOT decrypt X's ciphertext (different DEK + AAD)
    with pytest.raises(InvalidTag):
        await client.decrypt(tenant_id=TENANT_A, gcid=GCID_Y, ciphertext=ct_x)

    # Shredding X does NOT affect Y
    await client.delete_user_dek(tenant_id=TENANT_A, gcid=GCID_X)
    plaintext_y = b"user-Y-data-untouched"
    ct_y = await client.encrypt(tenant_id=TENANT_A, gcid=GCID_Y, plaintext=plaintext_y)
    assert await client.decrypt(tenant_id=TENANT_A, gcid=GCID_Y, ciphertext=ct_y) == plaintext_y


@pytest.mark.asyncio
async def test_d4_per_tenant_dek_isolation() -> None:
    """Same gcid across two tenants gets distinct DEKs.

    Defense against tenant-id mix-ups: even if the same GCID literal
    appears under two tenants (it shouldn't per the GCID design, but
    defense-in-depth), the encrypted state is isolated.
    """
    client = LocalKMSClient(kek=TEST_KEK)
    await client.create_user_dek(tenant_id=TENANT_A, gcid=GCID_X)
    await client.create_user_dek(tenant_id=TENANT_B, gcid=GCID_X)

    pt = b"shared-gcid-different-tenant"
    ct_a = await client.encrypt(tenant_id=TENANT_A, gcid=GCID_X, plaintext=pt)

    # Different tenant cannot decrypt
    with pytest.raises(InvalidTag):
        await client.decrypt(tenant_id=TENANT_B, gcid=GCID_X, ciphertext=ct_a)


@pytest.mark.asyncio
async def test_d4_decrypt_rejects_short_ciphertext() -> None:
    """Defensive — ciphertext shorter than nonce length is rejected cleanly."""
    client = LocalKMSClient(kek=TEST_KEK)
    await client.create_user_dek(tenant_id=TENANT_A, gcid=GCID_X)
    with pytest.raises(ValueError, match="ciphertext too short"):
        await client.decrypt(tenant_id=TENANT_A, gcid=GCID_X, ciphertext=b"short")


@pytest.mark.asyncio
async def test_d4_get_user_dek_reflects_lifecycle() -> None:
    """get_user_dek returns None pre-create, populated post-create, deleted=True post-shred."""
    client = LocalKMSClient(kek=TEST_KEK)
    assert await client.get_user_dek(tenant_id=TENANT_A, gcid=GCID_X) is None
    await client.create_user_dek(tenant_id=TENANT_A, gcid=GCID_X)
    meta_alive = await client.get_user_dek(tenant_id=TENANT_A, gcid=GCID_X)
    assert meta_alive is not None
    assert meta_alive.deleted is False
    await client.delete_user_dek(tenant_id=TENANT_A, gcid=GCID_X)
    meta_dead = await client.get_user_dek(tenant_id=TENANT_A, gcid=GCID_X)
    assert meta_dead is not None
    assert meta_dead.deleted is True


# -----------------------------------------------------------------------------
# Live local round-trip — gated on CHORA_LOCAL_KEK
# -----------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.skipif(
    not os.environ.get(LOCAL_KEK_ENV),
    reason=(
        f"live local KMS test requires {LOCAL_KEK_ENV} — a base64-encoded "
        "32-byte master KEK injected by the deployment."
    ),
)
@pytest.mark.asyncio
async def test_d4_live_local_kms_round_trip_and_shred() -> None:
    """Env-configured KEK exercise — verifies the adapter end-to-end
    against the deployment's master key.

    Runs the full envelope lifecycle: create DEK → encrypt PII payload →
    decrypt to recover plaintext → crypto-shred → verify subsequent
    decrypt fails.
    """
    client = LocalKMSClient.from_env()
    await client.create_user_dek(tenant_id=TENANT_A, gcid=GCID_X)
    plaintext = b"chora-closure-d4-live-roundtrip-payload"
    ct = await client.encrypt(tenant_id=TENANT_A, gcid=GCID_X, plaintext=plaintext)
    assert await client.decrypt(tenant_id=TENANT_A, gcid=GCID_X, ciphertext=ct) == plaintext

    await client.delete_user_dek(tenant_id=TENANT_A, gcid=GCID_X)
    with pytest.raises(DEKDeletedError):
        await client.decrypt(tenant_id=TENANT_A, gcid=GCID_X, ciphertext=ct)
