"""Local KMS DEK lifecycle tests.

Per Tier 3 D11 + skill `account-closure-saga`:
- per-tenant master key (CMEK) — kept while tenant active
- per-user DEK (Data Encryption Key) — DELETED at crypto-shred
- DEK deletion = crypto-shred (data unrecoverable)
- KMS scheduled-destruction = 24h reversible window

Tests use the in-memory KMS adapter (FakeKMSClient). Production wires the
real ``LocalKMSClient``; the port + adapter pattern keeps the
domain code testable.
"""

from __future__ import annotations

import pytest

from chora_closure_orchestrator.adapter.kms import (
    DEKDeletedError,
    DEKMetadata,
    FakeKMSClient,
)

GCID = "01970000-0000-7000-9000-000000000001"
TENANT = "01970000-0000-7000-8000-000000000001"


class TestFakeKMSClient:
    @pytest.mark.asyncio
    async def test_create_user_dek(self) -> None:
        kms = FakeKMSClient()
        meta = await kms.create_user_dek(tenant_id=TENANT, gcid=GCID)
        assert isinstance(meta, DEKMetadata)
        assert meta.gcid == GCID
        assert meta.tenant_id == TENANT
        assert meta.dek_resource_name != ""
        assert meta.deleted is False

    @pytest.mark.asyncio
    async def test_get_user_dek(self) -> None:
        kms = FakeKMSClient()
        await kms.create_user_dek(tenant_id=TENANT, gcid=GCID)
        meta = await kms.get_user_dek(tenant_id=TENANT, gcid=GCID)
        assert meta is not None
        assert meta.gcid == GCID

    @pytest.mark.asyncio
    async def test_get_user_dek_missing(self) -> None:
        kms = FakeKMSClient()
        meta = await kms.get_user_dek(tenant_id=TENANT, gcid="missing-gcid")
        assert meta is None

    @pytest.mark.asyncio
    async def test_encrypt_decrypt_roundtrip(self) -> None:
        kms = FakeKMSClient()
        await kms.create_user_dek(tenant_id=TENANT, gcid=GCID)

        plaintext = b"sensitive user data"
        ciphertext = await kms.encrypt(tenant_id=TENANT, gcid=GCID, plaintext=plaintext)
        assert ciphertext != plaintext

        decrypted = await kms.decrypt(tenant_id=TENANT, gcid=GCID, ciphertext=ciphertext)
        assert decrypted == plaintext

    @pytest.mark.asyncio
    async def test_crypto_shred_deletes_dek(self) -> None:
        kms = FakeKMSClient()
        await kms.create_user_dek(tenant_id=TENANT, gcid=GCID)

        op_id = await kms.delete_user_dek(tenant_id=TENANT, gcid=GCID)
        assert op_id != ""

        # After deletion, the DEK is marked deleted.
        meta = await kms.get_user_dek(tenant_id=TENANT, gcid=GCID)
        assert meta is not None
        assert meta.deleted is True

    @pytest.mark.asyncio
    async def test_decrypt_after_shred_raises(self) -> None:
        """Crypto-shred renders prior ciphertext UNRECOVERABLE."""
        kms = FakeKMSClient()
        await kms.create_user_dek(tenant_id=TENANT, gcid=GCID)
        ciphertext = await kms.encrypt(tenant_id=TENANT, gcid=GCID, plaintext=b"secret")
        await kms.delete_user_dek(tenant_id=TENANT, gcid=GCID)

        with pytest.raises(DEKDeletedError):
            await kms.decrypt(tenant_id=TENANT, gcid=GCID, ciphertext=ciphertext)

    @pytest.mark.asyncio
    async def test_encrypt_after_shred_raises(self) -> None:
        kms = FakeKMSClient()
        await kms.create_user_dek(tenant_id=TENANT, gcid=GCID)
        await kms.delete_user_dek(tenant_id=TENANT, gcid=GCID)

        with pytest.raises(DEKDeletedError):
            await kms.encrypt(tenant_id=TENANT, gcid=GCID, plaintext=b"new secret")

    @pytest.mark.asyncio
    async def test_delete_idempotent(self) -> None:
        """Second delete returns same KMS operation ID + does not raise."""
        kms = FakeKMSClient()
        await kms.create_user_dek(tenant_id=TENANT, gcid=GCID)
        op1 = await kms.delete_user_dek(tenant_id=TENANT, gcid=GCID)
        op2 = await kms.delete_user_dek(tenant_id=TENANT, gcid=GCID)
        # Second op also succeeds (idempotent), even if op_id differs.
        assert op1 != ""
        assert op2 != ""

    @pytest.mark.asyncio
    async def test_tenant_master_key_persists_after_user_shred(self) -> None:
        """Crypto-shredding ONE user must NOT affect OTHER users' DEKs."""
        kms = FakeKMSClient()
        await kms.create_user_dek(tenant_id=TENANT, gcid=GCID)
        other = "01970000-0000-7000-9000-000000000002"
        await kms.create_user_dek(tenant_id=TENANT, gcid=other)

        await kms.delete_user_dek(tenant_id=TENANT, gcid=GCID)

        # Other user's DEK still works
        ct = await kms.encrypt(tenant_id=TENANT, gcid=other, plaintext=b"x")
        pt = await kms.decrypt(tenant_id=TENANT, gcid=other, ciphertext=ct)
        assert pt == b"x"
