"""Additional FakeKMSClient branch tests."""

from __future__ import annotations

import pytest

from chora_closure_orchestrator.adapter.kms import (
    DEKDeletedError,
    FakeKMSClient,
)

GCID = "01970000-0000-7000-9000-000000000001"
TENANT = "01970000-0000-7000-8000-000000000001"


class TestKMSEdges:
    @pytest.mark.asyncio
    async def test_create_idempotent(self) -> None:
        kms = FakeKMSClient()
        m1 = await kms.create_user_dek(tenant_id=TENANT, gcid=GCID)
        m2 = await kms.create_user_dek(tenant_id=TENANT, gcid=GCID)
        assert m1.dek_resource_name == m2.dek_resource_name

    @pytest.mark.asyncio
    async def test_encrypt_without_dek_raises(self) -> None:
        kms = FakeKMSClient()
        with pytest.raises(DEKDeletedError):
            await kms.encrypt(tenant_id=TENANT, gcid=GCID, plaintext=b"x")

    @pytest.mark.asyncio
    async def test_decrypt_without_dek_raises(self) -> None:
        kms = FakeKMSClient()
        with pytest.raises(DEKDeletedError):
            await kms.decrypt(tenant_id=TENANT, gcid=GCID, ciphertext=b"x")

    @pytest.mark.asyncio
    async def test_delete_nonexistent_returns_op_id(self) -> None:
        kms = FakeKMSClient()
        op = await kms.delete_user_dek(tenant_id=TENANT, gcid="nonexistent")
        # Idempotent — returns op_id even when no DEK existed
        assert op != ""
