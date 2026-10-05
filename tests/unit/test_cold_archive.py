"""Cold-archive pipeline tests.

Cold archive + per-user DEK encryption + multi-jurisdiction retention.
Object naming: ``{tenant_id}/{gcid}/{closure_id}.tar.gz.enc`` per spec.
"""

from __future__ import annotations

import pytest

from chora_closure_orchestrator.adapter.coldarchive import (
    ColdArchiveJobSpec,
    ColdArchiveResult,
    InMemoryColdArchiveClient,
)

GCID = "01970000-0000-7000-9000-000000000001"
TENANT = "01970000-0000-7000-8000-000000000001"
SAGA_ID = "saga-12345"


class TestInMemoryColdArchiveClient:
    @pytest.mark.asyncio
    async def test_archive_writes_object(self) -> None:
        client = InMemoryColdArchiveClient(bucket="chora-cold-archive-dev")
        spec = ColdArchiveJobSpec(
            saga_id=SAGA_ID,
            gcid=GCID,
            tenant_id=TENANT,
            jurisdiction="SG",
            payload=b"raw user data bundle",
            dek_resource_name="kms-dek-12345",
        )
        result = await client.archive(spec)
        assert isinstance(result, ColdArchiveResult)
        assert result.success is True
        # Naming convention: {tenant_id}/{gcid}/{closure_id}.tar.gz.enc
        expected_object = f"{TENANT}/{GCID}/{SAGA_ID}.tar.gz.enc"
        assert result.object_name == expected_object
        assert result.gcs_uri == (f"gs://chora-cold-archive-dev/{expected_object}")

    @pytest.mark.asyncio
    async def test_archive_recorded(self) -> None:
        client = InMemoryColdArchiveClient(bucket="chora-cold-archive-dev")
        spec = ColdArchiveJobSpec(
            saga_id=SAGA_ID,
            gcid=GCID,
            tenant_id=TENANT,
            jurisdiction="SG",
            payload=b"bundle",
            dek_resource_name="kms-dek",
        )
        await client.archive(spec)

        archived = client.list_archives()
        assert len(archived) == 1
        assert archived[0].object_name.startswith(f"{TENANT}/{GCID}/")

    @pytest.mark.asyncio
    async def test_jurisdiction_default_retention_sg(self) -> None:
        from chora_closure_orchestrator.adapter.coldarchive import (
            jurisdictional_retention_days,
        )

        # Per skill table: PDPA SG = 7y = 2557 days
        assert jurisdictional_retention_days("SG") == 2557

    @pytest.mark.asyncio
    async def test_jurisdiction_default_retention_eu(self) -> None:
        from chora_closure_orchestrator.adapter.coldarchive import (
            jurisdictional_retention_days,
        )

        # GDPR EU = 7y = 2557 days
        assert jurisdictional_retention_days("EU") == 2557

    @pytest.mark.asyncio
    async def test_jurisdiction_default_retention_us(self) -> None:
        from chora_closure_orchestrator.adapter.coldarchive import (
            jurisdictional_retention_days,
        )

        # CCPA US = 5y = 1826 days
        assert jurisdictional_retention_days("US") == 1826

    @pytest.mark.asyncio
    async def test_jurisdiction_unknown_uses_default(self) -> None:
        from chora_closure_orchestrator.adapter.coldarchive import (
            jurisdictional_retention_days,
        )

        # Unknown → default (2557 days, the SG/EU floor)
        assert jurisdictional_retention_days("XX") == 2557

    @pytest.mark.asyncio
    async def test_archive_with_empty_payload_raises(self) -> None:
        client = InMemoryColdArchiveClient(bucket="chora-cold-archive-dev")
        spec = ColdArchiveJobSpec(
            saga_id=SAGA_ID,
            gcid=GCID,
            tenant_id=TENANT,
            jurisdiction="SG",
            payload=b"",
            dek_resource_name="kms-dek",
        )
        with pytest.raises(ValueError):
            await client.archive(spec)

    @pytest.mark.asyncio
    async def test_archive_missing_dek_raises(self) -> None:
        client = InMemoryColdArchiveClient(bucket="chora-cold-archive-dev")
        spec = ColdArchiveJobSpec(
            saga_id=SAGA_ID,
            gcid=GCID,
            tenant_id=TENANT,
            jurisdiction="SG",
            payload=b"bundle",
            dek_resource_name="",
        )
        with pytest.raises(ValueError):
            await client.archive(spec)
