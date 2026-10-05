"""MinioColdArchiveClient — real ColdArchiveClient writing the encrypted
bundle to the MinIO bucket (skill ``account-closure-saga`` cold-archive
pipeline step 4).

Unit tests fake the minio client; the object write runs in a thread
(asyncio.to_thread) to keep the async port shape.
"""

from __future__ import annotations

from typing import Any

import pytest

from chora_closure_orchestrator.adapter.coldarchive.inmem import (
    ColdArchiveJobSpec,
)
from chora_closure_orchestrator.adapter.coldarchive.minio import (
    MinioColdArchiveClient,
)

GCID = "01970000-0000-7000-9000-000000000001"
TENANT = "01970000-0000-7000-8000-000000000001"
SAGA = "01970000-0000-7000-a000-000000000001"


class _FakePutRecord:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, bytes, int, str, dict[str, str]]] = []

    def __call__(
        self,
        bucket: str,
        name: str,
        data: Any,
        length: int,
        content_type: str = "",
        metadata: dict[str, str] | None = None,
    ) -> None:
        self.calls.append((bucket, name, data.read(), length, content_type, metadata or {}))


class _FakeMinioClient:
    def __init__(self) -> None:
        self.put_object = _FakePutRecord()


def _spec(**overrides: Any) -> ColdArchiveJobSpec:
    kwargs: dict[str, Any] = {
        "saga_id": SAGA,
        "gcid": GCID,
        "tenant_id": TENANT,
        "jurisdiction": "SG",
        "payload": b"encrypted-bundle-bytes",
        "dek_resource_name": "chora-closure/dek-wrap/t/g",
    }
    kwargs.update(overrides)
    return ColdArchiveJobSpec(**kwargs)


class TestMinioColdArchive:
    @pytest.mark.asyncio
    async def test_archive_uploads_encrypted_object(self) -> None:
        minio = _FakeMinioClient()
        client = MinioColdArchiveClient(bucket="chora-cold-archive", client=minio)
        result = await client.archive(_spec())

        object_name = f"{TENANT}/{GCID}/{SAGA}.tar.gz.enc"
        assert result.success is True
        assert result.object_name == object_name
        assert result.gcs_uri == f"gs://chora-cold-archive/{object_name}"
        assert result.retention_days == 2557  # SG floor

        bucket, name, uploaded, length, content_type, _meta = minio.put_object.calls[0]
        assert bucket == "chora-cold-archive"
        assert name == object_name
        assert uploaded == b"encrypted-bundle-bytes"
        assert length == len(b"encrypted-bundle-bytes")
        assert content_type == "application/octet-stream"

    @pytest.mark.asyncio
    async def test_archive_stamps_audit_metadata(self) -> None:
        minio = _FakeMinioClient()
        client = MinioColdArchiveClient(bucket="b", client=minio)
        await client.archive(_spec(jurisdiction="US"))
        _bucket, _name, _data, _length, _ct, metadata = minio.put_object.calls[0]
        assert metadata is not None
        assert metadata["saga_id"] == SAGA
        assert metadata["jurisdiction"] == "US"
        assert metadata["retention_days"] == "1826"
        assert metadata["dek_resource_name"] == "chora-closure/dek-wrap/t/g"

    @pytest.mark.asyncio
    async def test_archive_rejects_empty_payload(self) -> None:
        client = MinioColdArchiveClient(bucket="b", client=_FakeMinioClient())
        with pytest.raises(ValueError):
            await client.archive(_spec(payload=b""))

    @pytest.mark.asyncio
    async def test_archive_rejects_missing_dek(self) -> None:
        client = MinioColdArchiveClient(bucket="b", client=_FakeMinioClient())
        with pytest.raises(ValueError):
            await client.archive(_spec(dek_resource_name=""))

    def test_requires_bucket(self) -> None:
        with pytest.raises(ValueError):
            MinioColdArchiveClient(bucket="", client=_FakeMinioClient())
