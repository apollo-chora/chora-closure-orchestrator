"""MinioColdArchiveClient — real ``ColdArchiveClient`` writing the
DEK-encrypted bundle to the MinIO bucket (the local, cloud-neutral object
store that replaces Google Cloud Storage).

Per skill ``account-closure-saga`` cold-archive pipeline:

* object: ``{tenant_id}/{gcid}/{saga_id}.tar.gz.enc`` (payload arrives
  ALREADY encrypted with the per-user DEK — crypto-shred stays local).
* bucket lifecycle / retention is the object-store's concern; this client
  only writes.
* audit metadata stamped on the object for O+ / compliance drill-down.

The ``gs://`` URI scheme is retained as the canonical object reference
(``ColdArchiveResult.gcs_uri``) so downstream consumers — and the audit
trail — see one object-reference shape regardless of the backing store.

The minio client is blocking; the upload runs in a thread to keep the
async ``ColdArchiveClient`` port shape.

Env vars (per ``feedback_no_inline_config``):

* ``CHORA_COLD_ARCHIVE_BUCKET`` — target bucket (also the constructor
  default source in wiring).
* ``CHORA_MINIO_ENDPOINT`` — MinIO endpoint, e.g. ``minio:9000``.
* ``CHORA_MINIO_ACCESS_KEY`` / ``CHORA_MINIO_SECRET_KEY`` — credentials.
* ``CHORA_MINIO_SECURE`` — ``true`` for TLS (default false).
"""

from __future__ import annotations

import asyncio
import io
from typing import Any

from chora_closure_orchestrator.adapter.coldarchive.inmem import (
    ColdArchiveJobSpec,
    ColdArchiveResult,
    jurisdictional_retention_days,
)


class MinioColdArchiveClient:
    """MinIO-backed cold archive writer."""

    def __init__(
        self,
        *,
        bucket: str,
        client: Any | None = None,
        endpoint: str = "",
        access_key: str = "",
        secret_key: str = "",
        secure: bool = False,
    ) -> None:
        if not bucket:
            raise ValueError("bucket name required")
        self._bucket_name = bucket
        if client is None:  # pragma: no cover — exercised live only
            from minio import Minio

            client = Minio(
                endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=secure,
            )
        self._client = client

    async def archive(self, spec: ColdArchiveJobSpec) -> ColdArchiveResult:
        if not spec.payload:
            raise ValueError("cold-archive payload empty")
        if not spec.dek_resource_name:
            raise ValueError("cold-archive requires per-user DEK (dek_resource_name)")

        object_name = f"{spec.tenant_id}/{spec.gcid}/{spec.saga_id}.tar.gz.enc"
        retention_days = jurisdictional_retention_days(spec.jurisdiction)

        def _upload() -> None:
            self._client.put_object(
                self._bucket_name,
                object_name,
                io.BytesIO(spec.payload),
                len(spec.payload),
                content_type="application/octet-stream",
                metadata={
                    "saga_id": spec.saga_id,
                    "tenant_id": spec.tenant_id,
                    "gcid": spec.gcid,
                    "jurisdiction": spec.jurisdiction,
                    "retention_days": str(retention_days),
                    "dek_resource_name": spec.dek_resource_name,
                },
            )

        await asyncio.to_thread(_upload)

        return ColdArchiveResult(
            success=True,
            object_name=object_name,
            gcs_uri=f"gs://{self._bucket_name}/{object_name}",
            retention_days=retention_days,
        )


__all__ = ["MinioColdArchiveClient"]
