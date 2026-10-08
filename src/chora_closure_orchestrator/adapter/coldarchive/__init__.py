"""MinIO cold-archive pipeline adapter.

Triggered when a saga transitions PSEUDONYMIZED -> COLD_ARCHIVED. Real
implementation: MinIO object store (the local, cloud-neutral replacement
for Google Cloud Storage).

1. Pull pre-pseudonymised user data from each domain (via the snapshot the
   domain published in its ``pseudonymised.v1`` event payload — never via
   cross-DB query)
2. Compose into a single archive bundle (JSONL + manifest)
3. Encrypt with the per-user DEK (under the master KEK)
4. Write to ``{bucket}/{tenant}/{gcid}/{closure_id}.tar.gz.enc``
5. Record retention end — no event is emitted

Tests use the in-memory adapter; the deployed topology swaps in
``MinioColdArchiveClient``.
"""

from chora_closure_orchestrator.adapter.coldarchive.inmem import (
    ColdArchiveJobSpec,
    ColdArchiveResult,
    InMemoryColdArchiveClient,
    jurisdictional_retention_days,
)
from chora_closure_orchestrator.adapter.coldarchive.minio import (
    MinioColdArchiveClient,
)
from chora_closure_orchestrator.adapter.coldarchive.port import (
    ColdArchiveClient,
)

__all__ = [
    "ColdArchiveClient",
    "ColdArchiveJobSpec",
    "ColdArchiveResult",
    "InMemoryColdArchiveClient",
    "MinioColdArchiveClient",
    "jurisdictional_retention_days",
]
