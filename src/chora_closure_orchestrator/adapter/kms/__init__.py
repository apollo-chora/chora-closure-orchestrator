"""Local KMS DEK lifecycle adapter.

Per Tier 3 D11 + skill `account-closure-saga`:
- per-tenant master key (CMEK) — kept while tenant active
- per-user DEK (Data Encryption Key) — DELETED at crypto-shred
- DEK deletion = crypto-shred (data unrecoverable)

Production wires ``LocalKMSClient`` (env-backed master KEK via
``CHORA_LOCAL_KEK``); tests use ``FakeKMSClient``. The port + adapter
pattern keeps the domain code testable.
"""

from chora_closure_orchestrator.adapter.kms.fake import (
    DEKDeletedError,
    DEKMetadata,
    FakeKMSClient,
)
from chora_closure_orchestrator.adapter.kms.local import (
    LOCAL_KEK_ENV,
    LOCAL_KEK_VERSION,
    LocalKMSClient,
    kek_from_env,
)
from chora_closure_orchestrator.adapter.kms.port import KMSClient
from chora_closure_orchestrator.adapter.kms.store import (
    InMemoryWrappedDEKStore,
    PostgresWrappedDEKStore,
    WrappedDEK,
    WrappedDEKStore,
)

__all__ = [
    "DEKDeletedError",
    "DEKMetadata",
    "FakeKMSClient",
    "InMemoryWrappedDEKStore",
    "KMSClient",
    "LOCAL_KEK_ENV",
    "LOCAL_KEK_VERSION",
    "LocalKMSClient",
    "PostgresWrappedDEKStore",
    "WrappedDEK",
    "WrappedDEKStore",
    "kek_from_env",
]
