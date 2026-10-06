# chora-closure-orchestrator

## About

`chora-closure-orchestrator` is a Python service that coordinates Chora's federated account-closure saga. It starts and advances closure workflows, fans out per-domain pseudonymisation requests, records domain acknowledgements, writes an encrypted cold archive, and crypto-shreds the per-user data-encryption key. The service exposes a small FastAPI HTTP surface and uses PostgreSQL, NATS JetStream, and MinIO for durable workflow state, events, and archive storage.

## Quick start

Requires Python 3.13 and `uv`. The project metadata and locked dependencies are in `pyproject.toml` and `uv.lock`.

Clone the repository and install the development environment:

```sh
git clone https://github.com/apollo-chora/chora-closure-orchestrator.git
cd chora-closure-orchestrator

uv sync --all-groups
```

Copy the example environment file and configure the required deployment values:

```sh
cp .env.example .env
```

Run the application locally:

```sh
uv run uvicorn chora_closure_orchestrator.main:app --host 0.0.0.0 --port 8080
```

The service listens on port `8080` by default. Check liveness:

```sh
curl -s http://localhost:8080/healthz
```

A container image can be built from the repository root:

```sh
docker build -t chora-closure-orchestrator .
docker run --rm -p 8080:8080 --env-file .env chora-closure-orchestrator
```

The container runs as UID `65532` and starts Uvicorn on port `8080`.

## Usage

The service exposes three operational areas:

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/healthz` | Liveness probe |
| `GET` | `/readyz` | Readiness probe |
| `POST` | `/v1/closure/request` | Start a closure saga |
| `POST` | `/v1/closure/{closure_id}/cancel` | Cancel a closure during the grace period |
| `GET` | `/v1/closure/{closure_id}/status` | Read the current saga state, history, and domain acknowledgements |

Closure endpoints authenticate with a `Bearer <GCID>` header. Operator fast-close requests use `fast_close: true` and require the gateway to stamp `X-Chora-Role: PLATFORM_OPERATOR`.

The closure request starts in `CLOSING`. The background SagaDriver advances the workflow through the grace period and, after expiry, fans out pseudonymisation requests to the configured domain subjects. Once all required domain acknowledgements have been recorded, the saga can reach `PSEUDONYMIZED`, then `COLD_ARCHIVED`, and finally `CRYPTO_SHREDDED`. The acknowledgement timeout defaults to 24 hours and can move an incomplete saga to `PSEUDONYMISE_PARTIAL`.

Per-user data-encryption keys are 32-byte values wrapped by the configured master key. The local KMS adapter uses AES-256-GCM with tenant and GCID as additional authenticated data. The wrapped key is persisted in PostgreSQL; crypto-shredding removes the wrapped key so the archive ciphertext cannot be decrypted through this service.

The service writes saga events to the PostgreSQL transactional outbox before publishing them to NATS JetStream. Lifecycle events under the `chora.closure.*` subjects use the vendored Protobuf bindings under `src/chora_contracts_gen`; per-domain pseudonymisation request and acknowledgement messages use JSON.

The cold archive is written to MinIO. Archive objects use the key format:

```text
{tenant_id}/{gcid}/{saga_id}.tar.gz.enc
```

Retention metadata is jurisdiction-specific: Singapore and EU use 2557 days, while the US, South Africa, and Brazil use 1826 days.

Configuration is environment-backed:

| Variable | Purpose | Default |
| --- | --- | --- |
| `CHORA_AI_KERNEL_PG_DSN` | PostgreSQL DSN for the closure repository, outbox, and wrapped-DEK store | unset |
| `CHORA_AI_KERNEL_CONNINFO` | Alias for the PostgreSQL DSN | unset |
| `CHORA_AI_KERNEL_PG_DSN_SECRET_ID` | Environment-backed secret alias whose value is used as the DSN | unset |
| `CHORA_SOURCE_PROJECT` | Logical event source name; enables the outbox dispatcher when the DSN is also configured | unset |
| `CHORA_LOCAL_KEK` | Base64-encoded 32-byte master key for the local KMS adapter | unset |
| `CHORA_COLD_ARCHIVE_BUCKET` | MinIO bucket for cold archives | unset |
| `CHORA_NATS_URL` | NATS broker URL | `nats://localhost:4222` |
| `CHORA_MINIO_ENDPOINT` | MinIO endpoint | unset |
| `CHORA_MINIO_ACCESS_KEY` | MinIO access key | unset |
| `CHORA_MINIO_SECRET_KEY` | MinIO secret key | unset |
| `CHORA_MINIO_SECURE` | Whether MinIO uses TLS | unset |
| `CLOSURE_ACK_SUBSCRIPTIONS` | Comma-separated domain acknowledgement subjects | configured ten-domain set |
| `CLOSURE_DRIVER_POLL_SECONDS` | Saga-driver polling interval | `30` |
| `CLOSURE_OUTBOX_POLL_SECONDS` | Outbox polling interval | `0.5` |
| `CLOSURE_ACK_TIMEOUT_SECONDS` | Domain-ack timeout before partial escalation | `86400` |
| `CHORA_CLOSURE_FAKE_ADAPTERS` | Forces in-memory/fake adapters for development | `false` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP/gRPC tracing endpoint | unset |

For local development, `CHORA_CLOSURE_FAKE_ADAPTERS=true` switches the service to its in-memory/fake adapters. This is a development escape hatch and is not intended for production.

The repository's Python console script is also available after installation:

```sh
uv run chora-closure-orchestrator
```

## Development

Run the project's checks:

```sh
uv sync --all-groups
uv run pytest
uv run ruff check src tests
uv run ruff format --check src tests
uv run mypy
```

The pytest configuration enables strict markers. The `integration` marker is used for end-to-end saga tests covering closure, cancellation, and compensation behavior.

The package declares an 85 percent minimum coverage threshold:

```sh
uv run pytest --cov
```

The project layout is:

```text
src/chora_closure_orchestrator/
  main.py                         FastAPI entrypoint and service startup
  wiring.py                       Runtime adapter and saga wiring
  adapter/
    coldarchive/                  MinIO cold-archive adapter
    kms/                          Envelope-encryption and key-storage adapters
    pii_map/                      PII closure-map loading
    postgres/                     PostgreSQL runtime helpers
    pubsub/                       NATS subjects, subscribers, publisher, and outbox
    repository/                   Closure repository ports and implementations
    secrets/                      Environment-backed DSN/secret resolution
  domain/
    closure/                      Saga coordinator, states, and domain errors
    state/                        Shared state types
  orchestrators/
    closure_graph.py              LangGraph closure workflow
    saga_driver.py                Background saga advancement
src/chora_contracts_gen/          Vendored generated Protobuf bindings
config/                           PII closure configuration
migrations/                       PostgreSQL schema migrations
tests/
  unit/                           Fast in-process tests
  integration/                    End-to-end saga and infrastructure tests
Dockerfile                        Multi-stage container build
pyproject.toml                    Dependencies and development tooling
uv.lock                           Locked dependency graph
```

The Docker build installs the wheel from the repository itself, including the vendored `chora_contracts_gen` package, so a sibling `chora-contracts` checkout is not required.
