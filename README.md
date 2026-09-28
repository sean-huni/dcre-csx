# dcre-csx

> Part of the DCRE fleet. For the fleet map, the rulings and the diagrams that specify every stage, start at the [DCRE design register](https://github.com/sean-huni/dcre-design-register); the complete list of live repositories is its [Repositories](https://github.com/sean-huni/dcre-design-register/blob/dev/README.md#repositories) table.

SBSR response reader: a short-lived Spring Batch job that ingests Fintegrate SBSR reply files into `sbsr_resp`, one verdict row per transaction, replay-safe on `UNIQUE (response_file, e2e)`.

## What it does

| | |
|---|---|
| Stage | `CSX` |
| Family / leg | Collections (DC), RES |
| Trigger | arrival-launched: a reply file on the `fint-resp` route whose name carries `_SBSR`, for a client whose flow is collections |
| Upstream | none in the DAG (response DAGs have no edges; AGT token-picks exactly one of `CIX`, `CSX`, `CPX` per reply). The replied-to batch was emitted by `CRW` |
| Downstream | none in the DAG. `CRG` (clock-launched) reads `sbsr_resp` through its `ext_tx_status` view |
| Diagram sheet | `dcre-collections-res` |

DAG position per AGT `RouteDags.FINT_RESP_COL` and the `_SBSR` token pick in `DagEngine` on origin/dev (checked 2026-09-28). CSX parses the reply (one `<OrgnlMsgId>`, repeated `<Tx>` blocks of `<OrgnlEndToEndId>` + `<TxSts>` + optional `<Rsn>`, the [SYNTHETIC-CONTRACT R-35] shape) and upserts one `sbsr_resp` row per Tx block. CRG's `ext_tx_status` view consolidates the verdicts with stage-rank precedence PBSR 4 > SBSR 3 > ISR 2 > CTV 1; SBSR carries the interim per-transaction statuses.

Stack: Java 25, Spring Boot 4.1.0, Spring Batch 6, Spring Data JDBC, Liquibase, CockroachDB via the PostgreSQL driver.

## Architecture and principles

3-tier layer-first packages, one responsibility per class (SOLID):

- `service/ReaderTasklet`: thin entry adapter, no SQL, no parsing. Reads `input.file`, hands the text plus `original.name` to the business tier, records the ingested count as `rows` in the execution context.
- `service/ReaderService`: business tier. Extracts the single `<OrgnlMsgId>` (missing one fails the job) and every `<Tx>` block, then upserts in bounded slices (`dcre.csx.ingest-slice-size`, default 10000). Each slice commits in its own `REQUIRES_NEW` transaction wrapped in `CrdbRetry` (bounded backoff retry of SQLSTATE 40001 serialization aborts, fresh transaction per attempt because an aborted CRDB transaction rejects every further statement with 25P02). One giant serializable transaction is unrefreshable at 300k rows; slices ratchet progress instead.
- `data/repo/SbsrRespRepo` + `data/model/SbsrRespEntity` (extends platform `BaseEntity`): persistence tier. The write is a native `INSERT ... ON CONFLICT (response_file, e2e) DO UPDATE` on the business identity, because CRDB `UPSERT` arbitrates on the primary key only.
- Batch correlation (SCRUM-55): the file's `OrgnlMsgId` resolves once to the CRW emission (`crw_emission.outbound_msg_id`) and its frozen member set (`crw_emission_member`). A verdict whose e2e is not a member is skipped with a `FOREIGN_E2E` WARN (fail closed); an unknown `OrgnlMsgId` ingests fail-open with `emission_id` NULL and an `UNKNOWN_OUTBOUND_MSG` WARN.

Idempotent restart semantics: `arrival.id` is the sole identifying JobParameter. Replaying the same file (step-level retry or a same-identity relaunch after a pod kill) no-ops over committed slices via the ON CONFLICT business identity, and an `@Order(-10)` runner calls `StaleExecutionSweeper.abandonStale(ds, "CSX_BATCH_", 60)` before launch so a relaunch never throws `JobExecutionAlreadyRunning`.

Job wiring (`config/CsxJobConfig`): single tasklet step `readerStep` with the shared `CrdbRetryExceptionHandler` covering the chunk-commit boundary; the step transaction stays thin (all writes commit in the per-slice transactions). platform-batch's `OutcomeSeamListener` writes `BUSINESS_ACCEPTED` to `<exchange-root>/outcomes/<JOB_NAME>` on COMPLETED only; a non-COMPLETED run writes nothing and the JVM exit code (`ExitCodeMain`) plus the k8s Failed condition are the witnesses.

12FactorApp Alignment - https://12factor.net/: config strictly from the environment over committed working dev defaults (a clean clone runs with no `.env`), stateless one-shot process (no server; the exit code is the verdict transport), CockroachDB and the exchange directory as attached resources, dev/prod parity via the same image and Liquibase-owned DDL everywhere.

Shared-DB isolation: Batch metadata lives under the `CSX_BATCH_` prefix (Liquibase-owned copy of the Batch 6 DDL with `EXIT_MESSAGE` widened to TEXT, `db/changelog/2026/08/001-batch-metadata.xml`; the prefix is set by `dcre.batch.table-prefix` because Boot 4.1 no longer binds `spring.batch.jdbc.*`), and Liquibase history is per-service: `csx_databasechangelog` / `csx_databasechangeloglock`.

### Data

| Datasource | Database (dev default) | Env vars | Access |
|---|---|---|---|
| primary | `dcre_col` | `DCRE_DB_URL`, `DCRE_DB_USER`, `DCRE_DB_PASSWORD` | read/write |
| heartbeat (platform-batch) | `agt_ops` | `DCRE_AGTOPS_DB_URL`, `DCRE_AGTOPS_DB_USER`, `DCRE_AGTOPS_DB_PASSWORD` | `HeartbeatWriter` liveness stamp on `launch_intent` |

- Writes: `sbsr_resp` (`db/changelog/2026/08/002-sbsr-resp.xml`, the v1 baseline): `response_file` VARCHAR(512), `orgnl_msg_id`, `emission_id` (nullable, no FK), `e2e`, `status`, `reason` (nullable) plus the `BaseEntity` columns; `UNIQUE (response_file, e2e)` and index `ix_sbsr_emission`. Plus the `CSX_BATCH_*` tables.
- Reads: `crw_emission`, `crw_emission_member` (CRW-owned).

## Prerequisites

- Java 25 (`.sdkmanrc`: `java=25-tem`; `build.gradle` sets source/target compatibility 25)
- Gradle 9.5.1 via the wrapper
- Docker (Testcontainers CockroachDB in tests; image build)
- Platform libs in Maven Local: `za.co.fnb.dcre:platform-persistence:0.1.0` and `za.co.fnb.dcre:platform-batch:0.1.0` (they resolve from `mavenLocal()` only, no remote repository)

## Quickstart

```bash
# 1) publish the platform libs to Maven Local (order matters:
#    platform-batch brings platform-files and platform-model transitively via its api chain;
#    paths assume the fleet checkout: collections/csx beside platform/*)
(cd ../../platform/platform-model && ./gradlew publishToMavenLocal)
(cd ../../platform/platform-files && ./gradlew publishToMavenLocal)
(cd ../../platform/platform-batch && ./gradlew publishToMavenLocal)
(cd ../../platform/platform-persistence && ./gradlew publishToMavenLocal)

# 2) build + test (Docker required)
./gradlew test

# 3) one-shot local run (committed defaults target CockroachDB on localhost:26257; no .env needed)
./gradlew bootJar
java -jar build/libs/csx-2.0.jar \
  'arrival.id=<uuid>,java.lang.String,true' \
  'input.file=/path/to/reply.xml,java.lang.String,false' \
  'original.name=20260715_FNB_SBSR_reply.xml,java.lang.String,false'
```

## Configuration

Committed working dev defaults in `application.yml`; environment variables override.

| Env var | Default | Purpose |
|---|---|---|
| `DCRE_DB_URL` | `jdbc:postgresql://localhost:26257/dcre_col?sslmode=disable` | Shared collections DB (CockroachDB) |
| `DCRE_DB_USER` | `root` | DB user |
| `DCRE_DB_PASSWORD` | (empty) | DB password |
| `DCRE_AGTOPS_DB_URL` | `jdbc:postgresql://localhost:26257/agt_ops?sslmode=disable` | heartbeat datasource |
| `DCRE_AGTOPS_DB_USER` / `DCRE_AGTOPS_DB_PASSWORD` | `root` / (empty) | heartbeat credentials |
| `DCRE_EXCHANGE_ROOT` | `../../../../../../infra/dcre-infra/exchange` | Exchange root for the outcome seam file (AGT sets `/exchange`) |
| `DCRE_CSX_INGEST_SLICE_SIZE` | `10000` | Rows per committed ingest slice |
| `JOB_NAME` | `local-csx-<executionId>` | Set by AGT on the k8s Job; names the outcome seam file |

This is the documented set, not a closed total: Spring relaxed binding lets any Spring or `dcre.*` property be overridden by its derived environment variable name.

JobParameters: `arrival.id` (identifying), `input.file` and `original.name` (non-identifying; `original.name` becomes the `response_file` identity column).

## Testing

```bash
./gradlew test   # Docker required (Testcontainers cockroachdb/cockroach:v26.2.3)
```

- `CsxJobTest`: full job against a real CRDB; ingests a 4-Tx reply (ACSC/RJCT with reason AC04), asserts per-row status/reason/orgnl_msg_id, proves replay of the same file stays at 4 rows.
- `ReaderServiceSliceTest`: sliced-ingest proofs; a slice that exhausts its retry budget fails the run without rolling back committed slices, a transient 40001 abort retries in a fresh transaction, a re-run over committed slices no-ops.
- `BatchCorrelationIT`: emission resolution with foreign-e2e exclusion; an unknown `OrgnlMsgId` ingests fail-open with a NULL `emission_id`.
- `ResponseFileWidthIT`: a 200-character `response_file` round-trips; the replay-guard unique constraint holds at baseline width.
- `CsxJobConfigRetryTest`: proves the shared 40001 retry handler is registered on the step the real job config builds and covers commit-time aborts.
- Cucumber BDD (`src/test/resources/features/sbsr-reply-reader.feature`): positive and negative ingestion scenarios (per-Tx verdict rows, reason retention, replay dedup, missing `OrgnlMsgId` fails the job, empty reply completes with zero rows).

## Local cluster deployment

```bash
VERSION=<fleet release tag>
./gradlew bootJar
docker build -t dcre-csx:$VERSION .                # eclipse-temurin:25-jre-alpine
kind load docker-image --name dcre-dev dcre-csx:$VERSION
kubectl set env -n dcre deploy/dcre-agt AGT_CSX_IMAGE=dcre-csx:$VERSION
```

The kind cluster `dcre-dev` and the database come from dcre-infra (`scripts/kind-up.sh`). AGT resolves the image from `AGT_CSX_IMAGE` (empty by default, which leaves the stage launch-disabled). dcre-infra `scripts/switch-version.sh` does not set `AGT_CSX_IMAGE`: its stage list still names the retired `SXR` (checked 2026-09-28), hence the `kubectl set env` above. When a `fint-resp` arrival's filename carries `_SBSR`, AGT mints a short-lived Job in the collections flow namespace (AGT `AGT_NAMESPACE_COL`, default `dcre-col`) with program args `arrival.id`, `input.file`, `original.name` and env `JOB_NAME`, `DCRE_DB_URL` (AGT `service-db-url`, `dcre_col`), `DCRE_EXCHANGE_ROOT=/exchange`, `DCRE_AGTOPS_DB_URL`, `DCRE_AGTOPS_DB_USER` (AGT `JobLauncher` on origin/dev, checked 2026-09-28). Release tags are digits-only 3-component SemVer, uniform across the fleet.

## Related repositories

The complete, current list of live DCRE repositories (stage services, orchestrator, platform libraries, infra and tooling) lives in one place: the [DCRE design register README](https://github.com/sean-huni/dcre-design-register/blob/dev/README.md#repositories). Deprecated and archived repositories are deliberately absent from it. This README does not copy that list, so it cannot drift.

- Design register: https://github.com/sean-huni/dcre-design-register (start at `docs/specs/DESIGN-REGISTER.md`; the diagrams in `docs/diagrams/` are the specification)
