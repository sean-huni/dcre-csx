# dcre-csx

SBSR response reader: a short-lived Spring Batch job that ingests Fintegrate SBSR reply files into `sbsr_resp`, one verdict row per transaction, replay-safe on `UNIQUE (response_file, e2e)`.

## What it does

CSX is the SBSR leg of the DCRE response flow (`CIX | CSX | CPX -> ext_tx_status -> CRG`). When Fintegrate drops a reply file on the `fint-resp` exchange route, dcre-agt selects the reader by the filename reply-type token and launches CSX as a short-lived Kubernetes Job for `_SBSR` files (unknown tokens quarantine fail-closed). CSX parses the reply (one `<OrgnlMsgId>`, repeated `<Tx>` blocks of `<OrgnlEndToEndId>` + `<TxSts>` + optional `<Rsn>`, the [SYNTHETIC-CONTRACT R-35] shape) and upserts one `sbsr_resp` row per Tx block. CRG's `ext_tx_status` view consolidates those verdicts with stage-rank precedence PBSR > SBSR > ISR > CTV; SBSR carries the interim per-transaction statuses.

Stack: Java 25, Spring Boot 4.1.0, Spring Batch 6, Spring Data JDBC, Liquibase, CockroachDB via the PostgreSQL driver.

## Architecture and principles

3-tier layer-first packages, one responsibility per class (SOLID):

- `service/ReaderTasklet`: thin entry adapter, no SQL, no parsing. Reads `input.file`, hands the text plus `original.name` to the business tier, records the ingested count as `rows` in the execution context.
- `service/ReaderService`: business tier. Extracts the single `<OrgnlMsgId>` (missing one fails the job) and every `<Tx>` block, then upserts in bounded slices (`dcre.csx.ingest-slice-size`, default 10000). Each slice commits in its own `REQUIRES_NEW` transaction wrapped in `CrdbRetry` (bounded backoff retry of SQLSTATE 40001 serialization aborts, fresh transaction per attempt because an aborted CRDB transaction rejects every further statement with 25P02). One giant serializable transaction is unrefreshable at 300k rows; slices ratchet progress instead.
- `data/repo/SbsrRespRepo` + `data/model/SbsrRespEntity` (extends platform `BaseEntity`): persistence tier. The write is a native `INSERT ... ON CONFLICT (response_file, e2e) DO UPDATE` on the business identity, because CRDB `UPSERT` arbitrates on the primary key only.

Idempotent restart semantics: `arrival.id` is the sole identifying JobParameter. Replaying the same file (step-level retry or a same-identity relaunch after a pod kill) no-ops over committed slices via the ON CONFLICT business identity, and `StaleExecutionSweeper.abandonStale(ds, "CSX_BATCH_", 60)` runs before launch so a relaunch never throws `JobExecutionAlreadyRunning`.

Job wiring (`config/CsxJobConfig`): single tasklet step `readerStep` with the shared `CrdbRetryExceptionHandler` covering the chunk-commit boundary; the step transaction stays thin (all writes commit in the per-slice transactions). `SeamListener` writes `BUSINESS_ACCEPTED` to `<exchange-root>/outcomes/<JOB_NAME>` on COMPLETED only; a non-COMPLETED run writes nothing and the JVM exit code (`ExitCodeMain`) plus the k8s Failed condition are the witnesses.

12FactorApp Alignment - https://12factor.net/: config strictly from the environment over committed working dev defaults (a clean clone runs with no `.env`), stateless one-shot process (no server; the exit code is the verdict transport), CockroachDB and the exchange directory as attached resources, dev/prod parity via the same image and Liquibase-owned DDL everywhere.

Shared-DB isolation: Batch metadata lives under the `CSX_BATCH_` prefix (Liquibase-owned copy of the Batch 6 DDL with `EXIT_MESSAGE` widened to TEXT, `spring.batch.jdbc.initialize-schema: never`), and Liquibase history is per-service: `csx_databasechangelog` / `csx_databasechangeloglock`.

## Prerequisites

- Java 25 (Gradle toolchain; the committed wrapper is Gradle 9.5.1)
- Docker (Testcontainers CockroachDB in tests; image build)
- Platform libs in Maven Local: `za.co.fnb.dcre:platform-persistence:0.1.0` and `za.co.fnb.dcre:platform-batch:0.1.0` (they resolve from `mavenLocal()` only, no remote repository)

## Quickstart

```bash
# 1) publish the platform libs to Maven Local (order matters:
#    platform-batch brings platform-files and platform-model transitively via its api chain)
(cd ../platform-model && ./gradlew publishToMavenLocal)
(cd ../platform-files && ./gradlew publishToMavenLocal)
(cd ../platform-batch && ./gradlew publishToMavenLocal)
(cd ../platform-persistence && ./gradlew publishToMavenLocal)

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
| `DCRE_EXCHANGE_ROOT` | `../../../../../infra/dcre-infra/exchange` | Exchange root for the outcome seam file |
| `DCRE_CSX_INGEST_SLICE_SIZE` | `10000` | Rows per committed ingest slice |
| `JOB_NAME` | `local-<executionId>` | Set by AGT on the k8s Job; names the outcome seam file |

JobParameters: `arrival.id` (identifying), `input.file` and `original.name` (non-identifying; `original.name` becomes the `response_file` identity column).

## Testing

```bash
./gradlew test   # Docker required (Testcontainers cockroachdb/cockroach:v26.2.3)
```

- `CsxJobTest`: full job against a real CRDB; ingests a 4-Tx reply (ACSC/RJCT with reason AC04), asserts per-row status/reason/orgnl_msg_id, proves replay of the same file stays at 4 rows.
- `ReaderServiceSliceTest`: sliced-ingest proofs; a slice that exhausts its retry budget fails the run without rolling back committed slices, a transient 40001 abort retries in a fresh transaction, a re-run over committed slices no-ops.
- `CsxJobConfigRetryTest`: proves the shared 40001 retry handler is registered on the step the real job config builds and covers commit-time aborts.
- Cucumber BDD (`src/test/resources/features/sbsr-reply-reader.feature`): positive and negative ingestion scenarios (per-Tx verdict rows, reason retention, replay dedup, missing `OrgnlMsgId` fails the job, empty reply completes with zero rows).

## Local cluster deployment

```bash
./gradlew bootJar
docker build -t dcre-csx:2.1.1 .
kind load docker-image --name dcre-dev dcre-csx:2.1.1
```

AGT launches CSX on demand: when a `fint-resp` arrival's filename carries the `_SBSR` token, AGT mints a short-lived k8s Job from the image named in its `AGT_CSX_IMAGE` env, passing the JobParameters as program args and `JOB_NAME` in the env. `scripts/switch-version.sh VERSION` in dcre-infra repoints the whole fleet (`AGT_CSX_IMAGE=dcre-csx:VERSION`). Release tags are digits-only 3-component SemVer, uniform across the fleet.

## Related repositories

The complete, current list of live DCRE repositories (stage services, orchestrator, platform libraries, infra and tooling) lives in one place: the [DCRE design register README](https://github.com/sean-huni/dcre-design-register#repositories). Deprecated and archived repositories are deliberately absent from it. This README does not copy that list, so it cannot drift.

- Design register: https://github.com/sean-huni/dcre-design-register (start at `docs/specs/DESIGN-REGISTER.md`; the diagrams in `docs/diagrams/` are the specification)
