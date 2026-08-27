# Multi-Source Observability & Governed Data Lakehouse PoC

A proof-of-concept system demonstrating a full observability stack (Grafana/InfluxDB/MongoDB) combined with a governed data lakehouse layer (Apache Iceberg + Apache Polaris) backed by S3-compatible object storage (RustFS), with centralized authentication and role-based access control provided by Keycloak.

## Overview

This project was built incrementally to explore how modern open-source data infrastructure components fit together in practice. It covers two connected concerns:

1. **Observability pipeline** — collecting time-series and API-sourced data, visualizing it in Grafana with role-scoped dashboard access.
2. **Governed lakehouse layer** — using Apache Iceberg as the table format and Apache Polaris as the REST catalog in front of S3-compatible storage, with all catalog operations authenticated via an external OIDC identity provider (Keycloak) rather than Polaris's built-in credential system.

The system is intentionally built without cloud-managed services (no AWS, no managed Postgres, no managed Keycloak) to keep the entire stack reproducible in Docker Compose.

## Architecture

```
                        ┌───────────────────────────────────────────────────┐
                        │              Data Sources                         │
                        │  CoinGecko API(BTC) ──▶ InfluxDB                  │
                        │  CoinGecko API(ETH) ──▶ eth_collector ──▶ MongoDB │
                        └───────────────┬───────────────────────────────────┘
                                        │
                        ┌───────────────▼──────────────────────────────┐
                        │                 Grafana                      │
                        │  Dashboard 1 (InfluxDB)                      │
                        │  Dashboard 2 (MongoDB via eth_api + Infinity)│
                        │  Access scoped by Team → Folder permissions  │
                        └───────────────┬──────────────────────────────┘
                                        │ OAuth2 login
                        ┌───────────────▼──────────────────────────────┐
                        │                Keycloak                      │
                        │  Realm roles: dashboard-{1,2}-{viewer,editor}│
                        │               universal-admin, service_admin │
                        └───────┬────────────────────────┬─────────────┘
                                │ OAuth2 login           │ OIDC client credentials
                                │                        │
                        (Grafana, above)          ┌──────▼───────────┐
                                                  │  Apache Polaris  │
                                                  │  REST Catalog    │
                                                  │  auth: external  │
                                                  │  OIDC (Keycloak) │
                                                  │  persistence:    │
                                                  │  PostgreSQL      │
                                                  └────────┬─────────┘
                                                           │ S3 API
                                                   ┌───────▼─────────┐
                                                   │      RustFS     │
                                                   │  S3-compatible  │
                                                   │  object storage │
                                                   └───────▲─────────┘
                                                           │ writes/reads
                                                           │ Iceberg tables
                                                   ┌───────┴─────────────┐
                                                   │   spark-sync        │
                                                   │  PySpark, runs      │
                                                   │  every 5 minutes    │
                                                   │  MongoDB ──▶ Iceberg│
                                                   └─────────────────────┘
```

## Tech Stack

| Layer                             | Technology         |
| Time-series storage               | InfluxDB 2.7       |
| Metrics collection                | Telegraf           |
| Document storage                  | MongoDB 7          |
| Custom API layer                  | FastAPI (Python)   |
| Dashboards                        | Grafana 11.6.1     |
| Grafana external datasource       | Infinity plugin    |
| Identity & access management      | Keycloak 24        |
| Table format                      | Apache Iceberg 1.6 |
| REST catalog                      | Apache Polaris 1.7 |
| Catalog metadata persistence      | PostgreSQL 16      |
| Object storage                    | RustFS             |
| Data processing engine            | Apache Spark 3.5   |
| Orchestration                     | Docker Compose     |

## Repository Structure

```
.
├── docker-compose.yml
├── telegraf/
│   └── telegraf.conf
├── eth_collector/
│   ├── Dockerfile
│   └── collector.py
├── eth_api/
│   ├── Dockerfile
│   └── main.py
├── spark-job/
│   ├── eth_to_iceberg.py
│   └── periodic_sync.sh
└── polaris-config/
    └── application.properties
```

## Setup

### Prerequisites

- Docker and Docker Compose

### 1. Clone and start the core stack

```bash
git clone https://github.com/LadnoSam/Multi-Source-Observability-Governed-Data-Lakehouse-PoC.git
cd Multi-Source-Observability-Governed-Data-Lakehouse-PoC
docker compose up -d
```

This starts InfluxDB, Telegraf, MongoDB, the ETH collector/API, Grafana, Keycloak, RustFS, Polaris, and its PostgreSQL backend.

### 2. Bootstrap Polaris (first run only)

Polaris uses PostgreSQL for persistence. Before the schema exists, the admin tool must run once:

```bash
docker run --rm -it --network <project>_monitoring \
  --env="polaris.persistence.type=relational-jdbc" \
  --env="quarkus.datasource.username=polaris" \
  --env="quarkus.datasource.password=<password>" \
  --env="quarkus.datasource.jdbc.url=jdbc:postgresql://polaris-db:5432/POLARIS" \
  apache/polaris-admin-tool:latest bootstrap -r POLARIS -c POLARIS,root,rootpassword
```

### 3. Configure Keycloak

- Create realm roles: `dashboard-1-viewer`, `dashboard-1-editor`, `dashboard-2-viewer`, `dashboard-2-editor`, `universal-admin`, `service_admin`.
- Create a confidential client `grafana` for Grafana OAuth2 login.
- Create a confidential client `polaris-client` with **Service Accounts** enabled, and assign it the `service_admin` role. Add hardcoded claim mappers for `principal_id` and `principal_name` (see [Keycloak ↔ Polaris integration](#keycloak--polaris-integration) below).

### 4. Provision the Iceberg catalog

```bash
curl -X POST http://localhost:8181/api/management/v1/catalogs \
  -H "Authorization: Bearer $ROOT_TOKEN" -H "Content-Type: application/json" \
  -d '{
    "catalog": {
      "name": "poc-catalog",
      "type": "INTERNAL",
      "properties": {"default-base-location": "s3://test-bucket/"},
      "storageConfigInfo": {
        "storageType": "S3",
        "endpoint": "http://rustfs:9000",
        "endpointInternal": "http://rustfs:9000",
        "pathStyleAccess": true,
        "stsUnavailable": true,
        "allowedLocations": ["s3://test-bucket/"]
      }
    }
  }'
```

Grant the `service_admin` principal role access to it (see [documentation](#) for the full grant sequence).

### 5. Start the periodic sync job

```bash
docker compose up -d spark-sync
```

This continuously reads new documents from MongoDB and appends them to the `poc-catalog.crypto.ethereum_prices` Iceberg table every 5 minutes (configurable via `SYNC_INTERVAL_SECONDS`).

## Keycloak ↔ Polaris Integration

This is the core requirement this project satisfies: **all catalog operations on Polaris are authenticated via Keycloak, not Polaris's own built-in token endpoint.**

By default, Polaris issues its own JWTs for catalog access (`polaris.authentication.type=internal`). This project switches Polaris to **external OIDC authentication**, configured via `polaris-config/application.properties`:

```properties
polaris.authentication.type=mixed

quarkus.oidc.tenant-enabled=true
quarkus.oidc.auth-server-url=http://keycloak:8080/realms/Viewer
quarkus.oidc.client-id=polaris-client
quarkus.oidc.credentials.secret=<secret>
quarkus.oidc.token.issuer=any
quarkus.oidc.roles.role-claim-path=realm_access/roles

polaris.oidc.principal-mapper.type=default
polaris.oidc.principal-mapper.id-claim-path=principal_id
polaris.oidc.principal-mapper.name-claim-path=principal_name

polaris.oidc.principal-roles-mapper.type=default
polaris.oidc.principal-roles-mapper.filter=^service_admin$
polaris.oidc.principal-roles-mapper.mappings[0].regex=^.*$
polaris.oidc.principal-roles-mapper.mappings[0].replacement=PRINCIPAL_ROLE:$0
```

**Request flow:**

1. A client (e.g. Spark) requests an access token directly from Keycloak using the `polaris-client` service account credentials.
2. Keycloak issues a JWT containing custom claims — `principal_id`, `principal_name`, and the assigned realm role `service_admin` — configured via Keycloak client scope mappers, since Polaris expects a numeric principal ID that does not naturally exist in a Keycloak token.
3. The client presents this JWT as a bearer token on every request to Polaris's REST API (`/api/catalog/v1/...`, `/api/management/v1/...`).
4. Polaris validates the token signature against Keycloak's JWKS endpoint, maps the `service_admin` claim to an internal `PRINCIPAL_ROLE:service_admin`, and checks whether that role has been granted the required privilege (e.g. `CATALOG_MANAGE_CONTENT`) on the target catalog.
5. Only after authorization succeeds does Polaris use its own, separate static S3 credentials to read or write catalog data in RustFS. RustFS itself has no knowledge of Keycloak — it trusts Polaris as a standard S3 client.

`polaris.authentication.type=mixed` was used (rather than `external`) to retain Polaris's internal root credentials for administrative bootstrapping (catalog and role provisioning) alongside the external OIDC path used for regular client authentication.

**Verification:**

```bash
# Confirm the internal token endpoint is disabled for external-only tenants
curl -i -X POST http://localhost:8181/api/catalog/v1/oauth/tokens ...
# → 501 Not Implemented

# Obtain a token from Keycloak and use it against Polaris
curl -X POST http://localhost:8080/realms/Viewer/protocol/openid-connect/token \
  -d "client_id=polaris-client" -d "client_secret=<secret>" -d "grant_type=client_credentials"

curl -i -X GET http://localhost:8181/api/management/v1/catalogs \
  -H "Authorization: Bearer <keycloak_token>"
# → 200 OK, returns the catalog list
```

## RBAC in Grafana

Access to Grafana dashboards is scoped per-team, independent of the standard Grafana organization roles (Admin/Editor/Viewer):

- Each dashboard lives in its own Grafana folder.
- Each folder's permissions are granted only to a specific Team (`Dashboard 1 Viewers`, `Dashboard 1 Editors`, etc.), not to the built-in Admin/Editor/Viewer roles.
- Users are added to Teams manually, matching their Keycloak realm role, since automatic OAuth Team Sync is a Grafana Enterprise-only feature unavailable in the OSS edition used here.

## Design Decisions & Constraints

- **`stsUnavailable: true` on the Polaris storage config.** Polaris defaults to vending temporary credentials via AWS STS `AssumeRole`. RustFS, like most non-AWS S3-compatible stores, does not implement STS. Static credentials are used instead.
- **PostgreSQL persistence for Polaris.** The default in-memory metastore loses all catalogs, roles, and grants on every container restart, which is unsuitable for anything beyond a single demo session.
- **Continuous sync instead of a live query proxy.** Iceberg is a table format, not a query-interception layer — it cannot transparently proxy live queries against MongoDB or InfluxDB. Instead, `spark-sync` performs incremental batch synchronization on a fixed interval, so the Iceberg table reflects near-real-time state without requiring a custom database proxy.
- **Grafana OSS limitations.** Two Enterprise-only features (MongoDB native datasource, OAuth Team Sync) were substituted with OSS-compatible equivalents (Infinity plugin + REST API, manual team membership) rather than working around licensing.

## Engineering Notes

A number of non-obvious issues were resolved during development; documented here for reference:

- **Grafana/plugin version mismatches** caused a bundled app plugin (`grafana-lokiexplore-app`) and the Infinity datasource plugin to fail loading via SystemJS. Resolved by pinning a known-compatible plugin release and aligning it with a Grafana version meeting its minimum requirement.
- **Docker-in-Docker networking**: Polaris and Spark both run inside the same Docker network, so the S3 storage endpoint configured on the Polaris catalog must reference the internal service name (`rustfs:9000`), not `localhost`, which only resolves on the host machine.
- **Indexed environment variable configuration in Quarkus** (used internally by Polaris) proved unreliable for array-typed config (`principal-roles-mapper.mappings[0].regex`). Mounting an explicit `application.properties` file resolved this deterministically.
- **Principal ID mismatch**: Polaris requires a numeric principal ID for its internal role bindings, while Keycloak's standard `sub` claim is a UUID string. Hardcoded claim mappers in Keycloak were used to inject a matching numeric `principal_id` into the token.

