# Azure E2E ETL Project — Olist E-Commerce

**Batch 11 | Bhumeet Selokar**

Metadata-driven, end-to-end ETL on Azure: eight heterogeneous e-commerce sources land in a raw ADLS Gen2 zone, are routed dynamically by Azure Data Factory into partitioned Parquet, merged into Delta tables by Azure Databricks using SCD Type 1 / Type 2 patterns, and served to Power BI.

```
8 sources → ADLS raw → metadata-driven ADF (Parquet) → ADLS src → Databricks SCD1/SCD2 → Delta curated (std) → Power BI
```

---

## 1. Overview

Eight sources — SQL Server (on-prem), SFTP, Snowflake, Excel, CSV, JSON, bulk, and a REST API — land in a **raw** ADLS Gen2 zone. Azure Data Factory routes each load through a **3-level Switch hierarchy** driven by an **Azure SQL metadata database**, converting everything to snappy Parquet partitioned `type/yyyy/MM/dd` in the **src** zone. Azure Databricks consumes the latest partition and merges into **Delta** tables in the curated (**std**) zone using **SCD Type 1** (fact) and **SCD Type 2** (dimension) patterns. Every run is audited in a log table via stored procedures.

## 2. Architecture

| Layer | Service | Purpose |
|-------|---------|---------|
| Ingestion | Azure Data Factory | Metadata-driven routing + Copy to Parquet |
| Raw zone | ADLS Gen2 (`raw`) | Landing area for all 8 sources |
| Src zone | ADLS Gen2 (`src`) | Parquet, partitioned `type/yyyy/MM/dd` |
| Metadata | Azure SQL DB | Trigger → job → switch-level routing + logging |
| Transform | Azure Databricks | SCD1 / SCD2 MERGE into Delta |
| Curated zone | ADLS Gen2 (`std`) | Delta curated tables |
| Serving | Power BI | Dashboard |

## 3. Data Sources (8)

| # | Source | Landing |
|---|--------|---------|
| 1 | SQL Server (on-prem, via SHIR) | `sql/` |
| 2 | SFTP (WinSCP) | `sftp/` |
| 3 | Snowflake (`order_items`, 112,650 rows) | `snowflake/` |
| 4 | Excel (`reviews`) | `excel/` |
| 5 | CSV (`payments`) | `csv/` |
| 6 | JSON (array of documents) | `json/` |
| 7 | Bulk | `bulk/` |
| 8 | REST API (Retool, RFC5988 pagination) | `rest/` |

## 4. Metadata (Azure SQL DB)

Routing tables `vb_tbl_trigger`, `vb_tbl_job`, and audit table `vb_tbl_log_dtls`. One trigger row per source (`Tr_sample_csv` … `Tr_sample_bulk`). The pipeline reads:

- **L2** — first routing decision: source category.
- **L3 / L4** — populated only for `csv` and `excel` (cloud-storage branch: which cloud, which file format), blank for the rest.

Adding a CSV/Excel source later = **one metadata row**, no new dataset, no ADF redeploy.

## 5. Integration Runtimes

- `AutoResolveIntegrationRuntime` — cloud copies.
- `IR-B11-SelfHosted` (SHIR) — on-prem SQL Server.

## 6. Linked Services (12)

Parameterized where reused; e.g. ADLS URL: `https://@{linkedService().storage_account_loc}.dfs.core.windows.net`. Secrets (SQL passwords, account keys, SAS) are **not** stored in this repo — the ARM export leaves them empty (`secureString`) to be supplied at deploy time.

## 7. Datasets (15) — two patterns

- **Pattern 1 — Param-driven** (reused across triggers): the Connection tab holds placeholders (`@dataset().container`, `@dataset().filename`, …) filled at runtime from the `LKUP_JOB_DETAILS` metadata lookup. Same dataset serves `PL4_1_CSV` (payments) and `PL4_2_EXCEL` (reviews).
- **Pattern 2 — Fixed-expr** (single-owner pipeline): sink directory is a hardcoded date expression, no parameters.

## 8. Pipelines — metadata-driven routing chain

| Pipeline | Role |
|----------|------|
| `PL1_Master` | Entry orchestrator; `SET_VAR_TRIGGER` resolves trigger name (param wins, else `TriggerName`) |
| `PL2_RAW_DATA` | L2 router — `SELECT DISTINCT L2_switch_type …` |
| `PL3_CLOUDSG_CSV_EXCEL` | L3 Switch → file-type router |
| `PL4_FILETYPE_CSV_EXCEL` | L4 Switch → grain copy pipeline |
| `PL4_1_CSV` | CSV → src Parquet, with start/success/fail logging |
| `PL4_2_EXCEL` | Excel → src Parquet (cloned from CSV with deltas) |
| `PL_JSON` | JSON array → `json/yyyy/MM/dd` Parquet |
| `PL_RESTAPI` | REST GET w/ RFC5988 pagination → `rest/…` Parquet |

**Logging chain** (`PL4_1_CSV`): `LKUP_JOB_DETAILS` → `SP-START-LOG-ENTRY` → `SET_VAR_MAPPING` (TabularTranslator) → `COPY-CSV` (`Mapping=@json(variables(mapping))`) → `SP-SUCCESS-LOG-ENTRY` (green) / `SP-FAIL-LOG-ENTRY` (red, `error=@activity('COPY-CSV').error.message`).

**Failure-path proof**: source set to `wrong.csv` → `UserErrorFileNotFound` → `SP-FAIL-LOG-ENTRY` wrote a `Failed` row with full error text; reverted after test.

### Migration pipelines

- `PL_SQL_PARQ` — on-prem SQL → src via SHIR (`orders`, `sellers`).
- `PL_SFTP` — raw SFTP landing → src (`customers`).
- `PL_SNOWFLAKE` — 2-hop: Snowflake → staging Blob (SAS) → `PL_BLOB_ADLS` (wildcard `*.parquet`, merge files) → ADLS src. Blob-created event trigger `Tr_sample_snowflake_evt` auto-fires hop 2. Requires `Microsoft.EventGrid` registered.

### Incremental load — watermark (`PL_SQL_INCR`)

Watermark table stores last-loaded timestamp; two parallel lookups (old WM, `MAX(modified_date)`); copy filters the delta window; `usp_write_watermark` advances the mark on success.

```sql
SELECT * FROM dbo.olist_orders_dataset
WHERE modified_date > '@{...LKUP_OLD_WM...}'
  AND modified_date <= '@{...LKUP_NEW_WM...}'
```

## 9. Triggers (8 scheduled + 1 event)

8 schedule triggers named exactly as `vb_tbl_trigger` rows (name = metadata key), daily, 15-min stagger, all attached to `PL1_Master`, each passing its own name as the `Trigger` parameter. 1 event trigger covers Snowflake hop 2.

## 10. Databricks — SCD1 + SCD2 to Delta

**Setup**: workspace `dbw-vb-dev-b11`, single-node cluster (`Standard_D4s_v3`, DBR 17.3 LTS, 120-min auto-terminate). Service principal `sp-dbw-adls-b11` with **Storage Blob Data Contributor** on `src` + `std`; secret in Databricks scope `vb-scope`; `abfss://` direct access via `spark.conf`.

| Notebook | Pattern | Target | Rows (first load) |
|----------|---------|--------|-------------------|
| `00_mounts` | SP OAuth config | — | — |
| `01_SCD1_order_items` | SCD Type 1 (fact) | `cust_order_delta` | 112,654 |
| `02_SCD2_payments` | SCD Type 2 (dim) | `orderpay_scd2_delta` | 103,886 |
| `03_verify` | Row-count / history checks | — | — |

- **SCD1** — latest-partition auto-pick, audit cols (`ingest_date`/`file_date`), dedup window on `(ORDER_ID, ORDER_ITEM_ID)`, Delta MERGE upsert (latest wins, no history). Idempotent: reruns MERGE in place, count stays 112,654. Unity Catalog blocks named tables on raw paths → queried by `` delta.`abfss://...` `` path.
- **SCD2** — each incoming row gets `payment_dim_id` (uuid), `is_active=1`, `start_date=today`, `end_date=NULL`. MERGE on `(orderid, paymentsequential)`: matched + tracked value changed → expire old row (`is_active=0`, set `end_date`) and insert new; not matched → insert. History preserved. First load: all 103,886 `is_active=1`.

## Repository layout

```
AzureE2EProject/
├── docs/
│   ├── ECOM_ETL_Project_Assignment_Batch11_Bhumeet_Selokar.docx
│   └── screenshots/
├── sql/          # metadata DDL / stored procs
├── adf/          # exported ARM template (ARMTemplateForFactory.json + params + linkedTemplates)
└── databricks/   # exported notebooks (00_mounts, 01_SCD1, 02_SCD2, 03_verify)
```

## Deployment

1. **ADF** — deploy `adf/arm_template/ARMTemplateForFactory.json` with `…ParametersForFactory.json`. Supply secrets (SQL passwords, account key, SAS) at deploy time — parameter file ships them empty.
2. **Metadata DB** — create Azure SQL DB, load `sql/` DDL + trigger/job/log tables + stored procedures.
3. **SHIR** — install `IR-B11-SelfHosted` on the on-prem host for SQL Server copies.
4. **Databricks** — create scope `vb-scope`, store the SP secret, grant `sp-dbw-adls-b11` Storage Blob Data Contributor on `src` + `std`, import notebooks from `databricks/`.

## Security

No credentials are committed. ADF secrets are empty `secureString` parameters; Databricks secrets are resolved via `dbutils.secrets.get("vb-scope", …)` at runtime. See `.gitignore`.
