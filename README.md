# Azure E2E ETL Project — Olist E-Commerce

**Batch 11 | Bhumeet Selokar**

Metadata-driven, end-to-end ETL on Azure: eight heterogeneous e-commerce sources land in a raw ADLS Gen2 zone, are routed dynamically by Azure Data Factory into partitioned Parquet, merged into Delta tables by Azure Databricks using SCD Type 1 / Type 2 patterns, and served to Power BI.

```
8 sources → ADLS raw → metadata-driven ADF (Parquet) → ADLS src → Databricks SCD1/SCD2 → Delta curated (std) → Power BI
```

![Architecture](docs/screenshots/01_architecture_diagram.png)

---

## 1. Overview

Eight sources — SQL Server (on-prem), SFTP, Snowflake, Excel, CSV, JSON, bulk, and a REST API — land in a **raw** ADLS Gen2 zone. Azure Data Factory routes each load through a **3-level Switch hierarchy** driven by an **Azure SQL metadata database**, converting everything to snappy Parquet partitioned `type/yyyy/MM/dd` in the **src** zone. Azure Databricks consumes the latest partition and merges into **Delta** tables in the curated (**std**) zone using **SCD Type 1** (fact) and **SCD Type 2** (dimension) patterns. Every run is audited in a log table via stored procedures.

## 2. Azure Objects

| Layer | Service | Purpose |
|-------|---------|---------|
| Ingestion | Azure Data Factory `ADF-B11-ETL-DEV` | Metadata-driven routing + Copy to Parquet |
| Raw zone | ADLS Gen2 `bhumeetadlsdevraw01` | Landing area for all 8 sources |
| Src zone | ADLS Gen2 `bhumeetadlsdevsrc001` | Parquet, partitioned `type/yyyy/MM/dd` |
| Metadata | Azure SQL DB `sql-db-vb-dev` | Trigger → job → switch routing + logging |
| Transform | Azure Databricks `dbw-vb-dev-b11` | SCD1 / SCD2 MERGE into Delta |
| Curated zone | ADLS Gen2 `bhumeetadlsdevstd01` | Delta curated tables |
| Staging | Blob `bhumeetblobstagedev01/snowstage` | Snowflake export staging (SAS) |
| Compute (on-prem) | SHIR `IR-B11-SelfHosted` | On-prem SQL Server tunnel |
| Serving | Power BI | Dashboard |

![Azure objects](docs/screenshots/02_azure_objects_table.png)
![Resource group resources](docs/screenshots/03_resource_group_resources.png)

## 3. Data Sources (8)

| # | Source | Dataset | Landing |
|---|--------|---------|---------|
| S1 | SQL Server (on-prem, SHIR) | `olist_orders` + `olist_products` + `olist_sellers` | `sql/` |
| S2 | SFTP (storage-native) | `olist_customers` | `sftp/` |
| S3 | Snowflake | `olist_order_items` (112,650) | `snowflake/` (2-hop) |
| S4 | Excel | `olist_order_reviews.xlsx` | `excel/` |
| S5 | CSV | `olist_order_payments.csv` | `csv/` |
| S6 | JSON | `jsontest.json` | `json/` |
| S7 | Bulk | `olist_geolocation.csv` | `bulk/` |
| S8 | REST API (Retool) | `product_category_name_translation` | `rest/` |

![Data sources summary](docs/screenshots/04_data_sources_summary.png)
![SSMS vbproject tables](docs/screenshots/05_ssms_vbproject_tables.png)
![SFTP local user config](docs/screenshots/06_sftp_local_user_config.png)
![WinSCP customers transfer](docs/screenshots/07_winscp_customers_transfer.png)
![Snowflake order_items load](docs/screenshots/08_snowflake_order_items_load.png)
![Raw container folders](docs/screenshots/09_raw_container_folders.png)
![Src container folders](docs/screenshots/10_src_container_folders.png)
![REST API Retool JSON](docs/screenshots/11_restapi_retool_json.png)

## 4. Metadata (Azure SQL DB)

Routing tables `vb_tbl_trigger`, `vb_tbl_job`, `vb_tbl_job_dtls`, audit table `vb_tbl_log_dtls`, and stored procs `vb_start_log_entry` / `vb_end_log_entry` / `vb_get_job_dtls`. One trigger row per source (`Tr_sample_csv` … `Tr_sample_bulk`). **L2** is the first routing decision (source category); **L3/L4** populate only for `csv` and `excel` (cloud-storage branch), blank for the rest. Adding a CSV/Excel source later = **one metadata row**, no new dataset, no ADF redeploy.

![Metadata objects + roles](docs/screenshots/12_metadata_objects_roles.png)
![Log_dtls completed row](docs/screenshots/13_log_dtls_completed_row.png)
![Metadata switch routing rows](docs/screenshots/14_metadata_switch_routing.png)

## 5. Integration Runtimes

- `AutoResolveIntegrationRuntime` — cloud copies.
- `IR-B11-SelfHosted` (SHIR) — on-prem SQL Server.

![Integration runtimes list](docs/screenshots/15_integration_runtimes_list.png)
![SHIR node connected](docs/screenshots/16_shir_node_connected.png)

## 6. Linked Services (12)

Parameterized where reused; e.g. ADLS URL: `https://@{linkedService().storage_account_loc}.dfs.core.windows.net`. Secrets (SQL passwords, account keys, SAS) are **not** stored in this repo — the ARM export leaves them empty (`secureString`) to be supplied at deploy time.

![Linked services table](docs/screenshots/17_linked_services_table.png)
![Linked services ADF list](docs/screenshots/18_linked_services_adf_list.png)
![Param URL test success](docs/screenshots/19_ls_param_url_test_success.png)

## 7. Datasets (15) — two patterns

- **Pattern 1 — Param-driven** (reused across triggers): the Connection tab holds placeholders (`@dataset().container`, `@dataset().filename`, …) filled at runtime from the `LKUP_JOB_DETAILS` metadata lookup. Same dataset serves `PL4_1_CSV` (payments) and `PL4_2_EXCEL` (reviews).
- **Pattern 2 — Fixed-expr** (single-owner pipeline): sink directory is a hardcoded date expression, no parameters.

![Datasets param pattern table](docs/screenshots/20_datasets_param_pattern_table.png)
![Datasets owner-pipeline table](docs/screenshots/21_datasets_owner_pipeline_table.png)
![CSV dataset parameters](docs/screenshots/22_csv_dataset_parameters.png)
![CSV dataset connection](docs/screenshots/23_csv_dataset_connection.png)
![CSV preview param values](docs/screenshots/24_csv_preview_param_values.png)
![CSV preview payments data](docs/screenshots/25_csv_preview_payments_data.png)
![Excel dataset connection](docs/screenshots/26_excel_dataset_connection.png)
![Excel dataset parameters](docs/screenshots/27_excel_dataset_parameters.png)
![Excel preview reviews data](docs/screenshots/28_excel_preview_reviews_data.png)

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

![PL1_Master canvas](docs/screenshots/29_pl1_master_canvas.png)
![PL2_RAW_DATA switch canvas](docs/screenshots/30_pl2_raw_data_switch_canvas.png)
![PL2_RAW_DATA switch cases](docs/screenshots/31_pl2_raw_data_switch_cases.png)
![PL3 execute settings](docs/screenshots/32_pl3_cloudsg_execute_settings.png)
![PL4 file-type switch canvas](docs/screenshots/33_pl4_filetype_switch_canvas.png)
![PL4_1_CSV canvas](docs/screenshots/34_pl4_1_csv_canvas.png)
![SP-SUCCESS-LOG-ENTRY settings](docs/screenshots/35_sp_success_log_entry_settings.png)
![COPY-CSV source](docs/screenshots/36_copy_csv_source.png)
![COPY-CSV sink](docs/screenshots/37_copy_csv_sink.png)
![Log table jobid=1 Completed](docs/screenshots/38_log_table_jobid1_completed.png)
![payments.parquet in src](docs/screenshots/39_payments_parquet_src.png)

### 8.5 Excel ingestion (`PL4_2_EXCEL`)

Cloned from the CSV version; `COPY-EXCEL` source adds a `sheetname` param resolved from metadata.

![PL4_2_EXCEL canvas + source](docs/screenshots/40_pl4_2_excel_canvas_source.png)
![reviews.parquet in src](docs/screenshots/41_reviews_parquet_src.png)

### 8.6 Standalone `PL_JSON` + `PL_RESTAPI`

`PL_JSON`: JSON array → `json/yyyy/MM/dd` Parquet. `PL_RESTAPI`: GET with RFC5988 pagination → `rest/yyyy/MM/dd` Parquet.

![PL_JSON source](docs/screenshots/42_pl_json_source.png)
![PL_RESTAPI source + pagination](docs/screenshots/43_pl_restapi_source_pagination.png)
![rest/ parquet in src](docs/screenshots/44_rest_parquet_src.png)
![json/ parquet in src](docs/screenshots/45_json_parquet_src.png)

### 8.7 Failure-path proof

Source set to `wrong.csv` → `UserErrorFileNotFound` → `SP-FAIL-LOG-ENTRY` wrote a `Failed` row with full error text in `error_dtls`; reverted after test.

![Log table Failed row](docs/screenshots/46_log_table_failed_row.png)

## 9. Migration Pipelines

- `PL_SQL_PARQ` — on-prem SQL → src via SHIR (`orders`, `sellers`).
- `PL_SFTP` — raw SFTP landing → src (`customers`).
- `PL_SNOWFLAKE` — 2-hop: Snowflake → staging Blob (SAS) → `PL_BLOB_ADLS` (wildcard `*.parquet`, merge files) → ADLS src. Blob-created event trigger `Tr_sample_snowflake_evt` auto-fires hop 2. Requires `Microsoft.EventGrid` registered.

![LS_SqlServerTable SHIR config](docs/screenshots/47_ls_sqlserver_shir_config.png)
![sql/ parquet in src](docs/screenshots/48_sql_parquet_src.png)
![sftp/ parquet in src](docs/screenshots/49_sftp_parquet_src.png)
![Snowflake order_items count](docs/screenshots/50_snowflake_order_items_count.png)
![PL_SNOWFLAKE debug run](docs/screenshots/51_pl_snowflake_debug_run.png)
![snowstage blob parquet](docs/screenshots/52_snowstage_blob_parquet.png)
![Snowflake event trigger config](docs/screenshots/53_snowflake_event_trigger_config.png)
![Snowflake event trigger runs](docs/screenshots/54_snowflake_event_trigger_runs.png)
![snowflake/ parquet in src](docs/screenshots/55_snowflake_parquet_src.png)

## 10. Incremental Load — Watermark (`PL_SQL_INCR`)

Watermark table stores last-loaded timestamp; two parallel lookups (old WM, `MAX(modified_date)`); copy filters the delta window; `usp_write_watermark` advances the mark on success.

```sql
SELECT * FROM dbo.olist_orders_dataset
WHERE modified_date > '@{...LKUP_OLD_WM...}'
  AND modified_date <= '@{...LKUP_NEW_WM...}'
```

![PL_SQL_INCR canvas](docs/screenshots/56_pl_sql_incr_canvas.png)
![PL_SQL_INCR delta run](docs/screenshots/57_pl_sql_incr_delta_run.png)
![Watermark table result](docs/screenshots/58_watermark_table_result.png)

## 11. Triggers (8 scheduled + 1 event)

8 schedule triggers named exactly as `vb_tbl_trigger` rows (name = metadata key), daily, 15-min stagger, all attached to `PL1_Master`, each passing its own name as the `Trigger` parameter. 1 event trigger covers Snowflake hop 2.

![Triggers list](docs/screenshots/59_triggers_list_all.png)
![PL1_Master pipeline runs](docs/screenshots/60_pl1_master_pipeline_runs.png)
![PL1_Master activity runs](docs/screenshots/61_pl1_master_activity_runs.png)

## 12. Databricks — SCD1 + SCD2 to Delta

Workspace `dbw-vb-dev-b11`, single-node cluster (`Standard_D4s_v3`, DBR 17.3 LTS, 120-min auto-terminate). Service principal `sp-dbw-adls-b11` with **Storage Blob Data Contributor** on `src` + `std`; secret in Databricks scope `vb-scope`; `abfss://` direct access via `spark.conf`.

| Notebook | Pattern | Target | Rows (first load) |
|----------|---------|--------|-------------------|
| `00_mounts` | SP OAuth config | — | — |
| `01_SCD1_order_items` | SCD Type 1 (fact) | `cust_order_delta` | 112,654 |
| `02_SCD2_payments` | SCD Type 2 (dim) | `orderpay_scd2_delta` | 103,886 |
| `03_verify` | Row-count / history checks | — | — |

- **SCD1** — latest-partition auto-pick, audit cols (`ingest_date`/`file_date`), dedup window on `(ORDER_ID, ORDER_ITEM_ID)`, Delta MERGE upsert (latest wins, no history). Idempotent: reruns MERGE in place, count stays 112,654. Unity Catalog blocks named tables on raw paths → queried by `` delta.`abfss://...` `` path.
- **SCD2** — each incoming row gets `payment_dim_id` (uuid), `is_active=1`, `start_date=today`, `end_date=NULL`. MERGE on `(orderid, paymentsequential)`: matched + tracked value changed → expire old row (`is_active=0`, set `end_date`) and insert new; not matched → insert. History preserved. First load: all 103,886 `is_active=1`.

![Databricks cluster config](docs/screenshots/62_databricks_cluster_config.png)
![IAM src role assignments](docs/screenshots/63_iam_src_role_assignments.png)
![IAM std role assignments](docs/screenshots/64_iam_std_role_assignments.png)
![Databricks src access verify](docs/screenshots/65_databricks_src_access_verify.png)
![SCD1 count 112,654](docs/screenshots/66_scd1_count_112654.png)
![SCD2 count 103,886](docs/screenshots/67_scd2_count_103886.png)

## 13. Issues & Resolutions

![Issues and resolutions](docs/screenshots/68_issues_resolutions_table.png)

## Repository layout

```
AzureE2EProject/
├── docs/
│   ├── ECOM_ETL_Project_Assignment_Batch11_Bhumeet_Selokar.docx
│   └── screenshots/         # 01..68 (report-ordered)
├── sql/                     # metadata DDL / stored procs
├── adf/                     # exported ARM template (factory + params + linkedTemplates)
└── databricks/              # exported notebooks (00_mounts, 01_SCD1, 02_SCD2, 03_verify)
```

## Deployment

1. **ADF** — deploy `adf/arm_template/ARMTemplateForFactory.json` with `…ParametersForFactory.json`. Supply secrets (SQL passwords, account key, SAS) at deploy time — parameter file ships them empty.
2. **Metadata DB** — create Azure SQL DB, load `sql/` DDL + trigger/job/log tables + stored procedures.
3. **SHIR** — install `IR-B11-SelfHosted` on the on-prem host for SQL Server copies.
4. **Databricks** — create scope `vb-scope`, store the SP secret, grant `sp-dbw-adls-b11` Storage Blob Data Contributor on `src` + `std`, import notebooks from `databricks/`.

## Security

No credentials are committed. ADF secrets are empty `secureString` parameters; Databricks secrets are resolved via `dbutils.secrets.get("vb-scope", …)` at runtime. See `.gitignore`.
