# SalesLT + Marketing Data Pipeline

> End-to-end Azure data engineering project unifying transactional sales data with behavioral marketing data to provide actionable intelligence for sales and marketing teams — tracking revenue performance, product profitability, lead pipeline value and campaign ROI from a single governed platform.

---

## Architecture — Medallion Pattern

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              SOURCE SYSTEMS                                     │
│                                                                                 │
│   ┌──────────────────────┐              ┌──────────────────────────────────┐   │
│   │   Azure SQL Database │              │    Azure Table Storage           │   │
│   │   operational_db     │              │    storageproject12026           │   │
│   │                      │              │                                  │   │
│   │  Sales.Customer      │              │  MarketingCampaigns    (50)      │   │
│   │  Sales.Product       │              │  MarketingLeads        (200)     │   │
│   │  Sales.SalesOrder    │              │  MarketingInteractions (300)     │   │
│   │  Sales.OrderDetail   │              │  [nested JSON fields]            │   │
│   └──────────┬───────────┘              └───────────────┬──────────────────┘   │
└──────────────┼──────────────────────────────────────────┼──────────────────────┘
               │ ADF Copy Activity                         │ Databricks Notebook
               │ (scheduled daily)                         │ (azure-data-tables SDK)
               ▼                                           ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  🥉 BRONZE LAYER  —  Raw Landing Zone                                           │
│  storageproject12026 / bronze /                                                 │
│                                                                                 │
│   Sales_DB/                          SalesLT/                                  │
│   ├── Customer.parquet               ├── MarketingCampaigns/  (parquet)        │
│   ├── Product.parquet                ├── MarketingLeads/      (parquet)        │
│   ├── SalesOrderHeader.parquet       └── MarketingInteractions/(parquet)       │
│   └── SalesOrderDetail.parquet                                                 │
│                                                                                 │
│   ✓ No transformation   ✓ Original format preserved   ✓ Append only           │
│   ✓ Registered in Unity Catalog: saleslt.bronze.*                              │
└─────────────────────────────────────────────────────────────────────────────────┘
               │
               │ Databricks Notebook (Bronze → Silver)
               │ inferSchema=True  |  mergeSchema=True
               ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  🥈 SILVER LAYER  —  Cleaned & Validated                                        │
│  saleslt.silver  (Delta format, Unity Catalog managed)                          │
│                                                                                 │
│   customer              product             sales_order_header                  │
│   sales_order_detail    marketingleads                                          │
│                                                                                 │
│   Cleaning rules applied:                                                       │
│   ├── Trim whitespace + empty string → NULL                                    │
│   ├── Standardize casing (email lower, names title, country upper)             │
│   ├── NULL validation on required fields → _dq_bad_row flag                   │
│   ├── Duplicate primary key detection                                           │
│   └── Numeric range validation (prices, quantities, scores)                    │
│                                                                                 │
│   Schema enforcement: StructType defined per table                             │
│   Write mode: overwrite + mergeSchema=True (Delta)                             │
└─────────────────────────────────────────────────────────────────────────────────┘
               │
               │ Databricks Notebook (Silver → Gold)
               │ SCD2 on dims  |  MERGE on facts  |  JSON flattening
               ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  🥇 GOLD LAYER  —  Star Schema  (Business-Ready)                                │
│  saleslt.gold  +  storageproject12026 / gold / saleslt /                        │
│                                                                                 │
│   DIMENSIONS (SCD Type 2)                FACTS (Delta MERGE)                   │
│   ┌─────────────────────┐               ┌──────────────────────────────┐       │
│   │  dim_customer       │               │  fact_sales                  │       │
│   │  ─ customer_key     │◄──────────────│  ─ sales_order_detail_key    │       │
│   │  ─ full_name        │               │  ─ customer_key (FK)         │       │
│   │  ─ email            │               │  ─ product_key  (FK)         │       │
│   │  ─ customer_type    │               │  ─ order_date_key (FK)       │       │
│   │  ─ scd_start_date   │               │  ─ line_total                │       │
│   │  ─ scd_end_date     │               │  ─ order_qty                 │       │
│   │  ─ scd_is_current   │               │  ─ days_to_ship              │       │
│   └─────────────────────┘               └──────────────────────────────┘       │
│                                                                                 │
│   ┌─────────────────────┐               ┌──────────────────────────────┐       │
│   │  dim_product        │               │  fact_marketing              │       │
│   │  ─ product_key      │◄──────────────│  ─ lead_key                  │       │
│   │  ─ product_name     │               │  ─ campaign_id               │       │
│   │  ─ price_range      │               │  ─ email (flattened)         │       │
│   │  ─ gross_margin     │               │  ─ engagement_score          │       │
│   │  ─ product_status   │               │  ─ lead_quality              │       │
│   │  ─ scd_is_current   │               │  ─ is_converted              │       │
│   └─────────────────────┘               │  ─ estimated_value           │       │
│                                         └──────────────────────────────┘       │
│   ┌─────────────────────┐                                                       │
│   │  dim_date           │◄──────── fact_sales + fact_marketing                 │
│   │  ─ date_key         │                                                       │
│   │  ─ year/month/week  │                                                       │
│   │  ─ quarter_label    │                                                       │
│   │  ─ is_weekend       │                                                       │
│   └─────────────────────┘                                                       │
└─────────────────────────────────────────────────────────────────────────────────┘
               │
               │ Synapse Serverless SQL (OPENROWSET + Delta format)
               ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  📊 CONSUMPTION LAYER                                                           │
│                                                                                 │
│   Synapse Analytics (gold_saleslt · salesDW schema)                            │
│   ┌────────────────────────────────────────────────────────────────────────┐   │
│   │  5 base views          6 KPI views                                     │   │
│   │  ─ dim_customer        ─ kpi_daily_sales                               │   │
│   │  ─ dim_product         ─ kpi_monthly_revenue                           │   │
│   │  ─ dim_date            ─ kpi_product_performance                       │   │
│   │  ─ fact_sales          ─ kpi_daily_leads                               │   │
│   │  ─ fact_marketing      ─ kpi_monthly_campaign_roi                      │   │
│   │                        ─ kpi_lead_funnel                               │   │
│   └────────────────────────────────────────────────────────────────────────┘   │
│                │                                                                │
│                │ DirectQuery                                                    │
│                ▼                                                                │
│   Power BI Desktop (30+ DAX measures)                                          │
│   ┌─────────────────┐  ┌─────────────────┐  ┌──────────────┐  ┌────────────┐ │
│   │  Sales Overview │  │   Marketing     │  │   Products   │  │ Customers  │ │
│   │  ─ Daily sales  │  │  ─ Lead funnel  │  │  ─ By range  │  │ ─ B2B/B2C │ │
│   │  ─ Monthly trend│  │  ─ Campaign ROI │  │  ─ Top 5     │  │ ─ Top 5   │ │
│   │  ─ YoY growth   │  │  ─ Conversion % │  │  ─ Profit    │  │ ─ ARPU    │ │
│   │  ─ Channel split│  │  ─ Pipeline $   │  │  ─ Status    │  │ ─ SCD2    │ │
│   └─────────────────┘  └─────────────────┘  └──────────────┘  └────────────┘ │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Infrastructure

| Resource | Name | Purpose |
|---|---|---|
| Azure SQL Database | `operational_db` | Source transactional data |
| ADLS Gen2 | `storageproject12026` | Bronze + Gold storage |
| Azure Table Storage | `storageproject12026` | Marketing NoSQL data |
| Azure Data Factory | `superromelus0080/project1` | Pipeline orchestration |
| Databricks | `superromelus@gmail.com` | All transformations |
| Unity Catalog | `saleslt` catalog | Metadata + governance |
| Synapse Analytics | `synapasedevworkspace1` | Serverless SQL warehouse |
| Key Vault | `AzureKeyVault1` | Secrets management |
| Access Connector | `unity-catalog-access-connector` | Managed Identity for UC |

---

## Repository Structure

```
project1/
├── linkedService/          ← ADF linked services
│   ├── azuresqlparam.json
│   ├── azuredatalake.json
│   ├── azuredatabricks1.json
│   └── azurekeyvault1.json
├── dataset/                ← ADF datasets
├── pipeline/               ← ADF pipelines
├── databrick_notebook/     ← Databricks notebook references
└── publish_config.json     ← ADF publish config
```

---

## Databricks Notebooks

| Notebook | Purpose |
|---|---|
| `TableStorage_To_Bronze.py` | Copy Azure Table Storage → Bronze ADLS as Parquet |
| `Marketing_Campaigns_Leads.py` | Generate + insert MarketingCampaigns + MarketingLeads |
| `Marketing_Interactions_Enriched.py` | Generate enriched interactions with nested JSON |
| `Generate_CSV_Colab.py` | Generate customer/product/transaction CSVs (Google Colab) |
| `SalesLT_Gold_Layer.py` | Gold layer with aggregated Delta tables |
| `Silver_To_Gold_StarSchema.py` | Full star schema: SCD2 dims + MERGE facts + 6 KPI reports |
| `Customer_360.py` | Unified customer view joining SQL + Marketing via email |
| `Unified_Customer_Table.py` | Full outer join all sources + 8 KPIs |

---

## Data Flow

```
1. ADF copies Azure SQL → Bronze (Parquet)       daily schedule
2. Databricks copies Table Storage → Bronze       daily schedule
3. Databricks cleans Bronze → Silver (Delta)      after step 1 + 2
4. Databricks builds Gold star schema             after step 3
   ├── SCD2 merge on dimensions
   └── MERGE upsert on facts
5. Synapse views read Gold Delta files            on demand
6. Power BI queries Synapse views                 DirectQuery
```

---

## Unity Catalog Structure

```
saleslt (catalog)
├── bronze (schema)
│   ├── customer                 ← Sales_DB/Customer.parquet
│   ├── product                  ← Sales_DB/Product.parquet
│   ├── sales_order_header       ← Sales_DB/SalesOrderHeader.parquet
│   ├── sales_order_detail       ← Sales_DB/SalesOrderDetail.parquet
│   ├── marketingcampaigns       ← SalesLT/MarketingCampaigns/
│   ├── marketingleads           ← SalesLT/MarketingLeads/
│   └── marketinginteractions    ← SalesLT/MarketingInteractions/
│
├── silver (schema)              ← Unity Catalog managed Delta
│   ├── customer
│   ├── product
│   ├── sales_order_header
│   ├── sales_order_detail
│   └── marketingleads
│
└── gold (schema)                ← External Delta (storageproject12026)
    ├── dim_customer   (SCD2)
    ├── dim_product    (SCD2)
    ├── dim_date       (static)
    ├── fact_sales     (MERGE)
    └── fact_marketing (MERGE)
```

---

## External Locations

| Name | URL | Credential |
|---|---|---|
| `salesltbronze` | `abfss://bronze@storageproject12026.../SalesLT` | `saleslt_credential` |
| `saleslt_silver` | `abfss://silver@storageproject12026.../` | `saleslt_credential` |
| `saleslt_gold` | `abfss://gold@storageproject12026.../` | `saleslt_credential` |
| `saleslt_sales_db` | `abfss://bronze@storageproject12026.../Sales_DB` | `saleslt_credential` |

---

## Synapse Views (salesDW schema)

```sql
-- Base views (read Gold Delta via OPENROWSET)
salesDW.dim_customer        salesDW.dim_product
salesDW.dim_date            salesDW.fact_sales
salesDW.fact_marketing

-- KPI views (aggregations on base views)
salesDW.kpi_daily_sales            → daily revenue, orders, avg ship time
salesDW.kpi_monthly_revenue        → monthly trend, YoY comparison
salesDW.kpi_product_performance    → revenue, profit, discount by product
salesDW.kpi_daily_leads            → new leads, conversions, pipeline per day
salesDW.kpi_monthly_campaign_roi   → campaign conversion rate + pipeline value
salesDW.kpi_lead_funnel            → lead status × quality breakdown
```

---

## Power BI DAX Measures

```
Sales           : Total Revenue · Total Orders · Avg Order Value
                  Revenue PY · Revenue YoY % · Revenue YTD/MTD/QTD
                  Cumulative Revenue · Weekly Growth %
                  Online Revenue % · B2B Revenue %

Marketing       : Total Leads · Converted Leads · Conversion Rate %
                  Total Pipeline Value · Hot Leads · Hot Lead Rate %
                  Avg Lead Score · Avg Engagement Score

Customer        : Total Customers · Avg Revenue Per Customer
                  B2B Revenue · B2C Revenue
```

---

## Key Design Decisions

| Decision | Rationale |
|---|---|
| Gold tables written to `storageproject12026` not UC managed storage | Synapse + Power BI can read directly — UC managed storage is locked |
| Serverless SQL views instead of external tables | No location conflicts, zero maintenance, same query performance |
| `User Identity` credential in Synapse | Power BI passes end-user Azure AD token — Managed Identity only works service-to-service |
| `_dq_bad_row` flag instead of `_corrupt_record` | Delta tables don't support PERMISSIVE mode — manual NULL check per required column |
| SCD2 on dims, MERGE on facts | Dims track historical changes; facts are upserted by grain key |
| Full outer join for Customer 360 | Preserves SQL-only customers AND marketing-only leads — no data lost |
| `mergeSchema=True` on Silver writes | Safe schema evolution when source adds new columns |

---

## Pending Items

- [ ] Move storage account key to Databricks secret scope backed by Key Vault
- [ ] Set up Key Vault backed secret scope: `dbutils.secrets.get(scope="saleslt", key="storage-account-key")`
- [ ] Build ADF pipeline to orchestrate full Bronze→Silver→Gold on daily schedule
- [ ] Add Auto Loader for incremental Bronze→Silver processing
- [ ] Fix Power BI DAX table name references (verify exact names after Synapse import)
- [ ] Build Power BI relationships manually after import

---

## Power BI Relationships

```
fact_sales[customer_key]         → dim_customer[customer_key]
fact_sales[product_key]          → dim_product[product_key]
fact_sales[order_date_key]       → dim_date[date_key]
fact_marketing[created_date_key] → dim_date[date_key]
```

---

## Project Description

This end-to-end Azure data engineering project was built to provide actionable intelligence to both the sales and marketing teams of a cycling products company. Using a medallion architecture (Bronze → Silver → Gold) orchestrated by ADF and transformed in Databricks, the data is governed through Unity Catalog and served via Synapse Serverless SQL views to a Power BI dashboard tracking six core KPIs — **daily sales summary**, **monthly revenue trend** and **product performance** on the sales side, and **daily lead activity**, **monthly campaign ROI** and **lead conversion funnel** on the marketing side. Together these metrics enable the sales team to monitor revenue, order volume and product profitability while the marketing team tracks pipeline value, lead quality and campaign return on investment — all refreshed automatically on each pipeline run.
