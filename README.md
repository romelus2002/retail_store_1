
## Project Description ( SalesLT + Marketing Data Pipeline)

This end-to-end Azure data engineering project was built to provide actionable intelligence to both the sales and marketing teams of a cycling products company. Using a medallion architecture (Bronze → Silver → Gold) orchestrated by ADF and transformed in Databricks, the data is governed through Unity Catalog and served via Synapse Serverless SQL views to a Power BI dashboard tracking six core KPIs — **daily sales summary**, **monthly revenue trend** and **product performance** on the sales side, and **daily lead activity**, **monthly campaign ROI** and **lead conversion funnel** on the marketing side. Together these metrics enable the sales team to monitor revenue, order volume and product profitability while the marketing team tracks pipeline value, lead quality and campaign return on investment — all refreshed automatically on each pipeline run.


## Architecture — Medallion Pattern

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
│   Sales_DB/                          CRM-marketing/                                  │
│   ├── Customer.parquet               ├── MarketingCampaigns/  (JSON)        │
│   ├── Product.parquet                ├── MarketingLeads/      (JSON)        │
│   ├── SalesOrderHeader.parquet       └── MarketingInteractions/(JSON)       │
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





