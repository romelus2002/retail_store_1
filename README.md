
## Project Description ( SalesLT + Marketing Data Pipeline)

This end-to-end Azure data engineering project was built to provide actionable intelligence to both the sales and marketing teams of a cycling products company. Using a medallion architecture (Bronze → Silver → Gold) orchestrated by ADF and transformed in Databricks, the data is governed through Unity Catalog and served via Synapse Serverless SQL views to a Power BI dashboard tracking six core KPIs — **daily sales summary**, **monthly revenue trend** and **product performance** on the sales side, and **daily lead activity**, **monthly campaign ROI** and **lead conversion funnel** on the marketing side. Together these metrics enable the sales team to monitor revenue, order volume and product profitability while the marketing team tracks pipeline value, lead quality and campaign return on investment — all refreshed automatically on each pipeline run.


## Architecture — Medallion Pattern

## Repository Structure

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
┘





