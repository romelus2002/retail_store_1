
## Project Description ( SalesLT + Marketing Data Pipeline)

This end-to-end Azure data engineering project was built to provide actionable intelligence to both the sales and marketing teams of a cycling products company. Using a medallion architecture (Bronze → Silver → Gold) orchestrated by ADF and transformed in Databricks, the data is governed through Unity Catalog and served via Synapse Serverless SQL views to a Power BI dashboard tracking six core KPIs — **daily sales summary**, **monthly revenue trend** and **product performance** on the sales side, and **daily lead activity**, **monthly campaign ROI** and **lead conversion funnel** on the marketing side. Together these metrics enable the sales team to monitor revenue, order volume and product profitability while the marketing team tracks pipeline value, lead quality and campaign return on investment — all refreshed automatically on each pipeline run.


## Architecture — Medallion Pattern

<img width="541" height="261" alt="image" src="https://github.com/user-attachments/assets/ba54faa5-4ae7-4635-87f2-7bec988af104" />






