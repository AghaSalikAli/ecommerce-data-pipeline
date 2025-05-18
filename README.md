# Sales Data Warehouse & Automated Dashboard Pipeline

This project builds a sales-based data mart on Snowflake, orchestrated by Apache Airflow and visualized in Power BI. It demonstrates an end-to-end pipeline that:

- Ingests CSV data into Snowflake’s raw schema using an Airflow DAG.

- Transforms and upserts dimensions and fact tables into a star schema (data mart).

- Visualizes sales metrics in Power BI with incremental refresh as new data is ingested

- Automates the entire flow: drop new CSVs → trigger DAG → data loads → dashboard refreshes.

## Key Components

1. Snowflake: Scalable cloud data warehouse:

- raw schema: landing tables loaded via CSV

- dwh schema: star schema with dimensions (dim_product, dim_customer, dim_shipper, dim_order_status, dim_date) and fact_order_item

2. Apache Airflow: Workflow Automation engine.

- DAG (load_raw_all_sequence): sequential tasks that TRUNCATE, PUT, COPY raw tables, then MERGE dims and fact.

3. Power BI: BI tool for dashboarding.

- Connect via Import and perform Incremental Refresh.

## Airflow DAG Details

load_raw_all_sequence.py:

1. Loop through raw table list: orders, order_items, products, …, payments.

2. TRUNCATE each raw table.

3. PUT CSV to staging area (@~/).

4. COPY INTO raw table with lenient parsing so that no rows are skipped.

5. After all raw loads, MERGE upsert into:

- dim_product

- dim_customer

- dim_shipper

- dim_order_status

6. Lastly, MERGE fact_order_item (sales line items).

#

This pipeline demonstrates a modern ELT approach: automating data ingestion, enforcing a star-schema in a cloud data warehouse, and delivering live BI dashboards that update seamlessly when new data arrives.
