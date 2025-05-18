# ~/airflow/dags/load_raw_all_sequence.py

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

# List of all 13 raw tables to load
RAW_TABLES = [
    "orders",
    "order_items",
    "products",
    "product_categories",
    "categories",
    "customers",
    "addresses",
    "shippers",
    "order_statuses",
    "reviews",
    "inventory",
    "customer_wishlist",
    "payments",
]

default_args = {
    "owner": "airflow",
    "snowflake_conn_id": "snowflake_conn",
}

with DAG(
    dag_id="load_raw_all_sequence",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    concurrency=1,
    max_active_runs=1,
) as dag:
    prev_task = None

    # 1) Truncate, upload, and load each raw table sequentially
    for table in RAW_TABLES:
        # Truncate existing data
        truncate = SnowflakeOperator(
            task_id=f"truncate_{table}",
            sql=f"TRUNCATE TABLE raw.{table};"
        )

        # Stage CSV into user stage
        put = SnowflakeOperator(
            task_id=f"put_{table}",
            sql=f"PUT file:///data/{table}.csv @~/ OVERWRITE = TRUE;"
        )

        # Copy staged file into raw table with lenient parsing (so that no rows are skipped)
        copy = SnowflakeOperator(
            task_id=f"copy_{table}",
            sql=f"""
COPY INTO raw.{table}
FROM @~/{table}.csv
FILE_FORMAT=(
  TYPE = 'CSV',
  SKIP_HEADER = 1,
  FIELD_OPTIONALLY_ENCLOSED_BY = '"',
  TRIM_SPACE = TRUE,
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
)
ON_ERROR = 'CONTINUE';
"""
        )

        # Chain tasks: ensure sequential execution
        if prev_task:
            prev_task >> truncate
        truncate >> put >> copy
        prev_task = copy

    # 2) Sequentially upsert dimensions and fact
    merge_dim_product = SnowflakeOperator(
        task_id="merge_dim_product",
        sql="""
MERGE INTO dwh.dim_product AS tgt
USING (
  SELECT product_id, sku, name, description,
         base_price, created_at, category_id,
         category_name, category_description
  FROM (
    SELECT p.product_id, p.sku, p.name, p.description,
           p.base_price, p.created_at,
           pc.category_id, c.name        AS category_name,
           c.description                 AS category_description,
           ROW_NUMBER() OVER (
             PARTITION BY p.product_id
             ORDER BY pc.category_id
           ) AS rn
    FROM raw.products p
    JOIN raw.product_categories pc USING(product_id)
    JOIN raw.categories c         USING(category_id)
  ) ranked
  WHERE rn = 1
) src
ON tgt.product_id = src.product_id
WHEN NOT MATCHED THEN
  INSERT (product_id, sku, name, description,
          base_price, created_at,
          category_id, category_name, category_description)
  VALUES (src.product_id, src.sku, src.name, src.description,
          src.base_price, src.created_at,
          src.category_id, src.category_name, src.category_description);
"""
    )

    merge_dim_customer = SnowflakeOperator(
        task_id="merge_dim_customer",
        sql="""
MERGE INTO dwh.dim_customer AS tgt
USING (
  SELECT customer_id, first_name, last_name, email,
         phone, join_date, default_address_id,
         default_city, default_state,
         default_country, default_postal_code
  FROM (
    SELECT c.customer_id, c.first_name, c.last_name,
           c.email, c.phone, c.date_joined      AS join_date,
           a.address_id        AS default_address_id,
           a.city              AS default_city,
           a.state             AS default_state,
           a.country           AS default_country,
           a.postal_code       AS default_postal_code,
           ROW_NUMBER() OVER (
             PARTITION BY c.customer_id
             ORDER BY a.is_default DESC NULLS LAST
           ) AS rn
    FROM raw.customers c
    LEFT JOIN raw.addresses a ON c.customer_id = a.customer_id
                            AND a.is_default = TRUE
  ) filtered
  WHERE rn = 1
) src
ON tgt.customer_id = src.customer_id
WHEN NOT MATCHED THEN
  INSERT (customer_id, first_name, last_name, email, phone,
          join_date, default_address_id,
          default_city, default_state,
          default_country, default_postal_code)
  VALUES (src.customer_id, src.first_name, src.last_name, src.email, src.phone,
          src.join_date, src.default_address_id,
          src.default_city, src.default_state,
          src.default_country, src.default_postal_code);
"""
    )

    merge_dim_shipper = SnowflakeOperator(
        task_id="merge_dim_shipper",
        sql="""
MERGE INTO dwh.dim_shipper AS tgt
USING (
  SELECT shipper_id, name, phone, tracking_url_template
  FROM raw.shippers
) src
ON tgt.shipper_id = src.shipper_id
WHEN NOT MATCHED THEN
  INSERT (shipper_id, name, phone, tracking_url_template)
  VALUES (src.shipper_id, src.name, src.phone, src.tracking_url_template);
"""
    )

    merge_dim_order_status = SnowflakeOperator(
        task_id="merge_dim_order_status",
        sql="""
MERGE INTO dwh.dim_order_status AS tgt
USING (
  SELECT order_status_id, status_name, description
  FROM raw.order_statuses
) src
ON tgt.order_status_id = src.order_status_id
WHEN NOT MATCHED THEN
  INSERT (order_status_id, status_name, description)
  VALUES (src.order_status_id, src.status_name, src.description);
"""
    )

    merge_fact_order_item = SnowflakeOperator(
        task_id="merge_fact_order_item",
        sql="""
MERGE INTO dwh.fact_order_item AS tgt
USING (
  SELECT o.order_id,
         TO_NUMBER(TO_CHAR(o.order_date,'YYYYMMDD')) AS order_date_key,
         dp.product_key, dc.customer_key,
         ds.shipper_key, dos.status_key,
         oi.quantity, oi.unit_price,
         oi.quantity * oi.unit_price       AS extended_price
  FROM raw.orders o
  JOIN raw.order_items     oi  ON o.order_id = oi.order_id
  JOIN dwh.dim_product     dp  ON dp.product_id       = oi.product_id
  JOIN dwh.dim_customer    dc  ON dc.customer_id      = o.customer_id
  JOIN dwh.dim_shipper     ds  ON ds.shipper_id       = o.shipper_id
  JOIN dwh.dim_order_status dos ON dos.order_status_id = o.order_status_id
) src
ON tgt.order_id    = src.order_id
AND tgt.product_key = src.product_key
WHEN NOT MATCHED THEN
  INSERT (order_id, order_date_key, product_key, customer_key,
          shipper_key, status_key, quantity, unit_price, extended_price)
  VALUES (src.order_id, src.order_date_key, src.product_key, src.customer_key,
          src.shipper_key, src.status_key, src.quantity, src.unit_price, src.extended_price);
"""
    )

    # 3) Sequence merges after the final raw copy
    prev_task >> merge_dim_product
    merge_dim_product >> merge_dim_customer
    merge_dim_customer >> merge_dim_shipper
    merge_dim_shipper >> merge_dim_order_status
    merge_dim_order_status >> merge_fact_order_item
