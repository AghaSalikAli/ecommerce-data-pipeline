--dwh creation
CREATE OR REPLACE DATABASE ecommerce_dwh;

USE DATABASE ecommerce_dwh;

-- Schemas
CREATE OR REPLACE SCHEMA raw;

CREATE OR REPLACE SCHEMA dwh;

-- raw tables
CREATE
OR
REPLACE
TABLE raw.orders (
    order_id INT,
    customer_id INT,
    order_status_id INT,
    ship_address_id INT,
    shipper_id INT,
    order_date DATE,
    total_amount NUMBER (12, 2)
);

CREATE
OR
REPLACE
TABLE raw.order_items (
    order_id INT,
    product_id INT,
    quantity INT,
    unit_price NUMBER (12, 2)
);

CREATE
OR
REPLACE
TABLE raw.products (
    product_id INT,
    name VARCHAR,
    description TEXT,
    sku VARCHAR,
    base_price NUMBER (12, 2),
    created_at TIMESTAMP
);

CREATE
OR
REPLACE
TABLE raw.product_categories (
    product_id INT,
    category_id INT
);

CREATE
OR
REPLACE
TABLE raw.categories (
    category_id INT,
    name VARCHAR,
    description TEXT
);

CREATE
OR
REPLACE
TABLE raw.customers (
    customer_id INT,
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR,
    phone VARCHAR,
    date_joined DATE
);

CREATE
OR
REPLACE
TABLE raw.addresses (
    address_id INT,
    customer_id INT,
    line1 VARCHAR,
    line2 VARCHAR,
    city VARCHAR,
    state VARCHAR,
    postal_code VARCHAR,
    country VARCHAR,
    is_default BOOLEAN
);

CREATE
OR
REPLACE
TABLE raw.shippers (
    shipper_id INT,
    name VARCHAR,
    phone VARCHAR,
    tracking_url_template VARCHAR
);

CREATE
OR
REPLACE
TABLE raw.order_statuses (
    order_status_id INT,
    status_name VARCHAR,
    description TEXT
);

CREATE
OR
REPLACE
TABLE raw.reviews (
    review_id INT,
    product_id INT,
    customer_id INT,
    rating INT,
    review_text TEXT,
    review_date DATE
);

CREATE
OR
REPLACE
TABLE raw.inventory (
    inventory_id INT,
    product_id INT,
    quantity_available INT,
    reorder_level INT,
    last_updated TIMESTAMP
);

CREATE
OR
REPLACE
TABLE raw.customer_wishlist (
    customer_id INT,
    product_id INT,
    added_at TIMESTAMP
);

CREATE
OR
REPLACE
TABLE raw.payments (
    payment_id INT,
    order_id INT,
    payment_method VARCHAR,
    payment_date DATE,
    amount NUMBER (12, 2),
    status VARCHAR
);

-- Star Schema
CREATE
OR
REPLACE
TABLE dwh.dim_product (
    product_key INT IDENTITY PRIMARY KEY,
    product_id INT UNIQUE,
    sku VARCHAR,
    name VARCHAR,
    description TEXT,
    base_price NUMBER (12, 2),
    created_at TIMESTAMP,
    category_id INT,
    category_name VARCHAR,
    category_description TEXT
);

CREATE
OR
REPLACE
TABLE dwh.dim_customer (
    customer_key INT IDENTITY PRIMARY KEY,
    customer_id INT UNIQUE,
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR,
    phone VARCHAR,
    join_date DATE,
    default_address_id INT,
    default_city VARCHAR,
    default_state VARCHAR,
    default_country VARCHAR,
    default_postal_code VARCHAR
);

CREATE
OR
REPLACE
TABLE dwh.dim_date (
    date_key INT PRIMARY KEY, -- YYYYMMDD
    actual_date DATE,
    day_name VARCHAR,
    day_of_month INT,
    week INT,
    month INT,
    quarter INT,
    year INT,
    is_weekend BOOLEAN
);

CREATE
OR
REPLACE
TABLE dwh.dim_shipper (
    shipper_key INT IDENTITY PRIMARY KEY,
    shipper_id INT UNIQUE,
    name VARCHAR,
    phone VARCHAR,
    tracking_url_template VARCHAR
);

CREATE
OR
REPLACE
TABLE dwh.dim_order_status (
    status_key INT IDENTITY PRIMARY KEY,
    order_status_id INT UNIQUE,
    status_name VARCHAR,
    description TEXT
);

-- Fact
CREATE
OR
REPLACE
TABLE dwh.fact_order_item (
    order_item_key INT IDENTITY PRIMARY KEY,
    order_id INT,
    product_key INT REFERENCES dwh.dim_product (product_key),
    customer_key INT REFERENCES dwh.dim_customer (customer_key),
    shipper_key INT REFERENCES dwh.dim_shipper (shipper_key),
    status_key INT REFERENCES dwh.dim_order_status (status_key),
    order_date_key INT REFERENCES dwh.dim_date (date_key),
    quantity INT,
    unit_price NUMBER (12, 2),
    extended_price NUMBER (12, 2)
);

-- adding data role bug fixing
USE ROLE ACCOUNTADMIN;

USE DATABASE ecommerce_dwh;

GRANT USAGE ON SCHEMA ecommerce_dwh.raw TO ROLE ACCOUNTADMIN;

GRANT USAGE ON ALL STAGES IN SCHEMA ecommerce_dwh.raw TO ROLE ACCOUNTADMIN;

-- Populating dim tables
-- dim date
INSERT INTO dwh.dim_date
SELECT
  TO_NUMBER(TO_CHAR(d,'YYYYMMDD')) AS date_key,
  d                                AS actual_date,
  TO_CHAR(d,'DY')                  AS day_name,
  DATE_PART('DAY', d)              AS day_of_month,   -- new!
  DATE_PART('WEEK', d)             AS week,
  DATE_PART('MONTH', d)            AS month,
  DATE_PART('QUARTER', d)          AS quarter,
  DATE_PART('YEAR', d)             AS year,
  CASE WHEN DAYOFWEEK(d) IN (1,7) THEN TRUE ELSE FALSE END AS is_weekend
FROM (
  SELECT DATEADD(day, seq8(), '2020-01-01'::date) AS d
  FROM TABLE(GENERATOR(ROWCOUNT => 3653))
) AS calendar;

-- dim product
MERGE INTO dwh.dim_product AS tgt USING (
    SELECT
        product_id,
        sku,
        name,
        description,
        base_price,
        created_at,
        category_id,
        category_name,
        category_description
    FROM (
            SELECT
                p.product_id, p.sku, p.name, p.description, p.base_price, p.created_at, pc.category_id, c.name AS category_name, c.description AS category_description, ROW_NUMBER() OVER (
                    PARTITION BY
                        p.product_id
                    ORDER BY pc.category_id -- picks the lowest category_id per product
                ) AS rn
            FROM raw.products p
                JOIN raw.product_categories pc ON p.product_id = pc.product_id
                JOIN raw.categories c ON pc.category_id = c.category_id
        ) ranked
    WHERE
        ranked.rn = 1
) AS src ON tgt.product_id = src.product_id WHEN NOT MATCHED THEN
INSERT (
        product_id,
        sku,
        name,
        description,
        base_price,
        created_at,
        category_id,
        category_name,
        category_description
    )
VALUES (
        src.product_id,
        src.sku,
        src.name,
        src.description,
        src.base_price,
        src.created_at,
        src.category_id,
        src.category_name,
        src.category_description
    );

-- dim customer
MERGE INTO dwh.dim_customer AS tgt USING (
    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        phone,
        join_date,
        default_address_id,
        default_city,
        default_state,
        default_country,
        default_postal_code
    FROM (
            SELECT
                c.customer_id, c.first_name, c.last_name, c.email, c.phone, c.date_joined AS join_date, a.address_id AS default_address_id, a.city AS default_city, a.state AS default_state, a.country AS default_country, a.postal_code AS default_postal_code, ROW_NUMBER() OVER (
                    PARTITION BY
                        c.customer_id
                    ORDER BY a.is_default DESC NULLS LAST
                ) AS rn
            FROM raw.customers c
                LEFT JOIN raw.addresses a ON c.customer_id = a.customer_id
                AND a.is_default = TRUE
        ) filtered
    WHERE
        filtered.rn = 1
) AS src ON tgt.customer_id = src.customer_id WHEN NOT MATCHED THEN
INSERT (
        customer_id,
        first_name,
        last_name,
        email,
        phone,
        join_date,
        default_address_id,
        default_city,
        default_state,
        default_country,
        default_postal_code
    )
VALUES (
        src.customer_id,
        src.first_name,
        src.last_name,
        src.email,
        src.phone,
        src.join_date,
        src.default_address_id,
        src.default_city,
        src.default_state,
        src.default_country,
        src.default_postal_code
    );

-- dim shippers
MERGE INTO dwh.dim_shipper AS tgt USING (
    SELECT
        shipper_id,
        name,
        phone,
        tracking_url_template
    FROM raw.shippers
) AS src ON tgt.shipper_id = src.shipper_id WHEN NOT MATCHED THEN
INSERT (
        shipper_id,
        name,
        phone,
        tracking_url_template
    )
VALUES (
        src.shipper_id,
        src.name,
        src.phone,
        src.tracking_url_template
    );

-- dim order status
MERGE INTO dwh.dim_order_status AS tgt USING (
    SELECT
        order_status_id,
        status_name,
        description
    FROM raw.order_statuses
) AS src ON tgt.order_status_id = src.order_status_id WHEN NOT MATCHED THEN
INSERT (
        order_status_id,
        status_name,
        description
    )
VALUES (
        src.order_status_id,
        src.status_name,
        src.description
    );

-- fact table
MERGE INTO dwh.fact_order_item AS tgt USING (
    SELECT
        o.order_id,
        TO_NUMBER (
            TO_CHAR (o.order_date, 'YYYYMMDD')
        ) AS order_date_key,
        dp.product_key,
        dc.customer_key,
        ds.shipper_key,
        dos.status_key,
        oi.quantity,
        oi.unit_price,
        oi.quantity * oi.unit_price AS extended_price
    FROM
        raw.orders o
        JOIN raw.order_items oi ON o.order_id = oi.order_id
        JOIN dwh.dim_product dp ON dp.product_id = oi.product_id
        JOIN dwh.dim_customer dc ON dc.customer_id = o.customer_id
        JOIN dwh.dim_shipper ds ON ds.shipper_id = o.shipper_id
        JOIN dwh.dim_order_status dos ON dos.order_status_id = o.order_status_id
) AS src ON tgt.order_id = src.order_id
AND tgt.product_key = src.product_key WHEN NOT MATCHED THEN
INSERT (
        order_id,
        order_date_key,
        product_key,
        customer_key,
        shipper_key,
        status_key,
        quantity,
        unit_price,
        extended_price
    )
VALUES (
        src.order_id,
        src.order_date_key,
        src.product_key,
        src.customer_key,
        src.shipper_key,
        src.status_key,
        src.quantity,
        src.unit_price,
        src.extended_price
    );

-- Some Analytical Queries
-- revenue by day (first 60 days)
SELECT
    dd.actual_date,
    SUM(fi.extended_price) AS total_revenue,
    SUM(fi.quantity) AS total_units
FROM dwh.fact_order_item fi
    JOIN dwh.dim_date dd ON fi.order_date_key = dd.date_key
GROUP BY
    1
ORDER BY 1
LIMIT 60;

-- top 10 products by revenue
SELECT dp.name AS product_name, SUM(fi.extended_price) AS revenue
FROM dwh.fact_order_item fi
    JOIN dwh.dim_product dp ON fi.product_key = dp.product_key
GROUP BY
    1
ORDER BY 2 DESC
LIMIT 10;

-- new vs returning customers count
WITH
    first_order AS (
        SELECT
            customer_key,
            MIN(order_date_key) AS first_order_date
        FROM dwh.fact_order_item
        GROUP BY
            1
    )
SELECT
    CASE
        WHEN fi.order_date_key = fo.first_order_date THEN 'New'
        ELSE 'Returning'
    END AS customer_type,
    COUNT(DISTINCT fi.customer_key) AS customer_count
FROM dwh.fact_order_item fi
    JOIN first_order fo ON fi.customer_key = fo.customer_key
GROUP BY
    1;