-- ==============================================================================
-- FILE: training/ddl/08_B2B_B2C_VIEW.sql
-- PURPOSE: The complete CREATE VIEW statement for [dbo].[B2B_B2C].
--          This is the PRIMARY QUERY TARGET for all analytics.
--          ALL user questions must be answered by querying this view ONLY.
--          Never query the underlying source tables directly.
-- DATABASE: Charmacy_f_automate
-- OBJECT:   [dbo].[B2B_B2C]
-- TYPE:     VIEW (NOT a physical table)
-- COLUMNS:  42
-- ROWS:     ~10,000–15,000 (grows daily)
-- ==============================================================================

USE [Charmacy_f_automate];
GO

-- ==============================================================================
-- COLUMN SCHEMA SUMMARY (all 42 columns in order):
-- ==============================================================================
-- #   Column                  Type            Nullable  Notes
-- --- ----------------------  -------------- --------- -------------------------
--  1  order_id                NVARCHAR(255)  YES       NULL for Shopify/Nykaa/Zepto
--  2  fulfilment_type         NVARCHAR       YES       Shopify/Flipkart/Myntra
--  3  ordertype               VARCHAR(3)     NO        'B2B' or 'B2C'
--  4  salestype               VARCHAR(9)     NO        'Primary' or 'Secondary'
--  5  transaction_type        NVARCHAR       YES       Amazon event; Shopify=payment method
--  6  order_date              DATE           YES       NULL for ALL Shopify rows
--  7  month_year              VARCHAR(7)     YES       e.g. '2026-01', NULL with order_date
--  8  quantity                INT            YES       Units in line-item
--  9  product_description     NVARCHAR       YES       Raw listing text — NOT for grouping
-- 10  product_code            NVARCHAR       YES       ASIN (Amazon), FSN (Flipkart)
-- 11  invoice_number          VARCHAR(50)    YES       100% NULL — ignore
-- 12  invoice_date            DATE           YES       100% NULL — ignore
-- 13  bill_from_city          NVARCHAR       YES       Shopify only
-- 14  bill_from_state         NVARCHAR       YES       Shopify only
-- 15  bill_from_country       NVARCHAR       YES       Shopify only
-- 16  bill_from_postal_code   NVARCHAR(20)   YES       Shopify only
-- 17  ship_to_city            NVARCHAR       YES       Amazon/Myntra/Shopify
-- 18  ship_to_state           NVARCHAR       YES       All B2C; 70 distinct values; mixed format
-- 19  ship_to_postal_code     NVARCHAR(20)   YES       Amazon/Shopify
-- 20  payment_method          NVARCHAR       YES       Amazon/Myntra/Shopify; 15 distinct values
-- 21  event_sub_type          NVARCHAR       YES       Flipkart/Myntra
-- 22  EAN                     NVARCHAR(255)  YES       From EAN_Master; starts with 8906148
-- 23  article_type            NVARCHAR(255)  YES       From EAN_Master; ~34% NULL
-- 24  sku_code                NVARCHAR(255)  YES       From EAN_Master; ~34% NULL
-- 25  product_name            NVARCHAR(255)  YES       From EAN_Master; USE FOR GROUPING; ~34% NULL
-- 26  MRP                     DECIMAL(18,2)  YES       TOTAL line-item amount (NOT unit price)
-- 27  warehouse_id            NVARCHAR       YES       Amazon/Myntra only; ~91% NULL
-- 28  platform                NVARCHAR       NO        Amazon/Flipkart/Myntra/Shopify/Nykaa/Zepto
-- 29  seller_code             NVARCHAR(100)  YES       Nykaa only
-- 30  display_name            NVARCHAR(100)  YES       Nykaa only
-- 31  company_name            NVARCHAR(100)  YES       Nykaa only
-- 32  seller_type             NVARCHAR(100)  YES       Nykaa only
-- 33  brand                   NVARCHAR(100)  YES       Nykaa + Zepto; always 'Charmacy Milano'
-- 34  sku_name                NVARCHAR(100)  YES       Nykaa + Zepto; marketplace display name
-- 35  category_l1             NVARCHAR(100)  YES       Nykaa only; always 'Makeup'
-- 36  category_l2             NVARCHAR(100)  YES       Nykaa only; Eyes/Face/Lips/Makeup Kits
-- 37  category_l3             NVARCHAR(100)  YES       Nykaa only; Foundation/Concealer/etc.
-- 38  display_price           DECIMAL(18,2)  YES       Nykaa only; listed price before discount
-- 39  selling_price           DECIMAL(18,2)  YES       Nykaa only; actual price after discount
-- 40  total_qty               INT            YES       Nykaa + Zepto; pre-aggregated units
-- 41  total_orders            INT            YES       Nykaa only; pre-aggregated order count
-- 42  total_customers         INT            YES       Nykaa only; pre-aggregated customer count
-- ==============================================================================

ALTER VIEW [dbo].[B2B_B2C] AS

-- ─────────────────────────────────────────────────────────────────────────────
-- SEGMENT 1: AMAZON B2C
-- Source: Amazon_B2C_Daily
-- ordertype = 'B2C', salestype = 'Primary'
-- Cancel/Refund/FreeReplacement rows must be excluded for net revenue.
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    CAST(order_id AS NVARCHAR(255))                                                             AS order_id,
    CAST(NULL AS NVARCHAR(100))                                                                 AS fulfilment_type,
    'B2C'                                                                                       AS ordertype,
    'Primary'                                                                                   AS salestype,
    transaction_type,
    TRY_CONVERT(DATE, order_date)                                                               AS order_date,
    CONVERT(VARCHAR(7), TRY_CONVERT(DATE, order_date), 120)                                     AS month_year,
    TRY_CONVERT(INT, quantity)                                                                  AS quantity,
    item_description                                                                            AS product_description,
    asin                                                                                        AS product_code,
    CAST(NULL AS VARCHAR(50))                                                                   AS invoice_number,
    CAST(NULL AS DATE)                                                                          AS invoice_date,
    CAST(NULL AS NVARCHAR(100))                                                                 AS bill_from_city,
    CAST(NULL AS NVARCHAR(100))                                                                 AS bill_from_state,
    CAST(NULL AS NVARCHAR(100))                                                                 AS bill_from_country,
    CAST(NULL AS NVARCHAR(20))                                                                  AS bill_from_postal_code,
    UPPER(LEFT(ship_to_city,1))  + LOWER(SUBSTRING(ship_to_city,2,LEN(ship_to_city)))          AS ship_to_city,
    UPPER(LEFT(ship_to_state,1)) + LOWER(SUBSTRING(ship_to_state,2,LEN(ship_to_state)))        AS ship_to_state,
    CAST(REPLACE(ship_to_postal_code,'''','') AS NVARCHAR(20))                                  AS ship_to_postal_code,
    payment_method_code                                                                         AS payment_method,
    CAST(NULL AS NVARCHAR(100))                                                                 AS event_sub_type,
    e.[EAN],
    e.[Article Type]                                                                            AS article_type,
    e.[Sku code]                                                                                AS sku_code,
    e.[Product Name]                                                                            AS product_name,
    TRY_CONVERT(DECIMAL(18,2), invoice_amount)                                                  AS MRP,
    warehouse_id,
    platform,
    CAST(NULL AS NVARCHAR(100)) AS seller_code,
    CAST(NULL AS NVARCHAR(100)) AS display_name,
    CAST(NULL AS NVARCHAR(100)) AS company_name,
    CAST(NULL AS NVARCHAR(100)) AS seller_type,
    CAST(NULL AS NVARCHAR(100)) AS brand,
    CAST(NULL AS NVARCHAR(100)) AS sku_name,
    CAST(NULL AS NVARCHAR(100)) AS category_l1,
    CAST(NULL AS NVARCHAR(100)) AS category_l2,
    CAST(NULL AS NVARCHAR(100)) AS category_l3,
    CAST(NULL AS DECIMAL(18,2)) AS display_price,
    CAST(NULL AS DECIMAL(18,2)) AS selling_price,
    CAST(NULL AS INT)           AS total_qty,
    CAST(NULL AS INT)           AS total_orders,
    CAST(NULL AS INT)           AS total_customers
FROM Amazon_B2C_Daily a
LEFT JOIN EAN_Master e ON a.asin = e.[Amazon - ASIN]

UNION ALL

-- ─────────────────────────────────────────────────────────────────────────────
-- SEGMENT 2: FLIPKART B2C
-- Source: Flipkart_B2C_Daily
-- ordertype = 'B2C', salestype = 'Primary'
-- Cancellation/Return/RTO rows must be excluded for net revenue.
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    CAST(order_id AS NVARCHAR(255)),
    fulfilment_type,
    'B2C',
    'Primary',
    CAST(NULL AS NVARCHAR(100)),                          -- transaction_type NULL for Flipkart
    TRY_CONVERT(DATE, order_date),
    CONVERT(VARCHAR(7), TRY_CONVERT(DATE, order_date), 120),
    TRY_CONVERT(INT, item_quantity),
    product_title_description,
    fsn,
    CAST(NULL AS VARCHAR(50)),
    CAST(NULL AS DATE),
    CAST(NULL AS NVARCHAR(100)),
    CAST(NULL AS NVARCHAR(100)),
    CAST(NULL AS NVARCHAR(100)),
    CAST(NULL AS NVARCHAR(20)),
    CAST(NULL AS NVARCHAR(100)),                          -- ship_to_city NULL for Flipkart
    UPPER(LEFT(state,1)) + LOWER(SUBSTRING(state,2,LEN(state))),
    CAST(NULL AS NVARCHAR(20)),                           -- ship_to_postal_code NULL for Flipkart
    CAST(NULL AS NVARCHAR(100)),                          -- payment_method NULL for Flipkart
    event_sub_type,
    e.[EAN],
    e.[Article Type],
    e.[Sku code],
    e.[Product Name],
    TRY_CONVERT(DECIMAL(18,2), final_invoice_amount_price_after_discount_shipping_charges),
    CAST(NULL AS NVARCHAR(100)),                          -- warehouse_id NULL for Flipkart
    platform,
    CAST(NULL AS NVARCHAR(100)), CAST(NULL AS NVARCHAR(100)), CAST(NULL AS NVARCHAR(100)),
    CAST(NULL AS NVARCHAR(100)), CAST(NULL AS NVARCHAR(100)), CAST(NULL AS NVARCHAR(100)),
    CAST(NULL AS NVARCHAR(100)), CAST(NULL AS NVARCHAR(100)), CAST(NULL AS NVARCHAR(100)),
    CAST(NULL AS DECIMAL(18,2)), CAST(NULL AS DECIMAL(18,2)),
    CAST(NULL AS INT), CAST(NULL AS INT), CAST(NULL AS INT)
FROM Flipkart_B2C_Daily f
LEFT JOIN EAN_Master e ON f.fsn = e.[Flipkart - FSN]

UNION ALL

-- ─────────────────────────────────────────────────────────────────────────────
-- SEGMENT 3: MYNTRA B2C
-- Source: Myntra_B2C_Daily
-- ordertype = 'B2C', salestype = 'Primary'
-- quantity is ALWAYS 1 (hardcoded). No cancel-exclusion filter for Myntra.
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    CAST(REPLACE(order_number,'''','') AS NVARCHAR(255)),
    f_type,
    'B2C',
    'Primary',
    CAST(NULL AS NVARCHAR(100)),
    TRY_CONVERT(DATE, order_date),
    CONVERT(VARCHAR(7), TRY_CONVERT(DATE, order_date), 120),
    CAST(1 AS INT),                                       -- quantity always 1 for Myntra
    CAST(NULL AS NVARCHAR(255)),                          -- product_description NULL for Myntra
    CAST(NULL AS NVARCHAR(255)),                          -- product_code NULL for Myntra
    CAST(NULL AS VARCHAR(50)),
    CAST(NULL AS DATE),
    CAST(NULL AS NVARCHAR(100)),
    CAST(NULL AS NVARCHAR(100)),
    CAST(NULL AS NVARCHAR(100)),
    CAST(NULL AS NVARCHAR(20)),
    UPPER(LEFT(city,1))  + LOWER(SUBSTRING(city,2,LEN(city))),
    UPPER(LEFT(state,1)) + LOWER(SUBSTRING(state,2,LEN(state))),
    CAST(NULL AS NVARCHAR(20)),                           -- postal_code NULL for Myntra
    payment_method,
    order_item_status,
    e.[EAN],
    e.[Article Type],
    e.[Sku code],
    e.[Product Name],
    TRY_CONVERT(DECIMAL(18,2), total_customer_paid),
    warehouse_id,
    platform,
    CAST(NULL AS NVARCHAR(100)), CAST(NULL AS NVARCHAR(100)), CAST(NULL AS NVARCHAR(100)),
    CAST(NULL AS NVARCHAR(100)), CAST(NULL AS NVARCHAR(100)), CAST(NULL AS NVARCHAR(100)),
    CAST(NULL AS NVARCHAR(100)), CAST(NULL AS NVARCHAR(100)), CAST(NULL AS NVARCHAR(100)),
    CAST(NULL AS DECIMAL(18,2)), CAST(NULL AS DECIMAL(18,2)),
    CAST(NULL AS INT), CAST(NULL AS INT), CAST(NULL AS INT)
FROM Myntra_B2C_Daily m
LEFT JOIN EAN_Master e ON m.style_id = e.[Myntra - Style Id]

UNION ALL

-- ─────────────────────────────────────────────────────────────────────────────
-- SEGMENT 4: SHOPIFY B2C
-- Source: Shopify_B2C_Daily
-- ordertype = 'B2C', salestype = 'Primary'
-- ⚠ NO order_date — ALL date fields are NULL. Excludes from all date queries.
-- ⚠ NO order_id — use COUNT(*) for Shopify counts.
-- Unfulfilled rows must be excluded for net revenue.
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    CAST(NULL AS NVARCHAR(255)),                          -- order_id NULL for Shopify
    fulfillment_status,
    'B2C',
    'Primary',
    payment_method,                                       -- maps to transaction_type (payment gateway)
    CAST(NULL AS DATE),                                   -- order_date NULL for ALL Shopify rows
    CAST(NULL AS VARCHAR(7)),                             -- month_year NULL for ALL Shopify rows
    TRY_CONVERT(INT, lineitem_quantity),
    lineitem_name,
    CAST(NULL AS NVARCHAR(255)),                          -- product_code NULL for Shopify
    CAST(NULL AS VARCHAR(50)),
    CAST(NULL AS DATE),
    UPPER(LEFT(billing_city,1))     + LOWER(SUBSTRING(billing_city,2,LEN(billing_city))),
    UPPER(LEFT(billing_province,1)) + LOWER(SUBSTRING(billing_province,2,LEN(billing_province))),
    UPPER(LEFT(billing_country,1))  + LOWER(SUBSTRING(billing_country,2,LEN(billing_country))),
    CAST(REPLACE(billing_zip,'''','') AS NVARCHAR(20)),
    UPPER(LEFT(shipping_city,1))     + LOWER(SUBSTRING(shipping_city,2,LEN(shipping_city))),
    UPPER(LEFT(shipping_province,1)) + LOWER(SUBSTRING(shipping_province,2,LEN(shipping_province))),
    CAST(REPLACE(shipping_zip,'''','') AS NVARCHAR(20)),
    payment_method,
    CAST(NULL AS NVARCHAR(100)),                          -- event_sub_type NULL for Shopify
    e.[EAN],
    e.[Article Type],
    e.[Sku code],
    e.[Product Name],
    TRY_CONVERT(DECIMAL(18,2), lineitem_price),
    CAST(NULL AS NVARCHAR(100)),                          -- warehouse_id NULL for Shopify
    platform,
    CAST(NULL AS NVARCHAR(100)), CAST(NULL AS NVARCHAR(100)), CAST(NULL AS NVARCHAR(100)),
    CAST(NULL AS NVARCHAR(100)), CAST(NULL AS NVARCHAR(100)), CAST(NULL AS NVARCHAR(100)),
    CAST(NULL AS NVARCHAR(100)), CAST(NULL AS NVARCHAR(100)), CAST(NULL AS NVARCHAR(100)),
    CAST(NULL AS DECIMAL(18,2)), CAST(NULL AS DECIMAL(18,2)),
    CAST(NULL AS INT), CAST(NULL AS INT), CAST(NULL AS INT)
FROM Shopify_B2C_Daily s
LEFT JOIN EAN_Master e ON s.lineitem_sku = e.[Sku Code]

UNION ALL

-- ─────────────────────────────────────────────────────────────────────────────
-- SEGMENT 5: NYKAA B2B SECONDARY
-- Source: Nykaa_B2B_Secondary_Daily
-- ordertype = 'B2B', salestype = 'Secondary'
-- ⚠ PRE-AGGREGATED rows — each row = SKU × date totals.
-- ⚠ Secondary salestype — NOT brand direct revenue.
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    CAST(NULL AS NVARCHAR(255)),
    CAST(NULL AS NVARCHAR(100)),
    'B2B',
    'Secondary',
    CAST(NULL AS NVARCHAR(100)),
    TRY_CONVERT(DATE, date),
    CONVERT(VARCHAR(7), TRY_CONVERT(DATE, date), 120),
    TRY_CONVERT(INT, total_qty),
    CAST(NULL AS NVARCHAR(255)),
    CAST(NULL AS NVARCHAR(255)),
    CAST(NULL AS VARCHAR(50)),
    CAST(NULL AS DATE),
    CAST(NULL AS NVARCHAR(100)),
    CAST(NULL AS NVARCHAR(100)),
    CAST(NULL AS NVARCHAR(100)),
    CAST(NULL AS NVARCHAR(20)),
    CAST(NULL AS NVARCHAR(100)),
    CAST(NULL AS NVARCHAR(100)),
    CAST(NULL AS NVARCHAR(20)),
    CAST(NULL AS NVARCHAR(100)),
    CAST(NULL AS NVARCHAR(100)),
    e.[EAN],
    e.[Article Type],
    e.[Sku code],
    e.[Product Name],
    TRY_CONVERT(DECIMAL(18,2), n.mrp),
    CAST(NULL AS NVARCHAR(100)),
    platform,
    seller_code,
    display_name,
    company_name,
    seller_type,
    brand,
    sku_name,
    category_l1,
    category_l2,
    category_l3,
    TRY_CONVERT(DECIMAL(18,2), display_price),
    TRY_CONVERT(DECIMAL(18,2), selling_price),
    TRY_CONVERT(INT, total_qty),
    TRY_CONVERT(INT, total_orders),
    TRY_CONVERT(INT, total_customers)
FROM Nykaa_B2B_Secondary_Daily n
LEFT JOIN EAN_Master e ON n.sku_code = e.[Sku Code]

UNION ALL

-- ─────────────────────────────────────────────────────────────────────────────
-- SEGMENT 6: ZEPTO B2B SECONDARY
-- Source: Zepto_B2B_Secondary_Daily
-- ordertype = 'B2B', salestype = 'Secondary'
-- ⚠ Very sparse data (~3 rows). Pre-aggregated like Nykaa.
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    CAST(NULL AS NVARCHAR(255)),
    CAST(NULL AS NVARCHAR(100)),
    'B2B',
    'Secondary',
    CAST(NULL AS NVARCHAR(100)),
    TRY_CONVERT(DATE, date),
    CONVERT(VARCHAR(7), TRY_CONVERT(DATE, date), 120),
    TRY_CONVERT(INT, sales_qty_units),
    CAST(NULL AS NVARCHAR(255)),
    CAST(NULL AS NVARCHAR(255)),
    CAST(NULL AS VARCHAR(50)),
    CAST(NULL AS DATE),
    CAST(NULL AS NVARCHAR(100)),
    CAST(NULL AS NVARCHAR(100)),
    CAST(NULL AS NVARCHAR(100)),
    CAST(NULL AS NVARCHAR(20)),
    CAST(NULL AS NVARCHAR(100)),
    CAST(NULL AS NVARCHAR(100)),
    CAST(NULL AS NVARCHAR(20)),
    CAST(NULL AS NVARCHAR(100)),
    CAST(NULL AS NVARCHAR(100)),
    e.[EAN],
    e.[Article Type],
    e.[Sku code],
    e.[Product Name],
    TRY_CONVERT(DECIMAL(18,2), z.mrp),
    CAST(NULL AS NVARCHAR(100)),
    platform,
    CAST(NULL AS NVARCHAR(100)),
    CAST(NULL AS NVARCHAR(100)),
    CAST(NULL AS NVARCHAR(100)),
    CAST(NULL AS NVARCHAR(100)),
    brand_name,
    sku_name,
    CAST(NULL AS NVARCHAR(100)),
    CAST(NULL AS NVARCHAR(100)),
    CAST(NULL AS NVARCHAR(100)),
    CAST(NULL AS DECIMAL(18,2)),
    CAST(NULL AS DECIMAL(18,2)),
    TRY_CONVERT(INT, sales_qty_units),
    CAST(NULL AS INT),
    CAST(NULL AS INT)
FROM Zepto_B2B_Secondary_Daily z
LEFT JOIN EAN_Master e ON z.ean = e.[Zepto - EAN]

UNION ALL 

-- ─────────────────────────────────────────────────────────────────────────────
-- SEGMENT 7: AMAZON B2B (BUSINESS)
-- Source: Amazon_B2B_Daily
-- ordertype = 'B2B', salestype = 'Primary'
-- No cancellation filter for B2B orders.
-- Amazon is the ONLY platform with BOTH B2C and B2B rows.
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    CAST(order_id AS NVARCHAR(255)),
    CAST(NULL AS NVARCHAR(100)),
    'B2B',
    'Primary',
    CAST(NULL AS NVARCHAR(100)),
    TRY_CONVERT(DATE, order_date),
    CONVERT(VARCHAR(7), TRY_CONVERT(DATE, order_date), 120),
    TRY_CONVERT(INT, quantity),
    CAST(NULL AS NVARCHAR(255)),
    CAST(NULL AS NVARCHAR(255)),
    CAST(NULL AS VARCHAR(50)),
    CAST(NULL AS DATE),
    CAST(NULL AS NVARCHAR(100)),
    CAST(NULL AS NVARCHAR(100)),
    CAST(NULL AS NVARCHAR(100)),
    CAST(NULL AS NVARCHAR(20)),
    UPPER(LEFT(ship_to_city,1))  + LOWER(SUBSTRING(ship_to_city,2,LEN(ship_to_city))),
    UPPER(LEFT(ship_to_state,1)) + LOWER(SUBSTRING(ship_to_state,2,LEN(ship_to_state))),
    CAST(ship_to_postal_code AS NVARCHAR(20)),
    payment_method_code,
    CAST(NULL AS NVARCHAR(100)),
    e.[EAN],
    e.[Article Type],
    e.[Sku code],
    e.[Product Name],
    TRY_CONVERT(DECIMAL(18,2), invoice_amount),
    CAST(NULL AS NVARCHAR(100)),
    platform,
    CAST(NULL AS NVARCHAR(100)), CAST(NULL AS NVARCHAR(100)), CAST(NULL AS NVARCHAR(100)),
    CAST(NULL AS NVARCHAR(100)), CAST(NULL AS NVARCHAR(100)), CAST(NULL AS NVARCHAR(100)),
    CAST(NULL AS NVARCHAR(100)), CAST(NULL AS NVARCHAR(100)), CAST(NULL AS NVARCHAR(100)),
    CAST(NULL AS DECIMAL(18,2)), CAST(NULL AS DECIMAL(18,2)),
    CAST(NULL AS INT), CAST(NULL AS INT), CAST(NULL AS INT)
FROM Amazon_B2B_Daily ab
LEFT JOIN EAN_Master e ON ab.asin = e.[Amazon - ASIN];
GO

-- ==============================================================================
-- ALWAYS QUERY THIS VIEW AS:
--   SELECT ... FROM [dbo].[B2B_B2C]
-- or fully qualified:
--   SELECT ... FROM [Charmacy_f_automate].[dbo].[B2B_B2C]
-- NEVER query the underlying source tables directly.
-- ==============================================================================