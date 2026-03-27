-- ==============================================================================
-- FILE: training/ddl/03_Myntra_B2C_Daily.sql
-- PURPOSE: DDL for the Myntra B2C source table.
-- PLATFORM: Myntra  |  ORDER TYPE: B2C  |  SALES TYPE: Primary
-- ROW GRAIN: One row = one product in one order (quantity is always 1).
-- ROW COUNT: ~714 rows (as of Jan 2026 snapshot, grows daily).
-- ==============================================================================

USE [Charmacy_f_automate];
GO

-- ------------------------------------------------------------------------------
-- Myntra_B2C_Daily
-- Myntra B2C orders exported daily.
-- Joined to EAN_Master on: m.style_id = e.[Myntra - Style Id]
-- ------------------------------------------------------------------------------
CREATE TABLE [dbo].[Myntra_B2C_Daily] (
    -- Order identification
    order_number        NVARCHAR(255) NULL,  -- Myntra order number, e.g. 131960898155027381203
                                             -- View: CAST(REPLACE(order_number,'''','') AS NVARCHAR(255))
                                             -- One order may span multiple rows
                                             -- → Count orders with COUNT(DISTINCT order_id) in B2B_B2C

    -- Fulfilment type
    f_type              NVARCHAR(10)  NULL,  -- Maps to fulfilment_type in view
                                             -- Values: PPMP (Myntra Fulfilled), SJIT (seller fulfilled)

    -- Event type — used for net sales filtering
    order_item_status   NVARCHAR(20)  NULL,  -- Maps to event_sub_type in view
                                             -- Values: SH (shipped/delivered), PK (packed),
                                             --         F (forward), WP (warehouse processing)
                                             -- NOTE: Myntra cancellation/return logic not in this field;
                                             --       Myntra net sales filter is NOT applied (no cancel flag)

    -- Date
    order_date          NVARCHAR(50)  NULL,  -- All Myntra rows have a valid order_date
                                             -- TRY_CONVERT(DATE,...) applied in view

    -- Quantity
    -- NOTE: Myntra quantity is ALWAYS 1 per row (view hardcodes CAST(1 AS INT))
    -- No quantity column in source; view sets quantity = 1 for all Myntra rows

    -- Product info
    style_id            NVARCHAR(50)  NULL,  -- Myntra style ID — JOIN KEY to EAN_Master.[Myntra - Style Id]
                                             -- product_description and product_code are NULL for Myntra in view

    -- Geography
    city                NVARCHAR(100) NULL,  -- Maps to ship_to_city in view (title-case applied)
    state               NVARCHAR(100) NULL,  -- Maps to ship_to_state in view (title-case applied)
                                             -- Mixed format dual spellings apply
                                             -- ship_to_postal_code is NULL for all Myntra rows

    -- Payment
    payment_method      NVARCHAR(100) NULL,  -- Maps to payment_method in view

    -- Financial
    total_customer_paid NVARCHAR(30)  NULL,  -- Maps to MRP in view via TRY_CONVERT(DECIMAL(18,2),...)
                                             -- Total amount paid by customer for this line-item
                                             -- Example: 674.00, 1011.00

    -- Warehouse
    warehouse_id        NVARCHAR(20)  NULL,  -- Myntra warehouse code, e.g. 63664

    -- Platform tag (always 'Myntra' for this table)
    platform            NVARCHAR(50)  NULL
);
GO

-- IMPORTANT NOTES FOR SQL GENERATION:
-- 1. Myntra quantity is ALWAYS 1. SUM(quantity) = row count for Myntra.
-- 2. Use COUNT(DISTINCT order_id) for Myntra order counts.
-- 3. All Myntra rows have valid order_date and month_year.
-- 4. ship_to_postal_code is NULL for all Myntra rows.
-- 5. style_id links to EAN_Master for product_name, article_type, sku_code, EAN.
-- 6. MRP (total_customer_paid) may differ from Amazon/Shopify for same product — platform pricing varies.
--    Example: Matte Foundation 03 = ₹989 on Shopify, ₹1011 on Myntra.