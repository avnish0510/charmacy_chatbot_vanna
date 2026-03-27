-- ==============================================================================
-- FILE: training/ddl/02_Flipkart_B2C_Daily.sql
-- PURPOSE: DDL for the Flipkart B2C source table.
-- PLATFORM: Flipkart  |  ORDER TYPE: B2C  |  SALES TYPE: Primary
-- ROW GRAIN: One row = one line-item in one customer order.
-- ROW COUNT: ~372 rows (as of Jan 2026 snapshot, grows daily).
-- ==============================================================================

USE [Charmacy_f_automate];
GO

-- ------------------------------------------------------------------------------
-- Flipkart_B2C_Daily
-- Flipkart B2C orders exported daily.
-- Joined to EAN_Master on: f.fsn = e.[Flipkart - FSN]
-- ------------------------------------------------------------------------------
CREATE TABLE [dbo].[Flipkart_B2C_Daily] (
    -- Order identification
    order_id            NVARCHAR(255) NULL,  -- Flipkart order number, e.g. OD436456263392762100
                                             -- One order can have MULTIPLE rows (one per product)
                                             -- → Count orders with COUNT(DISTINCT order_id)

    -- Fulfilment type
    fulfilment_type     NVARCHAR(50)  NULL,  -- Values: FBF (Flipkart Fulfilled), NON_FBF (seller fulfilled)

    -- Event type — CRITICAL for net sales filter
    event_sub_type      NVARCHAR(50)  NULL,  -- Values: Sale, Cancellation, Return, RTO
                                             -- Net filter: EXCLUDE Cancellation, Return, RTO
                                             -- WHERE NOT (platform='Flipkart' AND
                                             --   ISNULL(event_sub_type,'') IN ('Cancellation','Return','RTO'))
                                             -- For Flipkart order count: WHERE event_sub_type = 'Sale'

    -- Date
    order_date          NVARCHAR(50)  NULL,  -- Stored as string; TRY_CONVERT(DATE,...) in view
                                             -- ALL Flipkart rows have a valid order_date

    -- Quantity
    item_quantity       NVARCHAR(20)  NULL,  -- Maps to quantity in view; TRY_CONVERT(INT,...)

    -- Product description (raw listing text)
    product_title_description NVARCHAR(MAX) NULL,
                                             -- Maps to product_description in view
                                             -- Do NOT use for grouping — use product_name from EAN_Master

    -- Platform product code
    fsn                 NVARCHAR(50)  NULL,  -- Flipkart Serial Number
                                             -- JOIN KEY to EAN_Master.[Flipkart - FSN]
                                             -- Maps to product_code in view
                                             -- Example: FNDGHV97M6UH6MFQ

    -- Geography (shipping destination)
    -- NOTE: Flipkart provides state only; no city or postal code
    state               NVARCHAR(100) NULL,  -- Maps to ship_to_state in view
                                             -- View applies title-case; mixed formats (e.g. 'Mh'/'Maharashtra')
                                             -- ship_to_city and ship_to_postal_code are NULL for all Flipkart rows

    -- Financial
    final_invoice_amount_price_after_discount_shipping_charges NVARCHAR(30) NULL,
                                             -- Maps to MRP in view via TRY_CONVERT(DECIMAL(18,2),...)
                                             -- Total line-item amount after discount + shipping charges applied
                                             -- NOT unit price

    -- Platform tag (always 'Flipkart' for this table)
    platform            NVARCHAR(50)  NULL
);
GO

-- IMPORTANT NOTES FOR SQL GENERATION:
-- 1. Always exclude Cancellation/Return/RTO from revenue queries.
-- 2. Use COUNT(DISTINCT order_id) for Flipkart order counts — never COUNT(*).
-- 3. For "Flipkart orders placed" specifically, filter: WHERE event_sub_type = 'Sale'.
-- 4. ship_to_city and ship_to_postal_code are NULL for ALL Flipkart rows.
-- 5. state has dual-spelling problem: 'Maharashtra' AND 'Mh' both exist — use IN().
-- 6. fsn links to EAN_Master for product_name, article_type, sku_code, EAN.