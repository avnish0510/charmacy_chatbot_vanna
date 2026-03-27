-- ==============================================================================
-- FILE: training/ddl/04_Shopify_B2C_Daily.sql
-- PURPOSE: DDL for the Shopify B2C source table.
-- PLATFORM: Shopify  |  ORDER TYPE: B2C  |  SALES TYPE: Primary
-- ROW GRAIN: One row = one line-item in one D2C order.
-- ROW COUNT: ~7,716 rows (~80% of ALL rows in B2B_B2C), grows daily.
--
-- ⚠  CRITICAL: Shopify has NO order_date — all date fields are NULL.
--    Any date-filtered query automatically excludes ALL Shopify revenue.
--    Shopify also has NO order_id — use COUNT(*) for order counts.
-- ==============================================================================

USE [Charmacy_f_automate];
GO

-- ------------------------------------------------------------------------------
-- Shopify_B2C_Daily
-- Charmacy Milano's own D2C website orders (powered by Shopify), exported daily.
-- Joined to EAN_Master on: s.lineitem_sku = e.[Sku Code]
-- ------------------------------------------------------------------------------
CREATE TABLE [dbo].[Shopify_B2C_Daily] (
    -- Order identification
    -- NOTE: Shopify has NO order_id. order_id is NULL for all rows in B2B_B2C.
    -- Each row = 1 line-item. Use COUNT(*) for Shopify order/transaction counts.

    -- Fulfilment status — CRITICAL for net sales filter
    fulfillment_status  NVARCHAR(50)  NULL,  -- Maps to fulfilment_type in view
                                             -- Values: 'fulfilled', 'unfulfilled'
                                             -- Net filter: EXCLUDE unfulfilled rows
                                             -- WHERE NOT (platform='Shopify' AND
                                             --   ISNULL(fulfilment_type,'') = 'unfulfilled')

    -- Payment / transaction type
    payment_method      NVARCHAR(100) NULL,  -- Maps to BOTH transaction_type AND payment_method in view
                                             -- (Shopify reuses same column for payment gateway name)
                                             -- Values: Cash on Delivery (COD), Razorpay, NB, GC,
                                             --         Installments, POA, manual, gift_card

    -- Date — ⚠ ALL NULL IN SHOPIFY
    -- order_date is NULL for ALL Shopify rows.
    -- order_date and month_year are NULL in view for all Shopify rows.
    -- NO date column exists in Shopify_B2C_Daily source table.

    -- Quantity
    lineitem_quantity   NVARCHAR(20)  NULL,  -- Maps to quantity in view; TRY_CONVERT(INT,...)

    -- Product info
    lineitem_name       NVARCHAR(MAX) NULL,  -- Maps to product_description in view
                                             -- Raw Shopify line-item name, e.g. 'CMC MATTE FOUNDATION - #MF-03'
                                             -- Do NOT use for grouping — use product_name from EAN_Master
    lineitem_sku        NVARCHAR(100) NULL,  -- Internal SKU code
                                             -- JOIN KEY to EAN_Master.[Sku Code]
                                             -- Maps to product_code = NULL in view (not exposed)

    -- Financial
    lineitem_price      NVARCHAR(30)  NULL,  -- Maps to MRP in view via TRY_CONVERT(DECIMAL(18,2),...)
                                             -- Total line-item price charged to customer
                                             -- Example: 989.00 (Matte Foundation), 399.00 (Beauty Blender)

    -- Billing address (Shopify only — brand's billing info)
    billing_city        NVARCHAR(100) NULL,  -- Maps to bill_from_city in view
    billing_province    NVARCHAR(100) NULL,  -- Maps to bill_from_state in view
    billing_country     NVARCHAR(100) NULL,  -- Maps to bill_from_country in view; typically 'In'
    billing_zip         NVARCHAR(20)  NULL,  -- Maps to bill_from_postal_code in view

    -- Shipping address (customer destination)
    shipping_city       NVARCHAR(100) NULL,  -- Maps to ship_to_city in view (title-case applied)
    shipping_province   NVARCHAR(100) NULL,  -- Maps to ship_to_state in view (title-case applied)
                                             -- Mixed format: 'Up' / 'Uttar pradesh', 'Mh' / 'Maharashtra' etc.
    shipping_zip        NVARCHAR(20)  NULL,  -- Maps to ship_to_postal_code in view

    -- Platform tag (always 'Shopify' for this table)
    platform            NVARCHAR(50)  NULL
);
GO

-- IMPORTANT NOTES FOR SQL GENERATION:
-- 1. ⚠  Shopify has NO order_date. ALL date filters exclude Shopify automatically.
--        Time-series analysis for Shopify is not possible with this data.
-- 2. ⚠  Shopify has NO order_id (NULL in view). Use COUNT(*) for Shopify counts.
--        NEVER use COUNT(DISTINCT order_id) for Shopify.
-- 3. Shopify is ~80% of all rows in B2B_B2C (~7,716 of ~9,670 total rows).
--    Cross-platform date-filtered totals are Shopify-exclusive by design.
-- 4. Net filter: exclude WHERE fulfilment_type = 'unfulfilled'.
-- 5. bill_from_* columns are populated ONLY for Shopify (brand's own address).
-- 6. lineitem_sku joins to EAN_Master for product_name, article_type, sku_code, EAN.
-- 7. Products like 'CMC BEAUTY BLENDER', 'CMC FINGER BLENDER' are NOT in EAN_Master
--    → product_name, article_type, sku_code, EAN will all be NULL for these rows.
--    These accessories MRP = ₹399 (Beauty Blender) and ₹99 (Finger Blender).