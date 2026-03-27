-- ==============================================================================
-- FILE: training/ddl/01_Amazon_B2C_Daily.sql
-- PURPOSE: DDL for the Amazon B2C source table.
-- PLATFORM: Amazon  |  ORDER TYPE: B2C  |  SALES TYPE: Primary
-- ROW GRAIN: One row = one line-item in one customer order.
-- ROW COUNT: ~464 rows (as of Jan 2026 snapshot, grows daily).
-- ==============================================================================

USE [Charmacy_f_automate];
GO

-- ------------------------------------------------------------------------------
-- Amazon_B2C_Daily
-- Amazon direct-to-consumer orders exported daily.
-- Joined to EAN_Master on: a.asin = e.[Amazon - ASIN]
-- ------------------------------------------------------------------------------
CREATE TABLE [dbo].[Amazon_B2C_Daily] (
    -- Order identification
    order_id            NVARCHAR(255) NULL,  -- Amazon order number, e.g. 404-8134387-1934718
                                             -- One order can have MULTIPLE rows (one per product)
                                             -- → Count orders with COUNT(DISTINCT order_id)

    -- Event type — CRITICAL for net sales filter
    transaction_type    NVARCHAR(100) NULL,  -- Values: Shipment, Cancel, Refund, FreeReplacement
                                             -- Net filter: EXCLUDE Cancel, Refund, FreeReplacement
                                             -- WHERE NOT (platform='Amazon' AND
                                             --   ISNULL(transaction_type,'') IN ('Cancel','Refund','FreeReplacement'))

    -- Date
    order_date          NVARCHAR(50)  NULL,  -- Stored as string in source; TRY_CONVERT(DATE,...) in view
                                             -- ~87 rows have NULL order_date (older unmapped rows)

    -- Quantity
    quantity            NVARCHAR(20)  NULL,  -- Stored as string; TRY_CONVERT(INT,...) in view

    -- Product description (raw listing text — do NOT use for grouping)
    item_description    NVARCHAR(MAX) NULL,  -- Maps to product_description in view
                                             -- Example: "Charmacy Milano Intense Eyebrow Filler Brown..."
                                             -- Use product_name from EAN_Master instead

    -- Platform product code
    asin                NVARCHAR(20)  NULL,  -- Amazon Standard Identification Number
                                             -- JOIN KEY to EAN_Master.[Amazon - ASIN]
                                             -- Maps to product_code in view

    -- Financial
    invoice_amount      NVARCHAR(30)  NULL,  -- TRY_CONVERT(DECIMAL(18,2),...) in view → MRP
                                             -- This is the TOTAL LINE-ITEM amount (qty × unit price)
                                             -- NOT unit price. SUM(invoice_amount) = revenue.

    -- Geography (shipping destination)
    ship_to_city        NVARCHAR(100) NULL,  -- City name; view applies title-case normalisation
    ship_to_state       NVARCHAR(100) NULL,  -- State name; view applies title-case normalisation
                                             -- Has MIXED formats: 'Punjab' and 'Pb' both appear
                                             -- Always use IN() with both spellings in queries
    ship_to_postal_code NVARCHAR(20)  NULL,  -- View applies REPLACE to strip embedded quotes

    -- Payment
    payment_method_code NVARCHAR(50)  NULL,  -- Maps to payment_method in view
                                             -- Example values: COD, Razorpay, NB, CC, GC

    -- Warehouse
    warehouse_id        NVARCHAR(20)  NULL,  -- Amazon fulfilment centre ID, e.g. PMZJ
                                             -- NULL for ~60% of Amazon rows

    -- Platform tag (always 'Amazon' for this table)
    platform            NVARCHAR(50)  NULL
);
GO

-- IMPORTANT NOTES FOR SQL GENERATION:
-- 1. Always exclude Cancels/Refunds/FreeReplacements from revenue queries.
-- 2. Use COUNT(DISTINCT order_id) for Amazon order counts — never COUNT(*).
-- 3. order_date may be NULL for ~87 rows; add AND order_date IS NOT NULL in date filters.
-- 4. asin links to EAN_Master for product_name, article_type, sku_code, EAN.
-- 5. invoice_amount = total line-item amount, NOT unit price.