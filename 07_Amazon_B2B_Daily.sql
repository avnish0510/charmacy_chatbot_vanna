-- ==============================================================================
-- FILE: training/ddl/07_Amazon_B2B_Daily.sql
-- PURPOSE: DDL for the Amazon B2B (Business) source table.
-- PLATFORM: Amazon  |  ORDER TYPE: B2B  |  SALES TYPE: Primary
-- ROW GRAIN: One row = one line-item in one B2B customer order.
-- NOTE: Amazon is the ONLY platform with BOTH B2C and B2B rows in B2B_B2C.
--       Amazon B2B rows have ordertype='B2B', salestype='Primary'.
--       Amazon B2C rows have ordertype='B2C', salestype='Primary'.
-- ==============================================================================

USE [Charmacy_f_automate];
GO

-- ------------------------------------------------------------------------------
-- Amazon_B2B_Daily
-- Amazon Business (B2B) orders — bulk/corporate buyers on Amazon Business.
-- Joined to EAN_Master on: ab.asin = e.[Amazon - ASIN]
-- ------------------------------------------------------------------------------
CREATE TABLE [dbo].[Amazon_B2B_Daily] (
    -- Order identification
    order_id            NVARCHAR(255) NULL,  -- Amazon Business order number
                                             -- One order can have MULTIPLE rows (one per product)
                                             -- → Count orders with COUNT(DISTINCT order_id)

    -- Date
    order_date          NVARCHAR(50)  NULL,  -- Stored as string; TRY_CONVERT(DATE,...) in view
                                             -- B2B orders generally have valid dates

    -- Quantity
    quantity            NVARCHAR(20)  NULL,  -- Units ordered; TRY_CONVERT(INT,...) in view

    -- Product identification
    asin                NVARCHAR(20)  NULL,  -- Amazon Standard Identification Number
                                             -- JOIN KEY to EAN_Master.[Amazon - ASIN]
                                             -- product_description and product_code are NULL in view for B2B

    -- Financial
    invoice_amount      NVARCHAR(30)  NULL,  -- Maps to MRP in view via TRY_CONVERT(DECIMAL(18,2),...)
                                             -- Total line-item invoice amount (NOT unit price)

    -- Geography
    ship_to_city        NVARCHAR(100) NULL,  -- Maps to ship_to_city in view (title-case applied)
    ship_to_state       NVARCHAR(100) NULL,  -- Maps to ship_to_state in view (title-case applied)
                                             -- Mixed format dual spellings apply
    ship_to_postal_code NVARCHAR(20)  NULL,  -- Postal code (no quote-stripping unlike B2C)

    -- Payment
    payment_method_code NVARCHAR(50)  NULL,  -- Maps to payment_method in view

    -- Platform tag (always 'Amazon' for this table)
    platform            NVARCHAR(50)  NULL

    -- COLUMNS NOT IN Amazon B2B (NULL in view):
    -- fulfilment_type, transaction_type, event_sub_type (no cancel flag for B2B)
    -- warehouse_id, bill_from_*, all Nykaa-specific columns
);
GO

-- IMPORTANT NOTES FOR SQL GENERATION:
-- 1. Amazon B2B is PRIMARY salestype — it IS brand revenue (unlike Nykaa/Zepto).
-- 2. To query only Amazon B2B: WHERE platform = 'Amazon' AND ordertype = 'B2B'
-- 3. To query only Amazon B2C: WHERE platform = 'Amazon' AND ordertype = 'B2C'
-- 4. Use COUNT(DISTINCT order_id) for Amazon B2B order counts.
-- 5. No transaction_type (cancel flag) for B2B — no cancellation filter needed.
-- 6. asin links to EAN_Master for product_name, article_type, sku_code, EAN.
-- 7. In B2B_B2C view, both Amazon tables appear as platform='Amazon';
--    distinguish by: ordertype='B2B' (Amazon_B2B_Daily) vs ordertype='B2C' (Amazon_B2C_Daily).