-- ==============================================================================
-- FILE: training/ddl/05_Nykaa_B2B_Secondary_Daily.sql
-- PURPOSE: DDL for the Nykaa B2B Secondary source table.
-- PLATFORM: Nykaa  |  ORDER TYPE: B2B  |  SALES TYPE: Secondary
-- ROW GRAIN: One row = all orders for ONE SKU on ONE date (pre-aggregated).
-- ROW COUNT: ~401 rows (as of Jan 2026 snapshot, grows daily).
--
-- ⚠  CRITICAL: Nykaa rows are PRE-AGGREGATED at SKU × date level.
--    Each row does NOT represent a single order — it represents daily totals.
--    This is a Secondary / sell-through channel, NOT brand direct revenue.
--    NEVER mix Nykaa revenue with Primary salestype revenue (Amazon/Flipkart/etc.)
-- ==============================================================================

USE [Charmacy_f_automate];
GO

-- ------------------------------------------------------------------------------
-- Nykaa_B2B_Secondary_Daily
-- Nykaa marketplace sell-through data exported daily.
-- Joined to EAN_Master on: n.sku_code = e.[Sku Code]
-- ------------------------------------------------------------------------------
CREATE TABLE [dbo].[Nykaa_B2B_Secondary_Daily] (
    -- Date (aggregation period)
    date                NVARCHAR(50)  NULL,  -- Stored as string; TRY_CONVERT(DATE,...) in view
                                             -- Represents the date these aggregated sales occurred
                                             -- All Nykaa rows have a valid date → mapped to order_date in view

    -- Aggregated quantity metrics
    total_qty           NVARCHAR(20)  NULL,  -- Total units sold on this date for this SKU
                                             -- Maps to BOTH quantity AND total_qty in view
                                             -- SUM(quantity) works correctly for Nykaa

    -- Aggregated order & customer counts
    total_orders        NVARCHAR(20)  NULL,  -- Total orders on this date for this SKU
                                             -- Maps to total_orders in view
                                             -- For Nykaa order count: SUM(total_orders) — NOT COUNT(*)
    total_customers     NVARCHAR(20)  NULL,  -- Unique customers on this date for this SKU
                                             -- Maps to total_customers in view

    -- Financial — Nykaa has two price columns
    mrp                 NVARCHAR(30)  NULL,  -- Listed/maximum retail price
                                             -- Maps to MRP column in view via TRY_CONVERT(DECIMAL(18,2),...)
                                             -- This is the PRE-DISCOUNT price per unit (Nykaa-specific)
    display_price       NVARCHAR(30)  NULL,  -- Price shown to customer before discount
                                             -- Maps to display_price in view
    selling_price       NVARCHAR(30)  NULL,  -- Actual price after discount
                                             -- Maps to selling_price in view
                                             -- Discount % = (display_price - selling_price) / display_price * 100

    -- Product identification
    sku_code            NVARCHAR(100) NULL,  -- JOIN KEY to EAN_Master.[Sku Code]
                                             -- Example: CMS_F1, CMCCON_3, CMC021

    -- Seller information (Nykaa-specific — NULL for all other platforms)
    seller_code         NVARCHAR(50)  NULL,  -- Nykaa seller identifier, e.g. 2953
    display_name        NVARCHAR(255) NULL,  -- Seller display name on Nykaa, e.g. 'Charmacy Milano'
    company_name        NVARCHAR(255) NULL,  -- Seller company name, e.g. 'Charmacy Milano'
    seller_type         NVARCHAR(100) NULL,  -- Seller classification, e.g. 'Brand Company'

    -- Brand
    brand               NVARCHAR(100) NULL,  -- Always 'Charmacy Milano' for Nykaa rows

    -- Product display name (Nykaa marketplace listing name)
    sku_name            NVARCHAR(MAX) NULL,  -- Nykaa marketplace product display name
                                             -- Example: 'Charmacy Milano Insane Shifters Eyeshadow - 503...'
                                             -- Different from product_name in EAN_Master

    -- Category hierarchy (Nykaa-specific taxonomy)
    category_l1         NVARCHAR(100) NULL,  -- Always 'Makeup' for Charmacy Milano on Nykaa
    category_l2         NVARCHAR(100) NULL,  -- Values: Eyes, Face, Lips, Makeup Kits & Combos
    category_l3         NVARCHAR(100) NULL,  -- Granular: Foundation, Concealer, Eye Shadow, etc.

    -- Platform tag (always 'Nykaa' for this table)
    platform            NVARCHAR(50)  NULL
);
GO

-- IMPORTANT NOTES FOR SQL GENERATION:
-- 1. ⚠  Nykaa is SECONDARY salestype. NEVER add Nykaa MRP to Primary revenue sums.
--        Always filter: WHERE salestype = 'Primary' for brand revenue analysis.
-- 2. ⚠  Each row = aggregated day totals, NOT individual orders.
--        For Nykaa order count: use SUM(total_orders) — NOT COUNT(*) or COUNT(DISTINCT order_id).
-- 3. SUM(quantity) works for Nykaa units (quantity = total_qty in view).
-- 4. No ship_to_state or geographic data for Nykaa — geography columns are NULL.
-- 5. MRP on Nykaa may differ from other platforms for the same product.
-- 6. Nykaa discount: SELECT (SUM(display_price)-SUM(selling_price))/NULLIF(SUM(display_price),0)*100
--    FROM [dbo].[B2B_B2C] WHERE platform='Nykaa'
-- 7. All Nykaa rows have valid order_date (date field populated).
-- 8. sku_code links to EAN_Master for product_name, article_type, EAN.