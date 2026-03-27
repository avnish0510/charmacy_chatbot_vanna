-- ==============================================================================
-- FILE: training/ddl/00_EAN_Master.sql
-- PURPOSE: DDL for the EAN_Master lookup table.
-- ROLE: Every source table LEFT JOINs to this table to resolve
--       product_name, article_type, sku_code, and EAN.
--       ~34% of rows in B2B_B2C have NULL product fields because the
--       product (e.g. Beauty Blender, Finger Blender accessories) is
--       not in EAN_Master.
-- ALWAYS TRAINED FIRST — all source-table DDL references this table.
-- ==============================================================================

USE [Charmacy_f_automate];
GO

-- ------------------------------------------------------------------------------
-- EAN_Master
-- The single product master for Charmacy Milano.
-- 134 distinct mapped products.
-- All Charmacy EANs start with 8906148 (GS1 India prefix).
-- Columns are named with spaces and brackets — always quote them in SQL.
-- ------------------------------------------------------------------------------
CREATE TABLE [dbo].[EAN_Master] (
    -- Product identifiers — each platform uses a different code
    [EAN]              NVARCHAR(13)  NULL,   -- Global barcode; starts with 8906148
    [Sku code]         NVARCHAR(50)  NULL,   -- Internal SKU code, e.g. CMS_F1, CMCCON_3
    [Product Name]     NVARCHAR(255) NULL,   -- Canonical product name; USE THIS for grouping

    -- Platform-specific product codes (JOIN keys per source table)
    [Amazon - ASIN]    NVARCHAR(20)  NULL,   -- Amazon Standard Identification Number
    [Flipkart - FSN]   NVARCHAR(50)  NULL,   -- Flipkart Serial Number
    [Myntra - Style Id] NVARCHAR(20) NULL,   -- Myntra style identifier
    -- Shopify and Nykaa join on [Sku code] directly

    -- Product classification
    [Article Type]     NVARCHAR(100) NULL,   -- Product category
                                             -- 17 values: Foundation, Concealer, Lipstick,
                                             -- Liquid Lipstick, Mascara, Eyeliner, Eyeshadow,
                                             -- Eyebrow Enhancer, Blush, Highlighter,
                                             -- Loose Powder, Compact, Face Primer, Lip Balm,
                                             -- Lip Liner, PREP & SET, (others)

    -- Zepto-specific key
    [Zepto - EAN]      NVARCHAR(13)  NULL    -- EAN as Zepto records it (may match [EAN])
);
GO

-- JOIN KEY REFERENCE (how each source table links to EAN_Master):
-- Amazon_B2C_Daily      → a.asin     = e.[Amazon - ASIN]
-- Amazon_B2B_Daily      → ab.asin    = e.[Amazon - ASIN]
-- Flipkart_B2C_Daily    → f.fsn      = e.[Flipkart - FSN]
-- Myntra_B2C_Daily      → m.style_id = e.[Myntra - Style Id]
-- Shopify_B2C_Daily     → s.lineitem_sku = e.[Sku Code]
-- Nykaa_B2B_Secondary_Daily → n.sku_code = e.[Sku Code]
-- Zepto_B2B_Secondary_Daily → z.ean  = e.[Zepto - EAN]

-- SKU NAMING CONVENTIONS:
-- CMS_F1  .. CMS_F12   = Matte Foundation shades 1-12
-- CMCCON_1 .. CMCCON_4 = Concealer shades 1-4
-- CMCDR_77 .. CMCDR_82 = Diamond Rush Lipstick shades
-- CMC021               = Set and Fix Loose Powder
-- CMC022               = Pro-Pore Conceal Primer
-- CMCSD_01 onward      = Star Dust Highlighters
-- CMCHDCC_01 onward    = HD Cover Compacts
-- CMCSE_01 onward      = Stellar Eyeliners
-- CMCZES_nnn           = Zodiac Duochrome Eyeshadow Sticks
-- CMC007_01/02         = Intense Eyebrow Filler (Black / Brown)
-- CMC009_02            = Duo Eyebrow Filler and Eyeliner Sketch
-- CMC016_01/02         = Star Bomb Eyeshadow palettes