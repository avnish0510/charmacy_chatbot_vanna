# Column Definitions — [dbo].[B2B_B2C]
# Charmacy Milano · SQL Server (T-SQL) · Text-to-SQL Training Reference
#
# HOW THIS FILE IS USED
# ─────────────────────
# Each section is trained into ChromaDB as a separate documentation chunk via:
#   vn.train(documentation=<section_text>)
# Vanna's RAG retrieves the most relevant sections when generating SQL.
# Keep each section focused on one column or column group.
#
# NEVER query source tables directly. Always use [dbo].[B2B_B2C].
# ═══════════════════════════════════════════════════════════════════════════════


## DATABASE AND VIEW OVERVIEW

Database:   Charmacy_f_automate
Schema:     dbo
View:       B2B_B2C
Full name:  [Charmacy_f_automate].[dbo].[B2B_B2C]

B2B_B2C is a SQL Server VIEW — not a physical table. It unifies 7 source tables
via UNION ALL, each LEFT JOINed to EAN_Master for standardised product attributes.

Total columns: 42
Row grain:     One row = one line-item (one SKU) in one order or daily aggregate.
Currency:      All monetary values in INR (Indian Rupees ₹).
No primary key exists. order_id is NULL for Shopify, Nykaa, and Zepto rows.


## PLATFORM COLUMN

Column name:  platform
Data type:    VARCHAR (NVARCHAR in actual schema)
Nullable:     NO — always populated, never NULL
Description:  The e-commerce sales channel. CASE-SENSITIVE. Exactly 6 values.

Valid values (use these exact strings in WHERE clauses):
  'Amazon'   — Amazon India marketplace (both B2C and B2B rows exist)
  'Flipkart' — Flipkart marketplace (B2C only)
  'Myntra'   — Myntra fashion/beauty marketplace (B2C only)
  'Shopify'  — Charmacy Milano brand website, D2C (B2C only, NO order_date)
  'Nykaa'    — Nykaa beauty marketplace (B2B Secondary, pre-aggregated rows)
  'Zepto'    — Zepto quick-commerce (B2B Secondary, pre-aggregated, very few rows)

Key facts:
  - Amazon is the ONLY platform with BOTH B2C and B2B rows. Use ordertype to separate.
  - Shopify has the most rows (~7,700 of ~10,000 total = ~80% of data).
  - Nykaa and Zepto rows represent daily SKU-level aggregates, not individual orders.
  - Zepto has very few rows — not suitable for trend analysis.

Correct usage:
  WHERE platform = 'Shopify'
  WHERE platform IN ('Amazon', 'Flipkart', 'Myntra')
  GROUP BY platform ORDER BY SUM(MRP) DESC


## ORDERTYPE COLUMN

Column name:  ordertype
Data type:    VARCHAR(3)
Nullable:     NO — always populated
Description:  Business model of the transaction.

Valid values:
  'B2C' — Business-to-Consumer. Platforms: Amazon, Flipkart, Myntra, Shopify
  'B2B' — Business-to-Business. Platforms: Nykaa, Zepto, Amazon B2B

Key facts:
  - Never NULL. Hard-coded per source table.
  - Amazon appears in BOTH B2C and B2B rows. Always use ordertype to separate them.
  - Synonyms: "retail" / "consumer" → B2C | "wholesale" / "distributor" → B2B

Correct usage:
  WHERE ordertype = 'B2C'
  WHERE ordertype = 'B2B'
  GROUP BY ordertype   -- to see B2B vs B2C split


## SALESTYPE COLUMN

Column name:  salestype
Data type:    VARCHAR(9)
Nullable:     NO — always populated
Description:  Whether the sale is Charmacy Milano's own revenue (Primary) or
              marketplace sell-through tracking (Secondary).

Valid values:
  'Primary'   — Brand sells directly. Platforms: Amazon, Flipkart, Myntra, Shopify.
                This IS brand revenue. Use for financial/P&L reporting.
  'Secondary' — Marketplace tracks sell-through to consumers. Platforms: Nykaa, Zepto.
                This is NOT brand revenue. Use ONLY for market demand analysis.

CRITICAL RULE:
  NEVER mix Primary + Secondary in the same SUM(MRP) without explicitly noting it.
  For brand revenue reporting, always filter: WHERE salestype = 'Primary'
  Or equivalently:  WHERE platform NOT IN ('Nykaa', 'Zepto')

Synonyms: "sell-in" / "brand sales" → Primary | "sell-out" / "sell-through" → Secondary


## MRP COLUMN — REVENUE AND PRICING

Column name:  MRP
Data type:    DECIMAL(18,2)
Nullable:     YES (but effectively 0% NULL — always populated in practice)
Description:  TOTAL line-item invoice amount in INR. This is NOT the per-unit price.
              MRP = quantity × unit_price. It is the full invoice value for this row.

⚠️ CRITICAL: MRP is the TOTAL amount for the line-item, not the unit price.
   Example: 2 units at ₹200 each → MRP = ₹400 (not ₹200)

Correct formulas:
  Total Revenue:          SUM(MRP)
  Per-unit price:         MRP / NULLIF(quantity, 0)
  Average Selling Price:  SUM(MRP) / NULLIF(SUM(quantity), 0)
  AOV (orders with ID):   SUM(MRP) / NULLIF(COUNT(DISTINCT order_id), 0)

WRONG formulas:
  AVG(MRP)   — DO NOT USE. This averages invoice amounts, not unit prices.
  MAX(MRP)   — DO NOT USE alone for "price of a product" (includes multi-unit rows).

Additional notes:
  - MRP varies by platform for the same product. Foundation 03 = ₹989 on Shopify,
    ₹1,011 on Myntra, different on Nykaa. Always GROUP BY platform when reporting price.
  - Currency is always INR (₹). No foreign currency data exists.
  - Amazon Refund rows have NEGATIVE MRP values (e.g., -₹674). The net sales filter
    excludes these rows entirely.


## QUANTITY COLUMN

Column name:  quantity
Data type:    FLOAT
Nullable:     YES (but 0% NULL in practice — always populated)
Description:  Number of units for this line-item.

Key facts:
  - Myntra always hardcodes quantity = 1 per row (each row is one unit).
  - Nykaa and Zepto: quantity is the AGGREGATED daily total for the SKU, not per-order.
  - Amazon/Flipkart/Shopify: actual quantity ordered (usually 1, sometimes 2+).

Correct usage:
  SUM(quantity)  — total units sold. Works for ALL platforms including Nykaa.
  Never GROUP BY quantity. Use for aggregation only.

Confirmed by data: Nykaa's sum(quantity) equals sum(total_qty), so SUM(quantity)
is the correct and universal formula for units across all platforms.


## ORDER_ID COLUMN

Column name:  order_id
Data type:    NVARCHAR(255)
Nullable:     YES — NULL for Shopify, Nykaa, and Zepto (~84% of rows are NULL)
Description:  Unique order identifier assigned by the platform. Format varies by platform.

Platform availability:
  Amazon:   NOT NULL — format: hyphenated numeric e.g. '406-2606305-0233961'
  Flipkart: NOT NULL — format: 'OD' prefix e.g. 'OD436456263392762100'
  Myntra:   NOT NULL — format: long numeric string e.g. '131921186466800350003'
  Shopify:  ALWAYS NULL — Shopify does not provide order_id in this pipeline
  Nykaa:    ALWAYS NULL — pre-aggregated data, no individual order IDs
  Zepto:    ALWAYS NULL — pre-aggregated data, no individual order IDs

CRITICAL: One order_id can have MULTIPLE rows (one per product/line-item).
  Never COUNT(*) for Amazon/Flipkart/Myntra — this overcounts orders.

Correct order count per platform:
  Amazon / Flipkart / Myntra → COUNT(DISTINCT order_id)
  Nykaa                      → SUM(total_orders)
  Shopify                    → COUNT(*)
  Zepto                      → COUNT(*)

Do NOT use:
  COUNT(DISTINCT order_id) for Shopify/Nykaa/Zepto — order_id is NULL
  COUNT(*) for Amazon/Flipkart/Myntra — one order = multiple rows


## ORDER_DATE COLUMN

Column name:  order_date
Data type:    DATE (format: YYYY-MM-DD)
Nullable:     YES — NULL for ALL Shopify rows and ~87 Amazon rows (~81% overall NULL)
Description:  Date the order was placed or the transaction occurred.

⚠️ CRITICAL: Shopify rows have NO order_date. ALL Shopify rows have NULL order_date.
  Any WHERE clause filtering by date automatically excludes ALL Shopify revenue.
  When you say "total revenue last month", Shopify is not included.
  Inform users that date-filtered results exclude Shopify.

Platform date availability:
  Shopify:  NEVER has order_date (NULL for all ~7,700 rows)
  Amazon:   ~377 of 464 rows have dates; ~87 rows have NULL order_date
  Flipkart: All 372 rows have dates
  Myntra:   All 714 rows have dates
  Nykaa:    All 401 rows have dates
  Zepto:    All 3 rows have dates

Correct T-SQL date filtering:
  WHERE order_date >= '2026-01-01' AND order_date <= '2026-01-31'
  WHERE order_date >= DATEADD(day, -30, GETDATE())
  WHERE order_date IS NOT NULL  -- always add this for date-based queries

For monthly sorting: ORDER BY MIN(order_date)
  DO NOT ORDER BY month_year — that sorts alphabetically ('Dec 2025' < 'Jan 2026' is WRONG).


## MONTH_YEAR COLUMN

Column name:  month_year
Data type:    NVARCHAR (format: 'MMM yyyy', e.g. 'Jan 2026')
Nullable:     YES — NULL wherever order_date is NULL (same as order_date nullability)
Description:  Human-readable month-year string derived from order_date.

Valid example values: 'Dec 2025', 'Jan 2026', 'Feb 2026'

Use for:  GROUP BY month_year for monthly aggregation
Use NOT for:  ORDER BY month_year (sorts alphabetically, not chronologically)

Correct monthly trend query pattern:
  GROUP BY month_year
  ORDER BY MIN(order_date)  -- ensures chronological order

NULL for all Shopify rows. Add WHERE month_year IS NOT NULL to exclude Shopify from trends.


## TRANSACTION_TYPE COLUMN

Column name:  transaction_type
Data type:    NVARCHAR
Nullable:     YES — ~56% NULL (only populated for Amazon B2C and Shopify)
Description:  For Amazon: order lifecycle event. For Shopify: the payment method.
              ⚠️ This column serves TWO different purposes depending on platform.

Amazon values (order lifecycle):
  'Shipment'        — Confirmed sale. INCLUDE in net revenue.
  'Cancel'          — Cancelled order. EXCLUDE from net revenue.
  'Refund'          — Refund issued. EXCLUDE from net revenue.
  'FreeReplacement' — Free replacement sent. EXCLUDE from revenue (no new revenue).

Shopify values (payment method — yes, payment_method and transaction_type BOTH carry this):
  'Cash on Delivery (COD)' — COD payment
  'Razorpay'               — Razorpay gateway
  '1Razorpay - UPI, Cards, Wallets, NB' — Razorpay sub-method
  'manual'                 — Manually recorded payment

Platforms where this column is NULL:
  Flipkart, Myntra, Nykaa, Zepto, Amazon B2B

Net sales filter usage:
  WHERE NOT (platform = 'Amazon' AND ISNULL(transaction_type, '') IN ('Cancel', 'Refund', 'FreeReplacement'))


## EVENT_SUB_TYPE COLUMN

Column name:  event_sub_type
Data type:    NVARCHAR
Nullable:     YES — ~89% NULL (only Flipkart and Myntra)
Description:  Order event status / lifecycle stage. Used to identify confirmed sales
              vs cancellations, returns, and RTO on Flipkart and Myntra.

Flipkart values:
  'Sale'         — Confirmed sale. INCLUDE in net revenue.
  'Cancellation' — Cancelled. EXCLUDE from net revenue.
  'Return'       — Returned by customer. EXCLUDE from net revenue.
  'RTO'          — Return to Origin (delivery failed). EXCLUDE from net revenue.

Myntra values:
  'SH' — Shipped to customer (confirmed delivery in progress).
  'PK' — Packed, awaiting shipment.
  'F'  — Failed / Forward (requires verification).
  'WP' — Waiting for pickup.

Platforms where this column is NULL:
  Amazon, Shopify, Nykaa, Zepto

Net sales filter usage:
  WHERE NOT (platform = 'Flipkart' AND ISNULL(event_sub_type, '') IN ('Cancellation', 'Return', 'RTO'))

For confirmed Myntra shipped orders:
  WHERE platform = 'Myntra' AND event_sub_type = 'SH'


## FULFILMENT_TYPE COLUMN

Column name:  fulfilment_type
Data type:    NVARCHAR
Nullable:     YES — ~49% NULL (populated for Shopify, Flipkart, Myntra)
Description:  Fulfilment status or model. Meaning differs per platform.

Shopify values:
  'fulfilled'   — Order shipped and fulfilled. INCLUDE in net revenue.
  'unfulfilled' — Order NOT yet shipped. EXCLUDE from net revenue.

Flipkart values:
  'FBF'     — Fulfilled by Flipkart. Flipkart warehouses and ships.
  'NON_FBF' — Seller-fulfilled. Charmacy Milano ships directly.

Myntra values:
  'PPMP' — Pre-Positioned Marketplace. Inventory pre-positioned at Myntra warehouse.
  'SJIT' — Seller Just-In-Time. Seller ships upon order.

Platforms where this column is NULL:
  Amazon (all rows), Nykaa, Zepto

⚠️ NULL does NOT mean unfulfilled — it means the platform does not track this.

Net sales filter usage:
  WHERE NOT (platform = 'Shopify' AND ISNULL(fulfilment_type, '') = 'unfulfilled')


## PRODUCT IDENTIFICATION COLUMNS

### product_name (USE THIS FOR ALL PRODUCT GROUPING)
Column name:  product_name
Data type:    NVARCHAR(255)
Nullable:     YES — ~34% NULL (products not matched in EAN_Master)
Description:  Standardised canonical product name from EAN_Master lookup table.
              This is consistent across ALL platforms. Always use for GROUP BY.
              NULL means the product is not in EAN_Master (accessories like Beauty Blender).

When grouping by product, always add: WHERE product_name IS NOT NULL
134 distinct product_name values exist.

Examples: 'Matte Foundation 01', 'Diamond Rush - Garnet 79', 'Set and Fix Loose Powder'

### article_type (PRODUCT CATEGORY)
Column name:  article_type
Data type:    NVARCHAR(255)
Nullable:     YES — ~34% NULL (same rows as product_name)
Description:  Standardised product category from EAN_Master. 17 distinct values.

Valid values:
  Foundation, Concealer, Lipstick, Liquid Lipstick, Mascara, Eyeliner,
  Eyeshadow, Eyebrow Enhancer, Blush, Highlighter, Loose Powder,
  Compact, Face Primer, Lip Balm, Lip Liner, PREP & SET

### sku_code (INTERNAL SKU IDENTIFIER)
Column name:  sku_code
Data type:    NVARCHAR(255)
Nullable:     YES — ~34% NULL
Description:  Charmacy Milano internal SKU code. One sku_code = one product variant.
              134 distinct values. Use for precise product filtering.

Naming patterns:
  CMS_F1 to CMS_F12      — Matte Foundation shades 1–12
  CMCCON_1 to CMCCON_6   — Concealer shades
  CMCDR_77 to CMCDR_82   — Diamond Rush Lipstick shades
  CMCSD_01 onward         — Star Dust Highlighters
  CMCHDCC_01 onward       — HD Cover Compacts
  CMC_IQEP1 onward        — Insane Quad Eyeshadow Palettes
  CMC021                  — Set and Fix Loose Powder
  CMC022                  — Pro-Pore Conceal Primer

### EAN (INTERNATIONAL BARCODE)
Column name:  EAN
Data type:    NVARCHAR(255)
Nullable:     YES — ~34% NULL
Description:  13-digit barcode. All Charmacy Milano EANs start with prefix 8906148.
              Use for precise cross-platform product matching.

### product_description (RAW PLATFORM TEXT — DO NOT GROUP BY)
Column name:  product_description
Data type:    NVARCHAR(MAX)
Nullable:     YES — populated only for Amazon B2C, Flipkart, Shopify
Description:  Raw product listing title from the selling platform. Unstandardised.
              DO NOT use for GROUP BY. Use product_name instead.
              Only use product_description when user explicitly asks to see the description text.

### product_code (PLATFORM-SPECIFIC ID)
Column name:  product_code
Data type:    NVARCHAR
Nullable:     YES — only Amazon (ASIN) and Flipkart (FSN) have this
Description:  Amazon: ASIN. Flipkart: FSN. NULL for all other platforms.
              Use sku_code or EAN for cross-platform identification instead.


## GEOGRAPHY COLUMNS

### ship_to_state
Column name:  ship_to_state
Data type:    NVARCHAR
Nullable:     YES — ~44% NULL (Nykaa and Zepto have no state data)
Description:  Shipping destination state in India.
              ⚠️ CRITICAL: Contains MIXED formats — 2-letter abbreviations AND full names.
              Same state appears twice with different spellings.

Platform coverage:
  Has data:  Amazon, Flipkart, Myntra, Shopify
  Always NULL:  Nykaa, Zepto

Dual-spelling states (all 20 confirmed — always use IN() with both):
  Up / Uttar Pradesh    Mh / Maharashtra      Dl / Delhi
  Pb / Punjab           Gj / Gujarat          Hr / Haryana
  Wb / West Bengal      Mp / Madhya Pradesh   Ka / Karnataka
  Rj / Rajasthan        Br / Bihar            Ts,Tg / Telangana
  Jh / Jharkhand        Tn / Tamil Nadu       Or / Odisha
  As / Assam            Uk,Ut / Uttarakhand   Kl / Kerala
  Ap / Andhra Pradesh   Cg,Ct / Chhattisgarh

Single-spelling states (abbreviation only):
  Jk=J&K  Hp=Himachal  Ml=Meghalaya  Mn=Manipur  Mz=Mizoram
  Nl=Nagaland  Tr=Tripura  Ch=Chandigarh  Ga=Goa  Sk=Sikkim
  Ar=Arunachal  An=Andaman  Dn=Dadra  La=Ladakh

Correct filtering for Maharashtra:
  WHERE ship_to_state IN ('Mh', 'Maharashtra')

### ship_to_city
Column name:  ship_to_city
Data type:    NVARCHAR
Nullable:     YES — NULL for Flipkart, Nykaa, Zepto
Description:  Shipping destination city. 550+ distinct values. Proper case.
              Available for: Amazon, Myntra, Shopify

### ship_to_postal_code
Column name:  ship_to_postal_code
Data type:    NVARCHAR(255)
Nullable:     YES — only Amazon and Shopify
Description:  6-digit Indian PIN code stored as string. Do NOT cast to INT.

### bill_from_city / bill_from_state / bill_from_country / bill_from_postal_code
Data type:    NVARCHAR
Nullable:     YES — Shopify ONLY. NULL for all other platforms.
Description:  Billing address of the customer. bill_from_country is always 'In' (India).


## PAYMENT COLUMN

Column name:  payment_method
Data type:    NVARCHAR
Nullable:     YES — NULL for Flipkart, Nykaa, Zepto
Description:  Payment method used by the customer.
              Available for: Amazon, Myntra, Shopify
              ⚠️ For Shopify, transaction_type ALSO carries payment method values.

Valid values:
  'Cash on Delivery (COD)' — COD (Shopify/Myntra format)
  'COD'                    — COD (Amazon format)
  'Razorpay'               — Razorpay gateway
  '1Razorpay - UPI, Cards, Wallets, NB' — Razorpay sub-method
  'NB'                     — Net Banking
  'CC'                     — Credit Card
  'CC_PayStation'          — Credit Card via Amazon PayStation
  'GC'                     — Gift Card
  'GC_PayStation'          — Gift Card via Amazon PayStation
  'PayStation'             — Amazon generic payment
  'PayStation_PayStation'  — Amazon combined payment
  'POA'                    — Pay on Acceptance
  'Installments'           — EMI payment
  'manual'                 — Manual Shopify payment

COD vs Prepaid analysis:
  COD:     payment_method IN ('COD', 'Cash on Delivery (COD)')
  Prepaid: payment_method IS NOT NULL AND payment_method NOT IN ('COD', 'Cash on Delivery (COD)')


## WAREHOUSE COLUMN

Column name:  warehouse_id
Data type:    NVARCHAR
Nullable:     YES — ~91% NULL (only Amazon and Myntra)
Description:  Warehouse or fulfilment centre ID. Amazon uses codes like BLR7, BOM5, LKO1.
              Myntra uses numeric IDs like 15774, 36, 63664.
              NULL for Flipkart, Shopify, Nykaa, Zepto.


## NYKAA-ONLY COLUMNS (NULL for all other platforms)

All columns below are ONLY populated when platform = 'Nykaa'.
Always add WHERE platform = 'Nykaa' when using any of these columns.

### total_orders
Column name:  total_orders
Data type:    NVARCHAR in schema (numeric values stored as text)
Nullable:     YES — ~96% NULL (Nykaa only)
Description:  Total number of orders for this SKU on this date on Nykaa.
              This is an AGGREGATED daily metric, not per-order.
              For Nykaa order count: SUM(CAST(total_orders AS INT))

### total_qty
Column name:  total_qty
Data type:    NVARCHAR in schema (numeric values stored as text)
Nullable:     YES — ~96% NULL (Nykaa and Zepto)
Description:  Total units sold for this SKU on this date (Nykaa + Zepto).
              Confirmed: sum(total_qty) = sum(quantity) for Nykaa.
              Use SUM(quantity) universally — no need to use total_qty.

### total_customers
Column name:  total_customers
Data type:    NVARCHAR in schema
Nullable:     YES — ~96% NULL (Nykaa only)
Description:  Unique customers who bought this SKU on this date on Nykaa.

### display_price
Column name:  display_price
Data type:    NVARCHAR in schema (decimal values stored as text)
Nullable:     YES — ~96% NULL (Nykaa only)
Description:  Listed price on Nykaa BEFORE discount.
              Discount % = (display_price - selling_price) / display_price * 100

### selling_price
Column name:  selling_price
Data type:    NVARCHAR in schema (decimal values stored as text)
Nullable:     YES — ~96% NULL (Nykaa only)
Description:  Actual selling price on Nykaa AFTER discount.
              selling_price ≤ display_price always.

Nykaa discount formula:
  (SUM(CAST(display_price AS DECIMAL(18,2))) - SUM(CAST(selling_price AS DECIMAL(18,2))))
  / NULLIF(SUM(CAST(display_price AS DECIMAL(18,2))), 0) * 100

### category_l1 / category_l2 / category_l3
Nullable:     YES — Nykaa only
Description:  Nykaa product taxonomy. Only use for Nykaa-specific category analysis.
              For cross-platform categories, always use article_type instead.

  category_l1: Always 'Makeup' for Charmacy Milano
  category_l2: Eyes / Face / Lips / Makeup Kits & Combos
  category_l3: Foundation, Concealer, Lipstick, Eye Shadow, Eye Brow Enhancers,
               Highlighters, Loose Powder, Compact, Face Primer, Lip Liner, etc.

  Note: category_l3 names differ slightly from article_type:
    article_type: 'Eyeshadow'       ↔  category_l3: 'Eye Shadow'
    article_type: 'Highlighter'     ↔  category_l3: 'Highlighters'
    article_type: 'Eyebrow Enhancer'↔  category_l3: 'Eye Brow Enhancers'

### seller_code / display_name / company_name / seller_type
Nullable:     YES — Nykaa only
Description:  Nykaa seller identification. Always 'Charmacy Milano' / '2953'.
              Single-brand dataset — these columns add no analytical value.


## NYKAA + ZEPTO COLUMNS

### brand
Column name:  brand
Nullable:     YES — Nykaa and Zepto only (~96% NULL)
Description:  Always 'Charmacy Milano' when populated. Single-brand dataset.

### sku_name
Column name:  sku_name
Data type:    NVARCHAR(MAX)
Nullable:     YES — Nykaa and Zepto only
Description:  Marketplace product display name including marketing copy.
              More descriptive than product_name. Use for display only, not grouping.


## COLUMNS TO NEVER USE

### invoice_number
Column name:  invoice_number
Data type:    INT
Status:       ALWAYS NULL across all platforms. Being removed from schema.
Action:       Never reference this column in any query.

### invoice_date
Column name:  invoice_date
Data type:    INT
Status:       ALWAYS NULL across all platforms. Being removed from schema.
Action:       Never reference this column. Use order_date for all date filtering.


## NULL PERCENTAGE SUMMARY

Column                    NULL%   Present for
─────────────────────────────────────────────────────────────────────
platform                  0%      All platforms (never null)
ordertype                 0%      All platforms (never null)
salestype                 0%      All platforms (never null)
MRP                       0%      All platforms (never null)
quantity                  0%      All platforms (never null)
order_id                  84%     Amazon, Flipkart, Myntra only
order_date                81%     NOT Shopify; some Amazon rows missing
month_year                81%     Same as order_date
transaction_type          56%     Amazon B2C + Shopify only
event_sub_type            89%     Flipkart + Myntra only
fulfilment_type           49%     Shopify + Flipkart + Myntra
ship_to_state             44%     NOT Nykaa, NOT Zepto
ship_to_city              48%     Amazon, Myntra, Shopify only
payment_method            57%     Amazon, Myntra, Shopify only
product_name              34%     All platforms (EAN_Master join)
article_type              34%     All platforms (EAN_Master join)
sku_code                  34%     All platforms (EAN_Master join)
EAN                       34%     All platforms (EAN_Master join)
warehouse_id              91%     Amazon + Myntra only
total_orders              96%     Nykaa only
total_qty                 96%     Nykaa + Zepto
total_customers           96%     Nykaa only
display_price             96%     Nykaa only
selling_price             96%     Nykaa only
category_l1/l2/l3         96%     Nykaa only
seller_code               96%     Nykaa only
brand                     96%     Nykaa + Zepto
sku_name                  96%     Nykaa + Zepto
invoice_number            100%    NEVER USE
invoice_date              100%    NEVER USE
