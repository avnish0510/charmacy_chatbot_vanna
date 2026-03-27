# Charmacy Milano — Glossary of Business Terms, Abbreviations, and Domain Concepts

This glossary defines every term, abbreviation, metric, and concept that may appear in user questions
about Charmacy Milano's sales data. Mapped to the SQL column or formula that answers each query.

---

## SECTION 1: PLATFORM NAMES AND ALIASES

### Official Platform Names (case-sensitive in SQL)
| Platform | Also Known As | Type | Salestype |
|---|---|---|---|
| `Amazon` | Amz | B2C + B2B | Primary |
| `Flipkart` | FK, Fk | B2C | Primary |
| `Myntra` | Myn | B2C | Primary |
| `Shopify` | D2C, website, own website, brand website | B2C | Primary |
| `Nykaa` | NK | B2B | Secondary |
| `Zepto` | Zep | B2B | Secondary |

**SQL mapping:**
```sql
WHERE platform = 'Amazon'     -- exact match, case-sensitive value
WHERE platform = 'Flipkart'
WHERE platform = 'Myntra'
WHERE platform = 'Shopify'
WHERE platform = 'Nykaa'
WHERE platform = 'Zepto'
WHERE platform IN ('Amazon','Flipkart','Myntra','Shopify')  -- all Primary B2C
WHERE platform IN ('Nykaa','Zepto')  -- all Secondary
```

### Platform Groups
| User Says | SQL Filter |
|---|---|
| "marketplaces" | `platform IN ('Amazon','Flipkart','Myntra','Nykaa','Zepto')` |
| "D2C" / "direct" / "own website" | `platform = 'Shopify'` |
| "B2B channels" | `ordertype = 'B2B'` |
| "B2C channels" | `ordertype = 'B2C'` |
| "primary channels" | `salestype = 'Primary'` |
| "secondary channels" | `salestype = 'Secondary'` |
| "quick commerce" / "q-commerce" | `platform = 'Zepto'` |
| "fashion platforms" | `platform = 'Myntra'` |
| "beauty platforms" | `platform IN ('Nykaa','Myntra')` |

---

## SECTION 2: REVENUE AND FINANCIAL METRICS

### MRP
**Column:** `MRP` (DECIMAL)
**Definition:** Total line-item invoice amount in INR (₹). This is quantity × unit price, already multiplied. It is NOT the unit price. The term "MRP" in this database means invoice amount, NOT Maximum Retail Price in the traditional FMCG sense.
**Usage:** `SUM(MRP)` = Total Revenue. Always apply net sales filter.

### Revenue / Sales / GMV / Turnover / Sales Value
All these user terms mean: `SUM(MRP)` with net sales filter applied.
```sql
SUM(MRP) AS revenue
-- With net filter:
WHERE NOT (
    (platform='Amazon' AND ISNULL(transaction_type,'') IN ('Cancel','Refund','FreeReplacement'))
    OR (platform='Flipkart' AND ISNULL(event_sub_type,'') IN ('Cancellation','Return','RTO'))
    OR (platform='Shopify' AND ISNULL(fulfilment_type,'') = 'unfulfilled')
)
```

### Gross Revenue / Gross Sales
Revenue WITHOUT the cancellation/return filter: `SUM(MRP)` with no WHERE clause exclusions.

### Net Revenue / Net Sales
Revenue WITH the cancellation/return filter applied (the default — use this unless asked for gross).

### ASP — Average Selling Price
Price per unit, averaged across all sales.
```sql
SUM(MRP) / NULLIF(SUM(quantity), 0) AS asp
```
Also referred to as: "average price per unit", "unit ASP", "selling price per piece".

### AOV — Average Order Value
Average revenue per order. Only meaningful for Amazon, Flipkart, and Myntra (platforms with order_id).
```sql
SUM(MRP) / NULLIF(COUNT(DISTINCT order_id), 0) AS aov
-- WHERE platform IN ('Amazon','Flipkart','Myntra')
```
Also referred to as: "basket size", "average basket", "order value".

### Discount / Discount % / Markdown
Only available for Nykaa (has display_price and selling_price columns).
```sql
(SUM(display_price) - SUM(selling_price)) / NULLIF(SUM(display_price), 0) * 100 AS discount_pct
-- WHERE platform = 'Nykaa'
```

### INR / ₹ / Rupees
All monetary values in the database are in Indian Rupees. No currency conversion is needed.

---

## SECTION 3: VOLUME AND QUANTITY METRICS

### Units / Units Sold / Pieces / Quantity / Volume
**Column:** `quantity` (INT) — units in a line-item.
```sql
SUM(quantity) AS units_sold   -- works for all platforms
```
For net units (excluding cancellations):
```sql
SUM(CASE
    WHEN platform='Amazon'   AND ISNULL(transaction_type,'') IN ('Cancel','Refund','FreeReplacement') THEN 0
    WHEN platform='Flipkart' AND ISNULL(event_sub_type,'')   IN ('Cancellation','Return','RTO') THEN 0
    WHEN platform='Shopify'  AND ISNULL(fulfilment_type,'')  = 'unfulfilled' THEN 0
    ELSE quantity
END) AS net_units
```

### Orders / Order Count / Number of Orders
Platform-specific — see T-SQL Rules Section 4.
```sql
-- Cross-platform:
  COUNT(DISTINCT CASE WHEN platform IN ('Amazon','Flipkart','Myntra') THEN order_id END)
+ SUM(CASE WHEN platform = 'Nykaa' THEN total_orders ELSE 0 END)
+ COUNT(CASE WHEN platform IN ('Shopify','Zepto') THEN 1 END)
```

### Transactions / Rows / Records
For Shopify and Zepto, each row = 1 transaction. `COUNT(*)` gives transaction count.

### Customers / Customer Count / Unique Customers
Only available for Nykaa: `SUM(total_customers)`.
Not available for Amazon, Flipkart, Myntra, Shopify, or Zepto.

---

## SECTION 4: CANCELLATION AND RETURN METRICS

### Cancellations / Cancellation Rate / Cancel %
```sql
-- Cancellation rate (Amazon + Flipkart only — Myntra/Shopify/others not tracked):
CAST(SUM(CASE
    WHEN platform='Amazon'   AND transaction_type='Cancel'       THEN 1
    WHEN platform='Flipkart' AND event_sub_type='Cancellation'   THEN 1
    ELSE 0
END) AS FLOAT) / NULLIF(COUNT(*), 0) * 100 AS cancel_rate_pct
```

### Returns / Return Rate / RTO Rate
- Amazon: `transaction_type = 'Refund'`
- Flipkart: `event_sub_type = 'Return'` or `event_sub_type = 'RTO'`
- RTO = Return to Origin (courier could not deliver; returned to warehouse)

### Unfulfilled Orders
- Shopify only: `fulfilment_type = 'unfulfilled'` — order placed but not yet shipped.
- These are excluded from net revenue.

### Free Replacement
- Amazon only: `transaction_type = 'FreeReplacement'` — replacement sent to customer at no charge.
- Excluded from net revenue.

---

## SECTION 5: PRODUCT TERMINOLOGY

### Product / SKU / Item / Style
All refer to individual products. Always group by `product_name` (NOT `product_description`).

### Product Name
**Column:** `product_name` — standardised canonical name from EAN_Master.
Examples: "Matte Foundation 03", "Diamond Rush - Ruby 78", "Set and Fix Loose Powder"
Always add `WHERE product_name IS NOT NULL` when grouping by product (~34% of rows have NULL product_name).

### SKU / SKU Code
**Column:** `sku_code` — internal product code.
Examples: `CMS_F3`, `CMCDR_78`, `CMC021`
Pattern: CMS_F1–F12 = Foundation shades; CMCDR_77–82 = Diamond Rush Lipstick; CMC021 = Loose Powder.

### EAN / Barcode
**Column:** `EAN` — 13-digit barcode. All Charmacy Milano EANs start with `8906148`.

### ASIN
**Column:** `product_code` for Amazon rows — Amazon Standard Identification Number.
Example: `B08S7KCPKS`

### FSN
**Column:** `product_code` for Flipkart rows — Flipkart Serial Number.
Example: `FNDGHV97M6UH6MFQ`

### Style ID
Myntra's internal product identifier — used as the JOIN key to EAN_Master, not exposed in B2B_B2C.

### Article Type / Category / Product Type
**Column:** `article_type` — 17 values:
Foundation, Concealer, Lipstick, Liquid Lipstick, Mascara, Eyeliner, Eyeshadow,
Eyebrow Enhancer, Blush, Highlighter, Loose Powder, Compact, Face Primer,
Lip Balm, Lip Liner, PREP & SET

### Unmapped Products / Unknown Products / Accessories
Products NOT in EAN_Master: Beauty Blender (₹399), Finger Blender (₹99).
These have `product_name IS NULL`. Exclude with `WHERE product_name IS NOT NULL` for product analysis.

### MRP Variation by Platform
The same product has different MRP on different platforms (platform pricing varies):
- Matte Foundation 03: Shopify = ₹989, Myntra = ₹1,011, Flipkart = ₹763–₹835
- Always include `platform` as a dimension when comparing product prices across channels.

---

## SECTION 6: BUSINESS MODEL TERMS

### B2C — Business to Consumer
**Column value:** `ordertype = 'B2C'`
Platforms: Amazon B2C, Flipkart, Myntra, Shopify.
Individual customers buying for personal use.

### B2B — Business to Business
**Column value:** `ordertype = 'B2B'`
Platforms: Amazon B2B, Nykaa, Zepto.
Corporate/business buyers, or channel partners.

### Primary Sales / Primary Channel
**Column value:** `salestype = 'Primary'`
Charmacy Milano is the direct seller. Revenue = brand's actual revenue.
Platforms: Amazon (B2C + B2B), Flipkart, Myntra, Shopify.

### Secondary Sales / Sell-through / Channel Sales
**Column value:** `salestype = 'Secondary'`
Marketplace partner's sales of Charmacy Milano products. NOT brand direct revenue.
Used for market presence tracking only.
Platforms: Nykaa, Zepto.

### D2C — Direct to Consumer
Refers to Shopify (Charmacy Milano's own website). `platform = 'Shopify'`.

---

## SECTION 7: FULFILMENT TERMS

### FBF — Flipkart Fulfilled by Flipkart
**Column value:** `fulfilment_type = 'FBF'` for Flipkart rows.
Flipkart handles warehousing and delivery.

### NON_FBF — Non-Flipkart Fulfilled
**Column value:** `fulfilment_type = 'NON_FBF'` for Flipkart rows.
Seller (Charmacy) handles delivery.

### PPMP — Myntra Fulfilled
**Column value:** `fulfilment_type = 'PPMP'` for Myntra rows.
Myntra handles warehousing and delivery.

### SJIT — Seller Fulfilled (Myntra)
**Column value:** `fulfilment_type = 'SJIT'` for Myntra rows.
Seller ships directly to customer.

### Fulfilled / Unfulfilled (Shopify)
**Column values:** `fulfilment_type IN ('fulfilled','unfulfilled')` for Shopify rows.
`unfulfilled` = order placed, not yet shipped → excluded from net revenue.

---

## SECTION 8: TIME AND DATE TERMS

### Month / Monthly
Group by `month_year`, sort by `MIN(order_date)` (NOT by `month_year` alphabetically).
`month_year` format is `'2026-01'` (YYYY-MM).

### Year-to-Date / YTD
```sql
WHERE order_date >= '2026-01-01' AND order_date IS NOT NULL
```

### Last Month / Previous Month
```sql
WHERE order_date >= DATEADD(month, DATEDIFF(month,0,GETDATE())-1, 0)
  AND order_date <  DATEADD(month, DATEDIFF(month,0,GETDATE()), 0)
  AND order_date IS NOT NULL
```

### Last 30 Days / Past Month
```sql
WHERE order_date >= DATEADD(day, -30, GETDATE())
  AND order_date IS NOT NULL
```

### No date data for Shopify
All Shopify rows have `order_date = NULL`. Any date-filtered query excludes Shopify.
When a user asks for "total revenue this month", the result will NOT include Shopify revenue.

---

## SECTION 9: GEOGRAPHY TERMS

### States / State-wise / Region-wise
**Column:** `ship_to_state` — available for Amazon, Flipkart, Myntra, Shopify.
Has dual-spelling problem. Always use `IN()` with both abbreviation and full name.
See T-SQL Rules Section 7 for full mapping.

### City-wise / City-level
**Column:** `ship_to_city` — available for Amazon, Myntra, Shopify only.
Not available for Flipkart, Nykaa, Zepto.

### Postal Code / PIN Code / Pincode
**Column:** `ship_to_postal_code` — available for Amazon and Shopify only.

### North India / South India / Metro Cities
No predefined geographic groupings exist in the data. Must define manually:
```sql
-- North India example:
WHERE ship_to_state IN ('Up','Uttar pradesh','Dl','Delhi','Hr','Haryana','Pb','Punjab',
                        'Uk','Ut','Uttarakhand','Hp','Himachal pradesh','Jk','Jammu & kashmir')
-- South India example:
WHERE ship_to_state IN ('Ka','Karnataka','Tn','Tamil nadu','Kl','Kerala','Ap','Andhra pradesh',
                        'Ts','Tg','Telangana')
```

---

## SECTION 10: NYKAA-SPECIFIC TERMS

### Display Price (Nykaa)
**Column:** `display_price` — listed price on Nykaa before any discount.

### Selling Price (Nykaa)
**Column:** `selling_price` — actual price paid after discount on Nykaa.

### Discount % (Nykaa)
```sql
(SUM(display_price) - SUM(selling_price)) / NULLIF(SUM(display_price),0) * 100
```

### Category L1 / L2 / L3 (Nykaa)
Nykaa's taxonomy. Always: category_l1 = 'Makeup'. category_l2 = Eyes/Face/Lips/Makeup Kits.
category_l3 = Foundation, Concealer, Eye Shadow, etc.

### Seller / Seller Code / Seller Name (Nykaa)
Columns: `seller_code`, `display_name`, `company_name`, `seller_type`.
All = Charmacy Milano's own seller account on Nykaa.

---

## SECTION 11: PAYMENT METHOD TERMS

**Column:** `payment_method` — available for Amazon, Myntra, Shopify.

| User Says | SQL Value |
|---|---|
| COD / Cash on Delivery | `'COD'` or `'Cash on Delivery (COD)'` |
| Razorpay / UPI / Online | `'Razorpay'` |
| Net banking | `'NB'` |
| Credit card | `'CC'` |
| Gift card | `'GC'` |
| Installments / EMI | `'Installments'` |
| Pay on Arrival | `'POA'` |

```sql
-- COD vs Online for Shopify:
WHERE payment_method IN ('COD','Cash on Delivery (COD)')     -- Cash on Delivery
WHERE payment_method NOT IN ('COD','Cash on Delivery (COD)') -- Online payment
```

---

## SECTION 12: WAREHOUSE TERMS

**Column:** `warehouse_id` — available for Amazon and Myntra only (~91% NULL overall).
Amazon example: `PMZJ`
Myntra example: `63664`, `36`

---

## SECTION 13: KEY METRIC SQL TEMPLATES (Quick Reference)

### Total Net Revenue (all platforms)
```sql
SELECT SUM(MRP) AS net_revenue
FROM [dbo].[B2B_B2C]
WHERE NOT (
    (platform='Amazon'   AND ISNULL(transaction_type,'') IN ('Cancel','Refund','FreeReplacement'))
    OR (platform='Flipkart' AND ISNULL(event_sub_type,'') IN ('Cancellation','Return','RTO'))
    OR (platform='Shopify'  AND ISNULL(fulfilment_type,'') = 'unfulfilled')
)
```

### Revenue by Platform
```sql
SELECT platform, SUM(MRP) AS revenue, SUM(quantity) AS units
FROM [dbo].[B2B_B2C]
WHERE NOT (
    (platform='Amazon'   AND ISNULL(transaction_type,'') IN ('Cancel','Refund','FreeReplacement'))
    OR (platform='Flipkart' AND ISNULL(event_sub_type,'') IN ('Cancellation','Return','RTO'))
    OR (platform='Shopify'  AND ISNULL(fulfilment_type,'') = 'unfulfilled')
)
GROUP BY platform
ORDER BY revenue DESC
```

### Monthly Revenue Trend
```sql
SELECT month_year, SUM(MRP) AS revenue, SUM(quantity) AS units
FROM [dbo].[B2B_B2C]
WHERE month_year IS NOT NULL
  AND NOT (
    (platform='Amazon'   AND ISNULL(transaction_type,'') IN ('Cancel','Refund','FreeReplacement'))
    OR (platform='Flipkart' AND ISNULL(event_sub_type,'') IN ('Cancellation','Return','RTO'))
    OR (platform='Shopify'  AND ISNULL(fulfilment_type,'') = 'unfulfilled')
  )
GROUP BY month_year
ORDER BY MIN(order_date)
```

### Top 10 Products by Revenue
```sql
SELECT TOP 10 product_name, article_type, SUM(quantity) AS units, SUM(MRP) AS revenue
FROM [dbo].[B2B_B2C]
WHERE product_name IS NOT NULL
  AND NOT (
    (platform='Amazon'   AND ISNULL(transaction_type,'') IN ('Cancel','Refund','FreeReplacement'))
    OR (platform='Flipkart' AND ISNULL(event_sub_type,'') IN ('Cancellation','Return','RTO'))
    OR (platform='Shopify'  AND ISNULL(fulfilment_type,'') = 'unfulfilled')
  )
GROUP BY product_name, article_type
ORDER BY revenue DESC
```

### Top States by Revenue
```sql
SELECT TOP 10 ship_to_state, SUM(MRP) AS revenue
FROM [dbo].[B2B_B2C]
WHERE ship_to_state IS NOT NULL
  AND NOT (
    (platform='Amazon'   AND ISNULL(transaction_type,'') IN ('Cancel','Refund','FreeReplacement'))
    OR (platform='Flipkart' AND ISNULL(event_sub_type,'') IN ('Cancellation','Return','RTO'))
    OR (platform='Shopify'  AND ISNULL(fulfilment_type,'') = 'unfulfilled')
  )
GROUP BY ship_to_state
ORDER BY revenue DESC
-- NOTE: States appear twice (abbreviation + full name). Consider normalising in application.
```

### ASP (Average Selling Price)
```sql
SELECT SUM(MRP) / NULLIF(SUM(quantity), 0) AS asp
FROM [dbo].[B2B_B2C]
WHERE NOT (
    (platform='Amazon'   AND ISNULL(transaction_type,'') IN ('Cancel','Refund','FreeReplacement'))
    OR (platform='Flipkart' AND ISNULL(event_sub_type,'') IN ('Cancellation','Return','RTO'))
    OR (platform='Shopify'  AND ISNULL(fulfilment_type,'') = 'unfulfilled')
)
```

### Flipkart Order Count (only confirmed sales)
```sql
SELECT COUNT(DISTINCT order_id) AS orders
FROM [dbo].[B2B_B2C]
WHERE platform = 'Flipkart'
  AND ISNULL(event_sub_type,'') = 'Sale'
```

### Nykaa Discount %
```sql
SELECT
  (SUM(display_price) - SUM(selling_price)) / NULLIF(SUM(display_price),0) * 100 AS discount_pct
FROM [dbo].[B2B_B2C]
WHERE platform = 'Nykaa'
```

---

*This glossary covers all confirmed terminology from Charmacy Milano's sales operations.
When a user question uses informal language, map it to the correct column/formula using this reference.*