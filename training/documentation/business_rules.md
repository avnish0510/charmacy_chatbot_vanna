# Business Rules — [dbo].[B2B_B2C]
# Charmacy Milano · SQL Server (T-SQL) · Text-to-SQL Training Reference
#
# HOW THIS FILE IS USED
# ─────────────────────
# Each section is trained into ChromaDB as a separate documentation chunk via:
#   vn.train(documentation=<section_text>)
# Rules here are retrieved by Vanna's RAG when the user question relates to
# revenue, order counts, dates, states, products, returns, or specific platforms.
#
# All queries target: [dbo].[B2B_B2C] or [Charmacy_f_automate].[dbo].[B2B_B2C]
# T-SQL syntax only (SQL Server). Never LIMIT — use TOP N.
# ═══════════════════════════════════════════════════════════════════════════════


## RULE: NET SALES FILTER — ALWAYS APPLY

The database contains RAW data including cancelled, returned, refunded, and
unfulfilled transactions. For any revenue or units calculation, ALWAYS exclude
these rows using the net sales filter.

Standard net sales filter (apply in ALL revenue queries unless user says "gross"):

  WHERE NOT (
      (platform = 'Amazon'   AND ISNULL(transaction_type, '') IN ('Cancel', 'Refund', 'FreeReplacement'))
   OR (platform = 'Flipkart' AND ISNULL(event_sub_type, '')   IN ('Cancellation', 'Return', 'RTO'))
   OR (platform = 'Shopify'  AND ISNULL(fulfilment_type, '')  = 'unfulfilled')
  )

Why ISNULL is used: transaction_type, event_sub_type, and fulfilment_type are NULL
for many platforms. ISNULL(col, '') converts NULL to empty string so the IN check
works safely across all platforms without accidentally excluding valid rows.

Gross revenue (when user says "gross", "before returns", "including cancellations"):
  Simply omit the WHERE NOT (...) filter. Use plain SUM(MRP).

Platform-specific rules within the filter:
  Amazon:   Exclude transaction_type IN ('Cancel', 'Refund', 'FreeReplacement')
  Flipkart: Exclude event_sub_type IN ('Cancellation', 'Return', 'RTO')
  Shopify:  Exclude fulfilment_type = 'unfulfilled'
  Myntra:   No exclusion needed (event_sub_type 'SH','PK','F','WP' are all kept)
  Nykaa:    No exclusion needed (pre-aggregated sell-through data)
  Zepto:    No exclusion needed (pre-aggregated sell-through data)

CASE-WHEN version (inline, useful for mixed SELECT):
  SUM(CASE
      WHEN platform = 'Amazon'   AND ISNULL(transaction_type, '') IN ('Cancel','Refund','FreeReplacement') THEN 0
      WHEN platform = 'Flipkart' AND ISNULL(event_sub_type, '')   IN ('Cancellation','Return','RTO')       THEN 0
      WHEN platform = 'Shopify'  AND ISNULL(fulfilment_type, '')  = 'unfulfilled'                          THEN 0
      ELSE MRP
  END) AS net_revenue


## RULE: ORDER COUNT PER PLATFORM

There is no universal method to count orders. Use the correct method per platform.

Amazon:               COUNT(DISTINCT order_id)
Flipkart:             COUNT(DISTINCT order_id)
Myntra:               COUNT(DISTINCT order_id)
Nykaa:                SUM(CAST(total_orders AS INT))
Shopify:              COUNT(*)   ← each row is one transaction, no order_id
Zepto:                COUNT(*)   ← each row is one transaction, no order_id

NEVER use COUNT(*) for Amazon/Flipkart/Myntra — one order has multiple rows (one per SKU).
NEVER use COUNT(DISTINCT order_id) for Shopify/Nykaa/Zepto — order_id is NULL.

Cross-platform total order count:
  SELECT
      COUNT(DISTINCT CASE WHEN platform IN ('Amazon','Flipkart','Myntra') THEN order_id END)
    + SUM(CASE WHEN platform = 'Nykaa' THEN CAST(total_orders AS INT) ELSE 0 END)
    + COUNT(CASE WHEN platform IN ('Shopify','Zepto') THEN 1 END)
  AS total_orders
  FROM [dbo].[B2B_B2C]

Flipkart order count (confirmed sales only — exclude cancellations):
  SELECT COUNT(DISTINCT order_id) AS order_count
  FROM [dbo].[B2B_B2C]
  WHERE platform = 'Flipkart'
    AND ISNULL(event_sub_type, '') = 'Sale'

Amazon order count (confirmed shipments only):
  SELECT COUNT(DISTINCT order_id) AS order_count
  FROM [dbo].[B2B_B2C]
  WHERE platform = 'Amazon'
    AND ISNULL(transaction_type, '') = 'Shipment'


## RULE: REVENUE CALCULATIONS

Total net revenue (all platforms, all time):
  SELECT SUM(MRP) AS net_revenue
  FROM [dbo].[B2B_B2C]
  WHERE NOT (
      (platform = 'Amazon'   AND ISNULL(transaction_type, '') IN ('Cancel','Refund','FreeReplacement'))
   OR (platform = 'Flipkart' AND ISNULL(event_sub_type, '')   IN ('Cancellation','Return','RTO'))
   OR (platform = 'Shopify'  AND ISNULL(fulfilment_type, '')  = 'unfulfilled')
  )

Revenue by platform:
  SELECT platform, SUM(MRP) AS revenue, SUM(quantity) AS units
  FROM [dbo].[B2B_B2C]
  WHERE NOT ( ... net_sales_filter ... )
  GROUP BY platform
  ORDER BY SUM(MRP) DESC

Average Selling Price (ASP) — correct formula:
  SUM(MRP) / NULLIF(SUM(quantity), 0) AS asp
  DO NOT USE AVG(MRP) — MRP is a line-item total, not a unit price.

Average Order Value (AOV) — only for platforms with order_id:
  SUM(MRP) / NULLIF(COUNT(DISTINCT order_id), 0) AS aov
  WHERE platform IN ('Amazon', 'Flipkart', 'Myntra')

Per-unit price (when needed):
  MRP / NULLIF(quantity, 0) AS unit_price

Brand revenue only (exclude Nykaa/Zepto sell-through):
  WHERE salestype = 'Primary'
  -- or equivalently: WHERE platform NOT IN ('Nykaa', 'Zepto')


## RULE: UNITS SOLD

SUM(quantity) works correctly for ALL 6 platforms including Nykaa.
Data confirmed: Nykaa sum(quantity) = sum(total_qty) — they are identical.
Total units = SUM(quantity) universally.

Net units (excluding cancelled/returned):
  SUM(CASE
      WHEN platform = 'Amazon'   AND ISNULL(transaction_type, '') IN ('Cancel','Refund','FreeReplacement') THEN 0
      WHEN platform = 'Flipkart' AND ISNULL(event_sub_type, '')   IN ('Cancellation','Return','RTO')       THEN 0
      WHEN platform = 'Shopify'  AND ISNULL(fulfilment_type, '')  = 'unfulfilled'                          THEN 0
      ELSE quantity
  END) AS net_units


## RULE: DATE FILTERING

Shopify rows have NO order_date — ALL Shopify rows have NULL order_date.
Any date filter automatically excludes all Shopify revenue. This is expected behaviour.

Amazon has ~87 rows with NULL order_date (older data). These are also excluded by date filters.

Correct date filter pattern:
  WHERE order_date >= '2026-01-01'
    AND order_date <= '2026-01-31'
    AND order_date IS NOT NULL

T-SQL date functions (NOT MySQL / ANSI):
  Last 30 days:          WHERE order_date >= DATEADD(day, -30, GETDATE())
  This month:            WHERE YEAR(order_date) = YEAR(GETDATE()) AND MONTH(order_date) = MONTH(GETDATE())
  Date difference:       DATEDIFF(month, start_date, end_date)

NEVER use:
  NOW()   → use GETDATE()
  DATE_SUB()  → use DATEADD()

Monthly trend — correct chronological sort:
  GROUP BY month_year
  ORDER BY MIN(order_date)   ← ALWAYS use this for chronological order

DO NOT USE:
  ORDER BY month_year  ← sorts alphabetically: 'Dec 2025' > 'Jan 2026' is WRONG

When user asks for monthly trend, exclude Shopify-only rows:
  WHERE month_year IS NOT NULL  -- this excludes Shopify automatically


## RULE: PRODUCT GROUPING

Always use product_name for GROUP BY when the question is about products.
NEVER use product_description for GROUP BY — it is raw, unstandardised platform text.

When grouping by product, always add:
  WHERE product_name IS NOT NULL
Reason: NULL product_name means unmapped accessories (Beauty Blender, Finger Blender,
etc.) not in EAN_Master. Showing NULL as a "product" confuses users.

Product performance query pattern:
  SELECT TOP 10
      product_name,
      article_type,
      SUM(quantity) AS units_sold,
      SUM(MRP) AS revenue
  FROM [dbo].[B2B_B2C]
  WHERE product_name IS NOT NULL
    AND NOT ( ... net_sales_filter ... )
  GROUP BY product_name, article_type
  ORDER BY SUM(MRP) DESC

When user asks about a specific product, filter with LIKE or exact match:
  WHERE product_name LIKE '%Foundation%'     ← for category search
  WHERE product_name = 'Matte Foundation 03' ← for exact product
  WHERE article_type = 'Foundation'           ← for category

MRP varies by platform for the same product. When reporting price per product:
  Always GROUP BY product_name, platform
  Never report a single MRP for a product without platform breakdown.


## RULE: GEOGRAPHIC / STATE FILTERING

ship_to_state contains MIXED formats. The same state appears with both:
  - 2-letter abbreviation (from Shopify, Myntra): 'Mh', 'Dl', 'Up'
  - Full name (from Amazon, Flipkart): 'Maharashtra', 'Delhi', 'Uttar Pradesh'

Always use IN() with BOTH spellings when filtering by state.

State IN() reference (use this exact format for SQL generation):
  Maharashtra:       IN ('Mh', 'Maharashtra')
  Delhi:             IN ('Dl', 'Delhi')
  Uttar Pradesh:     IN ('Up', 'Uttar pradesh')
  Punjab:            IN ('Pb', 'Punjab')
  Gujarat:           IN ('Gj', 'Gujarat')
  Haryana:           IN ('Hr', 'Haryana')
  West Bengal:       IN ('Wb', 'West bengal')
  Madhya Pradesh:    IN ('Mp', 'Madhya pradesh')
  Karnataka:         IN ('Ka', 'Karnataka')
  Rajasthan:         IN ('Rj', 'Rajasthan')
  Bihar:             IN ('Br', 'Bihar')
  Telangana:         IN ('Ts', 'Tg', 'Telangana')
  Jharkhand:         IN ('Jh', 'Jharkhand')
  Tamil Nadu:        IN ('Tn', 'Tamil nadu')
  Odisha:            IN ('Or', 'Odisha')
  Assam:             IN ('As', 'Assam')
  Uttarakhand:       IN ('Uk', 'Ut', 'Uttarakhand')
  Kerala:            IN ('Kl', 'Kerala')
  Andhra Pradesh:    IN ('Ap', 'Andhra pradesh')
  Chhattisgarh:      IN ('Cg', 'Ct', 'Chhattisgarh')
  Himachal Pradesh:  IN ('Hp', 'Himachal pradesh')
  Jammu & Kashmir:   IN ('Jk', 'Jammu & kashmir', 'Jammu and kashmir')
  Chandigarh:        IN ('Ch', 'Chandigarh')
  Goa:               IN ('Ga', 'Goa')

Single-abbreviation states (no dual spelling):
  Meghalaya: 'Ml' | Manipur: 'Mn' | Mizoram: 'Mz' | Nagaland: 'Nl'
  Tripura: 'Tr' | Sikkim: 'Sk' | Arunachal Pradesh: 'Ar'
  Andaman & Nicobar: 'An' | Dadra & Nagar Haveli: 'Dn' | Ladakh: 'La'

Nykaa and Zepto have NO state data. Geographic analysis automatically excludes them.
Always add:  WHERE ship_to_state IS NOT NULL


## RULE: B2B vs B2C ANALYSIS

ordertype column: 'B2B' or 'B2C' — never NULL, hard-coded per source table.

When user asks about B2B sales, wholesale, or distributor orders:
  WHERE ordertype = 'B2B'

When user asks about B2C, retail, or consumer orders:
  WHERE ordertype = 'B2C'

Amazon is the ONLY platform with both B2B and B2C rows.
  Amazon B2C: WHERE platform = 'Amazon' AND ordertype = 'B2C'
  Amazon B2B: WHERE platform = 'Amazon' AND ordertype = 'B2B'

B2B vs B2C revenue split:
  SELECT ordertype, SUM(MRP) AS revenue, SUM(quantity) AS units
  FROM [dbo].[B2B_B2C]
  WHERE NOT ( ... net_sales_filter ... )
  GROUP BY ordertype


## RULE: NYKAA-SPECIFIC QUERIES

All Nykaa-specific columns are NULL for other platforms. Always filter by platform = 'Nykaa'.

Nykaa order count:
  SELECT SUM(CAST(total_orders AS INT)) AS nykaa_orders
  FROM [dbo].[B2B_B2C]
  WHERE platform = 'Nykaa'

Nykaa discount percentage:
  SELECT
      product_name,
      SUM(CAST(display_price AS DECIMAL(18,2))) AS total_display,
      SUM(CAST(selling_price AS DECIMAL(18,2))) AS total_selling,
      (SUM(CAST(display_price AS DECIMAL(18,2))) - SUM(CAST(selling_price AS DECIMAL(18,2))))
        / NULLIF(SUM(CAST(display_price AS DECIMAL(18,2))), 0) * 100 AS discount_pct
  FROM [dbo].[B2B_B2C]
  WHERE platform = 'Nykaa'
    AND product_name IS NOT NULL
  GROUP BY product_name
  ORDER BY discount_pct DESC

Nykaa category breakdown:
  SELECT category_l2, category_l3, SUM(MRP) AS revenue
  FROM [dbo].[B2B_B2C]
  WHERE platform = 'Nykaa'
  GROUP BY category_l2, category_l3
  ORDER BY SUM(MRP) DESC

Nykaa customer count:
  SELECT SUM(CAST(total_customers AS INT)) AS total_customers
  FROM [dbo].[B2B_B2C]
  WHERE platform = 'Nykaa'

⚠️ display_price, selling_price, total_orders, total_customers, category_l1/l2/l3
   are stored as NVARCHAR in the database. Cast before arithmetic:
   CAST(display_price AS DECIMAL(18,2))
   CAST(total_orders AS INT)


## RULE: SHOPIFY-SPECIFIC BEHAVIOUR

Shopify accounts for ~80% of all rows (~7,700 of ~10,000 rows).
ALL Shopify rows have NULL order_date and NULL month_year.

Consequences:
  1. Any date filter excludes ALL Shopify revenue.
  2. Monthly trend analysis excludes Shopify.
  3. Shopify order count uses COUNT(*) — not COUNT(DISTINCT order_id).
  4. Shopify has NO geographic data from ship_to columns (actually it does have ship_to).
     Correction: Shopify HAS ship_to_state and ship_to_city data. 34 distinct states.

Shopify fulfilment analysis:
  SELECT fulfilment_type, COUNT(*) AS row_count, SUM(MRP) AS revenue
  FROM [dbo].[B2B_B2C]
  WHERE platform = 'Shopify'
  GROUP BY fulfilment_type

Shopify net revenue (exclude unfulfilled):
  SELECT SUM(MRP) AS shopify_net_revenue
  FROM [dbo].[B2B_B2C]
  WHERE platform = 'Shopify'
    AND ISNULL(fulfilment_type, '') != 'unfulfilled'

Shopify payment method analysis (use transaction_type, not payment_method):
  SELECT transaction_type AS payment_method, COUNT(*) AS orders, SUM(MRP) AS revenue
  FROM [dbo].[B2B_B2C]
  WHERE platform = 'Shopify'
    AND ISNULL(fulfilment_type, '') = 'fulfilled'
  GROUP BY transaction_type
  ORDER BY COUNT(*) DESC


## RULE: CANCELLATION AND RETURN ANALYSIS

Cancellation rate (Amazon + Flipkart only — only platforms with cancellation data):
  SELECT
      CAST(SUM(CASE
          WHEN (platform = 'Amazon'   AND ISNULL(transaction_type, '') = 'Cancel')
            OR (platform = 'Flipkart' AND ISNULL(event_sub_type, '')   = 'Cancellation')
          THEN 1 ELSE 0
      END) AS FLOAT) / NULLIF(COUNT(*), 0) * 100 AS cancel_rate
  FROM [dbo].[B2B_B2C]
  WHERE platform IN ('Amazon', 'Flipkart')

Return rate (Amazon + Flipkart only):
  SELECT
      CAST(SUM(CASE
          WHEN (platform = 'Amazon'   AND ISNULL(transaction_type, '') = 'Refund')
            OR (platform = 'Flipkart' AND ISNULL(event_sub_type, '')   IN ('Return', 'RTO'))
          THEN 1 ELSE 0
      END) AS FLOAT) / NULLIF(COUNT(*), 0) * 100 AS return_rate
  FROM [dbo].[B2B_B2C]
  WHERE platform IN ('Amazon', 'Flipkart')

Cancelled orders value (Amazon):
  SELECT SUM(ABS(MRP)) AS cancelled_value
  FROM [dbo].[B2B_B2C]
  WHERE platform = 'Amazon'
    AND ISNULL(transaction_type, '') IN ('Cancel', 'Refund', 'FreeReplacement')

Note: Amazon Refund rows have NEGATIVE MRP values. Use ABS(MRP) when summing refunded amounts.


## RULE: MONTHLY TREND ANALYSIS

Standard monthly trend (all platforms with dates):
  SELECT
      month_year,
      SUM(MRP) AS revenue,
      SUM(quantity) AS units
  FROM [dbo].[B2B_B2C]
  WHERE month_year IS NOT NULL
    AND NOT (
        (platform = 'Amazon'   AND ISNULL(transaction_type, '') IN ('Cancel','Refund','FreeReplacement'))
     OR (platform = 'Flipkart' AND ISNULL(event_sub_type, '')   IN ('Cancellation','Return','RTO'))
     OR (platform = 'Shopify'  AND ISNULL(fulfilment_type, '')  = 'unfulfilled')
    )
  GROUP BY month_year
  ORDER BY MIN(order_date)

Shopify is automatically excluded from this query (month_year is NULL for Shopify).
Inform users that monthly trend results do not include Shopify data.

Monthly trend by platform:
  SELECT
      month_year,
      platform,
      SUM(MRP) AS revenue
  FROM [dbo].[B2B_B2C]
  WHERE month_year IS NOT NULL
    AND NOT ( ... net_sales_filter ... )
  GROUP BY month_year, platform
  ORDER BY MIN(order_date), platform


## RULE: TOP N QUERIES

T-SQL uses TOP N syntax, not LIMIT N (LIMIT does not exist in SQL Server).

Top 10 products by revenue:
  SELECT TOP 10
      product_name,
      article_type,
      SUM(MRP) AS revenue,
      SUM(quantity) AS units
  FROM [dbo].[B2B_B2C]
  WHERE product_name IS NOT NULL
    AND NOT ( ... net_sales_filter ... )
  GROUP BY product_name, article_type
  ORDER BY SUM(MRP) DESC

Top 5 states by units:
  SELECT TOP 5
      ship_to_state,
      SUM(quantity) AS units
  FROM [dbo].[B2B_B2C]
  WHERE ship_to_state IS NOT NULL
    AND NOT ( ... net_sales_filter ... )
  GROUP BY ship_to_state
  ORDER BY SUM(quantity) DESC

Top product per platform (using ROW_NUMBER window function):
  WITH ranked AS (
      SELECT
          platform,
          product_name,
          SUM(MRP) AS revenue,
          ROW_NUMBER() OVER (PARTITION BY platform ORDER BY SUM(MRP) DESC) AS rn
      FROM [dbo].[B2B_B2C]
      WHERE product_name IS NOT NULL
        AND NOT ( ... net_sales_filter ... )
      GROUP BY platform, product_name
  )
  SELECT platform, product_name, revenue
  FROM ranked
  WHERE rn = 1
  ORDER BY revenue DESC


## RULE: T-SQL SYNTAX REFERENCE

These are common MySQL / ANSI SQL constructs that DO NOT WORK in SQL Server.
Always use the T-SQL equivalent.

WRONG                               CORRECT (T-SQL / SQL Server)
──────────────────────────────────────────────────────────────────
LIMIT N                           → TOP N  (before SELECT list)
NOW()                             → GETDATE()
COALESCE(col, default)            → ISNULL(col, default)  [for 2-arg]
ORDER BY month_year               → ORDER BY MIN(order_date)
AVG(MRP)                          → SUM(MRP) / NULLIF(SUM(quantity), 0)
GROUP BY product_description      → GROUP BY product_name
DATE_SUB(NOW(), INTERVAL 30 DAY)  → DATEADD(day, -30, GETDATE())
DATEDIFF('day', d1, d2)           → DATEDIFF(day, d1, d2)  [no quotes]
ROUND(x, 2)                       → CAST(x AS DECIMAL(10,2))
CONCAT(a, b)                      → CONCAT(a, b)  [valid in SQL Server 2012+]
IF(cond, a, b)                    → CASE WHEN cond THEN a ELSE b END
IFNULL(col, 0)                    → ISNULL(col, 0)

Always return ONLY the SQL query — no markdown fences, no explanations.


## RULE: COMMON COMPLETE QUERY PATTERNS

### Total revenue and units, all time
SELECT
    SUM(MRP) AS net_revenue,
    SUM(quantity) AS net_units,
    COUNT(DISTINCT CASE WHEN platform IN ('Amazon','Flipkart','Myntra') THEN order_id END)
      + SUM(CASE WHEN platform = 'Nykaa' THEN CAST(total_orders AS INT) ELSE 0 END)
      + COUNT(CASE WHEN platform IN ('Shopify','Zepto') THEN 1 END) AS total_orders
FROM [dbo].[B2B_B2C]
WHERE NOT (
    (platform = 'Amazon'   AND ISNULL(transaction_type,'') IN ('Cancel','Refund','FreeReplacement'))
 OR (platform = 'Flipkart' AND ISNULL(event_sub_type,'')   IN ('Cancellation','Return','RTO'))
 OR (platform = 'Shopify'  AND ISNULL(fulfilment_type,'')  = 'unfulfilled')
)

### Revenue by platform with share
SELECT
    platform,
    SUM(MRP) AS revenue,
    SUM(quantity) AS units,
    CAST(SUM(MRP) * 100.0 / SUM(SUM(MRP)) OVER () AS DECIMAL(5,2)) AS revenue_share_pct
FROM [dbo].[B2B_B2C]
WHERE NOT (
    (platform = 'Amazon'   AND ISNULL(transaction_type,'') IN ('Cancel','Refund','FreeReplacement'))
 OR (platform = 'Flipkart' AND ISNULL(event_sub_type,'')   IN ('Cancellation','Return','RTO'))
 OR (platform = 'Shopify'  AND ISNULL(fulfilment_type,'')  = 'unfulfilled')
)
GROUP BY platform
ORDER BY SUM(MRP) DESC

### Top 10 products by revenue
SELECT TOP 10
    product_name,
    article_type,
    SUM(quantity) AS units_sold,
    SUM(MRP) AS revenue,
    SUM(MRP) / NULLIF(SUM(quantity), 0) AS asp
FROM [dbo].[B2B_B2C]
WHERE product_name IS NOT NULL
  AND NOT (
      (platform = 'Amazon'   AND ISNULL(transaction_type,'') IN ('Cancel','Refund','FreeReplacement'))
   OR (platform = 'Flipkart' AND ISNULL(event_sub_type,'')   IN ('Cancellation','Return','RTO'))
   OR (platform = 'Shopify'  AND ISNULL(fulfilment_type,'')  = 'unfulfilled')
  )
GROUP BY product_name, article_type
ORDER BY SUM(MRP) DESC

### Monthly revenue trend
SELECT
    month_year,
    SUM(MRP) AS revenue,
    SUM(quantity) AS units
FROM [dbo].[B2B_B2C]
WHERE month_year IS NOT NULL
  AND NOT (
      (platform = 'Amazon'   AND ISNULL(transaction_type,'') IN ('Cancel','Refund','FreeReplacement'))
   OR (platform = 'Flipkart' AND ISNULL(event_sub_type,'')   IN ('Cancellation','Return','RTO'))
   OR (platform = 'Shopify'  AND ISNULL(fulfilment_type,'')  = 'unfulfilled')
  )
GROUP BY month_year
ORDER BY MIN(order_date)

### Sales by state
SELECT
    ship_to_state,
    SUM(MRP) AS revenue,
    SUM(quantity) AS units
FROM [dbo].[B2B_B2C]
WHERE ship_to_state IS NOT NULL
  AND NOT (
      (platform = 'Amazon'   AND ISNULL(transaction_type,'') IN ('Cancel','Refund','FreeReplacement'))
   OR (platform = 'Flipkart' AND ISNULL(event_sub_type,'')   IN ('Cancellation','Return','RTO'))
   OR (platform = 'Shopify'  AND ISNULL(fulfilment_type,'')  = 'unfulfilled')
  )
GROUP BY ship_to_state
ORDER BY SUM(MRP) DESC

### Foundation sales breakdown by shade
SELECT
    product_name,
    SUM(quantity) AS units_sold,
    SUM(MRP) AS revenue
FROM [dbo].[B2B_B2C]
WHERE article_type = 'Foundation'
  AND product_name IS NOT NULL
  AND NOT (
      (platform = 'Amazon'   AND ISNULL(transaction_type,'') IN ('Cancel','Refund','FreeReplacement'))
   OR (platform = 'Flipkart' AND ISNULL(event_sub_type,'')   IN ('Cancellation','Return','RTO'))
   OR (platform = 'Shopify'  AND ISNULL(fulfilment_type,'')  = 'unfulfilled')
  )
GROUP BY product_name
ORDER BY SUM(MRP) DESC

### COD vs Prepaid split
SELECT
    CASE
        WHEN ISNULL(payment_method,'') IN ('COD','Cash on Delivery (COD)') THEN 'COD'
        WHEN payment_method IS NULL THEN 'Unknown'
        ELSE 'Prepaid'
    END AS payment_type,
    COUNT(*) AS transactions,
    SUM(MRP) AS revenue
FROM [dbo].[B2B_B2C]
WHERE platform IN ('Amazon', 'Myntra', 'Shopify')
  AND NOT (
      (platform = 'Amazon'   AND ISNULL(transaction_type,'') IN ('Cancel','Refund','FreeReplacement'))
   OR (platform = 'Shopify'  AND ISNULL(fulfilment_type,'')  = 'unfulfilled')
  )
GROUP BY
    CASE
        WHEN ISNULL(payment_method,'') IN ('COD','Cash on Delivery (COD)') THEN 'COD'
        WHEN payment_method IS NULL THEN 'Unknown'
        ELSE 'Prepaid'
    END
ORDER BY SUM(MRP) DESC


## RULE: WHAT NOT TO DO — COMMON MISTAKES

1. NEVER use LIMIT N — use TOP N
2. NEVER use AVG(MRP) for average price — use SUM(MRP)/NULLIF(SUM(quantity),0)
3. NEVER use COUNT(*) for Amazon/Flipkart/Myntra orders — use COUNT(DISTINCT order_id)
4. NEVER use COUNT(DISTINCT order_id) for Shopify/Nykaa/Zepto — order_id is NULL
5. NEVER GROUP BY product_description — use product_name
6. NEVER ORDER BY month_year — use ORDER BY MIN(order_date)
7. NEVER filter WHERE ship_to_state = 'Maharashtra' alone — use IN('Mh','Maharashtra')
8. NEVER use NOW() — use GETDATE()
9. NEVER query source tables — ONLY query [dbo].[B2B_B2C]
10. NEVER use invoice_date or invoice_number — both are 100% NULL and being removed
11. NEVER mix Primary + Secondary salestype in one revenue SUM without noting it
12. NEVER omit product_name IS NOT NULL when grouping by product
13. NEVER assume Shopify revenue appears in date-filtered results — it won't


## RULE: SECURITY — READ-ONLY ONLY

All queries must be read-only SELECT statements.
Never generate: INSERT, UPDATE, DELETE, DROP, ALTER, TRUNCATE, CREATE
Never generate: EXEC, xp_, GRANT, REVOKE, BULK INSERT, OPENROWSET
Never use: stacked statements (semicolon-separated multiple statements)
Never access: linked servers or four-part names other than [Charmacy_f_automate].[dbo].[B2B_B2C]

Valid query starters:
  SELECT ...
  WITH cte_name AS ( SELECT ... ) SELECT ...

Return ONLY the SQL query. No markdown fences. No explanations. No inline comments.
The query must be directly executable on SQL Server.
