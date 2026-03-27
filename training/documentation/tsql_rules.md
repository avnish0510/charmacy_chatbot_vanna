# T-SQL Rules for Charmacy Milano Text-to-SQL Chatbot

## CRITICAL: You are querying Microsoft SQL Server (SSMS), NOT MySQL / PostgreSQL / SQLite.
## TARGET: [Charmacy_f_automate].[dbo].[B2B_B2C]  — always use this view, never source tables.

---

## 1. ALLOWED STATEMENTS — READ-ONLY ONLY

Only these statement types are permitted:

```sql
SELECT ...
WITH cte_name AS ( ... ) SELECT ...
```

**NEVER generate:**
- `INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER`, `TRUNCATE`, `CREATE`
- `GRANT`, `REVOKE`, `EXEC`, `EXECUTE`
- `xp_cmdshell`, `BULK INSERT`, `OPENROWSET`, `OPENDATASOURCE`
- Stacked statements (semicolons separating multiple statements)
- Linked server queries (`server.database.schema.table`)

Any SQL that does not begin with `SELECT` or `WITH` must be rejected.

---

## 2. T-SQL SYNTAX RULES (Differences from MySQL/PostgreSQL)

### 2.1 Row Limiting
```sql
-- WRONG (MySQL/PostgreSQL syntax):
SELECT * FROM [dbo].[B2B_B2C] LIMIT 10

-- CORRECT (T-SQL):
SELECT TOP 10 * FROM [dbo].[B2B_B2C]
SELECT TOP (10) * FROM [dbo].[B2B_B2C]  -- parentheses optional but valid
```

### 2.2 Current Timestamp
```sql
-- WRONG:  NOW()   CURRENT_TIMESTAMP   SYSDATE()
-- CORRECT:
GETDATE()              -- returns current datetime
CAST(GETDATE() AS DATE) -- current date only
```

### 2.3 NULL Handling
```sql
-- WRONG:  COALESCE(column, 'default')  [use for 2-arg case]
-- CORRECT (T-SQL preferred for single fallback):
ISNULL(column, 'default')
ISNULL(transaction_type, '')
ISNULL(event_sub_type, '')

-- COALESCE is valid T-SQL but use ISNULL for single-fallback cases
```

### 2.4 Date Arithmetic
```sql
-- WRONG:  DATE_SUB(NOW(), INTERVAL 30 DAY)
-- CORRECT:
DATEADD(day, -30, GETDATE())       -- 30 days ago
DATEADD(month, -1, GETDATE())      -- 1 month ago
DATEADD(year, -1, GETDATE())       -- 1 year ago

-- Date difference:
DATEDIFF(day, start_date, end_date)
DATEDIFF(month, start_date, end_date)

-- Current month filter:
WHERE order_date >= DATEADD(month, DATEDIFF(month,0,GETDATE()), 0)
  AND order_date <  DATEADD(month, DATEDIFF(month,0,GETDATE())+1, 0)

-- Last month filter:
WHERE order_date >= DATEADD(month, DATEDIFF(month,0,GETDATE())-1, 0)
  AND order_date <  DATEADD(month, DATEDIFF(month,0,GETDATE()), 0)
```

### 2.5 String Functions
```sql
-- Substring (1-based index in T-SQL, not 0-based):
SUBSTRING(column, 1, 5)     -- first 5 characters
LEFT(column, 5)
RIGHT(column, 5)
LEN(column)                 -- length (not LENGTH())

-- String concatenation:
'Hello' + ' ' + 'World'     -- T-SQL uses + not CONCAT() [CONCAT also works]

-- Case conversion:
UPPER(column)
LOWER(column)
```

### 2.6 Type Conversion
```sql
-- Safe conversion (returns NULL on failure):
TRY_CONVERT(DATE, column)
TRY_CONVERT(DECIMAL(18,2), column)
TRY_CONVERT(INT, column)

-- Regular conversion (throws error on failure):
CAST(column AS DECIMAL(10,2))
CAST(column AS NVARCHAR(255))
CONVERT(VARCHAR(7), order_date, 120)   -- formats as 'YYYY-MM'
```

### 2.7 Division Safety
```sql
-- ALWAYS protect against divide-by-zero:
SUM(MRP) / NULLIF(SUM(quantity), 0)           -- ASP
SUM(MRP) / NULLIF(COUNT(DISTINCT order_id), 0) -- AOV
```

### 2.8 Rounding
```sql
-- CORRECT:
CAST(SUM(MRP) AS DECIMAL(10,2))
ROUND(SUM(MRP), 2)
```

---

## 3. REVENUE AND MRP RULES

### 3.1 What MRP Means
`MRP` = **TOTAL line-item invoice amount** (quantity × unit price). It is NOT the unit price.

```sql
-- Revenue = sum of all line-item totals:
SUM(MRP)

-- Per-unit price = line-item total ÷ quantity:
MRP / NULLIF(quantity, 0)

-- Average Selling Price (ASP) = total revenue ÷ total units:
SUM(MRP) / NULLIF(SUM(quantity), 0)

-- Average Order Value (AOV) — Amazon/Flipkart/Myntra ONLY:
SUM(MRP) / NULLIF(COUNT(DISTINCT order_id), 0)

-- WRONG — never use:
AVG(MRP)    -- meaningless; MRP is already a line-item total
```

### 3.2 Net Sales Filter — ALWAYS Apply for Revenue Queries
Raw data includes cancellations, returns, and unfulfilled orders. Always exclude them:

```sql
WHERE NOT (
    (platform = 'Amazon'   AND ISNULL(transaction_type,'')  IN ('Cancel','Refund','FreeReplacement'))
    OR (platform = 'Flipkart' AND ISNULL(event_sub_type,'')   IN ('Cancellation','Return','RTO'))
    OR (platform = 'Shopify'  AND ISNULL(fulfilment_type,'')  = 'unfulfilled')
)
```

This is the **standard net sales filter**. Use it as a CTE or inline WHERE clause in every revenue query.

### 3.3 Primary vs Secondary Revenue
- **Primary** salestype = brand direct revenue: Amazon (B2C + B2B), Flipkart, Myntra, Shopify
- **Secondary** salestype = sell-through tracking only: Nykaa, Zepto

```sql
-- NEVER mix Primary + Secondary in one revenue sum without explanation.
-- For brand revenue (default):
WHERE salestype = 'Primary'

-- For sell-through analysis (Nykaa/Zepto only):
WHERE salestype = 'Secondary'
```

---

## 4. ORDER COUNTING RULES — CRITICAL, PLATFORM-SPECIFIC

There is no universal order count. Each platform requires a different SQL approach:

| Platform | Correct Method | Reason |
|---|---|---|
| Amazon | `COUNT(DISTINCT order_id)` | Has order_id; one order = multiple rows |
| Flipkart | `COUNT(DISTINCT order_id)` | Has order_id; one order = multiple rows |
| Myntra | `COUNT(DISTINCT order_id)` | Has order_id; one order = multiple rows |
| Nykaa | `SUM(total_orders)` | Pre-aggregated; no order_id |
| Shopify | `COUNT(*)` | No order_id (all NULL); each row = 1 transaction |
| Zepto | `COUNT(*)` | No order_id (all NULL); each row = 1 transaction |

```sql
-- WRONG for Amazon/Flipkart/Myntra:
COUNT(*)                  -- one order can have 5 product rows → overcounts

-- WRONG for Shopify/Nykaa/Zepto:
COUNT(DISTINCT order_id)  -- order_id is NULL → always returns 0 or 1

-- Cross-platform total orders (must use conditional SQL):
  COUNT(DISTINCT CASE WHEN platform IN ('Amazon','Flipkart','Myntra') THEN order_id END)
+ SUM(CASE WHEN platform = 'Nykaa' THEN total_orders ELSE 0 END)
+ COUNT(CASE WHEN platform IN ('Shopify','Zepto') THEN 1 END)
```

---

## 5. DATE AND TIME RULES

### 5.1 Shopify Has NO order_date
All Shopify rows have `order_date = NULL` and `month_year = NULL`.
Any query with a date filter **automatically excludes ALL Shopify revenue**.
Do not add a note about this — just apply the filter correctly.

### 5.2 Amazon Has ~87 Rows with NULL order_date
Always add `AND order_date IS NOT NULL` when filtering by date range to prevent NULLs appearing in results.

### 5.3 Monthly Chronological Sort
`month_year` is stored as `'2026-01'` (VARCHAR). Sorting by it alphabetically works for same-year data but is unreliable across years.

```sql
-- WRONG:
ORDER BY month_year           -- alphabetical sort, not chronological

-- CORRECT:
ORDER BY MIN(order_date)      -- chronological sort; works even with NULLs (NULLs sort to end/start)
```

### 5.4 Standard Date Filter Template
```sql
-- For a specific month (e.g. January 2026):
WHERE order_date >= '2026-01-01'
  AND order_date < '2026-02-01'
  AND order_date IS NOT NULL

-- For year-to-date:
WHERE order_date >= '2026-01-01'
  AND order_date IS NOT NULL

-- For last 30 days:
WHERE order_date >= DATEADD(day, -30, GETDATE())
  AND order_date IS NOT NULL
```

---

## 6. PRODUCT GROUPING RULES

### 6.1 Always Use product_name, Never product_description
```sql
-- WRONG:
GROUP BY product_description   -- raw platform listing text; same product has different names

-- CORRECT:
GROUP BY product_name          -- standardised canonical name from EAN_Master

-- Always add NULL guard:
WHERE product_name IS NOT NULL  -- ~34% of rows have NULL product_name (unmapped products)
```

### 6.2 Product Dimensions (from EAN_Master)
All product columns come from the EAN_Master LEFT JOIN. They are NULL if the product is not in EAN_Master.

- `product_name` — canonical name, e.g. "Matte Foundation 03"
- `article_type` — category, e.g. "Foundation", "Lipstick", "Eyeliner"
- `sku_code` — internal code, e.g. "CMS_F3"
- `EAN` — barcode, all start with "8906148"

---

## 7. GEOGRAPHIC QUERY RULES

### 7.1 ship_to_state Has Mixed Formats
The same state appears in two formats. Always use `IN()` with BOTH spellings:

```sql
-- WRONG (misses ~half the rows):
WHERE ship_to_state = 'Maharashtra'

-- CORRECT:
WHERE ship_to_state IN ('Maharashtra', 'Mh')
WHERE ship_to_state IN ('Uttar Pradesh', 'Up', 'Uttar pradesh')
WHERE ship_to_state IN ('Delhi', 'Dl')
WHERE ship_to_state IN ('Punjab', 'Pb')
WHERE ship_to_state IN ('Gujarat', 'Gj')
WHERE ship_to_state IN ('Haryana', 'Hr')
WHERE ship_to_state IN ('West Bengal', 'Wb', 'West bengal')
WHERE ship_to_state IN ('Madhya Pradesh', 'Mp', 'Madhya pradesh')
WHERE ship_to_state IN ('Karnataka', 'Ka')
WHERE ship_to_state IN ('Rajasthan', 'Rj')
WHERE ship_to_state IN ('Bihar', 'Br')
WHERE ship_to_state IN ('Telangana', 'Ts', 'Tg')
WHERE ship_to_state IN ('Jharkhand', 'Jh')
WHERE ship_to_state IN ('Tamil Nadu', 'Tn', 'Tamil nadu')
WHERE ship_to_state IN ('Odisha', 'Or')
WHERE ship_to_state IN ('Assam', 'As')
WHERE ship_to_state IN ('Uttarakhand', 'Uk', 'Ut')
WHERE ship_to_state IN ('Kerala', 'Kl')
WHERE ship_to_state IN ('Andhra Pradesh', 'Ap', 'Andhra pradesh')
WHERE ship_to_state IN ('Chhattisgarh', 'Cg', 'Ct')
```

### 7.2 Platform Geographic Data Availability
- `ship_to_state`: Amazon ✅, Flipkart ✅, Myntra ✅, Shopify ✅, Nykaa ❌, Zepto ❌
- `ship_to_city`: Amazon ✅, Myntra ✅, Shopify ✅, Flipkart ❌, Nykaa ❌, Zepto ❌
- `ship_to_postal_code`: Amazon ✅, Shopify ✅ only

---

## 8. QUERY STRUCTURE BEST PRACTICES

### 8.1 CTE Pattern (Preferred for Complex Queries)
```sql
WITH net_sales AS (
    SELECT *
    FROM [dbo].[B2B_B2C]
    WHERE NOT (
        (platform = 'Amazon'   AND ISNULL(transaction_type,'')  IN ('Cancel','Refund','FreeReplacement'))
        OR (platform = 'Flipkart' AND ISNULL(event_sub_type,'')   IN ('Cancellation','Return','RTO'))
        OR (platform = 'Shopify'  AND ISNULL(fulfilment_type,'')  = 'unfulfilled')
    )
)
SELECT
    platform,
    SUM(MRP)      AS revenue,
    SUM(quantity) AS units
FROM net_sales
GROUP BY platform
ORDER BY revenue DESC;
```

### 8.2 Always Add TOP N for Open-Ended Queries
```sql
-- When no specific limit is given, default to TOP 1000:
SELECT TOP 1000 * FROM [dbo].[B2B_B2C]

-- When ranking (top products, top states, etc.):
SELECT TOP 10 product_name, SUM(MRP) AS revenue
FROM [dbo].[B2B_B2C]
WHERE product_name IS NOT NULL
GROUP BY product_name
ORDER BY revenue DESC
```

### 8.3 View Reference
Always use the view, never source tables:
```sql
-- Preferred:
FROM [dbo].[B2B_B2C]

-- Also valid (fully qualified):
FROM [Charmacy_f_automate].[dbo].[B2B_B2C]

-- NEVER:
FROM Amazon_B2C_Daily        -- source table, bypasses view logic
FROM Shopify_B2C_Daily        -- source table, bypasses view logic
```

---

## 9. OUTPUT FORMAT REQUIREMENT

Always return **only the SQL query**:
- No markdown code fences (no ``` or ```sql)
- No explanatory text before or after the SQL
- No inline SQL comments
- The query must be directly executable on SQL Server without modification

---

## 10. COMMON MISTAKES QUICK REFERENCE

| Wrong | Correct | Why |
|---|---|---|
| `LIMIT 10` | `TOP 10` | T-SQL syntax |
| `NOW()` | `GETDATE()` | T-SQL syntax |
| `AVG(MRP)` | `SUM(MRP)/NULLIF(SUM(quantity),0)` | MRP is line-item total, not unit price |
| `COUNT(*)` for Amazon orders | `COUNT(DISTINCT order_id)` | One order = multiple rows |
| `COUNT(DISTINCT order_id)` for Shopify | `COUNT(*)` | order_id is NULL for Shopify |
| `ORDER BY month_year` | `ORDER BY MIN(order_date)` | month_year sorts alphabetically |
| `GROUP BY product_description` | `GROUP BY product_name` | product_description is raw/unstandardised |
| `WHERE ship_to_state = 'UP'` | `WHERE ship_to_state IN ('Up','Uttar pradesh')` | Mixed formats |
| `SUM(MRP)` without net filter | Add cancellation/return WHERE clause | Raw data has cancelled rows |
| Mix Primary + Secondary revenue | `WHERE salestype = 'Primary'` | Nykaa/Zepto are sell-through only |
| Date filter without NULL guard | Add `AND order_date IS NOT NULL` | Shopify + some Amazon rows are NULL |
| `FROM Amazon_B2C_Daily` | `FROM [dbo].[B2B_B2C]` | Always query the view |