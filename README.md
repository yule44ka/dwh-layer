For my work I created test script for creating synthetic data and test my SQL scripts. 
You can setup environment with row tables if you want using guidance in SETUP.md. 
Assume that while transactions of payment is successful, subscription lasts. If subscription is not payed, it is cancelled. All subscriptions, that were/are active, are covered by payments.
# Task 1: Data Cleaning & Modeling (ETL)
## Raw tables

### Raw subscriptions

| **Column**      | **Description**                                             |
| --------------- | ----------------------------------------------------------- |
| **sub_id**      | Unique subscription identifier                              |
| **customer_id** | Link to the customer                                        |
| **plan_type**   | Subscription type: 'Monthly' or 'Annual'                    |
| **start_date**  | Subscription start date                                     |
| **end_date**    | Expiration date (can be NULL if the subscription is active) |
| **amount**      | Total price paid for the subscription period                |
### Raw customers

|**Column**|**Description**|
|---|---|
|**customer_id**|Unique identifier of the customer|
|**company_name**|Name of the client company|
|**country**|Country of origin|
|**signup_date**|Date when the customer account was created|
### Raw transactions

| **Column**  | **Description**                                        |
| ----------- | ------------------------------------------------------ |
| **tx_id**   | Unique transaction identifier                          |
| **sub_id**  | Reference to the subscription                          |
| **tx_date** | Date of the payment                                    |
| **status**  | Transaction status: 'Success', 'Failed', or 'Refunded' |
## Data filling
First, we create the Data Warehouse (DWH) layer by transferring data from the raw CRM tables into structured analytical tables.

| **DWH Table**         | **Source Table**  | **Purpose**                     |
| --------------------- | ----------------- | ------------------------------- |
| dwh.dim_customers     | raw_customers     | Stores customer attributes      |
| dwh.fct_subscriptions | raw_subscriptions | Stores subscription data        |
| dwh.fct_transactions  | raw_transactions  | Stores cleaned transaction data |
**Create schema**
```sql
CREATE SCHEMA IF NOT EXISTS dwh;
```

**Create tables**
### `dwh.dim_customers`
```sql
DROP TABLE IF EXISTS dwh.dim_customers;

CREATE TABLE dwh.dim_customers AS
SELECT
    customer_id,
    company_name,
    country,
    signup_date
FROM raw_customers;
```
### `dwh.fct_subscriptions`
```sql
DROP TABLE IF EXISTS dwh.fct_subscriptions;

CREATE TABLE dwh.fct_subscriptions AS
SELECT
    sub_id,
    customer_id,
    plan_type,
    start_date,
    end_date,
    amount
FROM raw_subscriptions;
```
### `dwh.fct_transactions`
Drop duplicates during creating:
```sql
DROP TABLE IF EXISTS dwh.fct_transactions;

CREATE TABLE dwh.fct_transactions AS
WITH ranked_transactions AS (
    SELECT
        tx_id,
        sub_id,
        tx_date,
        status,
        ROW_NUMBER() OVER (
            PARTITION BY tx_id, sub_id, tx_date, status
            ORDER BY tx_id
        ) AS rn
    FROM raw_transactions
)
SELECT
    tx_id,
    sub_id,
    tx_date,
    status
FROM ranked_transactions
WHERE rn = 1;
```
## Data quality checks
I implemented basic necessary quality checks. This list can be expanded by accuracy and timeliness checks.

| **Check name**                 | **Dimension** | **What it verifies**                                             | **Severity** |
| ------------------------------ | ------------- | ---------------------------------------------------------------- | ------------ |
| duplicate_transactions_check   | Uniqueness    | Checks whether the transactions table contains duplicate records | Critical     |
| null_values_check              | Completeness  | Checks whether required fields contain null values               | Critical     |
| valid_plan_type_check          | Validity      | Checks whether plan_type contains only Monthly or Annual         | Critical     |
| valid_transaction_status_check | Validity      | Checks whether status contains only Success, Failed, or Refunded | Critical     |
Create table for data quality checks:
```sql
DROP TABLE IF EXISTS dwh.data_quality_checks;

CREATE TABLE dwh.data_quality_checks (
    check_name TEXT,
    source_table TEXT,
    dimension TEXT,
    issue_count INT,
    severity TEXT,
    check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```
### Uniqueness of transactions check
```sql
INSERT INTO dwh.data_quality_checks (check_name, source_table, dimension, issue_count, severity)
SELECT
    'duplicate_transactions_check',
    'dwh.fct_transactions',
    'Uniqueness',
    COUNT(*),
    'Critical'
FROM (
    SELECT
        tx_id,
        sub_id,
        tx_date,
        status,
        COUNT(*) AS cnt
    FROM dwh.fct_transactions
    GROUP BY tx_id, sub_id, tx_date, status
    HAVING COUNT(*) > 1
) t;
```
### Completeness check
```sql
INSERT INTO dwh.data_quality_checks (check_name, source_table, dimension, issue_count, severity)
SELECT
    'null_values_check_customers',
    'dwh.dim_customers',
    'Completeness',
    COUNT(*),
    'Critical'
FROM dwh.dim_customers
WHERE customer_id IS NULL
   OR company_name IS NULL
   OR country IS NULL
   OR signup_date IS NULL;
   
INSERT INTO dwh.data_quality_checks (check_name, source_table, dimension, issue_count, severity)
SELECT
    'null_values_check_subscriptions',
    'dwh.fct_subscriptions',
    'Completeness',
    COUNT(*),
    'Critical'
FROM dwh.fct_subscriptions
WHERE sub_id IS NULL
   OR customer_id IS NULL
   OR plan_type IS NULL
   OR start_date IS NULL
   OR amount IS NULL;
   
INSERT INTO dwh.data_quality_checks (check_name, source_table, dimension, issue_count, severity)
SELECT
    'null_values_check_transactions',
    'dwh.fct_transactions',
    'Completeness',
    COUNT(*),
    'Critical'
FROM dwh.fct_transactions
WHERE tx_id IS NULL
   OR sub_id IS NULL
   OR tx_date IS NULL
   OR status IS NULL;
```
### Valid plan type
```sql
INSERT INTO dwh.data_quality_checks (check_name, source_table, dimension, issue_count, severity)
SELECT
    'valid_plan_type_check',
    'dwh.fct_subscriptions',
    'Validity',
    COUNT(*),
    'Critical'
FROM dwh.fct_subscriptions
WHERE plan_type NOT IN ('Monthly', 'Annual');
```
### Valid transaction status
```sql
INSERT INTO dwh.data_quality_checks (check_name, source_table, dimension, issue_count, severity)
SELECT
    'valid_transaction_status_check',
    'dwh.fct_transactions',
    'Validity',
    COUNT(*),
    'Critical'
FROM dwh.fct_transactions
WHERE status NOT IN ('Success', 'Failed', 'Refunded');
```
## Optimizations
Indexes are added on key columns used in joins to improve query performance in the data warehouse.
```sql
CREATE INDEX IF NOT EXISTS idx_fct_subscriptions_customer_id
    ON dwh.fct_subscriptions (customer_id);

CREATE INDEX IF NOT EXISTS idx_fct_subscriptions_sub_id
    ON dwh.fct_subscriptions (sub_id);

CREATE INDEX IF NOT EXISTS idx_fct_transactions_sub_id
    ON dwh.fct_transactions (sub_id);

CREATE INDEX IF NOT EXISTS idx_dim_customers_customer_id
    ON dwh.dim_customers (customer_id);
```
### Data Mart
This view aggregates data from the DWH layer and provides a simplified structure for analytics and reporting.
The duration of a subscription is calculated as the difference between `end_date` and `start_date`.  
If the subscription is still active (`end_date` is `NULL`), the current date is used instead.
Successful payments are counted from the transactions table.  
A `LEFT JOIN` is used to ensure that subscriptions without successful payments are still included in the result.

| **Column**                   | **Description**                                                        |
| ---------------------------- | ---------------------------------------------------------------------- |
| sub_id                       | Subscription identifier                                                |
| company_name                 | Customer company name                                                  |
| country                      | Customer country                                                       |
| subscription_duration_months | Calculated duration of the subscription                                |
| total_successful_payments    | Number of successful payments for the subscription - number of refunds |
**Create chema**
```sql
CREATE SCHEMA IF NOT EXISTS dm;
```
**Create view**
```sql
DROP VIEW IF EXISTS dm.dm_sales_performance;

CREATE VIEW dm.dm_sales_performance AS
SELECT
    s.sub_id,
    c.company_name,
    c.country,
    GREATEST(
        (
            EXTRACT(YEAR FROM AGE(COALESCE(s.end_date, CURRENT_DATE), s.start_date)) * 12
          + EXTRACT(MONTH FROM AGE(COALESCE(s.end_date, CURRENT_DATE), s.start_date))
          + CASE
                WHEN s.end_date IS NULL
                 AND EXTRACT(DAY FROM CURRENT_DATE) >= EXTRACT(DAY FROM s.start_date)
                THEN 1
                ELSE 0
            END
        )::int,
        1
    ) AS subscription_duration_months,
    COUNT(*) FILTER (WHERE t.status = 'Success')
    - COUNT(*) FILTER (WHERE t.status = 'Refunded') AS total_successful_payments
FROM dwh.fct_subscriptions s
JOIN dwh.dim_customers c
    ON s.customer_id = c.customer_id
LEFT JOIN dwh.fct_transactions t
    ON s.sub_id = t.sub_id
GROUP BY
    s.sub_id,
    c.company_name,
    c.country,
    s.start_date,
    s.end_date;
```
# Task 2: Advanced Analytical SQL
## Performance 

## Monthly Recurring Revenue
To calculate MRR, I distribute each subscription amount evenly across its billing lifetime in months.
Duration of subscription we can get from `dm_sales_performance`.
The monthly contribution of a single subscription is calculated as:
```
monthly_contribution = amount / subscription_duration_months
```
The query generates one row per active subscription month using generate_series, and the monthly revenue is aggregated across subscriptions.
```sql
DROP MATERIALIZED VIEW IF EXISTS dm.dm_mrr;

CREATE MATERIALIZED VIEW dm.dm_mrr AS
SELECT
    (
        DATE_TRUNC('month', s.start_date)
        + gs.month_offset * INTERVAL '1 month'
    )::date AS revenue_month,
    SUM(
        s.amount / d.subscription_duration_months
    ) AS mrr
FROM dwh.fct_subscriptions s
JOIN dm.dm_sales_performance d
    ON s.sub_id = d.sub_id
CROSS JOIN LATERAL generate_series(
    0,
    d.subscription_duration_months - 1
) AS gs(month_offset)
GROUP BY revenue_month
ORDER BY revenue_month;
```
### Optimization
```sql
CREATE INDEX IF NOT EXISTS idx_dm_mrr_revenue_month
    ON dm.dm_mrr (revenue_month);
```
## Lifetime Value
First, each subscription is expanded into multiple rows — one for each month it is active — using generate_series. This allows representing subscription revenue on a monthly basis.
Then, the monthly revenue is aggregated at the customer level by summing contributions from all their subscriptions for each month.
Finally, cumulative LTV is calculated using a window function that computes a running total of revenue for each customer over time, ordered by month. 
```sql
DROP MATERIALIZED VIEW IF EXISTS dm.dm_cumulative_ltv;

CREATE MATERIALIZED VIEW dm.dm_cumulative_ltv AS
WITH base AS (
    SELECT
        c.customer_id,
        c.company_name,
        (
            DATE_TRUNC('month', s.start_date)
            + gs.month_offset * INTERVAL '1 month'
        )::date AS revenue_month,
        s.amount / d.subscription_duration_months AS monthly_revenue
    FROM dwh.fct_subscriptions s
    JOIN dwh.dim_customers c
        ON s.customer_id = c.customer_id
    JOIN dm.dm_sales_performance d
        ON s.sub_id = d.sub_id
    CROSS JOIN LATERAL generate_series(
        0,
        d.subscription_duration_months - 1
    ) AS gs(month_offset)
)
SELECT
    customer_id,
    company_name,
    revenue_month,
    SUM(monthly_revenue) AS monthly_revenue,
    SUM(SUM(monthly_revenue)) OVER (
        PARTITION BY customer_id
        ORDER BY revenue_month
    ) AS cumulative_ltv
FROM base
GROUP BY
    customer_id,
    company_name,
    revenue_month
ORDER BY
    customer_id,
    revenue_month;
```
### Optimization
```sql
	CREATE INDEX IF NOT EXISTS idx_dm_cumulative_ltv_customer_month
    ON dm.dm_cumulative_ltv (customer_id, revenue_month);
```