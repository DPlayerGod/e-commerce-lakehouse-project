CREATE DATABASE IF NOT EXISTS analytics;

-- Sales Overview table for BI
-- Using ReplacingMergeTree(_synced_at) for idempotency
CREATE TABLE IF NOT EXISTS analytics.mart_sales_overview (
    order_id        String,
    order_date      Date,
    customer_id     String,
    country         String,
    product_name    String,
    category        String,
    quantity        Int32,
    amount_usd      Float64,
    payment_status  String,
    _synced_at      DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(_synced_at)
PARTITION BY toYYYYMM(order_date)
ORDER BY (order_date, order_id);

-- Customer LTV table for BI
-- Using ReplacingMergeTree(_synced_at) to keep latest stats
CREATE TABLE IF NOT EXISTS analytics.mart_customer_lifetime_value (
    customer_id      String,
    total_orders     Int64,
    total_spent_usd  Float64,
    first_order      Date,
    last_order       Date,
    _synced_at       DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(_synced_at)
ORDER BY customer_id;

