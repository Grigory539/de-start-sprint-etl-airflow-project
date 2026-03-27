DELETE FROM mart.f_customer_retention 
WHERE period_id = EXTRACT(WEEK FROM '{{ date }}'::date);

WITH weekly_orders AS (
    SELECT 
        customer_id, item_id,
        EXTRACT(WEEK FROM DATE(date_time))::INT AS week_num,
        COUNT(*) AS order_count,
        SUM(CASE WHEN status = 'refunded' THEN 1 ELSE 0 END) AS refunded_count,
        SUM(CASE WHEN status = 'refunded' THEN -payment_amount ELSE payment_amount END) AS revenue
    FROM staging.user_order_log
    WHERE DATE(date_time) BETWEEN '{{ date }}'::date - INTERVAL '6 days' AND '{{ date }}'::date
    GROUP BY customer_id, item_id, EXTRACT(WEEK FROM DATE(date_time))
),
classification AS (
    SELECT 
        item_id, week_num AS period_id,
        COUNT(CASE WHEN order_count = 1 AND refunded_count = 0 THEN 1 END) AS new_customers_count,
        COUNT(CASE WHEN order_count > 1 THEN 1 END) AS returning_customers_count,
        COUNT(CASE WHEN refunded_count > 0 THEN 1 END) AS refunded_customer_count,
        SUM(CASE WHEN order_count = 1 AND refunded_count = 0 THEN revenue ELSE 0 END) AS new_customers_revenue,
        SUM(CASE WHEN order_count > 1 THEN revenue ELSE 0 END) AS returning_customers_revenue,
        SUM(CASE WHEN refunded_count > 0 THEN ABS(revenue) ELSE 0 END) AS customers_refunded
    FROM weekly_orders
    GROUP BY item_id, week_num
)
INSERT INTO mart.f_customer_retention (
    new_customers_count, returning_customers_count, refunded_customer_count,
    period_name, period_id, item_id,
    new_customers_revenue, returning_customers_revenue, customers_refunded
)
SELECT 
    COALESCE(new_customers_count,0), COALESCE(returning_customers_count,0), COALESCE(refunded_customer_count,0),
    'weekly', period_id, item_id,
    COALESCE(new_customers_revenue,0), COALESCE(returning_customers_revenue,0), COALESCE(customers_refunded,0)
FROM classification;