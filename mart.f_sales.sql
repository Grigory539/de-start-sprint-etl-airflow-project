DELETE FROM mart.f_sales 
WHERE date_id = (
    SELECT date_id FROM mart.d_calendar WHERE fact_date = '{{ date }}'::date
);

INSERT INTO mart.f_sales (
    date_id, item_id, customer_id, city_id, 
    quantity, payment_amount, order_status
)
SELECT 
    dc.date_id,
    uol.item_id,
    uol.customer_id,
    uol.city_id,
    CASE 
        WHEN uol.status = 'refunded' THEN -1 * SUM(uol.quantity)
        ELSE SUM(uol.quantity)
    END AS quantity,
    CASE 
        WHEN uol.status = 'refunded' THEN -1 * SUM(uol.payment_amount)
        ELSE SUM(uol.payment_amount)
    END AS payment_amount,
    COALESCE(uol.status, 'shipped') AS order_status
FROM staging.user_order_log uol
JOIN mart.d_calendar dc ON DATE(uol.date_time) = dc.fact_date
WHERE DATE(uol.date_time) = '{{ date }}'::date
GROUP BY dc.date_id, uol.item_id, uol.customer_id, uol.city_id, uol.status;
