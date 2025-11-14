SELECT
    c.name AS customer_name,
    c.email AS customer_email,
    o.order_id,
    o.order_date,
    p.product_name,
    oi.quantity,
    oi.item_price,
    oi.quantity * oi.item_price AS total_item_value,
    pay.payment_method,
    pay.payment_status
FROM customers AS c
JOIN orders AS o
    ON o.customer_id = c.customer_id
JOIN order_items AS oi
    ON oi.order_id = o.order_id
JOIN products AS p
    ON p.product_id = oi.product_id
LEFT JOIN payments AS pay
    ON pay.order_id = o.order_id;
