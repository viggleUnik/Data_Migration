-- A query to return data from joins of: CUSTOMERS,ORDERS, ORDER_ITEMS, PRODUCTS and PRODUCT_CATEGORIES
SELECT
c."name" ,
p.product_name as product_bought,
o.order_date
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
WHERE o.order_date =:target_order_date