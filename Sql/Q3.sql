CREATE TABLE CustomerPurchases (
    customer_id VARCHAR(5),
    product_id VARCHAR(1),
    purchase_date DATE,
    price DECIMAL(10, 2),
    payment_status VARCHAR(10)
);


INSERT INTO CustomerPurchases (customer_id, product_id, purchase_date, price, payment_status)
VALUES 
    ('C001', 'A', '2024-01-01', 50.00, 'Paid'),
    ('C001', 'B', '2024-01-05', 30.00, 'Paid'),
    ('C002', 'A', '2024-01-10', 50.00, 'Paid'),
    ('C003', 'C', '2024-01-15', 20.00, 'Paid'),
    ('C002', 'B', '2024-01-20', 30.00, 'Unpaid'),
    ('C004', 'A', '2024-01-25', 50.00, 'Paid'),
    ('C004', 'B', '2024-01-30', 30.00, 'Paid')
    ('C003', 'A', '2024-01-01', 50.00, 'Paid');


WITH customers_with_A_B AS (
    SELECT * 
    FROM CustomerPurchases cp 
    WHERE customer_id IN (
        SELECT customer_id 
        FROM CustomerPurchases 
        WHERE product_id = 'A'
    ) 
    AND cp.product_id = 'B'
),
total_customers AS (
    SELECT COUNT(customer_id) AS total_customers
    FROM customers_with_A_B
),
paid_customers AS (
    SELECT COUNT(customer_id) AS paid_customers
    FROM customers_with_A_B 
    WHERE customer_id IN (
        SELECT customer_id 
        FROM CustomerPurchases 
        WHERE product_id = 'A' AND payment_status = 'Paid'
    )
    AND customer_id IN (
        SELECT customer_id 
        FROM CustomerPurchases 
        WHERE product_id = 'B' AND payment_status = 'Paid'
    )
)
SELECT 
    round(paid_customers::numeric / total_customers::numeric * 100,2) AS percentage_paid
FROM 
    total_customers, paid_customers;

