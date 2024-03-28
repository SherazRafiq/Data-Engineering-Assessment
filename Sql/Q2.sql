CREATE TABLE Sales (
    sale_id VARCHAR(5),
    product_id VARCHAR(1),
    sale_date DATE,
    amount DECIMAL(10, 2),
    promotion_id VARCHAR(5)
);

INSERT INTO Sales (sale_id, product_id, sale_date, amount, promotion_id)
VALUES 
    ('S001', 'A', '2024-01-01', 45.00, 'P001'),
    ('S002', 'B', '2024-01-02', 25.00, 'P002'),
    ('S003', 'A', '2024-01-03', 50.00, NULL),
    ('S004', 'C', '2024-01-04', 18.00, 'P001'),
    ('S005', 'B', '2024-01-05', 30.00, NULL)
    ('S005', 'B', '2024-01-03', 30.00, 'P002'),
    ('S005', 'B', '2024-01-08', 50.00, 'P002'),
	('S005', 'B', '2024-01-02', 30.00, 'P002');   

CREATE TABLE Promotions (
    promotion_id VARCHAR(5),
    start_date DATE,
    end_date DATE,
    discount_rate VARCHAR(5)
);


INSERT INTO Promotions (promotion_id, start_date, end_date, discount_rate)
VALUES 
    ('P001', '2024-01-01', '2024-01-07', '10%'),
    ('P002', '2024-01-02', '2024-01-08', '15%');


SELECT 
    promotion_start_date,
    promotion_end_date,
    first_day_percentage,
    last_day_percentage
FROM (
    SELECT
        p.start_date AS promotion_start_date,
        p.end_date AS promotion_end_date,
        SUM(CASE WHEN s.sale_date = p.end_date THEN s.amount ELSE 0 END) / SUM(s.amount) * 100 AS last_day_percentage
    FROM Sales s
    LEFT JOIN Promotions p ON s.promotion_id = p.promotion_id
    GROUP BY p.start_date, p.end_date
) subquery;
