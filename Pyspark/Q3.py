# Average Sale Amount Per Category for January 2024:Average sale amount per category in January 2024.

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

spark = SparkSession.builder \
    .appName("AverageSaleAmountPerCategory") \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

sales_data = [
    ('1', 'P001', '2024-01-01', 100.00, 'C1'),
    ('2', 'P002', '2024-01-05', 150.00, 'C2'),
    ('3', 'P001', '2024-01-10', 100.00, 'C1'),
    ('4', 'P003', '2024-01-15', 200.00, 'C3'),
    ('5', 'P002', '2024-01-20', 150.00, 'C2')
]

products_data = [
    ('P001', 'Product A', 'C1'),
    ('P002', 'Product B', 'C2'),
    ('P003', 'Product C', 'C3')
]

categories_data = [
    ('C1', 'Electronics'),
    ('C2', 'Clothing'),
    ('C3', 'Home Appliances')
]

sales_df = spark.createDataFrame(sales_data, ['sale_id', 'product_id', 'sale_date', 'amount', 'category_id'])
products_df = spark.createDataFrame(products_data, ['product_id', 'product_name', 'category_id'])
categories_df = spark.createDataFrame(categories_data, ['category_id', 'category_name'])

sales_df.createOrReplaceTempView("sales")
products_df.createOrReplaceTempView("products")
categories_df.createOrReplaceTempView("categories")

sql_query = """
    SELECT c.category_name, AVG(s.amount) AS avg_sale_amount
    FROM sales s
    JOIN products p ON s.product_id = p.product_id
    JOIN categories c ON p.category_id = c.category_id
    WHERE s.sale_date >= '2024-01-01' AND s.sale_date <= '2024-01-31'
    GROUP BY c.category_name
"""

avg_sale_amount_per_category_result = spark.sql(sql_query)

avg_sale_amount_per_category_result.show()

spark.stop()
