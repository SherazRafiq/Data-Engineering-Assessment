# Total Sales Revenue for January 2024: Sum of sales in January 2024.

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum


spark = SparkSession.builder \
    .appName("TotalSalesRevenue") \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

sales_data = [
    ('S001', 'A', '2024-01-01', 45.00, 'P001'),
    ('S002', 'B', '2024-01-02', 25.00, 'P002'),
    ('S003', 'A', '2024-01-03', 50.00, None),
    ('S004', 'C', '2024-01-04', 18.00, 'P001'),
    ('S005', 'B', '2024-01-05', 30.00, None)
]


sales_df = spark.createDataFrame(sales_data, ['sale_id', 'product_id', 'sale_date', 'amount', 'promotion_id'])

sales_df.createOrReplaceTempView("sales")

sql_query = """
    SELECT COALESCE(SUM(amount), 0) AS total_sales_revenue
    FROM sales
    WHERE sale_date >= '2024-01-01' AND sale_date <= '2024-01-31'
"""

total_sales_revenue_result = spark.sql(sql_query)


total_sales_revenue = total_sales_revenue_result.first()[0]

print("Total Sales Revenue for January 2024:", total_sales_revenue)

spark.stop()
