# Top Selling Product Category in January 2024: Product category with highest sales in January 2024.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("TopSellingProductCategory") \
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

sales_df = spark.createDataFrame(sales_data, ['sale_id', 'product_id', 'sale_date', 'amount', 'sale_category_id'])
products_df = spark.createDataFrame(products_data, ['product_id', 'product_name', 'product_category_id'])
categories_df = spark.createDataFrame(categories_data, ['category_id', 'category_name'])

filtered_sales_df = sales_df.filter((col("sale_date") >= "2024-01-01") & (col("sale_date") <= "2024-01-31"))

joined_df = filtered_sales_df.join(products_df, filtered_sales_df.product_id == products_df.product_id)\
    .join(categories_df, products_df.product_category_id == categories_df.category_id)

result_df = joined_df.groupBy("category_id", "category_name")\
    .agg({"amount": "sum"})\
    .withColumnRenamed("sum(amount)", "total_sales")\
    .orderBy(col("total_sales").desc())\
    .limit(1)

result_df.show()

spark.stop()
