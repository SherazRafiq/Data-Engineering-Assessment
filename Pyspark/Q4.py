# Number of New Users in January 2024: Count of users who joined in January 2024.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("NumberOfNewUsers") \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

users_data = [
    ('101', 'Alice', '2023-05-10'),
    ('102', 'Bob', '2023-06-15'),
    ('103', 'Charlie', '2023-07-20'),
    ('104', 'Dana', '2023-08-25'),
    ('105', 'Emily', '2023-09-30')
]

users_df = spark.createDataFrame(users_data, ['user_id', 'user_name', 'join_date'])

filtered_df = users_df.filter((col("join_date") >= "2024-01-01") & (col("join_date") <= "2024-01-31"))

new_users_count_result = filtered_df.count()

print("Number of new users in January 2024:", new_users_count_result)

spark.stop()
