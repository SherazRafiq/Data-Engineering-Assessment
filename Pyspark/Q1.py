# Monthly Active Users (MAU) for January 2024: Count of unique users active in January 2024.

import logging

# # Set the log level to ERROR for Py4j and PySpark
# logging.getLogger("py4j").setLevel(logging.ERROR)
# logging.getLogger("pyspark").setLevel(logging.ERROR)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("MonthlyActiveUsers") \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

user_activity_data = [
    (1, 101, '2024-01-05'),
    (2, 102, '2024-01-06'),
    (3, 103, '2024-01-07'),
    (4, 101, '2024-01-15'),
    (5, 104, '2024-01-20'),
    (6, 102, '2024-01-25'),
    (7, 105, '2024-01-30')
]

users_data = [
    (101, 'Alice', '2023-05-10'),
    (102, 'Bob', '2023-06-15'),
    (103, 'Charlie', '2023-07-20'),
    (104, 'Dana', '2023-08-25'),
    (105, 'Emily', '2023-09-30')
]


user_activity_df = spark.createDataFrame(user_activity_data, ['activity_id', 'user_id', 'activity_date'])
users_df = spark.createDataFrame(users_data, ['user_id', 'user_name', 'join_date'])

january_activity_df = user_activity_df.filter((col("activity_date") >= "2024-01-01") & (col("activity_date") <= "2024-01-31"))

joined_df = january_activity_df.join(users_df, "user_id")


mau_count = joined_df.select("user_id").distinct().count()

print("Monthly Active Users (MAU) for January 2024:", mau_count)

spark.stop()


