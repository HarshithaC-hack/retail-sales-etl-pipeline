# Sample PySpark Silver Script
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Silver").getOrCreate()

# Replace with real logic
df = spark.range(100).withColumnRenamed("id", "sample_id")
df.show()
