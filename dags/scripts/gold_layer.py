# Sample PySpark Gold Script
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Gold").getOrCreate()

# Replace with real logic
df = spark.range(100).withColumnRenamed("id", "sample_id")
df.show()
