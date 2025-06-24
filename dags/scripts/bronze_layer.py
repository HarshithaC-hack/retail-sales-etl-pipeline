# Sample PySpark Bronze Script
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Bronze").getOrCreate()

# Replace with real logic
df = spark.range(100).withColumnRenamed("id", "sample_id")
df.show()
