# Sample PySpark Silver Script
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Silver").getOrCreate()

# Replace with real logic
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
import os

spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

# STEP 1: Read raw sales data
df = spark.read.option("header", True).csv("../data/raw_sales.csv")

# STEP 2: Clean & transform (Silver layer)
df_clean = (
    df.dropna()
      .withColumn("quantity", col("quantity").cast("int"))
      .withColumn("price", col("price").cast("double"))
      .withColumn("total_amount", col("quantity") * col("price"))
      .withColumn("date", to_date(col("date"), "dd-MM-yyyy"))
)

# STEP 3: Save to Silver layer
silver_output = os.path.abspath("../data/silver_sales_output_csv")
df_clean.write.mode("overwrite").option("header", True).csv(silver_output)

print(f"✅ Bronze → Silver ETL complete! Output written to: {silver_output}")

spark.stop()
