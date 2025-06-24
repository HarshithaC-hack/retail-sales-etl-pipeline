# Sample PySpark Gold Script
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Gold").getOrCreate()

# Replace with real logic
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, count, to_date, month, year,
    format_number, rank
)
from pyspark.sql.window import Window
import os
import shutil

spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

# Input path
silver_path = os.path.abspath("../data/silver_sales_output_csv")
df = spark.read.option("header", True).option("inferSchema", True).csv(silver_path)

print("\n👀 Sample rows:")
df.show(5)
print(f"\n✅ Total rows read: {df.count()}")
print(f"🛒 Distinct stores: {df.select('store_id').distinct().count()}")
print(f"📦 Distinct products: {df.select('product_id').distinct().count()}")

# Output folder
gold_folder = os.path.abspath("../data/gold")
os.makedirs(gold_folder, exist_ok=True)

# Utility to save one single file per use case
def write_single_csv(df, filename, format_cols=None):
    if format_cols:
        for col_name in format_cols:
            df = df.withColumn(col_name, format_number(col(col_name), 2))
    temp_path = os.path.join(gold_folder, "temp")
    final_path = os.path.join(gold_folder, filename)
    df.repartition(1).write.mode("overwrite").option("header", True).csv(temp_path)

    for file in os.listdir(temp_path):
        if file.startswith("part") and file.endswith(".csv"):
            shutil.move(os.path.join(temp_path, file), final_path)
    shutil.rmtree(temp_path)

# --- GOLD LAYER OUTPUTS ---

# 1. Total sales per day
write_single_csv(
    df.groupBy("date").agg(_sum("total_amount").alias("total_sales")),
    "total_sales_per_day.csv", format_cols=["total_sales"]
)

# 2. Total sales per store
write_single_csv(
    df.groupBy("store_id").agg(_sum("total_amount").alias("total_sales")),
    "total_sales_per_store.csv", format_cols=["total_sales"]
)

# 3. Monthly sales per store
write_single_csv(
    df.withColumn("month", month("date")).withColumn("year", year("date"))
      .groupBy("store_id", "year", "month")
      .agg(_sum("total_amount").alias("monthly_sales")),
    "monthly_sales_per_store.csv", format_cols=["monthly_sales"]
)

# 4. Product-level sales summary
write_single_csv(
    df.groupBy("product_id")
      .agg(
        count("transaction_id").alias("transactions"),
        _sum("quantity").alias("units_sold"),
        _sum("total_amount").alias("total_sales")
      ),
    "product_sales_summary.csv", format_cols=["units_sold", "total_sales"]
)

# 5. Daily quantity per store
write_single_csv(
    df.groupBy("date", "store_id").agg(_sum("quantity").alias("total_quantity")),
    "daily_quantity_per_store.csv", format_cols=["total_quantity"]
)

# 6. Average price per product
write_single_csv(
    df.groupBy("product_id")
      .agg((_sum("price") / count("price")).alias("avg_price")),
    "average_price_per_product.csv", format_cols=["avg_price"]
)

# 7. Top-selling product per store
sales_per_product_store = df.groupBy("store_id", "product_id") \
    .agg(_sum("total_amount").alias("sales"))
windowSpec = Window.partitionBy("store_id").orderBy(col("sales").desc())
top_products = sales_per_product_store \
    .withColumn("rank", rank().over(windowSpec)) \
    .filter(col("rank") == 1).drop("rank")
write_single_csv(top_products, "top_selling_product_per_store.csv", format_cols=["sales"])

# 8. Daily revenue trend
write_single_csv(
    df.groupBy("date").agg(_sum("total_amount").alias("daily_revenue")),
    "daily_revenue_trend.csv", format_cols=["daily_revenue"]
)

# 9. Store-product revenue breakdown
write_single_csv(
    df.groupBy("store_id", "product_id").agg(_sum("total_amount").alias("revenue")),
    "store_product_revenue.csv", format_cols=["revenue"]
)

# 10. Store-wise average transaction value
write_single_csv(
    df.groupBy("store_id")
      .agg((_sum("total_amount") / count("transaction_id")).alias("avg_transaction_value")),
    "avg_transaction_value_per_store.csv", format_cols=["avg_transaction_value"]
)

print(f"\n✅ All 10 Gold outputs written to: {gold_folder}")
spark.stop()

