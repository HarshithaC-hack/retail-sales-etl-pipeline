# Sample PySpark Bronze Script
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Bronze").getOrCreate()

# Replace with real logic
import csv
import random
from datetime import datetime, timedelta

output_file = "data/raw_sales.csv"
products = ['P001', 'P002', 'P003', 'P004', 'P005']
stores = ['S001', 'S002', 'S003', 'S004']
start_date = datetime(2024, 1, 1)
num_rows = 5_000_000

# Base prices for each (product_id, store_id) combo
price_map = {
    'P001': {'S001': 300, 'S002': 310, 'S003': 295, 'S004': 305},
    'P002': {'S001': 500, 'S002': 490, 'S003': 510, 'S004': 505},
    'P003': {'S001': 700, 'S002': 690, 'S003': 710, 'S004': 720},
    'P004': {'S001': 400, 'S002': 390, 'S003': 410, 'S004': 405},
    'P005': {'S001': 200, 'S002': 210, 'S003': 190, 'S004': 205},
}



with open(output_file, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['transaction_id', 'date', 'product_id', 'store_id', 'quantity', 'price'])

    for i in range(1, num_rows + 1):
        transaction_id = f"T{i:07d}"
        date = (start_date + timedelta(days=random.randint(0, 180))).strftime("%d-%m-%Y")
        product_id = random.choice(products)
        store_id = random.choice(stores)
        quantity = random.randint(1, 10)

        # Get base price for product-store and add slight random noise
        base_price = price_map[product_id][store_id]
        price = round(random.normalvariate(base_price, 10))  # ±10 noise

        writer.writerow([transaction_id, date, product_id, store_id, quantity, price])

print(f"✅ Done generating {num_rows:,} rows to {output_file}")

