from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("Bronze Layer").getOrCreate()
    print("SparkSession created!")
    # Add your ETL logic here
    spark.stop()

if __name__ == "__main__":
    main()