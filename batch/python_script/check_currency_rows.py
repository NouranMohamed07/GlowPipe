import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, count as spark_count


AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")

spark = (
    SparkSession.builder
    .appName("Check-Currency-Rows")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .config("spark.sql.debug.maxToStringFields", "300")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

BUCKET = "skincare-recommendation-system-data"
PRODUCTS_PATH = f"s3a://{BUCKET}/bronze/NewData/ProductTable_price_size_fixed/"

print("\nReading products table...")

products = spark.read.csv(
    PRODUCTS_PATH,
    header=True,
    inferSchema=False,
    multiLine=True,
    escape='"',
    quote='"',
)

print(f"Total products rows: {products.count():,}")

print("\nCurrency distribution:")

products.groupBy(
    trim(col("currency")).alias("currency")
).agg(
    spark_count("*").alias("rows")
).orderBy(
    col("rows").desc()
).show(100, truncate=False)


non_usd_rows = products.filter(
    col("currency").isNotNull()
    & (trim(col("currency")) != "")
    & (trim(col("currency")) != "USD")
)

print(f"\nNon-USD rows count: {non_usd_rows.count():,}")

print("\nDistinct non-USD currencies:")

non_usd_rows.groupBy(
    trim(col("currency")).alias("currency")
).agg(
    spark_count("*").alias("rows")
).orderBy(
    col("rows").desc()
).show(100, truncate=False)


print("\nNon-USD products: product_name + currency only")

non_usd_rows.select(
    col("product_name"),
    trim(col("currency")).alias("currency")
).show(100, truncate=60)


spark.stop()