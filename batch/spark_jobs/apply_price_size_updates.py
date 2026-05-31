import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, lit, lower
from pyspark.sql.types import StringType, FloatType


AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")

spark = SparkSession.builder \
    .appName("GlowPipe-Apply-Price-Size-Updates") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.debug.maxToStringFields", "200") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    ) \
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


BUCKET = "skincare-recommendation-system-data"

BRONZE_ROOT = f"s3a://{BUCKET}/bronze"
BRONZE_NEWDATA = f"s3a://{BUCKET}/bronze/NewData"

PRODUCTS_PATH = f"{BRONZE_NEWDATA}/ProductTable.csv"
UPDATES_PATH = f"{BRONZE_ROOT}/price_scraper.csv"

OUTPUT_PATH = f"{BRONZE_NEWDATA}/ProductTable_price_size_fixed"


print("Reading product table...")
products = spark.read.csv(
    PRODUCTS_PATH,
    header=True,
    inferSchema=False,
    multiLine=True,
    escape='"',
    quote='"'
)

print("Reading price scraper updates...")
updates = spark.read.csv(
    UPDATES_PATH,
    header=True,
    inferSchema=False,
    multiLine=True,
    escape='"',
    quote='"'
)

products_count = products.count()
updates_count = updates.count()

print(f"Products rows: {products_count}")
print(f"Price updates rows: {updates_count}")


# clean product_id on both sides
products = products.withColumn(
    "product_id",
    lower(trim(col("product_id").cast(StringType())))
)

updates = updates.withColumn(
    "product_id",
    lower(trim(col("product_id").cast(StringType())))
)


# trim important update columns
for c in updates.columns:
    updates = updates.withColumn(c, trim(col(c)))

if "price" in products.columns:
    products = products.withColumn("price", trim(col("price")))

if "size" in products.columns:
    products = products.withColumn("size", trim(col("size")))


# prepare update table
updates = updates.withColumn(
    "updated_price",
    col("price_usd").cast(FloatType())
)

updates = updates.withColumn(
    "updated_size",
    col("size_label")
)

valid_updates = updates.filter(
    col("product_id").isNotNull() &
    col("updated_price").isNotNull()
).select(
    "product_id",
    "updated_price",
    "updated_size"
).dropDuplicates(["product_id"])

valid_updates_count = valid_updates.count()

print(f"Valid updates rows: {valid_updates_count}")


# check matching ids before applying update
matched_ids = products.join(
    valid_updates.select("product_id"),
    on="product_id",
    how="inner"
)

matched_ids_count = matched_ids.count()

print(f"Matched product_ids: {matched_ids_count}")

print("Sample product ids from products:")
products.select("product_id").show(5, truncate=False)

print("Sample product ids from updates:")
valid_updates.select("product_id").show(5, truncate=False)


# apply update
products_updated = products.join(
    valid_updates,
    on="product_id",
    how="left"
)

products_updated = products_updated.withColumn(
    "price",
    when(
        col("updated_price").isNotNull(),
        col("updated_price")
    ).otherwise(col("price").cast(FloatType()))
)

if "size" in products_updated.columns:
    products_updated = products_updated.withColumn(
        "size",
        when(
            col("updated_size").isNotNull() &
            (col("updated_size") != ""),
            col("updated_size")
        ).otherwise(col("size"))
    )
else:
    products_updated = products_updated.withColumn(
        "size",
        when(
            col("updated_size").isNotNull() &
            (col("updated_size") != ""),
            col("updated_size")
        ).otherwise(lit(None))
    )


updated_count = products_updated.filter(
    col("updated_price").isNotNull()
).count()

still_invalid_price_count = products_updated.filter(
    col("price").isNull() |
    (col("price") <= 0.01) |
    (col("price") > 10000)
).count()

products_updated = products_updated.drop(
    "updated_price",
    "updated_size"
)


print("UPDATE SUMMARY")
print(f"Original products rows: {products_count}")
print(f"Scraper update rows: {updates_count}")
print(f"Valid update rows: {valid_updates_count}")
print(f"Matched product ids: {matched_ids_count}")
print(f"Products updated by product_id: {updated_count}")
print(f"Products still with invalid price: {still_invalid_price_count}")


print("Sample updated products:")
products_updated.select(
    "product_id",
    "product_name",
    "brand_name",
    "price",
    "size"
).show(20, truncate=40)


print("Writing new product table to Bronze/NewData...")
products_updated.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(OUTPUT_PATH)

print(f"Saved to: {OUTPUT_PATH}")

spark.stop()