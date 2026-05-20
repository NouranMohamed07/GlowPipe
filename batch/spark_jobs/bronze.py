from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, col, split, explode, trim
import os

# ── Config ───────────────────────────────────────────────────────────────────
BUCKET = "skincare-recommendation-system-data"
RAW    = f"s3a://{BUCKET}/raw"
BRONZE = f"s3a://{BUCKET}/bronze"

# ── Spark Session ─────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("GlowPipe-Bronze") \
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ── Helper ────────────────────────────────────────────────────────────────────
def add_metadata(df, source_name):
    return df \
        .withColumn("_ingested_at", current_timestamp()) \
        .withColumn("_source", lit(source_name))

# ── 1. Products ───────────────────────────────────────────────────────────────
cosmetics = spark.read.csv(f"{RAW}/cosmetics.csv",
                           header=True, inferSchema=True)
skincare  = spark.read.csv(f"{RAW}/skincare_products.csv",
                           header=True, inferSchema=True)
datasheet = spark.read.csv(f"{RAW}/datasheet.csv",
                           header=True, inferSchema=True)
dermstore = spark.read.csv(f"{RAW}/products.csv",
                           header=True, inferSchema=True)

cosmetics = add_metadata(cosmetics, "kaggle_cosmetics")
skincare  = add_metadata(skincare,  "lookfantastic_scrape")
datasheet = add_metadata(datasheet, "datasheet")
dermstore = add_metadata(dermstore, "dermstore_scrape")

cosmetics.write.mode("overwrite").parquet(
    f"{BRONZE}/entity=products/source=kaggle_cosmetics/")
skincare.write.mode("overwrite").parquet(
    f"{BRONZE}/entity=products/source=lookfantastic/")
datasheet.write.mode("overwrite").parquet(
    f"{BRONZE}/entity=products/source=datasheet/")
dermstore.write.mode("overwrite").parquet(
    f"{BRONZE}/entity=products/source=dermstore/")

print("entity=products/ done")

# ── 2. Ingredients ────────────────────────────────────────────────────────────
ingredients = spark.read.csv(f"{RAW}/ingredients.csv",
                             header=True, inferSchema=True)
ingredients = add_metadata(ingredients, "paulas_choice_scrape")
ingredients.write.mode("overwrite").parquet(
    f"{BRONZE}/entity=ingredients/")

print("entity=ingredients/ done")

# ── 3. Product-Ingredients bridge ─────────────────────────────────────────────
product_ingredients = cosmetics \
    .select("Name", "Brand", "Ingredients", "_ingested_at", "_source") \
    .withColumn("ingredient", explode(split(col("Ingredients"), ","))) \
    .withColumn("ingredient", trim(col("ingredient"))) \
    .drop("Ingredients")

product_ingredients.write.mode("overwrite").parquet(
    f"{BRONZE}/entity=product_ingredients/")

print("entity=product_ingredients/ done")

# ── 4. Categories ─────────────────────────────────────────────────────────────
categories = cosmetics \
    .select("Label").distinct() \
    .withColumnRenamed("Label", "category_name")
categories = add_metadata(categories, "kaggle_cosmetics")
categories.write.mode("overwrite").parquet(
    f"{BRONZE}/entity=categories/")

print("entity=categories/ done")

# ── 5. Brands ─────────────────────────────────────────────────────────────────
brands = cosmetics \
    .select("Brand").distinct() \
    .withColumnRenamed("Brand", "brand_name")
brands = add_metadata(brands, "kaggle_cosmetics")
brands.write.mode("overwrite").parquet(
    f"{BRONZE}/entity=brands/")

print("entity=brands/ done")

# ── 6. Reviews Summary ────────────────────────────────────────────────────────
reviews_summary = datasheet \
    .select("name", "brand", "afterUse", "_ingested_at", "_source") \
    .withColumnRenamed("afterUse", "skin_effects")
reviews_summary.write.mode("overwrite").parquet(
    f"{BRONZE}/entity=reviews_summary/")

print("entity=reviews_summary/ done")
print("Bronze layer complete")

spark.stop()