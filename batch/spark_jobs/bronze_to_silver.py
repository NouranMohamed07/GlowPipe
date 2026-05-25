import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, lit, broadcast
from pyspark.sql.types import StringType, IntegerType

# ── Spark Session ─────────────────────────────────────────────────────────────
AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")

spark = SparkSession.builder \
    .appName("GlowPipe-Bronze-To-Silver") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark ready")

# ── Paths ─────────────────────────────────────────────────────────────────────
BUCKET = "skincare-recommendation-system-data"
BRONZE = f"s3a://{BUCKET}/bronze/NewData"
SILVER = f"s3a://{BUCKET}/silver"

# ── Read Bronze Tables ────────────────────────────────────────────────────────
print("Reading bronze tables...")
products    = spark.read.csv(f"{BRONZE}/ProductTable.csv",     header=True, inferSchema=False)
ingredients = spark.read.csv(f"{BRONZE}/IngredientsTable.csv", header=True, inferSchema=False)
bridge      = spark.read.csv(f"{BRONZE}/BridgeTable.csv",      header=True, inferSchema=False)

print(f"Products:    {products.count()}")
print(f"Ingredients: {ingredients.count()}")
print(f"Bridge:      {bridge.count()}")

# ── Cast IDs as STRING  ─────────────────────────────
products    = products.withColumn("product_id",    col("product_id").cast(StringType()))
ingredients = ingredients.withColumn("ingredient_id", col("ingredient_id").cast(StringType()))
bridge      = bridge.withColumn("product_id",     col("product_id").cast(StringType())) \
                    .withColumn("ingredient_id",  col("ingredient_id").cast(StringType()))
print("IDs cast as STRING")

# ── Clean Products ────────────────────────────────────────────────────────────
print("Cleaning products...")

# Drop useless column
products = products.drop("_norm")

# Fix currency — only keep valid currency codes
products = products.withColumn(
    "currency",
    when(col("currency").isin(["USD", "GBP", "EUR"]), col("currency"))
    .otherwise(lit("USD"))
)

# Fix source — only keep valid source names
valid_sources = ["datasheet", "skincarisma", "dermstore", "cosmetics", "skincare_products"]
products = products.withColumn(
    "source",
    when(col("source").isin(valid_sources), col("source"))
    .otherwise(lit("unknown"))
)

# Fix price — set 0.0 and 0.01 to null (clearly wrong values)
products = products.withColumn(
    "price",
    when(col("price").cast("float") <= 0.01, lit(None))
    .otherwise(col("price"))
)

# Fix rating — set 0 to null (0 means no rating not bad rating)
products = products.withColumn(
    "rating",
    when(col("rating").cast("float") <= 0, lit(None))
    .otherwise(col("rating"))
)

# Fill brand_name nulls
products = products.withColumn(
    "brand_name",
    when(col("brand_name").isNull(), lit("Unknown")).otherwise(col("brand_name"))
)

# Cast float .0 columns to integer
int_cols = [
    "ingredients_count", "number_of_reviews",
    "oily_skin_score", "dry_skin_score", "sensitive_skin_score",
    "combination_skin_score", "normal_skin_score", "acne_prone_score",
    "pregnancy_safe", "fungal_acne_safe", "comedogenic_rating",
    "vegan", "cruelty_free", "reef_safe",
    "fragrance_free", "alcohol_free", "paraben_free",
    "sulfate_free", "silicone_free", "oil_free"
]
for c in int_cols:
    products = products.withColumn(c, col(c).cast(IntegerType()))

print(f"Products cleaned: {products.count()} rows")

# ── Clean Ingredients ─────────────────────────────────────────────────────────
print("Cleaning ingredients...")

# Trim ingredient_name + fix evidence_level type
ingredients = ingredients \
    .withColumn("ingredient_name", trim(col("ingredient_name"))) \
    .withColumn("evidence_level",  col("evidence_level").cast(StringType()))

# Fix source
valid_ing_sources = ["skincarisma", "extracted_from_products"]
ingredients = ingredients.withColumn(
    "source",
    when(col("source").isin(valid_ing_sources), col("source"))
    .otherwise(lit("unknown"))
)

# Remove garbage ingredient and its bridge rows
bridge      = bridge.filter(col("ingredient_name") != "**†")
ingredients = ingredients.filter(col("ingredient_name") != "**†")

# Trim ingredient_name in bridge to stay consistent with ingredients table
bridge = bridge.withColumn("ingredient_name", trim(col("ingredient_name")))

print(f"Ingredients cleaned: {ingredients.count()} rows")
print(f"Bridge after garbage removal: {bridge.count()} rows")

# ── Clean Bridge ──────────────────────────────────────────────────────────────
print("Cleaning bridge...")

# Drop 98%+ null columns — useless
bridge = bridge.drop("ingredient_concentration", "evidence_level")

# Cast IDs as string
bridge = bridge \
    .withColumn("product_id",    col("product_id").cast(StringType())) \
    .withColumn("ingredient_id", col("ingredient_id").cast(StringType()))

# Filter orphan rows — maintain FK integrity
valid_product_ids    = products.select("product_id")
valid_ingredient_ids = ingredients.select("ingredient_id")

bridge = bridge.join(broadcast(valid_product_ids),    on="product_id",    how="inner")
bridge = bridge.join(broadcast(valid_ingredient_ids), on="ingredient_id", how="inner")

print(f"Bridge cleaned: {bridge.count()} rows")




# ── Write to Silver ───────────────────────────────────────────────────────────
print("Writing to silver...")

products.write.mode("overwrite").parquet(f"{SILVER}/products/")
print("Products written")

ingredients.write.mode("overwrite").parquet(f"{SILVER}/ingredients/")
print("Ingredients written")

bridge.write.mode("overwrite").parquet(f"{SILVER}/product_ingredients/")
print("Bridge written")

print("\n Silver layer complete!")
spark.stop()
