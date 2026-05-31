import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    when,
    lit,
    broadcast,
    regexp_replace,
    regexp_extract,
    initcap,
    lower,
)
from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType


AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")

spark = (
    SparkSession.builder.appName("GlowPipe-Bronze-To-Silver-NewDataV3")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .config("spark.sql.shuffle.partitions", "200")
    .config("spark.sql.debug.maxToStringFields", "200")
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
print("Spark ready")


BUCKET = "skincare-recommendation-system-data"

BRONZE = f"s3a://{BUCKET}/bronze/NewData"
SILVER_NEW = f"s3a://{BUCKET}/silver/NewDataV3"

PRODUCTS_PATH = f"{BRONZE}/ProductTable_price_size_fixed/"
INGREDIENTS_PATH = f"{BRONZE}/IngredientsTable.csv"
BRIDGE_NEW_PATH = f"{BRONZE}/BridgeNewTable/"


def clean_name_text(df, column_name):
    if column_name in df.columns:
        return df.withColumn(
            column_name,
            initcap(
                trim(
                    regexp_replace(
                        regexp_replace(col(column_name), r"[\n\r\t]", " "),
                        r"\s+",
                        " ",
                    )
                )
            ),
        )
    return df


def log_section(title):
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)


def audit_ok(msg):
    print(f"OK - {msg}")


def audit_fail(msg, errors):
    print(f"FAIL - {msg}")
    errors.append(msg)


def audit_warn(msg, warnings):
    print(f"WARN - {msg}")
    warnings.append(msg)


def audit_info(msg):
    print(f"INFO - {msg}")


def check_zero(value, ok_msg, fail_msg, errors):
    if value == 0:
        audit_ok(ok_msg)
    else:
        audit_fail(fail_msg.format(value), errors)


log_section("STEP 1 - READ BRONZE TABLES")

products = spark.read.csv(
    PRODUCTS_PATH,
    header=True,
    inferSchema=False,
    multiLine=True,
    escape='"',
    quote='"',
)

ingredients = spark.read.csv(
    INGREDIENTS_PATH,
    header=True,
    inferSchema=False,
    multiLine=True,
    escape='"',
    quote='"',
)

bridge_new = spark.read.csv(
    BRIDGE_NEW_PATH,
    header=True,
    inferSchema=False,
    multiLine=True,
    escape='"',
    quote='"',
)

audit_info(f"Products rows: {products.count():,}")
audit_info(f"Ingredients rows: {ingredients.count():,}")
audit_info(f"Bridge new rows: {bridge_new.count():,}")


log_section("STEP 2 - CAST IDS")

products = products.withColumn(
    "product_id",
    trim(col("product_id").cast(StringType())),
)

ingredients = ingredients.withColumn(
    "ingredient_id",
    trim(col("ingredient_id").cast(StringType())),
)

bridge_new = (
    bridge_new.withColumn(
        "product_id",
        trim(col("product_id").cast(StringType())),
    )
    .withColumn(
        "ingredient_id",
        trim(col("ingredient_id").cast(StringType())),
    )
)


log_section("STEP 3 - CLEAN PRODUCTS")

products = clean_name_text(products, "product_name")

if "_norm" in products.columns:
    products = products.drop("_norm")
    audit_info("Dropped _norm column")

safe_product_text_cols = [
    "brand_name",
    "category",
    "sub_category",
    "source",
    "product_url",
    "image_url",
    "description",
    "size",
]

for c in safe_product_text_cols:
    if c in products.columns:
        products = products.withColumn(c, trim(col(c)))

products = products.withColumn("currency", lit("USD"))

products = products.withColumn(
    "price",
    col("price").cast(FloatType()),
)

if "size" not in products.columns:
    products = products.withColumn("size", lit(None).cast(StringType()))
else:
    products = products.withColumn(
        "size",
        when(
            col("size").isNull() | (trim(col("size")) == ""),
            lit(None).cast(StringType()),
        ).otherwise(trim(col("size"))),
    )

products = products.withColumn(
    "size_clean",
    lower(trim(col("size"))),
)

products = products.withColumn(
    "size_value",
    regexp_extract(col("size_clean"), r"([0-9]+\.?[0-9]*)", 1).cast(DoubleType()),
)

products = products.withColumn(
    "size_unit",
    when(col("size_clean").rlike("fl.?\\s*oz"), lit("ML"))
    .when(col("size_clean").rlike("ml"), lit("ML"))
    .when(col("size_clean").rlike("kg"), lit("G"))
    .when(col("size_clean").rlike("g"), lit("G"))
    .otherwise(lit(None).cast(StringType())),
)

products = products.withColumn(
    "size_value",
    when(
        col("size_clean").rlike("fl.?\\s*oz"),
        col("size_value") * 29.57,
    )
    .when(
        col("size_clean").rlike("kg"),
        col("size_value") * 1000,
    )
    .otherwise(col("size_value")),
)

products = products.drop("size_clean")

if "source" in products.columns:
    valid_sources = [
        "datasheet",
        "skincarisma",
        "dermstore",
        "cosmetics",
        "skincare_products",
        "unknown",
    ]

    products = products.withColumn(
        "source",
        when(col("source").isin(valid_sources), col("source")).otherwise(lit("unknown")),
    )

if "number_of_reviews" in products.columns:
    products = products.withColumn(
        "number_of_reviews",
        col("number_of_reviews").cast(IntegerType()),
    )
    products = products.fillna({"number_of_reviews": 0})

if "rating" in products.columns:
    products = products.withColumn(
        "rating",
        col("rating").cast(FloatType()),
    )

    products = products.withColumn(
        "rating",
        when(col("number_of_reviews") == 0, lit(0.0))
        .when(col("rating").isNull(), lit(5.0))
        .when(col("rating") > 5, lit(5.0))
        .when(col("rating") < 0, lit(0.0))
        .otherwise(col("rating")),
    )

if "brand_name" in products.columns:
    products = products.withColumn(
        "brand_name",
        when(
            col("brand_name").isNull() | (trim(col("brand_name")) == ""),
            lit("Unknown"),
        ).otherwise(col("brand_name")),
    )

int_cols = [
    "ingredients_count",
    "oily_skin_score",
    "dry_skin_score",
    "sensitive_skin_score",
    "combination_skin_score",
    "normal_skin_score",
    "acne_prone_score",
    "pregnancy_safe",
    "fungal_acne_safe",
    "comedogenic_rating",
    "vegan",
    "cruelty_free",
    "reef_safe",
    "fragrance_free",
    "alcohol_free",
    "paraben_free",
    "sulfate_free",
    "silicone_free",
    "oil_free",
]

for c in int_cols:
    if c in products.columns:
        products = products.withColumn(c, col(c).cast(IntegerType()))
        products = products.fillna({c: 0})

products = products.filter(
    col("product_id").isNotNull() & col("product_name").isNotNull()
)

products = products.dropDuplicates(["product_id"])

audit_info(f"Products cleaned rows: {products.count():,}")


log_section("STEP 4 - CLEAN INGREDIENTS")

ingredients = clean_name_text(ingredients, "ingredient_name")

safe_ingredient_text_cols = [
    "description",
    "evidence_level",
    "science_tags",
    "science_details",
    "callout_type",
    "callout_text",
    "warning_type",
    "warning_text",
    "source",
]

for c in safe_ingredient_text_cols:
    if c in ingredients.columns:
        ingredients = ingredients.withColumn(c, trim(col(c)))

if "evidence_level" in ingredients.columns:
    ingredients = ingredients.withColumn(
        "evidence_level",
        col("evidence_level").cast(StringType()),
    )

if "source" in ingredients.columns:
    valid_ing_sources = ["skincarisma", "extracted_from_products", "unknown"]

    ingredients = ingredients.withColumn(
        "source",
        when(col("source").isin(valid_ing_sources), col("source")).otherwise(
            lit("unknown")
        ),
    )

ingredients = ingredients.filter(col("ingredient_name") != "**†")

ingredients = ingredients.filter(
    col("ingredient_id").isNotNull() & col("ingredient_name").isNotNull()
)

ingredients = ingredients.dropDuplicates(["ingredient_id"])

audit_info(f"Ingredients cleaned rows: {ingredients.count():,}")


log_section("STEP 5 - CLEAN EXISTING BRIDGE_NEW")

bridge_new = bridge_new.select(
    "product_id",
    "product_name",
    "ingredient_id",
    "ingredient_name",
    "source",
)

bridge_new = bridge_new.filter(
    col("product_id").isNotNull() & col("ingredient_id").isNotNull()
)

if "source" in bridge_new.columns:
    bridge_new = bridge_new.withColumn("source", trim(col("source")))

    bridge_new = bridge_new.withColumn(
        "source",
        when(col("source").isNull() | (col("source") == ""), lit("unknown")).otherwise(
            col("source")
        ),
    )

valid_product_ids = products.select("product_id").dropDuplicates()
valid_ingredient_ids = ingredients.select("ingredient_id").dropDuplicates()

bridge_new = bridge_new.join(
    broadcast(valid_product_ids),
    on="product_id",
    how="inner",
)

bridge_new = bridge_new.join(
    broadcast(valid_ingredient_ids),
    on="ingredient_id",
    how="inner",
)

bridge_new = bridge_new.dropDuplicates(["product_id", "ingredient_id"])

if "product_name" in bridge_new.columns:
    bridge_new = bridge_new.drop("product_name")

if "ingredient_name" in bridge_new.columns:
    bridge_new = bridge_new.drop("ingredient_name")

bridge_new = bridge_new.join(
    products.select("product_id", "product_name"),
    on="product_id",
    how="left",
)

bridge_new = bridge_new.join(
    ingredients.select("ingredient_id", "ingredient_name"),
    on="ingredient_id",
    how="left",
)

bridge_new = bridge_new.select(
    "product_id",
    "product_name",
    "ingredient_id",
    "ingredient_name",
    "source",
)

audit_info(f"Bridge new cleaned rows: {bridge_new.count():,}")


errors = []
warnings = []

log_section("CHECK 1 - REQUIRED COLUMNS")

required_products = {
    "product_id",
    "product_name",
    "price",
    "size",
    "size_value",
    "size_unit",
    "currency",
    "rating",
    "number_of_reviews",
    "source",
}

required_ingredients = {
    "ingredient_id",
    "ingredient_name",
    "source",
}

required_bridge = {
    "product_id",
    "product_name",
    "ingredient_id",
    "ingredient_name",
    "source",
}

missing_products = required_products - set(products.columns)
missing_ingredients = required_ingredients - set(ingredients.columns)
missing_bridge = required_bridge - set(bridge_new.columns)

if not missing_products:
    audit_ok("Products required columns exist")
else:
    audit_fail(f"Products missing columns: {missing_products}", errors)

if not missing_ingredients:
    audit_ok("Ingredients required columns exist")
else:
    audit_fail(f"Ingredients missing columns: {missing_ingredients}", errors)

if not missing_bridge:
    audit_ok("Bridge new required columns exist")
else:
    audit_fail(f"Bridge new missing columns: {missing_bridge}", errors)


log_section("CHECK 2 - NULL IDS AND NAMES")

check_zero(
    products.filter(col("product_id").isNull()).count(),
    "No null product_id",
    "Null product_id rows: {}",
    errors,
)

check_zero(
    products.filter(col("product_name").isNull()).count(),
    "No null product_name",
    "Null product_name rows: {}",
    errors,
)

check_zero(
    ingredients.filter(col("ingredient_id").isNull()).count(),
    "No null ingredient_id",
    "Null ingredient_id rows: {}",
    errors,
)

check_zero(
    ingredients.filter(col("ingredient_name").isNull()).count(),
    "No null ingredient_name",
    "Null ingredient_name rows: {}",
    errors,
)

check_zero(
    bridge_new.filter(
        col("product_id").isNull() | col("ingredient_id").isNull()
    ).count(),
    "No null IDs in bridge_new",
    "Null bridge_new IDs rows: {}",
    errors,
)

check_zero(
    bridge_new.filter(
        col("product_name").isNull() | col("ingredient_name").isNull()
    ).count(),
    "No null names in bridge_new",
    "Null bridge_new names rows: {}",
    errors,
)


log_section("CHECK 3 - DUPLICATES")

products_count = products.count()
ingredients_count = ingredients.count()
bridge_count = bridge_new.count()

dup_products = products_count - products.select("product_id").distinct().count()
dup_ingredients = ingredients_count - ingredients.select("ingredient_id").distinct().count()

dup_bridge = bridge_count - bridge_new.select(
    "product_id",
    "ingredient_id",
).distinct().count()

check_zero(
    dup_products,
    "No duplicate product_id",
    "Duplicate product_id rows: {}",
    errors,
)

check_zero(
    dup_ingredients,
    "No duplicate ingredient_id",
    "Duplicate ingredient_id rows: {}",
    errors,
)

check_zero(
    dup_bridge,
    "No duplicate product-ingredient pairs",
    "Duplicate bridge_new pairs: {}",
    errors,
)


log_section("CHECK 4 - FK INTEGRITY")

invalid_product_refs = bridge_new.join(
    products.select("product_id"),
    on="product_id",
    how="left_anti",
).count()

invalid_ingredient_refs = bridge_new.join(
    ingredients.select("ingredient_id"),
    on="ingredient_id",
    how="left_anti",
).count()

check_zero(
    invalid_product_refs,
    "All bridge_new product_ids exist in products",
    "Invalid product refs: {}",
    errors,
)

check_zero(
    invalid_ingredient_refs,
    "All bridge_new ingredient_ids exist in ingredients",
    "Invalid ingredient refs: {}",
    errors,
)


log_section("CHECK 5 - NAME CONSISTENCY")

product_name_mismatch = (
    bridge_new.alias("b")
    .join(
        products.select("product_id", "product_name").alias("p"),
        on="product_id",
        how="inner",
    )
    .filter(col("b.product_name") != col("p.product_name"))
    .count()
)

ingredient_name_mismatch = (
    bridge_new.alias("b")
    .join(
        ingredients.select("ingredient_id", "ingredient_name").alias("i"),
        on="ingredient_id",
        how="inner",
    )
    .filter(col("b.ingredient_name") != col("i.ingredient_name"))
    .count()
)

check_zero(
    product_name_mismatch,
    "Product names are consistent",
    "Product name mismatches: {}",
    errors,
)

check_zero(
    ingredient_name_mismatch,
    "Ingredient names are consistent",
    "Ingredient name mismatches: {}",
    errors,
)


log_section("CHECK 6 - PRICE AND SIZE QUALITY")

price_null_count = products.filter(col("price").isNull()).count()

price_logical_count = products.filter(
    col("price").isNotNull() & (col("price") > 0) & (col("price") <= 10000)
).count()

price_not_logical_count = products.filter(
    col("price").isNotNull() & ((col("price") <= 0) | (col("price") > 10000))
).count()

size_null_count = products.filter(col("size").isNull()).count()
size_not_null_count = products.filter(col("size").isNotNull()).count()

size_ml_count = products.filter(col("size_unit") == "ML").count()
size_g_count = products.filter(col("size_unit") == "G").count()

size_unknown_count = products.filter(
    col("size").isNotNull() & col("size_unit").isNull()
).count()

audit_info(f"Price null count: {price_null_count:,}")
audit_info(f"Price logical count: {price_logical_count:,}")
audit_info(f"Price not logical count: {price_not_logical_count:,}")
audit_info(f"Size null count: {size_null_count:,}")
audit_info(f"Size not null count: {size_not_null_count:,}")
audit_info(f"ML size count: {size_ml_count:,}")
audit_info(f"G size count: {size_g_count:,}")
audit_info(f"Unknown size unit count: {size_unknown_count:,}")

if price_not_logical_count > 0:
    audit_warn(
        f"Found {price_not_logical_count:,} products with not logical price",
        warnings,
    )

    print("\nNOT LOGICAL PRICE SAMPLE")
    products.filter(
        col("price").isNotNull() & ((col("price") <= 0) | (col("price") > 10000))
    ).select(
        "product_id",
        "product_name",
        "brand_name",
        "price",
        "size",
        "size_value",
        "size_unit",
        "currency",
        "source",
    ).show(50, truncate=40)
else:
    audit_ok("No not logical price values found")

if size_unknown_count > 0:
    audit_warn(
        f"Found {size_unknown_count:,} products with unknown size unit",
        warnings,
    )

    print("\nUNKNOWN SIZE UNIT SAMPLE")
    products.filter(
        col("size").isNotNull() & col("size_unit").isNull()
    ).select(
        "product_id",
        "product_name",
        "size",
        "size_value",
        "size_unit",
        "source",
    ).show(50, truncate=40)
else:
    audit_ok("No unknown size units found")


log_section("CHECK 7 - RATING AND REVIEWS")

bad_rating = products.filter(
    col("rating").isNull() | (col("rating") < 0) | (col("rating") > 5)
).count()

bad_reviews = products.filter(
    col("number_of_reviews").isNull() | (col("number_of_reviews") < 0)
).count()

bad_zero_review_rating = products.filter(
    (col("number_of_reviews") == 0) & (col("rating") != 0)
).count()

check_zero(
    bad_rating,
    "Ratings are valid",
    "Invalid rating rows: {}",
    errors,
)

check_zero(
    bad_reviews,
    "Reviews are valid",
    "Invalid number_of_reviews rows: {}",
    errors,
)

check_zero(
    bad_zero_review_rating,
    "Zero-review products have rating = 0",
    "Zero-review products with non-zero rating: {}",
    errors,
)


log_section("CHECK 8 - SUMMARY COUNTS")

audit_info(f"Products rows: {products_count:,}")
audit_info(f"Ingredients rows: {ingredients_count:,}")
audit_info(f"Bridge new rows: {bridge_count:,}")

audit_info(
    f"Unique products in bridge: {bridge_new.select('product_id').distinct().count():,}"
)

audit_info(
    f"Unique ingredients in bridge: {bridge_new.select('ingredient_id').distinct().count():,}"
)

avg_ing = bridge_count / max(bridge_new.select("product_id").distinct().count(), 1)
audit_info(f"Avg ingredients per product: {avg_ing:.2f}")


log_section("FINAL AUDIT STATUS")

if len(errors) == 0:
    if len(warnings) == 0:
        print("OVERALL PASSED - no errors, no warnings")
    else:
        print(f"OVERALL PASSED WITH WARNINGS - {len(warnings)} warning(s)")

    print("Saving data to S3")

    products.write.mode("overwrite").parquet(f"{SILVER_NEW}/products")
    print(f"Products saved to: {SILVER_NEW}/products")

    ingredients.write.mode("overwrite").parquet(f"{SILVER_NEW}/ingredients")
    print(f"Ingredients saved to: {SILVER_NEW}/ingredients")

    bridge_new.write.mode("overwrite").parquet(f"{SILVER_NEW}/bridge_new")
    print(f"Bridge new saved to: {SILVER_NEW}/bridge_new")

    print("Silver NewDataV3 saved successfully")

else:
    print(f"OVERALL FAILED - {len(errors)} error(s)")
    print("Data was not saved")

    for e in errors:
        print(f"- {e}")


log_section("PRODUCTS SAMPLE")

products.select(
    "product_id",
    "product_name",
    "brand_name",
    "price",
    "size",
    "size_value",
    "size_unit",
    "currency",
    "rating",
    "number_of_reviews",
    "source",
).show(10, truncate=40)


log_section("INGREDIENTS SAMPLE")

ingredients.select(
    "ingredient_id",
    "ingredient_name",
    "evidence_level",
    "source",
).show(10, truncate=40)


log_section("BRIDGE_NEW SAMPLE")

bridge_new.select(
    "product_id",
    "product_name",
    "ingredient_id",
    "ingredient_name",
    "source",
).show(10, truncate=40)


spark.stop()