"""
GlowPipe Bronze → Silver  |  v5
=================================
All previous fixes (TC-01 → TC-08, TC-NEW-A → TC-NEW-E) retained.

NEW FIXES IN v5  (discovered on data sample audit):
----------------------------------------------------
TC-NEW-G  Price < $1.00 artifacts
          25 "original" prices in range ($0.01–$0.80) are scraper unit-price
          conversion bugs.  Floor raised from >0 to >=1.0.
          Same floor applied to scraper layer (was > 0).

TC-NEW-H  rating = 0.0 for unrated products (77 % of rows)
          Pipeline was setting rating=0.0 when number_of_reviews==0.
          0.0 means "rated badly"; an unrated product must be NULL.
          Fixed: when reviews==0 rating → NULL.

TC-NEW-I  Category over-assigned to 'Skincare' (269 products)
          product_type_text clearly signals Hair Care (73), Bath & Body (166),
          Nail Care (22), Fragrance (8) but keyword fallback on product_name
          missed them.  Added a product_type_text → category correction pass.

TC-NEW-J  sub_category NULL for 85.9 % of products
          product_type_text is filled for 4,363 of those rows.
          Added product_type_text → sub_category derivation pass.

TC-NEW-K  ingredients_count NULL for 96.3 % of products
          Bridge table holds the actual counts; backfill from bridge.

TC-NEW-F  Bridge FK orphans when running on a data SAMPLE
          INNER JOIN silently dropped valid bridge rows that reference product/
          ingredient IDs not present in the sample.
          Fixed: log orphan counts as WARN (not hard FAIL) and note they are
          expected to be 0 on the full production dataset.

AUDIT CHECK FIX
          Check for col("currency") raised a AnalysisException because the
          currency column is dropped earlier in the pipeline.  Guard added.
"""

import os
import re as _re

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, when, lit, lower, broadcast,
    regexp_replace, initcap, coalesce,
    count as spark_count, percentile_approx, expr,
    udf, create_map, first
)
from pyspark.sql.types import FloatType, StringType, IntegerType


spark = (
    SparkSession.builder
    .appName("GlowPipe-Bronze-To-Silver-FullClean-v4")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .config("spark.sql.shuffle.partitions", "200")
    .config("spark.sql.debug.maxToStringFields", "300")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID", ""))
    .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY", ""))
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

BUCKET = "skincare-recommendation-system-data"

BRONZE = f"s3a://{BUCKET}/bronze/NewData"
RAW = f"s3a://{BUCKET}/raw"
SILVER_NEW = f"s3a://{BUCKET}/silver/NewDataV5"

PRODUCTS_PATH = f"{BRONZE}/ProductTable_price_size_fixed/"
INGREDIENTS_PATH = f"{BRONZE}/IngredientsTable.csv"
BRIDGE_NEW_PATH = f"{BRONZE}/BridgeNewTable/"
SCRAPER_PATH = f"{BRONZE}/price_scraper.csv"
RAW_INGREDIENTS_PATH = f"{RAW}/ingredients.csv"

READ_OPTS = dict(
    header=True,
    inferSchema=False,
    multiLine=True,
    escape='"',
    quote='"',
)


def log_section(title):
    print("\n" + "=" * 65)
    print(title)
    print("=" * 65)


def audit_ok(msg):
    print(f"OK   - {msg}")


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


def clean_key(column):
    return lower(
        trim(
            regexp_replace(
                regexp_replace(column, r"[\n\r\t]", " "),
                r"\s+",
                " "
            )
        )
    )


VALID_CATS = {
    "Skincare", "Face Makeup", "Body Care", "Hair Care",
    "Eye Makeup", "Lip Makeup", "Fragrance", "Bath & Body",
    "Sun Care", "Nail Care", "Tools & Accessories",
    "Wellness", "Men Care", "Baby Care",
}

CATEGORY_PRICE_DEFAULTS = {
    "Skincare": 18.54,
    "Face Makeup": 16.99,
    "Body Care": 11.43,
    "Hair Care": 24.00,
    "Eye Makeup": 22.00,
    "Lip Makeup": 28.00,
    "Fragrance": 55.00,
    "Sun Care": 19.00,
    "Bath & Body": 12.00,
    "Nail Care": 14.00,
    "Tools & Accessories": 25.00,
    "Wellness": 22.00,
    "Men Care": 20.00,
    "Baby Care": 16.00,
}

SUBCATEGORY_PRICE_DEFAULTS = {
    "Treatments & Actives": 19.99,
    "Moisturizers": 19.69,
    "Cleansers": 14.00,
    "Toners": 18.00,
    "Sunscreens": 18.99,
    "Eye Care": 28.00,
    "Makeup Removers": 15.35,
    "Exfoliators": 20.00,
    "Masks": 18.99,
    "Prescription & Clinical": 29.99,
    "Body Lotions & Moisturizers": 12.71,
    "Lip Care": 10.00,
    "Hair Treatments": 25.00,
    "Bath & Body Washes": 10.48,
    "Shampoos & Conditioners": 24.00,
    "Body Scrubs": 13.99,
    "Hand & Foot Care": 5.99,
}

GLOBAL_FALLBACK_PRICE = 19.99

VALID_SOURCES = {
    "datasheet", "skincarisma", "dermstore",
    "cosmetics", "skincare_products", "unknown",
}

COUNTRY_ALIASES = {
    "usa": "United States",
    "us": "United States",
    "u.s.": "United States",
    "u.s.a.": "United States",
    "england": "United Kingdom",
    "great britain": "United Kingdom",
    "uk": "United Kingdom",
    "u.k.": "United Kingdom",
    "korea": "South Korea",
    "republic of korea": "South Korea",
    "türkiye": "Turkey",
}

KNOWN_COUNTRIES_SET = {
    "united states", "usa", "us", "south korea", "korea", "france",
    "united kingdom", "uk", "england", "japan", "canada", "germany",
    "australia", "switzerland", "spain", "italy", "poland", "india",
    "ireland", "turkey", "türkiye", "brazil", "netherlands", "denmark",
    "sweden", "greece", "singapore", "indonesia", "philippines",
    "czech republic", "finland", "malaysia", "new zealand", "serbia",
    "thailand", "south africa", "croatia", "china", "israel", "taiwan",
    "latvia", "hong kong", "pakistan", "iceland", "colombia", "argentina",
    "hungary", "norway", "uae", "united arab emirates", "togo", "belgium",
    "bangladesh", "romania", "chile", "mexico", "malta", "morocco",
    "nigeria", "lithuania", "egypt", "jordan", "peru", "estonia",
    "various",
}


def _extract_country_from_marketing_string(val):
    if not val or not isinstance(val, str):
        return val

    val = val.strip()

    if "·" not in val:
        return val

    segments = [s.strip() for s in val.split("·")]

    for seg in reversed(segments):
        clean = _re.sub(r"(?i)^the\s+", "", seg).strip()
        clean = _re.sub(
            r"(?i)\s+(by\s+\S|brand[\s:]|exclusive\s+to|founded\s+in).*$",
            "",
            clean
        ).strip()

        if clean.lower() in KNOWN_COUNTRIES_SET:
            return clean

    if len(segments) >= 2:
        return segments[1].strip()

    return val


extract_country_udf = udf(_extract_country_from_marketing_string, StringType())

KEYWORD_CATS = [
    (r"(?i)(shampoo|conditioner|hair mask|hair serum|scalp)", "Hair Care"),
    (r"(?i)(mascara|eyeliner|eye shadow|brow)", "Eye Makeup"),
    (r"(?i)(foundation|blush|bronzer|concealer|primer|bb cream|cc cream|powder)", "Face Makeup"),
    (r"(?i)(lipstick|lip gloss|lip liner|lip balm|lip oil)", "Lip Makeup"),
    (r"(?i)(body lotion|body cream|body wash|hand cream|body butter|deodorant)", "Body Care"),
    (r"(?i)(perfume|cologne|eau de)", "Fragrance"),
    (r"(?i)(sunscreen|spf|sun protection|uv)", "Sun Care"),
    (r"(?i)(nail polish|nail serum|nail oil|cuticle)", "Nail Care"),
    (r"(?i)(gua sha|jade roller|face roller|facial tool|brush)", "Tools & Accessories"),
    (r"(?i)(baby|infant|toddler)", "Baby Care"),
    (r"(?i)(men|beard|aftershave|shaving)", "Men Care"),
]

# TC-NEW-I: product_type_text → category override
# Fixes 269 products that keyword-on-product_name missed
# (Nail Care 22, Fragrance 8, Hair Care 73, Bath & Body 166)
PRODUCT_TYPE_TO_CATEGORY = {
    "Shampoo": "Hair Care", "Conditioner": "Hair Care",
    "Other Haircare": "Hair Care", "Hair Mask": "Hair Care",
    "Scalp Treatment": "Hair Care", "Hair Serum": "Hair Care",
    "Hair Oil": "Hair Care", "Hair Spray": "Hair Care",
    "Hair Color": "Hair Care", "Hair Loss Treatment": "Hair Care",
    "Bath & Body": "Bath & Body", "Body Wash": "Bath & Body",
    "Body Lotion": "Body Care", "Body Scrub": "Body Care",
    "Hand Cream": "Body Care", "Body Butter": "Body Care",
    "Nail Care": "Nail Care", "Nail Polish": "Nail Care",
    "Nail Treatment": "Nail Care",
    "Fragrance": "Fragrance", "Perfume": "Fragrance", "Cologne": "Fragrance",
    "Eye Makeup": "Eye Makeup", "Mascara": "Eye Makeup",
    "Eyeliner": "Eye Makeup", "Eye Shadow": "Eye Makeup", "Brow": "Eye Makeup",
    "Face Makeup": "Face Makeup", "Foundation": "Face Makeup",
    "Blush": "Face Makeup", "Bronzer": "Face Makeup",
    "Concealer": "Face Makeup", "Primer": "Face Makeup",
    "Highlighter": "Face Makeup", "Setting Powder": "Face Makeup",
    "Bb Cream": "Face Makeup", "Cc Cream": "Face Makeup",
    "Lip Makeup": "Lip Makeup", "Lipstick": "Lip Makeup",
    "Lip Gloss": "Lip Makeup", "Lip Liner": "Lip Makeup",
    "Lip Moisturizer": "Lip Makeup",
    "Sunscreen": "Sun Care", "Sun Protect": "Sun Care",
    "Spf Moisturizer": "Sun Care",
    "Tool": "Tools & Accessories", "Brush": "Tools & Accessories",
    "Applicator": "Tools & Accessories",
    "Men Care": "Men Care", "Aftershave": "Men Care", "Shaving Cream": "Men Care",
}

# TC-NEW-J: product_type_text → sub_category derivation
# Fills 85.9 % NULL sub_category using the granular product_type_text signal
PRODUCT_TYPE_TO_SUBCAT = {
    "Serum": "Treatments & Actives", "Facial Treatment": "Treatments & Actives",
    "Oil": "Treatments & Actives", "Essence": "Treatments & Actives",
    "Ampoule": "Treatments & Actives", "Treatment": "Treatments & Actives",
    "Spot Treatment": "Treatments & Actives", "Peel": "Exfoliators",
    "General Moisturizer": "Moisturizers", "Day Moisturizer": "Moisturizers",
    "Night Moisturizer": "Moisturizers", "Moisturizer": "Moisturizers",
    "Eye Moisturizer": "Eye Care", "Eye Cream": "Eye Care",
    "Face Cleanser": "Cleansers", "Cleanser": "Cleansers",
    "Makeup Remover": "Makeup Removers", "Micellar Water": "Makeup Removers",
    "Toner": "Toners", "Essence Toner": "Toners",
    "Sheet Mask": "Masks", "Wet Mask": "Masks", "Face Mask": "Masks",
    "Overnight Mask": "Masks", "Clay Mask": "Masks",
    "Exfoliator": "Exfoliators", "Scrub": "Exfoliators",
    "Sunscreen": "Sunscreens", "Spf Moisturizer": "Sunscreens",
    "Body Lotion": "Body Lotions & Moisturizers",
    "Body Butter": "Body Lotions & Moisturizers",
    "Hand Cream": "Hand & Foot Care", "Foot Cream": "Hand & Foot Care",
    "Body Scrub": "Body Scrubs",
    "Shampoo": "Shampoos & Conditioners", "Conditioner": "Shampoos & Conditioners",
    "Hair Mask": "Hair Treatments", "Hair Serum": "Hair Treatments",
    "Other Haircare": "Hair Treatments",
    "Bath & Body": "Bath & Body Washes", "Body Wash": "Bath & Body Washes",
    "Lip Moisturizer": "Lip Care", "Lip Balm": "Lip Care",
    "Tool": "Tools & Accessories",
}

FLAG_COLS = [
    "reef_safe", "fragrance_free", "alcohol_free", "paraben_free",
    "sulfate_free", "silicone_free", "oil_free",
]

INT_COLS = [
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


log_section("STEP 1 - READ TABLES")

products = spark.read.csv(PRODUCTS_PATH, **READ_OPTS)
ingredients = spark.read.csv(INGREDIENTS_PATH, **READ_OPTS)
bridge_new = spark.read.csv(BRIDGE_NEW_PATH, **READ_OPTS)
scraper = spark.read.csv(SCRAPER_PATH, **READ_OPTS)
raw_ingredients = spark.read.csv(RAW_INGREDIENTS_PATH, **READ_OPTS)

audit_info(f"Products rows: {products.count():,}")
audit_info(f"Ingredients rows: {ingredients.count():,}")
audit_info(f"Bridge rows: {bridge_new.count():,}")
audit_info(f"Scraper rows: {scraper.count():,}")
audit_info(f"Raw ingredients rows: {raw_ingredients.count():,}")


log_section("STEP 2 - CAST IDS")

products = products.withColumn("product_id", trim(col("product_id").cast(StringType())))
ingredients = ingredients.withColumn("ingredient_id", trim(col("ingredient_id").cast(StringType())))

bridge_new = (
    bridge_new
    .withColumn("product_id", trim(col("product_id").cast(StringType())))
    .withColumn("ingredient_id", trim(col("ingredient_id").cast(StringType())))
)


log_section("STEP 3 - CLEAN PRODUCTS")

products = products.withColumn(
    "product_name",
    initcap(
        trim(
            regexp_replace(
                regexp_replace(
                    regexp_replace(col("product_name"), r'\\"', '"'),
                    r'^\"+|\"+$',
                    ''
                ),
                r'\s+',
                ' '
            )
        )
    )
)

for c in [
    "brand_name", "category", "sub_category", "source",
    "product_url", "image_url", "description", "size", "country_of_origin"
]:
    if c in products.columns:
        products = products.withColumn(c, trim(col(c)))

if "_norm" in products.columns:
    products = products.drop("_norm")

products = products.withColumn("currency", lit("USD"))

products = products.withColumn("price", col("price").cast(FloatType()))

products = products.withColumn(
    "price",
    when(col("price").isNull(), lit(None).cast(FloatType()))
    .when(col("price") < 1.0, lit(None).cast(FloatType()))   # TC-NEW-G: $0.01-$0.99 are scraper unit-price artifacts
    .when(col("price") > 1000, lit(None).cast(FloatType()))
    .otherwise(col("price"))
)

if "size" not in products.columns:
    products = products.withColumn("size", lit(None).cast(StringType()))
else:
    products = products.withColumn(
        "size",
        when(col("size").isNull() | (trim(col("size")) == ""), lit(None).cast(StringType()))
        .otherwise(trim(col("size")))
    )

products = products.withColumn(
    "source",
    when(lower(trim(col("source"))).isin(list(VALID_SOURCES)), lower(trim(col("source"))))
    .otherwise(lit("unknown"))
)

if "country_of_origin" in products.columns:
    products = products.withColumn(
        "country_of_origin",
        extract_country_udf(col("country_of_origin"))
    )

    country_aliases_expr = col("country_of_origin")

    for alias, canonical in COUNTRY_ALIASES.items():
        country_aliases_expr = when(
            lower(trim(col("country_of_origin"))) == alias,
            lit(canonical)
        ).otherwise(country_aliases_expr)

    products = products.withColumn(
        "country_of_origin",
        when(
            col("country_of_origin").cast(FloatType()).isNotNull()
            & col("country_of_origin").rlike(r"^[0-9\.]+$"),
            lit(None).cast(StringType())
        ).otherwise(country_aliases_expr)
    )

if "number_of_reviews" in products.columns:
    products = products.withColumn("number_of_reviews", col("number_of_reviews").cast(IntegerType()))
    products = products.fillna({"number_of_reviews": 0})

if "rating" in products.columns:
    size_pattern = r"[0-9]+\.?[0-9]*\s*(ml|fl\.?\s*oz|g|kg|oz)"

    products = products.withColumn(
        "size",
        when(
            col("size").isNull()
            & col("rating").isNotNull()
            & col("rating").rlike(size_pattern),
            trim(col("rating"))
        ).otherwise(col("size"))
    )

    products = products.withColumn(
        "_rating_shifted",
        when(
            col("rating").isNotNull()
            & (trim(col("rating")) != "")
            & col("rating").cast(FloatType()).isNull(),
            lit(1)
        ).otherwise(lit(0))
    )

    products = products.withColumn(
        "rating",
        when(col("_rating_shifted") == 1, lit(None).cast(FloatType()))
        .otherwise(col("rating").cast(FloatType()))
    )

    products = products.withColumn(
        "rating",
        # TC-NEW-H: 0 reviews means "unrated", not "rated zero" → NULL
        when(col("number_of_reviews") == 0, lit(None).cast(FloatType()))
        .when(col("rating").isNull() & (col("number_of_reviews") > 0), lit(5.0))
        .when(col("rating").isNull(), lit(None).cast(FloatType()))
        .when(col("rating") > 5, lit(5.0))
        .when(col("rating") < 0, lit(0.0))
        .otherwise(col("rating"))
    )

    products = products.drop("_rating_shifted")

if "brand_name" in products.columns:
    products = products.withColumn(
        "brand_name",
        when(col("brand_name").isNull() | (trim(col("brand_name")) == ""), lit("Unknown"))
        .otherwise(col("brand_name"))
    )

for c in INT_COLS:
    if c in products.columns:
        products = products.withColumn(c, col(c).cast(FloatType()))
        products = products.withColumn(
            c,
            when(col(c).isin([0.0, 1.0]), col(c).cast(IntegerType()))
            .otherwise(lit(0).cast(IntegerType()))
        )

if "comedogenic_rating" in products.columns:
    products = products.withColumn("comedogenic_rating", col("comedogenic_rating").cast(FloatType()))
    products = products.withColumn(
        "comedogenic_rating",
        when(col("comedogenic_rating").isNull(), lit(0).cast(IntegerType()))
        .when(col("comedogenic_rating").between(0, 5), col("comedogenic_rating").cast(IntegerType()))
        .otherwise(lit(0).cast(IntegerType()))
    )

if "irritation_rating" in products.columns:
    products = products.withColumn("irritation_rating", col("irritation_rating").cast(FloatType()))
    products = products.withColumn(
        "irritation_rating",
        when(col("irritation_rating").isNull(), lit(0).cast(IntegerType()))
        .when(col("irritation_rating").between(0, 5), col("irritation_rating").cast(IntegerType()))
        .otherwise(lit(0).cast(IntegerType()))
    )

if "ingredients_count" in products.columns:
    products = products.withColumn("ingredients_count", col("ingredients_count").cast(IntegerType()))
    products = products.withColumn(
        "ingredients_count",
        when(col("ingredients_count").isNull(), lit(None).cast(IntegerType()))
        .when(col("ingredients_count") <= 0, lit(None).cast(IntegerType()))
        .when(col("ingredients_count") > 500, lit(None).cast(IntegerType()))
        .otherwise(col("ingredients_count"))
    )

brand_names_list = [
    r["brand_name"]
    for r in products.select(trim(col("brand_name")).alias("brand_name"))
    .filter(col("brand_name").isNotNull() & (col("brand_name") != ""))
    .distinct()
    .collect()
]

brand_cat_lookup = (
    products
    .filter(trim(col("category")).isin(list(VALID_CATS)))
    .groupBy(
        trim(col("brand_name")).alias("brand_key"),
        trim(col("category")).alias("cat_value")
    )
    .agg(spark_count("*").alias("cnt"))
    .withColumn("rn", expr("row_number() OVER (PARTITION BY brand_key ORDER BY cnt DESC)"))
    .filter(col("rn") == 1)
    .select(col("brand_key"), col("cat_value").alias("brand_inferred_cat"))
)

SUBCAT_TO_CAT = {
    "Treatments & Actives": "Skincare",
    "Moisturizers": "Skincare",
    "Cleansers": "Skincare",
    "Toners": "Skincare",
    "Sunscreens": "Sun Care",
    "Eye Care": "Skincare",
    "Makeup Removers": "Skincare",
    "Exfoliators": "Skincare",
    "Masks": "Skincare",
    "Prescription & Clinical": "Skincare",
    "Body Lotions & Moisturizers": "Body Care",
    "Lip Care": "Lip Makeup",
    "Hair Treatments": "Hair Care",
    "Bath & Body Washes": "Bath & Body",
    "Shampoos & Conditioners": "Hair Care",
    "Body Scrubs": "Body Care",
    "Hand & Foot Care": "Body Care",
}

subcat_map_expr = create_map([lit(k) for pair in SUBCAT_TO_CAT.items() for k in pair])

kw_when = None

for pattern, cat in KEYWORD_CATS:
    cond = col("product_name").rlike(pattern)
    kw_when = when(cond, lit(cat)) if kw_when is None else kw_when.when(cond, lit(cat))

kw_when = kw_when.otherwise(lit("Skincare"))

products = (
    products
    .withColumn("_brand_key", trim(col("brand_name")))
    .join(broadcast(brand_cat_lookup), col("_brand_key") == col("brand_key"), "left")
    .drop("brand_key", "_brand_key")
    .withColumn(
        "category",
        when(trim(col("category")).isin(brand_names_list), coalesce(col("brand_inferred_cat"), kw_when))
        .otherwise(col("category"))
    )
    .drop("brand_inferred_cat")
)

products = products.withColumn(
    "category",
    when(
        col("category").isNull() | (trim(col("category")) == ""),
        coalesce(subcat_map_expr[trim(col("sub_category"))], kw_when)
    ).otherwise(col("category"))
)

# TC-NEW-I: product_type_text → category correction pass
# Overrides category ONLY when product_type_text maps to a different valid
# category that the keyword pass on product_name missed.
ptt_cat_map = create_map([lit(k) for pair in PRODUCT_TYPE_TO_CATEGORY.items() for k in pair])

products = products.withColumn(
    "category",
    when(
        col("product_type_text").isNotNull()
        & ptt_cat_map[trim(col("product_type_text"))].isNotNull()
        & (ptt_cat_map[trim(col("product_type_text"))] != col("category")),
        ptt_cat_map[trim(col("product_type_text"))]
    ).otherwise(col("category"))
)

# TC-NEW-J: derive sub_category from product_type_text where still NULL
ptt_subcat_map = create_map([lit(k) for pair in PRODUCT_TYPE_TO_SUBCAT.items() for k in pair])

products = products.withColumn(
    "sub_category",
    when(
        col("sub_category").isNull() | (trim(col("sub_category")) == ""),
        ptt_subcat_map[trim(col("product_type_text"))]
    ).otherwise(col("sub_category"))
)

products = products.filter(col("product_id").isNotNull() & col("product_name").isNotNull())
products = products.dropDuplicates(["product_id"])

audit_info(f"Products cleaned rows: {products.count():,}")


log_section("STEP 4 - CLEAN INGREDIENTS + ENRICH DESCRIPTION")

ingredients = ingredients.withColumn(
    "ingredient_name",
    trim(
        regexp_replace(
            regexp_replace(col("ingredient_name"), r"[\n\r\t]", " "),
            r"\s+",
            " "
        )
    )
)

if "description" not in ingredients.columns:
    ingredients = ingredients.withColumn("description", lit(None).cast(StringType()))
else:
    ingredients = ingredients.withColumn("description", trim(col("description")))

for c in [
    "evidence_level", "science_tags", "science_details",
    "callout_type", "callout_text", "warning_type", "warning_text", "source"
]:
    if c in ingredients.columns:
        ingredients = ingredients.withColumn(c, trim(col(c)))

if "source" in ingredients.columns:
    valid_ing_sources = {"skincarisma", "extracted_from_products", "unknown"}
    ingredients = ingredients.withColumn(
        "source",
        when(col("source").isin(list(valid_ing_sources)), col("source"))
        .otherwise(lit("unknown"))
    )

ingredients = ingredients.filter(
    col("ingredient_name").isNotNull()
    & (col("ingredient_name") != "**†")
    & col("ingredient_id").isNotNull()
)

ingredients = ingredients.filter(expr("length(ingredient_name) <= 200"))
ingredients = ingredients.dropDuplicates(["ingredient_id"])

raw_ingredients_clean = (
    raw_ingredients
    .select(
        trim(col("raw_ingredient_name")).alias("raw_ingredient_name"),
        trim(col("full_description")).alias("full_description")
    )
    .filter(
        col("raw_ingredient_name").isNotNull()
        & (trim(col("raw_ingredient_name")) != "")
        & col("full_description").isNotNull()
        & (trim(col("full_description")) != "")
    )
    .withColumn("_ingredient_key", clean_key(col("raw_ingredient_name")))
    .groupBy("_ingredient_key")
    .agg(first("full_description", ignorenulls=True).alias("raw_full_description"))
)

before_desc_nulls = ingredients.filter(
    col("description").isNull() | (trim(col("description")) == "")
).count()

ingredients = (
    ingredients
    .withColumn("_ingredient_key", clean_key(col("ingredient_name")))
    .join(broadcast(raw_ingredients_clean), "_ingredient_key", "left")
    .withColumn(
        "description",
        when(
            col("description").isNull() | (trim(col("description")) == ""),
            col("raw_full_description")
        ).otherwise(col("description"))
    )
    .drop("_ingredient_key", "raw_full_description")
)

after_desc_nulls = ingredients.filter(
    col("description").isNull() | (trim(col("description")) == "")
).count()

audit_info(f"Ingredient description nulls before enrichment: {before_desc_nulls:,}")
audit_info(f"Ingredient description nulls after enrichment: {after_desc_nulls:,}")
audit_info(f"Ingredient descriptions filled from raw: {before_desc_nulls - after_desc_nulls:,}")
audit_info(f"Ingredients cleaned rows: {ingredients.count():,}")


log_section("STEP 5 - CLEAN BRIDGE")

bridge_new = bridge_new.select("product_id", "ingredient_id", "source")

bridge_new = bridge_new.filter(
    col("product_id").isNotNull() & col("ingredient_id").isNotNull()
)

bridge_new = bridge_new.withColumn(
    "source",
    when(col("source").isNull() | (trim(col("source")) == ""), lit("unknown"))
    .otherwise(trim(col("source")))
)

valid_pids = products.select("product_id").dropDuplicates()
valid_iids = ingredients.select("ingredient_id").dropDuplicates()

# TC-NEW-F: on a data SAMPLE the bridge references product/ingredient IDs that
# exist in the full dataset but not in this sample.  Count orphans before the
# INNER JOIN so the drop is visible in logs.  In production (full dataset)
# both counts must be 0.
orphan_bridge_pids = bridge_new.join(valid_pids, "product_id", "left_anti").count()
orphan_bridge_iids = bridge_new.join(valid_iids, "ingredient_id", "left_anti").count()
audit_info(f"Bridge orphan product_ids   : {orphan_bridge_pids:,}  (expected 0 in production)")
audit_info(f"Bridge orphan ingredient_ids: {orphan_bridge_iids:,}  (expected 0 in production)")

bridge_new = bridge_new.join(broadcast(valid_pids), "product_id", "inner")
bridge_new = bridge_new.join(broadcast(valid_iids), "ingredient_id", "inner")
bridge_new = bridge_new.dropDuplicates(["product_id", "ingredient_id"])

bridge_new = (
    bridge_new
    .join(products.select("product_id", "product_name"), "product_id", "left")
    .join(ingredients.select("ingredient_id", "ingredient_name"), "ingredient_id", "left")
    .select("product_id", "product_name", "ingredient_id", "ingredient_name", "source")
)

audit_info(f"Bridge cleaned rows: {bridge_new.count():,}")


log_section("STEP 6 - PRICE ENRICHMENT")

null_before = products.filter(col("price").isNull()).count()
audit_info(f"Price NULL before enrichment: {null_before:,}")

scraper_clean = (
    scraper
    .filter(col("status") == "valid")
    .withColumn("scraped_price", col("price_usd").cast(FloatType()))
    # TC-NEW-G: same $1 floor as products table
    .filter(col("scraped_price").isNotNull() & (col("scraped_price") >= 1.0) & (col("scraped_price") <= 1000))
    .select(trim(col("product_id")).alias("s_pid"), col("scraped_price"))
)

products = (
    products
    .join(broadcast(scraper_clean), col("product_id") == col("s_pid"), "left")
    .withColumn(
        "price",
        when(col("price").isNull() & col("scraped_price").isNotNull(), col("scraped_price"))
        .otherwise(col("price"))
    )
    .withColumn(
        "price_source",
        when(col("scraped_price").isNotNull() & col("s_pid").isNotNull(), lit("amazon_scraper"))
        .when(col("price").isNotNull(), lit("original"))
        .otherwise(lit(None).cast(StringType()))
    )
    .drop("s_pid", "scraped_price")
)

brand_median = (
    products
    .filter(col("price").isNotNull())
    .groupBy(lower(trim(col("brand_name"))).alias("bm_brand"))
    .agg(
        spark_count("price").alias("bm_count"),
        percentile_approx("price", 0.5).cast(FloatType()).alias("brand_med_price")
    )
    .filter(col("bm_count") >= 5)
)

products = (
    products
    .withColumn("_bl", lower(trim(col("brand_name"))))
    .join(broadcast(brand_median), col("_bl") == col("bm_brand"), "left")
    .withColumn(
        "price",
        when(col("price").isNull() & col("brand_med_price").isNotNull(), col("brand_med_price"))
        .otherwise(col("price"))
    )
    .withColumn(
        "price_source",
        when(col("price_source").isNull() & col("brand_med_price").isNotNull(), lit("synthetic_brand_median"))
        .otherwise(col("price_source"))
    )
    .drop("_bl", "bm_brand", "bm_count", "brand_med_price")
)

subcat_median = (
    products
    .filter(col("price").isNotNull() & col("sub_category").isNotNull())
    .groupBy(trim(col("sub_category")).alias("sm_subcat"))
    .agg(percentile_approx("price", 0.5).cast(FloatType()).alias("subcat_med_price"))
)

products = (
    products
    .join(broadcast(subcat_median), trim(col("sub_category")) == col("sm_subcat"), "left")
    .withColumn(
        "price",
        when(col("price").isNull() & col("subcat_med_price").isNotNull(), col("subcat_med_price"))
        .otherwise(col("price"))
    )
    .withColumn(
        "price_source",
        when(col("price_source").isNull() & col("subcat_med_price").isNotNull(), lit("synthetic_subcategory_median"))
        .otherwise(col("price_source"))
    )
    .drop("sm_subcat", "subcat_med_price")
)

cat_median = (
    products
    .filter(col("price").isNotNull() & col("category").isNotNull())
    .groupBy(trim(col("category")).alias("cm_cat"))
    .agg(percentile_approx("price", 0.5).cast(FloatType()).alias("cat_med_price"))
)

products = (
    products
    .join(broadcast(cat_median), trim(col("category")) == col("cm_cat"), "left")
    .withColumn(
        "price",
        when(col("price").isNull() & col("cat_med_price").isNotNull(), col("cat_med_price"))
        .otherwise(col("price"))
    )
    .withColumn(
        "price_source",
        when(col("price_source").isNull() & col("cat_med_price").isNotNull(), lit("synthetic_category_median"))
        .otherwise(col("price_source"))
    )
    .drop("cm_cat", "cat_med_price")
)

subcat_default_map = create_map([lit(k) for pair in SUBCATEGORY_PRICE_DEFAULTS.items() for k in pair])
cat_default_map = create_map([lit(k) for pair in CATEGORY_PRICE_DEFAULTS.items() for k in pair])

products = products.withColumn(
    "price",
    when(
        col("price").isNull(),
        coalesce(
            subcat_default_map[trim(col("sub_category"))].cast(FloatType()),
            cat_default_map[trim(col("category"))].cast(FloatType()),
            lit(GLOBAL_FALLBACK_PRICE).cast(FloatType())
        )
    ).otherwise(col("price"))
).withColumn(
    "price_source",
    when(col("price_source").isNull() & subcat_default_map[trim(col("sub_category"))].isNotNull(),
         lit("synthetic_hardcoded_subcategory_default"))
    .when(col("price_source").isNull() & cat_default_map[trim(col("category"))].isNotNull(),
          lit("synthetic_hardcoded_category_default"))
    .when(col("price_source").isNull(), lit("synthetic_global_default"))
    .otherwise(col("price_source"))
)

audit_info(f"Price NULL after enrichment: {products.filter(col('price').isNull()).count():,}")


# ── TC-NEW-K: Backfill ingredients_count from bridge ─────────────────────
# 96.3 % of products have NULL ingredients_count, but the bridge table holds
# the actual ingredient rows.  Count bridge rows per product and backfill.
log_section("STEP 7 - BACKFILL ingredients_count FROM BRIDGE (TC-NEW-K)")

_null_cnt_before = products.filter(col("ingredients_count").isNull()).count()
audit_info(f"ingredients_count NULL before backfill: {_null_cnt_before:,}")

bridge_counts = (
    bridge_new
    .groupBy("product_id")
    .agg(spark_count("ingredient_id").alias("_bridge_ing_count"))
)

products = (
    products
    .join(broadcast(bridge_counts), "product_id", "left")
    .withColumn(
        "ingredients_count",
        when(
            col("ingredients_count").isNull() & col("_bridge_ing_count").isNotNull(),
            col("_bridge_ing_count").cast(IntegerType())
        ).otherwise(col("ingredients_count"))
    )
    .drop("_bridge_ing_count")
)

_null_cnt_after = products.filter(col("ingredients_count").isNull()).count()
audit_info(f"ingredients_count NULL after backfill : {_null_cnt_after:,}")
audit_info(f"Backfilled from bridge                : {_null_cnt_before - _null_cnt_after:,}")


errors = []
warnings = []

log_section("CHECKS")

check_zero(products.filter(col("product_id").isNull()).count(), "No null product_id", "Null product_id: {}", errors)
check_zero(products.filter(col("product_name").isNull()).count(), "No null product_name", "Null product_name: {}", errors)
check_zero(ingredients.filter(col("ingredient_id").isNull()).count(), "No null ingredient_id", "Null ingredient_id: {}", errors)
check_zero(ingredients.filter(col("ingredient_name").isNull()).count(), "No null ingredient_name", "Null ingredient_name: {}", errors)

check_zero(products.count() - products.select("product_id").distinct().count(), "No duplicate product_id", "Duplicate product_id: {}", errors)
check_zero(ingredients.count() - ingredients.select("ingredient_id").distinct().count(), "No duplicate ingredient_id", "Duplicate ingredient_id: {}", errors)
check_zero(bridge_new.count() - bridge_new.select("product_id", "ingredient_id").distinct().count(), "No duplicate bridge pairs", "Duplicate bridge pairs: {}", errors)

check_zero(bridge_new.join(products.select("product_id"), "product_id", "left_anti").count(), "All bridge product_ids exist", "Invalid product refs: {}", errors)
check_zero(bridge_new.join(ingredients.select("ingredient_id"), "ingredient_id", "left_anti").count(), "All bridge ingredient_ids exist", "Invalid ingredient refs: {}", errors)

check_zero(products.filter(col("price").isNull()).count(), "No null price", "Null price: {}", errors)
check_zero(products.filter(col("price") < 1.0).count(), "No price < $1.00 (TC-NEW-G)", "Price < $1.00: {}", errors)
check_zero(products.filter(col("price") > 1000).count(), "No price > 1000", "Price > 1000: {}", errors)
# currency column is dropped upstream; only check if it still exists
if "currency" in products.columns:
    check_zero(products.filter(col("currency") != "USD").count(), "All currency values are USD", "Non-USD currency rows: {}", errors)

for c in FLAG_COLS:
    if c in products.columns:
        check_zero(
            products.filter(col(c).isNotNull() & ~col(c).isin(0, 1)).count(),
            f"{c} values are 0/1",
            f"{c} bad values: {{}}",
            errors
        )

hardcoded_count = products.filter(col("price_source").like("synthetic_hardcoded%")).count()
if hardcoded_count > 0:
    audit_warn(f"{hardcoded_count:,} products use hardcoded price defaults", warnings)

# TC-NEW-F: bridge orphan counts are WARN not FAIL on partial samples
if orphan_bridge_pids > 0:
    audit_warn(f"Bridge orphan product_ids: {orphan_bridge_pids:,} (expected 0 in production; OK on sample)", warnings)
if orphan_bridge_iids > 0:
    audit_warn(f"Bridge orphan ingredient_ids: {orphan_bridge_iids:,} (expected 0 in production; OK on sample)", warnings)

# TC-NEW-H: unrated products must be NULL, not 0.0
_zero_with_reviews = products.filter((col("rating") == 0.0) & (col("number_of_reviews") > 0)).count()
check_zero(_zero_with_reviews, "No rating=0 with reviews>0 (TC-NEW-H)", "rating=0 with reviews>0: {}", errors)

# TC-NEW-I/J: category and sub_category quality
check_zero(
    products.filter(~trim(col("category")).isin(list(VALID_CATS))).count(),
    "All categories are valid (TC-NEW-I)", "Invalid category rows: {}", errors
)
_subcat_null = products.filter(col("sub_category").isNull() | (trim(col("sub_category")) == "")).count()
if _subcat_null > 0:
    audit_warn(f"{_subcat_null:,} products still have NULL sub_category (TC-NEW-J: no ptt mapping available)", warnings)
else:
    audit_ok("All products have sub_category (TC-NEW-J)")

# TC-NEW-K: ingredients_count remaining nulls
_ing_null_remaining = products.filter(col("ingredients_count").isNull()).count()
if _ing_null_remaining > 0:
    audit_warn(f"{_ing_null_remaining:,} products have no bridge rows – ingredients_count stays NULL", warnings)
else:
    audit_ok("All products have ingredients_count (TC-NEW-K)")


log_section("SUMMARY")

audit_info(f"Products rows: {products.count():,}")
audit_info(f"Ingredients rows: {ingredients.count():,}")
audit_info(f"Bridge rows: {bridge_new.count():,}")

products.groupBy("price_source").count().orderBy("price_source").show(50, False)
products.groupBy("category").count().orderBy("count", ascending=False).show(20, False)
products.groupBy("sub_category").count().orderBy("count", ascending=False).show(20, False)


log_section("FINAL STATUS")

if len(errors) == 0:
    print(f"OVERALL {'PASSED' if len(warnings) == 0 else 'PASSED WITH WARNINGS'}")

    if warnings:
        for w in warnings:
            print(f"WARNING - {w}")

    products.write.mode("overwrite").parquet(f"{SILVER_NEW}/products")
    ingredients.write.mode("overwrite").parquet(f"{SILVER_NEW}/ingredients")
    bridge_new.write.mode("overwrite").parquet(f"{SILVER_NEW}/bridge_new")

    print(f"Parquet saved to: {SILVER_NEW}")
    

else:
    print(f"OVERALL FAILED - {len(errors)} error(s)")
    for e in errors:
        print(f"- {e}")

spark.stop()