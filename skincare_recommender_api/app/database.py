import snowflake.connector
import pandas as pd
import logging
from app.config import get_settings

logger = logging.getLogger(__name__)

_products_df: pd.DataFrame | None = None
_ingredients_df: pd.DataFrame | None = None
_fact_df: pd.DataFrame | None = None


def get_connection():
    s = get_settings()
    return snowflake.connector.connect(
        user=s.snowflake_user,
        password=s.snowflake_password,
        account=s.snowflake_account,
        warehouse=s.snowflake_warehouse,
        database=s.snowflake_database,
        schema=s.snowflake_schema,
    )


def test_connection() -> bool:
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            return True
    except Exception as e:
        logger.error(f"Snowflake connection failed: {e}")
        return False


def load_products() -> pd.DataFrame:
    global _products_df
    if _products_df is not None:
        return _products_df

    query = """
        SELECT
            PRODUCT_ID, PRODUCT_URL, PRODUCT_NAME, BRAND_NAME,
            CATEGORY, SUB_CATEGORY, IMAGE_URL,
            PRICE, CURRENCY, SIZE, RATING, NUMBER_OF_REVIEWS,
            OILY_SKIN_SCORE, DRY_SKIN_SCORE, SENSITIVE_SKIN_SCORE,
            COMBINATION_SKIN_SCORE, NORMAL_SKIN_SCORE,
            ACNE_PRONE_SCORE,
            PREGNANCY_SAFE, FUNGAL_ACNE_SAFE, COMEDOGENIC_RATING,
            VEGAN, CRUELTY_FREE, REEF_SAFE,
            FRAGRANCE_FREE, ALCOHOL_FREE, PARABEN_FREE,
            SULFATE_FREE, SILICONE_FREE, OIL_FREE,
            SOURCE
        FROM GLOWPIPE_DB.GOLD.VIEW_RECOMMENDATION_PRODUCTS
    """
    with get_connection() as conn:
        _products_df = pd.read_sql(query, conn)
    _products_df.columns = [c.lower() for c in _products_df.columns]
    logger.info(f"Loaded {len(_products_df)} products from Snowflake")
    return _products_df


def load_ingredients() -> pd.DataFrame:
    global _ingredients_df
    if _ingredients_df is not None:
        return _ingredients_df

    query = """
        SELECT
            INGREDIENT_ID, INGREDIENT_NAME, DESCRIPTION,
            EVIDENCE_LEVEL, SCIENCE_TAGS, WARNING_TYPE, CALLOUT_TYPE, SOURCE
        FROM GLOWPIPE_DB.GOLD.DIM_INGREDIENTS
    """
    with get_connection() as conn:
        _ingredients_df = pd.read_sql(query, conn)
    _ingredients_df.columns = [c.lower() for c in _ingredients_df.columns]
    return _ingredients_df


def load_fact() -> pd.DataFrame:
    global _fact_df
    if _fact_df is not None:
        return _fact_df

    query = """
        SELECT
            FACT_ID, PRODUCT_ID, INGREDIENT_ID,
            INGREDIENT_POSITION, IS_TOP5, IS_TOP10,
            IS_ACTIVE_ZONE, PRODUCT_RATING, PRODUCT_PRICE,
            SOURCE, CREATED_AT
        FROM GLOWPIPE_DB.GOLD.FACT_PRODUCT_INGREDIENT_ANALYSIS
    """
    with get_connection() as conn:
        _fact_df = pd.read_sql(query, conn)
    _fact_df.columns = [c.lower() for c in _fact_df.columns]
    return _fact_df


def reload_cache():
    """Force reload all cached DataFrames."""
    global _products_df, _ingredients_df, _fact_df
    _products_df = None
    _ingredients_df = None
    _fact_df = None