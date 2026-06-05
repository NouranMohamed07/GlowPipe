import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from app.database import load_products, load_ingredients, load_fact, test_connection
from app.schemas import (
    RecommendRequest, RecommendResponse,
    SimilarRequest, SimilarResponse,
    ProductDetail, HealthResponse, IngredientSearchResult,
    IngredientDetail,
)
from app.recommender import recommend, find_similar
from app.ingredients import get_top_ingredients, get_all_ingredients, search_ingredients
from app.utils import extract_badges, safe_float, safe_str

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Loading data from Snowflake...")
    try:
        load_products()
        load_ingredients()
        load_fact()
        logger.info("Data loaded successfully.")
    except Exception as e:
        logger.error(f"Failed to load data: {e}")
    yield


app = FastAPI(
    title="GlowPipe Skincare Recommender API",
    description="Hybrid content-based skincare recommendation API powered by Snowflake",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── 1. Root ───────────────────────────────────────────────────────────────────

@app.get("/")
def root():
    return {
        "api": "GlowPipe Skincare Recommender",
        "version": "1.0.0",
        "status": "running",
        "docs": "/docs",
    }


# ── 2. Health ────────────────────────────────────────────────────────────────

@app.get("/health", response_model=HealthResponse)
def health():
    connected = test_connection()
    try:
        products = load_products()
        count = len(products)
    except Exception:
        count = 0
    return HealthResponse(
        status="healthy" if connected else "degraded",
        snowflake_connected=connected,
        product_count=count,
        message="All systems operational." if connected else "Snowflake connection issue.",
    )


# ── 3. Recommend ─────────────────────────────────────────────────────────────

@app.post("/recommend", response_model=RecommendResponse)
def post_recommend(request: RecommendRequest):
    try:
        return recommend(request)
    except Exception as e:
        logger.exception("Recommendation error")
        raise HTTPException(status_code=500, detail=str(e))


# ── 4. Product detail ────────────────────────────────────────────────────────

@app.get("/products/{product_id}", response_model=ProductDetail)
def get_product(product_id: str):
    df = load_products()
    match = df[df["product_id"] == product_id]
    if match.empty:
        raise HTTPException(status_code=404, detail=f"Product '{product_id}' not found.")

    row = match.iloc[0]
    badges = extract_badges(row)
    ingredients = get_top_ingredients(product_id, top_n=10)

    return ProductDetail(
        product_id=str(row["product_id"]),
        product_url=safe_str(row.get("product_url")),
        product_name=safe_str(row.get("product_name"), "Unknown Product"),
        brand_name=safe_str(row.get("brand_name")),
        category=safe_str(row.get("category")),
        sub_category=safe_str(row.get("sub_category")),
        image_url=safe_str(row.get("image_url")),
        price=safe_float(row.get("price"), 0.0),
        currency=safe_str(row.get("currency"), "USD"),
        size=safe_str(row.get("size")),
        rating=safe_float(row.get("rating")),
        number_of_reviews=int(row["number_of_reviews"]) if row.get("number_of_reviews") is not None else None,
        oily_skin_score=safe_float(row.get("oily_skin_score")),
        dry_skin_score=safe_float(row.get("dry_skin_score")),
        sensitive_skin_score=safe_float(row.get("sensitive_skin_score")),
        combination_skin_score=safe_float(row.get("combination_skin_score")),
        normal_skin_score=safe_float(row.get("normal_skin_score")),
        acne_prone_score=safe_float(row.get("acne_prone_score")),
        pregnancy_safe=bool(row.get("pregnancy_safe")),
        fungal_acne_safe=bool(row.get("fungal_acne_safe")),
        comedogenic_rating=safe_float(row.get("comedogenic_rating")),
        vegan=bool(row.get("vegan")),
        cruelty_free=bool(row.get("cruelty_free")),
        reef_safe=bool(row.get("reef_safe")),
        fragrance_free=bool(row.get("fragrance_free")),
        alcohol_free=bool(row.get("alcohol_free")),
        paraben_free=bool(row.get("paraben_free")),
        sulfate_free=bool(row.get("sulfate_free")),
        silicone_free=bool(row.get("silicone_free")),
        oil_free=bool(row.get("oil_free")),
        badges=badges,
        top_ingredients=ingredients,
    )


# ── 5. Similar products ──────────────────────────────────────────────────────

@app.post("/similar-products", response_model=SimilarResponse)
def post_similar(request: SimilarRequest):
    try:
        return find_similar(request.product_id, request.top_n)
    except Exception as e:
        logger.exception("Similar products error")
        raise HTTPException(status_code=500, detail=str(e))


# ── 6. Ingredient search ─────────────────────────────────────────────────────

@app.get("/ingredients/search", response_model=list[IngredientSearchResult])
def ingredient_search(ingredient_name: str = Query(..., min_length=2)):
    try:
        return search_ingredients(ingredient_name)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── 7. Product ingredients ───────────────────────────────────────────────────

@app.get("/products/{product_id}/ingredients", response_model=list[IngredientDetail])
def product_ingredients(product_id: str):
    df = load_products()
    if df[df["product_id"] == product_id].empty:
        raise HTTPException(status_code=404, detail=f"Product '{product_id}' not found.")
    return get_all_ingredients(product_id)