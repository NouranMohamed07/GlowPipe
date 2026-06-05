from pydantic import BaseModel, Field
from typing import Optional, List


# ── Request schemas ─────────────────────────────────────────

class Preferences(BaseModel):
    vegan: bool = False
    cruelty_free: bool = False
    fragrance_free: bool = False
    alcohol_free: bool = False
    paraben_free: bool = False
    sulfate_free: bool = False
    silicone_free: bool = False
    oil_free: bool = False
    reef_safe: bool = False
    pregnancy_safe: bool = False
    fungal_acne_safe: bool = False


class RecommendRequest(BaseModel):
    skin_type: str = Field(..., pattern="^(oily|dry|sensitive|combination|normal)$")
    acne_prone: bool = False
    max_budget: float = Field(..., gt=0, le=500)
    preferences: Preferences = Preferences()
    top_n: int = Field(default=10, ge=1, le=50)
    # Optional category filters
    category: Optional[str] = None
    sub_category: Optional[str] = None


class SimilarRequest(BaseModel):
    product_id: str
    top_n: int = Field(default=10, ge=1, le=50)


# ── Response schemas ─────────────────────────────────────────

class IngredientDetail(BaseModel):
    ingredient_id: Optional[str] = None
    ingredient_name: str
    ingredient_position: int
    description: str = "No detailed description available."
    evidence_level: Optional[str] = None
    science_tags: Optional[str] = None
    warning_type: Optional[str] = None
    callout_type: Optional[str] = None


class ProductRecommendation(BaseModel):
    product_id: str
    product_url: Optional[str] = None
    product_name: str
    brand_name: Optional[str] = None
    category: Optional[str] = None
    sub_category: Optional[str] = None
    image_url: Optional[str] = None
    price: float
    currency: Optional[str] = "USD"
    size: Optional[str] = None
    rating: Optional[float] = None
    number_of_reviews: Optional[int] = None
    final_score: float
    skin_match_score: float
    safety_score: float
    preference_match_score: float
    matched_preferences: List[str]
    badges: List[str]
    reason: str
    top_3_ingredients: List[IngredientDetail]


class RecommendResponse(BaseModel):
    skin_type: str
    acne_prone: bool
    max_budget: float
    total_candidates: int
    recommendations: List[ProductRecommendation]


class ProductDetail(BaseModel):
    product_id: str
    product_url: Optional[str] = None
    product_name: str
    brand_name: Optional[str] = None
    category: Optional[str] = None
    sub_category: Optional[str] = None
    image_url: Optional[str] = None
    price: float
    currency: Optional[str] = "USD"
    size: Optional[str] = None
    rating: Optional[float] = None
    number_of_reviews: Optional[int] = None
    oily_skin_score: Optional[float] = None
    dry_skin_score: Optional[float] = None
    sensitive_skin_score: Optional[float] = None
    combination_skin_score: Optional[float] = None
    normal_skin_score: Optional[float] = None
    acne_prone_score: Optional[float] = None
    pregnancy_safe: Optional[bool] = None
    fungal_acne_safe: Optional[bool] = None
    comedogenic_rating: Optional[float] = None
    vegan: Optional[bool] = None
    cruelty_free: Optional[bool] = None
    reef_safe: Optional[bool] = None
    fragrance_free: Optional[bool] = None
    alcohol_free: Optional[bool] = None
    paraben_free: Optional[bool] = None
    sulfate_free: Optional[bool] = None
    silicone_free: Optional[bool] = None
    oil_free: Optional[bool] = None
    badges: List[str]
    top_ingredients: List[IngredientDetail]


class HealthResponse(BaseModel):
    status: str
    snowflake_connected: bool
    product_count: int
    message: str


class IngredientSearchResult(BaseModel):
    ingredient_id: Optional[str] = None
    ingredient_name: str
    description: str = "No detailed description available."
    evidence_level: Optional[str] = None
    science_tags: Optional[str] = None
    warning_type: Optional[str] = None
    callout_type: Optional[str] = None
    product_count: int


class SimilarResponse(BaseModel):
    source_product_id: str
    similar_products: List[ProductRecommendation]