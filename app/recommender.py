import numpy as np
import pandas as pd
from typing import List
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics.pairwise import cosine_similarity

from app.database import load_products
from app.schemas import (
    RecommendRequest, RecommendResponse, SimilarResponse,
    ProductRecommendation
)
from app.ingredients import get_top_ingredients
from app.utils import extract_badges, build_reason, safe_float, safe_str

SKIN_SCORE_MAP = {
    "oily": "oily_skin_score",
    "dry": "dry_skin_score",
    "sensitive": "sensitive_skin_score",
    "combination": "combination_skin_score",
    "normal": "normal_skin_score",
}

PREF_COLS = [
    "vegan", "cruelty_free", "fragrance_free", "alcohol_free",
    "paraben_free", "sulfate_free", "silicone_free", "oil_free",
    "reef_safe", "pregnancy_safe", "fungal_acne_safe",
]

SIMILARITY_FEATURES = [
    "price", "rating", "number_of_reviews",
    "oily_skin_score", "dry_skin_score", "sensitive_skin_score",
    "combination_skin_score", "normal_skin_score", "acne_prone_score",
    "pregnancy_safe", "fungal_acne_safe", "comedogenic_rating",
    "vegan", "cruelty_free", "reef_safe",
    "fragrance_free", "alcohol_free", "paraben_free",
    "sulfate_free", "silicone_free", "oil_free",
]


def _to_bool_int(val) -> int:
    if isinstance(val, bool):
        return int(val)
    if isinstance(val, (int, float)):
        return 1 if val == 1 else 0
    if isinstance(val, str):
        return 1 if val.lower() in ("1", "true", "yes") else 0
    return 0


def _normalize(series: pd.Series) -> pd.Series:
    mn, mx = series.min(), series.max()
    if mx == mn:
        return pd.Series(np.zeros(len(series)), index=series.index)
    return (series - mn) / (mx - mn)


def _safety_score(df: pd.DataFrame) -> pd.Series:
    """Score = 0.5*(1 - comedogenic_norm) + 0.25*pregnancy_safe + 0.25*fungal_acne_safe"""
    com = df["comedogenic_rating"].fillna(df["comedogenic_rating"].median())
    com_norm = _normalize(com)
    preg = df["pregnancy_safe"].fillna(0).apply(_to_bool_int)
    fa = df["fungal_acne_safe"].fillna(0).apply(_to_bool_int)
    return 0.50 * (1 - com_norm) + 0.25 * preg + 0.25 * fa


def _preference_match(df: pd.DataFrame, prefs: dict) -> tuple[pd.Series, pd.DataFrame]:
    """Return (score_series, matched_cols_df) based on active preferences."""
    active = {k: v for k, v in prefs.items() if v}
    if not active:
        return pd.Series(np.ones(len(df)), index=df.index), pd.DataFrame(index=df.index)

    matches = pd.DataFrame(index=df.index)
    for col in active.keys():
        if col in df.columns:
            matches[col] = df[col].fillna(0).apply(_to_bool_int)

    if matches.empty:
        return pd.Series(np.zeros(len(df)), index=df.index), matches

    score = matches.mean(axis=1)
    return score, matches


def _affordability_score(df: pd.DataFrame, max_budget: float) -> pd.Series:
    """Higher score = cheaper within budget."""
    prices = df["price"].fillna(max_budget)
    return 1 - (prices / max_budget).clip(0, 1)


def _build_product_rec(
    row: pd.Series,
    final_score: float,
    skin_match: float,
    safety: float,
    pref_match: float,
    matched_prefs: List[str],
    skin_type: str,
    acne_prone: bool,
    max_budget: float,
) -> ProductRecommendation:
    badges = extract_badges(row)
    reason = build_reason(
        skin_type, acne_prone, matched_prefs,
        safe_float(row.get("rating")),
        safe_float(row.get("comedogenic_rating")),
        safe_float(row.get("price")),
        max_budget,
    )
    ingredients = get_top_ingredients(str(row["product_id"]), top_n=3)

    return ProductRecommendation(
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
        number_of_reviews=int(row["number_of_reviews"]) if pd.notna(row.get("number_of_reviews")) else None,
        final_score=round(float(final_score), 4),
        skin_match_score=round(float(skin_match), 4),
        safety_score=round(float(safety), 4),
        preference_match_score=round(float(pref_match), 4),
        matched_preferences=matched_prefs,
        badges=badges,
        reason=reason,
        top_3_ingredients=ingredients,
    )


def recommend(request: RecommendRequest) -> RecommendResponse:
    df = load_products().copy()
    prefs_dict = request.preferences.model_dump()

    # ── Filter ──────────────────────────────────────────────
    df = df[df["price"] <= request.max_budget].copy()

    # Category filter
    if request.category:
        df = df[df["category"].str.lower() == request.category.lower()]

    # Sub-category filter
    if request.sub_category:
        df = df[df["sub_category"].str.lower() == request.sub_category.lower()]

    for col, val in prefs_dict.items():
        if val and col in df.columns:
            df = df[df[col].fillna(0).apply(_to_bool_int) == 1]

    total_candidates = len(df)

    if df.empty:
        return RecommendResponse(
            skin_type=request.skin_type,
            acne_prone=request.acne_prone,
            max_budget=request.max_budget,
            total_candidates=0,
            recommendations=[],
        )

    # ── Skin score ───────────────────────────────────────────
    skin_col = SKIN_SCORE_MAP[request.skin_type]
    skin_scores = df[skin_col].fillna(0)

    if request.acne_prone:
        acne_scores = df["acne_prone_score"].fillna(0)
        combined = (skin_scores + acne_scores) / 2
    else:
        combined = skin_scores

    normalized_skin = combined / 5.0

    # ── Rating ───────────────────────────────────────────────
    normalized_rating = df["rating"].fillna(0) / 5.0

    # ── Review confidence ────────────────────────────────────
    log_reviews = np.log1p(df["number_of_reviews"].fillna(0))
    normalized_reviews = _normalize(log_reviews)

    # ── Safety ───────────────────────────────────────────────
    safety = _safety_score(df)

    # ── Preference match ─────────────────────────────────────
    pref_score, match_cols = _preference_match(df, prefs_dict)

    # ── Affordability ────────────────────────────────────────
    afford = _affordability_score(df, request.max_budget)

    # ── Final score ──────────────────────────────────────────
    final = (
        0.35 * normalized_skin
        + 0.20 * normalized_rating
        + 0.15 * normalized_reviews
        + 0.15 * safety
        + 0.10 * pref_score
        + 0.05 * afford
    )

    df = df.copy()
    df["_final_score"] = final.values
    df["_skin_match"] = normalized_skin.values
    df["_safety"] = safety.values
    df["_pref_match"] = pref_score.values

    df = df.sort_values("_final_score", ascending=False).head(request.top_n)

    recommendations = []
    active_prefs = [k for k, v in prefs_dict.items() if v]

    for _, row in df.iterrows():
        if match_cols.empty or not active_prefs:
            matched = []
        else:
            matched = [
                col.replace("_", " ").title()
                for col in active_prefs
                if col in match_cols.columns and match_cols.at[row.name, col] == 1
            ]

        rec = _build_product_rec(
            row=row,
            final_score=row["_final_score"],
            skin_match=row["_skin_match"],
            safety=row["_safety"],
            pref_match=row["_pref_match"],
            matched_prefs=matched,
            skin_type=request.skin_type,
            acne_prone=request.acne_prone,
            max_budget=request.max_budget,
        )
        recommendations.append(rec)

    return RecommendResponse(
        skin_type=request.skin_type,
        acne_prone=request.acne_prone,
        max_budget=request.max_budget,
        total_candidates=total_candidates,
        recommendations=recommendations,
    )


def find_similar(product_id: str, top_n: int) -> SimilarResponse:
    df = load_products().copy()

    avail_features = [f for f in SIMILARITY_FEATURES if f in df.columns]
    feature_df = df[avail_features].copy()

    for col in avail_features:
        feature_df[col] = pd.to_numeric(feature_df[col], errors="coerce").fillna(0)

    scaler = MinMaxScaler()
    scaled = scaler.fit_transform(feature_df)

    product_idx = df.index[df["product_id"] == product_id].tolist()
    if not product_idx:
        return SimilarResponse(source_product_id=product_id, similar_products=[])

    idx = product_idx[0]
    source_vec = scaled[df.index.get_loc(idx)].reshape(1, -1)
    sims = cosine_similarity(source_vec, scaled)[0]

    # Exclude self
    sim_series = pd.Series(sims, index=df.index)
    sim_series = sim_series.drop(idx).nlargest(top_n)

    similar_rows = df.loc[sim_series.index]

    # Build ProductRecommendation for similar products (no user context)
    results = []
    for _, row in similar_rows.iterrows():
        ing = get_top_ingredients(str(row["product_id"]), top_n=3)
        badges = extract_badges(row)
        results.append(ProductRecommendation(
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
            number_of_reviews=int(row["number_of_reviews"]) if pd.notna(row.get("number_of_reviews")) else None,
            final_score=round(float(sim_series.loc[row.name]), 4),
            skin_match_score=0.0,
            safety_score=0.0,
            preference_match_score=0.0,
            matched_preferences=[],
            badges=badges,
            reason="Similar product based on profile features.",
            top_3_ingredients=ing,
        ))

    return SimilarResponse(source_product_id=product_id, similar_products=results)