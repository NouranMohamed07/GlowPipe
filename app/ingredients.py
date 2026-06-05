import pandas as pd
from typing import List, Optional
from app.database import load_ingredients, load_fact
from app.schemas import IngredientDetail, IngredientSearchResult
from app.utils import safe_str


def _enrich_ingredient(fact_row: pd.Series, ing_row: Optional[pd.Series]) -> IngredientDetail:
    if ing_row is not None:
        desc = safe_str(ing_row.get("description"))
        return IngredientDetail(
            ingredient_id=safe_str(ing_row.get("ingredient_id")),
            ingredient_name=safe_str(ing_row.get("ingredient_name"), "Unknown"),
            ingredient_position=int(fact_row["ingredient_position"]),
            description=desc if desc else "No detailed description available.",
            evidence_level=safe_str(ing_row.get("evidence_level")),
            science_tags=safe_str(ing_row.get("science_tags")),
            warning_type=safe_str(ing_row.get("warning_type")),
            callout_type=safe_str(ing_row.get("callout_type")),
        )
    return IngredientDetail(
        ingredient_id=None,
        ingredient_name="Unknown Ingredient",
        ingredient_position=int(fact_row["ingredient_position"]),
        description="No detailed description available.",
    )


def get_top_ingredients(product_id: str, top_n: int = 3) -> List[IngredientDetail]:
    fact_df = load_fact()
    ing_df = load_ingredients()

    product_facts = fact_df[fact_df["product_id"] == product_id].copy()
    product_facts = product_facts.sort_values("ingredient_position")
    product_facts = product_facts.head(top_n)

    result = []
    for _, row in product_facts.iterrows():
        ing_id = row.get("ingredient_id")
        ing_row = None
        if ing_id is not None:
            match = ing_df[ing_df["ingredient_id"] == ing_id]
            if not match.empty:
                ing_row = match.iloc[0]
        result.append(_enrich_ingredient(row, ing_row))

    return result


def get_all_ingredients(product_id: str) -> List[IngredientDetail]:
    fact_df = load_fact()
    ing_df = load_ingredients()

    product_facts = fact_df[fact_df["product_id"] == product_id].copy()
    product_facts = product_facts.sort_values("ingredient_position")

    result = []
    for _, row in product_facts.iterrows():
        ing_id = row.get("ingredient_id")
        ing_row = None
        if ing_id is not None:
            match = ing_df[ing_df["ingredient_id"] == ing_id]
            if not match.empty:
                ing_row = match.iloc[0]
        result.append(_enrich_ingredient(row, ing_row))

    return result


def search_ingredients(query: str) -> List[IngredientSearchResult]:
    ing_df = load_ingredients()
    fact_df = load_fact()

    mask = ing_df["ingredient_name"].str.contains(query, case=False, na=False)
    matched = ing_df[mask].copy()

    results = []
    for _, ing in matched.iterrows():
        ing_id = ing["ingredient_id"]
        product_count = len(fact_df[fact_df["ingredient_id"] == ing_id]["product_id"].unique())
        desc = safe_str(ing.get("description"))
        results.append(IngredientSearchResult(
            ingredient_id=safe_str(ing_id),
            ingredient_name=safe_str(ing["ingredient_name"], "Unknown"),
            description=desc if desc else "No detailed description available.",
            evidence_level=safe_str(ing.get("evidence_level")),
            science_tags=safe_str(ing.get("science_tags")),
            warning_type=safe_str(ing.get("warning_type")),
            callout_type=safe_str(ing.get("callout_type")),
            product_count=product_count,
        ))

    return results