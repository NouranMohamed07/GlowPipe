"""
Final Data Model Audit
=======================
Complete integrity check across all three tables:
  - products        (final_combinedlast.csv)
  - ingredients     (ingredients_updatedlast.csv)
  - bridge          (bridge_finallast.csv)

Checks:
  1.  Required columns exist
  2.  Null IDs
  3.  PK uniqueness (products + ingredients)
  4.  Bridge unique pairs (product_id + ingredient_id)
  5.  FK bridge.product_id    → products
  6.  FK bridge.ingredient_id → ingredients
  7.  ID format (all must be 16-char)
  8.  ID type consistency (no mixed int/str)
  9.  Product coverage (by source)
  10. Ingredient coverage
  11. Ingredient name consistency (bridge vs ingredients table)
  12. Product name consistency (bridge vs products table)
  13. Source column check (no nulls, known values only)
  14. Bridge composition by source
  15. Overall summary counts

Outputs:
  - audit_report.txt                   → full report saved to file
  - audit_invalid_product_refs.csv     → FK violations on product_id  (if any)
  - audit_invalid_ingredient_refs.csv  → FK violations on ingredient_id (if any)
  - audit_name_mismatches.csv          → name mismatches bridge vs tables (if any)
  - audit_products_no_bridge.csv       → products with no bridge rows
"""

import pandas as pd

# ── CONFIG ────────────────────────────────────────────────────────────────────
PRODUCTS_FILE    = "final_combinedlast.csv"
INGREDIENTS_FILE = "ingredients_updatedlast.csv"
BRIDGE_FILE      = "bridge_finallast.csv"

OUTPUT_REPORT          = "audit_report.txt"
OUTPUT_BAD_PROD_REFS   = "audit_invalid_product_refs.csv"
OUTPUT_BAD_ING_REFS    = "audit_invalid_ingredient_refs.csv"
OUTPUT_NAME_MISMATCHES = "audit_name_mismatches.csv"
OUTPUT_NO_BRIDGE       = "audit_products_no_bridge.csv"

EXPECTED_ID_LENGTH  = 16
KNOWN_SOURCES       = {'skincarisma', 'datasheet', 'dermstore',
                       'cosmetics', 'skincare_products', 'extracted_from_products'}


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():

    lines  = []
    errors = []
    warns  = []

    def log(msg=''):
        print(msg)
        lines.append(str(msg))

    def err(msg):
        log(f"  ❌ {msg}")
        errors.append(msg)

    def ok(msg):
        log(f"  ✅ {msg}")

    def info(msg):
        log(f"  ℹ️  {msg}")

    def warn(msg):
        log(f"  ⚠️  {msg}")
        warns.append(msg)

    # ── Load ──────────────────────────────────────────────────────────────────
    log("Loading tables ...")
    prod   = pd.read_csv(PRODUCTS_FILE,    low_memory=False)
    ing    = pd.read_csv(INGREDIENTS_FILE, low_memory=False)
    bridge = pd.read_csv(BRIDGE_FILE,      low_memory=False)

    log(f"  Products     : {len(prod):,} rows  |  {len(prod.columns)} columns")
    log(f"  Ingredients  : {len(ing):,} rows  |  {len(ing.columns)} columns")
    log(f"  Bridge       : {len(bridge):,} rows  |  {len(bridge.columns)} columns")

    # Pre-build lookup sets
    prod_ids = set(prod['product_id'].dropna().astype(str))
    ing_ids  = set(ing['ingredient_id'].dropna().astype(str))
    bridge_prod_ids = set(bridge['product_id'].dropna().astype(str))
    bridge_ing_ids  = set(bridge['ingredient_id'].dropna().astype(str))

    # ── CHECK 1: Required columns ──────────────────────────────────────────────
    log("\n" + "=" * 70)
    log("CHECK 1 — REQUIRED COLUMNS")
    log("=" * 70)

    required = {
        "Products"   : (prod,   {"product_id", "product_name", "source"}),
        "Ingredients": (ing,    {"ingredient_id", "ingredient_name", "source"}),
        "Bridge"     : (bridge, {"product_id", "ingredient_id",
                                  "product_name", "ingredient_name", "source"}),
    }
    for name, (df, cols) in required.items():
        missing = cols - set(df.columns)
        if missing:
            err(f"{name}: missing columns {missing}")
        else:
            ok(f"{name}: all required columns present")

    # ── CHECK 2: Null IDs ──────────────────────────────────────────────────────
    log("\n" + "=" * 70)
    log("CHECK 2 — NULL IDS")
    log("=" * 70)

    null_checks = {
        "products.product_id"      : prod["product_id"].isna().sum(),
        "ingredients.ingredient_id": ing["ingredient_id"].isna().sum(),
        "bridge.product_id"        : bridge["product_id"].isna().sum(),
        "bridge.ingredient_id"     : bridge["ingredient_id"].isna().sum(),
    }
    for label, cnt in null_checks.items():
        if cnt == 0:
            ok(f"{label:<35}: {cnt:,} nulls")
        else:
            err(f"{label:<35}: {cnt:,} nulls found")

    # ── CHECK 3: PK uniqueness ─────────────────────────────────────────────────
    log("\n" + "=" * 70)
    log("CHECK 3 — PRIMARY KEY UNIQUENESS")
    log("=" * 70)

    prod_dup = prod["product_id"].duplicated().sum()
    ing_dup  = ing["ingredient_id"].duplicated().sum()

    if prod_dup == 0:
        ok(f"products.product_id: no duplicates")
    else:
        err(f"products.product_id: {prod_dup:,} duplicate values")

    if ing_dup == 0:
        ok(f"ingredients.ingredient_id: no duplicates")
    else:
        err(f"ingredients.ingredient_id: {ing_dup:,} duplicate values")

    # ── CHECK 4: Bridge unique pairs ───────────────────────────────────────────
    log("\n" + "=" * 70)
    log("CHECK 4 — BRIDGE UNIQUE PAIRS (product_id + ingredient_id)")
    log("=" * 70)

    bridge_dup = bridge.duplicated(subset=["product_id", "ingredient_id"]).sum()
    if bridge_dup == 0:
        ok("bridge unique on (product_id + ingredient_id)")
    else:
        err(f"bridge: {bridge_dup:,} duplicate (product_id, ingredient_id) pairs")

    # ── CHECK 5: FK bridge.product_id → products ───────────────────────────────
    log("\n" + "=" * 70)
    log("CHECK 5 — FK bridge.product_id → products")
    log("=" * 70)

    invalid_prod = bridge[~bridge["product_id"].astype(str).isin(prod_ids)]
    if len(invalid_prod) == 0:
        ok(f"all bridge product_id values exist in products table")
    else:
        err(f"{len(invalid_prod):,} bridge rows reference a product_id not in products table")
        log(f"    Sample invalid product_ids: {invalid_prod['product_id'].unique()[:5].tolist()}")

    # ── CHECK 6: FK bridge.ingredient_id → ingredients ────────────────────────
    log("\n" + "=" * 70)
    log("CHECK 6 — FK bridge.ingredient_id → ingredients")
    log("=" * 70)

    invalid_ing = bridge[~bridge["ingredient_id"].astype(str).isin(ing_ids)]
    if len(invalid_ing) == 0:
        ok("all bridge ingredient_id values exist in ingredients table")
    else:
        err(f"{len(invalid_ing):,} bridge rows reference an ingredient_id not in ingredients table")
        log(f"    Sample invalid ingredient_ids: {invalid_ing['ingredient_id'].unique()[:5].tolist()}")

    # ── CHECK 7: ID format (all 16-char) ──────────────────────────────────────
    log("\n" + "=" * 70)
    log("CHECK 7 — ID FORMAT (all must be 16-char)")
    log("=" * 70)

    id_series = {
        "products.product_id"      : prod["product_id"].dropna().astype(str),
        "ingredients.ingredient_id": ing["ingredient_id"].dropna().astype(str),
        "bridge.product_id"        : bridge["product_id"].dropna().astype(str),
        "bridge.ingredient_id"     : bridge["ingredient_id"].dropna().astype(str),
    }
    for label, series in id_series.items():
        lengths = sorted(series.apply(len).unique())
        all_ok  = all(l == EXPECTED_ID_LENGTH for l in lengths)
        if all_ok:
            ok(f"{label:<35}: {lengths} ✓")
        else:
            err(f"{label:<35}: mixed lengths {lengths} — expected all {EXPECTED_ID_LENGTH}")

    # ── CHECK 8: ID type consistency ───────────────────────────────────────────
    log("\n" + "=" * 70)
    log("CHECK 8 — ID TYPE CONSISTENCY")
    log("=" * 70)

    for label, series in [
        ("products.product_id",       prod["product_id"]),
        ("ingredients.ingredient_id", ing["ingredient_id"]),
        ("bridge.product_id",         bridge["product_id"]),
        ("bridge.ingredient_id",      bridge["ingredient_id"]),
    ]:
        dtype = series.dtype
        if str(dtype) in ('object', 'string'):
            ok(f"{label:<35}: dtype=string ✓")
        else:
            warn(f"{label:<35}: dtype={dtype} — IDs should be stored as string, not {dtype}")

    # ── CHECK 9: Product coverage ──────────────────────────────────────────────
    log("\n" + "=" * 70)
    log("CHECK 9 — PRODUCT COVERAGE")
    log("=" * 70)

    unref_prods = prod[~prod["product_id"].astype(str).isin(bridge_prod_ids)]
    covered_pct = 100 * (len(prod) - len(unref_prods)) / len(prod)

    log(f"  Total products              : {len(prod):,}")
    log(f"  With bridge rows            : {len(prod) - len(unref_prods):,}  ({covered_pct:.1f}%)")
    log(f"  Without bridge rows         : {len(unref_prods):,}")

    has_ing_list   = unref_prods[unref_prods['ingredients_list'].notna()] if 'ingredients_list' in unref_prods.columns else pd.DataFrame()
    no_ing_list    = unref_prods[unref_prods['ingredients_list'].isna()]  if 'ingredients_list' in unref_prods.columns else unref_prods

    log(f"    → No ingredients_list     : {len(no_ing_list):,}  (no source data — expected)")
    log(f"    → Has list but no match   : {len(has_ing_list):,}  (ingredient names not in table)")

    log(f"\n  Coverage by source:")
    for source, grp in prod.groupby('source'):
        covered = grp[grp['product_id'].astype(str).isin(bridge_prod_ids)]
        pct     = 100 * len(covered) / len(grp)
        missing = len(grp) - len(covered)
        icon    = '✅' if pct == 100 else '⚠️ ' if pct < 95 else 'ℹ️ '
        log(f"    {icon} {source:<25}: {len(covered):>6,}/{len(grp):>6,} ({pct:.1f}%)  | {missing:>5,} missing")

    # ── CHECK 10: Ingredient coverage ──────────────────────────────────────────
    log("\n" + "=" * 70)
    log("CHECK 10 — INGREDIENT COVERAGE")
    log("=" * 70)

    unref_ings  = ing[~ing["ingredient_id"].astype(str).isin(bridge_ing_ids)]
    ref_ings    = len(ing) - len(unref_ings)
    ing_ref_pct = 100 * ref_ings / len(ing)

    info(f"Ingredients referenced in bridge   : {ref_ings:,}/{len(ing):,} ({ing_ref_pct:.1f}%)")
    info(f"Ingredients NOT in any bridge row  : {len(unref_ings):,}  (not a violation)")

    if 'source' in ing.columns:
        log(f"\n  Unreferenced by source:")
        for source, grp in ing.groupby('source'):
            unref = grp[~grp['ingredient_id'].astype(str).isin(bridge_ing_ids)]
            log(f"    {source:<30}: {len(unref):,} unreferenced")

    # ── CHECK 11: ingredient_name consistency ──────────────────────────────────
    log("\n" + "=" * 70)
    log("CHECK 11 — INGREDIENT NAME CONSISTENCY (bridge vs ingredients table)")
    log("=" * 70)

    if 'ingredient_name' in bridge.columns:
        id_to_ing_name = dict(zip(ing['ingredient_id'].astype(str), ing['ingredient_name']))

        bridge['_table_ing_name'] = bridge['ingredient_id'].astype(str).map(id_to_ing_name)
        name_mismatch = bridge[
            bridge['_table_ing_name'].notna() &
            (bridge['ingredient_name'].astype(str).str.strip().str.lower() !=
             bridge['_table_ing_name'].astype(str).str.strip().str.lower())
        ]
        bridge = bridge.drop(columns=['_table_ing_name'])

        if len(name_mismatch) == 0:
            ok("all bridge ingredient_name values match ingredients table")
        else:
            warn(f"{len(name_mismatch):,} bridge rows have ingredient_name that differs from ingredients table")
    else:
        info("ingredient_name column not in bridge — skipping")

    # ── CHECK 12: product_name consistency ─────────────────────────────────────
    log("\n" + "=" * 70)
    log("CHECK 12 — PRODUCT NAME CONSISTENCY (bridge vs products table)")
    log("=" * 70)

    if 'product_name' in bridge.columns:
        id_to_prod_name = dict(zip(prod['product_id'].astype(str), prod['product_name']))

        bridge['_table_prod_name'] = bridge['product_id'].astype(str).map(id_to_prod_name)
        prod_name_mismatch = bridge[
            bridge['_table_prod_name'].notna() &
            (bridge['product_name'].astype(str).str.strip().str.lower() !=
             bridge['_table_prod_name'].astype(str).str.strip().str.lower())
        ]
        bridge = bridge.drop(columns=['_table_prod_name'])

        if len(prod_name_mismatch) == 0:
            ok("all bridge product_name values match products table")
        else:
            warn(f"{len(prod_name_mismatch):,} bridge rows have product_name that differs from products table")
    else:
        info("product_name column not in bridge — skipping")

    # ── CHECK 13: Source column ────────────────────────────────────────────────
    log("\n" + "=" * 70)
    log("CHECK 13 — SOURCE COLUMN")
    log("=" * 70)

    for label, df in [("Products", prod), ("Ingredients", ing), ("Bridge", bridge)]:
        if 'source' not in df.columns:
            warn(f"{label}: no source column")
            continue
        null_src    = df['source'].isna().sum()
        unknown_src = set(df['source'].dropna().unique()) - KNOWN_SOURCES
        if null_src == 0:
            ok(f"{label}: no null source values")
        else:
            warn(f"{label}: {null_src:,} null source values")
        if not unknown_src:
            ok(f"{label}: all source values are known")
        else:
            warn(f"{label}: unknown source values found: {unknown_src}")

    # ── CHECK 14: Bridge composition ───────────────────────────────────────────
    log("\n" + "=" * 70)
    log("CHECK 14 — BRIDGE COMPOSITION BY SOURCE")
    log("=" * 70)

    for source, count in bridge['source'].value_counts().items():
        pct = 100 * count / len(bridge)
        log(f"  {source:<30}: {count:,}  ({pct:.1f}%)")

    # ── CHECK 15: Summary counts ───────────────────────────────────────────────
    log("\n" + "=" * 70)
    log("CHECK 15 — SUMMARY COUNTS")
    log("=" * 70)

    log(f"  Products total         : {len(prod):,}")
    log(f"  Ingredients total      : {len(ing):,}")
    log(f"  Bridge total           : {len(bridge):,}")
    log(f"  Unique products in bridge    : {bridge['product_id'].nunique():,}")
    log(f"  Unique ingredients in bridge : {bridge['ingredient_id'].nunique():,}")
    log(f"  Avg ingredients per product  : {len(bridge) / max(bridge['product_id'].nunique(), 1):.1f}")

    # ── Final verdict ──────────────────────────────────────────────────────────
    log("\n" + "=" * 70)
    if len(errors) == 0 and len(warns) == 0:
        log("✅ OVERALL: FULLY PASSED — no errors, no warnings")
        log("   Safe for Spark / PostgreSQL / Snowflake / Power BI")
    elif len(errors) == 0:
        log(f"⚠️  OVERALL: PASSED WITH WARNINGS ({len(warns)} warning(s))")
        log("   Safe to use but review warnings above")
        for w in warns:
            log(f"   ⚠️  {w}")
    else:
        log(f"❌ OVERALL: FAILED — {len(errors)} error(s), {len(warns)} warning(s)")
        log("   Fix errors before loading to database:")
        for e in errors:
            log(f"   ❌ {e}")
        if warns:
            log("   Also review warnings:")
            for w in warns:
                log(f"   ⚠️  {w}")
    log("=" * 70)

    # ── Save outputs ──────────────────────────────────────────────────────────
    with open(OUTPUT_REPORT, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    print(f"\nSaved: {OUTPUT_REPORT}")

    if len(invalid_prod) > 0:
        invalid_prod.to_csv(OUTPUT_BAD_PROD_REFS, index=False)
        print(f"Saved: {OUTPUT_BAD_PROD_REFS}  ({len(invalid_prod):,} rows)")

    if len(invalid_ing) > 0:
        invalid_ing.to_csv(OUTPUT_BAD_ING_REFS, index=False)
        print(f"Saved: {OUTPUT_BAD_ING_REFS}  ({len(invalid_ing):,} rows)")

    if len(unref_prods) > 0:
        cols = ['product_id', 'product_name', 'source']
        if 'ingredients_list' in unref_prods.columns:
            cols.append('ingredients_list')
        unref_prods[cols].to_csv(OUTPUT_NO_BRIDGE, index=False)
        print(f"Saved: {OUTPUT_NO_BRIDGE}  ({len(unref_prods):,} rows)")


if __name__ == "__main__":
    main()
