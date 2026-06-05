import os
import re
import json
import time
import random
import hashlib
import logging
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from bs4 import BeautifulSoup


# =========================
# Paths / Config
# =========================
BASE_URL   = "https://www.skincarisma.com"
INPUT_CSV  = "data/raw/source=skincarisma/product_links.csv"
OUTPUT_DIR = "data/raw/source=skincarisma"
LOG_DIR    = "logs"

PRODUCTS_CSV          = os.path.join(OUTPUT_DIR, "products.csv")
INGREDIENTS_CSV       = os.path.join(OUTPUT_DIR, "ingredients.csv")
PRODUCT_INGREDIENTS_CSV = os.path.join(OUTPUT_DIR, "product_ingredients.csv")
FEATURES_CSV          = os.path.join(OUTPUT_DIR, "recommendation_features.csv")
FAILED_CSV            = os.path.join(OUTPUT_DIR, "failed_product_pages.csv")
LOG_FILE              = os.path.join(LOG_DIR,    "skincarisma_scraper.log")

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR,    exist_ok=True)

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
    "referer": "https://www.skincarisma.com/collections/all",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
}


# =========================
# Helpers
# =========================
def clean_text(value):
    if value is None:
        return None
    value = BeautifulSoup(str(value), "html.parser").get_text(" ", strip=True)
    value = re.sub(r"\s+", " ", value).strip()
    return value if value else None


def safe_lower(value):
    return str(value).lower().strip() if value else ""


def normalize_url(url):
    if not url:
        return None
    url = str(url).strip()
    if url.startswith("//"):
        return "https:" + url
    if url.startswith("/"):
        return BASE_URL + url
    return url


def make_id(text):
    return hashlib.md5(str(text or "").strip().lower().encode()).hexdigest()


def fetch_html(url, retries=3, timeout=25):
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            if r.status_code == 200 and len(r.text) > 500:
                return r.text
            logging.warning(f"Bad response {r.status_code} for {url}")
        except Exception as exc:
            logging.warning(f"Attempt {attempt} failed for {url}: {exc}")
        time.sleep(1.5 * attempt)
    return None


# =========================
# JSON-LD helpers
# =========================
def extract_all_jsonld(soup):
    """Return all parsed JSON-LD objects from ALL script tags on page."""
    objects = []
    for script in soup.select('script[type="application/ld+json"]'):
        raw = script.string or script.get_text(strip=True)
        if not raw:
            continue
        try:
            obj = json.loads(raw)
            # unwrap @graph arrays
            if isinstance(obj, dict) and "@graph" in obj:
                objects.extend(obj["@graph"])
            elif isinstance(obj, list):
                objects.extend(obj)
            else:
                objects.append(obj)
        except Exception:
            continue
    return objects


def find_product_jsonld(json_objects):
    """Find the Product-type JSON-LD object."""
    for obj in json_objects:
        if not isinstance(obj, dict):
            continue
        t = obj.get("@type", "")
        types = t if isinstance(t, list) else [t]
        if "Product" in types:
            return obj
    return None


def find_itemlist_ingredients(json_objects):
    """Find ItemList of ingredients from JSON-LD."""
    rows = []
    for obj in json_objects:
        if not isinstance(obj, dict):
            continue
        if obj.get("@type") == "ItemList":
            name = safe_lower(obj.get("name", ""))
            if "ingredient" in name or "inci" in name:
                for item in (obj.get("itemListElement") or []):
                    pos = item.get("position")
                    ing_name = clean_text(item.get("name"))
                    if ing_name:
                        rows.append((pos, ing_name))
    return rows


def extract_breadcrumb_names(json_objects):
    """e.g. ['Home', 'Skincare', 'Moisturizers', \"'H' Skin Healing Gel\"]"""
    for obj in json_objects:
        if not isinstance(obj, dict):
            continue
        if obj.get("@type") == "BreadcrumbList":
            items = sorted(
                obj.get("itemListElement", []),
                key=lambda x: x.get("position", 99),
            )
            return [clean_text(i.get("name", "")) for i in items if i.get("name")]
    return []


# =========================
# Product detection
# =========================
def is_fake_or_hidden_product(soup, product_json, card_row):
    brand = safe_lower(
        (product_json or {}).get("brand", {}).get("name", "")
        or card_row.get("brand_name", "")
    )
    image = safe_lower((product_json or {}).get("image", "") or card_row.get("image_url", ""))

    if brand == "hidden":
        return True, "hidden_brand"
    if "ingredient_example" in image:
        return True, "ingredient_example_image"

    breadcrumb_text = " ".join(
        clean_text(a.get_text()) or "" for a in soup.select(".breadcrumbs a")
    ).lower()
    if "hidden" in breadcrumb_text:
        return True, "hidden_breadcrumb"

    title_tag = soup.select_one("h1.product__title, .product__title h1, h1")
    if not title_tag or not clean_text(title_tag.get_text()):
        return True, "no_product_title"

    return False, "valid"


# =========================
# Core product meta
# =========================
def parse_product_meta(soup, product_json, card_row, breadcrumb_names):
    # --- Name ---
    product_name = clean_text((product_json or {}).get("name"))
    if not product_name:
        for sel in ["h1.product__title", ".product__title h1", "h1"]:
            t = soup.select_one(sel)
            if t:
                product_name = clean_text(t.get_text(" ", strip=True))
                break
    product_name = product_name or card_row.get("product_name")

    # --- Brand (from Product JSON-LD meta itemprop or card) ---
    brand_name = None
    if product_json:
        b = product_json.get("brand")
        brand_name = clean_text(b.get("name") if isinstance(b, dict) else b)
    if not brand_name:
        # itemprop="brand" meta tag inside .pp div
        meta = soup.select_one('[itemprop="brand"]')
        if meta:
            brand_name = clean_text(meta.get("content") or meta.get_text())
    if not brand_name:
        brand_name = card_row.get("brand_name")

    # --- Category from breadcrumb ---
    category    = None
    sub_category = None
    if len(breadcrumb_names) >= 3:
        category     = breadcrumb_names[1] if safe_lower(breadcrumb_names[1]) != "home" else None
        sub_category = breadcrumb_names[2] if len(breadcrumb_names) >= 4 else None
    if not category:
        cat_meta = soup.select_one('[itemprop="category"]')
        if cat_meta:
            category = clean_text(cat_meta.get("content") or cat_meta.get_text())
    if not category:
        category = card_row.get("category")

    # --- Image ---
    image_url = None
    if product_json:
        img = product_json.get("image")
        image_url = normalize_url(img[0] if isinstance(img, list) else img)
    if not image_url:
        for sel in [".product__media img", ".product__media-item img"]:
            t = soup.select_one(sel)
            if t:
                src = t.get("src") or t.get("data-src", "").split()[0]
                image_url = normalize_url(src)
                break
    if not image_url:
        image_url = card_row.get("image_url")

    # --- Price from Product JSON-LD offers (most reliable) ---
    price    = None
    currency = "USD"
    if product_json:
        offers = product_json.get("offers") or {}
        if isinstance(offers, dict):
            price    = offers.get("price")
            currency = offers.get("priceCurrency", "USD")
    if not price:
        for sel in [".f-price-item--regular", ".price-item--regular", ".product__price"]:
            t = soup.select_one(sel)
            if t:
                m = re.search(r"[\d.]+", (clean_text(t.get_text()) or "").replace(",", ""))
                if m:
                    price = m.group(0)
                    break
    if not price:
        price = card_row.get("price")

    # --- Description from #pp-overview > p.pp-subtitle ---
    description = None
    subtitle = soup.select_one("#pp-overview .pp-subtitle, section#pp-overview p.pp-subtitle")
    if subtitle:
        description = clean_text(subtitle.get_text(" ", strip=True))
    if not description and product_json:
        raw_desc = clean_text(product_json.get("description") or "")
        if raw_desc:
            # grab first real sentence block (before "Jump to section")
            m = re.search(r"Product overview\s+.{0,300}?\n(.+?)(?:\n|Jump to section)", raw_desc, re.S)
            if m:
                description = clean_text(m.group(1))
            else:
                description = raw_desc[:600]

    return {
        "product_name": product_name,
        "brand_name":   brand_name,
        "category":     category,
        "sub_category": sub_category,
        "image_url":    image_url,
        "description":  description,
        "price":        price,
        "currency":     currency,
    }


# =========================
# Scorecard / specs (pp-scorecard + pp-specs)
# =========================
def parse_scorecard(soup):
    """
    Parse the .pp-scorecard block:
      Pregnancy → Yes/No
      Fungal Acne → Yes/No
      Comedogenic → N/5
    """
    result = {
        "pregnancy_safe":    None,
        "fungal_acne_safe":  None,
        "comedogenic_rating": None,
    }

    for item in soup.select(".pp-scorecard .item"):
        lbl = safe_lower(item.select_one(".lbl").get_text() if item.select_one(".lbl") else "")
        val = safe_lower(item.select_one(".val").get_text() if item.select_one(".val") else "")

        if "pregnancy" in lbl:
            result["pregnancy_safe"] = 1 if "yes" in val else (0 if "no" in val else None)
        elif "fungal" in lbl:
            result["fungal_acne_safe"] = 1 if "yes" in val else (0 if "no" in val else None)
        elif "comedogenic" in lbl:
            m = re.search(r"(\d+(?:\.\d+)?)\s*/\s*5", val)
            if m:
                result["comedogenic_rating"] = float(m.group(1))

    return result


def parse_specs(soup):
    """
    Parse .pp-specs block:
      Size, Price, Type, Key Active, Ingredients count
    """
    result = {
        "size":                  None,
        "product_type_text":     None,
        "key_actives_text":      None,
        "ingredients_count":     None,
    }

    for spec in soup.select(".pp-spec"):
        lbl_tag = spec.select_one(".lbl")
        val_tag = spec.select_one(".val")
        if not lbl_tag or not val_tag:
            continue
        lbl = safe_lower(lbl_tag.get_text())
        val = clean_text(val_tag.get_text(" ", strip=True))

        if "size" in lbl:
            result["size"] = val
        elif "type" in lbl:
            result["product_type_text"] = val
        elif "key active" in lbl or "active" in lbl:
            result["key_actives_text"] = val
        elif "ingredient" in lbl:
            m = re.search(r"(\d+)", val or "")
            if m:
                result["ingredients_count"] = int(m.group(1))

    return result


# =========================
# Badges / claims (pp-badges)
# =========================
def parse_badges(soup):
    """
    Parse .pp-badge spans for product claims.
    e.g. "Cruelty-Free", "Pregnancy Safe", "Fungal Acne Safe", "Oil-Free" …
    """
    badges = []
    for b in soup.select(".pp-badge"):
        txt = clean_text(b.get_text(" ", strip=True))
        if txt:
            badges.append(txt)

    low = " ".join(badges).lower()
    return {
        "product_claims":  "|".join(sorted(set(badges))) if badges else None,
        "vegan":           int("vegan" in low),
        "cruelty_free":    int("cruelty-free" in low or "cruelty free" in low),
        "reef_safe":       int("reef safe" in low or "reef-safe" in low),
        "fragrance_free":  int("fragrance-free" in low or "fragrance free" in low),
        "alcohol_free":    int("alcohol-free" in low or "alcohol free" in low),
        "paraben_free":    int("paraben-free" in low or "paraben free" in low),
        "sulfate_free":    int("sulfate-free" in low or "sulfate free" in low),
        "silicone_free":   int("silicone-free" in low or "silicone free" in low),
        "oil_free":        int("oil-free" in low or "oil free" in low),
    }


# =========================
# Quick Product Notes (free-from, allergen labels)
# =========================
def parse_quick_notes(soup):
    """
    Parse .quick-products-container for free-from labels.
    e.g. Paraben-Free, Sulfate-Free, Alcohol-Free …
    """
    labels = []
    for span in soup.select(".quick-products-container span.qn-icon span"):
        txt = clean_text(span.get_text())
        if txt:
            labels.append(txt)
    return "|".join(sorted(set(labels))) if labels else None


# =========================
# Free-from list (.free-list inside accordion)
# =========================
def parse_free_from(soup):
    tag = soup.select_one(".free-list")
    if not tag:
        return None
    txt = clean_text(tag.get_text(" ", strip=True))
    txt = re.sub(r"^FREE FROM:\s*", "", txt, flags=re.I).strip()
    parts = [clean_text(x) for x in re.split(r",\s*|\|\s*", txt) if x.strip()]
    return "|".join(sorted(set(parts))) if parts else None


# =========================
# Skin type scores (.pp-skin-grid)
# =========================
def parse_skin_types(soup):
    """
    Parse .pp-skin-card blocks:
      <div class="type">Oily Skin</div>
      <span class="rate excellent">Excellent</span>
    """
    score_map = {
        "excellent": 5,
        "very good": 4,
        "good":      4,
        "moderate":  3,
        "fair":      2,
        "poor":      1,
        "avoid":     0,
        "caution":   2,
    }
    key_map = {
        "oily":        "oily_skin_score",
        "dry":         "dry_skin_score",
        "sensitive":   "sensitive_skin_score",
        "combination": "combination_skin_score",
        "normal":      "normal_skin_score",
        "acne":        "acne_prone_score",
    }

    result = {
        "oily_skin_score":        None,
        "dry_skin_score":         None,
        "sensitive_skin_score":   None,
        "combination_skin_score": None,
        "normal_skin_score":      None,
        "acne_prone_score":       None,
        "good_for_skin_types":    [],
        "bad_for_skin_types":     [],
        "skin_type_notes":        [],
    }

    for card in soup.select(".pp-skin-card"):
        type_tag = card.select_one(".type")
        rate_tag = card.select_one(".rate")
        if not type_tag or not rate_tag:
            continue

        type_txt = safe_lower(type_tag.get_text())
        rate_txt = safe_lower(rate_tag.get_text())

        score = next((v for k, v in score_map.items() if k in rate_txt), None)
        if score is None:
            continue

        for key, col in key_map.items():
            if key in type_txt:
                result[col] = score
                label = type_tag.get_text(strip=True)
                result["skin_type_notes"].append(f"{label}: {score}/5")
                if score >= 4:
                    result["good_for_skin_types"].append(label)
                elif score <= 2:
                    result["bad_for_skin_types"].append(label)
                break

    result["good_for_skin_types"] = "|".join(result["good_for_skin_types"]) or None
    result["bad_for_skin_types"]  = "|".join(result["bad_for_skin_types"])  or None
    result["skin_type_notes"]     = "|".join(result["skin_type_notes"])     or None
    return result


# =========================
# Safety section (.pp-safety)
# =========================
def parse_safety_section(soup):
    """
    Parse .pp-safety-item divs for pregnancy, fungal acne, comedogenic,
    irritation info.
    """
    result = {
        "pregnancy_safe":    None,
        "fungal_acne_safe":  None,
        "comedogenic_rating": None,
        "irritation_rating": None,
        "safety_notes":      [],
    }

    for item in soup.select(".pp-safety .pp-safety-item"):
        strong = item.select_one("strong")
        p_tag  = item.select_one("p")
        label  = safe_lower(strong.get_text() if strong else "")
        detail = safe_lower(p_tag.get_text()  if p_tag  else "")
        classes = " ".join(item.get("class", []))

        is_safe   = "safe"   in classes
        is_caution = "caution" in classes

        if "pregnancy" in label or "breastfeeding" in label:
            if result["pregnancy_safe"] is None:
                result["pregnancy_safe"] = 1 if is_safe else (0 if is_caution else None)
        if "fungal" in label:
            if result["fungal_acne_safe"] is None:
                result["fungal_acne_safe"] = 1 if is_safe else (0 if is_caution else None)

        m = re.search(r"(\d+(?:\.\d+)?)\s*/\s*5", label + " " + detail)
        if m and result["comedogenic_rating"] is None:
            result["comedogenic_rating"] = float(m.group(1))

        if "non-irritating" in detail or "non irritating" in detail:
            result["irritation_rating"] = 0
        elif "irritating" in detail and result["irritation_rating"] is None:
            result["irritation_rating"] = 1

        if strong and p_tag:
            note = f"{clean_text(strong.get_text())}: {clean_text(p_tag.get_text())}"
            result["safety_notes"].append(note[:300])

    result["safety_notes"] = "|".join(result["safety_notes"]) or None
    return result


# =========================
# Ratings (.pp-verdict + .pp-reviews)
# =========================
def parse_ratings(soup):
    rating      = None
    num_reviews = None

    # Skincarisma verdict score
    score_tag = soup.select_one(".pp-verdict .score")
    if score_tag:
        m = re.search(r"(\d+(?:\.\d+)?)", score_tag.get_text())
        if m:
            rating = float(m.group(1))

    # Count all reviews from .pp-review-card
    total = 0
    for card in soup.select(".pp-review-card .stat"):
        lbl = safe_lower(card.select_one(".lbl").get_text() if card.select_one(".lbl") else "")
        if "review" in lbl:
            m = re.search(r"(\d+)", card.select_one(".num").get_text() if card.select_one(".num") else "")
            if m:
                total += int(m.group(1))
    if total:
        num_reviews = total

    # Fallback: title like "115+ REVIEWS"
    if not num_reviews:
        title_tag = soup.select_one(".pp-title .num")
        if title_tag:
            m = re.search(r"(\d+)", title_tag.get_text())
            if m:
                num_reviews = int(m.group(1))

    return {"rating": rating, "number_of_reviews": num_reviews}


# =========================
# Country of origin (.pp-origin)
# =========================
def parse_origin(soup):
    origin_tag = soup.select_one(".pp-origin .text")
    if origin_tag:
        txt = clean_text(origin_tag.get_text())
        if txt:
            return txt.replace("Made in", "").strip()
    return None


# =========================
# Benefits / concerns (keyword-based)
# =========================
def parse_benefits_concerns(full_text):
    low = safe_lower(full_text)
    concerns, benefits = set(), set()

    concern_rules = {
        "acne":              ["acne", "blemish", "pimple", "comedogenic"],
        "hyperpigmentation": ["hyperpigmentation", "dark spots", "melasma", "brightening"],
        "dryness":           ["dry", "dryness", "hydration", "moisture"],
        "redness":           ["redness", "irritation", "calming", "soothing"],
        "anti_aging":        ["anti-aging", "wrinkle", "fine lines", "retinol", "peptide"],
        "barrier":           ["barrier", "repair", "tewl"],
    }
    benefit_rules = {
        "hydrating":       ["hydration", "hydrating", "moisture", "humectant"],
        "soothing":        ["soothing", "calm", "redness", "anti-inflammatory"],
        "brightening":     ["brightening", "dark spots", "vitamin c"],
        "barrier_support": ["barrier", "repair", "ceramide"],
        "oil_control":     ["oil control", "mattifies", "matte", "sebum"],
        "sun_protection":  ["spf", "uva", "uvb", "zinc oxide"],
        "exfoliating":     ["exfoliating", "aha", "bha", "salicylic", "glycolic"],
    }

    for k, keys in concern_rules.items():
        if any(k2 in low for k2 in keys):
            concerns.add(k)
    for k, keys in benefit_rules.items():
        if any(k2 in low for k2 in keys):
            benefits.add(k)

    return {
        "benefits": "|".join(sorted(benefits)) if benefits else None,
        "concerns": "|".join(sorted(concerns)) if concerns else None,
    }


# =========================
# Ingredient parsing
# =========================
def clean_ingredient_name(raw_name):
    name = clean_text(raw_name)
    if not name:
        return None, None

    name = name.strip(" .–—:")
    concentration = None

    m = re.search(r"(~?\s*\d+(?:\.\d+)?(?:\s*-\s*\d+(?:\.\d+)?)?\s*%)", name)
    if m:
        concentration = m.group(1).replace(" ", "")
        name = name.replace(m.group(0), "").strip(" .–—:")

    name = re.sub(r"\s+", " ", name).strip()

    if len(name) > 120:
        return None, None
    bad_words = [
        "cool features", "suited for", "free from", "what it is",
        "fun facts", "this product", "shopify", "critical:", "mandatory",
        "you are an assistant", "ingredient_example",
    ]
    if any(b in name.lower() for b in bad_words):
        return None, None

    return (name, concentration) if name else (None, None)


def parse_ingredient_cards(soup):
    """Parse detailed .pp-ingredient cards."""
    rows = []
    for idx, card in enumerate(soup.select("div.pp-ingredient"), start=1):
        h3 = card.select_one("h3[itemprop='name'], .ing-head h3, h3")
        if not h3:
            continue

        h3_copy = BeautifulSoup(str(h3), "html.parser")

        # Evidence level from .tag span
        tag_span = h3_copy.select_one(".tag")
        evidence_level = clean_text(tag_span.get_text(" ", strip=True)) if tag_span else None
        if tag_span:
            tag_span.decompose()

        raw_name = clean_text(h3_copy.get_text(" ", strip=True))
        ingredient_name, concentration = clean_ingredient_name(raw_name)
        if not ingredient_name:
            continue

        desc_tag = card.select_one("p.ing-desc, [itemprop='description']")
        description = clean_text(desc_tag.get_text(" ", strip=True)) if desc_tag else None

        science_tags, science_details = [], []
        for li in card.select("ul.pp-science li"):
            li_text = clean_text(li.get_text(" ", strip=True))
            strong  = li.select_one("strong")
            if strong:
                lbl = clean_text(strong.get_text(" ", strip=True))
                if lbl:
                    science_tags.append(lbl.rstrip(":"))
            if li_text:
                science_details.append(li_text)

        callout_type, callout_texts = None, []
        for c in card.select(".pp-callout"):
            cls = [x for x in c.get("class", []) if x != "pp-callout"]
            if cls and not callout_type:
                callout_type = "|".join(cls)
            txt = clean_text(c.get_text(" ", strip=True))
            if txt:
                callout_texts.append(txt)

        warning_type, warning_texts = None, []
        for w in card.select(".pp-warning"):
            cls = [x for x in w.get("class", []) if x != "pp-warning"]
            if cls and not warning_type:
                warning_type = "|".join(cls)
            txt = clean_text(w.get_text(" ", strip=True))
            if txt:
                warning_texts.append(txt)

        rows.append({
            "position":          idx,
            "raw_ingredient_name": raw_name,
            "name":              ingredient_name,
            "concentration":     concentration,
            "description":       description,
            "evidence_level":    evidence_level,
            "science_tags":      "|".join(sorted(set(science_tags))) if science_tags else None,
            "science_details":   "|".join(science_details) if science_details else None,
            "callout_type":      callout_type,
            "callout_text":      "|".join(callout_texts) if callout_texts else None,
            "warning_type":      warning_type,
            "warning_text":      "|".join(warning_texts) if warning_texts else None,
        })
    return rows


def parse_inci_list_from_accordion(soup):
    """Parse the full INCI list from .pp-accordion > .pp-acc-content."""
    rows = []
    for acc in soup.select(".pp-accordion"):
        label = acc.select_one("label")
        label_txt = safe_lower(label.get_text() if label else "")
        if "full ingredient" not in label_txt and "inci" not in label_txt:
            continue

        content = acc.select_one(".pp-acc-content")
        if not content:
            continue

        ingredient_text = None
        for p in content.select("p"):
            txt = clean_text(p.get_text(" ", strip=True))
            if not txt:
                continue
            if re.match(r"^\d+\s+ingredients?$", txt, re.I):
                continue
            if "," in txt and len(txt) > 20:
                ingredient_text = txt
                break

        if not ingredient_text:
            continue

        ingredient_text = re.sub(r"FREE FROM:.*", "", ingredient_text, flags=re.I | re.S)
        ingredient_text = re.sub(r"^\(?INCI\)?\s*:?\s*", "", ingredient_text, flags=re.I)

        for i, raw in enumerate(ingredient_text.split(","), start=1):
            raw = raw.strip(" .")
            name, conc = clean_ingredient_name(raw)
            if name:
                rows.append({
                    "position": i, "raw_ingredient_name": raw,
                    "name": name, "concentration": conc,
                    "description": None, "evidence_level": None,
                    "science_tags": None, "science_details": None,
                    "callout_type": None, "callout_text": None,
                    "warning_type": None, "warning_text": None,
                })
    return rows


def parse_inci_from_jsonld(json_objects):
    rows = []
    for pos, raw_name in find_itemlist_ingredients(json_objects):
        name, conc = clean_ingredient_name(raw_name)
        if name:
            rows.append({
                "position": pos, "raw_ingredient_name": raw_name,
                "name": name, "concentration": conc,
                "description": None, "evidence_level": None,
                "science_tags": None, "science_details": None,
                "callout_type": None, "callout_text": None,
                "warning_type": None, "warning_text": None,
            })
    return rows


def merge_ingredient_sources(*sources):
    merged = {}
    for source in sources:
        for row in source:
            name = row.get("name")
            if not name:
                continue
            key = name.lower()
            if key not in merged:
                merged[key] = dict(row)
            else:
                old = merged[key]
                for col, val in row.items():
                    if old.get(col) in [None, "", []] and val not in [None, "", []]:
                        old[col] = val
    result = list(merged.values())
    result.sort(key=lambda x: x.get("position") or 9999)
    return result


def parse_ingredients(soup, json_objects, product_id, product_name):
    rich     = parse_ingredient_cards(soup)
    inci     = parse_inci_list_from_accordion(soup)
    from_ld  = parse_inci_from_jsonld(json_objects)

    parsed = merge_ingredient_sources(rich, inci, from_ld)

    ingredient_rows, pi_rows = [], []
    for idx, item in enumerate(parsed, start=1):
        ing_name = item.get("name")
        if not ing_name:
            continue
        ing_id   = make_id(ing_name)
        position = item.get("position") or idx

        ingredient_rows.append({
            "ingredient_id":   ing_id,
            "ingredient_name": ing_name,
            "description":     item.get("description"),
            "evidence_level":  item.get("evidence_level"),
            "science_tags":    item.get("science_tags"),
            "science_details": item.get("science_details"),
            "callout_type":    item.get("callout_type"),
            "callout_text":    item.get("callout_text"),
            "warning_type":    item.get("warning_type"),
            "warning_text":    item.get("warning_text"),
            "source":          "skincarisma",
        })

        pi_rows.append({
            "product_id":              product_id,
            "product_name":            product_name,
            "ingredient_id":           ing_id,
            "ingredient_name":         ing_name,
            "ingredient_position":     position,
            "ingredient_concentration": item.get("concentration"),
            "raw_ingredient_name":     item.get("raw_ingredient_name"),
            "evidence_level":          item.get("evidence_level"),
            "source":                  "skincarisma",
        })

    return ingredient_rows, pi_rows


# =========================
# Recommendation features
# =========================
def generate_recommendation_features(product_row):
    text = " ".join(str(product_row.get(f) or "") for f in [
        "product_name", "brand_name", "category", "sub_category",
        "description", "key_actives_text", "product_claims",
        "benefits", "concerns", "free_from", "skin_type_notes",
    ]).lower()

    def has(words):
        return int(any(w in text for w in words))

    return {
        "product_id": product_row["product_id"],

        "skin_type_oily":       int((product_row.get("oily_skin_score") or 0) >= 4) or has(["oily", "oil-free", "sebum", "matte"]),
        "skin_type_dry":        int((product_row.get("dry_skin_score") or 0) >= 4) or has(["dry skin", "dryness", "hydrating"]),
        "skin_type_sensitive":  int((product_row.get("sensitive_skin_score") or 0) >= 4) or has(["sensitive", "soothing", "calming"]),
        "skin_type_combination":int((product_row.get("combination_skin_score") or 0) >= 4) or has(["combination skin"]),
        "skin_type_acne_prone": int((product_row.get("acne_prone_score") or 0) >= 4) or has(["acne-prone", "acne prone"]),

        "concern_acne":             has(["acne", "blemish", "salicylic", "bha", "non-comedogenic"]),
        "concern_redness":          has(["redness", "soothing", "calming", "centella"]),
        "concern_hyperpigmentation":has(["hyperpigmentation", "dark spot", "brightening", "niacinamide", "vitamin c"]),
        "concern_anti_aging":       has(["anti-aging", "wrinkle", "retinol", "peptide"]),
        "concern_dryness":          has(["dryness", "hydration", "moisture", "barrier"]),

        "suitable_for_oily":           int((product_row.get("oily_skin_score") or 0) >= 4) or has(["oily", "oil-free", "matte"]),
        "suitable_for_dry":            int((product_row.get("dry_skin_score") or 0) >= 4) or has(["dry skin", "hydrating", "moisturizing"]),
        "suitable_for_sensitive":      int((product_row.get("sensitive_skin_score") or 0) >= 4) or has(["sensitive", "soothing"]),
        "suitable_for_acne":           int((product_row.get("acne_prone_score") or 0) >= 4) or has(["acne-prone", "non-comedogenic"]),
        "suitable_for_hyperpigmentation": has(["hyperpigmentation", "dark spot", "brightening"]),
        "suitable_for_anti_aging":     has(["anti-aging", "wrinkle", "retinol", "peptide"]),
        "suitable_for_redness":        has(["redness", "soothing", "calming"]),

        "avoid_for_sensitive":   int(has(["fragrance", "alcohol denat"]) and not has(["fragrance-free", "alcohol-free"])),
        "avoid_for_fungal_acne": (int(product_row.get("fungal_acne_safe") == 0)
                                  if product_row.get("fungal_acne_safe") is not None else None),

        "fungal_acne_safe":  product_row.get("fungal_acne_safe"),
        "pregnancy_safe":    product_row.get("pregnancy_safe"),
        "comedogenic_risk":  product_row.get("comedogenic_rating"),
        "irritation_risk":   product_row.get("irritation_rating"),
    }


# =========================
# Main page scraper
# =========================
def scrape_product_page(card_row):
    product_url = str(card_row.get("product_url", "")).strip()
    if not product_url:
        return None, [], [], None, {"product_url": product_url, "reason": "empty_url"}

    html = fetch_html(product_url)
    if not html:
        return None, [], [], None, {"product_url": product_url, "reason": "failed_fetch"}

    soup         = BeautifulSoup(html, "html.parser")
    json_objects = extract_all_jsonld(soup)
    product_json = find_product_jsonld(json_objects)
    breadcrumbs  = extract_breadcrumb_names(json_objects)

    fake, reason = is_fake_or_hidden_product(soup, product_json, card_row)
    if fake:
        return None, [], [], None, {"product_url": product_url, "reason": reason}

    product_id = str(card_row.get("product_id") or make_id(product_url))

    meta     = parse_product_meta(soup, product_json, card_row, breadcrumbs)
    scorecard = parse_scorecard(soup)
    specs    = parse_specs(soup)
    badges   = parse_badges(soup)
    safety   = parse_safety_section(soup)
    skin     = parse_skin_types(soup)
    ratings  = parse_ratings(soup)
    free_from = parse_free_from(soup) or parse_quick_notes(soup)
    origin   = parse_origin(soup)

    # Scorecard takes priority for safety flags
    pregnancy_safe   = scorecard.get("pregnancy_safe")   if scorecard.get("pregnancy_safe")   is not None else safety.get("pregnancy_safe")
    fungal_acne_safe = scorecard.get("fungal_acne_safe") if scorecard.get("fungal_acne_safe") is not None else safety.get("fungal_acne_safe")
    comedogenic      = scorecard.get("comedogenic_rating") if scorecard.get("comedogenic_rating") is not None else safety.get("comedogenic_rating")

    full_text = " ".join([
        str(meta.get("description") or ""),
        str(specs.get("key_actives_text") or ""),
        str(badges.get("product_claims") or ""),
        str(free_from or ""),
    ])
    benefits_concerns = parse_benefits_concerns(full_text)

    product_row = {
        "product_id":   product_id,
        "product_url":  product_url,
        "product_name": meta["product_name"],
        "brand_name":   meta["brand_name"],
        "category":     meta["category"],
        "sub_category": meta["sub_category"],
        "image_url":    meta["image_url"],
        "description":  meta["description"],
        "price":        meta["price"],
        "currency":     meta["currency"],

        "size":             specs.get("size"),
        "product_type_text": specs.get("product_type_text"),
        "key_actives_text": specs.get("key_actives_text"),
        "ingredients_count": specs.get("ingredients_count"),

        "rating":           ratings.get("rating"),
        "number_of_reviews": ratings.get("number_of_reviews"),

        "good_for_skin_types":    skin.get("good_for_skin_types"),
        "bad_for_skin_types":     skin.get("bad_for_skin_types"),
        "oily_skin_score":        skin.get("oily_skin_score"),
        "dry_skin_score":         skin.get("dry_skin_score"),
        "sensitive_skin_score":   skin.get("sensitive_skin_score"),
        "combination_skin_score": skin.get("combination_skin_score"),
        "normal_skin_score":      skin.get("normal_skin_score"),
        "acne_prone_score":       skin.get("acne_prone_score"),
        "skin_type_notes":        skin.get("skin_type_notes"),

        "pregnancy_safe":    pregnancy_safe,
        "fungal_acne_safe":  fungal_acne_safe,
        "comedogenic_rating": comedogenic,
        "irritation_rating": safety.get("irritation_rating"),
        "safety_notes":      safety.get("safety_notes"),

        "product_claims": badges.get("product_claims"),
        "benefits":       benefits_concerns.get("benefits"),
        "concerns":       benefits_concerns.get("concerns"),
        "free_from":      free_from,

        "vegan":         badges.get("vegan", 0),
        "cruelty_free":  badges.get("cruelty_free", 0),
        "reef_safe":     badges.get("reef_safe", 0),
        "fragrance_free": badges.get("fragrance_free", 0),
        "alcohol_free":  badges.get("alcohol_free", 0),
        "paraben_free":  badges.get("paraben_free", 0),
        "sulfate_free":  badges.get("sulfate_free", 0),
        "silicone_free": badges.get("silicone_free", 0),
        "oil_free":      badges.get("oil_free", 0),

        "country_of_origin": origin,
        "source":            "skincarisma",
    }

    ingredient_rows, pi_rows = parse_ingredients(
        soup, json_objects, product_id, product_row["product_name"]
    )

    feature_row = generate_recommendation_features(product_row)

    return product_row, ingredient_rows, pi_rows, feature_row, None


# =========================
# CSV helpers
# =========================
def append_csv(rows, path, subset=None):
    if not rows:
        return
    new_df = pd.DataFrame(rows)
    if os.path.exists(path):
        try:
            old_df = pd.read_csv(path)
            df = pd.concat([old_df, new_df], ignore_index=True)
        except Exception:
            df = new_df
    else:
        df = new_df

    if subset:
        subset = [c for c in subset if c in df.columns]
        if subset:
            df = df.drop_duplicates(subset=subset, keep="last")
    else:
        df = df.drop_duplicates(keep="last")

    df.to_csv(path, index=False, encoding="utf-8-sig")


def get_done_urls():
    if os.path.exists(PRODUCTS_CSV):
        try:
            df = pd.read_csv(PRODUCTS_CSV)
            if "product_url" in df.columns:
                return set(df["product_url"].dropna().astype(str))
        except Exception:
            pass
    return set()


# =========================
# Entry point
# =========================
def main(limit=None, max_workers=4, save_every=25, delay_min=0.4, delay_max=1.2):
    if not os.path.exists(INPUT_CSV):
        raise FileNotFoundError(f"Input file not found: {INPUT_CSV}")

    df = pd.read_csv(INPUT_CSV)
    if "product_url" not in df.columns:
        raise ValueError("product_links.csv must have a 'product_url' column")

    for col in ["product_id", "brand_name", "category", "image_url", "price", "product_name"]:
        if col not in df.columns:
            df[col] = None

    if limit:
        df = df.head(limit)

    done = get_done_urls()
    df   = df[~df["product_url"].astype(str).isin(done)].copy()

    print(f"Already scraped : {len(done)}")
    print(f"Remaining       : {len(df)}")

    p_buf, i_buf, pi_buf, f_buf, fail_buf = [], [], [], [], []

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {
            ex.submit(scrape_product_page, row.to_dict()): row["product_url"]
            for _, row in df.iterrows()
        }

        for n, future in enumerate(as_completed(futures), start=1):
            url = futures[future]
            try:
                product_row, ing_rows, pi_rows, feat_row, failed = future.result()
                if product_row: p_buf.append(product_row)
                if ing_rows:    i_buf.extend(ing_rows)
                if pi_rows:     pi_buf.extend(pi_rows)
                if feat_row:    f_buf.append(feat_row)
                if failed:      fail_buf.append(failed)
            except Exception as exc:
                logging.exception(f"Error scraping {url}: {exc}")
                fail_buf.append({"product_url": url, "reason": str(exc)})

            if n % save_every == 0:
                append_csv(p_buf,    PRODUCTS_CSV,           subset=["product_id"])
                append_csv(i_buf,    INGREDIENTS_CSV,        subset=["ingredient_id"])
                append_csv(pi_buf,   PRODUCT_INGREDIENTS_CSV, subset=["product_id", "ingredient_id"])
                append_csv(f_buf,    FEATURES_CSV,           subset=["product_id"])
                append_csv(fail_buf, FAILED_CSV,             subset=["product_url"])
                print(f"  [saved] processed: {n}")
                p_buf, i_buf, pi_buf, f_buf, fail_buf = [], [], [], [], []

            time.sleep(random.uniform(delay_min, delay_max))

    append_csv(p_buf,    PRODUCTS_CSV,           subset=["product_id"])
    append_csv(i_buf,    INGREDIENTS_CSV,        subset=["ingredient_id"])
    append_csv(pi_buf,   PRODUCT_INGREDIENTS_CSV, subset=["product_id", "ingredient_id"])
    append_csv(f_buf,    FEATURES_CSV,           subset=["product_id"])
    append_csv(fail_buf, FAILED_CSV,             subset=["product_url"])

    print("\nDone.")
    print(f"  Products            -> {PRODUCTS_CSV}")
    print(f"  Ingredients         -> {INGREDIENTS_CSV}")
    print(f"  Product-Ingredients -> {PRODUCT_INGREDIENTS_CSV}")
    print(f"  Features            -> {FEATURES_CSV}")
    print(f"  Failed/skipped      -> {FAILED_CSV}")


if __name__ == "__main__":
    main(
        limit=None,       # None = scrape everything
        max_workers=6,    # polite concurrency
        save_every=25,
    )