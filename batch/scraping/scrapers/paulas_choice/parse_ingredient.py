"""
Parser for Paula's Choice Ingredient Dictionary detail pages.

This parser is designed to avoid collecting navigation/footer/product links.
It narrows extraction to the main ingredient content area.
"""

import re
from bs4 import BeautifulSoup

from utils import clean_text, clean_list


def get_soup(html):
    return BeautifulSoup(html, "html.parser")


def get_main_content(soup):
    """
    Narrow parsing to the ingredient main content area.
    This avoids header, footer, menus, product carousel, and global links.
    """

    selectors = [
        '[class*="IngredientPagestyles__Content"]',
        '[class*="IngredientPagestyles__Wrapper"]',
        "main",
        "body",
    ]

    for selector in selectors:
        element = soup.select_one(selector)
        if element:
            return element

    return soup


def extract_ingredient_name(soup):
    main = get_main_content(soup)
    h1 = main.select_one("h1")

    if not h1:
        return None

    return clean_text(h1.get_text(" ", strip=True))


def extract_rating(soup):
    """
    Extract only ingredient rating row.
    """

    main = get_main_content(soup)

    for element in main.find_all(["div", "span"]):
        text = clean_text(element.get_text(" ", strip=True))

        if not text:
            continue

        if text.lower().startswith("rating:"):
            value = text.split(":", 1)[-1]
            return clean_text(value)

    return None


def find_label_row(main, label):
    """
    Find the small row that starts with Benefits: or Categories:.

    Avoid selecting the whole page by requiring:
    - text starts with the label
    - text is not too long
    """

    label_lower = label.lower()

    for element in main.find_all("div"):
        text = clean_text(element.get_text(" ", strip=True))

        if not text:
            continue

        if not text.lower().startswith(label_lower + ":"):
            continue

        # Important: prevent matching huge parent containers
        if len(text) > 300:
            continue

        return element

    return None


def extract_values_from_label_row(soup, label):
    """
    Extract link texts only from the direct Benefits/Categories row.
    """

    main = get_main_content(soup)
    row = find_label_row(main, label)

    if not row:
        return None

    links = [
        clean_text(a.get_text(" ", strip=True))
        for a in row.find_all("a")
    ]

    links = clean_list(links)

    if links:
        return links

    # Fallback if row has no links
    text = clean_text(row.get_text(" ", strip=True))

    if not text:
        return None

    value = text.split(":", 1)[-1]
    parts = [part.strip() for part in value.split(",")]

    return clean_list(parts)


def extract_benefits(soup):
    return extract_values_from_label_row(soup, "Benefits")


def extract_categories(soup):
    return extract_values_from_label_row(soup, "Categories")


def extract_at_a_glance_points(soup):
    main = get_main_content(soup)

    heading = None

    for h in main.find_all(["h2", "h3"]):
        text = clean_text(h.get_text(" ", strip=True))

        if text and "at a glance" in text.lower():
            heading = h
            break

    if not heading:
        return None

    ul = heading.find_next("ul")

    if not ul:
        return None

    points = [
        clean_text(li.get_text(" ", strip=True))
        for li in ul.find_all("li")
    ]

    return clean_list(points)


def extract_full_description(soup):
    main = get_main_content(soup)

    description_heading = None

    for h in main.find_all(["h2", "h3"]):
        text = clean_text(h.get_text(" ", strip=True))

        if text and "description" in text.lower():
            description_heading = h
            break

    if not description_heading:
        return None

    description_container = description_heading.find_next(
        "div",
        class_=lambda class_name: class_name and "IngredientPagestyles__Description" in class_name,
    )

    if not description_container:
        description_container = description_heading.find_next("div")

    if not description_container:
        return None

    paragraphs = [
        clean_text(p.get_text(" ", strip=True))
        for p in description_container.find_all("p")
    ]

    paragraphs = [p for p in paragraphs if p]

    if not paragraphs:
        return None

    return "\n\n".join(paragraphs)


def extract_related_ingredients(soup):
    main = get_main_content(soup)
    related_values = []

    for element in main.find_all(["div", "section"]):
        text = clean_text(element.get_text(" ", strip=True))

        if not text:
            continue

        if text.startswith("See:") and len(text) < 200:
            links = [
                clean_text(a.get_text(" ", strip=True))
                for a in element.find_all("a")
            ]
            related_values.extend([link for link in links if link])

    return clean_list(related_values)


def extract_article_info_value(soup, label):
    """
    Extract:
    - Written by:
    - Reviewed by:
    - Updated on:
    """

    main = get_main_content(soup)
    label_lower = label.lower()

    for element in main.find_all("div"):
        text = clean_text(element.get_text(" ", strip=True))

        if not text:
            continue

        if not text.lower().startswith(label_lower):
            continue

        if len(text) > 200:
            continue

        value = text.split(":", 1)[-1]
        return clean_text(value)

    return None


def extract_synonyms_from_text(raw_ingredient_name, at_a_glance_points, full_description):
    synonyms = []

    if at_a_glance_points:
        for point in at_a_glance_points:
            point_lower = point.lower()

            if point_lower.startswith("aka "):
                value = point[4:].strip()
                parts = re.split(r"\band\b|,", value, flags=re.IGNORECASE)
                synonyms.extend(parts)

    if full_description:
        patterns = [
            r"formerly known by its original trade name\s+([^,.]+)",
            r"generically as\s+([^,.]+)",
            r"may also be referred to as\s+([^,.]+)",
            r"also known as\s+([^,.]+)",
        ]

        for pattern in patterns:
            matches = re.findall(pattern, full_description, flags=re.IGNORECASE)

            for match in matches:
                synonyms.append(match)

    synonyms = clean_list(synonyms)

    if not synonyms:
        return None

    if raw_ingredient_name:
        synonyms = [
            s for s in synonyms
            if s.lower() != raw_ingredient_name.lower()
        ]

    return synonyms if synonyms else None


def parse_ingredient_page(html, ingredient_url=None):
    """
    Parse one ingredient detail page.
    """

    soup = get_soup(html)

    raw_ingredient_name = extract_ingredient_name(soup)
    rating = extract_rating(soup)
    benefits = extract_benefits(soup)
    ingredient_categories = extract_categories(soup)
    at_a_glance_points = extract_at_a_glance_points(soup)
    full_description = extract_full_description(soup)

    synonyms = extract_synonyms_from_text(
        raw_ingredient_name=raw_ingredient_name,
        at_a_glance_points=at_a_glance_points,
        full_description=full_description,
    )

    parsed_record = {
        "raw_ingredient_name": raw_ingredient_name,
        "rating": rating,
        "benefits": benefits,
        "ingredient_categories": ingredient_categories,
        "at_a_glance_points": at_a_glance_points,
        "full_description": full_description,
        "synonyms": synonyms,
        "concerns": None,
        "safety_notes": None,
        "related_ingredients": extract_related_ingredients(soup),
        
    }

    return parsed_record