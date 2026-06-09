"""
amazon_scraper.py  v3
=====================
Amazon.com skincare price scraper — anti-bot hardened.

Key fixes in v3:
  - Longer delays (5–10s) to avoid rate limiting
  - CAPTCHA / block detection with automatic pause (2–4 min)
  - Long break every 20 products (30–60s human-like pause)
  - Browser restart every 50 products (fresh session/cookies)
  - Consecutive-failure streak detection → triggers block pause
  - EGP / non-USD currency detection & conversion
  - Concentration mismatch penalty (0.1% vs 0.3% = -20 pts)
  - Fuzzy threshold 65 (was 58)

Usage:
    python amazon_scraper.py --input your_file.csv --output results.csv
    python amazon_scraper.py --input your_file.csv --output results.csv --limit 50
    python amazon_scraper.py --input your_file.csv --output results.csv --resume
"""

import re, csv, time, random, argparse, sys, os
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import quote_plus

# ─────────────────────────────────────────────────────────────
# CONFIG  — tweak here without touching scraper logic
# ─────────────────────────────────────────────────────────────

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
]

HEADLESS           = True
PLAYWRIGHT_TIMEOUT = 25_000     # ms

FUZZY_THRESHOLD    = 20.0       # min score to accept a match

MAX_RETRIES        = 3
RETRY_BASE         = 1         # seconds (exponential backoff base)
RETRY_JITTER       = 1.5

# Normal per-request delay — longer = safer
MIN_DELAY          = 3.0
MAX_DELAY          = 6.0

# Periodic long break (mimics a human stepping away)
LONG_BREAK_EVERY   = 20         # products
LONG_BREAK_MIN     = 30         # seconds
LONG_BREAK_MAX     = 60

# Pause when Amazon appears to be blocking
BLOCK_PAUSE_MIN    = 120        # 2 min
BLOCK_PAUSE_MAX    = 240        # 4 min
BLOCK_STREAK_LIMIT = 5          # consecutive "No results" before pausing

# Restart browser session every N products
RESTART_EVERY      = 50

SEARCH_URL         = "https://www.amazon.com/s?k={query}&i=beauty&language=en_US"
PRODUCT_URL_SUFFIX = "&language=en_US&currency=USD"

# EGP → USD rate (update as needed; mid-2025 ≈ 49 EGP/USD)
CURRENCY_RATES = {
    "USD": 1.0,
    "EGP": 1 / 49.0,
    "CAD": 0.73,
    "GBP": 1.27,
    "EUR": 1.08,
    "AUD": 0.65,
}

# ─────────────────────────────────────────────────────────────
# OPTIONAL: rapidfuzz
# ─────────────────────────────────────────────────────────────
try:
    from rapidfuzz import fuzz
    _HAS_RAPIDFUZZ = True
except ImportError:
    _HAS_RAPIDFUZZ = False
    print("[WARN] rapidfuzz not installed — run: pip install rapidfuzz\n")


# ═════════════════════════════════════════════════════════════
# DATA CLASS
# ═════════════════════════════════════════════════════════════

@dataclass
class ProductResult:
    product_id:    str            = ""
    product_name:  str            = ""
    brand_name:    str            = ""
    matched_title: str            = ""
    matched_brand: str            = ""
    match_score:   float          = 0.0
    source_url:    str            = ""
    price_usd:     Optional[float]= None
    raw_price:     Optional[float]= None
    raw_currency:  str            = "USD"
    exchange_rate: float          = 1.0
    size_label:    str            = ""
    size_ml:       Optional[float]= None
    price_per_ml:  Optional[float]= None
    in_stock:      bool           = True
    success:       bool           = False
    status:        str            = "pending"
    error:         str            = ""
    scraped_at:    str            = ""


# ═════════════════════════════════════════════════════════════
# UTILITIES
# ═════════════════════════════════════════════════════════════

def clean_name(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = s.strip()
    s = re.sub(r'^[\\\"]+', '', s)
    s = re.sub(r'[\\\"]+$', '', s)
    if s.startswith('"') and s.endswith('"'):
        s = s[1:-1]
    return s.strip()


# ── Currency ─────────────────────────────────────────────────

_CUR_RE   = re.compile(r'^(EGP|USD|CAD|GBP|EUR|AUD|\$|£|€|CA\$)', re.I)
_SYM_MAP  = {"$": "USD", "£": "GBP", "€": "EUR", "CA$": "CAD"}

def detect_currency(text: str) -> str:
    m = _CUR_RE.match((text or "").strip())
    if not m:
        return "USD"
    return _SYM_MAP.get(m.group(1).upper(), m.group(1).upper())

def parse_raw_price(text: str) -> Optional[float]:
    if not text:
        return None
    cleaned = _CUR_RE.sub("", text.strip()).strip()
    cleaned = re.sub(r"[^\d.,]", "", cleaned)
    if not cleaned:
        return None
    if "," in cleaned and "." not in cleaned:
        parts = cleaned.split(",")
        cleaned = (parts[0] + "." + parts[1]) if len(parts)==2 and len(parts[1])<=2 \
                  else cleaned.replace(",","")
    else:
        cleaned = cleaned.replace(",","")
    try:
        v = float(cleaned)
        return v if v > 0 else None
    except ValueError:
        return None

def to_usd(raw: float, currency: str):
    rate = CURRENCY_RATES.get(currency.upper(), 1.0)
    return round(raw * rate, 2), rate

def get_price_and_currency(page):
    selectors = [
        "#corePriceDisplay_desktop_feature_div .apex-basisprice-value .a-offscreen",
        ".basisPrice .a-offscreen",
        "#corePriceDisplay_desktop_feature_div .priceToPay .a-offscreen",
        "#corePriceDisplay_desktop_feature_div .apex-pricetopay-value .a-offscreen",
        "#corePrice_feature_div .priceToPay .a-offscreen",
        ".priceToPay .a-offscreen",
        ".a-price .a-offscreen",
    ]
    for sel in selectors:
        try:
            el = page.query_selector(sel)
            if el:
                text     = el.inner_text().strip()
                currency = detect_currency(text)
                raw      = parse_raw_price(text)
                if raw:
                    usd, rate = to_usd(raw, currency)
                    if currency != "USD":
                        print(f"  [CURRENCY] {currency} {raw:.2f} → ${usd:.2f} USD")
                    return usd, currency, raw
        except Exception:
            continue
    # Whole + fraction fallback
    try:
        w = page.query_selector("span.a-price-whole")
        f = page.query_selector("span.a-price-fraction")
        if w:
            whole = re.sub(r"[^\d]", "", w.inner_text())
            frac  = re.sub(r"[^\d]", "", f.inner_text()) if f else "00"
            if whole:
                raw      = float(f"{whole}.{frac[:2]}")
                sym_el   = page.query_selector("span.a-price-symbol")
                currency = detect_currency((sym_el.inner_text().strip() if sym_el else "$") + "0")
                usd, _   = to_usd(raw, currency)
                return usd, currency, raw
    except Exception:
        pass
    return None, "USD", None

def get_card_price(card):
    try:
        el = card.query_selector(".a-price .a-offscreen")
        if el:
            text = el.inner_text().strip()
            raw  = parse_raw_price(text)
            if raw:
                usd, _ = to_usd(raw, detect_currency(text))
                return usd
    except Exception:
        pass
    return None


# ── Size ─────────────────────────────────────────────────────

_SIZE_PATS = [
    (r"(\d+(?:\.\d+)?)\s*fl\.?\s*oz", lambda m: float(m.group(1)) * 29.5735),
    (r"(\d+(?:\.\d+)?)\s*ml",          lambda m: float(m.group(1))),
    (r"(\d+(?:\.\d+)?)\s*l\b",         lambda m: float(m.group(1)) * 1000),
    (r"(\d+(?:\.\d+)?)\s*oz\b",        lambda m: float(m.group(1)) * 29.5735),
]

def extract_size_ml(text: str) -> Optional[float]:
    if not text:
        return None
    for pat, fn in _SIZE_PATS:
        m = re.search(pat, text, re.I)
        if m:
            return round(fn(m), 2)
    return None

def price_per_ml(price, size):
    return round(price / size, 4) if price and size and size > 0 else None


# ── Fuzzy match ───────────────────────────────────────────────

def _conc(text):
    m = re.search(r'(\d+(?:\.\d+)?)\s*%', text)
    return m.group(0).replace(" ","") if m else None

def match_score(qname, qbrand, cname, cbrand) -> float:
    if _HAS_RAPIDFUZZ:
        n = fuzz.token_set_ratio(qname.lower(), cname.lower())
        b = fuzz.token_set_ratio(qbrand.lower(), cbrand.lower())
    else:
        def _s(a, b):
            sa,sb = set(a.lower().split()), set(b.lower().split())
            u = sa|sb
            return len(sa&sb)/len(u)*100 if u else 0.0
        n, b = _s(qname, cname), _s(qbrand, cbrand)
    score = round(0.70*n + 0.30*b, 1)
    qc, cc = _conc(qname), _conc(cname)
    if qc and cc and qc != cc:
        score = max(0.0, score - 20.0)
    return score


# ═════════════════════════════════════════════════════════════
# SCRAPER
# ═════════════════════════════════════════════════════════════

class AmazonScraper:

    def __init__(self):
        self._pw = self._browser = self._context = self._page = None

    def start(self):
        from playwright.sync_api import sync_playwright
        self._pw      = sync_playwright().start()
        self._launch_browser()
        print(f"[INFO] Browser ready (headless={HEADLESS})")

    def _launch_browser(self):
        """Launch / re-launch browser with a fresh random identity."""
        if self._browser:
            try: self._browser.close()
            except Exception: pass

        ua = random.choice(USER_AGENTS)
        self._browser = self._pw.chromium.launch(
            headless=HEADLESS,
            args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
        )
        self._context = self._browser.new_context(
            user_agent=ua,
            viewport={"width": random.choice([1280,1366,1440]),
                      "height": random.choice([768,800,900])},
            locale="en-US",
            timezone_id="America/New_York",
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
        )
        self._context.add_cookies([
            {"name":"lc-main",    "value":"en_US","domain":".amazon.com","path":"/"},
            {"name":"i18n-prefs", "value":"USD",  "domain":".amazon.com","path":"/"},
            {"name":"currency",   "value":"USD",  "domain":".amazon.com","path":"/"},
        ])
        self._page = self._context.new_page()
        self._page.add_init_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
        )
        self._page.goto(
            "https://www.amazon.com/?language=en_US&currency=USD",
            wait_until="domcontentloaded", timeout=PLAYWRIGHT_TIMEOUT
        )

    def stop(self):
        try:
            self._browser and self._browser.close()
            self._pw      and self._pw.stop()
        except Exception: pass

    def __enter__(self):  self.start(); return self
    def __exit__(self, *_): self.stop()

    # ── Delays ───────────────────────────────────────────────

    def _sleep(self):
        time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

    def _long_break(self):
        w = random.uniform(LONG_BREAK_MIN, LONG_BREAK_MAX)
        print(f"  [BREAK] Human-like pause: {w:.0f}s...")
        time.sleep(w)

    def _block_pause(self):
        w = random.uniform(BLOCK_PAUSE_MIN, BLOCK_PAUSE_MAX)
        print(f"\n  ⚠ [BLOCKED] Amazon rate-limiting detected. Pausing {w:.0f}s...")
        time.sleep(w)
        # Restart browser for a completely fresh session
        print("  [RESTART] Relaunching browser with new identity...")
        self._launch_browser()

    # ── Block detection ───────────────────────────────────────

    def _is_blocked(self) -> bool:
        try:
            html = self._page.content().lower()
            return any(kw in html for kw in [
                "enter the characters you see below",
                "sorry, we just need to make sure you're not a robot",
                "captcha",
                "robot check",
            ])
        except Exception:
            return False

    # ── Retry wrapper ─────────────────────────────────────────

    def _retry(self, fn, label: str):
        last = ""
        for i in range(1, MAX_RETRIES + 1):
            try:
                result = fn()
                if self._is_blocked():
                    print(f"  [CAPTCHA] Caught on attempt {i}")
                    self._block_pause()
                    raise Exception("CAPTCHA")
                return result, None
            except Exception as e:
                last = str(e)
                if i < MAX_RETRIES:
                    w = RETRY_BASE ** i + random.uniform(0, RETRY_JITTER)
                    print(f"  [RETRY {i}/{MAX_RETRIES}] {label} — {last[:50]} — {w:.1f}s")
                    time.sleep(w)
        return None, last

    # ── Search ───────────────────────────────────────────────

    def _search(self, query: str) -> list[dict]:
        url = SEARCH_URL.format(query=quote_plus(query))
        self._page.goto(url, wait_until="domcontentloaded", timeout=PLAYWRIGHT_TIMEOUT)
        self._sleep()

        results = []
        for card in self._page.query_selector_all(
            "div[data-component-type='s-search-result']"
        )[:8]:
            try:
                title_el = card.query_selector("h2 span")
                if not title_el: continue
                title = title_el.inner_text().strip()

                link_el = card.query_selector("a.a-link-normal[href*='/dp/']")
                href = ""
                if link_el:
                    href = link_el.get_attribute("href") or ""
                    if href and not href.startswith("http"):
                        href = "https://www.amazon.com" + href
                    if href and "language=en_US" not in href:
                        href += PRODUCT_URL_SUFFIX

                brand = ""
                b_el = card.query_selector("span.a-size-base.s-underline-text")
                if b_el: brand = b_el.inner_text().strip()

                results.append({
                    "title": title, "brand": brand,
                    "price_usd": get_card_price(card), "url": href,
                })
            except Exception:
                continue
        return results

    # ── Product page ─────────────────────────────────────────

    def _scrape_page(self, url: str) -> dict:
        if "language=en_US" not in url:
            url += PRODUCT_URL_SUFFIX
        self._page.goto(url, wait_until="domcontentloaded", timeout=PLAYWRIGHT_TIMEOUT)
        self._sleep()

        data = {"price_usd": None, "raw_price": None, "raw_currency": "USD",
                "exchange_rate": 1.0, "size_label": "", "brand": "",
                "title": "", "in_stock": True}

        try:
            el = self._page.query_selector("#productTitle")
            if el: data["title"] = el.inner_text().strip()
        except Exception: pass

        try:
            el = self._page.query_selector("#bylineInfo")
            if el:
                data["brand"] = re.sub(
                    r"(?i)(visit the\s+|brand:\s*|\s+store$)", "",
                    el.inner_text().strip()
                ).strip()
        except Exception: pass

        try:
            el = self._page.query_selector("#availability span")
            if el: data["in_stock"] = "in stock" in el.inner_text().lower()
        except Exception: pass

        usd, cur, raw = get_price_and_currency(self._page)
        data["price_usd"]    = usd
        data["raw_price"]    = raw
        data["raw_currency"] = cur
        _, rate = to_usd(raw or 0, cur)
        data["exchange_rate"] = rate

        for sel in ["#inline-twister-expanded-dimension-text-size_name",
                    "#selected-size-name"]:
            try:
                el = self._page.query_selector(sel)
                if el: data["size_label"] = el.inner_text().strip(); break
            except Exception: continue

        if not data["size_label"] and data["title"]:
            m = re.search(r"(\d+(?:\.\d+)?\s*(?:ml|fl oz|oz|l\b))",
                          data["title"], re.I)
            if m: data["size_label"] = m.group(1)

        return data

    # ── Public scrape ────────────────────────────────────────

    def scrape(self, product_id: str, product_name: str,
               brand_name: str) -> ProductResult:

        result = ProductResult(
            product_id  = product_id,
            product_name= product_name,
            brand_name  = brand_name,
            scraped_at  = datetime.now(timezone.utc).isoformat(),
        )

        if not product_name or product_name in ("#NAME?", "N/A", ""):
            result.status = "skipped"
            result.error  = "Invalid product name"
            return result

        query = f"{brand_name} {product_name}".strip()
        print(f"  Query: {query[:80]!r}")

        # Search
        candidates, err = self._retry(
            lambda: self._search(query), f"search:{product_id[:8]}"
        )
        if err or not candidates:
            result.status = "scrape_failed"
            result.error  = err or "No results"
            return result

        # Fuzzy match
        best, best_score = None, 0.0
        for c in candidates:
            s = match_score(product_name, brand_name, c["title"], c.get("brand",""))
            if s > best_score:
                best_score, best = s, c

        result.match_score   = best_score
        result.matched_title = best["title"] if best else ""
        result.matched_brand = best.get("brand","") if best else ""
        result.source_url    = best.get("url","") if best else ""

        print(f"  Match {best_score:.0f}/100: {result.matched_title[:65]}")

        if best_score < FUZZY_THRESHOLD or not best:
            result.status = "unmatched"
            return result

        if not result.source_url:
            result.status = "scrape_failed"
            result.error  = "No URL"
            return result

        # Product page
        page_data, err = self._retry(
            lambda: self._scrape_page(result.source_url), f"page:{product_id[:8]}"
        )
        if err or not page_data:
            if best.get("price_usd"):
                page_data = {
                    "price_usd": best["price_usd"], "raw_price": best["price_usd"],
                    "raw_currency": "USD", "exchange_rate": 1.0,
                    "size_label": "", "brand": best.get("brand",""),
                    "title": best["title"], "in_stock": True,
                }
            else:
                result.status = "scrape_failed"
                result.error  = err or "Page failed"
                return result

        price = page_data.get("price_usd")
        if not price:
            result.status = "scrape_failed"
            result.error  = "Price not found"
            return result

        size_lbl = page_data.get("size_label","")
        size     = extract_size_ml(size_lbl) or extract_size_ml(product_name)
        ppm      = price_per_ml(price, size)

        result.price_usd     = price
        result.raw_price     = page_data.get("raw_price")
        result.raw_currency  = page_data.get("raw_currency","USD")
        result.exchange_rate = page_data.get("exchange_rate", 1.0)
        result.size_label    = size_lbl
        result.size_ml       = size
        result.price_per_ml  = ppm
        result.in_stock      = page_data.get("in_stock", True)
        result.matched_title = page_data.get("title") or result.matched_title
        result.matched_brand = page_data.get("brand") or result.matched_brand
        result.success       = True
        result.status        = "valid"

        cur_note = f" [{result.raw_currency}→USD]" if result.raw_currency != "USD" else ""
        print(f"  ${price:.2f} USD{cur_note} | {size_lbl or 'size?'} | "
              f"$/ml={ppm} | {'✓' if result.in_stock else '✗'} stock")
        return result


# ═════════════════════════════════════════════════════════════
# CSV I/O
# ═════════════════════════════════════════════════════════════

OUTPUT_COLS = [
    "product_id","product_name","brand_name",
    "status","price_usd","raw_price","raw_currency","exchange_rate",
    "size_label","size_ml","price_per_ml","in_stock",
    "match_score","matched_title","matched_brand",
    "source_url","scraped_at","error",
]

def load_products(path: str) -> list[dict]:
    try:
        import pandas as pd
        df = pd.read_csv(path, on_bad_lines="skip", engine="python", dtype=str)
    except Exception as e:
        sys.exit(f"[ERROR] Cannot read {path}: {e}")
    missing = {"product_id","product_name","brand_name"} - set(df.columns)
    if missing:
        sys.exit(f"[ERROR] CSV missing columns: {missing}")
    rows = []
    for _, row in df.iterrows():
        pid   = str(row.get("product_id","")).strip()
        name  = clean_name(str(row.get("product_name","")))
        brand = str(row.get("brand_name","")).strip()
        if not pid or not name or brand in ("nan",""):
            continue
        rows.append({"product_id": pid, "product_name": name,
                     "brand_name": brand if brand != "nan" else ""})
    print(f"[LOAD] {len(rows):,} products from {path}")
    return rows

def load_done(path: str) -> set[str]:
    done = set()
    if not os.path.exists(path):
        return done
    try:
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("product_id"):
                    done.add(row["product_id"])
    except Exception:
        pass
    return done

def append_result(r: ProductResult, path: str):
    exists = os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=OUTPUT_COLS, extrasaction="ignore")
        if not exists:
            w.writeheader()
        row = asdict(r)
        w.writerow({k: row.get(k,"") for k in OUTPUT_COLS})


# ═════════════════════════════════════════════════════════════
# CLI
# ═════════════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser(
        description="Scrape Amazon.com prices from a skincare product CSV."
    )
    ap.add_argument("--input",  "-i", required=True,  help="Input CSV path")
    ap.add_argument("--output", "-o", default="results.csv", help="Output CSV")
    ap.add_argument("--limit",  "-n", type=int, default=0,   help="Max products (0=all)")
    ap.add_argument("--resume", "-r", action="store_true",   help="Skip already done")
    args = ap.parse_args()

    products = load_products(args.input)
    if args.resume:
        done   = load_done(args.output)
        before = len(products)
        products = [p for p in products if p["product_id"] not in done]
        print(f"[RESUME] {before-len(products):,} already done, "
              f"{len(products):,} remaining")
    if args.limit:
        products = products[:args.limit]

    total        = len(products)
    counts       = {}
    fail_streak  = 0   # consecutive scrape_failed — signals bot block

    print(f"[START] {total:,} products → {args.output}\n")

    with AmazonScraper() as scraper:
        for i, row in enumerate(products, 1):
            print(f"[{i}/{total}] {row['brand_name']} — {row['product_name'][:55]}")

            # Periodic long break
            if i > 1 and (i - 1) % LONG_BREAK_EVERY == 0:
                scraper._long_break()

            # Restart browser every RESTART_EVERY products
            if i > 1 and (i - 1) % RESTART_EVERY == 0:
                print(f"  [RESTART] Refreshing browser session at product {i}...")
                scraper._launch_browser()

            r = scraper.scrape(row["product_id"], row["product_name"], row["brand_name"])
            append_result(r, args.output)
            counts[r.status] = counts.get(r.status, 0) + 1

            # Track consecutive failures (bot detection signal)
            if r.status == "scrape_failed" and r.error == "No results":
                fail_streak += 1
                if fail_streak >= BLOCK_STREAK_LIMIT:
                    print(f"\n  ⚠ {fail_streak} consecutive 'No results' — "
                          f"triggering block pause")
                    scraper._block_pause()
                    fail_streak = 0
            else:
                fail_streak = 0

    print(f"\n{'─'*55}")
    for k, v in sorted(counts.items()):
        print(f"  {k:20s}: {v:,}")
    print(f"  Output → {args.output}")
    print(f"{'─'*55}")

if __name__ == "__main__":
    main()