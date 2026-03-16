"""
HTTP-based scraper for Deal Scout.

Replaces browser/MCP-based scraping so the pipeline can run
headlessly in GitHub Actions (or any CI environment).

Targets:
  - RVParkStore.com
  - ParksAndPlaces.com
  - BizBuySell.com  (RV parks / campgrounds / MHP)
  - Crexi.com       (self-storage, MHP)
"""
import re
import json
import time
import logging
from dataclasses import dataclass
from typing import Optional

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

TARGET_STATES = {"AL", "MS", "LA", "GA", "TN", "FL", "NC", "VA"}

STATE_MAP = {
    "alabama": "AL", "mississippi": "MS", "louisiana": "LA",
    "georgia": "GA", "tennessee": "TN", "florida": "FL",
    "north carolina": "NC", "virginia": "VA",
    # abbreviations
    "al": "AL", "ms": "MS", "la": "LA", "ga": "GA",
    "tn": "TN", "fl": "FL", "nc": "NC", "va": "VA",
}


def _get(url: str, timeout: int = 15) -> Optional[BeautifulSoup]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        logger.warning(f"GET failed {url}: {e}")
        return None


def _detect_state(text: str) -> str:
    text_lower = text.lower()
    for name, abbr in STATE_MAP.items():
        if f", {name}" in text_lower or f" {name} " in text_lower or f", {abbr.lower()}" in text_lower:
            return abbr
    return ""


def _parse_price_text(text: str) -> int:
    """Parse price string like '$1.85M', '$295,000', 'Call for Price' → int."""
    if not text:
        return 0
    text = text.strip().replace(",", "").replace("$", "")
    m = re.search(r"([\d.]+)\s*([MmKk])?", text)
    if not m:
        return 0
    num = float(m.group(1))
    suffix = (m.group(2) or "").upper()
    if suffix == "M":
        num *= 1_000_000
    elif suffix == "K":
        num *= 1_000
    return int(num)


# ─────────────────────────────────────────────
# RVParkStore.com
# ─────────────────────────────────────────────

RVPS_SEARCH_URLS = [
    "https://www.rvparkstore.com/rv-parks-for-sale?state=FL",
    "https://www.rvparkstore.com/rv-parks-for-sale?state=GA",
    "https://www.rvparkstore.com/rv-parks-for-sale?state=TN",
    "https://www.rvparkstore.com/rv-parks-for-sale?state=NC",
    "https://www.rvparkstore.com/rv-parks-for-sale?state=AL",
    "https://www.rvparkstore.com/rv-parks-for-sale?state=LA",
    "https://www.rvparkstore.com/rv-parks-for-sale?state=VA",
]


def scrape_rvparkstore() -> list[dict]:
    results = []
    seen_urls = set()

    for search_url in RVPS_SEARCH_URLS:
        soup = _get(search_url)
        if not soup:
            continue

        # Listing cards: <div class="listing-card"> or <article>
        cards = soup.select(".listing-card, .park-listing, article.listing")
        if not cards:
            # Fallback: grab all links that look like direct listing URLs
            cards = []
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if re.search(r"/rv-parks/\d+", href):
                    full_url = href if href.startswith("http") else f"https://www.rvparkstore.com{href}"
                    if full_url not in seen_urls:
                        seen_urls.add(full_url)
                        detail = _scrape_rvparkstore_detail(full_url)
                        if detail:
                            results.append(detail)
            time.sleep(1)
            continue

        for card in cards:
            link = card.find("a", href=True)
            if not link:
                continue
            href = link["href"]
            if not re.search(r"/rv-parks/\d+", href):
                continue
            full_url = href if href.startswith("http") else f"https://www.rvparkstore.com{href}"
            if full_url in seen_urls:
                continue
            seen_urls.add(full_url)

            detail = _scrape_rvparkstore_detail(full_url)
            if detail:
                results.append(detail)
            time.sleep(0.8)

        time.sleep(1.5)

    logger.info(f"RVParkStore: scraped {len(results)} listings")
    return results


def _scrape_rvparkstore_detail(url: str) -> Optional[dict]:
    soup = _get(url)
    if not soup:
        return None

    title_el = soup.find("h1") or soup.find("h2")
    name = title_el.get_text(strip=True) if title_el else ""
    if not name:
        return None

    # Price
    price_el = soup.find(class_=re.compile(r"price|asking", re.I)) or \
               soup.find(string=re.compile(r"\$[\d,]+|\$\d+\.\d+[MmKk]"))
    price_text = price_el.get_text(strip=True) if hasattr(price_el, "get_text") else str(price_el or "")
    price = _parse_price_text(price_text)

    # Location / state
    loc_el = soup.find(class_=re.compile(r"location|city|address", re.I))
    location = loc_el.get_text(strip=True) if loc_el else ""
    state = _detect_state(location or name or url)
    if state not in TARGET_STATES:
        return None

    # Description
    desc_el = soup.find(class_=re.compile(r"description|details|summary", re.I))
    description = desc_el.get_text(" ", strip=True)[:600] if desc_el else ""

    # Lot count
    lot_match = re.search(r"(\d+)\s*(lots?|sites?|spaces?|pads?)", description, re.I)
    lot_count = int(lot_match.group(1)) if lot_match else 0

    # Days on market
    dom_el = soup.find(string=re.compile(r"days? on market|DOM", re.I))
    dom_text = dom_el.find_next(string=re.compile(r"\d+")) if dom_el else None
    dom_match = re.search(r"(\d+)", str(dom_text or ""))
    days_on_market = int(dom_match.group(1)) if dom_match else 0

    # Property type
    ptype = "rv_park"
    if re.search(r"campground|camp ground", name + description, re.I):
        ptype = "campground"

    return {
        "name": name,
        "url": url,
        "source": "RVParkStore",
        "property_type": ptype,
        "location": location,
        "state": state,
        "price": price,
        "description": description,
        "lot_count": lot_count,
        "days_on_market": days_on_market,
        "cap_rate": 0.0,
        "seller_financing": bool(re.search(r"seller financ", description, re.I)),
        "price_reduced": bool(re.search(r"price (reduc|improv)|make offer", description, re.I)),
        "contact_name": "",
        "contact_email": "",
        "contact_phone": "",
    }


# ─────────────────────────────────────────────
# ParksAndPlaces.com
# ─────────────────────────────────────────────

PNPLACES_URLS = [
    "https://www.parksandplaces.com/rv-parks-campgrounds-for-sale/",
    "https://www.parksandplaces.com/rv-parks-campgrounds-for-sale/page/2/",
    "https://www.parksandplaces.com/rv-parks-campgrounds-for-sale/page/3/",
]

SE_STATES_KEYWORDS = ["Florida", "Georgia", "Tennessee", "Alabama", "Louisiana",
                       "Mississippi", "North Carolina", "Virginia", " FL", " GA",
                       " TN", " AL", " LA", " MS", " NC", " VA"]


def scrape_parksandplaces() -> list[dict]:
    results = []
    seen_urls = set()

    for page_url in PNPLACES_URLS:
        soup = _get(page_url)
        if not soup:
            continue

        for a in soup.find_all("a", href=True):
            href = a["href"]
            # ParksAndPlaces listing URLs contain a long numeric suffix
            if not re.search(r"parksandplaces\.com/[^/]+-\d{5,}/?$", href):
                continue
            full_url = href if href.startswith("http") else f"https://www.parksandplaces.com{href}"
            if full_url in seen_urls:
                continue

            # Quick SE-state check from link text before full fetch
            link_text = a.get_text(" ", strip=True)
            parent_text = (a.parent.get_text(" ", strip=True) if a.parent else "")
            combined = (link_text + " " + parent_text)[:400]
            if not any(k in combined for k in SE_STATES_KEYWORDS):
                continue

            seen_urls.add(full_url)
            detail = _scrape_pnp_detail(full_url)
            if detail:
                results.append(detail)
            time.sleep(0.8)

        time.sleep(1.5)

    logger.info(f"ParksAndPlaces: scraped {len(results)} listings")
    return results


def _scrape_pnp_detail(url: str) -> Optional[dict]:
    soup = _get(url)
    if not soup:
        return None

    name = ""
    h1 = soup.find("h1")
    if h1:
        name = h1.get_text(strip=True)
    if not name:
        return None

    # State filter
    full_text = soup.get_text(" ", strip=True)
    state = _detect_state(full_text)
    if state not in TARGET_STATES:
        return None

    # Price
    price_el = soup.find(string=re.compile(r"\$[\d,]+|\$[\d.]+[MmKk]"))
    price = _parse_price_text(str(price_el or ""))

    # Description
    desc_el = (soup.find(class_=re.compile(r"description|content|entry", re.I)) or
               soup.find("article") or soup.find("main"))
    description = desc_el.get_text(" ", strip=True)[:600] if desc_el else full_text[:400]

    lot_match = re.search(r"(\d+)\s*(lots?|sites?|spaces?|pads?|units?)", description, re.I)
    lot_count = int(lot_match.group(1)) if lot_match else 0

    ptype = "rv_park"
    name_desc = (name + " " + description).lower()
    if re.search(r"campground|camp ground", name_desc):
        ptype = "campground"
    elif re.search(r"mobile home|mhp|manufactured", name_desc):
        ptype = "mhp"

    # Location string
    loc_el = soup.find(class_=re.compile(r"location|address|city", re.I))
    location = loc_el.get_text(strip=True) if loc_el else name

    return {
        "name": name,
        "url": url,
        "source": "ParksAndPlaces",
        "property_type": ptype,
        "location": location,
        "state": state,
        "price": price,
        "description": description,
        "lot_count": lot_count,
        "days_on_market": 0,
        "cap_rate": 0.0,
        "seller_financing": bool(re.search(r"seller financ", description, re.I)),
        "price_reduced": bool(re.search(r"price (reduc|improv)|make offer", description, re.I)),
        "contact_name": "",
        "contact_email": "",
        "contact_phone": "",
    }


# ─────────────────────────────────────────────
# BizBuySell.com
# ─────────────────────────────────────────────

BBS_SEARCH_URLS = [
    # RV Parks / Campgrounds
    "https://www.bizbuysell.com/rv-parks-and-campgrounds-for-sale/?q=l-AL",
    "https://www.bizbuysell.com/rv-parks-and-campgrounds-for-sale/?q=l-FL",
    "https://www.bizbuysell.com/rv-parks-and-campgrounds-for-sale/?q=l-GA",
    "https://www.bizbuysell.com/rv-parks-and-campgrounds-for-sale/?q=l-TN",
    "https://www.bizbuysell.com/rv-parks-and-campgrounds-for-sale/?q=l-NC",
    "https://www.bizbuysell.com/rv-parks-and-campgrounds-for-sale/?q=l-LA",
    "https://www.bizbuysell.com/rv-parks-and-campgrounds-for-sale/?q=l-VA",
    # Mobile Home Parks
    "https://www.bizbuysell.com/mobile-home-parks-for-sale/?q=l-FL",
    "https://www.bizbuysell.com/mobile-home-parks-for-sale/?q=l-GA",
    "https://www.bizbuysell.com/mobile-home-parks-for-sale/?q=l-AL",
]


def scrape_bizbuysell() -> list[dict]:
    results = []
    seen_urls = set()

    for search_url in BBS_SEARCH_URLS:
        soup = _get(search_url)
        if not soup:
            continue

        # BizBuySell listing links: /business-opportunity/.../NNNNNN/
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if not re.search(r"/business-(opportunity|for-sale|franchise)/[^/]+/\d+", href):
                continue
            full_url = href if href.startswith("http") else f"https://www.bizbuysell.com{href}"
            if full_url in seen_urls:
                continue
            seen_urls.add(full_url)

            detail = _scrape_bbs_detail(full_url)
            if detail:
                results.append(detail)
            time.sleep(0.8)

        time.sleep(1.5)

    logger.info(f"BizBuySell: scraped {len(results)} listings")
    return results


def _scrape_bbs_detail(url: str) -> Optional[dict]:
    soup = _get(url)
    if not soup:
        return None

    name_el = soup.find("h1") or soup.find(class_=re.compile(r"title|heading", re.I))
    name = name_el.get_text(strip=True) if name_el else ""
    if not name:
        return None

    full_text = soup.get_text(" ", strip=True)
    state = _detect_state(full_text)
    if state not in TARGET_STATES:
        return None

    # Price — look for "Asking Price" label
    price_el = soup.find(string=re.compile(r"asking price", re.I))
    if price_el:
        sibling = price_el.find_next(string=re.compile(r"\$[\d,]"))
        price_text = str(sibling or "")
    else:
        price_text = ""
    price = _parse_price_text(price_text)

    # Cash flow / cap rate hint
    cap_rate = 0.0
    cf_el = soup.find(string=re.compile(r"cash flow", re.I))
    if cf_el:
        cf_text = cf_el.find_next(string=re.compile(r"\$[\d,]")) or ""
        cf_val = _parse_price_text(str(cf_text))
        if cf_val > 0 and price > 0:
            cap_rate = round((cf_val / price) * 100, 1)

    # Description
    desc_el = soup.find(id="businessDescription") or \
              soup.find(class_=re.compile(r"description|details", re.I))
    description = desc_el.get_text(" ", strip=True)[:600] if desc_el else full_text[:400]

    lot_match = re.search(r"(\d+)\s*(lots?|sites?|spaces?|pads?|units?)", description, re.I)
    lot_count = int(lot_match.group(1)) if lot_match else 0

    name_lower = name.lower()
    ptype = "rv_park"
    if re.search(r"campground|fish camp|camp", name_lower):
        ptype = "campground"
    elif re.search(r"mobile home|mhp|manufactured", name_lower + description.lower()):
        ptype = "mhp"
    elif re.search(r"self.?storage|storage unit", name_lower + description.lower()):
        ptype = "self_storage"

    loc_el = soup.find(class_=re.compile(r"location|address|city|state", re.I))
    location = loc_el.get_text(strip=True) if loc_el else ""

    return {
        "name": name,
        "url": url,
        "source": "BizBuySell",
        "property_type": ptype,
        "location": location,
        "state": state,
        "price": price,
        "description": description,
        "lot_count": lot_count,
        "days_on_market": 0,
        "cap_rate": cap_rate,
        "seller_financing": bool(re.search(r"seller financ", description, re.I)),
        "price_reduced": bool(re.search(r"price (reduc|improv|drop)|make offer", description, re.I)),
        "contact_name": "",
        "contact_email": "",
        "contact_phone": "",
    }


# ─────────────────────────────────────────────
# Main scrape entry point
# ─────────────────────────────────────────────

def run_scraper(max_per_source: int = 30) -> list[dict]:
    """
    Run all scrapers and return a combined deduplicated list of raw listing dicts.
    Call this from run.py to populate raw_listings.json.
    """
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    all_raw = []
    seen = set()

    sources = [
        ("RVParkStore", scrape_rvparkstore),
        ("ParksAndPlaces", scrape_parksandplaces),
        ("BizBuySell", scrape_bizbuysell),
    ]

    for source_name, fn in sources:
        try:
            items = fn()[:max_per_source]
            before = len(all_raw)
            for item in items:
                url = item.get("url", "")
                if url and url not in seen:
                    seen.add(url)
                    all_raw.append(item)
            logger.info(f"{source_name}: added {len(all_raw) - before} unique listings")
        except Exception as e:
            logger.error(f"{source_name} scraper failed: {e}")

    logger.info(f"Total raw listings: {len(all_raw)}")
    return all_raw
