"""scraper.py — Deal Scout data acquisition layer.

Sources
-------
1. Crexi      — REST API (primary, most reliable in CI)
2. BizBuySell — HTML scraping with session warm-up
3. LoopNet    — HTML scraping with JSON-LD extraction

All results include a `state` key filtered to TARGET_STATES downstream.
"""
from __future__ import annotations

import json
import logging
import random
import re
import time
from typing import Optional

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TARGET_STATES = {"AL", "MS", "LA", "GA", "TN", "FL", "NC", "VA"}

STATE_MAP = {
    "alabama": "AL", "mississippi": "MS", "louisiana": "LA",
    "georgia": "GA", "tennessee": "TN", "florida": "FL",
    "north carolina": "NC", "virginia": "VA",
}

_STATE_SLUG_TO_ABBR = {
    "florida": "FL", "georgia": "GA", "tennessee": "TN",
    "north-carolina": "NC", "virginia": "VA",
    "alabama": "AL", "mississippi": "MS", "louisiana": "LA",
    "fl": "FL", "ga": "GA", "tn": "TN",
    "nc": "NC", "va": "VA", "al": "AL", "ms": "MS", "la": "LA",
}

_UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

# ---------------------------------------------------------------------------
# Shared HTTP helpers
# ---------------------------------------------------------------------------

def _new_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": random.choice(_UA_POOL),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
                  "image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "DNT": "1",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control": "max-age=0",
    })
    return s


def _jitter(lo: float = 1.0, hi: float = 3.0) -> None:
    time.sleep(random.uniform(lo, hi))


def _parse_price_text(text: str) -> int:
    if not text:
        return 0
    text = text.upper().replace(",", "").strip()
    m = re.search(r"\$?\s*([\d.]+)\s*([MK]?)", text)
    if not m:
        return 0
    val = float(m.group(1))
    suffix = m.group(2)
    if suffix == "M":
        val *= 1_000_000
    elif suffix == "K":
        val *= 1_000
    return int(val)


def _detect_state(text: str) -> str:
    """Extract 2-letter state abbreviation from a location string."""
    text_lower = text.lower()
    for name, abbr in STATE_MAP.items():
        if name in text_lower:
            return abbr
    # Handles "City, FL" or "City, FL 32801"
    m = re.search(r",\s*([A-Za-z]{2})(?:\s|\d|$)", text)
    if m:
        candidate = m.group(1).upper()
        if candidate in TARGET_STATES:
            return candidate
    return ""


# ===========================================================================
# Source 1: Crexi
# ===========================================================================

_CREXI_API = "https://api.crexi.com/assets"

_CREXI_PROP_TYPES: dict[str, list[str]] = {
    "rv_park":      ["RV Park / Campground", "Campground", "RV Park"],
    "mhp":          ["Mobile Home Park"],
    "self_storage": ["Self-Storage"],
    "marina":       ["Marina"],
}

_CREXI_STATE_ABBRS = ["FL", "GA", "TN", "NC", "VA", "AL", "MS", "LA"]


def scrape_crexi(max_results: int = 50) -> list[dict]:
    """Query Crexi's REST search API for each property type + state combo."""
    results: list[dict] = []
    seen_urls: set[str] = set()

    session = _new_session()
    session.headers.update({
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Origin": "https://www.crexi.com",
        "Referer": "https://www.crexi.com/",
        "x-crexi-client": "web",
    })

    # Warm up: get homepage cookies
    try:
        warm = requests.get(
            "https://www.crexi.com/",
            headers={"User-Agent": random.choice(_UA_POOL),
                     "Accept": "text/html,*/*"},
            timeout=12,
        )
        for k, v in warm.cookies.items():
            session.cookies.set(k, v)
        _jitter(1.0, 2.0)
    except Exception:
        pass

    for ptype, type_labels in _CREXI_PROP_TYPES.items():
        if len(results) >= max_results:
            break
        try:
            # Try bulk search across all target states first
            payload = {
                "filters": {
                    "propertyTypes": type_labels,
                    "states": _CREXI_STATE_ABBRS,
                    "transactionType": "sale",
                },
                "sort": "newest",
                "page": 1,
                "pageSize": min(max_results, 50),
            }
            resp = session.post(_CREXI_API, json=payload, timeout=25)

            if resp.status_code == 200:
                data = resp.json()
                assets = (
                    data.get("assets") or data.get("data") or
                    data.get("results") or data.get("items") or []
                )
                for asset in assets:
                    item = _crexi_asset_to_dict(asset, ptype)
                    if item and item["url"] not in seen_urls:
                        seen_urls.add(item["url"])
                        results.append(item)
                logger.info("Crexi %s bulk: %d assets", ptype, len(assets))
            else:
                logger.warning("Crexi %s: HTTP %s", ptype, resp.status_code)
                # Fallback: per-state queries
                for state in _CREXI_STATE_ABBRS:
                    try:
                        payload["filters"]["states"] = [state]
                        payload["pageSize"] = 10
                        r2 = session.post(_CREXI_API, json=payload, timeout=20)
                        if r2.status_code == 200:
                            d2 = r2.json()
                            for asset in (d2.get("assets") or d2.get("results") or []):
                                item = _crexi_asset_to_dict(asset, ptype)
                                if item and item["url"] not in seen_urls:
                                    seen_urls.add(item["url"])
                                    results.append(item)
                        _jitter(0.5, 1.5)
                    except Exception as e:
                        logger.debug("Crexi per-state %s/%s: %s", ptype, state, e)

            _jitter(1.0, 2.5)
        except Exception as e:
            logger.warning("Crexi %s error: %s", ptype, e)

    # If API calls all failed, try scraping HTML search pages
    if not results:
        logger.info("Crexi API returned 0 — trying HTML fallback")
        results.extend(_scrape_crexi_html(session, max_results, seen_urls))

    logger.info("Crexi total: %d listings", len(results))
    return results[:max_results]


def _crexi_asset_to_dict(asset: dict, ptype: str) -> Optional[dict]:
    asset_id = asset.get("id") or asset.get("assetId") or ""
    name = asset.get("name") or asset.get("title") or ""
    if not (name and asset_id):
        return None

    addr = asset.get("address") or {}
    if isinstance(addr, dict):
        city = addr.get("city", "")
        state = addr.get("state", "") or addr.get("stateCode", "")
        location = f"{city}, {state}".strip(", ")
    else:
        location = str(addr)
        state = _detect_state(location)

    state = (state or "").upper()[:2]

    price_raw = (
        asset.get("askingPrice") or asset.get("price") or
        asset.get("listPrice") or 0
    )
    try:
        price = int(float(str(price_raw).replace(",", "").replace("$", "")))
    except Exception:
        price = 0
    if not price:
        return None

    cap_rate = asset.get("capRate") or 0
    try:
        cap_rate = float(str(cap_rate).replace("%", ""))
    except Exception:
        cap_rate = 0.0

    lot_count = (
        asset.get("totalUnits") or asset.get("units") or
        asset.get("spaces") or asset.get("sites") or 0
    )
    try:
        lot_count = int(lot_count)
    except Exception:
        lot_count = 0

    broker = asset.get("broker") or {}

    return {
        "name": name,
        "location": location or f"{state}, Southeast US",
        "state": state,
        "price": price,
        "url": f"https://www.crexi.com/properties/{asset_id}",
        "source": "Crexi",
        "property_type": ptype,
        "description": str(asset.get("description") or asset.get("summary") or "")[:400],
        "days_on_market": int(asset.get("daysOnMarket") or 0),
        "cap_rate": cap_rate,
        "lot_count": lot_count,
        "seller_financing": bool(asset.get("sellerFinancing") or False),
        "price_reduced": bool(asset.get("priceReduced") or False),
        "contact_name": broker.get("name", "") if isinstance(broker, dict) else "",
        "contact_phone": broker.get("phone", "") if isinstance(broker, dict) else "",
        "contact_email": broker.get("email", "") if isinstance(broker, dict) else "",
    }


def _scrape_crexi_html(
    session: requests.Session,
    max_results: int,
    seen_urls: set,
) -> list[dict]:
    """Fallback: parse Crexi Next.js page data from __NEXT_DATA__ script tag."""
    results: list[dict] = []
    configs = [
        ("rv-parks", "FL", "rv_park"),
        ("mobile-home-parks", "FL", "mhp"),
        ("rv-parks", "GA", "rv_park"),
        ("rv-parks", "TN", "rv_park"),
    ]
    state_name_map = {
        "FL": "florida", "GA": "georgia", "TN": "tennessee",
        "NC": "north-carolina", "VA": "virginia",
    }
    for slug, state_abbr, ptype in configs:
        state_name = state_name_map.get(state_abbr, state_abbr.lower())
        url = f"https://www.crexi.com/properties/{slug}/{state_name}"
        try:
            resp = session.get(url, timeout=20)
            if resp.status_code != 200:
                _jitter(2, 4)
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            script = soup.find("script", {"id": "__NEXT_DATA__"})
            if script and script.string:
                nd = json.loads(script.string)
                pp = nd.get("props", {}).get("pageProps", {})
                assets = pp.get("assets") or pp.get("listings") or pp.get("results") or []
                for asset in assets:
                    item = _crexi_asset_to_dict(asset, ptype)
                    if item and item["url"] not in seen_urls:
                        seen_urls.add(item["url"])
                        results.append(item)
            _jitter(2, 4)
        except Exception as e:
            logger.warning("Crexi HTML %s/%s: %s", slug, state_abbr, e)
        if len(results) >= max_results:
            break
    return results


# ===========================================================================
# Source 2: BizBuySell
# ===========================================================================

_BBS_CONFIGS = [
    # (url_slug, property_type, [state_prefixes])
    ("rv-parks-campgrounds",    "rv_park",      ["fl", "ga", "tn", "nc", "va", "al"]),
    ("mobile-home-parks",       "mhp",          ["fl", "ga", "tn", "nc"]),
    ("self-storage-facilities", "self_storage",  ["fl", "ga", "tn"]),
    ("marinas-boat-dealers",    "marina",        ["fl", "nc", "va"]),
]


def scrape_bizbuysell(max_results: int = 50) -> list[dict]:
    results: list[dict] = []
    seen_urls: set[str] = set()
    session = _new_session()

    # Warm up
    try:
        session.get("https://www.bizbuysell.com/", timeout=12)
        _jitter(1.5, 2.5)
    except Exception:
        pass

    for slug, ptype, states in _BBS_CONFIGS:
        if len(results) >= max_results:
            break
        for state_slug in states:
            url = f"https://www.bizbuysell.com/{state_slug}/businesses-for-sale/{slug}/"
            try:
                resp = session.get(url, timeout=20)
                if resp.status_code != 200:
                    logger.warning("BBS %s/%s: HTTP %s", slug, state_slug, resp.status_code)
                    _jitter(2, 4)
                    continue

                soup = BeautifulSoup(resp.text, "html.parser")
                items = _parse_bbs_page(soup, ptype, state_slug.upper())
                added = 0
                for item in items:
                    if item["url"] not in seen_urls:
                        seen_urls.add(item["url"])
                        results.append(item)
                        added += 1
                logger.info("BBS %s/%s: +%d (total=%d)", slug, state_slug, added, len(results))
                _jitter(2.0, 4.0)

            except Exception as e:
                logger.warning("BBS %s/%s error: %s", slug, state_slug, e)
                _jitter(2, 4)

    logger.info("BizBuySell total: %d listings", len(results))
    return results[:max_results]


def _parse_bbs_page(soup: BeautifulSoup, ptype: str, state: str) -> list[dict]:
    results: list[dict] = []

    # BBS uses several different card selectors across redesigns
    cards = (
        soup.select("div[class*='listingCard']") or
        soup.select("article[class*='listing']") or
        soup.select("div[class*='listing-card']") or
        soup.select(".businessCard") or
        soup.select("li[class*='result']")
    )

    for card in cards:
        try:
            title_el = (
                card.select_one("h2, h3, h4") or
                card.select_one("[class*='title']") or
                card.select_one("[class*='name']")
            )
            if not title_el:
                continue
            name = title_el.get_text(strip=True)
            if not name or len(name) < 4:
                continue

            link = (
                card.select_one("a[href*='/business/']") or
                card.select_one("a[href*='/businesses-for-sale/']") or
                card.select_one("a[href]")
            )
            if not link:
                continue
            href = link.get("href", "")
            url = href if href.startswith("http") else f"https://www.bizbuysell.com{href}"
            # Skip search result pages — we want detail pages
            if not re.search(r"/business/|/\d{5,}[/-]", url):
                continue

            price_el = card.select_one("[class*='price'], [class*='asking']")
            price_text = price_el.get_text(strip=True) if price_el else ""
            price = _parse_price_text(price_text)
            if not price:
                continue

            loc_el = card.select_one(
                "[class*='location'], [class*='city'], [class*='state'], "
                "[class*='address']"
            )
            location = loc_el.get_text(strip=True) if loc_el else ""
            detected_state = _detect_state(location) or state

            desc_el = card.select_one("[class*='desc'], [class*='summary'], p")
            description = desc_el.get_text(strip=True)[:300] if desc_el else ""

            results.append({
                "name": name,
                "location": location or f"{detected_state}, Southeast US",
                "state": detected_state,
                "price": price,
                "url": url,
                "source": "BizBuySell",
                "property_type": ptype,
                "description": description,
                "days_on_market": 0,
                "cap_rate": 0.0,
                "lot_count": 0,
                "seller_financing": "seller financ" in description.lower(),
                "price_reduced": False,
                "contact_name": "",
                "contact_phone": "",
                "contact_email": "",
            })
        except Exception as e:
            logger.debug("BBS card error: %s", e)

    return results


# ===========================================================================
# Source 3: LoopNet
# ===========================================================================

_LOOPNET_CONFIGS = [
    ("rv-parks-campgrounds-for-sale", "rv_park"),
    ("mobile-home-parks-for-sale",    "mhp"),
    ("self-storage-for-sale",         "self_storage"),
]

_LOOPNET_STATES = [
    ("florida",        "FL"),
    ("georgia",        "GA"),
    ("tennessee",      "TN"),
    ("north-carolina", "NC"),
    ("virginia",       "VA"),
]


def scrape_loopnet(max_results: int = 30) -> list[dict]:
    results: list[dict] = []
    seen_urls: set[str] = set()
    session = _new_session()
    session.headers["Referer"] = "https://www.loopnet.com/"

    try:
        session.get("https://www.loopnet.com/", timeout=12)
        _jitter(2, 3)
    except Exception:
        pass

    for slug, ptype in _LOOPNET_CONFIGS:
        if len(results) >= max_results:
            break
        for state_slug, state_abbr in _LOOPNET_STATES[:3]:
            url = f"https://www.loopnet.com/{slug}/{state_slug}/"
            try:
                resp = session.get(url, timeout=20)
                if resp.status_code != 200:
                    logger.warning("LoopNet %s/%s: HTTP %s", slug, state_slug, resp.status_code)
                    _jitter(3, 5)
                    continue

                soup = BeautifulSoup(resp.text, "html.parser")
                added = 0

                # 1) JSON-LD structured data (most reliable)
                for script in soup.find_all("script", type="application/ld+json"):
                    try:
                        data = json.loads(script.string or "")
                        items = data if isinstance(data, list) else [data]
                        for item in items:
                            if item.get("@type") in (
                                "RealEstateListing", "Product", "ItemList"
                            ):
                                sub = item.get("itemListElement") or [item]
                                for s in sub:
                                    d = _loopnet_jsonld_to_dict(
                                        s.get("item", s), ptype, state_abbr
                                    )
                                    if d and d["url"] not in seen_urls:
                                        seen_urls.add(d["url"])
                                        results.append(d)
                                        added += 1
                    except Exception:
                        pass

                # 2) HTML property cards fallback
                cards = (
                    soup.select("[class*='PropertyCard']") or
                    soup.select("[class*='property-card']") or
                    soup.select("article[class*='result']")
                )
                for card in cards[:20]:
                    d = _loopnet_card_to_dict(card, ptype, state_abbr)
                    if d and d["url"] not in seen_urls:
                        seen_urls.add(d["url"])
                        results.append(d)
                        added += 1

                logger.info("LoopNet %s/%s: +%d", slug, state_slug, added)
                _jitter(3, 5)

            except Exception as e:
                logger.warning("LoopNet %s/%s error: %s", slug, state_slug, e)

    logger.info("LoopNet total: %d listings", len(results))
    return results[:max_results]


def _loopnet_jsonld_to_dict(item: dict, ptype: str, state: str) -> Optional[dict]:
    name = item.get("name") or ""
    url = item.get("url") or ""
    if not name or not url:
        return None
    url = url if url.startswith("http") else f"https://www.loopnet.com{url}"

    offer = item.get("offers") or {}
    price_raw = offer.get("price") if isinstance(offer, dict) else item.get("price", 0)
    try:
        price = int(float(str(price_raw).replace(",", "").replace("$", "")))
    except Exception:
        return None
    if not price:
        return None

    addr = item.get("address") or {}
    if isinstance(addr, dict):
        city = addr.get("addressLocality", "")
        st = addr.get("addressRegion", state)
        location = f"{city}, {st}".strip(", ")
    else:
        location = ""
        st = state

    detected_state = _detect_state(location) or st.upper()[:2]

    return {
        "name": name,
        "location": location or f"{detected_state}, Southeast US",
        "state": detected_state,
        "price": price,
        "url": url,
        "source": "LoopNet",
        "property_type": ptype,
        "description": str(item.get("description") or "")[:300],
        "days_on_market": 0,
        "cap_rate": 0.0,
        "lot_count": 0,
        "seller_financing": False,
        "price_reduced": False,
        "contact_name": "",
        "contact_phone": "",
        "contact_email": "",
    }


def _loopnet_card_to_dict(card, ptype: str, state: str) -> Optional[dict]:
    title_el = (
        card.select_one("h4, h3, h2") or
        card.select_one("[class*='title'], [class*='name']")
    )
    name = title_el.get_text(strip=True) if title_el else ""
    if not name:
        return None

    link = card.select_one("a[href]")
    if not link:
        return None
    href = link.get("href", "")
    url = href if href.startswith("http") else f"https://www.loopnet.com{href}"

    price_el = card.select_one("[class*='price'], [class*='Price']")
    price_text = price_el.get_text(strip=True) if price_el else ""
    price = _parse_price_text(price_text)
    if not price:
        return None

    loc_el = card.select_one(
        "[class*='location'], [class*='address'], [class*='city']"
    )
    location = loc_el.get_text(strip=True) if loc_el else ""
    detected_state = _detect_state(location) or state

    return {
        "name": name,
        "location": location or f"{detected_state}, Southeast US",
        "state": detected_state,
        "price": price,
        "url": url,
        "source": "LoopNet",
        "property_type": ptype,
        "description": "",
        "days_on_market": 0,
        "cap_rate": 0.0,
        "lot_count": 0,
        "seller_financing": False,
        "price_reduced": False,
        "contact_name": "",
        "contact_phone": "",
        "contact_email": "",
    }


# ===========================================================================
# Public entry point
# ===========================================================================

def run_scraper(max_per_source: int = 30) -> list[dict]:
    """
    Run all scrapers and return a combined deduplicated list of raw listing dicts.
    Called from run.py to populate raw_listings.json.
    """
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    all_raw: list[dict] = []
    seen: set[str] = set()

    sources = [
        ("Crexi",      scrape_crexi),
        ("BizBuySell", scrape_bizbuysell),
        ("LoopNet",    scrape_loopnet),
    ]

    for source_name, fn in sources:
        try:
            logger.info("=== Scraping %s ===", source_name)
            items = fn(max_per_source)
            before = len(all_raw)
            for item in items:
                url = item.get("url", "")
                if url and url not in seen:
                    seen.add(url)
                    all_raw.append(item)
            logger.info(
                "%s: added %d unique (total=%d)",
                source_name, len(all_raw) - before, len(all_raw),
            )
        except Exception as e:
            logger.error("%s scraper crashed: %s", source_name, e)

    logger.info("=== Total raw listings: %d ===", len(all_raw))
    return all_raw
