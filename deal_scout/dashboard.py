"""Dashboard generator — injects new listings into existing index.html template."""
import json
import os
import re
import pathlib
import datetime
import logging
from .models import Listing

logger = logging.getLogger(__name__)

_REPO_ROOT = pathlib.Path(__file__).parent.parent
_INDEX_HTML = _REPO_ROOT / "index.html"

_TYPE_MAP = {
    "rv_park": "rv",
    "campground": "campground",
    "mhp": "mhp",
    "self_storage": "storage",
    "marina": "marina",
}


def _make_id(l: Listing) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", l.name.lower()).strip("-")[:50]
    return slug or "listing"


def _make_tags(l: Listing) -> list[str]:
    tags = []
    if l.seller_financing:
        tags.append("Seller Fin.")
    if l.price_reduced:
        tags.append("Price ↓")
    if l.cap_rate and l.cap_rate >= 8:
        tags.append(f"{l.cap_rate:.0f}% Cap")
    if l.lot_count and l.lot_count >= 50:
        tags.append(f"{l.lot_count} lots")
    if l.is_new:
        tags.append("New")
    return tags


def _listing_to_js(l: Listing) -> dict:
    return {
        "id": _make_id(l),
        "name": l.name,
        "location": l.location,
        "type": _TYPE_MAP.get(l.property_type, "rv"),
        "price": l.price,
        "priceText": l.price_display,
        "oldPrice": None,
        "lots": l.lot_count or None,
        "capRate": round(l.cap_rate, 1) if l.cap_rate else None,
        "dom": l.days_on_market or None,
        "sellerFinancing": l.seller_financing,
        "hasWaterfront": False,
        "nearInterstate": False,
        "nearTourism": False,
        "occupancy": None,
        "isNew": l.is_new,
        "isRepeat": False,
        "priceChanged": l.price_reduced,
        "isAuction": False,
        "score": l.score,
        "source": l.source,
        "url": l.url,
        "contact": {
            "name": l.contact_name,
            "firm": l.source,
            "phone": l.contact_phone,
            "email": l.contact_email,
        },
        "snippet": (l.description or "")[:300],
        "tags": _make_tags(l),
    }


def generate_dashboard(listings: list, run_date, output_dir: str) -> str:
    """Inject scored listings into index.html and write to output_dir/index.html.

    Args:
        listings:   list of Listing dataclass instances
        run_date:   datetime.date of the pipeline run
        output_dir: directory to write index.html (repo root for GitHub Pages)

    Returns:
        Absolute path to the written index.html
    """
    # Read the living index.html from the repo root (serves as our template)
    html = _INDEX_HTML.read_text(encoding="utf-8")

    # Convert Listing objects to plain JS-serialisable dicts
    js_listings = [_listing_to_js(l) for l in listings]
    json_str = json.dumps(js_listings, indent=2, ensure_ascii=False)

    # Swap out the static LISTINGS array with fresh pipeline data
    new_block = f"const LISTINGS = {json_str};"
    html_out, n_subs = re.subn(
        r"const LISTINGS\s*=\s*\[.*?\];",
        new_block,
        html,
        flags=re.DOTALL,
    )
    if n_subs == 0:
        logger.warning("generate_dashboard: LISTINGS block not found — appending script tag")
        html_out = html + f"\n<script>\n{new_block}\n</script>\n"

    # Update the run-date stamp in the page <title> / visible header
    if run_date:
        try:
            date_str = run_date.strftime("%-d %B %Y") if hasattr(run_date, "strftime") else str(run_date)
        except Exception:
            date_str = str(run_date)
        html_out = re.sub(
            r"(Deal Scout\s*[—–\-]\s*)[^<"'\n]+?(\d{4})",
            lambda m: m.group(1) + date_str,
            html_out,
        )

    # Write result; output_dir == REPO_ROOT in normal pipeline runs
    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, "index.html")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html_out)

    logger.info("generate_dashboard: wrote %s (%d listings)", out_path, len(listings))
    return out_path
