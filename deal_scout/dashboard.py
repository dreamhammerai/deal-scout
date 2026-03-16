"""Dashboard generator — injects new listings into existing index.html template."""
import json
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
        "contact":{
            "name": l.contact_name,
            "firm": l.source,
            "phone": l.contact_phone,
            "email": l.contact_email,
        },
        "snippet": (l.description or "")[:300],
        "tags": _make_tags(l),
    }
