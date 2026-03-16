"""Data models, signal detection, and price parsing."""
import re
from dataclasses import dataclass, field
from typing import Optional

PROPERTY_TYPE_LABELS = {
    "rv_park": "RV Park",
    "campground": "Campground",
    "mhp": "Mobile Home Park",
    "self_storage": "Self-Storage",
    "marina": "Marina",
}

STATE_LABELS = {
    "AL": "Alabama", "MS": "Mississippi", "LA": "Louisiana",
    "GA": "Georgia", "TN": "Tennessee", "FL": "Florida",
    "NC": "North Carolina", "VA": "Virginia",
}

@dataclass
class Listing:
    name: str
    location: str
    state: str
    price: int
    url: str
    source: str
    property_type: str
    description: str
    days_on_market: int = 0
    cap_rate: float = 0.0
    lot_count: int = 0
    seller_financing: bool = False
    price_reduced: bool = False
    contact_name: str = ""
    contact_email: str = ""
    contact_phone: str = ""
    score: int = 0
    signals: list = field(default_factory=list)
    is_hot: bool = False
    is_new: bool = False
    price_display: str = ""

    def __post_init__(self):
        self.price_display = format_price(self.price)
        # Detect signals from description
        desc_lower = (self.description + " " + self.name).lower()
        if not self.seller_financing and re.search(r"seller financ", desc_lower):
            self.seller_financing = True
        if not self.price_reduced and re.search(r"price (reduc|improv|drop)|make offer", desc_lower):
            self.price_reduced = True
        if re.search(r"\bnew\b.*listing|new listing", desc_lower):
            self.is_new = True
        if re.search(r"motivat|partner exit|must sell|retiring|liquidat", desc_lower):
            self.signals.append("motivated_seller")
        if re.search(r"waterfront|riverfront|lakefront|marina|lake|river|coastal|gulf|ocean", desc_lower):
            self.signals.append("waterfront")
        if re.search(r"interstate|i-\d+|near.*highway|tourism|tourist|near.*beach|gulf|mountain resort", desc_lower):
            self.signals.append("near_tourism")
        if re.search(r"auction", desc_lower):
            self.signals.append("auction")

    def to_dict(self):
        return {
            "name": self.name,
            "location": self.location,
            "state": self.state,
            "price": self.price,
            "price_display": self.price_display,
            "url": self.url,
            "source": self.source,
            "property_type": self.property_type,
            "property_type_label": PROPERTY_TYPE_LABELS.get(self.property_type, self.property_type),
            "description": self.description,
            "days_on_market": self.days_on_market,
            "cap_rate": self.cap_rate,
            "lot_count": self.lot_count,
            "seller_financing": self.seller_financing,
            "price_reduced": self.price_reduced,
            "contact_name": self.contact_name,
            "contact_email": self.contact_email,
            "contact_phone": self.contact_phone,
            "score": self.score,
            "signals": self.signals,
            "is_hot": self.is_hot,
            "is_new": self.is_new,
        }


def format_price(price: int) -> str:
    if price == 0:
        return "Call for Price"
    if price >= 1_000_000:
        return f"${price/1_000_000:.2f}M".rstrip('0').rstrip('.')  + "M" if not f"${price/1_000_000:.2f}M".endswith('M') else f"${price/1_000_000:.2f}M"
    if price >= 1000:
        return f"${price:,}"
    return f"${price}"


def parse_price(val) -> int:
    if isinstance(val, (int, float)):
        return int(val)
    if not val:
        return 0
    s = str(val).replace(",", "").replace("$", "").strip()
    m = re.search(r"[\d.]+", s)
    if not m:
        return 0
    num = float(m.group())
    if "m" in s.lower():
        num *= 1_000_000
    elif "k" in s.lower():
        num *= 1_000
    return int(num)


def is_direct_listing_url(url: str) -> bool:
    """Validate that URL is a direct listing page (not a search/category page)."""
    if not url or not url.startswith("http"):
        return False
    patterns = [
        r"crexi\.com/properties/\d+",
        r"bizbuysell\.com/(business-for-sale|business-opportunity|franchise-for-sale)/[^/]+/\d+",
        r"rvparkstore\.com/rv-parks/\d+",
        r"loopnet\.com/Listing/\d+",
        r"parksandplaces\.com/[^/]+-\d{5,}",
    ]
    for p in patterns:
        if re.search(p, url):
            return True
    # Also accept any URL with a long numeric segment (generic)
    if re.search(r"/\d{6,}", url):
        return True
    return False
