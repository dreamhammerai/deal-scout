"""Cross-day deduplication using a seen-URL index file."""
import json
import os
import pathlib
from .models import Listing

# Repo-relative path — works both locally and in GitHub Actions CI
_REPO_ROOT = pathlib.Path(__file__).parent.parent
SEEN_INDEX_FILE = str(_REPO_ROOT / "data" / "seen_urls.json")


def load_seen_index() -> dict:
    if os.path.exists(SEEN_INDEX_FILE):
        try:
            with open(SEEN_INDEX_FILE) as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_seen_index(index: dict):
    os.makedirs(os.path.dirname(SEEN_INDEX_FILE), exist_ok=True)
    with open(SEEN_INDEX_FILE, "w") as f:
        json.dump(index, f, indent=2)


def deduplicate(listings: list[Listing], run_date: str) -> tuple[list[Listing], list[Listing]]:
    """
    Returns (all_listings_with_new_flag, new_listings_only).
    Marks listings as new if not seen before, updates seen index.
    """
    seen = load_seen_index()
    new_listings = []

    for listing in listings:
        url = listing.url
        if url not in seen:
            listing.is_new = True
            seen[url] = {"first_seen": run_date, "name": listing.name}
            new_listings.append(listing)
        else:
            listing.is_new = False

    save_seen_index(seen)
    return listings, new_listings
