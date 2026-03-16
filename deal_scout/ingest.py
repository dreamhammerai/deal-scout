"""Raw JSON → processed pipeline: validate, score, dedup, dashboard, newsletter."""
import json
import os
from datetime import datetime
from .models import Listing, is_direct_listing_url, parse_price
from .scorer import score_all
from .dedup import deduplicate
from .dashboard import generate_dashboard
from .newsletter import generate_newsletter

VALID_TYPES = {"rv_park", "campground", "mhp", "self_storage", "marina"}
TARGET_STATES = {"AL", "MS", "LA", "GA", "TN", "FL", "NC", "VA"}


def load_raw(path: str) -> list[dict]:
    with open(path) as f:
        return json.load(f)


def validate_and_build(raw: list[dict]) -> tuple[list[Listing], list[str]]:
    listings = []
    rejected = []

    for item in raw:
        url = item.get("url", "")
        name = item.get("name", "Unknown")

        # URL quality check
        if not is_direct_listing_url(url):
            rejected.append(f"REJECTED (bad URL): {name} — {url}")
            continue

        # State filter
        state = item.get("state", "").upper()
        if state not in TARGET_STATES:
            rejected.append(f"REJECTED (out-of-area state={state}): {name}")
            continue

        # Property type check
        ptype = item.get("property_type", "")
        if ptype not in VALID_TYPES:
            rejected.append(f"REJECTED (invalid type={ptype}): {name}")
            continue

        listing = Listing(
            name=name,
            location=item.get("location", ""),
            state=state,
            price=parse_price(item.get("price", 0)),
            url=url,
            source=item.get("source", ""),
            property_type=ptype,
            description=item.get("description", ""),
            days_on_market=int(item.get("days_on_market", 0)),
            cap_rate=float(item.get("cap_rate", 0)),
            lot_count=int(item.get("lot_count", 0)),
            seller_financing=bool(item.get("seller_financing", False)),
            price_reduced=bool(item.get("price_reduced", False)),
            contact_name=item.get("contact_name", ""),
            contact_email=item.get("contact_email", ""),
            contact_phone=item.get("contact_phone", ""),
        )
        listings.append(listing)

    return listings, rejected


def run_ingest_pipeline(raw_path: str, output_dir: str) -> tuple:
    """
    Main pipeline entry point.
    Returns (listings, dashboard_path, newsletter_html, subject)
    """
    now = datetime.now()
    run_date = now.strftime("%B %-d, %Y")
    weekday = now.strftime("%A")

    print(f"\n{'='*60}")
    print(f"  DEAL SCOUT PIPELINE — {weekday}, {run_date}")
    print(f"{'='*60}")

    # Step 1: Load raw listings
    raw = load_raw(raw_path)
    print(f"\n[1] Loaded {len(raw)} raw listings from {raw_path}")

    # Step 2: Validate & build
    listings, rejected = validate_and_build(raw)
    print(f"[2] Validated: {len(listings)} passed, {len(rejected)} rejected")
    for r in rejected:
        print(f"    {r}")

    # Step 3: Score
    listings = score_all(listings)
    hot_count = len([l for l in listings if l.is_hot])
    print(f"[3] Scored: {hot_count} hot deals (score ≥7)")
    for l in listings[:5]:
        print(f"    Score {l.score}/10 — {l.name}")

    # Step 4: Deduplicate
    listings, new_listings = deduplicate(listings, run_date)
    new_count = len(new_listings)
    print(f"[4] Dedup: {new_count} new listings today")

    # Step 5: Dashboard
    os.makedirs(output_dir, exist_ok=True)
    dash_path = generate_dashboard(listings, run_date, output_dir)
    print(f"[5] Dashboard → {dash_path}")

    # Step 6: Newsletter
    newsletter_html, subject = generate_newsletter(listings, run_date, weekday)
    nl_path = os.path.join(output_dir, "newsletter.html")
    with open(nl_path, "w") as f:
        f.write(newsletter_html)
    print(f"[6] Newsletter → {nl_path}")
    print(f"    Subject: {subject}")

    print(f"\n{'='*60}")
    print(f"  PIPELINE COMPLETE — {len(listings)} deals, {hot_count} hot, {new_count} new")
    print(f"{'='*60}\n")

    return listings, dash_path, newsletter_html, subject
