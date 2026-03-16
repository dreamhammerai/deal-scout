"""Deal scoring engine (1-10 scale) per SKILL.md specification."""
from .models import Listing


def score_listing(listing: Listing) -> tuple[int, list[str]]:
    """
    Score a listing 1-10 and return (score, signals_list).

    Financial Signals (max 8 pts):
    - Cap rate >= 8%: +3 | Cap rate 6-8%: +2
    - Motivated seller: +2 | Seller financing: +2
    - Price reduced: +1

    Price Change Bonus (max 2 pts):
    - Drop >= 15%: +2 | Drop 5-15%: +1

    Property Quality (max 4 pts):
    - 50+ lots: +2 | 20-49 lots: +1
    - Waterfront: +1 | Near interstate/tourism: +1
    - Occupancy >= 85%: +1

    Auction: +1
    Score 7+ = Hot Deal. Cap at 10.
    """
    score = 0
    reasons = []

    # --- Financial Signals ---
    if listing.cap_rate >= 8.0:
        score += 3
        reasons.append(f"Cap rate {listing.cap_rate:.1f}% (≥8%)")
    elif listing.cap_rate >= 6.0:
        score += 2
        reasons.append(f"Cap rate {listing.cap_rate:.1f}% (6-8%)")

    if "motivated_seller" in listing.signals:
        score += 2
        reasons.append("Motivated seller")

    if listing.seller_financing:
        score += 2
        reasons.append("Seller financing available")

    if listing.price_reduced:
        score += 1
        reasons.append("Price reduced / improved")

    # --- Lot Count / Property Quality ---
    if listing.lot_count >= 50:
        score += 2
        reasons.append(f"{listing.lot_count} lots (50+)")
    elif listing.lot_count >= 20:
        score += 1
        reasons.append(f"{listing.lot_count} lots (20-49)")

    if "waterfront" in listing.signals:
        score += 1
        reasons.append("Waterfront / water access")

    if "near_tourism" in listing.signals:
        score += 1
        reasons.append("Near interstate / tourism corridor")

    if "auction" in listing.signals:
        score += 1
        reasons.append("Auction")

    # Occupancy bonus — parse from description
    import re
    occ_match = re.search(r"(\d+)\s*%\s*occup", listing.description, re.I)
    if occ_match and int(occ_match.group(1)) >= 85:
        score += 1
        reasons.append(f"High occupancy ({occ_match.group(1)}%)")

    # --- Cap at 10 ---
    score = min(score, 10)

    # --- Hot deal flag ---
    listing.is_hot = score >= 7

    return score, reasons


def score_all(listings: list[Listing]) -> list[Listing]:
    for listing in listings:
        listing.score, listing.signals = score_listing(listing)
        listing.is_hot = listing.score >= 7
    return sorted(listings, key=lambda l: l.score, reverse=True)
