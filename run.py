#!/usr/bin/env python3
"""
Deal Scout — main entry point.

Usage:
  python run.py                  # Full run: scrape + ingest + dashboard + newsletter
  python run.py --replay         # Skip scraping, reuse data/raw_listings.json
  python run.py --dry-run        # Scrape + validate but don't write output files
  python run.py --email          # Send newsletter email after pipeline (requires Gmail env vars)

Environment variables (for --email):
  GMAIL_USER     - Gmail address to send from (e.g. jace.agentic@gmail.com)
  GMAIL_APP_PASS - Gmail App Password (16-char, no spaces)
  NOTIFY_EMAIL   - Recipient address (defaults to GMAIL_USER)
"""

import argparse
import json
import os
import smtplib
import sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

# Repo root = directory containing this script
REPO_ROOT = Path(__file__).parent
DATA_DIR = REPO_ROOT / "data"
OUTPUT_DIR = REPO_ROOT  # index.html goes to repo root for GitHub Pages


def parse_args():
    p = argparse.ArgumentParser(description="Deal Scout pipeline runner")
    p.add_argument("--replay", action="store_true",
                   help="Skip scraping; load from data/raw_listings.json")
    p.add_argument("--dry-run", action="store_true",
                   help="Run pipeline but skip file writes (validation test)")
    p.add_argument("--email", action="store_true",
                   help="Send newsletter email after pipeline completes")
    p.add_argument("--max-per-source", type=int, default=50,
                   help="Max listings to scrape per source (default: 50)")
    return p.parse_args()


def scrape_listings(max_per_source: int) -> list[dict]:
    """Run HTTP scrapers and return raw listing dicts."""
    print("\n[SCRAPER] Starting HTTP scrape...")
    from deal_scout.scraper import run_scraper
    listings = run_scraper(max_per_source=max_per_source)
    print(f"[SCRAPER] Collected {len(listings)} raw listings")
    return listings


def load_raw_listings() -> list[dict]:
    """Load previously saved raw listings for replay mode."""
    path = DATA_DIR / "raw_listings.json"
    if not path.exists():
        print(f"[ERROR] Replay mode: {path} not found. Run without --replay first.")
        sys.exit(1)
    with open(path) as f:
        data = json.load(f)
    print(f"[REPLAY] Loaded {len(data)} listings from {path}")
    return data


def save_raw_listings(listings: list[dict]):
    """Persist raw listings for audit trail and replay capability."""
    DATA_DIR.mkdir(exist_ok=True)
    path = DATA_DIR / "raw_listings.json"
    with open(path, "w") as f:
        json.dump(listings, f, indent=2)
    print(f"[SCRAPER] Saved raw listings → {path}")


def send_newsletter(html: str, subject: str):
    """Send newsletter via Gmail SMTP."""
    sender = os.environ.get("GMAIL_USER", "")
    password = os.environ.get("GMAIL_APP_PASS", "")
    recipient = os.environ.get("NOTIFY_EMAIL", sender)

    if not sender or not password:
        print("[EMAIL] Skipping: GMAIL_USER or GMAIL_APP_PASS not set.")
        return

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = recipient
    msg.attach(MIMEText(html, "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender, password)
            server.sendmail(sender, recipient, msg.as_string())
        print(f"[EMAIL] Newsletter sent → {recipient}")
    except Exception as e:
        print(f"[EMAIL] Failed to send: {e}")


def main():
    args = parse_args()

    # Step 1: Get raw listings
    if args.replay:
        raw = load_raw_listings()
    else:
        raw = scrape_listings(args.max_per_source)
        if not args.dry_run:
            save_raw_listings(raw)

    if args.dry_run:
        print(f"\n[DRY RUN] Would process {len(raw)} raw listings — no files written.")
        return

    # Step 2: Run ingest pipeline
    from deal_scout.ingest import run_ingest_pipeline

    raw_path = DATA_DIR / "raw_listings.json"
    # In replay mode the file already exists; otherwise we just saved it
    listings, dash_path, newsletter_html, subject = run_ingest_pipeline(
        raw_path=str(raw_path),
        output_dir=str(OUTPUT_DIR),
    )

    # Step 3: Optionally email the newsletter
    if args.email:
        send_newsletter(newsletter_html, subject)

    print(f"\n✅ Done — {len(listings)} deals processed.")
    print(f"   Dashboard: {dash_path}")


if __name__ == "__main__":
    main()
