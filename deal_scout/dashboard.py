"""HTML dashboard generator."""
from .models import Listing, PROPERTY_TYPE_LABELS, STATE_LABELS

TYPE_COLORS = {
    "rv_park": "#2563eb",
    "campground": "#16a34a",
    "mhp": "#9333ea",
    "self_storage": "#ea580c",
    "marina": "#0891b2",
}

TYPE_ICONS = {
    "rv_park": "🚐",
    "campground": "⛺",
    "mhp": "🏠",
    "self_storage": "🏢",
    "marina": "⚓",
}


def score_bar(score: int) -> str:
    filled = "█" * score
    empty = "░" * (10 - score)
    color = "#ef4444" if score >= 7 else "#f59e0b" if score >= 5 else "#6b7280"
    return f'<span style="color:{color};letter-spacing:1px;font-family:monospace">{filled}{empty}</span>'


def badge(text: str, color: str) -> str:
    return f'<span style="background:{color};color:white;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:600;white-space:nowrap">{text}</span>'


def listing_card(l: Listing) -> str:
    type_color = TYPE_COLORS.get(l.property_type, "#6b7280")
    icon = TYPE_ICONS.get(l.property_type, "🏘️")
    type_label = PROPERTY_TYPE_LABELS.get(l.property_type, l.property_type)
    badges = []
    if l.is_hot:
        badges.append(badge("🔥 HOT DEAL", "#ef4444"))
    if l.is_new:
        badges.append(badge("NEW", "#2563eb"))
    if l.price_reduced:
        badges.append(badge("PRICE REDUCED", "#f59e0b"))
    if l.seller_financing:
        badges.append(badge("SELLER FINANCING", "#16a34a"))
    if l.cap_rate >= 6:
        badges.append(badge(f"{l.cap_rate:.1f}% CAP", "#7c3aed"))

    signals_html = ""
    if l.signals:
        signals_html = '<div style="margin-top:6px;font-size:12px;color:#6b7280">' + " · ".join(l.signals) + "</div>"

    cap_html = f'<span style="color:#7c3aed;font-weight:600">{l.cap_rate:.1f}% cap</span> · ' if l.cap_rate >= 1 else ""
    lots_html = f'<span>{l.lot_count} lots</span> · ' if l.lot_count > 0 else ""
    dom_html = f'<span>{l.days_on_market} DOM</span>' if l.days_on_market > 0 else ""

    return f"""
<div style="border:1px solid #e5e7eb;border-radius:10px;padding:16px;margin-bottom:12px;background:white;border-left:4px solid {type_color}">
  <div style="display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:8px">
    <div style="flex:1;min-width:0">
      <div style="font-weight:700;font-size:15px;color:#111827;margin-bottom:4px">
        {icon} <a href="{l.url}" target="_blank" style="color:#111827;text-decoration:none">{l.name}</a>
      </div>
      <div style="font-size:13px;color:#6b7280;margin-bottom:6px">{l.location} · {type_label} · {l.source}</div>
      <div style="display:flex;flex-wrap:wrap;gap:4px;margin-bottom:8px">{"".join(badges)}</div>
      <div style="font-size:13px;color:#374151">{l.description[:250]}{"..." if len(l.description)>250 else ""}</div>
      {signals_html}
    </div>
    <div style="text-align:right;min-width:130px">
      <div style="font-size:22px;font-weight:800;color:#111827">{l.price_display}</div>
      <div style="font-size:12px;color:#6b7280;margin-top:2px">{cap_html}{lots_html}{dom_html}</div>
      <div style="margin-top:8px">{score_bar(l.score)}</div>
      <div style="font-size:11px;color:#6b7280;margin-top:2px">Score: {l.score}/10</div>
      <div style="margin-top:10px">
        <a href="{l.url}" target="_blank" style="display:inline-block;background:{type_color};color:white;font-size:12px;font-weight:600;padding:6px 14px;border-radius:6px;text-decoration:none;white-space:nowrap">📞 Contact Listing</a>
      </div>
    </div>
  </div>
</div>"""


def generate_dashboard(listings: list[Listing], run_date: str, output_path: str) -> str:
    hot = [l for l in listings if l.is_hot]
    new = [l for l in listings if l.is_new and not l.is_hot]
    rest = [l for l in listings if not l.is_hot and not l.is_new]

    state_counts = {}
    type_counts = {}
    for l in listings:
        state_counts[l.state] = state_counts.get(l.state, 0) + 1
        type_counts[l.property_type] = type_counts.get(l.property_type, 0) + 1

    stats_html = "".join([
        f'<div style="background:#f3f4f6;border-radius:8px;padding:12px;text-align:center"><div style="font-size:24px;font-weight:800;color:#111827">{len(listings)}</div><div style="font-size:12px;color:#6b7280">Total Deals</div></div>',
        f'<div style="background:#fef2f2;border-radius:8px;padding:12px;text-align:center"><div style="font-size:24px;font-weight:800;color:#ef4444">{len(hot)}</div><div style="font-size:12px;color:#6b7280">🔥 Hot Deals</div></div>',
        f'<div style="background:#eff6ff;border-radius:8px;padding:12px;text-align:center"><div style="font-size:24px;font-weight:800;color:#2563eb">{len([l for l in listings if l.is_new])}</div><div style="font-size:12px;color:#6b7280">New Today</div></div>',
        f'<div style="background:#f0fdf4;border-radius:8px;padding:12px;text-align:center"><div style="font-size:24px;font-weight:800;color:#16a34a">{len([l for l in listings if l.seller_financing])}</div><div style="font-size:12px;color:#6b7280">Seller Fin.</div></div>',
    ])

    def section(title: str, items: list[Listing]) -> str:
        if not items:
            return ""
        cards = "".join(listing_card(l) for l in items)
        return f'<h2 style="font-size:18px;font-weight:700;color:#111827;margin:24px 0 12px;padding-bottom:6px;border-bottom:2px solid #e5e7eb">{title}</h2>{cards}'

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Deal Scout — {run_date}</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #f9fafb; color: #111827; }}
  .container {{ max-width: 900px; margin: 0 auto; padding: 20px; }}
  a {{ color: #2563eb; }}
</style>
</head>
<body>
<div class="container">
  <div style="background:linear-gradient(135deg,#1e3a5f,#2563eb);color:white;border-radius:12px;padding:24px;margin-bottom:20px">
    <div style="font-size:28px;font-weight:900;letter-spacing:-0.5px">🏕️ Deal Scout</div>
    <div style="font-size:14px;opacity:0.85;margin-top:4px">SE US — RV Parks · Campgrounds · MHP · Self-Storage · Marinas</div>
    <div style="font-size:12px;opacity:0.7;margin-top:4px">Run date: {run_date}</div>
  </div>
  <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:20px">{stats_html}</div>
  {section("🔥 Hot Deals (Score 7+)", hot)}
  {section("🆕 New Listings Today", new)}
  {section("📋 All Other Listings", rest)}
  <div style="text-align:center;padding:20px;font-size:12px;color:#9ca3af;margin-top:20px">
    Deal Scout · jace.agentic@gmail.com · {run_date}
  </div>
</div>
</body>
</html>"""

    import os
    os.makedirs(output_path, exist_ok=True)
    path = os.path.join(output_path, "index.html")
    with open(path, "w") as f:
        f.write(html)
    return path
