"""Email newsletter HTML generator."""
from .models import Listing, PROPERTY_TYPE_LABELS
TYPE_COLORS = {
    "rv_park": "#2563eb", "campground": "#16a34a",
    "mhp": "#9333ea", "self_storage": "#ea580c", "marina": "#0891b2",
}
TYPE_ICONS = {
    "rv_park": "🚐", "campground": "⛺",
    "mhp": "🏠", "self_storage": "🏢", "marina": "⚓",
}


def nl_badge(text: str, color: str) -> str:
    return f'<span style="display:inline-block;background:{color};color:white;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:700;margin-right:4px">{text}</span>'


def nl_card(l: Listing) -> str:
    type_color = TYPE_COLORS.get(l.property_type, "#6b7280")
    icon = TYPE_ICONS.get(l.property_type, "🏘️")
    type_label = PROPERTY_TYPE_LABELS.get(l.property_type, l.property_type)
    badges = []
    if l.is_hot:
        badges.append(nl_badge("🔥 HOT", "#ef4444"))
    if l.is_new:
        badges.append(nl_badge("NEW", "#2563eb"))
    if l.price_reduced:
        badges.append(nl_badge("↓ PRICE REDUCED", "#f59e0b"))
    if l.seller_financing:
        badges.append(nl_badge("SELLER FIN.", "#16a34a"))
    if l.cap_rate >= 6:
        badges.append(nl_badge(f"{l.cap_rate:.1f}% CAP", "#7c3aed"))

    score_dots = "●" * l.score + "○" * (10 - l.score)
    meta_parts = [f"<b>{l.location}</b>", type_label, l.source]
    if l.lot_count:
        meta_parts.append(f"{l.lot_count} lots")
    if l.days_on_market:
        meta_parts.append(f"{l.days_on_market} DOM")

    contact_html = ""
    if l.contact_phone:
        contact_html += f'<span style="color:#6b7280">📞 {l.contact_phone}</span>'
    if l.contact_email:
        contact_html += f' &nbsp;✉️ <a href="mailto:{l.contact_email}" style="color:#2563eb">{l.contact_email}</a>'

    return f"""
<table width="100%" cellpadding="0" cellspacing="0" style="border:1px solid #e5e7eb;border-radius:8px;margin-bottom:12px;border-collapse:separate;border-left:4px solid {type_color}">
<tr>
<td style="padding:14px 16px">
  <div style="display:flex;justify-content:space-between">
    <div>
      <div style="font-size:15px;font-weight:700;color:#111827;margin-bottom:4px">
        {icon} <a href="{l.url}" style="color:#111827;text-decoration:none">{l.name}</a>
      </div>
      <div style="font-size:12px;color:#6b7280;margin-bottom:6px">{' · '.join(meta_parts)}</div>
      <div style="margin-bottom:6px">{"".join(badges)}</div>
      <div style="font-size:13px;color:#374151;line-height:1.5">{l.description[:220]}{"..." if len(l.description)>220 else ""}</div>
      {f'<div style="margin-top:6px;font-size:12px">{contact_html}</div>' if contact_html else ""}
    </div>
    <div style="text-align:right;padding-left:16px;min-width:120px">
      <div style="font-size:20px;font-weight:800;color:#111827;white-space:nowrap">{l.price_display}</div>
      <div style="font-size:11px;color:#6b7280;margin-top:4px;font-family:monospace;letter-spacing:1px">{score_dots}</div>
      <div style="font-size:11px;color:#6b7280">{l.score}/10</div>
    </div>
  </div>
</td>
</tr>
</table>"""


def generate_newsletter(listings: list[Listing], run_date: str, weekday: str) -> tuple[str, str]:
    """Returns (html_string, subject_line)."""
    hot = [l for l in listings if l.is_hot]
    new_listings = [l for l in listings if l.is_new]
    all_count = len(listings)
    hot_count = len(hot)
    new_count = len(new_listings)

    subject = f"Deal Scout — {weekday}, {run_date} — {all_count} deals, {new_count} new, {hot_count} hot"

    all_sorted = sorted(listings, key=lambda l: l.score, reverse=True)

    hot_section = ""
    if hot:
        cards = "".join(nl_card(l) for l in hot)
        hot_section = f"""
<tr><td style="padding:0 0 4px">
  <h2 style="font-size:16px;font-weight:700;color:#ef4444;margin:0 0 10px;padding-bottom:6px;border-bottom:2px solid #fecaca">
    🔥 Hot Deals ({hot_count})
  </h2>
  {cards}
</td></tr>"""

    all_section = ""
    non_hot = [l for l in all_sorted if not l.is_hot]
    if non_hot:
        cards = "".join(nl_card(l) for l in non_hot)
        all_section = f"""
<tr><td style="padding:0 0 4px">
  <h2 style="font-size:16px;font-weight:700;color:#374151;margin:16px 0 10px;padding-bottom:6px;border-bottom:2px solid #e5e7eb">
    📋 All Listings ({len(non_hot)})
  </h2>
  {cards}
</td></tr>"""

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f3f4f6;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f3f4f6">
<tr><td align="center" style="padding:24px 12px">
<table width="640" cellpadding="0" cellspacing="0" style="background:white;border-radius:12px;overflow:hidden;max-width:640px;width:100%">

<!-- Header -->
<tr><td style="background:linear-gradient(135deg,#1e3a5f,#2563eb);padding:24px 28px">
  <div style="color:white;font-size:24px;font-weight:900">🏕️ Deal Scout</div>
  <div style="color:rgba(255,255,255,0.8);font-size:13px;margin-top:4px">SE US · RV Parks · Campgrounds · MHP · Storage · Marinas</div>
  <div style="color:rgba(255,255,255,0.65);font-size:12px;margin-top:2px">{weekday}, {run_date}</div>
</td></tr>

<!-- Stats bar -->
<tr><td style="background:#1e3a5f;padding:10px 28px 14px">
  <table width="100%" cellpadding="0" cellspacing="0">
  <tr>
    <td align="center" style="color:white">
      <span style="font-size:22px;font-weight:800">{all_count}</span><br>
      <span style="font-size:11px;opacity:0.7">Deals</span>
    </td>
    <td align="center" style="color:#fca5a5">
      <span style="font-size:22px;font-weight:800">{hot_count}</span><br>
      <span style="font-size:11px;opacity:0.7">🔥 Hot</span>
    </td>
    <td align="center" style="color:#93c5fd">
      <span style="font-size:22px;font-weight:800">{new_count}</span><br>
      <span style="font-size:11px;opacity:0.7">New</span>
    </td>
    <td align="center" style="color:#86efac">
      <span style="font-size:22px;font-weight:800">{len([l for l in listings if l.seller_financing])}</span><br>
      <span style="font-size:11px;opacity:0.7">Seller Fin.</span>
    </td>
  </tr>
  </table>
</td></tr>

<!-- Body -->
<tr><td style="padding:20px 28px">
  <table width="100%" cellpadding="0" cellspacing="0">
    {hot_section}
    {all_section}
  </table>
</td></tr>

<!-- Footer -->
<tr><td style="background:#f9fafb;padding:16px 28px;border-top:1px solid #e5e7eb;text-align:center">
  <div style="font-size:12px;color:#9ca3af">
    Deal Scout · Sent from jace.agentic@gmail.com · {run_date}<br>
    <a href="https://dreamhammerai.github.io/deal-scout/" style="color:#2563eb">View Live Dashboard</a>
  </div>
</td></tr>

</table>
</td></tr>
</table>
</body>
</html>"""

    return html, subject
