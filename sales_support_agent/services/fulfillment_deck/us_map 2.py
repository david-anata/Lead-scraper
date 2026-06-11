"""US zone map for the rate sheet — a tile-grid cartogram, not a geo-accurate
projection. Each state is a rounded square placed on the familiar NPR-style
grid and colored by its USPS-style zone from the ship-from origin. Inline SVG:
no JS, prints perfectly, and stays deterministic.
"""

from __future__ import annotations

import html as _html

# state -> (col, row, representative_zip, full_name). Grid reads like the US.
STATE_TILES: dict[str, tuple[int, int, str, str]] = {
    "AK": (0, 0, "99501", "Alaska"),
    "ME": (11, 0, "04101", "Maine"),
    "VT": (10, 1, "05401", "Vermont"),
    "NH": (11, 1, "03101", "New Hampshire"),
    "WA": (1, 2, "98101", "Washington"),
    "ID": (2, 2, "83702", "Idaho"),
    "MT": (3, 2, "59601", "Montana"),
    "ND": (4, 2, "58102", "North Dakota"),
    "MN": (5, 2, "55401", "Minnesota"),
    "IL": (6, 2, "60601", "Illinois"),
    "WI": (7, 2, "53202", "Wisconsin"),
    "MI": (8, 2, "48226", "Michigan"),
    "NY": (9, 2, "10001", "New York"),
    "MA": (10, 2, "02108", "Massachusetts"),
    "RI": (11, 2, "02903", "Rhode Island"),
    "OR": (1, 3, "97201", "Oregon"),
    "NV": (2, 3, "89101", "Nevada"),
    "WY": (3, 3, "82001", "Wyoming"),
    "SD": (4, 3, "57104", "South Dakota"),
    "IA": (5, 3, "50309", "Iowa"),
    "IN": (6, 3, "46204", "Indiana"),
    "OH": (7, 3, "43215", "Ohio"),
    "PA": (8, 3, "19102", "Pennsylvania"),
    "NJ": (9, 3, "07102", "New Jersey"),
    "CT": (10, 3, "06103", "Connecticut"),
    "CA": (1, 4, "90012", "California"),
    "UT": (2, 4, "84101", "Utah"),
    "CO": (3, 4, "80202", "Colorado"),
    "NE": (4, 4, "68102", "Nebraska"),
    "MO": (5, 4, "63101", "Missouri"),
    "KY": (6, 4, "40202", "Kentucky"),
    "WV": (7, 4, "25301", "West Virginia"),
    "VA": (8, 4, "23219", "Virginia"),
    "MD": (9, 4, "21201", "Maryland"),
    "DE": (10, 4, "19801", "Delaware"),
    "AZ": (2, 5, "85004", "Arizona"),
    "NM": (3, 5, "87102", "New Mexico"),
    "KS": (4, 5, "67202", "Kansas"),
    "AR": (5, 5, "72201", "Arkansas"),
    "TN": (6, 5, "37203", "Tennessee"),
    "NC": (7, 5, "28202", "North Carolina"),
    "SC": (8, 5, "29201", "South Carolina"),
    "DC": (9, 5, "20001", "Washington, DC"),
    "OK": (3, 6, "73102", "Oklahoma"),
    "LA": (4, 6, "70112", "Louisiana"),
    "MS": (5, 6, "39201", "Mississippi"),
    "AL": (6, 6, "35203", "Alabama"),
    "GA": (7, 6, "30303", "Georgia"),
    "HI": (0, 7, "96813", "Hawaii"),
    "TX": (3, 7, "75201", "Texas"),
    "FL": (7, 7, "33101", "Florida"),
}

# Zone 1 (closest) -> zone 8 (farthest): brand sky ramping into brand ink.
ZONE_COLORS = {
    1: "#e7f1f9",
    2: "#cfe3f2",
    3: "#aed1e8",
    4: "#85bbda",
    5: "#5f9cc7",
    6: "#4f84c4",
    7: "#33598f",
    8: "#1d2d44",
}

_TILE = 46
_GAP = 6


def render_zone_tile_map(origin_zip: str) -> str:
    """Inline SVG cartogram of zones 1-8 from `origin_zip`, with legend."""
    from sales_support_agent.services.fulfillment_deck.zones import zone_for

    cols = max(col for col, _, _, _ in STATE_TILES.values()) + 1
    rows = max(row for _, row, _, _ in STATE_TILES.values()) + 1
    width = cols * (_TILE + _GAP) + _GAP
    height = rows * (_TILE + _GAP) + _GAP + 36  # legend strip below

    tiles: list[str] = []
    origin_state = ""
    for abbr, (col, row, rep_zip, name) in STATE_TILES.items():
        zone = zone_for(origin_zip, rep_zip)
        color = ZONE_COLORS.get(zone or 0, "#eee9dc")
        x = _GAP + col * (_TILE + _GAP)
        y = _GAP + row * (_TILE + _GAP)
        text_fill = "#1d2d44" if (zone or 9) <= 4 else "#fffdf9"
        is_origin = zone is not None and zone <= 1 and not origin_state
        if is_origin:
            origin_state = abbr
        ring = (
            f'<rect x="{x - 2.5}" y="{y - 2.5}" width="{_TILE + 5}" height="{_TILE + 5}" rx="12" '
            f'fill="none" stroke="#bfa889" stroke-width="2.5"/>'
            if is_origin
            else ""
        )
        title = f"{name} — zone {zone}" if zone else name
        tiles.append(
            f'<g><title>{_html.escape(title)}</title>'
            f'{ring}'
            f'<rect x="{x}" y="{y}" width="{_TILE}" height="{_TILE}" rx="10" fill="{color}"/>'
            f'<text x="{x + _TILE / 2}" y="{y + _TILE / 2 + 4}" text-anchor="middle" '
            f'font-size="13" font-weight="600" fill="{text_fill}">{abbr}</text>'
            f"</g>"
        )

    legend_y = rows * (_TILE + _GAP) + _GAP + 22
    legend: list[str] = [
        f'<text x="{_GAP}" y="{legend_y + 11}" font-size="11" fill="#6b7688">Zone</text>'
    ]
    swatch = 30
    for zone in range(1, 9):
        x = _GAP + 40 + (zone - 1) * (swatch + 4)
        text_fill = "#1d2d44" if zone <= 4 else "#fffdf9"
        legend.append(
            f'<rect x="{x}" y="{legend_y}" width="{swatch}" height="16" rx="4" fill="{ZONE_COLORS[zone]}"/>'
            f'<text x="{x + swatch / 2}" y="{legend_y + 12}" text-anchor="middle" font-size="10" '
            f'font-weight="600" fill="{text_fill}">{zone}</text>'
        )

    return (
        f'<svg viewBox="0 0 {width} {height}" role="img" '
        f'aria-label="US shipping zones from ZIP {_html.escape(str(origin_zip))}" '
        f'style="width:100%;max-width:680px;height:auto;font-family:inherit;">'
        + "".join(tiles)
        + "".join(legend)
        + "</svg>"
    )
