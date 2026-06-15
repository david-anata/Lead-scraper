"""USPS-style shipping zone estimation from 3-digit ZIP centroids.

Zones are derived from great-circle distance between the origin and
destination ZIP3 centroids using the standard USPS distance bands. This is
an approximation good enough for a rate sheet — real carrier zone charts
have per-lane quirks we deliberately ignore.
"""

from __future__ import annotations

import math
from typing import Optional

from .schema import clean_zip
from .zip3_centroids import ZIP3_CENTROIDS

_EARTH_RADIUS_MILES = 3958.8

# (max_miles, zone) — USPS-style distance bands, checked in order.
_ZONE_BANDS = (
    (50.0, 1),
    (150.0, 2),
    (300.0, 3),
    (600.0, 4),
    (1000.0, 5),
    (1400.0, 6),
    (1800.0, 7),
)
_MAX_ZONE = 8


def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in miles between two (lat, lon) points."""
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2
    return 2.0 * _EARTH_RADIUS_MILES * math.asin(math.sqrt(min(1.0, a)))


def zone_for(origin_zip: str, dest_zip: str) -> Optional[int]:
    """Estimated USPS-style zone (1-8) between two US ZIPs, or None.

    Returns None when either ZIP fails to normalize or its 3-digit prefix
    has no known centroid (unassigned / military prefixes).
    """
    origin = clean_zip(origin_zip)
    dest = clean_zip(dest_zip)
    if origin is None or dest is None:
        return None
    origin_centroid = ZIP3_CENTROIDS.get(origin[:3])
    dest_centroid = ZIP3_CENTROIDS.get(dest[:3])
    if origin_centroid is None or dest_centroid is None:
        return None
    miles = haversine_miles(origin_centroid[0], origin_centroid[1], dest_centroid[0], dest_centroid[1])
    for max_miles, zone in _ZONE_BANDS:
        if miles <= max_miles:
            return zone
    return _MAX_ZONE


# Continental-US metros used to sample destinations per zone. Spread across
# sub-regions (coastal + interior + mid-size) so most zones have 2-3 sample
# cities — a carrier with metro-specific coverage (e.g. UniUni) then surfaces
# in a zone if it serves ANY sampled city, not just the first one quoted.
#
# Order matters: the legacy single-city representative_destinations() takes
# the FIRST metro that lands in each zone, so the list stays deterministic by
# construction. Every ZIP must be ZIP3-resolvable via ZIP3_CENTROIDS (zone
# coverage from Anata HQ in Lehi UT spans 1-8). The near zones (1-3) are
# intentionally filled with real interior-mountain-west towns — they're thin
# from a Utah origin but still real, hoverable destinations.
REPRESENTATIVE_METROS: tuple = (
    # Zone 1 — Wasatch Front + southern UT.
    ("84101", "Salt Lake City, UT"),
    ("84601", "Provo, UT"),
    ("84401", "Ogden, UT"),
    ("84770", "St. George, UT"),
    # Zone 2 — eastern ID / eastern UT.
    ("83201", "Pocatello, ID"),
    ("84501", "Price, UT"),
    # Zone 3 — interior mountain west.
    ("83702", "Boise, ID"),
    ("83301", "Twin Falls, ID"),
    ("81501", "Grand Junction, CO"),
    ("89801", "Elko, NV"),
    # Zone 4 — Southwest + Front Range + NorCal + Pacific NW interior.
    ("80202", "Denver, CO"),
    ("85004", "Phoenix, AZ"),
    ("90012", "Los Angeles, CA"),
    ("94103", "San Francisco, CA"),
    ("89101", "Las Vegas, NV"),
    ("87102", "Albuquerque, NM"),
    ("95814", "Sacramento, CA"),
    ("85701", "Tucson, AZ"),
    ("89501", "Reno, NV"),
    ("59101", "Billings, MT"),
    # Zone 5 — Pacific NW coast, southern plains, upper midwest.
    ("98101", "Seattle, WA"),
    ("97201", "Portland, OR"),
    ("75201", "Dallas, TX"),
    ("73102", "Oklahoma City, OK"),
    ("64106", "Kansas City, MO"),
    ("55401", "Minneapolis, MN"),
    ("92101", "San Diego, CA"),
    ("99201", "Spokane, WA"),
    ("79901", "El Paso, TX"),
    # Zone 6 — Texas triangle, midwest, mid-south.
    ("77002", "Houston, TX"),
    ("78701", "Austin, TX"),
    ("60601", "Chicago, IL"),
    ("63101", "St. Louis, MO"),
    ("38103", "Memphis, TN"),
    ("37203", "Nashville, TN"),
    ("46204", "Indianapolis, IN"),
    ("53202", "Milwaukee, WI"),
    ("78205", "San Antonio, TX"),
    # Zone 7 — southeast + great lakes.
    ("30303", "Atlanta, GA"),
    ("28202", "Charlotte, NC"),
    ("15222", "Pittsburgh, PA"),
    ("44114", "Cleveland, OH"),
    ("43215", "Columbus, OH"),
    ("48226", "Detroit, MI"),
    ("70112", "New Orleans, LA"),
    # Zone 8 — Florida + eastern seaboard.
    ("33101", "Miami, FL"),
    ("32801", "Orlando, FL"),
    ("33602", "Tampa, FL"),
    ("27601", "Raleigh, NC"),
    ("20001", "Washington, DC"),
    ("19102", "Philadelphia, PA"),
    ("10001", "New York, NY"),
    ("02108", "Boston, MA"),
)


def representative_destinations(origin_zip: str) -> dict:
    """One representative metro per reachable zone from ``origin_zip``.

    Returns ``{zone: (dest_zip, "City, ST")}`` for each zone 1-8 where some
    metro in REPRESENTATIVE_METROS lands in that zone (first match wins).
    Zones with no matching metro are simply absent. Deterministic — pure
    function of the origin and the static metro list. The map's per-zone label
    keeps using this single-city helper.
    """
    found: dict = {}
    for dest_zip, label in REPRESENTATIVE_METROS:
        zone = zone_for(origin_zip, dest_zip)
        if zone is None or zone in found:
            continue
        found[zone] = (dest_zip, label)
        if len(found) == _MAX_ZONE:
            break
    return dict(sorted(found.items()))


def representative_destinations_multi(
    origin_zip: str, per_zone: int = 2, cap: int = 18
) -> dict:
    """Up to ``per_zone`` sample metros per reachable zone from ``origin_zip``.

    Returns ``{zone: [(dest_zip, "City, ST"), ...]}`` for each zone 1-8, the
    cities being those in REPRESENTATIVE_METROS that land in that zone (in list
    order, so deterministic). At most ``per_zone`` cities per zone, and at most
    ``cap`` destinations TOTAL across all zones — the cap protects generation
    latency (the real WMS quote call is ~7s each). The total cap is applied in
    a single round-robin pass across zones so coverage stays balanced rather
    than front-loaded onto the near zones.
    """
    # Bucket every resolvable metro into its zone, preserving list order.
    by_zone: dict = {}
    for dest_zip, label in REPRESENTATIVE_METROS:
        zone = zone_for(origin_zip, dest_zip)
        if zone is None:
            continue
        by_zone.setdefault(zone, []).append((dest_zip, label))

    # Cap per-zone first.
    for zone in by_zone:
        by_zone[zone] = by_zone[zone][:max(1, per_zone)]

    # Round-robin across zones (ascending) so the total cap trims evenly.
    result: dict = {zone: [] for zone in sorted(by_zone)}
    total = 0
    round_index = 0
    while total < cap:
        added_this_round = False
        for zone in sorted(by_zone):
            if round_index < len(by_zone[zone]):
                result[zone].append(by_zone[zone][round_index])
                total += 1
                added_this_round = True
                if total >= cap:
                    break
        if not added_this_round:
            break
        round_index += 1

    # Drop any zone left empty (shouldn't happen, but keep it tidy) and sort.
    return {zone: cities for zone, cities in sorted(result.items()) if cities}
