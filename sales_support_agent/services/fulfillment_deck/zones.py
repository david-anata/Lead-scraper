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


# Continental-US metros used to pick one representative destination per zone.
# Order matters: representative_destinations() takes the FIRST metro that
# lands in each zone, so the list is deterministic by construction.
REPRESENTATIVE_METROS: tuple = (
    ("84101", "Salt Lake City, UT"),
    ("80202", "Denver, CO"),
    ("85004", "Phoenix, AZ"),
    ("90012", "Los Angeles, CA"),
    ("94103", "San Francisco, CA"),
    ("98101", "Seattle, WA"),
    ("97201", "Portland, OR"),
    ("89101", "Las Vegas, NV"),
    ("83702", "Boise, ID"),
    ("87102", "Albuquerque, NM"),
    ("75201", "Dallas, TX"),
    ("77002", "Houston, TX"),
    ("78701", "Austin, TX"),
    ("73102", "Oklahoma City, OK"),
    ("64106", "Kansas City, MO"),
    ("55401", "Minneapolis, MN"),
    ("60601", "Chicago, IL"),
    ("63101", "St. Louis, MO"),
    ("38103", "Memphis, TN"),
    ("37203", "Nashville, TN"),
    ("30303", "Atlanta, GA"),
    ("33101", "Miami, FL"),
    ("32801", "Orlando, FL"),
    ("33602", "Tampa, FL"),
    ("28202", "Charlotte, NC"),
    ("27601", "Raleigh, NC"),
    ("20001", "Washington, DC"),
    ("19102", "Philadelphia, PA"),
    ("10001", "New York, NY"),
    ("02108", "Boston, MA"),
    ("15222", "Pittsburgh, PA"),
    ("44114", "Cleveland, OH"),
    ("43215", "Columbus, OH"),
    ("48226", "Detroit, MI"),
    ("46204", "Indianapolis, IN"),
    ("53202", "Milwaukee, WI"),
    ("70112", "New Orleans, LA"),
    ("78205", "San Antonio, TX"),
    ("92101", "San Diego, CA"),
    ("95814", "Sacramento, CA"),
)


def representative_destinations(origin_zip: str) -> dict:
    """One representative metro per reachable zone from ``origin_zip``.

    Returns ``{zone: (dest_zip, "City, ST")}`` for each zone 1-8 where some
    metro in REPRESENTATIVE_METROS lands in that zone (first match wins).
    Zones with no matching metro are simply absent. Deterministic — pure
    function of the origin and the static metro list.
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
