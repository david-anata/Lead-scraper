from __future__ import annotations

import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from sales_support_agent.services.fulfillment_deck.zip3_centroids import ZIP3_CENTROIDS
from sales_support_agent.services.fulfillment_deck.zones import (
    REPRESENTATIVE_METROS,
    haversine_miles,
    representative_destinations,
    zone_for,
)


class HaversineTests(unittest.TestCase):
    def test_zero_distance(self):
        self.assertAlmostEqual(haversine_miles(40.76, -111.89, 40.76, -111.89), 0.0)

    def test_known_distance_slc_to_nyc(self):
        # SLC <-> NYC is roughly 1,970 great-circle miles.
        miles = haversine_miles(40.76, -111.89, 40.75, -73.99)
        self.assertGreater(miles, 1800)
        self.assertLess(miles, 2100)

    def test_symmetry(self):
        a = haversine_miles(40.76, -111.89, 33.45, -112.07)
        b = haversine_miles(33.45, -112.07, 40.76, -111.89)
        self.assertAlmostEqual(a, b)


class ZoneForTests(unittest.TestCase):
    def test_local_zone_is_low(self):
        self.assertLessEqual(zone_for("84043", "84101"), 2)

    def test_cross_country_is_zone_8(self):
        self.assertEqual(zone_for("84043", "10001"), 8)

    def test_denver_mid_zone(self):
        self.assertIn(zone_for("84043", "80202"), (4, 5))

    def test_los_angeles_mid_zone(self):
        self.assertIn(zone_for("84043", "90012"), (4, 5))

    def test_invalid_zips_return_none(self):
        self.assertIsNone(zone_for("", "84101"))
        self.assertIsNone(zone_for("84043", ""))
        self.assertIsNone(zone_for("abc", "84101"))
        self.assertIsNone(zone_for("84043", "12"))
        # Assigned-looking but unknown prefix (000xx is not a real US prefix).
        self.assertIsNone(zone_for("84043", "00099"))

    def test_zone_range_bounds(self):
        for dest_zip, _label in REPRESENTATIVE_METROS:
            zone = zone_for("84043", dest_zip)
            self.assertIsNotNone(zone)
            self.assertGreaterEqual(zone, 1)
            self.assertLessEqual(zone, 8)


class CentroidDataTests(unittest.TestCase):
    def test_broad_coverage(self):
        self.assertGreater(len(ZIP3_CENTROIDS), 800)

    def test_all_keys_are_three_digit_strings(self):
        for key, value in ZIP3_CENTROIDS.items():
            self.assertEqual(len(key), 3)
            self.assertTrue(key.isdigit())
            lat, lon = value
            self.assertTrue(-90 <= lat <= 90)
            self.assertTrue(-180 <= lon <= 180)


class RepresentativeDestinationsTests(unittest.TestCase):
    def test_lehi_origin(self):
        dests = representative_destinations("84043")
        self.assertIsInstance(dests, dict)
        self.assertTrue(set(dests).issubset(set(range(1, 9))))
        self.assertIn(8, dests)
        for zone, value in dests.items():
            self.assertIsInstance(value, tuple)
            dest_zip, label = value
            self.assertEqual(zone_for("84043", dest_zip), zone)
            self.assertIn(",", label)

    def test_deterministic(self):
        self.assertEqual(
            representative_destinations("84043"),
            representative_destinations("84043"),
        )


if __name__ == "__main__":
    unittest.main()
