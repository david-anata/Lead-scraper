"""Tests for the outbound scoreboard (outbound_scoreboard.py)."""

from __future__ import annotations

import unittest

import outbound_scoreboard as sb


class TestComputeScoreboard(unittest.TestCase):
    def test_not_connected_when_no_stats(self):
        board = sb.compute_scoreboard(None)
        self.assertFalse(board.connected)
        self.assertIsNone(board.reply_rate)

    def test_rates(self):
        board = sb.compute_scoreboard(
            {"sent": 1000, "replies": 24, "positive_replies": 11, "bounces": 6},
        )
        self.assertTrue(board.connected)
        self.assertEqual(board.reply_rate, 2.4)
        self.assertEqual(board.positive_rate, 1.1)
        self.assertEqual(board.bounce_rate, 0.6)

    def test_emails_per_call_blank_without_calls(self):
        board = sb.compute_scoreboard({"sent": 1000, "replies": 10})
        self.assertIsNone(board.emails_per_booked_call)

    def test_emails_per_call_with_calls(self):
        board = sb.compute_scoreboard({"sent": 900, "replies": 10}, booked_calls=10)
        self.assertEqual(board.emails_per_booked_call, 90)

    def test_zero_sent_is_safe(self):
        board = sb.compute_scoreboard({"sent": 0, "replies": 0})
        self.assertIsNone(board.reply_rate)
        self.assertIsNone(board.emails_per_booked_call)

    def test_niches_sorted_by_positive_rate(self):
        board = sb.compute_scoreboard({
            "sent": 100, "replies": 2,
            "by_niche": [
                {"niche": "food", "sent": 100, "positive": 1},
                {"niche": "pets", "sent": 100, "positive": 3},
            ],
        })
        self.assertEqual(board.niches[0].niche, "pets")  # higher positive rate first


class TestGetScoreboard(unittest.TestCase):
    def test_no_key_not_connected(self):
        board = sb.get_scoreboard("", fetch=lambda k: None)
        self.assertFalse(board.connected)

    def test_fetch_error_is_not_connected(self):
        def boom(k):
            raise RuntimeError("api down")
        board = sb.get_scoreboard("x", fetch=boom)
        self.assertFalse(board.connected)
        self.assertIn("failed", board.reason)

    def test_happy_path(self):
        board = sb.get_scoreboard(
            "x",
            booked_calls=5,
            fetch=lambda k: {"sent": 500, "replies": 10, "positive_replies": 5, "bounces": 2},
        )
        self.assertTrue(board.connected)
        self.assertEqual(board.emails_per_booked_call, 100)


class TestRender(unittest.TestCase):
    def test_render_not_connected(self):
        board = sb.compute_scoreboard(None)
        page = sb.render_scoreboard_html(board)
        self.assertIn("not connected", page)
        self.assertIn("Outbound scoreboard", page)

    def test_render_connected(self):
        board = sb.compute_scoreboard(
            {"sent": 1000, "replies": 24, "positive_replies": 11, "bounces": 6},
            booked_calls=10,
        )
        page = sb.render_scoreboard_html(board)
        self.assertIn("2.4%", page)
        self.assertIn("per booked call", page)
        self.assertIn("1,000", page)


if __name__ == "__main__":
    unittest.main()
