"""Regression checks for boss warning thresholds and stale-state restarts."""
import unittest

from bot import _notification_threshold


THRESHOLDS = [91 * 60, 46 * 60, 16 * 60]
WINDOW = 5 * 60


class BossNotificationThresholdTests(unittest.TestCase):
    def test_regular_warnings_are_sent(self):
        for seconds_left, expected in [
            (90 * 60, 91 * 60),
            (45 * 60, 46 * 60),
            (15 * 60, 16 * 60),
        ]:
            with self.subTest(seconds_left=seconds_left):
                self.assertEqual(
                    _notification_threshold(seconds_left, THRESHOLDS, WINDOW),
                    expected,
                )

    def test_late_warning_is_not_replayed_after_restart(self):
        # Screenshot regression: 72 minutes left after 90-minute alert.
        self.assertIsNone(
            _notification_threshold(72 * 60, THRESHOLDS, WINDOW)
        )

    def test_too_late_for_every_threshold(self):
        self.assertIsNone(
            _notification_threshold(29 * 60, THRESHOLDS, WINDOW)
        )

    def test_warn_only_after_threshold_was_crossed(self):
        self.assertIsNone(
            _notification_threshold(94 * 60, THRESHOLDS, WINDOW)
        )

    def test_window_boundary(self):
        self.assertEqual(
            _notification_threshold(86 * 60, THRESHOLDS, WINDOW),
            91 * 60,
        )
        self.assertIsNone(
            _notification_threshold(85 * 60 + 59, THRESHOLDS, WINDOW)
        )

    def test_no_warnings_after_spawn(self):
        self.assertIsNone(_notification_threshold(0, THRESHOLDS, WINDOW))
        self.assertIsNone(_notification_threshold(-1, THRESHOLDS, WINDOW))


if __name__ == "__main__":
    unittest.main()
