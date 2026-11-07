"""Regression tests for independent Herald boss/event polling."""
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import bot


MSK = timezone(timedelta(hours=3))


class FixedNov7(datetime):
    @classmethod
    def now(cls, tz=None):
        fixed = datetime(2026, 11, 7, 19, 40, tzinfo=MSK)
        return fixed.astimezone(tz) if tz is not None else fixed.replace(tzinfo=None)


class FixedNov8(datetime):
    @classmethod
    def now(cls, tz=None):
        fixed = datetime(2026, 11, 8, 0, 54, tzinfo=MSK)
        return fixed.astimezone(tz) if tz is not None else fixed.replace(tzinfo=None)


LORD_EVENT_HTML = """
<div class="flex flex-col">
  <span class="event-header">Владыка Наёмников</span>
  <span class="text-xs">19:38 07.11.26</span>
</div>
<p class="text-sm">Викинги победили огромное количество наёмников, чем
потревожили Владыку Наёмников. Готовьтесь к бою в Норлунге.</p>
"""


class IndependentEventsTests(unittest.TestCase):
    def test_lord_event_recognized_on_event_day(self):
        with patch.object(bot, "datetime", FixedNov7):
            parsed = bot.merc_lord_block(LORD_EVENT_HTML)
        self.assertEqual(parsed["city"], "Норлунг")
        self.assertEqual(parsed["when_str"], "19:38 07.11.26")

    def test_stale_event_not_reannounced_next_day(self):
        with patch.object(bot, "datetime", FixedNov8):
            parsed = bot.merc_lord_block(LORD_EVENT_HTML)
        self.assertIsNone(parsed["when_iso"])

    def test_lord_notification_when_monastic_broken_and_no_duplicate(self):
        delivered = []

        def fake_fetch(*args, **kwargs):
            if kwargs["url_path"] == "/monastic":
                raise bot.GameUnavailableError("monastic did not contain boss timers")
            if kwargs["url_path"] == "/events":
                with patch.object(bot, "datetime", FixedNov7):
                    parsed = bot.events_block(LORD_EVENT_HTML)
                return {**parsed, "domain": "wekings.online"}
            raise AssertionError("Unexpected path")

        def fake_send(token, chat_ids, text, **kwargs):
            delivered.append(text)
            return [{"chat_id": str(cid), "ok": True} for cid in chat_ids]

        with tempfile.TemporaryDirectory() as folder:
            state_path = Path(folder) / "notify_state.json"
            with (
                patch.object(bot, "datetime", FixedNov7),
                patch.object(bot, "fetch_and_parse", side_effect=fake_fetch),
                patch.object(bot, "tg_send", side_effect=fake_send),
            ):
                for _ in range(2):
                    with self.assertRaisesRegex(RuntimeError, "monastic"):
                        bot.notify_if_needed(
                            "fake-cookies.json", "fake-token", ["-123"],
                            state_file=state_path,
                        )
            state = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(state["lord"], "2026-11-07")
            self.assertEqual(state["active_domain"], "wekings.online")
            self.assertEqual(len(delivered), 1)
            self.assertIn("Владыка Наемников", delivered[0])
            self.assertIn("Норлунге", delivered[0])
            self.assertIn("20:38", delivered[0])

    def test_monastic_alert_not_sent_to_telegram(self):
        messages = []

        def fake_fetch(*args, **kwargs):
            if kwargs["url_path"] == "/monastic":
                raise bot.GameUnavailableError("Cannot parse /monastic")
            return {"city": None, "when_iso": None, "swimming_weather": [], "domain": "wekings.online"}

        def fake_send(token, chat_ids, text, **kwargs):
            messages.append(text)
            return [{"chat_id": str(cid), "ok": True} for cid in chat_ids]

        with tempfile.TemporaryDirectory() as folder:
            with (
                patch.object(bot, "datetime", FixedNov7),
                patch.object(bot, "fetch_and_parse", side_effect=fake_fetch),
                patch.object(bot, "tg_send", side_effect=fake_send),
            ):
                with self.assertRaisesRegex(RuntimeError, "monastic"):
                    bot.notify_if_needed(
                        "fake-cookies.json", "fake-token", ["-123"],
                        state_file=Path(folder) / "notify_state.json",
                    )
        self.assertEqual(messages, [])


if __name__ == "__main__":
    unittest.main()
