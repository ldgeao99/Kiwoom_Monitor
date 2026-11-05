from datetime import datetime, timezone
import unittest
from unittest.mock import patch
from test_amount_surge import server


def at(text):
    return datetime.fromisoformat(text)


def quote(time, price=100):
    return {'price':price, 'prev':100, 'pct':price-100, 'quote_time':time.timestamp()}


class NasdaqSessionTests(unittest.TestCase):
    def test_winter_opens_at_eight_kst(self):
        self.assertEqual(server._nasdaq_session_date(at('2026-11-06T07:59:59+09:00')), '2026-11-05')
        self.assertEqual(server._nasdaq_session_date(at('2026-11-06T08:00:00+09:00')), '2026-11-06')

    def test_summer_opens_at_seven_and_utc_matches(self):
        self.assertEqual(server._nasdaq_session_date(at('2026-07-06T06:59:59+09:00')), '2026-07-05')
        self.assertEqual(server._nasdaq_session_date(at('2026-07-06T07:00:00+09:00')), '2026-07-06')
        self.assertEqual(server._nasdaq_session_date(at('2026-07-06T07:00:00+09:00').astimezone(timezone.utc)), '2026-07-06')

    def test_stale_quote_does_not_reset_extremes_or_send(self):
        now = at('2026-11-06T08:05:00+09:00')
        old = {'date':'2026-11-05','low':90,'high':110,'up_armed':True,'down_armed':True}
        with patch.object(server, '_nasdaq_rev_state', old), \
                patch.object(server, 'get_nasdaq_future', return_value=quote(at('2026-11-06T06:59:00+09:00'),105)), \
                patch.object(server, 'send_telegram_message') as send, \
                patch.object(server, 'now_kst', return_value=now):
            server.maybe_check_nasdaq_reversal(now)
            self.assertEqual(old['date'], '2026-11-05')
            display = server.get_nasdaq_display()
            self.assertIsNone(display['low'])
            self.assertIsNone(display['high'])
            send.assert_not_called()

    def test_new_session_resets_and_reversal_rule_stays_half_percent(self):
        now = at('2026-11-06T08:11:00+09:00')
        state = {'date':'2026-11-05','low':50,'high':200}
        with patch.object(server, '_nasdaq_rev_state', state), \
                patch.object(server, 'get_nasdaq_future', return_value=quote(now)) as fetch, \
                patch.object(server, 'send_telegram_message') as send, \
                patch.object(server, 'now_kst', return_value=now):
            server.maybe_check_nasdaq_reversal(now)
            self.assertEqual((state['date'],state['low'],state['high']), ('2026-11-06',100,100))
            send.assert_not_called()
            fetch.return_value = quote(now,100.5)
            server.maybe_check_nasdaq_reversal(now)
            self.assertEqual(send.call_count, 1)
            self.assertIn('나스닥100선물 상승', send.call_args.args[0])
            server.maybe_check_nasdaq_reversal(now)
            self.assertEqual(send.call_count, 1)
            display = server.get_nasdaq_display()
            self.assertAlmostEqual(display['low'],0)
            self.assertAlmostEqual(display['high'],0.5)

    def test_unknown_or_future_timestamp_is_not_used(self):
        now = at('2026-11-06T08:05:00+09:00')
        self.assertFalse(server._nasdaq_current_session_quote({'price':100}, now))
        self.assertFalse(server._nasdaq_current_session_quote(quote(at('2026-11-06T08:06:00+09:00')), now))
