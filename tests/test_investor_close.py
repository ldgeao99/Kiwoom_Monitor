from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from test_amount_surge import server


NOW = datetime.fromisoformat('2026-11-03T16:00:00+09:00')


class InvestorCloseMessageTests(unittest.TestCase):
    def setUp(self):
        self.row = {key: '0' for key, _ in server.INVESTOR_CLOSE_FIELDS}
        self.row.update(inds_cd='001_AL', ind_netprps='-1234', frgnr_netprps='+2,345',
                        orgn_netprps='-1111', sc_netprps='123.5', invtrt_netprps='-50.5')
        self.token = patch.object(server, 'cached_token', return_value='test')
        self.fetch = patch.object(server, 'fetch_row', return_value=self.row)
        self.token.start()
        self.fetch_mock = self.fetch.start()
        self.addCleanup(self.token.stop)
        self.addCleanup(self.fetch.stop)

    def test_exact_order_blank_line_signs_and_rounding(self):
        self.assertEqual(server.build_investor_close_message(NOW),
                         '🟢 코스피 투자자별 매매동향(2026-11-03, 16:00)\n\n'
                         '투자주체 | 순매수\n'
                         '개인 -1,234억\n외국인 +2,345억\n기관 -1,111억\n'
                         '금융투자 +124억\n투신 -51억\n연기금 +0억\n기타법인 +0억\n\n'
                         '보험 +0억\n기타금융 +0억\n은행 +0억\n사모펀드 +0억')
        self.fetch_mock.assert_called_once_with('test', '20261103', mrkt_tp='0', inds_cd='001_AL')

    def test_wrong_sector_is_rejected(self):
        self.row['inds_cd'] = '101_AL'
        with self.assertRaisesRegex(RuntimeError, '코스피 종합'):
            server.build_investor_close_message(NOW)

    def test_incomplete_or_invalid_values_are_not_zero(self):
        for value in (None, '', 'not-a-number'):
            self.row['bank_netprps'] = value
            with self.subTest(value=value), self.assertRaisesRegex(RuntimeError, '은행'):
                server.build_investor_close_message(NOW)

    def test_no_token_does_not_query(self):
        with patch.object(server, 'cached_token', return_value=None):
            with self.assertRaisesRegex(RuntimeError, '인증 실패'):
                server.build_investor_close_message(NOW)
        self.fetch_mock.assert_not_called()


class InvestorCloseScheduleTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        patches = [patch.object(server, 'DATA_DIR', self.temp.name),
                   patch.object(server, '_investor_close_sent_day', None),
                   patch.object(server, 'TELEGRAM_BOT_TOKEN', 'test'),
                   patch.object(server, 'TELEGRAM_CHAT_ID', 'test'),
                   patch.object(server, 'build_investor_close_message', return_value='investors'),
                   patch.object(server, 'send_telegram_message', return_value=True)]
        mocks = [p.start() for p in patches]
        for p in patches:
            self.addCleanup(p.stop)
        self.build, self.send = mocks[-2:]
        self.state = Path(self.temp.name) / '.kospi_investor_close_sent'

    def test_time_market_days_and_kst_conversion(self):
        for date in ('2026-11-03T15:59:00+09:00', '2026-11-03T17:00:00+09:00',
                     '2026-11-07T16:00:00+09:00', '2026-12-25T16:00:00+09:00'):
            server.maybe_send_investor_close(datetime.fromisoformat(date))
        self.send.assert_not_called()
        server.maybe_send_investor_close(datetime(2026, 11, 3, 7, tzinfo=timezone.utc))
        self.build.assert_called_once_with(NOW)
        self.send.assert_called_once_with('investors')

    def test_restart_dedup_and_next_trading_day(self):
        server.maybe_send_investor_close(NOW)
        server.maybe_send_investor_close(NOW.replace(minute=1))
        server._investor_close_sent_day = None
        server.maybe_send_investor_close(NOW.replace(minute=2))
        self.assertEqual(self.send.call_count, 1)
        server.maybe_send_investor_close(NOW.replace(day=4))
        self.assertEqual(self.send.call_count, 2)
        self.assertEqual(self.state.read_text(), '2026-11-04')

    def test_failed_delivery_retries_without_marking_sent(self):
        self.send.side_effect = [False, True]
        server.maybe_send_investor_close(NOW)
        self.assertFalse(self.state.exists())
        server.maybe_send_investor_close(NOW.replace(minute=1))
        self.assertEqual(self.send.call_count, 2)
        self.assertEqual(self.state.read_text(), '2026-11-03')

    def test_failed_data_query_retries_without_partial_message(self):
        self.build.side_effect = [RuntimeError('API failed'), 'investors']
        with self.assertRaises(RuntimeError):
            server.maybe_send_investor_close(NOW)
        self.send.assert_not_called()
        self.assertFalse(self.state.exists())
        server.maybe_send_investor_close(NOW.replace(minute=1))
        self.send.assert_called_once_with('investors')

    def test_missing_telegram_settings_skips_query(self):
        with patch.object(server, 'TELEGRAM_BOT_TOKEN', None):
            server.maybe_send_investor_close(NOW)
        self.build.assert_not_called()
        self.send.assert_not_called()


if __name__ == '__main__':
    unittest.main()
