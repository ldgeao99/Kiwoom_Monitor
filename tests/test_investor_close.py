from datetime import datetime, timezone
from pathlib import Path
from html import unescape
import unicodedata
import tempfile
import unittest
from unittest.mock import Mock, patch

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
        message = server.build_investor_close_message(NOW)
        self.assertTrue(message.startswith('🟢 코스피 투자자별 매매동향(2026-11-03, 16:00)\n\n<pre>'))
        self.assertTrue(message.endswith('</pre>'))
        table = unescape(message.split('<pre>')[1].split('</pre>')[0])
        # 정렬 기준인 숫자 칸을 왼쪽(순수 ASCII)에, 한글 라벨을 '|' 뒤 오른쪽에 둔다.
        self.assertEqual(table.splitlines()[0].replace(' ', ''), '순매수|투자주체')
        data = [line for line in table.splitlines()[1:] if line]
        self.assertEqual([line.split('|')[1].strip() for line in data],
                         ['개인', '외국인', '기관', '금융투자', '투신', '연기금', '기타법인',
                          '보험', '기타금융', '은행', '사모펀드'])
        self.assertEqual([line.split('|')[0].strip() for line in data],
                         ['-1,234억', '+2,345억', '-1,111억', '+124억', '-51억', '+0억',
                          '+0억', '+0억', '+0억', '+0억', '+0억'])
        self.assertIn('\n\n+0억', table.replace(' ', ''))   # 보험 행 앞 빈 줄
        # 숫자 칸('|' 앞 전체)은 순수 ASCII라 플랫폼과 무관하게 폭이 일정하다.
        self.assertEqual(len({len(line.split('|')[0]) for line in data}), 1)
        self.assertTrue(all(line.split('|')[0].rstrip().endswith('억') for line in data))
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
        self.send.assert_called_once_with('investors', parse_mode='HTML')

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
        self.send.assert_called_once_with('investors', parse_mode='HTML')

    def test_missing_telegram_settings_skips_query(self):
        with patch.object(server, 'TELEGRAM_BOT_TOKEN', None):
            server.maybe_send_investor_close(NOW)
        self.build.assert_not_called()
        self.send.assert_not_called()


class InvestorCloseDeliveryTests(unittest.TestCase):
    def test_only_formatted_messages_enable_html(self):
        response = Mock(status_code=200)
        response.json.return_value = {'ok': True}
        with patch.object(server, 'TELEGRAM_BOT_TOKEN', 'test'), \
                patch.object(server, 'TELEGRAM_CHAT_ID', 'test'), \
                patch.object(server.requests, 'post', return_value=response) as post:
            self.assertTrue(server.send_telegram_message('<pre>개인 +1억</pre>', parse_mode='HTML'))
            self.assertEqual(post.call_args.kwargs['json']['parse_mode'], 'HTML')
            self.assertTrue(server.send_telegram_message('ordinary alert'))
            self.assertNotIn('parse_mode', post.call_args.kwargs['json'])


if __name__ == '__main__':
    unittest.main()
