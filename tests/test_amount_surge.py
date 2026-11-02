import importlib.util
from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch


SERVER_PATH = Path(__file__).resolve().parents[1] / 'apps/dashboard/dashboard_server.py'
spec = importlib.util.spec_from_file_location('dashboard_server', SERVER_PATH)
server = importlib.util.module_from_spec(spec)
spec.loader.exec_module(server)


def item(rank, amount=130, flu=1):
    return {'code': f'{rank:06}', 'name': f'종목{rank}', 'rank': rank,
            'amount_eok': amount, 'flu': flu}


def page(items, cont='N', cursor=''):
    return {'items': items, 'cont': cont, 'next': cursor}


class MessageTests(unittest.TestCase):
    def test_filters_exact_threshold_positive_change_and_top_twenty(self):
        items = [item(i, amount=100, flu=0) for i in range(1, 22)]
        items[0] = item(1, amount=130, flu=11.98)
        items[1] = item(2, amount=129.99)  # 반올림하면 30%여도 실제 값은 기준 미달
        items[2] = item(3, amount=150, flu=-1)
        items[3] = item(4, amount=150, flu=0)
        items[19] = item(20, amount=1500)
        items[20] = item(21, amount=2000)
        with patch.object(server, 'build_amount_rank', return_value=page(items)) as rank, \
                patch.object(server, 'build_amount_previous', return_value={'amount_eok': 100}):
            message = server.build_amount_surge_message()
        self.assertEqual(message, '🟢 거래대금 20위내 거래대금 급증종목\n\n'
                         '순위 | 종목명 | 등락률 | 거래대금 | 대금 전일비\n\n'
                         '1위 | 종목1 | +11.98% | 0.01조 | +30%\n'
                         '20위 | 종목20 | +1.00% | 0.15조 | +1400%')
        rank.assert_called_once_with('N', '', exclude_etf_etn=True)

    def test_pagination_preserves_filtered_rank_and_deduplicates(self):
        pages = [page([item(1), item(3)], 'Y', 'next'),
                 page([item(3), item(7)])]
        with patch.object(server, 'build_amount_rank', side_effect=pages) as rank, \
                patch.object(server, 'build_amount_previous', return_value={'amount_eok': 100}):
            message = server.build_amount_surge_message()
        self.assertIn('3위 | 종목7', message)
        self.assertEqual(message.count('종목3'), 1)
        self.assertEqual(rank.call_args_list[1].args, ('Y', 'next'))

    def test_missing_or_zero_previous_amount_is_excluded(self):
        with patch.object(server, 'build_amount_rank', return_value=page([item(1), item(2)])), \
                patch.object(server, 'build_amount_previous', side_effect=[{'amount_eok': None}, {'amount_eok': 0}]):
            self.assertTrue(server.build_amount_surge_message().endswith('해당 종목 없음'))

    def test_message_rounds_percent_to_integer_and_amount_to_two_decimal_trillions(self):
        with patch.object(server, 'build_amount_rank', return_value=page([item(1, amount=130.5)])), \
                patch.object(server, 'build_amount_previous', return_value={'amount_eok': 100}):
            self.assertIn('1위 | 종목1 | +1.00% | 0.01조 | +31%', server.build_amount_surge_message())

    def test_amount_in_trillions_rounding_and_trailing_zero(self):
        items = [item(1, amount=2684), item(2, amount=2652), item(3, amount=2486),
                 item(4, amount=1864), item(5, amount=1620), item(6, amount=12500),
                 item(7, amount=2500), item(8, amount=12550), item(9, amount=2000)]
        with patch.object(server, 'build_amount_rank', return_value=page(items)), \
                patch.object(server, 'build_amount_previous', return_value={'amount_eok': 100}):
            message = server.build_amount_surge_message()
        amounts = [line.split(' | ')[3] for line in message.splitlines()[4:]]
        self.assertEqual(amounts, ['0.27조', '0.27조', '0.25조', '0.19조', '0.16조',
                                  '1.25조', '0.25조', '1.26조', '0.20조'])

    def test_previous_lookup_error_aborts_instead_of_sending_partial_results(self):
        with patch.object(server, 'build_amount_rank', return_value=page([item(1), item(2)])), \
                patch.object(server, 'build_amount_previous', side_effect=[{'amount_eok': 100}, RuntimeError('API failure')]):
            with self.assertRaisesRegex(RuntimeError, 'API failure'):
                server.build_amount_surge_message()

    def test_repeated_cursor_aborts(self):
        with patch.object(server, 'build_amount_rank', return_value=page([], 'Y', 'same')):
            with self.assertRaisesRegex(RuntimeError, '연속조회'):
                server.build_amount_surge_message()


class ScheduleTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        patches = [patch.object(server, 'DATA_DIR', self.temp.name),
                   patch.object(server, '_amount_surge_sent_day', None),
                   patch.object(server, 'TELEGRAM_BOT_TOKEN', 'test'),
                   patch.object(server, 'TELEGRAM_CHAT_ID', 'test'),
                   patch.object(server, 'build_amount_surge_message', return_value='summary'),
                   patch.object(server, 'send_telegram_message', return_value=True)]
        mocks = [p.start() for p in patches]
        for p in patches:
            self.addCleanup(p.stop)
        self.build, self.send = mocks[-2:]

    def at(self, date, hour, minute=0):
        return datetime.fromisoformat(f'{date}T{hour:02}:{minute:02}:00+09:00')

    def test_only_trading_days_at_sixteen_kst(self):
        for now in [self.at('2026-10-30', 15, 59), self.at('2026-10-30', 17),
                    self.at('2026-10-31', 16), self.at('2026-12-25', 16)]:
            server.maybe_send_amount_surge(now)
        self.send.assert_not_called()
        # 07:00 UTC = 16:00 KST
        server.maybe_send_amount_surge(datetime(2026, 10, 30, 7, tzinfo=timezone.utc))
        self.send.assert_called_once_with('summary')

    def test_deduplicates_after_restart_and_allows_next_trading_day(self):
        server.maybe_send_amount_surge(self.at('2026-10-30', 16))
        server.maybe_send_amount_surge(self.at('2026-10-30', 16, 1))
        server._amount_surge_sent_day = None  # restart
        server.maybe_send_amount_surge(self.at('2026-10-30', 16, 30))
        self.assertEqual(self.send.call_count, 1)
        server.maybe_send_amount_surge(self.at('2026-11-02', 16))
        self.assertEqual(self.send.call_count, 2)

    def test_failed_delivery_is_retried(self):
        self.send.side_effect = [False, True]
        server.maybe_send_amount_surge(self.at('2026-10-30', 16))
        self.assertFalse((Path(self.temp.name) / '.amount_surge_sent').exists())
        server.maybe_send_amount_surge(self.at('2026-10-30', 16, 1))
        self.assertEqual(self.send.call_count, 2)
        self.assertEqual((Path(self.temp.name) / '.amount_surge_sent').read_text(), '2026-10-30')

    def test_missing_telegram_settings_does_not_query_api(self):
        with patch.object(server, 'TELEGRAM_BOT_TOKEN', None):
            server.maybe_send_amount_surge(self.at('2026-10-30', 16))
        self.build.assert_not_called()
        self.send.assert_not_called()


class DeliveryTests(unittest.TestCase):
    def test_http_200_with_telegram_failure_is_not_success(self):
        response = Mock(status_code=200)
        response.json.return_value = {'ok': False}
        with patch.object(server, 'TELEGRAM_BOT_TOKEN', 'test'), \
                patch.object(server, 'TELEGRAM_CHAT_ID', 'test'), \
                patch.object(server.requests, 'post', return_value=response):
            self.assertFalse(server.send_telegram_message('summary'))
            response.json.return_value = {'ok': True}
            self.assertTrue(server.send_telegram_message('summary'))


if __name__ == '__main__':
    unittest.main()
