from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import json
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch

from test_amount_surge import server
from response_cache import ResponseCache


class ResponseCacheTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.now = 1000
        self.cache = ResponseCache(self.temp.name, clock=lambda: self.now)
        self.addCleanup(self.cache.pool.shutdown, wait=True)

    def test_cold_requests_share_one_fetch_and_return_independent_copies(self):
        started, release = threading.Event(), threading.Event()
        def load():
            started.set()
            self.assertTrue(release.wait(3))
            return {'items': [1]}
        loader = Mock(side_effect=load)
        with ThreadPoolExecutor(2) as clients:
            first = clients.submit(self.cache.get, 'rank', loader, 30)
            self.assertTrue(started.wait(2))
            second = clients.submit(self.cache.get, 'rank', loader, 30)
            release.set()
            a, b = first.result(3), second.result(3)
        loader.assert_called_once()
        a['items'].append(2)
        self.assertEqual(b['items'], [1])
        self.assertFalse(b['_cache']['stale'])

    def test_stale_returns_without_waiting_for_refresh(self):
        self.cache.get('rank', lambda: {'items':[1]}, 30)
        self.now += 31
        started, release = threading.Event(), threading.Event()
        def load():
            started.set()
            if not release.wait(3): raise TimeoutError()
            return {'items':[2]}
        try:
            data = self.cache.get('rank', load, 30)
            self.assertEqual(data['items'], [1])
            self.assertTrue(data['_cache']['stale'])
            self.assertTrue(started.wait(2))
        finally:
            release.set()
        self.cache.pending['rank'].result(3)
        self.assertEqual(self.cache.get('rank', load, 30)['items'], [2])

    def test_restart_restores_cache_without_network(self):
        self.cache.get('rank', lambda: {'items':[1]}, 30)
        restored = ResponseCache(self.temp.name, clock=lambda: self.now)
        self.addCleanup(restored.pool.shutdown, wait=True)
        loader = Mock(side_effect=AssertionError('unexpected network'))
        self.assertEqual(restored.get('rank', loader, 30)['items'], [1])
        loader.assert_not_called()

    def test_failure_preserves_old_value_and_backs_off(self):
        self.cache.get('rank', lambda: {'items':[1]}, 30)
        self.now += 31
        loader = Mock(side_effect=RuntimeError('unavailable'))
        self.assertEqual(self.cache.get('rank', loader, 30)['items'], [1])
        task = self.cache.pending['rank']
        with self.assertRaises(RuntimeError): task.result(3)
        self.assertEqual(self.cache.get('rank', loader, 30)['items'], [1])
        loader.assert_called_once()

    def test_domestic_summary_never_waits_for_external_services(self):
        with patch.object(server, 'records_for', return_value=([], None)), \
                patch.object(server, 'read_records', return_value=[]), \
                patch.object(server, 'get_nasdaq_display', side_effect=AssertionError()), \
                patch.object(server, 'get_overseas_index_display', side_effect=AssertionError()), \
                patch.object(server, 'get_index_open', side_effect=AssertionError()):
            self.assertEqual(len(server.build_summary()['markets']), 2)

    def test_latest_chart_uses_one_page_and_retains_history_cursor(self):
        rows = [{'cntr_tm':'20261103100000','cur_prc':'100','open_pric':'99',
                 'high_pric':'101','low_pric':'98','trde_qty':'100'}]
        with patch.object(server, 'cached_token', return_value='test'), \
                patch.object(server, 'fetch_candles', return_value=(rows,'cntr_tm','Y','next')) as fetch:
            data = server.build_candles('005930', '1m', latest_only=True)
        fetch.assert_called_once()
        self.assertEqual(data['cont'], 'Y')
        self.assertEqual(data['next'], 'next')
        self.assertEqual(data['items'][0]['c'], 100)

    def test_full_history_lookup_keeps_existing_behavior(self):
        rows = [{'cntr_tm':'20261103100000','cur_prc':'100','high_pric':'101'}]
        with patch.object(server, 'cached_token', return_value='test'), \
                patch.object(server, 'fetch_candles', side_effect=[(rows,'cntr_tm','Y','next'),
                                                                  ([], 'cntr_tm','N','')]) as fetch:
            server.build_candles('005930', '1m')
        self.assertEqual(fetch.call_count, 2)


class CachedRouteTests(unittest.TestCase):
    def test_rank_and_amount_share_cache_but_pagination_is_live(self):
        with tempfile.TemporaryDirectory() as directory:
            cache = ResponseCache(directory)
            try:
                handler = server.Handler.__new__(server.Handler)
                captured = []
                handler._send = lambda body, content_type: captured.append(json.loads(body))
                with patch.object(server, 'PUBLIC_CACHE', cache), \
                        patch.object(server, 'build_rank', return_value={'items':[{'code':'005930'}]}) as rank, \
                        patch.object(server, 'build_amount_rank', return_value={'items':[], 'cont':'N','next':''}) as amount:
                    for route in ('/api/rank', '/api/rank', '/api/amount_rank?exclude_etf_etn=1',
                                  '/api/amount_rank?exclude_etf_etn=1',
                                  '/api/amount_rank?exclude_etf_etn=1&cont=Y&next=cursor'):
                        handler.path = route
                        handler.do_GET()
                    rank.assert_called_once()
                    self.assertEqual(amount.call_count, 2)
                    self.assertEqual(amount.call_args.args, ('Y', 'cursor'))
                    self.assertTrue(amount.call_args.kwargs['exclude_etf_etn'])
                    self.assertIn('_cache', captured[0])
                    self.assertNotIn('_cache', captured[-1])
            finally:
                cache.pool.shutdown(wait=True)
