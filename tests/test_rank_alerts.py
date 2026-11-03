from datetime import datetime, timedelta
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch

path = Path(__file__).resolve().parents[1] / 'apps/rank_monitor/rank_monitor_main.py'
spec = importlib.util.spec_from_file_location('rank_monitor_main', path)
rank = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rank)


def snapshot(prefix='a'):
    return [{'stk_cd':f'{prefix}{i:05}', 'bigd_rank':str(i), 'rank_chg':'0',
             'prev_base_chgr':'+0.9'} for i in range(1, 21)]


class RankAlertTests(unittest.TestCase):
    def test_first_sample_ignores_api_jump_but_keeps_top_two_exception(self):
        items = snapshot()
        items[2]['rank_chg'] = '99'
        previous = {}
        changes = rank.compute_rank_changes(items, previous)
        self.assertTrue(all(value is None for value in changes.values()))
        self.assertEqual(len(previous), 20)
        self.assertEqual([r['bigd_rank'] for r in rank.find_surge_alerts(items, changes)], ['1', '2'])

    def test_new_third_place_alerts_despite_zero_api_change(self):
        previous = {}
        rank.compute_rank_changes(snapshot(), previous)
        items = snapshot()
        items[2].update(stk_cd='388050', stk_nm='지투파워', prev_base_chgr='+8.81')
        changes = rank.compute_rank_changes(items, previous)
        self.assertEqual(changes['388050'], 18)
        self.assertIn(items[2], rank.find_surge_alerts(items, changes))

    def test_new_entry_threshold_and_price_condition(self):
        previous = {}
        rank.compute_rank_changes(snapshot(), previous)
        items = snapshot('b')
        items[2]['prev_base_chgr'] = '+0.89'
        changes = rank.compute_rank_changes(items, previous)
        alerted = [int(r['bigd_rank']) for r in rank.find_surge_alerts(items, changes)]
        self.assertIn(16, alerted)  # 21 - 16 = 5
        self.assertNotIn(17, alerted)
        self.assertNotIn(3, alerted)

    def test_existing_ranks_use_actual_difference(self):
        previous = {}
        rank.compute_rank_changes(snapshot(), previous)
        items = snapshot()
        items[2]['stk_cd'], items[9]['stk_cd'] = items[9]['stk_cd'], items[2]['stk_cd']
        changes = rank.compute_rank_changes(items, previous)
        self.assertEqual(changes[items[2]['stk_cd']], 7)
        self.assertEqual(changes[items[9]['stk_cd']], -7)

    def test_removed_stock_does_not_keep_old_rank(self):
        previous = {}
        rank.compute_rank_changes(snapshot(), previous)
        rank.compute_rank_changes(snapshot('b'), previous)
        self.assertNotIn('a00003', previous)
        items = snapshot('b')
        items[2]['stk_cd'] = 'a00003'
        self.assertEqual(rank.compute_rank_changes(items, previous)['a00003'], 18)

    def test_incomplete_snapshots_do_not_replace_baseline(self):
        previous = {}
        rank.compute_rank_changes(snapshot(), previous)
        before = dict(previous)
        invalids = [[], snapshot()[:19], snapshot()+snapshot()[:1],
                    [dict(item, bigd_rank='3') for item in snapshot()],
                    [dict(item, stk_cd='same') for item in snapshot()]]
        for items in invalids:
            self.assertFalse(rank.is_complete_rank_snapshot(items))
            changes = rank.compute_rank_changes(items, previous)
            self.assertTrue(all(value is None for value in changes.values()))
            self.assertEqual(previous, before)

    def test_fetch_retries_partial_and_does_not_return_invalid_data(self):
        with patch.object(rank, 'fn_ka00198', side_effect=[{'item_inq_rank':snapshot()[:19]}, {'item_inq_rank':snapshot()}]), \
                patch.object(rank.time, 'sleep'):
            self.assertEqual(len(rank.fetch_rank_list('token', {})), 20)
        with patch.object(rank, 'fn_ka00198', return_value={'item_inq_rank':snapshot()[:19]}), \
                patch.object(rank.time, 'sleep'):
            self.assertIsNone(rank.fetch_rank_list('token', {}))

    def test_one_hour_cooldown_and_recording_windows_unchanged(self):
        item = {'stk_cd':'388050'}
        last = {}
        now = datetime.fromisoformat('2026-11-03T18:22:31+09:00')
        self.assertEqual(rank.filter_alert_cooldown([item], last, now), [item])
        self.assertEqual(rank.filter_alert_cooldown([item], last, now+timedelta(minutes=59)), [])
        self.assertEqual(rank.filter_alert_cooldown([item], last, now+timedelta(hours=1)), [item])
        self.assertTrue(rank.is_recording_hours(now))
        self.assertFalse(rank.is_notify_blackout(now))
