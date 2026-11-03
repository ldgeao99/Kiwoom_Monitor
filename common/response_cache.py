"""Shared public API responses; one refresh per key, bounded workers and disk fallback."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import threading
import time


class ResponseCache:
    def __init__(self, directory, workers=3, clock=time.time):
        self.directory = Path(directory)
        self.clock = clock
        self.pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix='public-cache')
        self.lock = threading.RLock()
        self.entries, self.pending, self.retry_after = {}, {}, {}

    def _path(self, key):
        return self.directory / (sha256(key.encode()).hexdigest() + '.json')

    def _load(self, key):
        if key not in self.entries:
            try:
                entry = json.loads(self._path(key).read_text(encoding='utf-8'))
                if isinstance(entry['data'], dict) and isinstance(entry['time'], (int, float)):
                    self.entries[key] = entry
            except (OSError, ValueError, KeyError, TypeError):
                pass
        return self.entries.get(key)

    def _refresh(self, key, loader, persist):
        try:
            data = loader()
            if not isinstance(data, dict) or data.get('error'):
                raise ValueError('Invalid API response')
            entry = {'time': self.clock(), 'data': deepcopy(data)}
            if persist:
                try:
                    self.directory.mkdir(parents=True, exist_ok=True)
                    path = self._path(key)
                    temp = path.with_suffix('.tmp')
                    temp.write_text(json.dumps(entry, ensure_ascii=False), encoding='utf-8')
                    temp.replace(path)
                except OSError as error:
                    print(f'[public-cache] snapshot write failed: {error}')
            with self.lock:
                if len(self.entries) >= 256 and key not in self.entries:
                    oldest = min(self.entries, key=lambda k: self.entries[k]['time'])
                    del self.entries[oldest]
                self.entries[key] = entry
            return entry
        except Exception as error:
            print(f'[public-cache] {key}: {type(error).__name__}; retry later')
            with self.lock:
                self.retry_after[key] = self.clock() + 10
            raise

    def get(self, key, loader, ttl, *, persist=True, wait=True):
        with self.lock:
            entry = self._load(key) if persist else self.entries.get(key)
            stale = entry is None or self.clock() - entry['time'] >= ttl
            for finished in [k for k, task in self.pending.items() if task.done()]:
                del self.pending[finished]
            future = self.pending.get(key)
            if stale and future is None and self.clock() >= self.retry_after.get(key, 0):
                # Do not let many distinct browser requests create an unbounded work queue.
                if len(self.pending) < 8:
                    future = self.pool.submit(self._refresh, key, loader, persist)
                    self.pending[key] = future
        if entry is None:
            if not wait:
                return None
            if future is None:
                raise RuntimeError('조회 대기 중입니다. 잠시 후 다시 시도해주세요.')
            entry = future.result(timeout=25)
        result = deepcopy(entry['data'])
        result['_cache'] = {'updated_at': datetime.fromtimestamp(entry['time'], timezone.utc).isoformat(),
                            'stale': self.clock() - entry['time'] >= ttl}
        return result
