# Dashboard initial loading

The first two requests are `/api/summary` (domestic only, local snapshots) and
`/api/netprps`. Other panels load when expanded and within 200 pixels of the
viewport. Restored stock charts also wait for visibility. Overseas indices use
`/api/overseas` independently. Periodic program requests share an in-flight
promise; summary, rank and stock detail requests also coalesce. Browser fetches
have a 28-second response timeout, and changing charts aborts their old request.

The HTTP listener no longer waits for the stock-name warmup. In normal collection
mode, background work prepares rank (30s), ETF-excluded amount rank (30s), default
program rank (5s), and overseas summary (60s). Other filter combinations populate
on demand. Each cache key has one refresh, with three workers and at most eight
pending jobs. Cold requests wait up to 25s for the shared job; subsequent requests
return the stored snapshot immediately while an expired value refreshes. Failed
refreshes back off for ten seconds. The UI displays the last successful query
time in KST and marks expired values as previous data. On a completely new server
without saved cache, a panel still needs its first provider response.

Public cache snapshots live in ignored `apps/dashboard/daily_snapshot/public_cache`.
They contain display data, never API tokens. In-flight jobs are not retained
across restarts. Successful snapshots are restored; their age remains visible.
Cache capacity is 256 in-memory entries. `--no-collect` skips background warmup.
Scheduled Telegram alerts continue using direct builders, not stale UI caches.

Chart requests with `latest=1` fetch one provider page; the response keeps its
history cursor for the existing additional-history button. Calls without that
flag retain the full-session behavior. Regular refreshes merge latest candles
only for the same instrument/timeframe, preserving loaded history. Historical
RSI precision continues to depend on the number of loaded bars.

Validation:

```sh
python -m unittest discover -s tests -v
node tests/test_dashboard_loading.cjs
```

The browser test requires Playwright and Chromium (`CHROMIUM_PATH` can override
`/usr/bin/chromium`). All its API responses are mocked: it verifies request
ordering, collapsed/offscreen panels, overseas independence, overlapping refresh
coalescing, rendered candles and the history cursor. It does not measure live
Kiwoom/Yahoo latency. Compare first domestic display, each panel's display time,
API request count and cache age in the deployed browser before claiming a latency
improvement in seconds.
