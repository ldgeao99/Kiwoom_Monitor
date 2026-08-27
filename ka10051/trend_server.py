"""투자자별 순매수 추이 — 수집 + 화면 서버 (단일 파일).

ka10051(업종별투자자순매수)을 08~20시 사이 1분 간격으로 조회하여
KOSPI/KOSDAQ 종합의 투자자별 순매수를 시장별 JSONL로 누적하고,
동시에 HTTP 서버로 화면을 서빙한다. 이 파일 하나가:

  1) 백그라운드 스레드에서 KOSPI/KOSDAQ 둘 다 매분 조회해
     netprps_snapshots_{kospi|kosdaq}_YYYY-MM-DD.jsonl 에 분당 1줄씩 누적
  2) HTTP 서버로 화면(HTML)과 /api/netprps?mrkt=kospi|kosdaq(오늘자 데이터)를 서빙
  3) 브라우저는 선택 시장을 60초마다 fetch 하여 표/차트를 갱신(전체 리로드 없음)
     화면 상단 라디오로 KOSPI/KOSDAQ 전환

분당 JSONL 컨벤션은 프로젝트의 rank_snapshots_*.jsonl 과 동일하다.

사용법:
    python trend_server.py                # 포트 8010, 수집+화면 (KOSPI/KOSDAQ 모두)
    python trend_server.py --port 8020
    python trend_server.py --loop 30      # 수집 간격(초) 지정
    python trend_server.py --reset        # 오늘자 누적 파일 삭제 후 시작
    python trend_server.py --no-collect   # 수집 없이 화면만 (이미 수집 중일 때)

옵션:
    --port PORT    서버 포트 (기본 8010)
    --no-collect   수집 스레드 없이 서버만 실행
    --loop [SEC]   수집 간격(초). 생략 시 60초
    --start HH     누적 시작 시각(시). 기본 8
    --end HH       누적 종료 시각(시). 기본 20
    --reset        오늘자 누적 파일(양 시장) 삭제 후 시작
"""
import http.server
import json
import os
import socketserver
import sys
import threading
import time
import requests
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, parse_qs

# 서버 환경 시간대(UTC 등)와 무관하게 항상 한국시간(KST) 기준으로 동작
KST = timezone(timedelta(hours=9))


def now_kst():
    return datetime.now(KST)

PORT = 8010
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# publish_auth_token.py 는 프로젝트 루트(상위 폴더)에 있으므로 import 경로에 추가
sys.path.insert(0, os.path.dirname(BASE_DIR))

from publish_auth_token import get_access_token

# 화면 표시 컬럼: (응답필드, 표시명, 색상) — 표/차트/체크박스 공통
# 순서/이름은 HTS 투자자 선택 화면과 유사하게(공통 항목 앞, ka10051 전용 항목 뒤)
COLUMNS = [
    ('ind_netprps', '개인', '#16a34a'),
    ('frgnr_netprps', '외국인', '#e5484d'),
    ('orgn_netprps', '기관계', '#1e3a8a'),
    ('sc_netprps', '금융투자', '#f5820a'),
    ('insrnc_netprps', '보험', '#0891b2'),
    ('invtrt_netprps', '투신', '#9333ea'),
    ('jnsinkm_netprps', '기타금융', '#eab308'),
    ('bank_netprps', '은행', '#0d9488'),
    ('endw_netprps', '연기금등', '#7c3aed'),
    ('samo_fund_netprps', '사모펀드', '#2563eb'),
    ('etc_corp_netprps', '기타법인', '#db2777'),
]

# 최초 로드시 기본으로 켜둘 투자자 (HTS 화면과 유사)
DEFAULT_SELECTED = ['frgnr_netprps', 'orgn_netprps', 'sc_netprps']

# 수집 대상 시장: key(파일/쿼리용), name(표시), mrkt_tp(API), inds_cd(종합 업종코드)
MARKETS = [
    {'key': 'kospi', 'name': 'KOSPI', 'mrkt_tp': '0', 'inds_cd': '001_AL'},
    {'key': 'kosdaq', 'name': 'KOSDAQ', 'mrkt_tp': '1', 'inds_cd': '101_AL'},
]
MARKET_BY_KEY = {m['key']: m for m in MARKETS}

# 수집 스레드에 전달할 설정 (main에서 채움)
POLL_CONF = {'interval': 60, 'start_h': 8, 'end_h': 20}


# ─────────────────────────── 데이터 수집 ───────────────────────────

def snapshot_path(market_key, date_str):
    """market_key: 'kospi'|'kosdaq', date_str: 'YYYY-MM-DD' -> JSONL 경로."""
    return os.path.join(BASE_DIR, f'netprps_snapshots_{market_key}_{date_str}.jsonl')


def columns_meta():
    return [{'key': k, 'name': n, 'color': c} for k, n, c in COLUMNS]


def fetch_row(token, base_dt, mrkt_tp='0', inds_cd='001_AL'):
    url = 'https://api.kiwoom.com/api/dostk/sect'
    headers = {
        'Content-Type': 'application/json;charset=UTF-8',
        'authorization': f'Bearer {token}',
        'cont-yn': 'N',
        'next-key': '',
        'api-id': 'ka10051',
    }
    data = {'mrkt_tp': mrkt_tp, 'amt_qty_tp': '0', 'base_dt': base_dt, 'stex_tp': '3'}
    resp = requests.post(url, headers=headers, json=data)
    resp.raise_for_status()
    body = resp.json()
    if body.get('return_code') != 0:
        raise RuntimeError(f"API 오류: {body.get('return_msg')}")
    rows = body.get('inds_netprps', [])
    target = next((r for r in rows if r.get('inds_cd') == inds_cd), None)
    if target is None and rows:
        target = rows[0]
    if target is None:
        raise RuntimeError('업종별 순매수 데이터가 비어 있습니다.')
    return target


def to_number(value):
    if value is None:
        return 0
    value = str(value).strip().replace('+', '')
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return 0


def read_records(market_key, date_str):
    """해당 시장/일자 JSONL을 읽어 레코드 리스트로 반환."""
    path = snapshot_path(market_key, date_str)
    if not os.path.exists(path):
        return []
    records = []
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return records


def last_record(market_key, date_str):
    records = read_records(market_key, date_str)
    return records[-1] if records else None


def poll_once(token, market):
    """market(dict)에 대해 1회 조회하여 해당 시장 JSONL에 append. 토큰을 반환."""
    if token is None:
        token = get_access_token()
        if not token:
            raise SystemExit('접근 토큰 발급 실패. .env 설정을 확인해주세요.')

    now = now_kst()
    base_dt = now.strftime('%Y%m%d')       # API 요청용
    date_str = now.strftime('%Y-%m-%d')    # 파일/레코드용
    mkey = market['key']

    row = fetch_row(token, base_dt, mrkt_tp=market['mrkt_tp'], inds_cd=market['inds_cd'])

    record = {
        'date': date_str,
        't': now.strftime('%H:%M:%S'),
        'label': row.get('inds_nm') or market['inds_cd'],
    }
    for key, _name, _color in COLUMNS:
        record[key] = to_number(row.get(key))

    # 수집 시간대에는 값이 직전과 같아도 매분 기록해 시간축에 빈칸이 없도록 한다.
    # (같은 분에 중복 실행되어 이미 그 분이 기록된 경우에만 스킵)
    prev = last_record(mkey, date_str)
    if prev and prev.get('t', '')[:5] == record['t'][:5]:
        print(f"[{record['t']}] {market['name']} 같은 분 이미 기록(스킵)")
        return token

    with open(snapshot_path(mkey, date_str), 'a', encoding='utf-8') as f:
        f.write(json.dumps(record, ensure_ascii=False) + '\n')

    n = len(read_records(mkey, date_str))
    print(f"[{record['t']}] {market['name']} 기록 · 누적 {n}개")
    return token


def in_collect_window(now, start_h, end_h):
    """평일(월~금) 08:00~20:00 안이면 True. 주말은 항상 False."""
    if now.weekday() >= 5:          # 5:토, 6:일
        return False
    return start_h <= now.hour < end_h


def sleep_to_next_boundary(interval):
    """벽시계 경계(interval 배수, 예: 60초면 매 분 :00)까지 대기.
    epoch 초 기준이라 시간대와 무관하며, KST(+9:00)에서 60초 배수는 :00에 정렬된다."""
    now = time.time()
    target = (int(now // interval) + 1) * interval
    time.sleep(max(0.0, target - now))


def run_loop(interval=60, start_h=8, end_h=20):
    """평일 start_h~end_h 시간대에 매 경계(:00)에 맞춰 모든 시장을 poll_once (무한 루프)."""
    print(f'{interval}초 간격(정각 정렬) 자동 반복 누적 시작 '
          f'(평일 {start_h:02d}:00~{end_h:02d}:00, 주말 휴무)')
    token = None
    idle_notified = False
    while True:
        if in_collect_window(now_kst(), start_h, end_h):
            idle_notified = False
            for market in MARKETS:
                try:
                    token = poll_once(token, market)
                except Exception as e:
                    print(f"  {market['name']} 조회 실패: {e}")
                    token = None  # 토큰 만료 등 대비해 재발급
            sleep_to_next_boundary(interval)   # 다음 :00 까지 대기(드리프트 없음)
        else:
            if not idle_notified:
                stamp = now_kst().strftime('%Y-%m-%d %H:%M:%S')
                print(f'[{stamp}] 수집 시간대 밖 — 대기 중 '
                      f'(평일 {start_h:02d}:00~{end_h:02d}:00에 재개)')
                idle_notified = True
            sleep_to_next_boundary(60)  # 시간대 밖에서는 매 분 :00에 확인


def get_opt(args, name, default=None):
    if name in args:
        i = args.index(name)
        if i + 1 < len(args) and not args[i + 1].startswith('--'):
            return args[i + 1]
    return default


# ─────────────────────────── 화면(HTML) ───────────────────────────

PAGE = """<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>투자자별 매매동향</title>
<style>
  :root {
    --bg:#f2f3f5; --panel:#fff; --line:#d5d8dd; --text:#1c1f24;
    --muted:#6b7280; --head:#eef0f3; --pos:#e5484d; --neg:#2563eb; --sel:#dbeafe;
  }
  * { box-sizing:border-box; }
  body { margin:0; padding:16px; background:var(--bg); color:var(--text);
    font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","Malgun Gothic",sans-serif; }
  .wrap { display:grid; grid-template-columns:1fr; gap:14px; max-width:1200px; margin:0 auto; }
  .panel { background:var(--panel); border:1px solid var(--line); border-radius:8px; overflow:hidden; }
  .title { padding:10px 14px; font-size:14px; font-weight:600; border-bottom:1px solid var(--line);
    display:flex; justify-content:space-between; align-items:center; gap:8px; }
  .title small { color:var(--muted); font-weight:400; }
  #status { font-size:11px; color:var(--muted); font-weight:400; }
  #status.live::before { content:"● "; color:#16a34a; }
  table { width:100%; border-collapse:collapse; font-size:12px; font-variant-numeric:tabular-nums; white-space:nowrap; }
  th,td { padding:5px 9px; text-align:right; border-bottom:1px solid #eceef1; }
  th { background:var(--head); color:var(--muted); font-weight:600; position:sticky; top:0; }
  th:first-child, td:first-child { text-align:center; color:var(--muted);
    position:sticky; left:0; background:var(--panel); z-index:1; }
  th:first-child { z-index:2; background:var(--head); }
  .summary td { font-weight:600; }
  .summary td:first-child { background:#f6f7f9; }
  /* 증감 행 강조 배경 */
  .summary.delta td { background:var(--sel); }
  /* 매분 갱신 시 번쩍임 */
  .summary.delta.flash td { animation:deltaFlash .9s ease-out; }
  @keyframes deltaFlash {
    0%   { background:#ffd54a; }
    60%  { background:#ffe9a6; }
    100% { background:var(--sel); }
  }
  .tbl-scroll { max-height:440px; overflow:auto; }
  tr.moretip td { text-align:center; color:var(--muted); font-size:11px; padding:8px;
    background:#fafbfc; position:static; }
  .pos { color:var(--pos); } .neg { color:var(--neg); }
  .legend { display:flex; gap:16px; padding:8px 14px; font-size:12px; color:var(--muted); flex-wrap:wrap; }
  .legend b { font-weight:600; }
  .legend i { display:inline-block; width:14px; height:3px; border-radius:2px; vertical-align:middle; margin-right:5px; }
  svg { display:block; width:100%; height:auto; }
  .empty { padding:40px; text-align:center; color:var(--muted); font-size:13px; }
  .controls { display:flex; flex-wrap:wrap; align-items:center; gap:6px 4px; padding:10px 12px; }
  .controls .chk { display:inline-flex; align-items:center; gap:5px; font-size:12px; cursor:pointer;
    padding:3px 8px; border:1px solid var(--line); border-radius:14px; user-select:none; background:#fafbfc; }
  .controls .chk input { margin:0; cursor:pointer; }
  .controls .chk .dot { width:10px; height:10px; border-radius:50%; display:inline-block; }
  .controls .chk.off { opacity:.45; }
  .controls .sep { width:1px; align-self:stretch; background:var(--line); margin:0 4px; }
  .controls .allbtn { font-size:11px; color:var(--muted); background:none; border:1px solid var(--line);
    border-radius:12px; padding:3px 9px; cursor:pointer; }
  .market-sel { display:inline-flex; gap:14px; font-size:14px; font-weight:600; }
  .market-sel label { display:inline-flex; align-items:center; gap:5px; cursor:pointer; color:var(--muted); }
  .market-sel label.on { color:var(--text); }
  .market-sel input { cursor:pointer; }
  .page-title { max-width:1200px; margin:0 auto 4px; font-size:18px; font-weight:700; color:var(--text); }
</style>
</head>
<body>
<h1 class="page-title">투자자별 매매동향</h1>
<div class="wrap">
  <div class="panel">
    <div class="title">
      <span class="market-sel" id="market-sel"></span>
      <small id="status">연결 중…</small>
    </div>
    <div class="controls" id="controls"></div>
  </div>
  <div class="panel">
    <div class="title"><span id="label-lbl"></span> 순매수 <small id="date-lbl"></small></div>
    <div id="table-host"></div>
  </div>
  <div class="panel">
    <div class="title">누적 순매수 추이</div>
    <div id="chart-host"></div>
  </div>
</div>
<script>
let DATA = { date:null, label:'', columns:[], points:[], default_selected:[], markets:[] };
let selected = null;           // Set<key> — 표시할 투자자

const fmtSigned = v => (v > 0 ? '+' : '') + v.toLocaleString();
const cls = v => v > 0 ? 'pos' : (v < 0 ? 'neg' : '');
const LS_KEY = 'netprps_selected';
const LS_MARKET = 'netprps_market';

// 현재 선택 시장 (기본 kospi) — 최초 fetch 전에도 사용
let curMarket = (function () {
  try { return localStorage.getItem(LS_MARKET) || 'kospi'; } catch (e) { return 'kospi'; }
})();

function renderMarkets() {
  const host = document.getElementById('market-sel');
  host.innerHTML = (DATA.markets || []).map(m =>
    `<label class="${m.key === curMarket ? 'on' : ''}">`
    + `<input type="radio" name="mkt" value="${m.key}" ${m.key === curMarket ? 'checked' : ''}>${m.name}</label>`
  ).join('');
  host.querySelectorAll('input[name="mkt"]').forEach(r => {
    r.addEventListener('change', () => {
      curMarket = r.value;
      try { localStorage.setItem(LS_MARKET, curMarket); } catch (e) {}
      visibleCount = PAGE_SIZE;                          // 새 시장은 처음부터
      document.getElementById('table-host').innerHTML = '';  // 스크롤 초기화
      refresh();
    });
  });
}

function loadSelected() {
  try {
    const raw = localStorage.getItem(LS_KEY);
    if (raw) return new Set(JSON.parse(raw));
  } catch (e) {}
  return new Set(DATA.default_selected || []);
}
function saveSelected() {
  try { localStorage.setItem(LS_KEY, JSON.stringify([...selected])); } catch (e) {}
}
// 선택된 컬럼(원래 순서 유지)
function activeCols() { return DATA.columns.filter(c => selected.has(c.key)); }

function renderControls() {
  const host = document.getElementById('controls');
  let html = '';
  DATA.columns.forEach(c => {
    const on = selected.has(c.key);
    html += `<label class="chk ${on ? '' : 'off'}" data-key="${c.key}">`
      + `<input type="checkbox" ${on ? 'checked' : ''}>`
      + `<span class="dot" style="background:${c.color}"></span>${c.name}</label>`;
  });
  html += '<span class="sep"></span>'
    + '<button class="allbtn" data-act="all">전체</button>'
    + '<button class="allbtn" data-act="none">해제</button>';
  host.innerHTML = html;

  host.querySelectorAll('.chk').forEach(el => {
    el.addEventListener('change', () => {
      const key = el.dataset.key;
      if (el.querySelector('input').checked) selected.add(key); else selected.delete(key);
      saveSelected(); renderControls(); renderTable(); renderChart();
    });
  });
  host.querySelectorAll('.allbtn').forEach(btn => {
    btn.addEventListener('click', () => {
      if (btn.dataset.act === 'all') DATA.columns.forEach(c => selected.add(c.key));
      else selected.clear();
      saveSelected(); renderControls(); renderTable(); renderChart();
    });
  });
}

const PAGE_SIZE = 30;          // 스크롤 시 한 번에 더 불러올 행 수
let visibleCount = PAGE_SIZE;  // 현재 표시 중인 시간별 행 수(최신부터)
let lastLatestT;               // 직전에 본 최신 시각(새 분 감지용)

function flashDelta() {
  const el = document.getElementById('delta-row');
  if (!el) return;
  el.classList.remove('flash');
  void el.offsetWidth;          // reflow 강제 → 애니메이션 재시작
  el.classList.add('flash');
}

function renderTable() {
  const host = document.getElementById('table-host');
  document.getElementById('label-lbl').textContent = DATA.label || '';
  document.getElementById('date-lbl').textContent = DATA.date ? `기준일 ${DATA.date}` : '';
  const pts = DATA.points, cols = activeCols();
  if (!pts.length) { host.innerHTML = '<div class="empty">아직 기록된 데이터가 없습니다.</div>'; return; }
  if (!cols.length) { host.innerHTML = '<div class="empty">위에서 투자자를 선택하세요.</div>'; return; }

  const last = pts[pts.length - 1];
  const prev = pts.length > 1 ? pts[pts.length - 2] : null;

  // 표시할 행 개수(최신 visibleCount개)
  const shown = Math.min(visibleCount, pts.length);
  const startIdx = pts.length - shown;  // 이 인덱스까지(포함) 최신 방향으로 표시

  let html = '<table><thead><tr><th>시간</th>';
  cols.forEach(c => html += `<th>${c.name}</th>`);
  html += '</tr></thead><tbody>';

  html += '<tr class="summary"><td>순매수</td>';
  cols.forEach(c => html += `<td class="${cls(last[c.key])}">${fmtSigned(last[c.key])}</td>`);
  html += `</tr><tr class="summary delta" id="delta-row"><td>증감</td>`;
  cols.forEach(c => {
    const d = prev ? last[c.key] - prev[c.key] : 0;
    html += `<td class="${cls(d)}">${fmtSigned(d)}</td>`;
  });
  html += '</tr>';

  for (let i = pts.length - 1; i >= startIdx; i--) {
    const p = pts[i];
    html += `<tr><td>${p.t.slice(0,5)}</td>`;
    cols.forEach(c => html += `<td class="${cls(p[c.key])}">${fmtSigned(p[c.key])}</td>`);
    html += '</tr>';
  }
  if (shown < pts.length) {
    const remain = pts.length - shown;
    html += `<tr class="moretip"><td colspan="${cols.length + 1}">아래로 스크롤하면 ${Math.min(PAGE_SIZE, remain)}개 더 불러옵니다 (남은 ${remain}개)</td></tr>`;
  }
  html += '</tbody></table>';

  // 스크롤 위치 보존을 위해 기존 컨테이너가 있으면 재사용
  let scroll = host.querySelector('.tbl-scroll');
  const prevTop = scroll ? scroll.scrollTop : 0;
  if (!scroll) {
    host.innerHTML = '<div class="tbl-scroll"></div>';
    scroll = host.querySelector('.tbl-scroll');
    scroll.addEventListener('scroll', onTableScroll);
  }
  scroll.innerHTML = html;
  scroll.scrollTop = prevTop;
}

function onTableScroll(e) {
  const el = e.currentTarget;
  // 바닥 근처(80px)에 도달하면 30개 더 표시
  if (el.scrollTop + el.clientHeight >= el.scrollHeight - 80) {
    if (visibleCount < DATA.points.length) {
      visibleCount += PAGE_SIZE;
      renderTable();
    }
  }
}

function renderChart() {
  const host = document.getElementById('chart-host');
  const pts = DATA.points, cols = activeCols();
  if (!cols.length) { host.innerHTML = '<div class="empty">위에서 투자자를 선택하세요.</div>'; return; }
  if (pts.length < 2) { host.innerHTML = '<div class="empty">차트를 그리려면 2개 이상의 스냅샷이 필요합니다.</div>'; return; }

  const W = 760, H = 420, mL = 8, mR = 56, mT = 16, mB = 28;
  const iw = W - mL - mR, ih = H - mT - mB;

  let vmin = 0, vmax = 0;
  pts.forEach(p => cols.forEach(c => { vmin = Math.min(vmin, p[c.key]); vmax = Math.max(vmax, p[c.key]); }));
  if (vmin === vmax) vmax = vmin + 1;
  const pad = (vmax - vmin) * 0.08; vmin -= pad; vmax += pad;

  const n = pts.length;
  const x = i => mL + (n === 1 ? iw/2 : iw * i / (n - 1));
  const y = v => mT + ih * (1 - (v - vmin) / (vmax - vmin));

  const ticks = 6; let grid = '';
  for (let k = 0; k <= ticks; k++) {
    const v = vmin + (vmax - vmin) * k / ticks, yy = y(v);
    grid += `<line x1="${mL}" y1="${yy}" x2="${mL+iw}" y2="${yy}" stroke="#eceef1"/>`;
    grid += `<text x="${mL+iw+6}" y="${yy+4}" font-size="11" fill="#9aa0a8">${Math.round(v).toLocaleString()}</text>`;
  }
  if (vmin < 0 && vmax > 0) {
    const y0 = y(0);
    grid += `<line x1="${mL}" y1="${y0}" x2="${mL+iw}" y2="${y0}" stroke="#c7ccd2"/>`;
  }

  let xlab = ''; const step = Math.max(1, Math.floor(n / 6));
  for (let i = 0; i < n; i += step)
    xlab += `<text x="${x(i)}" y="${H-8}" font-size="10" fill="#9aa0a8" text-anchor="middle">${pts[i].t.slice(0,5)}</text>`;

  let lines = '';
  cols.forEach(c => {
    const d = pts.map((p, i) => `${i === 0 ? 'M' : 'L'}${x(i).toFixed(1)},${y(p[c.key]).toFixed(1)}`).join(' ');
    lines += `<path d="${d}" fill="none" stroke="${c.color}" stroke-width="1.8"/>`;
    lines += `<circle cx="${x(n-1)}" cy="${y(pts[n-1][c.key])}" r="2.6" fill="${c.color}"/>`;
  });

  host.innerHTML = `<svg viewBox="0 0 ${W} ${H}" xmlns="http://www.w3.org/2000/svg">` + grid + lines + xlab + '</svg>';
}

async function refresh() {
  const st = document.getElementById('status');
  try {
    const res = await fetch('/api/netprps?mrkt=' + encodeURIComponent(curMarket), { cache: 'no-store' });
    if (!res.ok) throw new Error('HTTP ' + res.status);
    DATA = await res.json();
    if (DATA.market) curMarket = DATA.market;           // 서버가 보정한 값 반영
    if (selected === null) selected = loadSelected();  // 최초 1회 초기화

    // 최신 시각이 바뀌었으면(새 분 데이터) 증감 행 번쩍임 (최초 로드는 제외)
    const latestT = DATA.points.length ? DATA.points[DATA.points.length - 1].t : null;
    const isNew = latestT && lastLatestT !== undefined && latestT !== lastLatestT;
    lastLatestT = latestT;

    renderMarkets();
    renderControls();
    renderTable();
    renderChart();
    if (isNew) flashDelta();
    st.className = 'live';
    st.textContent = '갱신 ' + new Date().toLocaleTimeString('ko-KR', { hour: '2-digit', minute: '2-digit' });
  } catch (e) {
    st.className = '';
    st.textContent = '연결 실패: ' + e.message;
  }
}

// 서버는 매 분 :00에 조회·기록한다. 브라우저 갱신을 :00 직후로 맞춰 지연을 최소화.
const REFRESH_OFFSET_MS = 3000;   // :00 이후 서버가 기록을 마칠 여유(초)
function scheduleRefresh() {
  const now = Date.now();
  const nextTick = Math.floor(now / 60000) * 60000 + 60000 + REFRESH_OFFSET_MS;  // 다음 :00 + 여유
  setTimeout(() => { refresh(); scheduleRefresh(); }, nextTick - now);
}

refresh();          // 최초 즉시 1회
scheduleRefresh();  // 이후 매 분 :00+3초에 갱신
</script>
</body>
</html>
"""


# ─────────────────────────── HTTP 서버 ───────────────────────────

# 표시 기준 날짜가 새 날짜로 넘어가는 시각(시). 이 시각 전이면 전일을 기준으로 본다.
DISPLAY_ROLLOVER_HOUR = 6


def effective_date(now):
    """오전 DISPLAY_ROLLOVER_HOUR시 전이면 전일 날짜를 반환('YYYY-MM-DD')."""
    base = now
    if now.hour < DISPLAY_ROLLOVER_HOUR:
        base = now - timedelta(days=1)
    return base.strftime('%Y-%m-%d')


def build_payload(market_key):
    """해당 시장의 기준일(06시 전이면 전일, 없으면 가장 최근 일자) 레코드를 반환."""
    ref_date = effective_date(now_kst())
    records = read_records(market_key, ref_date)
    date_str = ref_date
    if not records:
        # 기준일 데이터가 없으면 기준일 이하의 가장 최근 일자 파일을 찾아 보여줌
        prefix = f'netprps_snapshots_{market_key}_'
        dates = []
        for name in os.listdir(BASE_DIR):
            if name.startswith(prefix) and name.endswith('.jsonl'):
                d = name[len(prefix):-len('.jsonl')]
                if d <= ref_date:            # 기준일보다 미래 파일은 제외
                    dates.append(d)
        if dates:
            date_str = max(dates)
            records = read_records(market_key, date_str)
    label = records[-1]['label'] if records else ''
    return {
        'market': market_key,
        'markets': [{'key': m['key'], 'name': m['name']} for m in MARKETS],
        'date': date_str if records else None,
        'label': label,
        'columns': columns_meta(),
        'default_selected': DEFAULT_SELECTED,
        'points': records,
    }


class Handler(http.server.BaseHTTPRequestHandler):
    timeout = 30

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        if path in ('/', '/index.html'):
            self._send(PAGE.encode('utf-8'), 'text/html; charset=utf-8')
        elif path == '/api/netprps':
            qs = parse_qs(parsed.query)
            mkey = (qs.get('mrkt', [MARKETS[0]['key']])[0])
            if mkey not in MARKET_BY_KEY:
                mkey = MARKETS[0]['key']
            body = json.dumps(build_payload(mkey), ensure_ascii=False).encode('utf-8')
            self._send(body, 'application/json; charset=utf-8')
        else:
            self.send_error(404, 'Not Found')

    def _send(self, body, content_type):
        self.send_response(200)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Cache-Control', 'no-store')
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


class ThreadingHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    allow_reuse_address = True
    daemon_threads = True


def poller_thread():
    try:
        run_loop(POLL_CONF['interval'], POLL_CONF['start_h'], POLL_CONF['end_h'])
    except Exception as e:
        print(f'[수집 스레드 종료] {e}')


def main():
    args = sys.argv[1:]
    port = int(get_opt(args, '--port', str(PORT)))
    POLL_CONF['interval'] = int(get_opt(args, '--loop', '60') or '60')
    POLL_CONF['start_h'] = int(get_opt(args, '--start', '8'))
    POLL_CONF['end_h'] = int(get_opt(args, '--end', '20'))

    if '--reset' in args:
        today = now_kst().strftime('%Y-%m-%d')
        for m in MARKETS:
            path = snapshot_path(m['key'], today)
            if os.path.exists(path):
                os.remove(path)
                print(f"오늘자 누적 파일 초기화: {os.path.basename(path)}")

    # 수집 스레드 (--no-collect 면 서버만 실행)
    collecting = '--no-collect' not in args
    if collecting:
        threading.Thread(target=poller_thread, daemon=True).start()

    with ThreadingHTTPServer(('', port), Handler) as httpd:
        print(f'>>> 추이 서버 실행: http://localhost:{port}  (JSONL 수집 {"ON" if collecting else "OFF"})')
        print('>>> 브라우저는 60초마다 /api/netprps 를 요청해 화면을 갱신합니다.')
        print('>>> 종료하려면 Ctrl+C')
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print('\n>>> 서버를 종료합니다.')


if __name__ == '__main__':
    main()
