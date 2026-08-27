"""투자자별 순매수 추이 — 수집 + 화면 서버.

이 파일은 데이터 수집과 HTTP 서버를 담당하고, 화면(HTML/CSS/JS)은
같은 폴더의 index.html에 분리되어 있다. 서버는 index.html을 매 요청 시
읽어 서빙하므로, 화면만 고칠 때는 서버 재시작 없이 새로고침이면 된다.

  1) 백그라운드 스레드에서 KOSPI/KOSDAQ 둘 다 매분 조회해
     netprps_snapshots_{kospi|kosdaq}_YYYY-MM-DD.jsonl 에 분당 1줄씩 누적
  2) HTTP 서버로 화면(index.html)과 /api/netprps?mrkt=kospi|kosdaq(데이터)를 서빙
  3) 브라우저는 선택 시장을 매 분 fetch 하여 표/차트를 갱신(전체 리로드 없음)
     화면 상단 라디오로 KOSPI/KOSDAQ 전환

구성 파일:
  - trend_server.py : 수집 + API + 정적 서빙 (이 파일)
  - index.html      : 화면(표/차트/컨트롤)

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


PAGE_PATH = os.path.join(BASE_DIR, 'index.html')


def load_page():
    """화면 HTML(index.html)을 매 요청 시 읽어 반환 — 서버 재시작 없이 화면 수정 반영."""
    with open(PAGE_PATH, 'r', encoding='utf-8') as f:
        return f.read()


class Handler(http.server.BaseHTTPRequestHandler):
    timeout = 30

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        if path in ('/', '/index.html'):
            try:
                body = load_page().encode('utf-8')
            except FileNotFoundError:
                self.send_error(500, 'index.html not found')
                return
            self._send(body, 'text/html; charset=utf-8')
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
