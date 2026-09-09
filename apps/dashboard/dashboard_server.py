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
  - dashboard_server.py : 수집 + API + 정적 서빙 (이 파일)
  - index.html      : 화면(표/차트/컨트롤)

분당 JSONL 컨벤션은 프로젝트의 rank_snapshots_*.jsonl 과 동일하다.

사용법:
    python apps/dashboard/dashboard_server.py                # 포트 8010, 수집+화면 (KOSPI/KOSDAQ 모두)
    python apps/dashboard/dashboard_server.py --port 8020
    python apps/dashboard/dashboard_server.py --loop 30      # 수집 간격(초) 지정
    python apps/dashboard/dashboard_server.py --reset        # 오늘자 누적 파일 삭제 후 시작
    python apps/dashboard/dashboard_server.py --no-collect   # 수집 없이 화면만 (이미 수집 중일 때)

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
import re
import socketserver
import sys
import threading
import time
import requests
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from urllib.parse import urlparse, parse_qs

# 서버 환경 시간대(UTC 등)와 무관하게 항상 한국시간(KST) 기준으로 동작
KST = timezone(timedelta(hours=9))


def now_kst():
    return datetime.now(KST)

PORT = 8010
BASE_DIR = os.path.dirname(os.path.abspath(__file__))          # apps/dashboard
REPO_ROOT = os.path.dirname(os.path.dirname(BASE_DIR))         # 레포 루트
DATA_DIR = os.path.join(BASE_DIR, 'daily_snapshot')            # 스냅샷 데이터 폴더
os.makedirs(DATA_DIR, exist_ok=True)
# 공용 모듈(common/publish_auth_token.py) import 경로 추가
sys.path.insert(0, os.path.join(REPO_ROOT, 'common'))

from publish_auth_token import get_access_token

# 화면 표시 컬럼: (응답필드, 표시명, 색상) — 표/차트/체크박스 공통
# 순서/이름은 HTS 투자자 선택 화면과 유사하게(공통 항목 앞, ka10051 전용 항목 뒤)
COLUMNS = [
    ('ind_netprps', '개인', '#16a34a'),
    ('frgnr_netprps', '외국인', '#e5484d'),
    ('orgn_netprps', '기관계', '#f5820a'),
    ('sc_netprps', '금융투자', '#1e3a8a'),
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

# 수집 대상 시장: key(파일/쿼리용), name/disp(표시), mrkt_tp+inds_cd(ka10051), idx_cd(ka20003 지수)
MARKETS = [
    {'key': 'kospi', 'name': 'KOSPI', 'disp': '코스피', 'mrkt_tp': '0', 'inds_cd': '001_AL', 'idx_cd': '001'},
    {'key': 'kosdaq', 'name': 'KOSDAQ', 'disp': '코스닥', 'mrkt_tp': '1', 'inds_cd': '101_AL', 'idx_cd': '101'},
]
MARKET_BY_KEY = {m['key']: m for m in MARKETS}

# 수집 스레드에 전달할 설정 (main에서 채움)
POLL_CONF = {'interval': 60, 'start_h': 8, 'end_h': 20}

# 스냅샷 보존 일수(=보존 파일 수): 다이제스트가 '직전 거래일 4개'를 참조하므로
# 여유를 둬 최근 8개 파일(=거래일 8일)을 유지 (주말/공휴일/수집 누락 대비)
KEEP_DAYS = 8

# 외국인 순매수 천단위 알림 대상 필드/단위
ALERT_FIELD = 'frgnr_netprps'   # 외국인 순매수
ALERT_STEP = 1000               # 이 값의 배수(천단위) milestone

# 시장별 알림 상태: 마지막으로 알린 1000 그리드 기준선(level). 일자 바뀌면 리셋.
# 기준선에서 한 칸(±ALERT_STEP) 이상 움직일 때만 알리고, 움직인 만큼 기준선을 따라 이동.
# → 상승 신고점(+1000,+2000…), 고점 후 되돌림(-1000,-2000…) 모두 잡고, 경계 진동은 무시.
_alert_state = {}   # mkey -> {'date','level'}


# ─────────────────────────── 텔레그램 알림 ───────────────────────────

def _load_env_value(key):
    """프로젝트 루트(.env)에서 key 값을 읽는다(실행 위치 무관)."""
    env_file = os.path.join(REPO_ROOT, '.env')
    if not os.path.exists(env_file):
        return None
    with open(env_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            k, v = line.split('=', 1)
            if k.strip() == key:
                return v.strip()
    return None


TELEGRAM_BOT_TOKEN = _load_env_value('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = _load_env_value('TELEGRAM_CHAT_ID')


def send_telegram_message(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print('[텔레그램] 토큰/챗ID 미설정 — 전송 생략')
        return
    url = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage'
    try:
        r = requests.post(url, json={'chat_id': TELEGRAM_CHAT_ID, 'text': text})
        if r.status_code != 200:
            print(f'[텔레그램 에러] HTTP {r.status_code} - {r.text}')
    except Exception as e:
        print(f'[텔레그램 에러] {e}')


# ─────────────────────────── 데이터 수집 ───────────────────────────

def snapshot_path(market_key, date_str):
    """market_key: 'kospi'|'kosdaq', date_str: 'YYYY-MM-DD' -> JSONL 경로."""
    return os.path.join(DATA_DIR, f'netprps_snapshots_{market_key}_{date_str}.jsonl')


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


def fetch_index(token, idx_cd):
    """ka20003(전업종지수)로 해당 종합지수 행을 조회."""
    url = 'https://api.kiwoom.com/api/dostk/sect'
    headers = {
        'Content-Type': 'application/json;charset=UTF-8',
        'authorization': f'Bearer {token}',
        'cont-yn': 'N',
        'next-key': '',
        'api-id': 'ka20003',
    }
    resp = requests.post(url, headers=headers, json={'inds_cd': idx_cd})
    resp.raise_for_status()
    body = resp.json()
    if body.get('return_code') != 0:
        raise RuntimeError(f"지수 API 오류: {body.get('return_msg')}")
    rows = body.get('all_inds_idex', [])
    row = next((r for r in rows if r.get('stk_cd') == idx_cd), rows[0] if rows else None)
    if row is None:
        raise RuntimeError('전업종지수 데이터가 비어 있습니다.')
    return row


def fetch_program(token, stk_cd, amt_qty_tp='1', cont_yn='N', next_key=''):
    """ka90008(종목시간별프로그램매매추이) 한 페이지 조회.
    반환: (rows, 응답 cont-yn, 응답 next-key) — 연속조회에 사용."""
    url = 'https://api.kiwoom.com/api/dostk/mrkcond'
    headers = {
        'Content-Type': 'application/json;charset=UTF-8',
        'authorization': f'Bearer {token}',
        'cont-yn': cont_yn,
        'next-key': next_key,
        'api-id': 'ka90008',
    }
    data = {'amt_qty_tp': amt_qty_tp, 'stk_cd': stk_cd, 'date': now_kst().strftime('%Y%m%d')}
    resp = requests.post(url, headers=headers, json=data)
    resp.raise_for_status()
    body = resp.json()
    if body.get('return_code') != 0:
        raise RuntimeError(f"프로그램매매 API 오류: {body.get('return_msg')}")
    rows = body.get('stk_tm_prm_trde_trnsn', [])
    return rows, resp.headers.get('cont-yn', 'N'), resp.headers.get('next-key', '')


def fetch_rank(token, qry_tp='5'):
    """ka00198(실시간종목조회순위) — 조회수 상위 20종목."""
    url = 'https://api.kiwoom.com/api/dostk/stkinfo'
    headers = {
        'Content-Type': 'application/json;charset=UTF-8',
        'authorization': f'Bearer {token}',
        'api-id': 'ka00198',
    }
    resp = requests.post(url, headers=headers, json={'qry_tp': qry_tp})
    resp.raise_for_status()
    body = resp.json()
    if body.get('return_code') not in (None, 0):
        raise RuntimeError(f"조회순위 API 오류: {body.get('return_msg')}")
    return body.get('item_inq_rank') or []


_token_cache = {'token': None}


def cached_token():
    if not _token_cache['token']:
        _token_cache['token'] = get_access_token()
    return _token_cache['token']


_name_map = None


def _local_name(code):
    """폴백: 로컬 krx_listed_companies.json(있을 때만) 코드→회사명."""
    global _name_map
    if _name_map is None:
        m = {}
        path = os.path.join(REPO_ROOT, 'common', 'krx_listed_companies.json')
        try:
            with open(path, encoding='utf-8') as f:
                data = json.load(f)
            for r in data:
                c = str(r.get('종목코드', '')).strip()
                if c and c != '000000':
                    m[c.zfill(6)] = r.get('회사명', '')
            print(f'[종목명맵(로컬)] 로드: {len(m)}개')
        except Exception as e:
            print(f'[종목명맵(로컬)] 없음/실패: {e}')
        _name_map = m
    return _name_map.get(code, '')


def stock_name(code):
    """종목코드→회사명. ka10099 API 맵 우선, 없으면 로컬 JSON 폴백."""
    stock_items()   # API 맵 로드 보장(하루 1회)
    n = (_stock_items.get('c2n') or {}).get(code)
    return n if n else _local_name(code)


def to_number(value):
    """부호 포함 문자열을 숫자로. 키움이 음수를 '--1'처럼 이중 부호로 주는 경우도 처리."""
    if value is None:
        return 0
    s = str(value).strip().replace(',', '').replace('+', '')
    neg = s.startswith('-')
    s = s.lstrip('-')          # 선행 '-' 모두 제거(이중 마이너스 대응)
    if not s:
        return 0
    try:
        n = int(s)
    except ValueError:
        try:
            n = float(s)
        except ValueError:
            return 0
    return -n if neg else n


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


def _grid_floor(v):
    """v를 아래쪽 1000 그리드로 내림(음수도 floor). 예: 2050->2000, -1500->-2000."""
    return (int(v) // ALERT_STEP) * ALERT_STEP


def _init_alert_state(market_key, date_str):
    """기준선(level)을 그날 마지막 기록값의 그리드로 초기화(없으면 0).
    서버 재시작 시 과거 이력을 되풀이 알림하지 않고 현재 위치에서 이어감."""
    prev = last_record(market_key, date_str)
    last_v = int(prev.get(ALERT_FIELD, 0)) if prev else 0
    return {'date': date_str, 'level': _grid_floor(last_v), 'last_v': last_v}


def check_move(st, cur_v):
    """기준선(level)에서 한 칸(ALERT_STEP) 이상 움직였으면 알림 신호 반환.
    반환: ('up'|'down', new_level, steps) 또는 None. 상태(level)는 움직인 만큼 이동."""
    level = st['level']
    if cur_v >= level + ALERT_STEP:
        steps = (cur_v - level) // ALERT_STEP
        st['level'] = level + steps * ALERT_STEP
        return ('up', st['level'], steps)
    if cur_v <= level - ALERT_STEP:
        steps = (level - cur_v) // ALERT_STEP
        st['level'] = level - steps * ALERT_STEP
        return ('down', st['level'], steps)
    return None


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

    # 지수(ka20003): 정규장 09:00~15:30 에만 수집(1분봉 종가). cur_prc 등은 이미 소수점 포함 실제값
    if in_index_window(now):
        try:
            irow = fetch_index(token, market['idx_cd'])
            record['idx'] = abs(to_number(irow.get('cur_prc')))   # 하락 시 부호(-)로 오므로 절댓값(가격)
            record['flu'] = to_number(irow.get('flu_rt'))
            record['pred'] = to_number(irow.get('pred_pre'))
            record['sig'] = str(irow.get('pre_sig') or '')
            record['upl'] = to_number(irow.get('upl'))         # 상한 종목수
            record['rising'] = to_number(irow.get('rising'))   # 상승 종목수
            record['flat'] = to_number(irow.get('stdns'))      # 보합 종목수
            record['fall'] = to_number(irow.get('fall'))       # 하락 종목수
            record['lst'] = to_number(irow.get('lst'))         # 하한 종목수
        except Exception as e:
            print(f"  {market['name']} 지수 조회 실패: {e}")

    # 수집 시간대에는 값이 직전과 같아도 매분 기록해 시간축에 빈칸이 없도록 한다.
    # (같은 분에 중복 실행되어 이미 그 분이 기록된 경우에만 스킵)
    prev = last_record(mkey, date_str)
    if prev and prev.get('t', '')[:5] == record['t'][:5]:
        print(f"[{record['t']}] {market['name']} 같은 분 이미 기록(스킵)")
        return token

    # --- 외국인 순매수 milestone 알림 (파일 기록 전에 판정) ---
    # 상승: 새 양수 천단위(+1000,+2000,…) 최초 도달 시만 / 하락: 새 음수 천단위(-1000,…) 최초 도달 시만
    st = _alert_state.get(mkey)
    if st is None or st['date'] != date_str:
        st = _init_alert_state(mkey, date_str)   # 이 시점 파일엔 현재 레코드 미포함
        _alert_state[mkey] = st

    cur_v = int(record[ALERT_FIELD])
    hit = check_move(st, cur_v)
    if hit:
        direction, level, steps = hit
        prev_v = int(st.get('last_v', cur_v))    # 직전 알림 때의 외국인 순매수 값
        st['last_v'] = cur_v
        head = '🟢' if direction == 'up' else '🔴'
        word = '상승' if direction == 'up' else '하락'
        send_telegram_message(f"{head} [{market['name']}] 외국인 순매수 {word}\n"
                              f"{record['t'][:5]}  이전 {prev_v:+,}억 · 현재 {cur_v:+,}억\n"
                              f"\n({ALERT_STEP:,}억 단위 변동 알림)")
        print(f"  → 텔레그램: {market['name']} {word} 이전 {prev_v:+,} 현재 {cur_v:+,}")

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


def in_index_window(now):
    """지수(1분봉 종가) 수집 시간대: 정규장 09:00~15:30."""
    mins = now.hour * 60 + now.minute
    return 9 * 60 <= mins <= 15 * 60 + 30


# 장 시작(수집 시작 08시) 직전 대기 시간대: 07:00~08:00
# 이 구간엔 지수현황을 '오늘 대기' 상태로 표시(전일 종가 + 등락률 0 + 순매수 0 + 회색)
PREOPEN_START_HOUR = 7
MARKET_OPEN_HOUR = 8


def in_preopen_window(now):
    """평일 07:00~08:00(장 시작 전 대기 시간대)이면 True."""
    if now.weekday() >= 5:
        return False
    return PREOPEN_START_HOUR <= now.hour < MARKET_OPEN_HOUR


def sleep_to_next_boundary(interval):
    """벽시계 경계(interval 배수, 예: 60초면 매 분 :00)까지 대기.
    epoch 초 기준이라 시간대와 무관하며, KST(+9:00)에서 60초 배수는 :00에 정렬된다."""
    now = time.time()
    target = (int(now // interval) + 1) * interval
    time.sleep(max(0.0, target - now))


# 매 거래일 이 시각에 KOSPI 외국인/기관 순매수 다이제스트를 텔레그램 전송
DIGEST_SLOTS = ('08:10', '08:20', '08:30', '09:10', '09:20')
_digest_sent = set()   # 'YYYY-MM-DD HH:MM' — 중복 발송 방지


_WEEKDAYS = ('월', '화', '수', '목', '금', '토', '일')


def _netprps_upto(date_str, hhmm):
    """해당 일자에서 hhmm(HH:MM) 시각까지의 마지막 기록(그 시점까지의 누적)."""
    recs = [r for r in read_records('kospi', date_str) if r.get('t', '')[:5] <= hhmm]
    return recs[-1] if recs else None


def _prev_data_dates(before_str, count):
    """before_str(YYYY-MM-DD) '미만'이면서 KOSPI 스냅샷 파일이 있는 날짜를
    최근 순으로 count개 반환. 주말·공휴일(파일 없음)은 자동으로 건너뛴다."""
    prefix = 'netprps_snapshots_kospi_'
    dates = sorted(name[len(prefix):-len('.jsonl')]
                   for name in os.listdir(DATA_DIR)
                   if name.startswith(prefix) and name.endswith('.jsonl'))
    dates = [d for d in dates if d < before_str]
    return list(reversed(dates))[:count]


def send_investor_digest(now, slot):
    """KOSPI 외국인/기관 순매수를 오늘·직전 거래일 2개의 '같은 시각(slot)까지 누적' 기준으로 전송.
    달력상 어제/그제가 아니라 '데이터가 있는 직전 거래일'을 쓰므로, 월요일이어도
    금·목요일 데이터가 나온다(주말/공휴일 자동 건너뜀)."""
    today = now.strftime('%Y-%m-%d')
    dates = [today] + _prev_data_dates(today, 4)   # 오늘 + 직전 거래일 4개(총 5일)
    lines = [f"🟩 {now.strftime('%H시%M분')} KOSPI 외국인, 기관 매매동향"]
    for d in dates:
        dt = datetime.strptime(d, '%Y-%m-%d')
        lab = f"{dt.strftime('%m/%d')}({_WEEKDAYS[dt.weekday()]})"
        tail = ' (오늘)' if d == today else ''   # 오늘 일자는 오른편에 표시
        rec = _netprps_upto(d, slot)      # 각 날의 slot 시각까지 누적
        if not rec or rec.get('frgnr_netprps') is None:
            lines.append(f"{lab} : 데이터 없음{tail}")
        else:
            f = int(rec.get('frgnr_netprps', 0))
            o = int(rec.get('orgn_netprps', 0))
            lines.append(f"{lab} : {f:+,}억 / {o:+,}억{tail}")
    send_telegram_message("\n".join(lines))
    print(f"  → 다이제스트 전송({slot})")


def maybe_send_digest(now):
    slot = now.strftime('%H:%M')
    if slot not in DIGEST_SLOTS:
        return
    key = now.strftime('%Y-%m-%d ') + slot
    if key in _digest_sent:
        return
    _digest_sent.add(key)
    try:
        send_investor_digest(now, slot)
    except Exception as e:
        print(f"  다이제스트 전송 실패: {e}")


# ─────────────── finviz 맵 이미지 텔레그램 전송(매일 07:00 KST) ───────────────
# 여러 맵을 보낼 수 있음. .env FINVIZ_MAP_URLS(콤마 구분)로 교체 가능(없으면 기본값).
# published_map 페이지 URL 또는 publish.finviz.com PNG URL 모두 허용.
FINVIZ_MAP_URLS = [u.strip() for u in (_load_env_value('FINVIZ_MAP_URLS') or (
    'https://finviz.com/published_map?t=sec_all&st=d1&f=090926&i=sec_all_d1_182843770,'
    'https://finviz.com/published_map?t=cap&st=d1&f=090926&i=cap_d1_184388155'
)).split(',') if u.strip()]
FINVIZ_MAP_TIME = '07:00'   # KST 전송 시각
_finviz_map_sent = set()    # 'YYYY-MM-DD' — 하루 1회 전송 보장

# 맵 종류(t 파라미터) → 표시 이름
_FINVIZ_LABELS = {'sec_all': 'All Stocks', 'cap': 'Market Cap', 'sec': 'S&P 500', 'geo': 'World'}
_FINVIZ_UA = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/124.0 Safari/537.36',
    'Referer': 'https://finviz.com/',
}


def _finviz_image_url(page_url):
    """published_map 페이지 URL → 실제 PNG URL(publish.finviz.com/{f}/{i}.png). 이미 png면 그대로."""
    f = re.search(r'[?&]f=([^&]+)', page_url)
    i = re.search(r'[?&]i=([^&]+)', page_url)
    if f and i:
        return f'https://publish.finviz.com/{f.group(1)}/{i.group(1)}.png'
    return page_url


def _finviz_label(page_url):
    t = re.search(r'[?&]t=([^&]+)', page_url)
    key = t.group(1) if t else ''
    return _FINVIZ_LABELS.get(key, key or 'Map')


def _fmt_et(last_modified):
    """HTTP Last-Modified(GMT) → 미국 동부시간(ET) 표기. 실패 시 빈 문자열."""
    if not last_modified:
        return ''
    try:
        dt = datetime.strptime(last_modified.replace(' GMT', ''), '%a, %d %b %Y %H:%M:%S')
        dt = dt.replace(tzinfo=timezone.utc).astimezone(ZoneInfo('America/New_York'))
        return dt.strftime('%a %b %d, %I:%M %p') + ' ET'
    except Exception:
        return ''


def send_finviz_map():
    """설정된 모든 finviz 맵 PNG를 내려받아 텔레그램으로 사진 전송."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print('[finviz] 텔레그램 토큰/챗ID 미설정 — 전송 생략')
        return
    api = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto'
    for page_url in FINVIZ_MAP_URLS:
        try:
            r = requests.get(_finviz_image_url(page_url), headers=_FINVIZ_UA, timeout=30)
            r.raise_for_status()
            when = _fmt_et(r.headers.get('Last-Modified', ''))
            cap = f'📊 Finviz 맵 ({_finviz_label(page_url)}, 1D)' + (f' · {when}' if when else '')
            resp = requests.post(api,
                                 data={'chat_id': TELEGRAM_CHAT_ID, 'caption': cap},
                                 files={'photo': ('finviz_map.png', r.content, 'image/png')})
            if resp.status_code != 200:
                print(f'[finviz 전송 에러] HTTP {resp.status_code} - {resp.text}')
            else:
                print(f'  → finviz 맵 전송({_finviz_label(page_url)})')
        except Exception as e:
            print(f'  finviz 맵 전송 실패({page_url}): {e}')


def maybe_send_finviz_map(now):
    if now.strftime('%H:%M') != FINVIZ_MAP_TIME:
        return
    key = now.strftime('%Y-%m-%d')
    if key in _finviz_map_sent:
        return
    _finviz_map_sent.add(key)
    try:
        send_finviz_map()
    except Exception as e:
        print(f'  finviz 섹터맵 전송 실패: {e}')


def run_loop(interval=60, start_h=8, end_h=20):
    """평일 start_h~end_h 시간대에 매 경계(:00)에 맞춰 모든 시장을 poll_once (무한 루프)."""
    print(f'{interval}초 간격(정각 정렬) 자동 반복 누적 시작 '
          f'(평일 {start_h:02d}:00~{end_h:02d}:00, 주말 휴무, 최근 {KEEP_DAYS}일 보존)')
    prune_old_snapshots()   # 시작 시 1회 정리
    token = None
    idle_notified = False
    while True:
        maybe_send_finviz_map(now_kst())       # 매일 07:00 KST finviz 섹터맵
        if in_collect_window(now_kst(), start_h, end_h):
            idle_notified = False
            for market in MARKETS:
                try:
                    token = poll_once(token, market)
                except Exception as e:
                    print(f"  {market['name']} 조회 실패: {e}")
                    token = None  # 토큰 만료 등 대비해 재발급
            maybe_send_digest(now_kst())       # 08:10 / 09:10 다이제스트
            prune_old_snapshots()              # 새 날짜 파일 생성 대비 정리
            sleep_to_next_boundary(interval)   # 다음 :00 까지 대기(드리프트 없음)
        else:
            if not idle_notified:
                stamp = now_kst().strftime('%Y-%m-%d %H:%M:%S')
                print(f'[{stamp}] 수집 시간대 밖 — 대기 중 '
                      f'(평일 {start_h:02d}:00~{end_h:02d}:00에 재개)')
                idle_notified = True
            sleep_to_next_boundary(60)  # 시간대 밖에서는 매 분 :00에 확인


def prune_old_snapshots():
    """시장별로 가장 최근 KEEP_DAYS일만 남기고 이전 스냅샷 파일 삭제."""
    for m in MARKETS:
        prefix = f"netprps_snapshots_{m['key']}_"
        dates = sorted(name[len(prefix):-len('.jsonl')]
                       for name in os.listdir(DATA_DIR)
                       if name.startswith(prefix) and name.endswith('.jsonl'))
        for d in dates[:-KEEP_DAYS]:                 # 최근 KEEP_DAYS일 제외 나머지
            try:
                os.remove(snapshot_path(m['key'], d))
                print(f"  오래된 파일 삭제: {os.path.basename(snapshot_path(m['key'], d))}")
            except OSError:
                pass


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


def records_for(market_key):
    """해당 시장의 기준일(06시 전이면 전일, 없으면 그 이하 가장 최근 일자) 레코드/일자를 반환."""
    ref_date = effective_date(now_kst())
    records = read_records(market_key, ref_date)
    date_str = ref_date
    if not records:
        prefix = f'netprps_snapshots_{market_key}_'
        dates = []
        for name in os.listdir(DATA_DIR):
            if name.startswith(prefix) and name.endswith('.jsonl'):
                d = name[len(prefix):-len('.jsonl')]
                if d <= ref_date:
                    dates.append(d)
        if dates:
            date_str = max(dates)
            records = read_records(market_key, date_str)
    return records, (date_str if records else None)


def build_payload(market_key):
    markets = [{'key': m['key'], 'name': m['name']} for m in MARKETS]
    if in_preopen_window(now_kst()):
        # 장 시작 전 대기: 전일 데이터 대신 빈 상태로 두고 클라이언트가 '대기 중' 표시
        return {
            'market': market_key, 'markets': markets,
            'date': now_kst().strftime('%Y-%m-%d'), 'label': '',
            'columns': columns_meta(), 'default_selected': DEFAULT_SELECTED,
            'points': [], 'preopen': True,
        }
    records, date_str = records_for(market_key)
    label = records[-1]['label'] if records else ''
    return {
        'market': market_key,
        'markets': markets,
        'date': date_str,
        'label': label,
        'columns': columns_meta(),
        'default_selected': DEFAULT_SELECTED,
        'points': records,
    }


def available_dates(market_key):
    """해당 시장의 저장된 일자 목록(오름차순)."""
    prefix = f'netprps_snapshots_{market_key}_'
    dates = []
    for name in os.listdir(DATA_DIR):
        if name.startswith(prefix) and name.endswith('.jsonl'):
            dates.append(name[len(prefix):-len('.jsonl')])
    return sorted(dates)


def idx_series(records):
    """지수 1분봉 종가 시계열 — 정규장 09:00~15:30 구간만. idx는 절댓값(가격)."""
    return [{'t': r['t'], 'idx': abs(r['idx'])} for r in records
            if r.get('idx') is not None and '09:00' <= r['t'][:5] <= '15:30']


def build_summary():
    """코스피/코스닥 요약: 지수/등락률 + 개인·외국인·기관 순매수 + 지수 1분봉 종가 시계열(오늘+전거래일)."""
    out = []
    now = now_kst()
    preopen = in_preopen_window(now)          # 07~08시 장 시작 전 대기 상태
    for m in MARKETS:
        records, date_str = records_for(m['key'])
        last = records[-1] if records else {}
        # 지수/등락/종목현황은 idx가 있는 마지막 레코드(=최근 정규장 값) 기준 → 장 마감 후에도 15:30 값 유지
        ir = next((r for r in reversed(records) if r.get('idx') is not None), {})
        # 전 거래일(현재 표시일보다 앞선 가장 최근 저장 일자) 시계열
        prev_records, prev_date = [], None
        if date_str:
            prevs = [d for d in available_dates(m['key']) if d < date_str]
            if prevs:
                prev_date = max(prevs)
                prev_records = read_records(m['key'], prev_date)

        today_series = idx_series(records)
        prev_series = idx_series(prev_records)
        # 오늘(표시일) 지수가 아직 없으면(07~08 대기 or 정규장 지수 시작 전) 대기 상태로 표시.
        # '데이터 수집 대기 중…' 대신 전일 종가 + (+0.00%) + 순매수 0 + 회색으로.
        if preopen or ir.get('idx') is None:
            prev_close = (today_series[-1]['idx'] if today_series
                          else (prev_series[-1]['idx'] if prev_series else None))
            gray_series = today_series if today_series else prev_series   # 직전 완료 세션 시계열
            # 순매수는 대기 중에도 '오늘' 값으로 갱신(07~08시엔 오늘 데이터가 없어 0)
            today_recs = read_records(m['key'], effective_date(now))
            iv = today_recs[-1] if today_recs else {}
            out.append({
                'key': m['key'], 'name': m['disp'], 'date': date_str, 'prev_date': prev_date,
                'idx': prev_close, 'flu': 0.0, 'sig': None, 'pred': None,
                'upl': 0, 'rising': 0, 'flat': 0, 'fall': 0, 'lst': 0,
                'ind': iv.get('ind_netprps', 0), 'frgnr': iv.get('frgnr_netprps', 0),
                'orgn': iv.get('orgn_netprps', 0),
                'series': [],                       # 오늘 데이터 없음(대기)
                'series_prev': gray_series,         # 전일 시계열(회색선)
                'preopen': True,
            })
            continue
        out.append({
            'key': m['key'], 'name': m['disp'], 'date': date_str, 'prev_date': prev_date,
            'idx': abs(ir['idx']), 'flu': ir.get('flu'),
            'sig': ir.get('sig'), 'pred': ir.get('pred'),
            'upl': ir.get('upl'), 'rising': ir.get('rising'), 'flat': ir.get('flat'),
            'fall': ir.get('fall'), 'lst': ir.get('lst'),
            'ind': last.get('ind_netprps'), 'frgnr': last.get('frgnr_netprps'),
            'orgn': last.get('orgn_netprps'),
            'series': today_series,
            'series_prev': prev_series,
        })
    return {'markets': out}


def build_program(stk_cd, cont_yn='N', next_key='', amt_qty_tp='1'):
    """프로그램매매 한 페이지(최신 또는 연속조회 다음 페이지) + 다음 페이지 커서 반환.
    클라이언트가 '연속조회' 버튼으로 페이지를 직접 이어받는다."""
    # 금액(1): 백만원, 수량(2): 주 — 필드 선택
    net_f = 'prm_netprps_qty' if amt_qty_tp == '2' else 'prm_netprps_amt'
    chg_f = 'prm_netprps_qty_irds' if amt_qty_tp == '2' else 'prm_netprps_amt_irds'

    # 통합(SOR) 거래소 데이터로 조회: 6자리 코드에 '_AL' 부착
    base_cd = stk_cd.split('_')[0]
    api_cd = base_cd + '_AL'

    def run(tok):
        rows, c, nk = fetch_program(tok, api_cd, amt_qty_tp, cont_yn, next_key)
        pts = []
        for r in rows:
            tm = r.get('tm')
            if not tm:
                continue
            pts.append({'t': tm[:2] + ':' + tm[2:4], 'tm': tm,
                        'net': to_number(r.get(net_f)),
                        'chg': to_number(r.get(chg_f)),
                        'cur': abs(to_number(r.get('cur_prc'))),   # 현재가는 절댓값(하락 시 부호 -)
                        'flu': to_number(r.get('flu_rt'))})
        return pts, c, nk

    try:
        pts, c, nk = run(cached_token())
    except Exception:
        _token_cache['token'] = None                 # 토큰 만료 등 대비 1회 재시도
        pts, c, nk = run(cached_token())

    return {'stk_cd': base_cd, 'name': stock_name(base_cd),
            'series': pts, 'cont': c, 'next': nk}


def fetch_stock_list(token, mrkt_tp):
    """ka10099(종목정보 리스트) — 시장의 전체 종목 [{code,name,...}] 반환."""
    url = 'https://api.kiwoom.com/api/dostk/stkinfo'
    headers = {
        'Content-Type': 'application/json;charset=UTF-8',
        'authorization': f'Bearer {token}',
        'cont-yn': 'N', 'next-key': '',
        'api-id': 'ka10099',
    }
    resp = requests.post(url, headers=headers, json={'mrkt_tp': mrkt_tp})
    resp.raise_for_status()
    body = resp.json()
    if body.get('return_code') not in (None, 0):
        raise RuntimeError(f"종목목록 API 오류: {body.get('return_msg')}")
    return body.get('list') or []


# 종목명↔코드 맵 캐시(하루 단위) — ka10099로 코스피/코스닥 전체 수집
_stock_items = {'date': None, 'items': None}   # items: [(code, name), ...]


def stock_items():
    today = now_kst().strftime('%Y-%m-%d')
    if _stock_items['items'] is None or _stock_items['date'] != today:
        items, c2n = [], {}
        try:
            tok = cached_token()
            for mrkt in ('0', '10'):          # 0:코스피, 10:코스닥 (ka10099 시장구분)
                for r in fetch_stock_list(tok, mrkt):
                    c = (r.get('code') or '').strip()[:6]
                    n = (r.get('name') or '').strip()
                    if c and n:
                        items.append((c, n)); c2n[c] = n
            print(f'[종목목록] ka10099 로드: {len(items)}개')
        except Exception as e:
            print(f'[종목목록] ka10099 로드 실패: {e}')
        if items:
            _stock_items['items'] = items
            _stock_items['c2n'] = c2n
            _stock_items['date'] = today
    return _stock_items['items'] or []


def _pick(items, q):
    for c, n in items:
        if n == q:
            return {'code': c, 'name': n}
    starts = [(c, n) for c, n in items if n.startswith(q)]
    if starts:
        starts.sort(key=lambda x: len(x[1]))
        return {'code': starts[0][0], 'name': starts[0][1]}
    contains = [(c, n) for c, n in items if q in n]
    if contains:
        contains.sort(key=lambda x: len(x[1]))
        return {'code': contains[0][0], 'name': contains[0][1]}
    return None


def search_stock(q):
    """종목명(부분)으로 종목코드 검색 — ka10099 종목목록 사용(정확→시작→포함).
    API 실패 시 로컬 krx_listed_companies.json 으로 폴백."""
    q = (q or '').strip()
    if not q:
        return None
    hit = _pick(stock_items(), q)
    if hit:
        return hit
    stock_name('')                       # 폴백: _name_map(코드→회사명)
    return _pick([(c, n) for c, n in _name_map.items()], q)


def build_rank():
    """조회수 상위 20종목: 순위/종목명/코드/등락율/부호."""
    def run(tok):
        rows = fetch_rank(tok)
        out = []
        for i, r in enumerate(rows[:20]):
            out.append({
                'rank': i + 1,
                'name': r.get('stk_nm', ''),
                'code': r.get('stk_cd', ''),
                'rank_sign': str(r.get('rank_chg_sign') or '').strip(),   # +:상승 -:하락
                'rank_chg': str(r.get('rank_chg') or '').strip(),
                'price': abs(to_number(r.get('past_curr_prc'))),           # 기준시점 주가(절댓값)
                'psign': str(r.get('base_comp_sign') or ''),              # 1상한2상승3보합4하한5하락
                'chgr': to_number(r.get('base_comp_chgr')),               # 기준시점 등락율(%)
                'prev': to_number(r.get('prev_base_chgr')),               # 직전(30초 전) 대비율(%)
            })
        return out
    try:
        items = run(cached_token())
    except Exception:
        _token_cache['token'] = None
        items = run(cached_token())
    return {'items': items}


# 서빙 허용 HTML 파일: 경로 -> 파일명
PAGES = {'/': 'index.html', '/index.html': 'index.html'}


def load_file(name):
    with open(os.path.join(BASE_DIR, name), 'r', encoding='utf-8') as f:
        return f.read()


class Handler(http.server.BaseHTTPRequestHandler):
    timeout = 30

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        if path in PAGES:
            try:
                body = load_file(PAGES[path]).encode('utf-8')
            except FileNotFoundError:
                self.send_error(500, f'{PAGES[path]} not found')
                return
            self._send(body, 'text/html; charset=utf-8')
        elif path == '/api/netprps':
            qs = parse_qs(parsed.query)
            mkey = (qs.get('mrkt', [MARKETS[0]['key']])[0])
            if mkey not in MARKET_BY_KEY:
                mkey = MARKETS[0]['key']
            body = json.dumps(build_payload(mkey), ensure_ascii=False).encode('utf-8')
            self._send(body, 'application/json; charset=utf-8')
        elif path == '/api/summary':
            body = json.dumps(build_summary(), ensure_ascii=False).encode('utf-8')
            self._send(body, 'application/json; charset=utf-8')
        elif path == '/api/rank':
            try:
                payload = build_rank()
            except Exception as e:
                payload = {'items': [], 'error': str(e)}
            body = json.dumps(payload, ensure_ascii=False).encode('utf-8')
            self._send(body, 'application/json; charset=utf-8')
        elif path == '/api/stock':
            qs = parse_qs(parsed.query)
            q = qs.get('q', [''])[0]
            hit = None
            try:
                hit = search_stock(q)
            except Exception:
                hit = None
            body = json.dumps(hit or {'code': '', 'name': ''}, ensure_ascii=False).encode('utf-8')
            self._send(body, 'application/json; charset=utf-8')
        elif path == '/api/program':
            qs = parse_qs(parsed.query)
            stk = (qs.get('stk', ['005930'])[0] or '005930').strip()[:20]
            cont = qs.get('cont', ['N'])[0]
            nkey = qs.get('next', [''])[0]
            amt = qs.get('amt', ['1'])[0]
            if amt not in ('1', '2'):
                amt = '1'
            try:
                payload = build_program(stk, cont, nkey, amt)
            except Exception as e:
                payload = {'stk_cd': stk, 'name': '', 'series': [], 'cont': 'N', 'next': '', 'error': str(e)}
            body = json.dumps(payload, ensure_ascii=False).encode('utf-8')
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

    stock_items()    # 종목명 맵(ka10099) 예열 — 콘솔에 개수/에러 출력

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
