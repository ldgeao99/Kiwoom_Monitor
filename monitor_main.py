# main.py
import json
import os
import sys
import time
from datetime import datetime
from zoneinfo import ZoneInfo
import requests

# 토큰 발급 모듈 불러오기
from publish_auth_token import get_access_token


KST = ZoneInfo("Asia/Seoul")
RECORD_START_HOUR = 8   # 08:00
RECORD_END_HOUR = 20    # 20:00 (미포함)

SNAPSHOT_DIR = "daily_snapshot"

# 텔레그램 급등 알림 조건: 조회순위가 이 값 이상 상승 & 직전대비 등락율이 이 값 이상
RANK_JUMP_THRESHOLD = 5
SURGE_CHGR_THRESHOLD = 0.9

# 조회순위가 이 값 이내(상위)인 종목은 순위 상승폭과 무관하게
# 직전대비 등락율(SURGE_CHGR_THRESHOLD) 조건만 만족하면 알림을 보냄
TOP_RANK_ALERT_THRESHOLD = 4

# 같은 종목은 알림을 보낸 뒤 이 시간(초) 동안 다시 알리지 않음
ALERT_COOLDOWN_SECONDS = 60 * 60

# 장 시작 직후처럼 노이즈가 많아 알림을 보내지 않을 시간대 (시, 분) 구간들
NOTIFY_BLACKOUT_WINDOWS = [
    ((8, 0), (8, 1)),
    ((9, 0), (9, 1)),
]


# 토큰이 만료/무효화되어 재발급이 필요함을 나타내는 예외
class TokenExpiredError(Exception):
    pass


def now_kst():
    return datetime.now(KST)


# 한국시간 기준 08:00~20:00 기록 시간대(토, 일 제외)인지 확인하는 함수
def is_recording_hours(now):
    if now.weekday() >= 5:  # 5=토요일, 6=일요일
        return False
    return RECORD_START_HOUR <= now.hour < RECORD_END_HOUR


# 장 시작 직후 등 노이즈가 많은 시간대(NOTIFY_BLACKOUT_WINDOWS)인지 확인하는 함수
def is_notify_blackout(now):
    current = (now.hour, now.minute)
    return any(start <= current < end for start, end in NOTIFY_BLACKOUT_WINDOWS)


# .env 파일에서 지정한 key의 값을 읽어오는 함수
def load_env_value(key):
    env_file = ".env"
    if not os.path.exists(env_file):
        return None
    with open(env_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            if k.strip() == key:
                return v.strip()
    return None


TELEGRAM_BOT_TOKEN = load_env_value("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = load_env_value("TELEGRAM_CHAT_ID")


# 텔레그램으로 메시지를 전송하는 함수
def send_telegram_message(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[텔레그램] TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID가 설정되지 않아 메시지를 보내지 않습니다.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        response = requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text})
        if response.status_code != 200:
            print(f"[텔레그램 에러] HTTP {response.status_code} - {response.text}")
    except Exception as e:
        print(f"[텔레그램 에러] 전송 중 예외 발생: {e}")


# 부호가 붙은 가격 문자열을 부호 없이 천단위 콤마로 포맷하는 함수
def format_price(price_str):
    try:
        value = abs(int(str(price_str).replace("+", "").replace("-", "")))
        return f"{value:,}"
    except (ValueError, TypeError):
        return str(price_str)


# 종목코드별 조회순위 변화를 계산하고 last_rank_by_code를 갱신하는 함수
# 직전 주기에도 top20 안에서 추적 중이던 종목은 우리가 직접 기록해둔 이전 순위와 비교해서 계산하고,
# top20 밖에서 새로 진입해 비교할 기준이 없는 종목만 API의 rank_chg 값을 그대로 사용한다.
def compute_rank_changes(curr_data, last_rank_by_code):
    rank_changes = {}
    for item in curr_data:
        code = item.get("stk_cd", "")
        rank = int(item.get("bigd_rank", 0) or 0)
        prev_rank = last_rank_by_code.get(code)

        if prev_rank is not None:
            rank_chg = prev_rank - rank
        else:
            try:
                rank_chg = int(item.get("rank_chg", "0") or "0")
            except ValueError:
                rank_chg = None

        rank_changes[code] = rank_chg
        last_rank_by_code[code] = rank

    return rank_changes


# 알림 대상 종목을 찾아내는 함수
# - 조회순위 상위(TOP_RANK_ALERT_THRESHOLD 이내) 종목: 직전대비 등락율 조건만 만족하면 알림
# - 그 외 종목: 조회순위 급등 + 직전대비 급등 조건을 동시에 만족해야 알림
def find_surge_alerts(curr_data, rank_changes):
    alerts = []
    for item in curr_data:
        code = item.get("stk_cd", "")
        rank = int(item.get("bigd_rank", 0) or 0)
        rank_chg = rank_changes.get(code)

        try:
            prev_chgr = float((item.get("prev_base_chgr") or "0").replace("+", ""))
        except ValueError:
            prev_chgr = 0.0

        # 조회순위 상위 종목은 순위 상승폭과 무관하게 직전대비 등락율만으로 알림
        if 0 < rank <= TOP_RANK_ALERT_THRESHOLD:
            if prev_chgr >= SURGE_CHGR_THRESHOLD:
                alerts.append(item)
            continue

        # 그 외 종목은 순위 급등과 직전대비 급등을 동시에 만족해야 알림
        if rank_chg is None:
            continue
        if rank_chg >= RANK_JUMP_THRESHOLD and prev_chgr >= SURGE_CHGR_THRESHOLD:
            alerts.append(item)

    return alerts


# 이미 최근(ALERT_COOLDOWN_SECONDS 이내)에 알림을 보낸 종목은 걸러내고,
# 실제로 알림을 보낼 종목만 남기며 그 시각으로 last_alert_time_by_code를 갱신하는 함수
def filter_alert_cooldown(alerts, last_alert_time_by_code, now):
    filtered = []
    for item in alerts:
        code = item.get("stk_cd", "")
        last_time = last_alert_time_by_code.get(code)
        if last_time is not None and (now - last_time).total_seconds() < ALERT_COOLDOWN_SECONDS:
            continue
        filtered.append(item)
        last_alert_time_by_code[code] = now

    return filtered


# 급등 종목 리스트를 하나의 텔레그램 메시지로 묶어서 전송하는 함수
def notify_surge_alerts(alerts):
    if not alerts:
        return

    lines = []
    for item in alerts:
        name = item.get("stk_nm", "")
        rank = item.get("bigd_rank", "")
        price = format_price(item.get("past_curr_prc", "0"))
        base_chgr = item.get("base_comp_chgr", "0.00")
        lines.append(f"🟢 {name} | {rank}위 | {price}원 | {base_chgr}%")

    lines.append("")
    lines.append(
        f"(조회 {RANK_JUMP_THRESHOLD}위 이상 급등 & 직전비 +{SURGE_CHGR_THRESHOLD}% 상승)"
    )
    lines.append(
        f"(조회 {TOP_RANK_ALERT_THRESHOLD}위 이내는 직전비 +{SURGE_CHGR_THRESHOLD}%만으로 알림)"
    )
    lines.append("(08:00, 09:00 직후엔 1분간 탐지스킵)")
    lines.append("(1시간 간격 중복종목 알림생략)")
    send_telegram_message("\n".join(lines))


# 날짜(한국시간 기준)에 해당하는 스냅샷 파일 경로를 반환하는 함수
def snapshot_file_for(date):
    return os.path.join(SNAPSHOT_DIR, f"rank_snapshots_{date.strftime('%Y-%m-%d')}.jsonl")


# 매 주기마다 조회한 전체 종목 스냅샷을 날짜별 JSONL 파일에 한 줄씩 추가 저장하는 함수
# (임계값을 나중에 바꿔가며 검증할 수 있도록 원본 데이터를 그대로 남긴다)
def save_snapshot(now, curr_data):
    os.makedirs(SNAPSHOT_DIR, exist_ok=True)
    record = {"timestamp": now.isoformat(), "items": curr_data}
    with open(snapshot_file_for(now), "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


# 실시간종목조회순위 API 호출 함수
def fn_ka00198(token, data):
    host = "https://api.kiwoom.com"  # 실전투자
    endpoint = "/api/dostk/stkinfo"
    url = host + endpoint

    headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "authorization": f"Bearer {token}",
        "api-id": "ka00198",
    }

    try:
        response = requests.post(url, headers=headers, json=data)
        if response.status_code != 200:
            print(
                f"[조회 에러] HTTP {response.status_code} - {response.text}"
            )
            return None

        body = response.json()
        return_code = body.get("return_code")
        return_msg = body.get("return_msg") or ""
        item_count = len(body.get("item_inq_rank") or [])
        print(f"[응답 수신] return_code={return_code} item_count={item_count}")
        if return_code not in (None, 0):
            print(f"[조회 에러] return_code={return_code} return_msg={return_msg}")
            if "8005" in return_msg or "Token" in return_msg:
                raise TokenExpiredError(return_msg)
            return None
        return body
    except TokenExpiredError:
        raise
    except Exception as e:
        print(f"[조회 에러] 요청 중 예외 발생: {e}")
        return None


# 빈 응답(item_inq_rank가 없거나 비어있음) 시 짧게 대기 후 재시도하는 함수
def fetch_rank_list(token, data, max_retries=2, retry_delay=0.5):
    for attempt in range(max_retries + 1):
        response_data = fn_ka00198(token=token, data=data)
        items = (response_data or {}).get("item_inq_rank") or []
        if items:
            return items
        if attempt < max_retries:
            print(f"[경고] 빈 응답 수신 (시도 {attempt + 1}/{max_retries + 1}) - {retry_delay}초 후 재시도")
            time.sleep(retry_delay)
    print("[경고] 재시도 후에도 데이터를 가져오지 못했습니다.")
    return None


# 직전 대비 등락율이 threshold 이상인 종목만 순위변화와 함께 걸러서 출력하는 함수
def print_surge_items(curr_data, rank_changes, threshold=SURGE_CHGR_THRESHOLD):
    print(f"\n>>> 📈 [직전대비 +{threshold}% 이상 상승 종목] <<<")
    has_changes = False

    for item in curr_data:
        code = item["stk_cd"]
        name = item["stk_nm"]
        rank = int(item["bigd_rank"])
        price = item.get("past_curr_prc", "0")
        prev_chgr_str = item.get("prev_base_chgr", "0.00")

        # 직전 기준 대비 등락율을 실수(float)로 변환
        try:
            # 부호(+, -) 제거 후 float 변환
            chgr_val = float(prev_chgr_str.replace("+", ""))
        except ValueError:
            chgr_val = 0.0

        # 조건: 직전대비 등락율이 threshold 이상이어야 함
        if chgr_val < threshold:
            continue

        rank_chg = rank_changes.get(code)
        rank_chg_str = f"{rank_chg:+d}" if rank_chg is not None else "N/A"

        print(
            f" 🔥 {rank}위: {name}({code}) | 현재가: {price}원 | 직전대비: +{chgr_val}% | 순위변화: {rank_chg_str}"
        )
        has_changes = True

    if not has_changes:
        print(f"직전대비 +{threshold}% 이상 상승한 종목 없음")


# 매 분 01초 혹은 31초가 될 때까지 대기하는 함수
def wait_until_next_cycle():
    while True:
        now = now_kst()
        if now.second == 1 or now.second == 31:
            break
        time.sleep(0.1)


# 실행 구간
if __name__ == "__main__":
    print(">>> 실시간 종목 순위 변동 모니터링 시스템을 시작합니다.")

    # 1. API 토큰 발급 받기
    access_token = get_access_token()
    if not access_token:
        print("토큰 발급에 실패하여 모니터링을 시작할 수 없습니다.")
        sys.exit(1)

    stk_params = {"qry_tp": "5"}  # 30초 누적 설정

    print(
        f">>> 기록 시간대: 한국시간 {RECORD_START_HOUR:02d}:00 ~ {RECORD_END_HOUR:02d}:00"
    )
    print(
        ">>> 대기 중... 매 분 01초 / 31초가 되면 조회를 시작합니다. (Ctrl+C로 종료)"
    )

    was_recording_hours = True
    last_rank_by_code = {}
    last_alert_time_by_code = {}

    try:
        while True:
            # 기록 시간대(08:00~20:00, KST)가 아니면 1분 간격으로만 확인하며 대기
            if not is_recording_hours(now_kst()):
                if was_recording_hours:
                    print(
                        f"\n>>> 기록 시간대({RECORD_START_HOUR:02d}:00~{RECORD_END_HOUR:02d}:00, 평일)가 아니므로 대기합니다."
                    )
                    was_recording_hours = False
                time.sleep(60)
                continue
            was_recording_hours = True

            # 01초 또는 31초 정각이 될 때까지 대기
            wait_until_next_cycle()

            current_time = now_kst().strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n==================================================")
            print(f"📊 [실시간 종목 순위 조회] - {current_time}")
            print(f"==================================================")

            # API 호출 (빈 응답 시 자동 재시도, 토큰 만료 시 재발급 후 재시도)
            try:
                current_rank_list = fetch_rank_list(token=access_token, data=stk_params)
            except TokenExpiredError:
                print("[경고] 토큰이 만료되어 재발급합니다.")
                access_token = get_access_token()
                if not access_token:
                    print("[경고] 토큰 재발급에 실패했습니다. 다음 주기에 다시 시도합니다.")
                    current_rank_list = None
                else:
                    try:
                        current_rank_list = fetch_rank_list(token=access_token, data=stk_params)
                    except TokenExpiredError:
                        print("[경고] 재발급한 토큰도 거부되었습니다. 다음 주기에 다시 시도합니다.")
                        current_rank_list = None

            if current_rank_list:
                save_snapshot(now_kst(), current_rank_list)

                rank_changes = compute_rank_changes(current_rank_list, last_rank_by_code)
                print_surge_items(current_rank_list, rank_changes)

                alerts = find_surge_alerts(current_rank_list, rank_changes)
                if is_notify_blackout(now_kst()):
                    print("[텔레그램] 노이즈가 많은 시간대라 알림을 보내지 않습니다.")
                else:
                    alerts = filter_alert_cooldown(alerts, last_alert_time_by_code, now_kst())
                    notify_surge_alerts(alerts)

            else:
                print("[경고] 데이터를 정상적으로 가져오지 못했습니다.")

            # 중복 실행 방지
            time.sleep(2)

    except KeyboardInterrupt:
        print("\n>>> 사용자에 의해 모니터링이 종료되었습니다.")