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


def now_kst():
    return datetime.now(KST)


# 한국시간 기준 08:00~20:00 기록 시간대(토, 일 제외)인지 확인하는 함수
def is_recording_hours(now):
    if now.weekday() >= 5:  # 5=토요일, 6=일요일
        return False
    return RECORD_START_HOUR <= now.hour < RECORD_END_HOUR


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
        item_count = len(body.get("item_inq_rank") or [])
        print(f"[응답 수신] return_code={return_code} item_count={item_count}")
        if return_code not in (None, 0):
            print(
                f"[조회 에러] return_code={return_code} return_msg={body.get('return_msg')}"
            )
            return None
        return body
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


# 직전 대비 등락율이 +1% 이상인 종목만 걸러서 출력하는 함수
def print_surge_items(curr_data, threshold=1.0):
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

        # 조건: 직전대비 등락율이 threshold(기본 1%) 이상이어야 함
        if chgr_val < threshold:
            continue

        print(
            f" 🔥 {rank}위: {name}({code}) | 현재가: {price}원 | 직전대비: +{chgr_val}%"
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

            # API 호출 (빈 응답 시 자동 재시도)
            current_rank_list = fetch_rank_list(token=access_token, data=stk_params)

            if current_rank_list:
                save_snapshot(now_kst(), current_rank_list)
                print_surge_items(current_rank_list, threshold=1.0)

            else:
                print("[경고] 데이터를 정상적으로 가져오지 못했습니다.")

            # 중복 실행 방지
            time.sleep(2)

    except KeyboardInterrupt:
        print("\n>>> 사용자에 의해 모니터링이 종료되었습니다.")