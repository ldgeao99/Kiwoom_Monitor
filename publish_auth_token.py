# auth_manager.py
import os
import requests


def get_credentials():
    """.env 파일에서 APP_KEY와 SECRET_KEY를 로드합니다."""
    # 실행 위치(cwd)와 무관하게 이 파일이 있는 폴더(프로젝트 루트)의 .env를 찾는다
    env_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")

    if not os.path.exists(env_file):
        raise FileNotFoundError(
            f"'{env_file}' 파일이 존재하지 않습니다. 설정 파일을 먼저 생성해주세요."
        )

    env_values = {}
    with open(env_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            env_values[key.strip()] = value.strip()

    try:
        app_key = env_values["APP_KEY"]
        secret_key = env_values["SECRET_KEY"]
        return app_key, secret_key
    except KeyError:
        raise KeyError(
            ".env 파일에 APP_KEY / SECRET_KEY 항목이 누락되었습니다."
        )


def get_access_token():
    """설정 파일의 키 정보를 이용하여 키움 API로부터 새로운 접근 토큰을 발급받습니다."""
    try:
        app_key, secret_key = get_credentials()
    except Exception as e:
        print(f"[인증 에러] 키 로드 실패: {e}")
        return None

    # host = 'https://mockapi.kiwoom.com' # 모의투자
    host = "https://api.kiwoom.com"  # 실전투자
    endpoint = "/oauth2/token"
    url = host + endpoint

    headers = {
        "Content-Type": "application/json;charset=UTF-8",
    }

    data = {
        "grant_type": "client_credentials",
        "appkey": app_key,
        "secretkey": secret_key,
    }

    try:
        response = requests.post(url, headers=headers, json=data)

        if response.status_code == 200:
            resp_json = response.json()
            token = resp_json.get("token")
            if token:
                print("[인증 성공] 토큰 발급 완료")
                return token
            else:
                print(
                    "[인증 실패] 응답 Body에 'token' 키가 존재하지 않습니다."
                )
                return None
        else:
            print(
                f"[인증 실패] HTTP Status Code: {response.status_code}"
            )
            print(f"응답 내용: {response.text}")
            return None

    except Exception as e:
        print(f"[인증 에러] 토큰 요청 중 오류 발생: {e}")
        return None