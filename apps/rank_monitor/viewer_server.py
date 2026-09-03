# server.py
import http.server
import json
import os
import re
import socketserver
from urllib.parse import unquote, urlparse

PORT = 8000
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "daily_snapshot")

# daily_snapshot 폴더 안에서 노출을 허용할 파일명 패턴 (rank_monitor_main.py가 저장하는 형식과 일치)
FILENAME_PATTERN = re.compile(r"^rank_snapshots_\d{4}-\d{2}-\d{2}\.jsonl$")


class ViewerHandler(http.server.BaseHTTPRequestHandler):
    # 멈춘/느린 커넥션이 스레드를 무한정 붙잡지 않도록 요청 소켓 타임아웃 설정
    timeout = 30

    def do_GET(self):
        path = unquote(urlparse(self.path).path)

        if path in ("/", "/viewer.html"):
            self._serve_file(os.path.join(BASE_DIR, "viewer.html"), "text/html; charset=utf-8")
        elif path == "/api/files":
            self._serve_file_list()
        elif path.startswith("/daily_snapshot/"):
            self._serve_data_file(path[len("/daily_snapshot/"):])
        else:
            self.send_error(404, "Not Found")

    def _serve_file(self, filepath, content_type):
        if not os.path.isfile(filepath):
            self.send_error(404, "Not Found")
            return
        with open(filepath, "rb") as f:
            body = f.read()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _serve_file_list(self):
        os.makedirs(DATA_DIR, exist_ok=True)
        names = sorted(
            (f for f in os.listdir(DATA_DIR) if FILENAME_PATTERN.match(f)),
            reverse=True,
        )
        entries = []
        for name in names:
            full_path = os.path.join(DATA_DIR, name)
            entries.append(
                {
                    "name": name,
                    "size": os.path.getsize(full_path),
                    "modified": os.path.getmtime(full_path),
                }
            )
        self._respond_json(entries)

    def _serve_data_file(self, raw_name):
        # ../ 등을 이용한 경로 이탈을 막기 위해 파일명만 취하고 허용 패턴을 검증한다
        name = os.path.basename(raw_name)
        if not FILENAME_PATTERN.match(name):
            self.send_error(403, "Forbidden")
            return
        filepath = os.path.join(DATA_DIR, name)
        if not os.path.isfile(filepath):
            self.send_error(404, "Not Found")
            return
        self._serve_file(filepath, "application/json; charset=utf-8")

    def _respond_json(self, data):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass


# 요청마다 별도 스레드로 처리해, 멈춘 커넥션 하나가 서버 전체를 막지 않도록 함
class ThreadingHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    # 재시작 직후 TIME_WAIT 상태의 이전 소켓 때문에 바인딩이 실패하지 않도록 허용
    allow_reuse_address = True
    # 남아있는 워커 스레드가 프로세스 종료를 막지 않도록 데몬으로 실행
    daemon_threads = True


if __name__ == "__main__":
    os.makedirs(DATA_DIR, exist_ok=True)
    with ThreadingHTTPServer(("", PORT), ViewerHandler) as httpd:
        print(f">>> 뷰어 서버 실행: http://localhost:{PORT}")
        print(f">>> 노출 폴더: {DATA_DIR} (rank_snapshots_YYYY-MM-DD.jsonl 형식만 허용)")
        print(">>> 종료하려면 Ctrl+C")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\n>>> 서버를 종료합니다.")
