#!/usr/bin/env bash
# 수동 배포: git pull 후 대시보드 서버 재시작
# 사용: ./deploy.sh   (레포 어디서 실행해도 됨)
cd "$(dirname "$0")" || exit 1          # 레포 루트로 이동
git pull --ff-only || { echo "git pull 실패"; exit 1; }
pkill -f 'dashboard_server.py' 2>/dev/null || true
sleep 1
cd apps/dashboard || exit 1
nohup python3 -u dashboard_server.py > dashboard.log 2>&1 &
sleep 1
echo "재시작됨:"
pgrep -af dashboard_server.py || echo "  (프로세스 확인 실패 — dashboard.log 확인)"
