#!/usr/bin/env bash
# 수동 배포: git pull 후 대시보드·순위 모니터 재시작
# 사용: ./deploy.sh   (레포 어디서 실행해도 됨)
cd "$(dirname "$0")" || exit 1          # 레포 루트로 이동
exec 9> .git/auto-deploy.lock
flock -n 9 || { echo "다른 배포가 진행 중입니다."; exit 1; }
git pull --ff-only || { echo "git pull 실패"; exit 1; }
pkill -f 'dashboard_server.py' 2>/dev/null || true
pkill -f '[r]ank_monitor_main[.]py' 2>/dev/null || true
sleep 1
cd apps/dashboard || exit 1
nohup python3 -u dashboard_server.py > dashboard.log 2>&1 9>&- &
dashboard_pid=$!
cd ../rank_monitor || exit 1
nohup python3 -u rank_monitor_main.py > rank_monitor.log 2>&1 9>&- < /dev/null &
rank_pid=$!
sleep 1
kill -0 "$dashboard_pid" 2>/dev/null || { echo "대시보드 시작 실패 — apps/dashboard/dashboard.log 확인"; exit 1; }
kill -0 "$rank_pid" 2>/dev/null || { echo "순위 모니터 시작 실패 — apps/rank_monitor/rank_monitor.log 확인"; exit 1; }
echo "재시작됨: dashboard=$dashboard_pid rank_monitor=$rank_pid"
