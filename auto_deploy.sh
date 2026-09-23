#!/usr/bin/env bash
# 자동 배포: origin/main 에 새 커밋이 있으면 pull + 대시보드 재시작.
# cron 으로 1분마다 실행 → push 하면 최대 1분 내 서버 반영.
#   crontab -e 에 아래 한 줄 추가(경로는 실제 위치로):
#   * * * * * /home/ubuntu/Kiwoom_Monitor/auto_deploy.sh >> /home/ubuntu/Kiwoom_Monitor/auto_deploy.log 2>&1
cd "$(dirname "$0")" || exit 0          # 레포 루트
git fetch -q origin main || exit 0
LOCAL=$(git rev-parse @ 2>/dev/null)
REMOTE=$(git rev-parse origin/main 2>/dev/null)
[ -z "$REMOTE" ] && exit 0
[ "$LOCAL" = "$REMOTE" ] && exit 0       # 변경 없음 → 종료(로그도 안 남김)

echo "[$(date '+%F %T')] 새 커밋 감지($REMOTE) → 배포 시작"
git pull --ff-only || { echo "  git pull 실패"; exit 1; }
pkill -f 'dashboard_server.py' 2>/dev/null || true
sleep 1
cd apps/dashboard || exit 1
nohup python3 -u dashboard_server.py > dashboard.log 2>&1 &
sleep 1
echo "  재시작 완료: $(pgrep -f dashboard_server.py | tr '\n' ' ')"
