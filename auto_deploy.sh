#!/usr/bin/env bash
# 자동 배포: origin/main 새 커밋을 30초 간격으로 확인하고 서버 재시작.
# 기존 1분 cron 등록을 유지한다. 매 실행에서 즉시 + 30초 후 두 번 확인한다.
#   * * * * * /home/ubuntu/Kiwoom_Monitor/auto_deploy.sh >> /home/ubuntu/Kiwoom_Monitor/auto_deploy.log 2>&1
cd "$(dirname "$0")" || exit 0
# 느린 배포나 중복 cron 실행으로 서버를 동시에 재시작하지 않는다.
exec 9> .git/auto-deploy.lock
flock -n 9 || exit 0
SECONDS=0
check_updates() (
  git fetch -q origin main || return 0
  LOCAL=$(git rev-parse @ 2>/dev/null)
  REMOTE=$(git rev-parse origin/main 2>/dev/null)
  [ -z "$REMOTE" ] && return 0
  [ "$LOCAL" = "$REMOTE" ] && return 0       # 변경 없음 → 종료(로그도 안 남김)

  echo "[$(TZ='Asia/Seoul' date '+%F %T')] 새 커밋 감지($REMOTE) → 배포 시작"
  git pull --ff-only || { echo "  git pull 실패"; return 1; }
  pkill -f 'dashboard_server.py' 2>/dev/null || true
  sleep 1
  cd apps/dashboard || return 1
  nohup python3 -u dashboard_server.py > dashboard.log 2>&1 9>&- &
  sleep 1
  echo "  재시작 완료: $(pgrep -f dashboard_server.py | tr '\n' ' ')"
)
check_updates || exit 1
# 첫 확인에 걸린 시간을 빼서 두 번째 확인을 시작 30초 뒤에 맞춘다.
remaining=$((30 - SECONDS))
if [ "$remaining" -gt 0 ]; then sleep "$remaining"; fi
check_updates
