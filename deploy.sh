#!/bin/bash
# Deploy 都更神探R 到 GCE VM (taipei.retty-ai.com)
#
# 流程：
#   1. git push origin main (從 local 推到 GitHub)
#   2. ssh 到 VM、cd ~/urban-renewal、git pull、restart systemd service
#
# 用法：
#   ./deploy.sh                # full deploy (push + remote pull + restart)
#   ./deploy.sh --no-push      # 假設已 pushed，只做 remote pull + restart
#
# Server: 35.234.38.27 (GCE VM, retty_liu)
# Service: taipei-urban.service (systemd)
# Domain: https://taipei.retty-ai.com

set -e

# 確保 gcloud default project 是 urban-renewal-32f02（之前 default 是 piano-key-detector
# 跑 gcloud compute 系列指令會卡 "Compute Engine API not enabled" Y/N prompt）。
# deploy.sh 本身用 plain ssh 不需要 gcloud，但這條一次性修保證後續 monitor batch / debug 用
# `gcloud compute ssh ...` 不會再卡。靜默跑（gcloud 沒裝 / 已是該 project 都不影響）。
gcloud config set project urban-renewal-32f02 --quiet >/dev/null 2>&1 || true

SSH_HOST="retty_liu@35.234.38.27"
SSH_KEY="$HOME/.ssh/google_compute_engine"
APP_DIR="/home/retty_liu/urban-renewal"
SERVICE="taipei-urban"

if [[ "$1" != "--no-push" ]]; then
    echo "==> git push origin main"
    git push origin main
fi

echo "==> SSH → git pull + restart $SERVICE"
ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$SSH_HOST" "
    cd $APP_DIR &&
    git pull origin main 2>&1 | tail -5 &&
    sudo systemctl restart $SERVICE &&
    echo '✓ service restarted'
"

echo "==> Verify CSS md5 (local vs server)"
LOCAL_MD5=$(md5sum frontend/static/style.css | awk '{print $1}')
sleep 2
REMOTE_MD5=$(curl -sf https://taipei.retty-ai.com/static/style.css | md5sum | awk '{print $1}')
if [[ "$LOCAL_MD5" == "$REMOTE_MD5" ]]; then
    echo "✓ CSS md5 match: $LOCAL_MD5"
    echo "✓ Deploy 完成 https://taipei.retty-ai.com"
else
    echo "✗ CSS md5 mismatch (server might still be warming up):"
    echo "  local:  $LOCAL_MD5"
    echo "  server: $REMOTE_MD5"
fi
