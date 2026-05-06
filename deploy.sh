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

# Firebase SDK self-host bundle：每次 deploy 重新 build (升 firebase 版號 / 改 _firebase_entry.js 都會反映)
# 為什麼放在 push 之前：bundle 產物 commit 進 repo (不 gitignore)，避免 server 端要裝 node_modules
echo "==> Build firebase-bundle.js (self-host SDK)"
if command -v npm >/dev/null 2>&1; then
    npm run build:firebase --silent 2>&1 | tail -5
    # 若 bundle 有變動就一併 stage 進當前 commit
    if git diff --quiet -- frontend/static/firebase-bundle.js; then
        echo "  bundle 無變化"
    else
        git add frontend/static/firebase-bundle.js
        git commit --amend --no-edit --no-verify >/dev/null
        echo "  bundle 已更新並 amend 進當前 commit"
    fi
else
    echo "  ⚠ npm 不在 PATH，跳過 bundle (server 會用 commit 裡既有的)"
fi

# Pre-check：production 有正在跑的任務時擋下 deploy（CLAUDE.md 規則 12）
# systemctl restart 會攔腰砍掉 scrape session → 落地殘缺 doc。
# --force 跳過（緊急情況用）；--no-busy-check 也可（若 endpoint 失靈）。
if [[ "$1" != "--force" && "$1" != "--no-busy-check" && "$2" != "--force" && "$2" != "--no-busy-check" ]]; then
    BUSY=$(curl -sf --max-time 5 https://taipei.retty-ai.com/api/busy_state || echo "")
    if [[ -n "$BUSY" ]]; then
        BATCH=$(echo "$BUSY" | sed -n 's/.*"batch_running":\s*\(true\|false\).*/\1/p')
        URL_IF=$(echo "$BUSY" | sed -n 's/.*"url_inflight":\s*\([0-9]*\).*/\1/p')
        if [[ "$BATCH" == "true" || ( -n "$URL_IF" && "$URL_IF" -gt 0 ) ]]; then
            echo "✗ Production 有正在執行的任務 — batch_running=$BATCH url_inflight=$URL_IF"
            echo "  deploy 會把任務攔腰砍斷 (CLAUDE.md 規則 12)。請等任務結束。"
            echo "  確認狀態：curl https://taipei.retty-ai.com/api/busy_state"
            echo "  非要強制可加 --force 跳過檢查"
            exit 1
        fi
        echo "✓ busy check pass (batch_running=$BATCH url_inflight=$URL_IF)"
    else
        echo "⚠ busy_state endpoint 無回應，跳過 busy check (續行 deploy)"
    fi
fi

if [[ "$1" != "--no-push" ]]; then
    echo "==> git push origin main"
    git push origin main
fi

echo "==> SSH → git pull + 寫 VERSION + restart $SERVICE"
# VERSION 檔給 /api/version 讀，admin UI 顯示在「管理後台」badge 旁邊（對版用）
ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$SSH_HOST" "
    cd $APP_DIR &&
    git pull origin main 2>&1 | tail -5 &&
    git rev-parse --short HEAD > VERSION &&
    sudo systemctl restart $SERVICE &&
    echo '✓ service restarted'
"

echo "==> Verify CSS md5 + 取版本號"
LOCAL_MD5=$(md5sum frontend/static/style.css | awk '{print $1}')
LOCAL_SHA=$(git rev-parse --short HEAD)
sleep 2
REMOTE_MD5=$(curl -sf https://taipei.retty-ai.com/static/style.css | md5sum | awk '{print $1}')
REMOTE_SHA=$(curl -sf https://taipei.retty-ai.com/api/version | sed -n 's/.*"sha":"\([^"]*\)".*/\1/p')
if [[ "$LOCAL_MD5" == "$REMOTE_MD5" ]]; then
    echo "✓ CSS md5 match: $LOCAL_MD5"
    echo "✓ Deploy 完成 https://taipei.retty-ai.com"
else
    echo "✗ CSS md5 mismatch (server might still be warming up):"
    echo "  local:  $LOCAL_MD5"
    echo "  server: $REMOTE_MD5"
fi
echo ""
echo "================================="
echo "  版本號 (對 admin UI 用)"
echo "  local commit:  $LOCAL_SHA"
echo "  server /api/version: ${REMOTE_SHA:-(unknown)}"
echo "================================="
