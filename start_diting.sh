#!/usr/bin/env bash
# 启动 Diting 会议智能服务（进房转写 / 滚动摘要 / Meeting Board）
#
# 端口默认 3001，与 CoAssist 的 DITING_BASE_URL=http://127.0.0.1:3001 对齐。
# 敏感配置从本文件同目录的 .env.local 读取（已 gitignore），示例：
#   DITING_ASR_BASE_URL=https://open.bigmodel.cn/api/paas/v4
#   DITING_ASR_API_KEY=sk-xxx
#   DITING_ASR_MODEL=glm-asr
#   DITING_LLM_BASE_URL=https://open.bigmodel.cn/api/paas/v4
#   DITING_LLM_API_KEY=sk-xxx
#   DITING_LLM_MODEL=glm-4-flash
# ASR/LLM 各三项都配置时才启用远程 provider，否则回退本地占位（转写为占位文本）。
set -euo pipefail
cd "$(dirname "$0")"

if [ -f .env.local ]; then
  set -a
  . ./.env.local
  set +a
fi

export DITING_ADDR="${DITING_ADDR:-0.0.0.0:3001}"

if [ -z "${DITING_ASR_BASE_URL:-}" ] || [ -z "${DITING_ASR_API_KEY:-}" ] || [ -z "${DITING_ASR_MODEL:-}" ]; then
  echo "[start_diting] 警告: DITING_ASR_* 未配齐，转写将使用本地占位 provider" >&2
fi

LOG="${DITING_LOG:-/tmp/diting.log}"
nohup ./target/debug/diting >> "$LOG" 2>&1 &
echo $! > /tmp/diting.pid
echo "diting started, pid=$(cat /tmp/diting.pid), addr=$DITING_ADDR, log=$LOG"
