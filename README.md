# Diting Meeting Service

一个使用 Axum、SQLite 和 Tokio 后台任务实现的会议处理服务。音频保存在本地文件系统，任务状态、Rolling Summary 和 Meeting Board 保存在 SQLite。

## 启动

```bash
cargo run
```

服务默认监听 `http://127.0.0.1:3000`，数据库默认为 `data/meeting.db`，可通过 `DITING_DATABASE_URL` 修改：

```bash
DITING_DATABASE_URL=sqlite://data/custom.db cargo run
```

监听地址可通过 `DITING_ADDR` 修改，例如 `DITING_ADDR=127.0.0.1:3001 cargo run`。

音频请求体默认限制为 100 MiB，可以通过 `DITING_MAX_UPLOAD_BYTES` 调整。

## 接入真实模型

服务支持 OpenAI 兼容接口。ASR 需要提供音频转写接口，LLM 需要提供 Chat Completions 接口；以下配置完整时自动启用远程 provider，未完整配置时使用本地 provider：

```bash
export DITING_ASR_BASE_URL=https://api.example.com/v1
export DITING_ASR_API_KEY=...
export DITING_ASR_MODEL=whisper-1
export DITING_LLM_BASE_URL=https://api.example.com/v1
export DITING_LLM_API_KEY=...
export DITING_LLM_MODEL=your-json-capable-model
cargo run
```

ASR 请求为 `POST {base_url}/audio/transcriptions` multipart，LLM 请求为 `POST {base_url}/chat/completions`。LLM 必须返回 `choices[0].message.content` 中的 `SummaryDocument` JSON。上传时显式提供 `transcript` 会跳过 ASR 请求。

## API

```text
POST /api/v1/meetings
GET  /api/v1/meetings          # 会议列表（可按 status 过滤，附分段/说话人/摘要计数）
GET  /api/v1/meetings/{id}
DELETE /api/v1/meetings/{id}
POST /api/v1/meetings/{id}/end
POST /api/v1/meetings/{id}/speakers
GET  /api/v1/meetings/{id}/speakers
POST /api/v1/meetings/{id}/segments
GET  /api/v1/meetings/{id}/segments
GET  /api/v1/meetings/{id}/segments/{segment_id}/audio  # 分段音频下载/播放
GET  /api/v1/meetings/{id}/summaries
GET  /api/v1/meetings/{id}/events   (SSE 实时事件流)
GET  /api/v1/meetings/{id}/board
GET  /api/v1/meetings/{id}/board/versions
GET  /api/v1/jobs?meeting_id={id}&status=failed
PATCH /api/v1/meetings/{id}/segments/{segment_id}  # 人工修订转写文本/说话人，广播 segment.updated
POST /api/v1/jobs/{id}/retry
GET  /health

Swagger UI：`/docs/`，OpenAPI JSON：`/api-docs/openapi.json`。
```

创建会议：

```bash
curl -X POST http://127.0.0.1:3000/api/v1/meetings \
  -H 'content-type: application/json' \
  -d '{"title":"产品周会"}'
```

上传音频分段：

```bash
curl -X POST http://127.0.0.1:3000/api/v1/meetings/$MEETING_ID/segments \
  -F speaker_id=$SPEAKER_ID \
  -F sequence_no=1 \
  -F start_ms=0 \
  -F end_ms=300000 \
  -F transcript='Alice: 本周完成登录模块' \
  -F audio=@sample.wav
```

## 前端页面

`/` 内置三视图控制台（编译进二进制）：

- **会议列表**（默认，`#/meetings`）：按创建时间倒序展示全部会议及状态、分段转写进度、说话人/摘要计数，可按状态过滤；点击进入详情。
- **会议详情**（`#/meetings/<id>`）：转写时间线逐段展示说话人、时间区间、转写状态、转写文本与可播放的分段音频（`<audio>` 直接指向 `/audio` 接口），并展示会议记录（滚动摘要）与 Meeting Board；通过 SSE 实时刷新。
- **操作台**（`#/console`）：创建会议、登记说话人、上传音频分段、查看后台任务。

## 转写调用日志

调用 ASR（`POST {base_url}/audio/transcriptions`）后，`info` 日志会打印完整出参（超过 2000 字符截断）：`status`、`model`、`file` 与 `response`（provider 原始 JSON，含 `text` 字段），便于排查转写内容问题；转写失败时另有 `ASR provider call failed` 告警日志（含会议/分段定位信息）。

## 实时接入

- **LiveKit 进房订阅**：`POST /api/v1/meetings` 携带 `livekit: {"url", "room_name", "token"}` 时，服务以 bot 身份进房订阅全部远端音频轨道，按 `DITING_INGEST_WINDOW_MS`（默认 5000ms）切窗落盘 16kHz 单声道 WAV 并自动转写；说话人按 LiveKit 显示名自动建档。`end_meeting`/`delete_meeting` 会通知进房任务退出并 flush 尾包。token 由调用方用 LiveKit API Key 签发（需 room join + subscribe 权限，建议长 TTL）。
- `POST /api/v1/meetings` 支持 `summary_window_ms`（默认 300000，范围 10000-3600000），实时场景可调小（如 30000），摘要和 Board 按该窗口滚动生成。
- `POST /segments` 的 `speaker_id` 与 `speaker_name` 二选一；只给 `speaker_name` 时按名字自动建档/复用说话人。
- `PATCH /segments/{segment_id}` 接受 `transcript` / `speaker_name`（至少一个）；只更新分段记录并广播 `segment.updated`，不重新转写、不回溯历史滚动摘要。
- `POST /segments` 中 `audio` 与 `transcript` 至少提供一个（进房模式不需要调用该接口）；上游已有实时 ASR 结果时可只传 `transcript`，跳过音频落盘与 ASR 调用，转写立即完成。
- 任务队列由固定 3 秒轮询改为入队即唤醒（上传分段、结束会议、重试任务都会触发），链式任务（转写→摘要）连续执行。
- `GET /api/v1/meetings/{id}/events` 以 SSE 实时推送：`segment.uploaded`、`segment.transcribed`、`segment.failed`、`summary.created`、`board.updated`、`meeting.ended`。订阅后建议先用 segments/summaries/board 接口补拉历史状态，SSE 只推新增事件。

```bash
curl -N http://127.0.0.1:3000/api/v1/meetings/$MEETING_ID/events
```

## 当前处理器

接口、SQLite 任务队列、5 分钟窗口和 Board 版本处理已经打通。上传时可以通过 `transcript` 字段直接提供转写文本；未提供时，当前转写处理器写入 `[transcript provider not configured]` 作为占位文本。`Transcriber` 和 `Summarizer` trait 已经注入 `AppState`，接入实际 ASR 和 LLM 时实现这两个 trait 并替换启动时的 provider 即可，不需要改 Worker 或 API。Summary 使用固定 JSON 结构，Board 会对主题、关键点和行动项去重合并。

Worker 仅消费已经到达 `available_at` 的任务，失败任务最多自动执行三次。服务重启时会自动恢复中断的 `running` 任务；最终失败的任务可以通过 jobs 接口查询并手动重试。

同一窗口内仍有音频正在转写时，Summary 会等待这些转写完成。若已经生成 Summary 后又补传较早时间段的音频，Worker 会创建 `rebuild` 任务，从最早受影响窗口开始重新生成后续 Summary 和 Meeting Board。

同一个受影响窗口的多个迟到分段会合并为一个 `rebuild` 任务；Summary 内容会清理空项、重复项和不支持的行动项状态后再写入 Board。

删除会议会在 SQLite 事务中删除会议、说话人、音频分段、Summary、Board 历史和 jobs，事务成功后删除该会议的本地音频目录。音频目录删除失败不会恢复数据库删除，但会写入错误日志。

转写重试耗尽后分段会标记为 `failed` 且不再阻塞摘要：Worker 会为已完成的部分继续排入后续 Summary；已结束的会议会生成最终 Summary。手动重试任务后该分段会重新参与摘要计算。

## 测试

```bash
cargo test
```

测试不依赖真实 ASR/LLM 服务：provider 层的用例会启动一个返回固定响应的本地 HTTP 服务来模拟 OpenAI 兼容接口；流水线层的用例通过 `FixedTranscriber`/`FixedSummarizer` 桩返回固定文本与固定摘要文档。

## 发布

推送任意 tag 会触发 GitHub Actions（`.github/workflows/release.yml`）：在 GitHub 托管的
x86_64 与 aarch64 runner 上原生执行 `cargo build --release`（webrtc-sys 需要 clang ≥ 21
与 glib 头文件，CI 会自动安装），打包二进制为
`diting-<tag>-<target>.tar.gz`（含 README），连同 `sha256sums.txt` 一起上传到
[GitHub Release](https://github.com/42tr/diting/releases)，并自动生成 release notes。

```bash
git tag v0.1.0
git push origin v0.1.0
```

产物为动态链接 glibc 的 Linux 二进制（构建环境为 Ubuntu 24.04，要求目标系统
glibc ≥ 2.39），前端资源已编译进二进制，部署只需要单个可执行文件。

同一流程还会用两个架构的二进制构建最小多架构 Docker 镜像（`ubuntu:24.04` + 单二进制，
无额外安装包）并推送到 ghcr.io，tag 为去 `v` 的版本号与 `latest`：

```bash
docker run -d --name diting -p 3000:3000 -v diting-data:/app/data \
  ghcr.io/42tr/diting:latest
```

`/app/data` 存放 SQLite 与音频文件，建议挂载持久卷；ASR/LLM/LiveKit 等配置通过
`-e DITING_ASR_BASE_URL=...` 等环境变量注入。仓库根目录提供了可直接使用的
`docker-compose.yaml`（含持久卷与 `/health` 探活）：

```bash
docker compose up -d
```
